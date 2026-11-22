"""Durable merchant prices from trusted RSU observations, separate from stock.

Construction is inert. Provisioning and seeding are explicit private-server
operations. Display reads never call a provider. Supplier quotes keep their
15-minute validity. Merchant units are reviewed daily and retained
for at most seven days so a bounded refresh batch does not blank the catalogue.
"""
from collections.abc import Mapping
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import threading
import time

REVIEW_INTERVAL = timedelta(days=1)
PRICE_TTL = timedelta(days=7)  # Keep the offer during the bounded refresh batch.
SUPPLIER_TTL = timedelta(minutes=15)
IDENTIFIER = re.compile(r'(?:tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}')
HASH = re.compile(r'[a-f0-9]{64}')
WHEELS = {'tyre', 'alloy_wheel', 'steel_wheel'}
CATEGORIES = WHEELS | {'body_part', 'oil', 'care', 'accessory'}
SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024


class PriceBookError(ValueError):
    pass


def _require(value, reason):
    if not value:
        raise PriceBookError(reason)


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        _require(isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None, 'invalid_clock')
        return value.astimezone(timezone.utc)
    except Exception:
        raise PriceBookError('invalid_clock') from None


class PublishedPriceBook:
    def __init__(self, path, *, products, approved_ids, mode='live', clock=None):
        path = Path(path)
        _require(path.is_absolute() and isinstance(products, Mapping) and len(products) <= 100_000
                 and isinstance(approved_ids, (set, frozenset)) and approved_ids <= products.keys()
                 and mode in {'test', 'live'}, 'invalid_configuration')
        self.products = {}
        for identifier in approved_ids:
            row = products[identifier]
            _require(isinstance(identifier, str) and IDENTIFIER.fullmatch(identifier)
                     and isinstance(row, Mapping) and row.get('id') == identifier
                     and isinstance(row.get('identity_hash'), str) and HASH.fullmatch(row['identity_hash'])
                     and row.get('category') in CATEGORIES, 'invalid_identity')
            self.products[identifier] = {key: row[key] for key in ('id', 'identity_hash', 'category')}
        self.path, self.mode = path, mode
        self.marker_path = path.with_name(path.name + '.initialized')
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.ready = False
        self._entries = {}
        self._requested_reviews = set()
        self._discrepancy_checked_until = {}
        self._last_now = None
        self._lock = threading.RLock()

    def _now(self):
        now = _stamp(self.clock())
        _require(self._last_now is None or now >= self._last_now, 'clock_moved_backwards')
        self._last_now = now
        return now

    def _safe_path(self):
        _require(not self.path.is_symlink() and not self.path.parent.is_symlink()
                 and not self.marker_path.is_symlink()
                 and self.path.resolve().parent == self.path.parent.resolve(), 'unsafe_pricebook_path')

    def _marker_bytes(self):
        return (json.dumps({'format': 'merchant-rsu-prices-v1', 'mode': self.mode,
                            'file': self.path.name}, sort_keys=True, separators=(',', ':')) + '\n').encode('ascii')

    def _marker_exists(self):
        """An established book must never silently become a new, empty store."""
        if not self.marker_path.exists():
            _require(not self.marker_path.is_symlink(), 'unsafe_pricebook_path')
            return False
        _require(not self.marker_path.is_symlink() and self.marker_path.is_file(), 'unsafe_pricebook_path')
        try:
            fd = os.open(self.marker_path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            with os.fdopen(fd, 'rb') as marker:
                _require(stat.S_ISREG(os.fstat(marker.fileno()).st_mode)
                         and marker.read(4096) == self._marker_bytes(), 'foreign_pricebook_marker')
        except OSError:
            raise PriceBookError('pricebook_unavailable') from None
        return True

    def _initialize_marker(self):
        if self._marker_exists():
            return
        try:
            fd = os.open(self.marker_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as marker:
                marker.write(self._marker_bytes())
                marker.flush()
                os.fsync(marker.fileno())
            os.chmod(self.marker_path, 0o600)
            if os.name != 'nt':
                directory = os.open(self.path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        except FileExistsError:
            # Another initializer may have completed; never replace its marker.
            _require(self._marker_exists(), 'pricebook_unavailable')
        except OSError:
            raise PriceBookError('pricebook_unavailable') from None

    @contextmanager
    def _connect(self):
        _require(self.ready, 'pricebook_unavailable')
        self._safe_path()
        _require(self._marker_exists(), 'pricebook_unavailable')
        try:
            db = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=5)
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA synchronous=FULL')
        except sqlite3.Error:
            raise PriceBookError('pricebook_unavailable') from None
        try:
            with db:
                yield db
        finally:
            db.close()

    def provision(self, *, create=False):
        with self._lock:
            self.ready = False
            self._entries = {}
            self._safe_path()
            _require(type(create) is bool, 'pricebook_unavailable')
            if create and not self.path.parent.exists():
                _require(self.path.parent.parent.is_dir() and not self.path.parent.parent.is_symlink(), 'unsafe_pricebook_path')
                self.path.parent.mkdir(mode=0o700)
                os.chmod(self.path.parent, 0o700)
            _require(self.path.parent.is_dir(), 'pricebook_unavailable')
            initialized = self._marker_exists()
            new = not self.path.exists()
            _require(not (initialized and new), 'initialized_pricebook_missing')
            _require(not new or create, 'explicit_pricebook_creation_required')
            if new:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(fd)
            _require(self.path.is_file() and not self.path.is_symlink(), 'unsafe_pricebook_path')
            os.chmod(self.path, 0o600)
            try:
                with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
                    db.execute('PRAGMA journal_mode=WAL')
                    db.execute('PRAGMA synchronous=FULL')
                    if new:
                        db.execute('CREATE TABLE shop_public_pricebook_meta (format TEXT NOT NULL, mode TEXT NOT NULL)')
                        db.execute('INSERT INTO shop_public_pricebook_meta VALUES (?,?)', ('merchant-rsu-prices-v1', self.mode))
                        db.execute('''CREATE TABLE shop_public_prices_v1 (
                            product_id TEXT PRIMARY KEY, identity_hash TEXT NOT NULL,
                            unit_gross_cent INTEGER NOT NULL, published_at TEXT NOT NULL,
                            valid_until TEXT NOT NULL, stock INTEGER NOT NULL,
                            stock_observed_at TEXT NOT NULL, stock_expires_at TEXT NOT NULL,
                            evidence_id TEXT NOT NULL)''')
                    _require(db.execute('SELECT format,mode FROM shop_public_pricebook_meta').fetchall()
                             == [('merchant-rsu-prices-v1', self.mode)], 'foreign_pricebook')
                    _require(db.execute('PRAGMA quick_check').fetchone() == ('ok',), 'pricebook_unavailable')
                    db.row_factory = sqlite3.Row
                    loaded = {row['product_id']: dict(row) for row in db.execute('SELECT * FROM shop_public_prices_v1')}
            except sqlite3.Error:
                raise PriceBookError('pricebook_unavailable') from None
            # This runs only after successful schema validation and commit.
            # A marker left by an interrupted write also fails closed.
            self._initialize_marker()
            self._entries = loaded
            self.ready = True
            return self

    def snapshot(self):
        """Return a private, consistent SQLite backup, including committed WAL."""
        with self._lock:
            _require(self.ready, 'pricebook_unavailable')
            return self._snapshot_existing()

    def _snapshot_existing(self):
        # Read-only backup also works while the public shop is disabled. It
        # never provisions, seeds or changes prices, nor needs product data.
        self._safe_path()
        _require(self.path.is_file() and self._marker_exists(), 'pricebook_unavailable')
        deadline = time.monotonic() + 15
        temporary = None
        try:
            fd, name = tempfile.mkstemp(prefix='.snapshot-', suffix='.sqlite3', dir=self.path.parent)
            temporary = Path(name)
            os.close(fd)
            os.chmod(temporary, 0o600)
            with (closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=5)) as source,
                  closing(sqlite3.connect(temporary)) as target):
                _require(source.execute('SELECT format,mode FROM shop_public_pricebook_meta').fetchall()
                         == [('merchant-rsu-prices-v1', self.mode)], 'foreign_pricebook')
                page_size = source.execute('PRAGMA page_size').fetchone()[0]
                page_count = source.execute('PRAGMA page_count').fetchone()[0]
                _require(0 < page_size * page_count <= SNAPSHOT_MAX_BYTES, 'pricebook_snapshot_too_large')

                def progress(status, remaining, total):
                    _require(time.monotonic() <= deadline, 'pricebook_snapshot_timed_out')
                    _require(total * page_size <= SNAPSHOT_MAX_BYTES, 'pricebook_snapshot_too_large')

                source.backup(target, pages=128, progress=progress, sleep=0.01)
                # Standalone recovery image: no missing source WAL/SHM and
                # usable by both file and in-memory recovery validators.
                target.execute('PRAGMA journal_mode=DELETE')
                _require(target.execute('PRAGMA quick_check').fetchone() == ('ok',), 'pricebook_unavailable')
            _require(0 < temporary.stat().st_size <= SNAPSHOT_MAX_BYTES, 'pricebook_snapshot_too_large')
            return temporary.read_bytes()
        except (sqlite3.Error, OSError):
            raise PriceBookError('pricebook_unavailable') from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _identity(self, product):
        if not isinstance(product, Mapping):
            return None
        identity = self.products.get(product.get('id')) if isinstance(product.get('id'), str) else None
        return identity if identity and all(product.get(key) == identity[key] for key in identity) else None

    def _validated(self, identifier, record, *, seed, now):
        identity = self.products.get(identifier)
        _require(identity and isinstance(record, Mapping) and record.get('identity_hash') == identity['identity_hash']
                 and record.get('product_id', identifier) == identifier and record.get('mode', self.mode) == self.mode
                 and record.get('source', None if seed else 'RSU') == 'RSU'
                 and (seed or record.get('confirmed') is True and record.get('procurement_confirmed') is True
                      and record.get('quantity') == 1 and type(record.get('quantity')) is int)
                 and isinstance(record.get('currency', 'EUR'), str) and record.get('currency', 'EUR').lower() == 'eur'
                 and record.get('vat_percent', '19.00') == '19.00'
                 and type(record.get('unit_gross_cent')) is int and 1 <= record['unit_gross_cent'] <= 10_000_000,
                 'invalid_rsu_price')
        stock = record.get('stock_reported') if seed else record.get('stock')
        _require(type(stock) is int and 0 <= stock <= 1_000_000, 'invalid_rsu_stock')
        observed = _stamp(record.get('observed_at'))
        _require(observed <= now, 'expired_rsu_price')
        if seed and now >= observed + PRICE_TTL:
            return None  # Valid old release seed; never retime it on recovery.
        _require(seed or now < observed + SUPPLIER_TTL, 'expired_rsu_price')
        if not seed:
            expiry = _stamp(record.get('expires_at'))
            _require(observed < expiry <= observed + SUPPLIER_TTL and now < expiry, 'expired_rsu_price')
        return {'product_id': identifier, 'identity_hash': identity['identity_hash'],
                'unit_gross_cent': record['unit_gross_cent'], 'published_at': observed.isoformat(),
                'valid_until': (observed + PRICE_TTL).isoformat(), 'stock': stock,
                'stock_observed_at': observed.isoformat(), 'stock_expires_at': (observed + SUPPLIER_TTL).isoformat()}

    @staticmethod
    def _insert(db, value, evidence):
        db.execute('''INSERT INTO shop_public_prices_v1 VALUES (?,?,?,?,?,?,?,?,?)''',
                   tuple(value[key] for key in ('product_id', 'identity_hash', 'unit_gross_cent',
                         'published_at', 'valid_until', 'stock', 'stock_observed_at', 'stock_expires_at')) + (evidence,))

    def seed(self, records, *, evidence_id):
        """Atomic initial import. Existing rows are never replaced on restart."""
        _require(isinstance(records, Mapping) and len(records) <= 100_000
                 and isinstance(evidence_id, str) and HASH.fullmatch(evidence_id), 'invalid_seed')
        with self._lock:
            now = self._now()
            inserted = 0
            with self._connect() as db:
                # Ignore immutable release seeds for existing rows even after
                # their offer window. A restart must not replace or retime
                # a newer background publication with the old bundled CSV.
                values = [value for identifier, record in records.items()
                          if (value := self._validated(identifier, record, seed=True, now=now)) is not None]
                for value in values:
                    if db.execute('SELECT 1 FROM shop_public_prices_v1 WHERE product_id=?', (value['product_id'],)).fetchone():
                        continue
                    self._insert(db, value, evidence_id)
                    inserted += 1
                loaded = {row['product_id']: dict(row) for row in db.execute('SELECT * FROM shop_public_prices_v1')}
            self._entries = loaded  # Only after the entire transaction commits.
            return inserted

    def publish_uniform(self, product, fresh_q1_quote):
        """Publish one verified unit for all quantities; stock stays separate."""
        with self._lock:
            identity = self._identity(product)
            _require(identity is not None, 'invalid_identity')
            now = self._now()
            value = self._validated(identity['id'], fresh_q1_quote, seed=False, now=now)
            identifier = identity['id']
            requested = identifier in self._requested_reviews
            evidence = sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            with self._connect() as db:
                old = db.execute('SELECT * FROM shop_public_prices_v1 WHERE product_id=?', (identifier,)).fetchone()
                same_variant = old and old['identity_hash'] == identity['identity_hash']
                # An in-flight or cached supplier observation must not replace
                # a more recent price (including one published from the export).
                if same_variant and (_stamp(value['published_at']) <= _stamp(old['published_at'])
                                     or not requested and now < _stamp(old['published_at']) + REVIEW_INTERVAL):
                    if _stamp(value['stock_observed_at']) >= _stamp(old['stock_observed_at']):
                        db.execute('UPDATE shop_public_prices_v1 SET stock=?,stock_observed_at=?,stock_expires_at=? WHERE product_id=?',
                                   (value['stock'], value['stock_observed_at'], value['stock_expires_at'], identity['id']))
                else:
                    if old:
                        if same_variant and _stamp(old['stock_observed_at']) > _stamp(value['stock_observed_at']):
                            for field in ('stock', 'stock_observed_at', 'stock_expires_at'):
                                value[field] = old[field]
                        db.execute('DELETE FROM shop_public_prices_v1 WHERE product_id=?', (identity['id'],))
                    self._insert(db, value, evidence)
                saved = dict(db.execute('SELECT * FROM shop_public_prices_v1 WHERE product_id=?', (identity['id'],)).fetchone())
            self._entries[identity['id']] = saved
            if requested:
                self._requested_reviews.discard(identifier)
                # A quantity-specific increase may coexist with an unchanged
                # Q1 price. Don't spend the hourly budget rechecking the same
                # evidence while that exact supplier observation is cached.
                self._discrepancy_checked_until[identifier] = _stamp(fresh_q1_quote['expires_at'])
            return True

    def due_ids(self):
        with self._lock:
            _require(self.ready, 'pricebook_unavailable')
            now = self._now()
            rows = self._entries
            return [identifier for identifier, identity in sorted(self.products.items())
                    if identifier in self._requested_reviews or identifier not in rows or rows[identifier]['identity_hash'] != identity['identity_hash']
                    or now >= _stamp(rows[identifier]['published_at']) + REVIEW_INTERVAL]

    def review_due(self, identifier):
        """A published variant is due daily or after a verified supplier increase."""
        with self._lock:
            _require(self.ready, 'pricebook_unavailable')
            row = self._entries.get(identifier)
            identity = self.products.get(identifier)
            return bool(row and identity and row['identity_hash'] == identity['identity_hash']
                        and (identifier in self._requested_reviews or self._now() >= _stamp(row['published_at']) + REVIEW_INTERVAL))

    def get(self, identifier):
        if not isinstance(identifier, str) or identifier not in self.products:
            return None
        with self._lock:
            _require(self.ready, 'pricebook_unavailable')
            now = self._now()
            row = self._entries.get(identifier)
            if (not row or row['identity_hash'] != self.products[identifier]['identity_hash']
                    or not _stamp(row['published_at']) <= now < _stamp(row['valid_until'])):
                return None
            maximum = 4 if self.products[identifier]['category'] in WHEELS else 20
            return {'source': 'RSU', 'kind': 'merchant_pricebook', 'currency': 'EUR', 'vat_percent': '19.00',
                    'observed_at': row['published_at'], 'expires_at': row['valid_until'],
                    'review_at': (_stamp(row['published_at']) + REVIEW_INTERVAL).isoformat(),
                    'stock_reported': row['stock'], 'stock_observed_at': row['stock_observed_at'],
                    'stock_expires_at': row['stock_expires_at'],
                    'stock_current': _stamp(row['stock_observed_at']) <= now < _stamp(row['stock_expires_at']),
                    'prices_by_quantity': {str(q): {'quantity': q, 'unit_gross_cent': row['unit_gross_cent'],
                        'items_gross_cent': q * row['unit_gross_cent'], 'vat_percent': '19.00'} for q in range(1, maximum + 1)}}

    def checkout_quote(self, product, quantity, fresh_quote, *, report_price_change=False):
        """Fresh procurement check; fixed merchant amount, never repricing."""
        identity = self._identity(product)
        if not identity or type(quantity) is not int or not 1 <= quantity <= (4 if identity['category'] in WHEELS else 20):
            return None
        published = self.get(identity['id'])
        if published is None:
            return None
        try:
            self._safe_path()
            _require(self.path.is_file() and self._marker_exists(), 'pricebook_unavailable')
            current = fresh_quote(product, quantity)
            with self._lock:
                # A scheduled review can publish a different price during the supplier read.
                published = self.get(identity['id'])
                now = self._now()
            if (published is None or not isinstance(current, Mapping) or current.get('confirmed') is not True
                    or current.get('procurement_confirmed') is not True
                    or current.get('source', 'RSU') != 'RSU'
                    or current.get('product_id') != identity['id'] or current.get('identity_hash') != identity['identity_hash']
                    or current.get('mode') != self.mode or current.get('currency') != 'eur'
                    or current.get('vat_percent') != '19.00' or type(current.get('quantity')) is not int
                    or current['quantity'] != quantity or type(current.get('stock')) is not int
                    or not quantity <= current['stock'] <= 1_000_000):
                return None
            observed, expiry = _stamp(current.get('observed_at')), _stamp(current.get('expires_at'))
            unit = published['prices_by_quantity'][str(quantity)]['unit_gross_cent']
            if (not observed <= now < expiry or not timedelta(0) < expiry - observed <= SUPPLIER_TTL
                    or type(current.get('unit_gross_cent')) is not int or not 0 < current['unit_gross_cent'] <= 10_000_000
                    or not now < _stamp(published['expires_at'])):
                return None
            if current['unit_gross_cent'] > unit:
                if report_price_change:
                    with self._lock:
                        identifier = identity['id']
                        until = self._discrepancy_checked_until.get(identifier)
                        if until is None or now >= until:
                            self._requested_reviews.add(identifier)
                return {'confirmed': False, 'reason': 'published_price_review_required'} if report_price_change else None
            return {**current, 'unit_gross_cent': unit,
                    'expires_at': min(expiry, _stamp(published['expires_at'])).isoformat()}
        except Exception:
            return None


def existing_pricebook_backup(persistent_data_dir, *, mode='live'):
    """Optional private backup material; never creates or restores a store.

    A completely uninstalled book is absent. An existing directory with lost,
    unmarked, corrupt or foreign data is an error, not an empty pricebook.
    """
    root = Path(persistent_data_dir)
    _require(root.is_absolute() and root.is_dir() and not root.is_symlink(), 'unsafe_pricebook_path')
    directory = root / 'shop-pricebook'
    _require(not directory.is_symlink(), 'unsafe_pricebook_path')
    if not directory.exists():
        return None
    _require(directory.is_dir() and directory.resolve().parent == root.resolve(), 'unsafe_pricebook_path')
    book = PublishedPriceBook(directory / 'prices.sqlite3', products={}, approved_ids=frozenset(), mode=mode)
    data = book._snapshot_existing()
    return {'data': data, 'marker': book._marker_bytes()}
