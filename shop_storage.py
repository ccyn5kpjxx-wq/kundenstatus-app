"""Explicit private SQLite provisioning for shop orders and buyer sessions.

No filesystem/database action occurs on import or construction. The hosting
bootstrap must deliberately provision below its persistent DATA_DIR; errors
never fall back to ephemeral storage or the portal's PostgreSQL connection.
"""
from contextlib import closing, ExitStack
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import time
from uuid import UUID

from shop_buyer import init_buyer_schema, SQLiteBuyerBackend
from shop_order_store import init_schema, OrderStore

MAX_SNAPSHOT = 64 * 1024 * 1024
MAX_OUTBOX_RECORDS = 1000
MAX_OUTBOX_BYTES = 64 * 1024 * 1024
MAX_OUTBOX_FILE = 16 * 1024 * 1024
MAX_OUTBOX_JOURNAL = 65536
SHOP_RESTORE_TABLES = frozenset({'shop_quotes_v2','shop_orders_v2','shop_payment_attempts_v2',
    'shop_payment_events_v2','shop_buyer_sessions_v2','shop_fulfillment_v2',
    'shop_order_workflow_v2','shop_withdrawals_v2','shop_lexware_invoices_v1','shop_invoice_activation_v1',
    'shop_public_pricebook_meta','shop_public_prices_v1'})
SHOP_RESTORE_MESSAGE = ('Diese Sicherung enthält Shop-Bestellungen oder veröffentlichte Shoppreise. Portal und Shop müssen gemeinsam '
    'mit Shopdatenbank, Preisbuch, Initialisierungsdateien und Versandprotokollen kontrolliert wiederhergestellt werden. '
    'Der bisherige Portalimport ist dafür gesperrt; keine Bestellungen oder Zahlungen erneut auslösen.')


def reject_portal_only_restore(*, export=None, manifest=None, names=None, imported_db=None):
    """Fence the legacy destructive importer before it can omit shop state.

    This validates evidence only; it never creates, restores or opens a target
    database. A standalone uploaded shop SQLite is inspected read-only.
    """
    for metadata in (export,manifest):
        if isinstance(metadata,dict):
            if 'shop_v2' in metadata:
                raise ValueError(SHOP_RESTORE_MESSAGE)
            tables=metadata.get('tables')
            if isinstance(tables,dict) and SHOP_RESTORE_TABLES.intersection(tables):
                raise ValueError(SHOP_RESTORE_MESSAGE)
    if names is not None:
        if not isinstance(names,(set,frozenset,list,tuple,dict)) or len(names)>100000:
            raise ValueError('Datenpaket ungültig: Archivliste kann nicht sicher geprüft werden.')
        for name in names:
            if not isinstance(name,str):
                raise ValueError('Datenpaket ungültig: Archivliste kann nicht sicher geprüft werden.')
            if {'shop-v2', 'shop-pricebook'}.intersection(name.replace('\\','/').casefold().split('/')):
                raise ValueError(SHOP_RESTORE_MESSAGE)
    if imported_db is not None:
        path=Path(imported_db).resolve()
        if not path.is_file():
            raise ValueError('Datenpaket ungültig: Datenbankdatei fehlt.')
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as source:
                placeholders=','.join('?' for _ in SHOP_RESTORE_TABLES)
                if source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ("+
                    placeholders+') LIMIT 1',sorted(SHOP_RESTORE_TABLES)).fetchone():
                    raise ValueError(SHOP_RESTORE_MESSAGE)
        except sqlite3.Error:
            raise ValueError('Datenpaket ungültig: Datenbankdatei kann nicht sicher geprüft werden.') from None


class StorageError(ValueError):
    pass


class ShopStorage:
    def __init__(self, persistent_data_dir, *, clock=None):
        root = Path(persistent_data_dir)
        if not root.is_absolute():
            raise StorageError('persistent_absolute_root_required')
        self.root = root
        self.directory = root / 'shop-v2'
        self.path = self.directory / 'shop.sqlite3'
        self.marker = self.directory / 'initialized'
        self.clock = clock
        self.ready = False

    def _paths(self):
        if (self.root.is_symlink() or self.directory.is_symlink() or self.path.is_symlink() or self.marker.is_symlink()
                or self.directory.resolve().parent != self.root.resolve()
                or self.path.resolve().parent != self.directory.resolve()):
            raise StorageError('unsafe_shop_storage_path')

    def provision(self, *, create=False):
        self.ready = False
        self._paths()
        if not self.root.is_dir():
            raise StorageError('persistent_root_unavailable')
        if type(create) is not bool:
            raise StorageError('explicit_storage_creation_required')
        initialized = self.marker.is_file()
        if initialized:
            if (self.marker.read_bytes() != b'shop_storage_v2\n' or not self.path.is_file()
                    or self.path.stat().st_size == 0):
                raise StorageError('initialized_shop_database_missing_or_invalid')
        elif not create or self.directory.exists():
            # A pre-existing unmarked directory may be an interrupted setup or
            # lost database. Never announce a fresh empty shop after a restart.
            raise StorageError('explicit_storage_creation_required')
        try:
            self.directory.mkdir(mode=0o700, exist_ok=True)
            self._paths()
            if self.path.exists() and not self.path.is_file():
                raise StorageError('unsafe_shop_storage_path')
            # Exclusive creation avoids inheriting a broad mode for a new file.
            if not self.path.exists():
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(descriptor)
            os.chmod(self.path, 0o600)
            with closing(sqlite3.connect(self.path, timeout=5)) as connection:
                if initialized:
                    objects = {(row[0], row[1]) for row in connection.execute('SELECT type,name FROM sqlite_master')}
                    required = {('table', name) for name in ('shop_quotes_v2', 'shop_orders_v2',
                        'shop_payment_attempts_v2', 'shop_payment_events_v2', 'shop_buyer_sessions_v2')}
                    required |= {('trigger', 'shop_quotes_v2_immutable'), ('trigger', 'shop_orders_v2_immutable')}
                    if not required <= objects:
                        raise StorageError('initialized_shop_database_missing_or_invalid')
                connection.execute('PRAGMA journal_mode=WAL')
                connection.execute('PRAGMA synchronous=FULL')
                init_schema(connection)
                init_buyer_schema(connection)
                connection.commit()
                if connection.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise StorageError('shop_storage_unavailable')
            if not initialized:
                descriptor = os.open(self.marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(descriptor, 'wb') as marker_file:
                    marker_file.write(b'shop_storage_v2\n')
                    marker_file.flush()
                    os.fsync(marker_file.fileno())
            self.ready = True
        except StorageError:
            raise
        except Exception:
            raise StorageError('shop_storage_unavailable') from None
        return self

    def connect(self):
        if self.ready is not True:
            raise StorageError('shop_storage_not_provisioned')
        self._paths()
        try:
            # mode=rw never silently recreates an empty lost database.
            connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA synchronous=FULL')
            return connection
        except Exception:
            raise StorageError('shop_storage_unavailable') from None

    def order_store(self):
        return OrderStore(self.connect, clock=self.clock)

    def buyer_backend(self):
        return SQLiteBuyerBackend(self.connect, clock=self.clock)

    def snapshot(self):
        """Consistent SQLite backup bytes, including WAL-committed records.

        Private customer data: caller may only place this inside the existing
        authenticated backup package, never static assets or a public route.
        """
        self._paths()
        if not self.ready or not self.path.is_file() or self.path.stat().st_size > MAX_SNAPSHOT:
            raise StorageError('shop_backup_unavailable')
        temporary = None
        try:
            descriptor, name = tempfile.mkstemp(prefix='.snapshot-', suffix='.sqlite3', dir=self.directory)
            temporary = Path(name)
            os.close(descriptor)
            os.chmod(temporary, 0o600)
            deadline = time.monotonic() + 15

            def progress(status, remaining, total):
                if time.monotonic() > deadline or total * 4096 > MAX_SNAPSHOT:
                    raise StorageError('shop_backup_unavailable')

            with closing(self.connect()) as source, closing(sqlite3.connect(temporary)) as target:
                source.backup(target, pages=128, progress=progress, sleep=0.01)
                # This independent recovery image needs no sidecar WAL/SHM;
                # the live source remains WAL-enabled and unchanged.
                target.execute('PRAGMA journal_mode=DELETE')
                if target.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise StorageError('shop_backup_unavailable')
            if temporary.stat().st_size > MAX_SNAPSHOT:
                raise StorageError('shop_backup_unavailable')
            return temporary.read_bytes()
        except StorageError:
            raise
        except Exception:
            raise StorageError('shop_backup_unavailable') from None
        finally:
            if temporary is not None and temporary.parent == self.directory:
                temporary.unlink(missing_ok=True)

    def write_backup(self, archive):
        """Private recovery package: ledger, published prices and mail fences.

        The SQLite image is the recovery point. Outbox rows must still match
        that image after all per-message OS locks are acquired; active sends
        fail the backup rather than claim a complete cross-store snapshot.
        Only referenced MIME/current-attempt journals and inert lock markers
        are copied. An installed merchant pricebook is backed up read-only
        with its marker; an invalid installed book cannot be silently omitted.
        Restoring an older financial backup still requires manual
        provider reconciliation before workers or checkout can be rearmed.
        """
        from mailbox_outbox import _lock_fd
        from shop_price_book import existing_pricebook_backup
        data = self.snapshot()
        deadline = time.monotonic() + 15
        try:
            self._paths()
            pricebook = existing_pricebook_backup(self.root)
            marker = self.marker.read_bytes()
            if marker != b'shop_storage_v2\n':
                raise ValueError()
            with closing(sqlite3.connect(':memory:')) as frozen:
                frozen.deserialize(data)
                rows = self._outbox_rows(frozen)
            folder = self.directory / 'mail-outbox'
            if folder.is_symlink() or folder.resolve().parent != self.directory.resolve():
                raise ValueError()
            files = {}
            with ExitStack() as locks:
                for row in rows:
                    if time.monotonic() > deadline:
                        raise ValueError()
                    key = sha256(row['token'].encode('ascii')).hexdigest()
                    path = folder / (key + '.lock')
                    self._checked_file(path, folder, maximum=1, expected=b'0')
                    descriptor = os.open(path, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
                    locks.callback(os.close, descriptor)
                    opened, named = os.fstat(descriptor), path.stat(follow_symlinks=False)
                    if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
                        raise ValueError()
                    _lock_fd(descriptor)
                    locks.callback(_lock_fd, descriptor, unlock=True)
                    files[key + '.lock'] = b'0'
                with closing(self.connect()) as current:
                    if self._outbox_rows(current) != rows:
                        raise ValueError()
                total = 0
                for row in rows:
                    key = sha256(row['token'].encode('ascii')).hexdigest()
                    journal_name = key + '.' + row['attempt'] + '.json'
                    journal_path = folder / journal_name
                    effective = row
                    if journal_path.exists() or journal_path.is_symlink():
                        raw = self._checked_file(journal_path, folder, maximum=MAX_OUTBOX_JOURNAL)
                        journal = self._checked_journal(raw, row)
                        files[journal_name] = raw
                        if journal['revision'] >= row['revision']:
                            effective = {**row, **journal}
                    path = folder / row['payload_name']
                    if path.exists() or path.is_symlink():
                        raw = self._checked_file(path, folder, maximum=MAX_OUTBOX_FILE)
                        from email import policy
                        from email.parser import BytesParser
                        from mailbox_outbox import _fingerprint
                        parser = BytesParser(policy=policy.default)
                        message = parser.parsebytes(raw)
                        # MailOutbox hashes the pre-serialization EmailMessage;
                        # SMTP serialization changes default-policy LF to CRLF.
                        # Check both equivalent framing forms, preserving the
                        # original archived bytes and attachment contents.
                        fingerprints = {_fingerprint(message),
                                        _fingerprint(parser.parsebytes(raw.replace(b'\r\n', b'\n')))}
                        if row['fingerprint'] not in fingerprints or str(message.get('Message-ID')) != row['message_id']:
                            raise ValueError()
                        files[row['payload_name']] = raw
                    elif not (effective['state'] in {'sent', 'partial'} and effective['copy_done'] == 1):
                        raise ValueError()
                    total = sum(len(value) for value in files.values())
                    if total > MAX_OUTBOX_BYTES or time.monotonic() > deadline:
                        raise ValueError()
                with closing(self.connect()) as current:
                    if self._outbox_rows(current) != rows:
                        raise ValueError()
                # The marker and all private mail material have been validated
                # before any of them enter the caller's authenticated archive.
                name = 'shop-v2/shop.sqlite3'
                marker_name = 'shop-v2/initialized'
                archive.writestr(name, data)
                archive.writestr(marker_name, marker)
                manifest = []
                for filename, raw in sorted(files.items()):
                    target = 'shop-v2/mail-outbox/' + filename
                    archive.writestr(target, raw)
                    manifest.append({'path': target, 'bytes': len(raw), 'sha256': sha256(raw).hexdigest()})
                result = {'format': 'shop_sqlite_v2', 'path': name, 'bytes': len(data),
                        'sha256': sha256(data).hexdigest(),
                        'marker': {'path': marker_name, 'bytes': len(marker), 'sha256': sha256(marker).hexdigest()},
                        'outbox': {'records': len(rows), 'bytes': total, 'files': manifest}}
                if pricebook is not None:
                    price_path, price_marker = ('shop-pricebook/prices.sqlite3',
                                                'shop-pricebook/prices.sqlite3.initialized')
                    archive.writestr(price_path, pricebook['data'])
                    archive.writestr(price_marker, pricebook['marker'])
                    result['pricebook'] = {'format': 'merchant-rsu-prices-v1', 'path': price_path,
                        'bytes': len(pricebook['data']), 'sha256': sha256(pricebook['data']).hexdigest(),
                        'marker': {'path': price_marker, 'bytes': len(pricebook['marker']),
                                   'sha256': sha256(pricebook['marker']).hexdigest()}}
                return result
        except Exception:
            raise StorageError('shop_backup_unavailable') from None

    @staticmethod
    def _outbox_rows(connection):
        connection.row_factory = sqlite3.Row
        if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mailbox_outbox'").fetchone():
            return []
        rows = [dict(row) for row in connection.execute('SELECT * FROM mailbox_outbox ORDER BY token LIMIT ?',
                                                       (MAX_OUTBOX_RECORDS + 1,))]
        if len(rows) > MAX_OUTBOX_RECORDS:
            raise ValueError()
        for row in rows:
            if (not isinstance(row['token'], str) or str(UUID(row['token'])) != row['token']
                    or not isinstance(row['attempt'], str) or not re.fullmatch(r'[a-f0-9]{32}', row['attempt'])
                    or not isinstance(row['fingerprint'], str) or not re.fullmatch(r'[a-f0-9]{64}', row['fingerprint'])
                    or row['state'] not in {'sending', 'uncertain', 'not_sent', 'copy_pending', 'partial', 'sent'}
                    or type(row['revision']) is not int or not 0 <= row['revision'] <= 1_000_000_000
                    or type(row['copy_done']) is not int or row['copy_done'] not in (0, 1)):
                raise ValueError()
            key = sha256(row['token'].encode('ascii')).hexdigest()
            if (not isinstance(row['payload_name'], str)
                    or not re.fullmatch(key + r'(?:\.[a-f0-9]{32})?\.eml', row['payload_name'])):
                raise ValueError()
        return rows

    @staticmethod
    def _checked_file(path, folder, *, maximum, expected=None):
        if path.is_symlink() or path.resolve().parent != folder.resolve():
            raise ValueError()
        before = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
            raise ValueError()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(descriptor, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError()
            data = stream.read(maximum + 1)
            after = os.fstat(stream.fileno())
        named = path.stat(follow_symlinks=False)
        identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
        if identity(before) != identity(after) or identity(after) != identity(named) or len(data) != before.st_size:
            raise ValueError()
        if expected is not None and data != expected:
            raise ValueError()
        return data

    @staticmethod
    def _checked_journal(raw, row):
        from mailbox_outbox import _RESULT_FIELDS
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result
        value = json.loads(raw, object_pairs_hook=unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if (not isinstance(value, dict) or set(value) != set((*_RESULT_FIELDS, 'attempt', 'token'))
                or value['token'] != row['token'] or value['attempt'] != row['attempt']
                or type(value['revision']) is not int or not 0 <= value['revision'] <= 1_000_000_000
                or value['state'] not in {'sending', 'uncertain', 'not_sent', 'copy_pending', 'partial', 'sent'}
                or type(value['copy_done']) not in (int, bool) or value['copy_done'] not in (0, 1)
                or not isinstance(value['accepted'], list) or not isinstance(value['refused'], dict)):
            raise ValueError()
        return value
