"""Daily material prices for the exact ALZURA and RSU inquiry-only releases.

The collection client runs outside this server. Only a checked, bounded price
batch is installed on the private disk. No credentials or supplier actions
are available in this module.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import threading
import time

from shop_cart_quotes import retail_unit_gross_cent
from shop_parts_inquiry import HASH, ID, _projection as rsu_projection
from shop_parts_partner_inquiry import _projection
from shop_parts_release import _stamp, decode, read

FORMAT = 'alzura-inquiry-wearparts-daily-v1'
POINTER = 'alzura-inquiry-daily.pointer.json'
RSU_FORMAT = 'rsu-inquiry-display-q1-daily-v1'
RSU_POINTER = 'rsu-inquiry-display-daily.pointer.json'
MAX_BYTES = 1024 * 1024


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def asset(root, digest, source='ALZURA'):
    require(isinstance(digest, str) and HASH.fullmatch(digest), 'invalid_partner_batch_digest')
    prefix = 'rsu-inquiry-display-daily-' if source == 'RSU' else 'alzura-inquiry-daily-'
    return root / (prefix + digest + '.json')


def scope(root, record):
    data = read(root / record['path'], 2 * 1024 * 1024)
    require(sha256(data).hexdigest() == record['sha256'], 'partner_manifest_changed')
    manifest = decode(data)
    require(record['base_bundle_sha256'] == manifest['base_bundle_sha256'], 'partner_base_changed')
    prices = manifest['display_prices']
    require(0 < len(prices) <= 5000 and all(ID.fullmatch(pid) for pid in prices), 'invalid_partner_scope')
    return manifest, prices


def validate(raw, record, prices, *, now, fresh=False, rsu_proofs=None):
    require(0 < len(raw) <= MAX_BYTES, 'invalid_partner_batch_size')
    data = decode(raw)
    require(isinstance(data, dict) and data.get('bundle_sha256') == record['base_bundle_sha256']
            and data.get('manifest_sha256') == record['sha256'], 'partner_release_mismatch')
    if rsu_proofs is None:
        require(set(data) == {'format', 'bundle_sha256', 'manifest_sha256', 'sources', 'rows'}
                and data['format'] == FORMAT, 'invalid_alzura_format')
        sources, rows = data['sources'], data['rows']
        require(isinstance(sources, list) and 1 <= len(sources) <= 30 and isinstance(rows, dict)
                and set(rows) == set(prices), 'partner_coverage_mismatch')
        stamps, indexed = [], set()
        for source in sources:
            require(isinstance(source, list) and len(source) == 4 and all(
                isinstance(d, str) and HASH.fullmatch(d) for d in source[:3])
                and source[0] not in indexed, 'invalid_partner_source')
            indexed.add(source[0])
            stamps.append(_stamp(source[3]))
    else:
        require(set(data) == {'format', 'bundle_sha256', 'manifest_sha256', 'receipts'}
                and data['format'] == RSU_FORMAT and isinstance(data['receipts'], dict)
                and set(data['receipts']) == set(rsu_proofs) == set(prices), 'invalid_rsu_scope')
        stamps, rows = [], {}
        for pid, receipt in data['receipts'].items():
            proof = rsu_proofs[pid]
            require(isinstance(receipt, dict) and receipt.get('article_id') == proof['mapping']['evidence']['rsu']['article_id']
                    and receipt.get('ean') == proof['expected_ean'] and type(receipt.get('quantity')) is int
                    and receipt['quantity'] == 1, 'rsu_receipt_identity_mismatch')
            fields = {'status', 'article_id', 'ean', 'quantity', 'observed_at'}
            if receipt.get('status') == 'confirmed':
                require(set(receipt) == fields | {'unit_net_cent', 'stock'}, 'invalid_rsu_receipt')
                rows[pid] = [len(stamps), receipt['unit_net_cent'], receipt['stock']]
            else:
                require(set(receipt) == fields and receipt['status'] == 'unavailable', 'invalid_rsu_receipt')
                rows[pid] = None
            stamps.append(_stamp(receipt['observed_at']))
    require(max(stamps) <= now and max(stamps) - min(stamps) <= timedelta(hours=1)
            and (not fresh or now < min(stamps) + timedelta(days=1)), 'invalid_partner_observation')
    for item in rows.values():
        if item is None:
            continue
        require(isinstance(item, list) and len(item) == 3 and all(type(n) is int for n in item)
                and 0 <= item[0] < len(stamps) and 0 < item[1] <= 10_000_000
                and 0 <= item[2] <= 1_000_000, 'invalid_partner_amount')
    return max(stamps), stamps, rows


def projections(prices, stamps, rows):
    values = {}
    for pid, item in rows.items():
        if item is None:
            values[pid] = None
            continue
        index, net_cent, stock = item
        stamp = stamps[index]
        # Both permitted categories use the merchant's same material rate.
        gross = retail_unit_gross_cent(Decimal(net_cent) / 100, 'accessory')
        project = rsu_projection if prices[pid]['source'] == 'RSU' else _projection
        quote = project(prices[pid]['identity_hash'], gross, stamp, stock)['price']
        quote['review_at'] = (stamp + timedelta(days=1)).isoformat()
        values[pid] = quote
    return values


class DailyPartnerPrices:
    def __init__(self, root, record, prices, *, rsu_proofs=None, clock=None, monotonic=time.monotonic):
        self.root, self.record, self.prices = root, record, prices
        self.rsu_proofs = rsu_proofs
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        self.next_read, self.digest, self.observation = 0, None, None
        self.projected = {}
        self.lock = threading.Lock()

    def refresh(self):
        # The pointer changes atomically. Preserve the last verified generation
        # on missing, corrupted or conflicting input. Its original expiry holds.
        pointer = self.root / (RSU_POINTER if self.rsu_proofs is not None else POINTER)
        if not pointer.exists():
            return
        value = decode(read(pointer, 256))
        require(isinstance(value, dict) and set(value) == {'sha256'}, 'invalid_partner_pointer')
        digest = value['sha256']
        target = asset(self.root, digest, 'RSU' if self.rsu_proofs is not None else 'ALZURA')
        if self.digest == digest:
            return
        raw = read(target, MAX_BYTES)
        require(sha256(raw).hexdigest() == digest, 'partner_batch_hash_mismatch')
        observed, stamps, rows = validate(raw, self.record, self.prices, now=self.clock(), rsu_proofs=self.rsu_proofs)
        require(self.observation is None or observed > self.observation, 'older_partner_batch')
        values = projections(self.prices, stamps, rows)
        self.projected, self.observation, self.digest = values, observed, digest

    def lookup(self, pid, previous):
        if pid not in self.prices:
            return previous
        current = self.monotonic()
        if current >= self.next_read and self.lock.acquire(blocking=False):
            try:
                if current >= self.next_read:
                    self.next_read = current + 30
                    try:
                        self.refresh()
                    except (KeyError, OSError, TypeError, ValueError):
                        pass
            finally:
                self.lock.release()
        return self.projected.get(pid, previous)


def attach_daily_partner_prices(catalogue, root, record, approved_ids, source='ALZURA'):
    manifest, prices = scope(Path(root), record)
    require(set(prices) == set(approved_ids) and source in ('RSU', 'ALZURA')
            and all(p['source'] == source for p in prices.values()), 'invalid_partner_scope')
    proofs = manifest['identity_proofs'] if source == 'RSU' else None
    catalogue.daily_inquiry_price_lookups[source] = DailyPartnerPrices(Path(root), record, prices, rsu_proofs=proofs).lookup
