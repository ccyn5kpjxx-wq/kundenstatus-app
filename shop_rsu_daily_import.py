"""Apply an approved, identity-bound RSU daily observation to the running price book."""
from datetime import timedelta
from decimal import Decimal
import gzip
from hashlib import sha256
import io
from itertools import combinations
import json
from pathlib import Path
import zlib

from shop_cart_quotes import retail_unit_gross_cent
from shop_price_book import HASH, PRICE_TTL, SUPPLIER_TTL, WHEELS, PriceBookError, _stamp

FORMAT = "rsu-daily-net-stock-v2"
POINTER = "rsu-daily-prices.current.json"
MAX_PACKED = 2_000_000
MAX_JSON = 8_000_000


def _check(ok):
    if not ok:
        raise PriceBookError("invalid_daily_price_import")


def binding_digest(rows):
    """Ordered [public ID, variant digest, category, RSU article ID] tuples."""
    return sha256(json.dumps(rows, ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest()


def decode_batch(packed):
    _check(isinstance(packed, bytes) and 0 < len(packed) <= MAX_PACKED)
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(packed), mode="rb") as stream:
            raw = stream.read(MAX_JSON + 1)
        _check(len(raw) <= MAX_JSON)
        value = json.loads(raw)
        _check(isinstance(value, dict) and set(value) == {"format", "bundle_sha256", "rsu_csv_sha256", "q1_evidence_sha256",
                     "pricebook_snapshot_sha256", "source_last_modified", "binding_sha256", "observed_at", "offers"})
        _check(value["format"] == FORMAT and all(isinstance(value[key], str) and HASH.fullmatch(value[key])
                                                for key in ("bundle_sha256", "rsu_csv_sha256", "q1_evidence_sha256", "pricebook_snapshot_sha256", "binding_sha256")))
        fetched = _stamp(value["observed_at"])
        if value["source_last_modified"] is not None:
            _check(isinstance(value["source_last_modified"], str) and _stamp(value["source_last_modified"]) <= fetched)
        offers = value["offers"]
        _check(isinstance(offers, list) and 0 < len(offers) <= 100_000 and any(v is not None for v in offers))
        for row in offers:
            _check(row is None or (isinstance(row, list) and len(row) in (2,3)
                   and type(row[0]) is int and 0 < row[0] <= 7_000_000
                   and type(row[1]) is int and 0 <= row[1] <= 1_000_000))
            if row is not None and len(row) == 3:
                _check(isinstance(row[2], str) and fetched <= _stamp(row[2]) < fetched + timedelta(days=1))
        return value
    except (ValueError, TypeError, OSError, EOFError, zlib.error):
        raise PriceBookError("invalid_daily_price_import") from None


def batch_path(root, digest):
    _check(isinstance(digest, str) and HASH.fullmatch(digest))
    return Path(root) / ("rsu-daily-" + digest + ".json.gz")


class DailyPriceImport:
    def __init__(self, book, release_root, bundle_sha256, supplier, *, supplementary_ids=()):
        self.book, self.root, self.bundle = book, Path(release_root), bundle_sha256
        self.supplier = supplier
        self.supplementary_ids = frozenset(supplementary_ids)
        self._applied = ""
        self.last_result = None

    def __call__(self):
        pointer = self.root / POINTER
        if not pointer.exists():
            return None
        _check(not self.root.is_symlink() and not pointer.is_symlink() and pointer.is_file() and pointer.stat().st_size <= 256)
        target = json.loads(pointer.read_bytes())
        _check(isinstance(target, dict) and set(target) == {"sha256"})
        digest = target["sha256"]
        path = batch_path(self.root, digest)
        if self._applied == digest:
            return self.last_result
        _check(not path.is_symlink() and path.is_file() and 0 < path.stat().st_size <= MAX_PACKED)
        packed = path.read_bytes()
        _check(sha256(packed).hexdigest() == digest)
        batch = decode_batch(packed)
        _check(batch["bundle_sha256"] == self.bundle)
        binding_rows = []
        for identifier, product in sorted(self.book.products.items()):
            if product["category"] not in WHEELS:
                continue
            verified = self.supplier.rsu_mapping(product)
            _check(verified and verified.get("confirmed") is True and verified.get("supplier") == "RSU"
                   and verified.get("product_id") == identifier and verified.get("identity_hash") == product["identity_hash"])
            binding_rows.append([identifier, product["identity_hash"], product["category"], verified["supplier_article_id"]])
        missing = len(binding_rows) - len(batch["offers"])
        if 0 < missing <= len(self.supplementary_ids):
            # Legacy producers can cover any previously installed subset of
            # the pinned additions. Never omit a product from the base scope.
            for excluded in combinations(self.supplementary_ids, missing):
                previous = [row for row in binding_rows if row[0] not in excluded]
                if binding_digest(previous) == batch["binding_sha256"]:
                    binding_rows = previous
                    break
        _check(binding_digest(binding_rows) == batch["binding_sha256"] and len(binding_rows) == len(batch["offers"]))
        observed = _stamp(batch["observed_at"])
        source = batch["rsu_csv_sha256"]
        with self.book._lock:
            now = self.book._now()
            _check(observed <= now)
            result = {"source_sha256": source, "observed_at": observed.isoformat(),
                      "applied": 0, "q1_applied": 0, "newer_or_same": 0, "recent_observation_preserved": 0,
                      "price_change_needs_q1": 0, "unmatched": 0}
            if now >= observed + timedelta(days=1):
                result["expired"] = True
            else:
                updated = set()
                needs_q1 = set()
                with self.book._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    existing = {r["product_id"]: dict(r) for r in db.execute("SELECT * FROM shop_public_prices_v1")}
                    for (identifier, identity, category, _), offer in zip(binding_rows, batch["offers"]):
                        if offer is None:
                            result["unmatched"] += 1
                            continue
                        old = existing.get(identifier)
                        if old and old["identity_hash"] != identity:
                            raise PriceBookError("daily_price_variant_changed")
                        direct = len(offer) == 3
                        stamp = (_stamp(offer[2]) if direct else
                                 _stamp(batch["source_last_modified"]) if batch["source_last_modified"] else observed)
                        _check(stamp <= now)
                        if now >= stamp + timedelta(days=1):
                            result["newer_or_same"] += 1
                            continue
                        last = max(_stamp(old["published_at"]), _stamp(old["stock_observed_at"])) if old else None
                        if last and last >= stamp:
                            result["newer_or_same"] += 1
                            continue
                        if last and not direct and not batch["source_last_modified"] and last >= observed - timedelta(days=1):
                            # HTTP download time is not evidence that an undated file is newer
                            # than a supplier observation in the preceding daily review window.
                            result["recent_observation_preserved"] += 1
                            continue
                        unit = retail_unit_gross_cent(Decimal(offer[0]) / 100, category)
                        if old and not direct and 4 * abs(unit - old["unit_gross_cent"]) >= old["unit_gross_cent"]:
                            result["price_change_needs_q1"] += 1
                            needs_q1.add(identifier)
                            continue
                        evidence = batch["q1_evidence_sha256"] if direct else source
                        row = {"product_id": identifier, "identity_hash": identity, "unit_gross_cent": unit,
                               "published_at": stamp.isoformat(), "valid_until": (stamp + PRICE_TTL).isoformat(),
                               "stock": offer[1], "stock_observed_at": stamp.isoformat(),
                               "stock_expires_at": (stamp + SUPPLIER_TTL).isoformat(), "evidence_id": evidence}
                        if old:
                            db.execute("DELETE FROM shop_public_prices_v1 WHERE product_id=?", (identifier,))
                        self.book._insert(db, row, evidence)
                        existing[identifier] = row
                        updated.add(identifier)
                        result["applied"] += 1
                        result["q1_applied"] += int(direct)
                # Only after SQLite committed, publish the complete new snapshot under its own lock.
                self.book._entries = existing
                self.book._requested_reviews.difference_update(updated)
                self.book._requested_reviews.update(needs_q1)
            self._applied, self.last_result = digest, result
            return result
