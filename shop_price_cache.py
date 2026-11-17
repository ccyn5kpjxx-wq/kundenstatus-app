"""Bounded background RSU gross-price cache; disabled until explicitly armed.

No thread, provider call, credentials or filesystem IO on import/construction.
get() never calls a provider. Anonymous page requests can enqueue only approved
identities within queue, singleflight, spacing and rolling read-budget limits.
The cache is a browsing aid, never an authoritative checkout/order provider.
Read-budget settings are permission/resource controls, not billing assertions.
"""
from collections import OrderedDict, deque
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math
import re
import threading
import time

ID = re.compile(r"(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}")
HASH = re.compile(r"[a-f0-9]{64}")
TTL = timedelta(minutes=15)
MAX_PRODUCTS = 100_000
QUEUE_LEASE_SECONDS = 30
WHEELS = frozenset({"tyre", "alloy_wheel", "steel_wheel"})
PARTS = frozenset({"body_part", "oil", "care", "accessory"})


class PriceCacheError(ValueError):
    """Fixed local errors, never provider exceptions or catalogue data."""


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise PriceCacheError("invalid_clock") from None


def _duration(value, minimum, maximum):
    return (type(value) in (int, float) and math.isfinite(value) and minimum <= value <= maximum)


class PublicPriceCache:
    def __init__(self, products, *, approved_ids, provider, mode="live", enabled=False,
                 reads_approved=False, max_provider_reads_per_period=0, period_seconds=3600,
                 spacing_seconds=2, failure_backoff_seconds=60, max_queue=48, max_workers=1,
                 max_cache_entries=512, clock=None, monotonic=None,
                 source='RSU', require_procurement=True):
        if (not isinstance(products, Mapping) or len(products) > MAX_PRODUCTS
                or not isinstance(approved_ids, (set, frozenset)) or len(approved_ids) > MAX_PRODUCTS
                or any(not isinstance(v, str) or not ID.fullmatch(v) for v in approved_ids)
                or not approved_ids <= products.keys() or not callable(getattr(provider, "retail_quote", None))
                or mode not in {"test", "live"} or type(enabled) is not bool or type(reads_approved) is not bool
                or source not in {'RSU', 'ALZURA'} or type(require_procurement) is not bool
                or source == 'RSU' and require_procurement is not True
                or type(max_provider_reads_per_period) is not int or not 0 <= max_provider_reads_per_period <= 100_000
                or not _duration(period_seconds, 1, 2_678_400) or not _duration(spacing_seconds, 0.1, 3600)
                or not _duration(failure_backoff_seconds, 1, 86400)
                or type(max_queue) is not int or not 1 <= max_queue <= 512
                or type(max_workers) is not int or not 1 <= max_workers <= 2
                or type(max_cache_entries) is not int or not 1 <= max_cache_entries <= 4096):
            raise PriceCacheError("invalid_configuration")
        selected = {}
        for pid in sorted(approved_ids):
            row = products[pid]
            if (not isinstance(row, Mapping) or row.get("id") != pid
                    or not isinstance(row.get("identity_hash"), str) or not HASH.fullmatch(row["identity_hash"])
                    or row.get("category") not in WHEELS | PARTS
                    or not pid.startswith((row["category"] if row["category"] in WHEELS else "parts") + ":")):
                raise PriceCacheError("invalid_product_identity")
            selected[pid] = {key: row[key] for key in ("id", "identity_hash", "category")}
        self._products, self._provider, self._mode = selected, provider, mode
        self._source, self._require_procurement = source, require_procurement
        self._enabled, self._permission = enabled, reads_approved
        self._budget, self._period, self._spacing = max_provider_reads_per_period, period_seconds, spacing_seconds
        self._backoff_seconds, self._queue_limit, self._workers_max = failure_backoff_seconds, max_queue, max_workers
        self._entry_limit = max_cache_entries
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic
        self._condition = threading.Condition(threading.RLock())
        self._queue, self._pending, self._attempts = deque(), set(), deque()
        self._queued_at = {}
        self._inflight = set()
        self._entries, self._backoffs, self._threads = OrderedDict(), OrderedDict(), []
        self._running, self._clock_bad = False, False
        self._last_wall, self._last_mono, self._next_read = None, None, 0.0

    def __repr__(self):
        return "<PublicPriceCache bounded_RSU_browse_reads cache_only_get>"

    def _times(self):
        try:
            now, mono = _stamp(self._clock()), self._monotonic()
        except Exception:
            self._clock_bad = True
            raise PriceCacheError("invalid_clock") from None
        if (type(mono) not in (int, float) or not math.isfinite(mono)
                or self._last_wall is not None and now < self._last_wall
                or self._last_mono is not None and mono < self._last_mono):
            self._clock_bad = True
            raise PriceCacheError("clock_moved_backwards")
        self._last_wall, self._last_mono = now, mono
        return now, mono

    def start(self, *, background=True):
        """Explicit lifecycle. background=False allows a controlled scheduler/test."""
        if type(background) is not bool:
            raise PriceCacheError("invalid_lifecycle")
        with self._condition:
            if self._running:
                return True
            if not self._enabled or not self._permission or self._budget <= 0 or self._clock_bad:
                return False
            self._threads = [thread for thread in self._threads if thread.is_alive()]
            if self._threads or self._inflight:
                return False
            self._times()
            self._running = True
            if background:
                for _ in range(self._workers_max):
                    thread = threading.Thread(target=self._worker, name="shop-rsu-price-cache", daemon=True)
                    self._threads.append(thread)
                    thread.start()
            return True

    def stop(self):
        with self._condition:
            self._running = False
            self._queue.clear()
            self._queued_at.clear()
            self._condition.notify_all()
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=1)
        # In-flight provider reads cannot be cancelled. Their pending key is
        # retained until completion, and no stopped worker can claim more work.
        with self._condition:
            self._threads = [thread for thread in self._threads if thread.is_alive()]
            self._pending.intersection_update(self._inflight)

    def _quote(self, product, quantity, value, now):
        if (not isinstance(value, Mapping) or value.get("confirmed") is not True
                or value.get('source', 'RSU' if self._source == 'RSU' else None) != self._source
                or self._require_procurement and value.get("procurement_confirmed") is not True
                or value.get("product_id") != product["id"]
                or value.get("identity_hash") != product["identity_hash"] or value.get("mode") != self._mode
                or value.get("currency") != "eur" or value.get("vat_percent") != "19.00"
                or type(value.get("quantity")) is not int or value["quantity"] != quantity
                or type(value.get("unit_gross_cent")) is not int or not 1 <= value["unit_gross_cent"] <= 10_000_000
                or type(value.get("stock")) is not int or not quantity <= value["stock"] <= 1_000_000):
            return None
        observed, expires = _stamp(value.get("observed_at")), _stamp(value.get("expires_at"))
        if not timedelta(0) < expires - observed <= TTL or not observed <= now < expires:
            return None
        result = {"quantity": quantity, "unit_gross_cent": value["unit_gross_cent"],
                "items_gross_cent": quantity * value["unit_gross_cent"], "observed_at": observed,
                "expires_at": expires, "stock": value["stock"]}
        if (value.get('base_price_unit') == 'L' and type(value.get('base_price_gross_cent')) is int
                and 1 <= value['base_price_gross_cent'] <= 10_000_000):
            result.update(base_price_unit='L', base_price_gross_cent=value['base_price_gross_cent'])
        return result

    def _fresh(self, key, now):
        value = self._entries.get(key)
        if value is None or not value["observed_at"] <= now < value["expires_at"]:
            return None
        return value

    def _requested_keys(self, product_ids, quantities):
        if (not isinstance(product_ids, (list, tuple)) or len(product_ids) > 24
                or any(not isinstance(pid, str) or not ID.fullmatch(pid) for pid in product_ids)
                or not isinstance(quantities, tuple) or not 1 <= len(quantities) <= 20
                or any(type(q) is not int or not 1 <= q <= 20 for q in quantities)
                or len(set(quantities)) != len(quantities)):
            raise PriceCacheError("invalid_page_request")
        keys = []
        for pid in dict.fromkeys(product_ids):
            product = self._products.get(pid)
            if product is None:
                continue
            maximum = 4 if product["category"] in WHEELS else 20
            if any(quantity > maximum for quantity in quantities):
                raise PriceCacheError("invalid_page_request")
            keys.extend((pid, quantity) for quantity in quantities)
        return keys

    def _prune(self, mono):
        while self._attempts and self._attempts[0] <= mono - self._period:
            self._attempts.popleft()
        # Abandoned pages must not spend a later period's budget. Active
        # requests renew their queued lease; in-flight reads are not cancelled.
        retained = deque()
        for key in self._queue:
            if self._queued_at.get(key, -math.inf) + QUEUE_LEASE_SECONDS <= mono:
                self._queued_at.pop(key, None)
                self._pending.discard(key)
            else:
                retained.append(key)
        self._queue = retained

    def _enqueue(self, keys, now, mono):
        self._prune(mono)
        added = 0
        remaining = self._budget - len(self._attempts)
        for key in keys:
            if key in self._queued_at:
                self._queued_at[key] = mono
                continue
            if (key in self._pending or self._fresh(key, now) is not None
                    or self._backoffs.get(key, 0) > mono or len(self._queue) >= self._queue_limit
                    or len(self._queue) >= remaining):
                continue
            self._pending.add(key)
            self._queue.append(key)
            self._queued_at[key] = mono
            added += 1
        self._condition.notify_all()
        return added

    def enqueue(self, product_ids, *, quantities=(1, 4)):
        keys = self._requested_keys(product_ids, quantities)
        with self._condition:
            if not self._running or self._clock_bad:
                return 0
            try:
                now, mono = self._times()
            except PriceCacheError:
                return 0
            return self._enqueue(keys, now, mono)

    def warmup(self, product_ids, *, quantities=(1, 4)):
        """True while requested missing prices have real queued/in-flight work.

        This is a polling hint, not a count of newly queued jobs. No budget is
        promised beyond the current rolling period; blocked/disabled/fresh work
        returns literal False. It performs no synchronous provider IO.
        """
        keys = self._requested_keys(product_ids, quantities)
        with self._condition:
            if not self._running or self._clock_bad:
                return False
            try:
                now, mono = self._times()
            except PriceCacheError:
                return False
            self._enqueue(keys, now, mono)
            remaining = self._budget - len(self._attempts)
            runnable = set(list(self._queue)[:max(0, remaining)])
            return any(self._fresh(key, now) is None and (key in self._inflight or key in runnable)
                       for key in keys)

    def run_once(self):
        """At most one bounded read; called by workers or an explicit scheduler."""
        with self._condition:
            if (not self._running or self._clock_bad or not self._queue
                    or len(self._inflight) >= self._workers_max):
                return False
            try:
                start, mono = self._times()
            except PriceCacheError:
                return False
            self._prune(mono)
            if not self._queue:
                return False
            if mono < self._next_read or len(self._attempts) >= self._budget:
                return False
            key = self._queue.popleft()
            self._queued_at.pop(key, None)
            self._inflight.add(key)
            self._attempts.append(mono)
            self._next_read = mono + self._spacing
            product, quantity = deepcopy(self._products[key[0]]), key[1]
        quote = None
        try:
            response = self._provider.retail_quote(product, quantity)
            with self._condition:
                now, _ = self._times()
                if now < start:
                    raise PriceCacheError("clock_moved_backwards")
                quote = self._quote(product, quantity, response, now)
        except Exception:
            # Never retain provider errors or raw response/net/binding fields.
            quote = None
        with self._condition:
            self._inflight.discard(key)
            self._pending.discard(key)
            if quote is not None and not self._clock_bad:
                self._entries[key] = quote
                self._entries.move_to_end(key)
                self._backoffs.pop(key, None)
                while len(self._entries) > self._entry_limit:
                    self._entries.popitem(last=False)
            else:
                # A failed newer observation must not leave a prior price fresh.
                self._entries.pop(key, None)
                try:
                    _, finished_mono = self._times()
                except PriceCacheError:
                    finished_mono = mono
                self._backoffs[key] = finished_mono + self._backoff_seconds
                self._backoffs.move_to_end(key)
                while len(self._backoffs) > self._entry_limit * 2:
                    self._backoffs.popitem(last=False)
            self._condition.notify_all()
        return True

    def _worker(self):
        while True:
            with self._condition:
                if not self._running or self._clock_bad:
                    return
            if not self.run_once():
                with self._condition:
                    if not self._running or self._clock_bad:
                        return
                    self._condition.wait(timeout=1)

    def get(self, product_id):
        """Detached public gross projection only; performs no provider IO."""
        if not isinstance(product_id, str) or not ID.fullmatch(product_id) or product_id not in self._products:
            return None
        with self._condition:
            if self._clock_bad:
                return None
            try:
                now, _ = self._times()
            except PriceCacheError:
                return None
            maximum = 4 if self._products[product_id]["category"] in WHEELS else 20
            values = [value for quantity in range(1, maximum + 1)
                      if (value := self._fresh((product_id, quantity), now))]
            if not values:
                return None
            stock = min(v["stock"] for v in values)
            values = [v for v in values if v["quantity"] <= stock]
            if not values:
                return None
            result = {"source": self._source, "currency": "EUR", "vat_percent": "19.00",
                    "observed_at": min(v["observed_at"] for v in values).isoformat(),
                    "expires_at": min(v["expires_at"] for v in values).isoformat(), "stock_reported": stock,
                    "prices_by_quantity": {str(v["quantity"]): {"quantity": v["quantity"],
                        "unit_gross_cent": v["unit_gross_cent"], "items_gross_cent": v["items_gross_cent"],
                        "vat_percent": "19.00"}
                        for v in values}}
            base = {(v.get('base_price_unit'), v.get('base_price_gross_cent')) for v in values}
            if len(base) == 1 and next(iter(base))[0] == 'L':
                result.update(base_price_unit='L', base_price_gross_cent=next(iter(base))[1])
            return result

    def status(self):
        with self._condition:
            return {"running": self._running and not self._clock_bad, "queued": len(self._queue),
                    "inflight": len(getattr(self, "_inflight", ())), "entries": len(self._entries),
                    "reserved_reads_in_period": len(self._attempts), "read_limit": self._budget}
