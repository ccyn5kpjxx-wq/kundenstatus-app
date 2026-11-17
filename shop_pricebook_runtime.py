"""Bounded background refresh of the merchant's persistent five-day prices.

Catalogue reads never contact a supplier. Each refresh reads one exact RSU
article at quantity one; the merchant then publishes one unit price for all
permitted quantities. Checkout still checks the actual requested quantity.
"""
from collections import deque
from collections.abc import Mapping
from copy import deepcopy
import math
import threading
import time

from shop_price_cache import ID, PriceCacheError, WHEELS


class PriceBookRuntime:
    def __init__(self, book, products, *, approved_ids, provider,
                 max_reads_per_hour=300, spacing_seconds=2.1,
                 failure_backoff_seconds=3600, monotonic=None):
        if (not isinstance(products, Mapping) or not isinstance(approved_ids, (set, frozenset))
                or not approved_ids <= products.keys() or len(approved_ids) > 100000
                or any(not isinstance(pid, str) or not ID.fullmatch(pid) for pid in approved_ids)
                or not callable(getattr(provider, 'retail_quote', None))
                or any(not callable(getattr(book, name, None)) for name in ('get', 'due_ids', 'publish_uniform'))
                or type(max_reads_per_hour) is not int or not 1 <= max_reads_per_hour <= 300
                or type(spacing_seconds) not in (int, float) or not 2 <= spacing_seconds <= 3600
                or type(failure_backoff_seconds) not in (int, float) or not 60 <= failure_backoff_seconds <= 86400):
            raise PriceCacheError('invalid_pricebook_runtime')
        self.book, self._products = book, {pid: deepcopy(products[pid]) for pid in approved_ids}
        self._provider, self._budget, self._spacing = provider, max_reads_per_hour, spacing_seconds
        self._backoff_seconds = failure_backoff_seconds
        self._monotonic = monotonic or time.monotonic
        self._condition = threading.Condition(threading.RLock())
        self._attempts, self._backoffs = deque(), {}
        self._next_read, self._last_mono, self._cursor = 0., None, ''
        self._running, self._clock_bad, self._inflight = False, False, None
        self._thread = None

    def _time(self):
        value = self._monotonic()
        if (type(value) not in (int, float) or not math.isfinite(value)
                or self._last_mono is not None and value < self._last_mono):
            self._clock_bad = True
            raise PriceCacheError('invalid_clock')
        self._last_mono = value
        while self._attempts and self._attempts[0] <= value - 3600:
            self._attempts.popleft()
        return value

    def get(self, product_id):
        return self.book.get(product_id) if isinstance(product_id, str) and product_id in self._products else None

    def warmup(self, product_ids, *, quantities=(1, 4)):
        if (not isinstance(product_ids, (list, tuple)) or len(product_ids) > 24
                or any(not isinstance(pid, str) or not ID.fullmatch(pid) for pid in product_ids)
                or not isinstance(quantities, tuple) or not 1 <= len(quantities) <= 20
                or len(set(quantities)) != len(quantities)
                or any(type(q) is not int or not 1 <= q <= 20 for q in quantities)):
            raise PriceCacheError('invalid_page_request')
        for pid in product_ids:
            product = self._products.get(pid)
            if product and any(q > (4 if product['category'] in WHEELS else 20) for q in quantities):
                raise PriceCacheError('invalid_page_request')
        with self._condition:
            if not self._running or self._clock_bad:
                return False
            now = self._time()
            if len(self._attempts) >= self._budget:
                return False
            missing = set(self.book.due_ids()) & set(product_ids) & self._products.keys()
            ready = any(self._backoffs.get(pid, 0) <= now for pid in missing)
            if ready:
                self._condition.notify_all()
            return ready

    def start(self, *, background=True):
        if type(background) is not bool:
            raise PriceCacheError('invalid_lifecycle')
        with self._condition:
            if self._running:
                return True
            if self._clock_bad or self._inflight or self._thread is not None and self._thread.is_alive():
                return False
            self._time()
            self._running = True
            if background:
                self._thread = threading.Thread(target=self._worker, name='shop-five-day-prices', daemon=True)
                self._thread.start()
            return True

    def stop(self):
        with self._condition:
            self._running = False
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=1)

    def run_once(self):
        with self._condition:
            if not self._running or self._clock_bad or self._inflight is not None:
                return False
            now = self._time()
            if now < self._next_read or len(self._attempts) >= self._budget:
                return False
            due = sorted(set(self.book.due_ids()) & self._products.keys())
            ready = [pid for pid in due if self._backoffs.get(pid, 0) <= now]
            if not ready:
                return False
            identifier = next((pid for pid in ready if pid > self._cursor), ready[0])
            self._cursor, self._inflight = identifier, identifier
            self._attempts.append(now)
            self._next_read = now + self._spacing
            product = deepcopy(self._products[identifier])
        success = False
        try:
            response = self._provider.retail_quote(product, 1)
            success = self.book.publish_uniform(product, response) is True
        except Exception:
            pass  # Never expose provider bodies, credentials or exceptions.
        finally:
            with self._condition:
                self._inflight = None
                finished = self._time()
                if success:
                    self._backoffs.pop(identifier, None)
                else:
                    self._backoffs[identifier] = finished + self._backoff_seconds
                self._condition.notify_all()
        return True

    def _worker(self):
        while True:
            with self._condition:
                if not self._running or self._clock_bad:
                    return
            try:
                worked = self.run_once()
            except Exception:
                worked = False
            if not worked:
                with self._condition:
                    if not self._running or self._clock_bad:
                        return
                    self._condition.wait(timeout=5)

    def status(self):
        with self._condition:
            self._time()
            return {'running': self._running and not self._clock_bad, 'queued': 0,
                    'inflight': int(self._inflight is not None), 'entries': len(self._products),
                    'reserved_reads_in_period': len(self._attempts), 'read_limit': self._budget,
                    'price_refresh_days': 5}
