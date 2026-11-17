"""Cache-only price routing: exact RSU bindings, all other rows market display.

Source membership is explicit and immutable for a release. A missing, disabled
or failed RSU cache never falls back to a market quote for a bound article.
No credentials, provider calls, threads or filesystem reads on construction.
"""
from collections.abc import Mapping
from copy import deepcopy
import threading

from shop_price_cache import ID, HASH, WHEELS, PARTS, PriceCacheError, PublicPriceCache


class HybridPriceCache:
    def __init__(self, products, *, released_ids, procurement_ids, rsu_cache=None, market_cache=None):
        if (not isinstance(products, Mapping) or not isinstance(released_ids, (set, frozenset))
                or not isinstance(procurement_ids, (set, frozenset)) or len(released_ids) > 100000
                or not procurement_ids <= released_ids or not released_ids <= products.keys()
                or any(not isinstance(pid, str) or not ID.fullmatch(pid) for pid in released_ids)):
            raise PriceCacheError('invalid_hybrid_configuration')
        self._products = {}
        for pid in released_ids:
            row = products[pid]
            if (not isinstance(row, Mapping) or row.get('id') != pid
                    or row.get('category') not in WHEELS | PARTS
                    or not isinstance(row.get('identity_hash'), str) or not HASH.fullmatch(row['identity_hash'])
                    or not pid.startswith((row['category'] if row['category'] in WHEELS else 'parts') + ':')):
                raise PriceCacheError('invalid_hybrid_configuration')
            self._products[pid] = {key: row[key] for key in ('id', 'identity_hash', 'category')}
        self._procurement = frozenset(procurement_ids)
        self._market = frozenset(released_ids - procurement_ids)
        self._caches = {'RSU': rsu_cache, 'ALZURA': market_cache}
        for source, cache in self._caches.items():
            allowed = self._procurement if source == 'RSU' else self._market
            if cache is not None and (not isinstance(cache, PublicPriceCache)
                    or cache._source != source or cache._require_procurement is not (source == 'RSU')
                    or not cache._products.keys() <= allowed):
                raise PriceCacheError('invalid_hybrid_configuration')
        self._cursor, self._lock = 0, threading.Lock()

    def __repr__(self):
        return '<HybridPriceCache exact_RSU_or_market cache_only_get>'

    def _partition(self, product_ids, quantities):
        if (not isinstance(product_ids, (list, tuple)) or len(product_ids) > 24
                or any(not isinstance(pid, str) or not ID.fullmatch(pid) for pid in product_ids)
                or not isinstance(quantities, tuple) or not 1 <= len(quantities) <= 20
                or any(type(q) is not int or not 1 <= q <= 20 for q in quantities)
                or len(set(quantities)) != len(quantities)):
            raise PriceCacheError('invalid_page_request')
        partition = {'RSU': [], 'ALZURA': []}
        for pid in dict.fromkeys(product_ids):
            product = self._products.get(pid)
            if product is None:
                continue
            if any(q > (4 if product['category'] in WHEELS else 20) for q in quantities):
                raise PriceCacheError('invalid_page_request')
            partition['RSU' if pid in self._procurement else 'ALZURA'].append(pid)
        return partition

    def get(self, product_id):
        if not isinstance(product_id, str) or product_id not in self._products:
            return None
        source = 'RSU' if product_id in self._procurement else 'ALZURA'
        cache = self._caches[source]
        if cache is None:
            return None
        try:
            value = cache.get(product_id)
            return deepcopy(value) if isinstance(value, Mapping) and value.get('source') == source else None
        except Exception:
            return None

    def warmup(self, product_ids, *, quantities=(1, 4)):
        partition = self._partition(product_ids, quantities)
        pending = False
        for source, ids in partition.items():
            cache = self._caches[source]
            if ids and cache is not None:
                # Do not short-circuit: both independent queues need warming.
                pending = cache.warmup(ids, quantities=quantities) is True or pending
        return pending

    def start(self, *, background=True):
        if type(background) is not bool:
            raise PriceCacheError('invalid_lifecycle')
        started = []
        try:
            for cache in self._caches.values():
                if cache is not None and cache.start(background=background) is True:
                    started.append(cache)
        except Exception:
            for cache in started:
                cache.stop()
            raise PriceCacheError('hybrid_cache_unavailable') from None
        return bool(started)

    def stop(self):
        for cache in self._caches.values():
            if cache is not None:
                cache.stop()

    def run_once(self):
        """At most one child-cache read, with round-robin source fairness."""
        sources = tuple(self._caches)
        with self._lock:
            start = self._cursor
            self._cursor = (self._cursor + 1) % len(sources)
        for offset in range(len(sources)):
            cache = self._caches[sources[(start + offset) % len(sources)]]
            if cache is not None and cache.run_once() is True:
                return True
        return False

    def status(self):
        counts = ('queued', 'inflight', 'entries', 'reserved_reads_in_period', 'read_limit')
        result = {'running': False, **dict.fromkeys(counts, 0), 'sources': {}}
        for source, cache in self._caches.items():
            raw = cache.status() if cache is not None else {}
            state = {'running': raw.get('running') is True,
                     **{key: raw[key] if type(raw.get(key)) is int and raw[key] >= 0 else 0 for key in counts}}
            result['sources'][source] = state
            result['running'] = result['running'] or state['running']
            for key in counts:
                result[key] += state[key]
        return result
