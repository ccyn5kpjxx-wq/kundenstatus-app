"""Server-side, bounded ALZURA product quote refresh for the loopback preview.

The browser supplies local selection IDs only. Provider requests, credentials,
native IDs and response bodies remain private; this module has no order route.
"""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import threading
import time
from uuid import uuid4

from . import alzura_readonly_catalog as api
from .alzura_live_products import ALLOWED, BODY_NAME, _conflicts, _quote, _rows, stamp
from .public_quotes import live_public_quote

WORK = Path(__file__).resolve().parent
LIVE = WORK / 'alzura-live-private'
RAW = WORK / 'alzura-catalog-private'
SELECTION = re.compile(r'(?:tyre|alloy_wheel|steel_wheel):[a-f0-9]{24}')
RUN_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,150}')
LOCK = threading.RLock()
COOLDOWN = timedelta(seconds=30)


def _read_json(path, maximum):
    path = api._private_path(path)
    if path.stat().st_size > maximum:
        raise ValueError('private_input_too_large')
    return api._json(path.read_bytes())


def _safe_error(code):
    # Only fixed local/transport/normalizer codes, never exception messages.
    allowed = {'authentication_or_scope_rejected', 'provider_rate_limited', 'provider_http_error',
        'redirect_blocked', 'response_not_json', 'provider_rejected', 'response_too_large',
        'response_byte_budget_exhausted', 'name_resolution_failed', 'connection_timeout',
        'response_read_timeout', 'transport_timeout', 'tls_certificate_rejected', 'tls_error',
        'connection_refused', 'connection_reset', 'connection_failed', 'transport_request_failed',
        'local_monthly_budget_exhausted', 'run_call_budget_exhausted', 'local_rate_limit',
        'local_rate_spacing', 'budget_ledger_unavailable', 'budget_ledger_invalid',
        'budget_ledger_clock_conflict', 'private_storage_error', 'area_token_missing'}
    return code if code in allowed else 'provider_read_failed'


class QuoteService:
    def __init__(self, snapshot_path=LIVE/'catalog-live.json', token_file=LIVE/'product-api-auth.json',
                 ledger_path=RAW/'budget-ledger.json', cache_path=LIVE/'local-quote-cache.json',
                 *, reader_factory=None, clock=None, sleeper=None, token_loader=None,
                 raw_root=None, output_root=None):
        self.snapshot_path = api._private_path(snapshot_path)
        self.token_file = api._private_path(token_file)
        self.ledger_path = api._private_path(ledger_path)
        self.cache_path = api._private_path(cache_path)
        self.raw_root = api._private_path(raw_root or self.snapshot_path.parent.parent/'alzura-catalog-private')
        self.output_root = api._private_path(output_root or self.cache_path.parent/'quote-read-runs')
        self.reader_factory = reader_factory or api.ReadOnlyCatalogClient
        self.token_loader = token_loader or api.load_tokens
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sleeper = sleeper or time.sleep
        self._snapshot_signature = None
        self._mapping, self._groups, self._entries, self._cooldowns = {}, {}, {}, {}
        self._trusted_request_cache = {}
        self._cache_loaded = False

    def __repr__(self):
        return '<LocalQuoteService credentials=private product_GETs_only>'

    def _now(self):
        return stamp(self.clock())

    def _fresh(self, value):
        quote = live_public_quote(value)
        if not quote or not quote['prices_by_quantity']:
            return None
        now = self._now()
        return quote if stamp(quote['observed_at']) <= now < stamp(quote['expires_at']) else None

    def _trusted_request(self, evidence, area):
        sources = evidence.get('sources', [])
        if not isinstance(sources, list):
            return None
        for source in sorted((s for s in sources if isinstance(s, dict)),
                             key=lambda s: str(s.get('observed_at', '')), reverse=True):
            run, body = source.get('run'), source.get('body_file')
            if not isinstance(run, str) or not RUN_NAME.fullmatch(run) or not isinstance(body, str) or not BODY_NAME.fullmatch(body):
                continue
            key = (run, body, area)
            if key not in self._trusted_request_cache:
                request = None
                directory = (self.raw_root/run).resolve()
                if directory.is_relative_to(self.raw_root) and not directory.is_symlink():
                    try:
                        plan = api.validate_plan(_read_json(directory/'plan.json', 256*1024))
                        index = int(body.split('-', 1)[0])-1
                        candidate = plan[index] if 0 <= index < len(plan) else None
                        if candidate and (candidate['area'], candidate['operation']) in ALLOWED and candidate['area'] == area:
                            request = {'area': area, 'operation': candidate['operation'], 'params': candidate.get('params', {})}
                    except Exception:
                        pass
                self._trusted_request_cache[key] = request
            request = self._trusted_request_cache[key]
            if request and source.get('area') == area and source.get('operation') == request['operation']:
                return request
        return None

    def _load_mapping(self):
        info = self.snapshot_path.stat()
        signature = (info.st_mtime_ns, info.st_size)
        if signature == self._snapshot_signature:
            return
        snapshot = _read_json(self.snapshot_path, 96*1024*1024)
        if not isinstance(snapshot, dict) or snapshot.get('mode') != 'licensed_live_local_preview':
            raise ValueError('snapshot_invalid')
        catalogues, evidence = snapshot.get('catalogues'), snapshot.get('private_evidence')
        if not isinstance(catalogues, dict) or not isinstance(evidence, dict):
            raise ValueError('snapshot_invalid')
        mapping, groups = {}, {}
        self._trusted_request_cache = {}
        for values in catalogues.values():
            if not isinstance(values, list):
                raise ValueError('snapshot_invalid')
            for product in values:
                if not isinstance(product, dict):
                    continue
                sid = product.get('selection_id')
                if not isinstance(sid, str) or not SELECTION.fullmatch(sid) or not sid.startswith(str(product.get('category'))+':'):
                    continue
                private = evidence.get(sid)
                if not isinstance(private, dict) or not isinstance(private.get('variant'), dict):
                    continue
                area = 'alloys' if product.get('category') == 'alloy_wheel' else 'tyres'
                native = private.get('provider_id')
                if not isinstance(native, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,150}', native):
                    continue
                detail = None
                if area == 'tyres':
                    detail = {'area': area, 'operation': 'details', 'params': {'idSolr': native, 'postcode': '74821'}}
                elif native.isdigit() and 0 < int(native) < 10**18:
                    cars = product.get('vehicle_candidates', [])
                    car = next((v.get('car_id') for v in cars if isinstance(v, dict)
                                and type(v.get('car_id')) is int and 0 < v['car_id'] < 10**18), None) if isinstance(cars, list) else None
                    if car:
                        detail = {'area': area, 'operation': 'details', 'params': {'alloyRimFrontID': int(native), 'carID': car}}
                request = self._trusted_request(private, area) or detail
                if request is None:
                    continue
                api.build_url(request['area'], request['operation'], request['params'])
                group = sha256(json.dumps(request, sort_keys=True, default=str).encode()).hexdigest()
                fingerprint = sha256(json.dumps({'area': area, 'provider_id': native,
                    'variant': private['variant']}, sort_keys=True, default=str).encode()).hexdigest()
                if sid in mapping:
                    raise ValueError('snapshot_duplicate_identity')
                mapping[sid] = {'area': area, 'provider_id': native, 'variant': private['variant'],
                    'conflict': bool(private.get('conflicts')), 'valid_dimensions': product.get('dimensions_valid') is True,
                    'baseline_quote': live_public_quote(product.get('quote_public')), 'fingerprint': fingerprint,
                    'request': request, 'group': group, 'detail': detail}
                groups.setdefault(group, []).append(sid)
        self._mapping, self._groups, self._snapshot_signature = mapping, groups, signature

    def _load_cache(self):
        if self._cache_loaded:
            return
        self._cache_loaded = True
        try:
            value = _read_json(self.cache_path, 12*1024*1024)
            if not isinstance(value, dict) or value.get('schema_version') != 1:
                return
            entries = value.get('entries', {})
            if isinstance(entries, dict):
                for sid, item in entries.items():
                    if isinstance(sid, str) and SELECTION.fullmatch(sid) and isinstance(item, dict) and re.fullmatch(r'[a-f0-9]{64}', str(item.get('fingerprint', ''))):
                        quote = live_public_quote(item.get('quote'))
                        if quote:
                            self._entries[sid] = {'fingerprint': item['fingerprint'], 'quote': quote}
                        elif item.get('quote') is None:
                            observed = stamp(item.get('observed_at'))
                            if observed <= self._now():
                                self._entries[sid] = {'fingerprint': item['fingerprint'], 'quote': None,
                                    'observed_at': observed.isoformat(), 'error': _safe_error(item.get('error'))}
            cooldowns = value.get('cooldowns', {})
            if isinstance(cooldowns, dict):
                for sid, item in cooldowns.items():
                    if sid in self._mapping and isinstance(item, dict) and item.get('fingerprint') == self._mapping[sid]['fingerprint']:
                        until = stamp(item.get('until'))
                        if self._now() < until <= self._now()+COOLDOWN:
                            self._cooldowns[sid] = {'fingerprint': item['fingerprint'], 'until': until.isoformat(),
                                                    'error': _safe_error(item.get('error'))}
        except Exception:
            return

    def _save_cache(self):
        try:
            api._atomic_json(self.cache_path, {'schema_version': 1, 'entries': self._entries, 'cooldowns': self._cooldowns})
        except Exception:
            pass  # A valid observed quote remains valid in memory; never redate it.

    def _cached(self, sid):
        item, mapping = self._entries.get(sid), self._mapping[sid]
        baseline = self._fresh(mapping.get('baseline_quote'))
        if item and item.get('fingerprint') == mapping['fingerprint']:
            observed = stamp(item['quote']['observed_at'] if item.get('quote') else item['observed_at'])
            if baseline and stamp(baseline['observed_at']) > observed:
                return baseline
            # A real newer negative observation invalidates an older snapshot
            # quote, even if that older amount has not reached its TTL yet.
            if item.get('quote') is None:
                return None
            fresh = self._fresh(item.get('quote'))
            if fresh:
                return fresh
        return baseline

    def _wait_for_ledger(self):
        # The client itself owns reservation/validation. This bounded wait only
        # avoids ordinary spacing conflicts with prior batches/processes.
        if not self.ledger_path.exists():
            return
        value = _read_json(self.ledger_path, 8*1024*1024)
        attempts = value.get('attempts') if isinstance(value, dict) else None
        if not isinstance(attempts, list):
            raise api.ReadError('budget_ledger_invalid')
        stamps = [stamp(v['at']) for v in attempts if isinstance(v, dict) and 'at' in v]
        now = self._now()
        if stamps and max(stamps) > now+timedelta(seconds=1):
            raise api.ReadError('budget_ledger_clock_conflict')
        if stamps:
            delay = api.MIN_INTERVAL-(now-max(stamps)).total_seconds()
            if delay > 0:
                self.sleeper(delay+0.02)

    def _quote_row(self, sid, row, observed):
        mapping = self._mapping[sid]
        area, native = mapping['area'], mapping['provider_id']
        actual = row.get('idSolr') if area == 'tyres' else row.get('alloyRimFrontID')
        if isinstance(actual, bool) or str(actual) != native:
            return None, 'provider_identity_mismatch'
        records = [{'area': area, 'row': mapping['variant']}, {'area': area, 'row': row}]
        if mapping['conflict'] or _conflicts(records):
            return None, 'provider_variant_mismatch'
        quote, reason = _quote({'area': area, 'row': row, 'source': {'observed_at': observed}},
            self._now(), dimensions_valid=mapping['valid_dimensions'], conflict=False)
        quote = live_public_quote(quote)
        return quote, None if quote else reason

    def get_quotes(self, ids):
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8 or any(not isinstance(sid, str) or not SELECTION.fullmatch(sid) for sid in ids):
            return {'quotes': {}, 'errors': {'_request': 'invalid_selection_ids'}}
        requested = list(dict.fromkeys(ids))
        with LOCK:
            result = {'quotes': {sid: None for sid in requested}, 'errors': {}}
            try:
                self._load_mapping(); self._load_cache()
            except Exception:
                result['errors'] = {sid: 'local_catalogue_unavailable' for sid in requested}
                return result
            pending = []
            for sid in requested:
                mapping = self._mapping.get(sid)
                if mapping is None:
                    result['errors'][sid] = 'unknown_selection_id'; continue
                if mapping['conflict'] or not mapping['valid_dimensions']:
                    result['errors'][sid] = 'product_identity_unconfirmed'; continue
                quote = self._cached(sid)
                if quote:
                    result['quotes'][sid] = quote; continue
                cooldown = self._cooldowns.get(sid)
                if cooldown and cooldown.get('fingerprint') == mapping['fingerprint'] and self._now() < stamp(cooldown['until']):
                    result['errors'][sid] = cooldown['error']; continue
                pending.append(sid)
            if not pending:
                return result
            try:
                tokens = self.token_loader(self.token_file)
                ledger = api.BudgetLedger(self.ledger_path, monthly_limit=9900, run_limit=8)
                client = self.reader_factory(tokens, output_dir=self.output_root/uuid4().hex, ledger=ledger,
                    authorize=lambda area, operation, params: (area, operation) in ALLOWED,
                    clock=self.clock, sleeper=self.sleeper, run_id=uuid4().hex)
            except Exception:
                for sid in pending: result['errors'][sid] = 'local_quote_access_unavailable'
                return result
            calls, stopped = 0, None
            done_groups, missing = set(), []

            def read(request, members):
                nonlocal calls, stopped
                if calls >= 8:
                    return {}, 'refresh_batch_limit'
                try:
                    self._wait_for_ledger()
                    response = client.read(request['area'], request['operation'], request['params']); calls += 1
                    if response.summary.get('ok') is not True:
                        stopped = _safe_error(response.summary.get('error'))
                        return {}, stopped
                    observed = stamp(response.summary.get('requested_at')).isoformat()
                    rows = _rows(response.data, request['area'], request['operation'])
                    indexed, duplicates = {}, set()
                    for row in rows:
                        native = row.get('idSolr') if request['area'] == 'tyres' else row.get('alloyRimFrontID')
                        if isinstance(native, bool) or not isinstance(native, (str, int)):
                            continue
                        key = str(native)
                        if key in indexed: duplicates.add(key)
                        indexed[key] = row
                    found = {}
                    for sid in members:
                        m = self._mapping[sid]
                        if m['provider_id'] in duplicates:
                            found[sid] = (None, 'provider_identity_ambiguous'); continue
                        row = indexed.get(m['provider_id'])
                        if row is None:
                            continue
                        quote, error = self._quote_row(sid, row, observed)
                        found[sid] = (quote, error)
                        if quote:
                            self._entries[sid] = {'fingerprint': m['fingerprint'], 'quote': quote}
                            self._cooldowns.pop(sid, None)
                        else:
                            self._entries[sid] = {'fingerprint': m['fingerprint'], 'quote': None,
                                'observed_at': observed, 'error': error or 'quote_unavailable'}
                            self._cooldowns[sid] = {'fingerprint': m['fingerprint'],
                                'until': (self._now()+COOLDOWN).isoformat(), 'error': error or 'quote_unavailable'}
                    return found, None
                except api.ReadError as error:
                    stopped = _safe_error(error.code); return {}, stopped
                except Exception:
                    stopped = 'provider_response_unusable'; return {}, stopped

            for sid in pending:
                group = self._mapping[sid]['group']
                if group in done_groups:
                    continue
                done_groups.add(group)
                members = self._groups[group]
                targets = [key for key in pending if self._mapping[key]['group'] == group]
                if stopped:
                    for key in targets: result['errors'][key] = stopped
                    continue
                found, error = read(self._mapping[sid]['request'], members)
                for key in targets:
                    if error: result['errors'][key] = error
                    elif key in found:
                        result['quotes'][key], failure = found[key]
                        if failure: result['errors'][key] = failure
                    else: missing.append(key)
            # Only a successful source-group omission allows an exact-ID detail
            # fallback. Never retry a rejected/timed-out provider request.
            for sid in missing:
                detail = self._mapping[sid]['detail']
                if stopped: result['errors'][sid] = stopped; continue
                if detail is None: result['errors'][sid] = 'price_context_unavailable'; continue
                if detail == self._mapping[sid]['request']:
                    result['errors'][sid] = 'provider_identity_mismatch'; continue
                found, error = read(detail, [sid])
                if error: result['errors'][sid] = error
                elif sid in found:
                    result['quotes'][sid], failure = found[sid]
                    if failure: result['errors'][sid] = failure
                else: result['errors'][sid] = 'provider_identity_mismatch'
            for sid in pending:
                if result['quotes'][sid] is None:
                    code = result['errors'].setdefault(sid, 'quote_unavailable')
                    self._cooldowns[sid] = {'fingerprint': self._mapping[sid]['fingerprint'],
                        'until': (self._now()+COOLDOWN).isoformat(), 'error': code}
            self._save_cache()
            return result
