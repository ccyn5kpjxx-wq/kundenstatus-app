"""Short-lived, exact-q1 parts freight observations composed with known tariffs.

No environment, file, database, provider-client or network access occurs here.
The caller supplies original-verified pinned source references and an injected
fresh read. A current REST read never renews the original UI freight evidence.
Unknown goods, quantity/offer ambiguity and unproved second legs fail closed.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re

from shop_cart_quotes import retail_unit_gross_cent
from shop_rsu_parts_evidence import validate_parts_mapping

RULE = 'rsu_ui_unique_q1_freight_v1'
HASH = re.compile(r'[a-f0-9]{64}')
ID = re.compile(r'(?:parts|tyre|alloy_wheel|steel_wheel):[a-f0-9]{24}')
MAX_CENTS = 1_000_000
TTL = timedelta(minutes=15)
PROOF_FIELDS = {'rule', 'source_ref', 'inquiry_ref', 'observed_at', 'quantity', 'offer_count',
    'supplier_article_id', 'ean', 'manufacturer', 'mpn', 'unit_net', 'stock_reported', 'freight_net',
    'currency', 'mainland_only', 'product_id', 'identity_hash', 'binding_evidence_id',
    'inquiry_observed_at', 'expires_at', 'retail_gross_cent', 'proof_sha256'}


class PartsFreightError(ValueError):
    pass


def _require(value):
    if not value:
        raise PartsFreightError('invalid_parts_freight_evidence')


def _hash(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _stamp(value):
    if isinstance(value, str) and len(value) <= 40:
        value = datetime.fromisoformat(value)
    _require(isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None)
    return value.astimezone(timezone.utc)


def _money(value, *, zero=False):
    _require(isinstance(value, str) and re.fullmatch(r'(?:0|[1-9][0-9]{0,6})(?:\.[0-9]{1,4})?', value))
    result = Decimal(value)
    _require((result >= 0 if zero else result > 0) and result <= Decimal('1000000'))
    return result


def _reference(value):
    _require(isinstance(value, Mapping) and set(value) == {'file', 'sha256'}
             and isinstance(value['file'], str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,160}', value['file'])
             and value['file'] not in {'.', '..'} and isinstance(value['sha256'], str) and HASH.fullmatch(value['sha256']))
    return value['file'], value['sha256']


def _binding(identifier, record):
    _require(isinstance(identifier, str) and identifier.startswith('parts:') and ID.fullmatch(identifier)
             and isinstance(record, Mapping) and set(record) == {'identity_hash', 'category', 'mapping', 'expected_ean'})
    supplier = validate_parts_mapping(record['mapping'], product_id=identifier, category=record['category'],
        expected_ean=record['expected_ean'], identity_hash=record['identity_hash'])
    safe = {'product_id': identifier, 'identity_hash': record['identity_hash'], 'category': record['category'], **supplier}
    return {**safe, 'evidence_id': _hash(safe)}


class PartsRetailFreight:
    def __init__(self, proofs, *, bindings, mode, destination_eligibility,
                 fresh_inquiry, source_refs=(), fallback=None, clock=None, allow_retained_inquiry=False):
        try:
            _require(isinstance(proofs, Mapping) and len(proofs) <= 10000 and isinstance(bindings, Mapping)
                     and mode in {'test', 'live'} and callable(destination_eligibility)
                     and (not proofs or callable(fresh_inquiry)) and (fallback is None or callable(fallback))
                     and (clock is None or callable(clock)) and isinstance(source_refs, (list, tuple))
                     and type(allow_retained_inquiry) is bool)
            references = {_reference(value) for value in source_refs}
            _require(len(references) == len(source_refs)
                     and len({file for file, digest in references}) == len(references))
            self._bindings = {key: _binding(key, value) for key, value in bindings.items()
                              if isinstance(key, str) and key.startswith('parts:')}
            self._proofs = {}
            for identifier, raw in proofs.items():
                binding = self._bindings.get(identifier)
                _require(binding is not None and isinstance(raw, Mapping) and set(raw) == PROOF_FIELDS
                         and raw['rule'] == RULE and raw['product_id'] == identifier
                         and raw['identity_hash'] == binding['identity_hash']
                         and raw['binding_evidence_id'] == binding['mapping_evidence_id']
                         and raw['supplier_article_id'] == binding['supplier_article_id']
                         and raw['ean'] == binding['expected_ean']
                         and type(raw['quantity']) is int and raw['quantity'] == 1
                         and type(raw['offer_count']) is int and raw['offer_count'] == 1
                         and type(raw['stock_reported']) is int and 1 <= raw['stock_reported'] <= 1_000_000_000
                         and raw['currency'] == 'EUR' and raw['mainland_only'] is True
                         and isinstance(raw['proof_sha256'], str) and HASH.fullmatch(raw['proof_sha256'])
                         and _hash({key: value for key, value in raw.items() if key != 'proof_sha256'}) == raw['proof_sha256'])
                source, inquiry = _reference(raw['source_ref']), _reference(raw['inquiry_ref'])
                _require(source in references and inquiry in references and source[0] != inquiry[0] and source[1] != inquiry[1])
                identity = bindings[identifier]['mapping']['evidence']['rsu']['identity']
                _require(raw['manufacturer'] == identity['manufacturer'] and raw['mpn'] == identity['mpn'])
                _money(raw['unit_net']); freight = _money(raw['freight_net'], zero=True)
                gross = 0 if freight == 0 else retail_unit_gross_cent(freight, binding['category'])
                _require(type(raw['retail_gross_cent']) is int and raw['retail_gross_cent'] == gross <= MAX_CENTS)
                observed, inquiry_observed, expiry = map(_stamp, (raw['observed_at'], raw['inquiry_observed_at'], raw['expires_at']))
                _require(max(observed, inquiry_observed) < expiry <= min(observed, inquiry_observed) + TTL)
                self._proofs[identifier] = deepcopy(dict(raw))
            self.mode, self.eligibility, self.inquiry, self.fallback = mode, destination_eligibility, fresh_inquiry, fallback
            self.allow_retained_inquiry = allow_retained_inquiry
            self.clock = clock or (lambda: datetime.now(timezone.utc))
            # Catalogue ranking only. Each real quote still rechecks every gate.
            inherited = getattr(fallback, 'classes', {})
            self.classes = {**(dict(inherited) if isinstance(inherited, Mapping) else {}),
                            **{identifier: 'parts_unique_q1_observation' for identifier in self._proofs}}
        except Exception:
            raise PartsFreightError('invalid_parts_freight_evidence') from None

    def _now(self):
        return _stamp(self.clock())

    def _fallback(self, shipment, context, rows):
        _require(callable(self.fallback))
        subset = deepcopy(dict(shipment))
        subset['private_items'] = deepcopy(rows)
        subset['items'] = [{'id': row['id'], 'quantity': row['quantity']} for row in rows]
        subset['shipment_hash'] = _hash({k: subset[k] for k in ('id', 'destination', 'private_items', 'context_hash')})
        result = self.fallback(subset, deepcopy(dict(context)))
        now = self._now()
        _require(isinstance(result, Mapping) and result.get('confirmed') is True and result.get('mode') == self.mode
                 and result.get('currency') == 'eur' and result.get('vat_percent') == '19.00'
                 and result.get('shipment_hash') == subset['shipment_hash'] and result.get('context_hash') == subset['context_hash']
                 and type(result.get('gross_cent')) is int and 0 <= result['gross_cent'] <= MAX_CENTS)
        observed, expiry = _stamp(result.get('observed_at')), _stamp(result.get('expires_at'))
        _require(observed <= now < expiry and timedelta(0) < expiry - observed <= TTL)
        return result['gross_cent'], expiry

    def __call__(self, shipment, context):
        try:
            now = self._now()
            _require(isinstance(shipment, Mapping) and isinstance(context, Mapping)
                     and shipment.get('destination') in {'workshop', 'customer'}
                     and shipment.get('id') == shipment['destination'] and shipment.get('context_hash') == _hash(context)
                     and shipment.get('shipment_hash') == _hash({k: shipment[k] for k in ('id', 'destination', 'private_items', 'context_hash')}))
            rows = shipment['private_items']
            _require(isinstance(rows, list) and 1 <= len(rows) <= 40)
            seen, parts, others = set(), [], []
            for row in rows:
                _require(isinstance(row, Mapping) and set(row) == {'id', 'identity_hash', 'quantity', 'mapping'}
                         and isinstance(row['id'], str) and ID.fullmatch(row['id']) and row['id'] not in seen
                         and isinstance(row['identity_hash'], str) and HASH.fullmatch(row['identity_hash'])
                         and type(row['quantity']) is int and 1 <= row['quantity'] <= 20)
                seen.add(row['id'])
                if not row['id'].startswith('parts:'):
                    others.append(row); continue
                proof, binding = self._proofs.get(row['id']), self._bindings.get(row['id'])
                _require(proof is not None and binding is not None and row['quantity'] == 1
                         and row['identity_hash'] == binding['identity_hash'] and isinstance(row['mapping'], Mapping)
                         and row['mapping'].get('supplier_article_id') == binding['supplier_article_id']
                         and row['mapping'].get('evidence_id') == binding['evidence_id']
                         and _stamp(proof['observed_at']) <= now and _stamp(proof['inquiry_observed_at']) <= now < _stamp(proof['expires_at']))
                parts.append((row, proof, binding))
            _require(shipment.get('items') == [{'id': row['id'], 'quantity': row['quantity']} for row in rows])
            if parts:
                _require(self.eligibility(shipment['destination'], deepcopy(dict(context))) is True
                         and context.get('workshop_delivery', 'collection') in {'collection', 'customer_reshipment'}
                         and not (shipment['destination'] == 'workshop' and context.get('workshop_delivery') == 'customer_reshipment'))
            total, deadlines = 0, []
            for row, proof, binding in parts:
                started = self._now()
                fresh = self.inquiry(deepcopy(dict(row)), 1)
                now = self._now()
                _require(isinstance(fresh, Mapping) and fresh.get('confirmed') is True and fresh.get('mode') == self.mode
                         and fresh.get('currency') == 'EUR' and fresh.get('product_id') == row['id']
                         and fresh.get('identity_hash') == binding['identity_hash']
                         and fresh.get('supplier_article_id') == binding['supplier_article_id']
                         and fresh.get('mapping_evidence_id') == binding['evidence_id']
                         and fresh.get('expected_ean') == binding['expected_ean'] and fresh.get('ean') == binding['expected_ean']
                         and type(fresh.get('quantity')) is int and fresh['quantity'] == 1
                         and type(fresh.get('stock')) is int and fresh['stock'] == proof['stock_reported'] >= 1
                         and _money(fresh.get('unit_net')) == _money(proof['unit_net']))
                observed, expiry = _stamp(fresh.get('observed_at')), _stamp(fresh.get('expires_at'))
                _require(observed <= now < expiry and (self.allow_retained_inquiry or started <= observed)
                         and timedelta(0) < expiry - observed <= TTL)
                total += proof['retail_gross_cent']
                deadlines.extend((_stamp(proof['expires_at']), expiry))
            if others:
                gross, expiry = self._fallback(shipment, context, others)
                total += gross; deadlines.append(expiry)
            now = self._now()
            _require(deadlines and type(total) is int and 0 <= total <= MAX_CENTS and now < min(deadlines))
            return {'confirmed': True, 'shipment_hash': shipment['shipment_hash'], 'context_hash': shipment['context_hash'],
                    'mode': self.mode, 'currency': 'eur', 'vat_percent': '19.00', 'gross_cent': total,
                    'observed_at': now.isoformat(), 'expires_at': min(deadlines).isoformat(),
                    'retail_tariff_evidence': 'reviewed-exact-observation-tariff-composition', 'shipping_markup_percent': '15.00'}
        except Exception:
            return None
