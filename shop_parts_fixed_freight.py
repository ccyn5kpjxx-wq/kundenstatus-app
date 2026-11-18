"""Explicit merchant-set freight for individually reviewed standard parts.

The fixed EUR 8.21 / 16.42 charges are merchant prices, not assertions about
the supplier's invoice. The merchant accepts higher actual delivery costs.
Every eligible SKU still needs an original-verified standard-delivery source,
an exact approved RSU binding and a current, exact-quantity retained Inquiry.
No category inference, supplier client, network, files or database IO occurs.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta

from shop_parts_freight import PartsRetailFreight, PartsFreightError, _binding, _hash, _reference, _stamp, _money, HASH, ID, TTL, MAX_CENTS

RULE = 'merchant_fixed_parts_freight_v1'
FIELDS = {'rule', 'product_id', 'identity_hash', 'binding_evidence_id', 'supplier_article_id', 'ean',
          'shipping_class', 'direct_gross_cent', 'reshipment_gross_cent', 'quantity_min', 'quantity_max',
          'merchant_cost_difference_accepted', 'source_ref', 'policy_evidence_id', 'proof_sha256'}


def _require(value):
    if not value:
        raise PartsFreightError('invalid_fixed_parts_freight')


class FixedPartsRetailFreight(PartsRetailFreight):
    """Uses the existing typed fallback codec, not its observation tariff rule."""
    def __init__(self, records, *, bindings, mode, destination_eligibility,
                 retained_inquiry, source_refs=(), fallback=None, clock=None):
        try:
            _require(isinstance(records, Mapping) and 1 <= len(records) <= 5000
                     and isinstance(bindings, Mapping) and len(bindings) <= 100000
                     and mode in {'test', 'live'} and callable(destination_eligibility) and callable(retained_inquiry)
                     and (fallback is None or callable(fallback)) and (clock is None or callable(clock))
                     and isinstance(source_refs, (list, tuple)) and len(source_refs) <= 1000)
            sources = {_reference(value) for value in source_refs}
            _require(len(sources) == len(source_refs) and len({file for file, digest in sources}) == len(sources))
            self._bindings = {key: _binding(key, value) for key, value in bindings.items()
                              if isinstance(key, str) and key.startswith('parts:')}
            self._records = {}
            for identifier, record in records.items():
                binding = self._bindings.get(identifier)
                _require(binding is not None and isinstance(record, Mapping) and set(record) == FIELDS
                         and record['rule'] == RULE and record['product_id'] == identifier
                         and record['identity_hash'] == binding['identity_hash']
                         and record['binding_evidence_id'] == binding['mapping_evidence_id']
                         and record['supplier_article_id'] == binding['supplier_article_id'] and record['ean'] == binding['expected_ean']
                         and record['shipping_class'] == 'standard'
                         and type(record['direct_gross_cent']) is int and record['direct_gross_cent'] == 821
                         and type(record['reshipment_gross_cent']) is int and record['reshipment_gross_cent'] == 1642
                         and type(record['quantity_min']) is int and record['quantity_min'] == 1
                         and type(record['quantity_max']) is int and record['quantity_max'] == 20
                         and record['merchant_cost_difference_accepted'] is True
                         and _reference(record['source_ref']) in sources
                         and isinstance(record['policy_evidence_id'], str) and 1 <= len(record['policy_evidence_id']) <= 160
                         and record['policy_evidence_id'] == record['policy_evidence_id'].strip()
                         and not any(ord(c) < 32 or ord(c) == 127 for c in record['policy_evidence_id'])
                         and isinstance(record['proof_sha256'], str) and HASH.fullmatch(record['proof_sha256'])
                         and _hash({k: v for k, v in record.items() if k != 'proof_sha256'}) == record['proof_sha256'])
                self._records[identifier] = deepcopy(dict(record))
            self.mode, self.eligibility, self.inquiry, self.fallback = mode, destination_eligibility, retained_inquiry, fallback
            if clock is None:
                from datetime import datetime, timezone
                clock = lambda: datetime.now(timezone.utc)
            self.clock = clock
            inherited = getattr(fallback, 'classes', {})
            self.classes = {**(dict(inherited) if isinstance(inherited, Mapping) else {}),
                            **{identifier: 'merchant_fixed_standard_parts' for identifier in self._records}}
        except Exception:
            raise PartsFreightError('invalid_fixed_parts_freight') from None

    def __call__(self, shipment, context):
        try:
            now = self._now()
            _require(isinstance(shipment, Mapping) and isinstance(context, Mapping)
                     and shipment.get('destination') in {'workshop', 'customer'}
                     and shipment.get('id') == shipment['destination'] and shipment.get('context_hash') == _hash(context)
                     and shipment.get('shipment_hash') == _hash({k: shipment[k] for k in ('id', 'destination', 'private_items', 'context_hash')}))
            rows = shipment['private_items']; _require(isinstance(rows, list) and 1 <= len(rows) <= 40)
            parts, others, seen = [], [], set()
            for row in rows:
                _require(isinstance(row, Mapping) and set(row) == {'id', 'identity_hash', 'quantity', 'mapping'}
                         and isinstance(row['id'], str) and ID.fullmatch(row['id']) and row['id'] not in seen
                         and isinstance(row['identity_hash'], str) and HASH.fullmatch(row['identity_hash'])
                         and type(row['quantity']) is int and 1 <= row['quantity'] <= 20)
                seen.add(row['id'])
                if not row['id'].startswith('parts:'):
                    others.append(row); continue
                record, binding = self._records.get(row['id']), self._bindings.get(row['id'])
                _require(record is not None and binding is not None and row['identity_hash'] == binding['identity_hash']
                         and isinstance(row['mapping'], Mapping)
                         and row['mapping'].get('supplier_article_id') == binding['supplier_article_id']
                         and row['mapping'].get('evidence_id') == binding['evidence_id'])
                parts.append((row, record, binding))
            _require(shipment.get('items') == [{'id': row['id'], 'quantity': row['quantity']} for row in rows])
            choice = context.get('workshop_delivery', 'collection')
            if parts:
                _require(choice in {'collection', 'customer_reshipment'}
                         and self.eligibility(shipment['destination'], deepcopy(dict(context))) is True)
                if shipment['destination'] == 'workshop' and choice == 'customer_reshipment':
                    _require(self.eligibility('customer', deepcopy(dict(context))) is True)
            total, deadlines = 0, []
            for row, record, binding in parts:
                # Caller supplies only RSUQuoteProvider.retained_inquiry (or an
                # equally cache-only trusted adapter); no source TTL is rebased.
                current = self.inquiry(deepcopy(dict(row)), row['quantity'])
                now = self._now()
                _require(isinstance(current, Mapping) and current.get('confirmed') is True and current.get('mode') == self.mode
                         and current.get('currency') == 'EUR' and current.get('product_id') == row['id']
                         and current.get('identity_hash') == binding['identity_hash']
                         and current.get('supplier_article_id') == binding['supplier_article_id']
                         and current.get('mapping_evidence_id') == binding['evidence_id']
                         and current.get('expected_ean') == binding['expected_ean'] and current.get('ean') == binding['expected_ean']
                         and type(current.get('quantity')) is int and current['quantity'] == row['quantity']
                         and type(current.get('stock')) is int and row['quantity'] <= current['stock'] <= 1_000_000_000)
                _money(current.get('unit_net'))
                observed, expiry = _stamp(current.get('observed_at')), _stamp(current.get('expires_at'))
                _require(observed <= now < expiry and timedelta(0) < expiry - observed <= TTL)
                two_legs = shipment['destination'] == 'workshop' and choice == 'customer_reshipment'
                total += record['reshipment_gross_cent'] if two_legs else record['direct_gross_cent']
                deadlines.append(expiry)
            if others:
                gross, expiry = self._fallback(shipment, context, others)
                total += gross; deadlines.append(expiry)
            now = self._now()
            _require(deadlines and 0 <= total <= MAX_CENTS and now < min(deadlines))
            return {'confirmed': True, 'shipment_hash': shipment['shipment_hash'], 'context_hash': shipment['context_hash'],
                    'mode': self.mode, 'currency': 'eur', 'vat_percent': '19.00', 'gross_cent': total,
                    'observed_at': now.isoformat(), 'expires_at': min(deadlines).isoformat(),
                    'retail_tariff_evidence': 'merchant-fixed-reviewed-standard-parts',
                    'merchant_fixed_tariff': bool(parts)}
        except Exception:
            return None
