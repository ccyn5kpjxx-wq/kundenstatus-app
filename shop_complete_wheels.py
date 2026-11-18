"""Typed payment-only complete-wheel proposals; never a fitment approval."""
from collections.abc import Mapping
import re

ID = re.compile(r'(?:tyre|alloy_wheel|steel_wheel):[a-f0-9]{24}')
HASH = re.compile(r'[a-f0-9]{64}')
RIMS = {'alloy_wheel', 'steel_wheel'}
NOTICE = ('Der Gesamtbetrag wird bei der Zahlung zunächst auf Ihrer Karte reserviert. '
          'Wir prüfen Fahrzeugpassung, Gutachten und die vollständige Lieferbarkeit. '
          'Erst nach erfolgreicher Prüfung und vollständiger Lieferantenbestätigung buchen wir ab. '
          'Kann die Bestellung nicht bestätigt werden, geben wir die Reservierung frei. '
          'Den Montagetermin vereinbaren wir nach Wareneingang.')


def valid_proposal(proof, rim, tyre):
    if not isinstance(proof, Mapping) or set(proof) != {
            'rim_id', 'tyre_id', 'rim_identity_hash', 'tyre_identity_hash',
            'tyre_size', 'rim_diameter', 'evidence_id'}:
        return False
    size = tyre.get('tyre_size')
    parsed = re.fullmatch(r'[0-9]{3}/[0-9]{2,3}\s*R([0-9]{2})(?:\s+[A-Za-z0-9 +./()\-]+)?', size or '') if isinstance(size, str) else None
    return (proof['rim_id'] == rim.get('id') and proof['tyre_id'] == tyre.get('id')
            and proof['rim_identity_hash'] == rim.get('identity_hash')
            and proof['tyre_identity_hash'] == tyre.get('identity_hash')
            and parsed is not None and proof['tyre_size'] == size
            and type(proof['rim_diameter']) is int and 8 <= proof['rim_diameter'] <= 30
            and proof['rim_diameter'] == rim.get('diameter') == tyre.get('diameter') == int(parsed.group(1))
            and isinstance(proof['evidence_id'], str) and 0 < len(proof['evidence_id']) <= 200
            and not any(ord(c) < 32 for c in proof['evidence_id']))


def is_review_snapshot(value):
    """Validate the extra v2 contract. Monetary validation stays in OrderStore.

    v2 deliberately cannot be interpreted by pre-review workers. No field in
    this record authorizes procurement, capture or a vehicle-fitment claim.
    """
    if (not isinstance(value, Mapping) or type(value.get('version')) is not int
            or value['version'] != 2 or value.get('authorization_ready') is not True
            or value.get('fulfillment_ready') is not False or value.get('ready') is not True
            or value.get('blocked_reasons') != [] or not isinstance(value.get('vehicle_ref'), str)
            or not 3 <= len(value['vehicle_ref'].strip()) <= 200
            or any(ord(c) < 32 for c in value['vehicle_ref'])):
        return False
    requests, lines = value.get('request'), value.get('lines')
    if (not isinstance(requests, list) or not isinstance(lines, list) or not requests
            or len(requests) != len(lines) or len(lines) > 40):
        return False
    groups, products = {}, {}
    for row, line in zip(requests, lines):
        if not isinstance(row, Mapping) or not isinstance(line, Mapping):
            return False
        identifier, group = row.get('id'), row.get('assembly_group')
        if (not isinstance(identifier, str) or not ID.fullmatch(identifier)
                or identifier != line.get('id') or not isinstance(group, str) or not 1 <= len(group) <= 80
                or not isinstance(line.get('identity_hash'), str) or not HASH.fullmatch(line['identity_hash'])
                or type(row.get('quantity')) is not int or not 1 <= row['quantity'] <= 4
                or row['quantity'] != line.get('quantity') or line.get('destination') != 'workshop'):
            return False
        services = row.get('services')
        if (not isinstance(services, Mapping) or set(services) != {
                'mounting', 'runflat', 'disposal', 'painting', 'installation', 'oil_service', 'paint_color', 'care'}
                or any(type(services[k]) is not bool for k in ('mounting', 'runflat', 'disposal', 'painting', 'installation', 'oil_service'))
                or any(services[k] is not False for k in ('painting', 'installation', 'oil_service'))
                or services['paint_color'] != '' or services['care'] != ''):
            return False
        groups.setdefault(group, []).append(line)
        products[identifier] = line
    proposals, approved = value.get('assembly_proposals'), value.get('assembly_evidence')
    if not isinstance(proposals, Mapping) or not isinstance(approved, Mapping) or set(proposals) & set(approved):
        return False
    if set(groups) != set(proposals) | set(approved):
        return False
    for group, members in groups.items():
        rim = next((p for p in members if p.get('category') in RIMS), None)
        tyre = next((p for p in members if p.get('category') == 'tyre'), None)
        if len(members) != 2 or rim is None or tyre is None or rim['quantity'] != tyre['quantity']:
            return False
        requested = [row for row in requests if row['assembly_group'] == group]
        if not any(row['services']['mounting'] for row in requested):
            return False
        proof = proposals.get(group, approved.get(group))
        if (not isinstance(proof, Mapping) or proof.get('rim_id') != rim['id']
                or proof.get('tyre_id') != tyre['id'] or not isinstance(proof.get('evidence_id'), str)
                or not proof['evidence_id']):
            return False
        if group in proposals:
            projected_rim = {'id': rim['id'], 'identity_hash': rim['identity_hash'], 'diameter': proof.get('rim_diameter')}
            projected_tyre = {'id': tyre['id'], 'identity_hash': tyre['identity_hash'],
                              'diameter': proof.get('rim_diameter'), 'tyre_size': proof.get('tyre_size')}
            if not valid_proposal(proof, projected_rim, projected_tyre):
                return False
    requirements = value.get('review_requirements')
    if not isinstance(requirements, list) or not 1 <= len(requirements) <= 81:
        return False
    seen, pending_groups, pending_rims = set(), set(), set()
    for item in requirements:
        if not isinstance(item, Mapping):
            return False
        code = item.get('code')
        if code == 'mixed_cart_manual_review_required' and set(item) == {'code'}:
            key = (code, '')
        elif code == 'fitment_unconfirmed' and set(item) == {'code', 'product_id'}:
            identifier = item['product_id']
            if not isinstance(identifier, str) or products.get(identifier, {}).get('category') not in RIMS:
                return False
            pending_rims.add(identifier)
            key = (code, identifier)
        elif code == 'assembly_fitment_unconfirmed' and set(item) == {'code', 'assembly_group'}:
            group = item['assembly_group']
            if not isinstance(group, str) or group not in proposals:
                return False
            pending_groups.add(group)
            key = (code, group)
        else:
            return False
        if key in seen:
            return False
        seen.add(key)
    return (pending_groups == set(proposals)
            and all(proposals[g]['rim_id'] in pending_rims for g in proposals))
