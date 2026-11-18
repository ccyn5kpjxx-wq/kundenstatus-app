"""Pinned licensed article/vehicle associations plus separately reviewed conditions.

An article returned for a provider carId is not a blanket fitment approval.
This pure resolver has no file, credential, supplier, order or payment access.
The caller must supply a pinned immutable association artifact and explicit
server-owned condition reviews. No source query, brand/year filter, customer
free text or association flag can manufacture those reviews.
"""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import re
import unicodedata

HASH = re.compile(r'[a-f0-9]{64}')
PRODUCT = re.compile(r'parts:[a-f0-9]{24}')
VEHICLE_REF = re.compile(r'alzura-vehicle:([1-9][0-9]{0,8})')
CATEGORIES = frozenset({'body_part', 'oil', 'care', 'accessory'})
MAX_BYTES = 2 * 1024 * 1024


class PartsFitmentError(ValueError):
    pass


def _require(value):
    if not value:
        raise PartsFitmentError('invalid_parts_fitment_evidence')


def _hash(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()


def _text(value, maximum=200):
    return (isinstance(value, str) and 0 < len(value) <= maximum and value == value.strip()
            and not any(unicodedata.category(c).startswith('C') for c in value))


def _id(value):
    return type(value) is int and 0 < value < 1_000_000_000


def _month(value):
    return type(value) is int and 190001 <= value <= 210012 and 1 <= value % 100 <= 12


def _decode(payload):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result)
            result[key] = value
        return result
    return json.loads(payload, object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(PartsFitmentError('invalid_parts_fitment_evidence')))


def _source(value, *, row=False):
    _require(isinstance(value, Mapping) and isinstance(value.get('sha256'), str)
             and HASH.fullmatch(value['sha256']) and _text(value.get('file'), 250))
    filename = value['file'].replace('\\', '/')
    _require(not filename.startswith('/') and ':' not in filename and '..' not in filename.split('/'))
    if row:
        _require(type(value.get('row_index')) is int and 0 <= value['row_index'] <= 300_000)


def _vehicle(proof):
    source = proof['vehicle_source']
    _source(source, row=True)
    vehicle = source.get('vehicle')
    details = vehicle.get('vehicleDetails') if isinstance(vehicle, Mapping) else None
    car_id = proof['car_id']
    _require(isinstance(details, Mapping) and _id(vehicle.get('carId')) and _id(details.get('carId'))
             and _id(details.get('typeNumber')) and _id(details.get('manuId')) and _id(details.get('modId'))
             and vehicle.get('carId') == car_id
             and details.get('carId') == car_id and details.get('typeNumber') == car_id
             and details.get('manuId') == proof['manufacturer_id'] and details.get('modId') == proof['model_id']
             and _text(details.get('manuName')) and _text(details.get('modelName'))
             and _text(details.get('typeName')) and _month(details.get('yearOfConstrFrom'))
             and (details.get('yearOfConstrTo') is None or _month(details['yearOfConstrTo'])
                  and details['yearOfConstrTo'] >= details['yearOfConstrFrom']))
    engines = vehicle.get('motorCodes', {}).get('array') if isinstance(vehicle.get('motorCodes'), Mapping) else None
    kba = vehicle.get('kbaNumbers', {}).get('array') if isinstance(vehicle.get('kbaNumbers'), Mapping) else None
    _require(isinstance(engines, list) and 1 <= len(engines) <= 20
             and all(isinstance(row, Mapping) and _text(row.get('motorCode'), 80) for row in engines)
             and isinstance(kba, list) and 1 <= len(kba) <= 100
             and all(isinstance(row, Mapping) and isinstance(row.get('kbaNo'), str)
                     and re.fullmatch(r'[0-9]{4}[A-Z0-9]{3}', row['kbaNo']) for row in kba))
    engine_codes, kba_numbers = [v['motorCode'] for v in engines], [v['kbaNo'] for v in kba]
    _require(len(set(engine_codes)) == len(engine_codes) and len(set(kba_numbers)) == len(kba_numbers))
    return {'vehicle_ref': 'alzura-vehicle:' + str(car_id), 'car_id': car_id,
            'manufacturer_id': proof['manufacturer_id'], 'model_id': proof['model_id'],
            'make': details['manuName'], 'model': details['modelName'], 'engine': details['typeName'],
            'engine_codes': engine_codes, 'date_from_yyyymm': details['yearOfConstrFrom'],
            'date_to_yyyymm': details.get('yearOfConstrTo'), 'hsn_tsn': kba_numbers,
            'power_kw_from': details.get('powerKwFrom'), 'power_kw_to': details.get('powerKwTo')}


class PartsVehicleFitments:
    def __init__(self, artifact_bytes, *, artifact_sha256, base_bundle_sha256, products,
                 approved_conditions=(), approved_certificate_hashes=()):
        try:
            _require(isinstance(artifact_bytes, bytes) and 0 < len(artifact_bytes) <= MAX_BYTES
                     and isinstance(artifact_sha256, str) and HASH.fullmatch(artifact_sha256)
                     and sha256(artifact_bytes).hexdigest() == artifact_sha256
                     and isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
                     and isinstance(products, Mapping))
            artifact = _decode(artifact_bytes)
            _require(isinstance(artifact, Mapping) and type(artifact.get('schema_version')) is int
                     and artifact['schema_version'] == 1 and artifact.get('bundle_sha256') == base_bundle_sha256
                     and isinstance(artifact.get('proofs'), list) and 1 <= len(artifact['proofs']) <= 5000
                     and type(artifact.get('associations')) is int and artifact['associations'] == len(artifact['proofs']))
            self._artifact_sha, self._associations, self._vehicles, self._products = artifact_sha256, {}, {}, {}
            proof_keys = {'article_attributes', 'article_info', 'article_source', 'car_id', 'certification_compulsory',
                          'full_vehicle_fitment_approved', 'identity_hash', 'manufacturer', 'manufacturer_id', 'model_id',
                          'mpn', 'native_provider_id', 'product_id', 'purchase_enabled', 'query_evidence', 'scope',
                          'source_query', 'vehicle_source'}
            for raw in artifact['proofs']:
                _require(isinstance(raw, Mapping) and set(raw) == proof_keys
                         and isinstance(raw.get('product_id'), str) and PRODUCT.fullmatch(raw['product_id'])
                         and isinstance(raw.get('identity_hash'), str) and HASH.fullmatch(raw['identity_hash'])
                         and _id(raw.get('car_id')) and _id(raw.get('manufacturer_id')) and _id(raw.get('model_id'))
                         and _text(raw.get('manufacturer'), 120) and _text(raw.get('mpn'), 160)
                         and raw.get('scope') == 'provider_vehicle_catalog_association'
                         and raw.get('full_vehicle_fitment_approved') is False and raw.get('purchase_enabled') is False
                         and type(raw.get('certification_compulsory')) is bool)
                product = products.get(raw['product_id'])
                _require(isinstance(product, Mapping) and product.get('identity_hash') == raw['identity_hash']
                         and product.get('category') in CATEGORIES)
                query = raw['source_query']
                _require(isinstance(query, Mapping) and set(query) == {'assemblyGroupNodeId', 'carId', 'linkingTargetType',
                         'manufacturerId', 'modelId', 'page', 'showPerPage'} and query['carId'] == raw['car_id']
                         and _id(query['carId']) and _id(query['manufacturerId']) and _id(query['modelId'])
                         and query['manufacturerId'] == raw['manufacturer_id'] and query['modelId'] == raw['model_id']
                         and query['linkingTargetType'] == 'P' and _id(query['assemblyGroupNodeId'])
                         and type(query['page']) is int and 0 <= query['page'] <= 10000
                         and type(query['showPerPage']) is int and 1 <= query['showPerPage'] <= 100)
                _source(raw['article_source'], row=True); _source(raw['query_evidence'])
                for value in (raw['article_attributes'], raw['article_info']):
                    _require(isinstance(value, Mapping) and set(value) == {'array'} and isinstance(value['array'], list)
                             and len(value['array']) <= 200)
                native = raw['native_provider_id']
                match = re.fullmatch(r'wearparts:([1-9][0-9]*)-([0-9a-f]+):([1-9][0-9]*)', native) if isinstance(native, str) else None
                _require(match is not None and match[1] == match[3]
                         and bytes.fromhex(match[2]).decode('utf-8') == raw['mpn'])
                vehicle = _vehicle(raw)
                ref = vehicle['vehicle_ref']; old = self._vehicles.get(ref)
                _require(old is None or old == vehicle)
                self._vehicles[ref] = vehicle
                key = (raw['product_id'], ref)
                _require(key not in self._associations)
                conditions = {k: raw[k] for k in ('article_attributes', 'article_info', 'certification_compulsory')}
                self._associations[key] = {'raw': deepcopy(dict(raw)), 'association_hash': _hash(raw),
                    'conditions_hash': _hash(conditions), 'vehicle_details_hash': _hash(raw['vehicle_source']['vehicle'])}
                self._products[raw['product_id']] = {'identity_hash': raw['identity_hash'], 'category': product['category']}
            _require(type(artifact.get('products_with_source_query_association')) is int
                     and artifact['products_with_source_query_association'] == len(self._products))
            _require(isinstance(approved_conditions, (list, tuple)) and len(approved_conditions) <= 5000
                     and isinstance(approved_certificate_hashes, (list, tuple, set, frozenset))
                     and all(isinstance(v, str) and HASH.fullmatch(v) for v in approved_certificate_hashes))
            certificates = frozenset(approved_certificate_hashes)
            self._approvals = {}
            fields = {'product_id', 'identity_hash', 'vehicle_ref', 'association_hash', 'conditions_hash', 'vehicle_details_hash',
                      'conditions_reviewed', 'vehicle_fitment_reviewed', 'unresolved_conditions', 'certificate_refs', 'review_evidence_id'}
            for review in approved_conditions:
                _require(isinstance(review, Mapping) and set(review) == fields
                         and review['conditions_reviewed'] is True and review['vehicle_fitment_reviewed'] is True
                         and review['unresolved_conditions'] == [] and type(review['unresolved_conditions']) is list
                         and isinstance(review['certificate_refs'], list) and len(review['certificate_refs']) <= 10
                         and all(isinstance(v, str) and HASH.fullmatch(v) for v in review['certificate_refs'])
                         and len(set(review['certificate_refs'])) == len(review['certificate_refs'])
                         and set(review['certificate_refs']) <= certificates
                         and _text(review['review_evidence_id'], 160) and '://' not in review['review_evidence_id'])
                key = (review['product_id'], review['vehicle_ref'])
                association = self._associations.get(key)
                _require(association is not None and key not in self._approvals
                         and review['identity_hash'] == association['raw']['identity_hash']
                         and all(review[k] == association[k] for k in ('association_hash', 'conditions_hash', 'vehicle_details_hash'))
                         and (not association['raw']['certification_compulsory'] or review['certificate_refs']))
                self._approvals[key] = deepcopy(dict(review))
        except Exception:
            raise PartsFitmentError('invalid_parts_fitment_evidence') from None

    def vehicles(self):
        """Canonical options; selecting one still does not approve a part."""
        return deepcopy([self._vehicles[k] for k in sorted(self._vehicles, key=lambda ref: self._vehicles[ref]['car_id'])])

    def requires_vehicle(self, product):
        if not isinstance(product, Mapping) or not isinstance(product.get('id'), str):
            return False
        record = self._products.get(product['id'])
        return bool(record and product.get('identity_hash') == record['identity_hash'] and product.get('category') == record['category'])

    def review_requirements(self, product_id, vehicle_ref):
        """Read-only review facts. Never generates an approval or certificate."""
        if not isinstance(product_id, str) or not isinstance(vehicle_ref, str):
            return None
        record = self._associations.get((product_id, vehicle_ref))
        if record is None:
            return None
        raw = record['raw']
        return deepcopy({'product_id': product_id, 'identity_hash': raw['identity_hash'], 'vehicle_ref': vehicle_ref,
            **{k: record[k] for k in ('association_hash', 'conditions_hash', 'vehicle_details_hash')},
            'article_attributes': raw['article_attributes'], 'article_info': raw['article_info'],
            'certification_compulsory': raw['certification_compulsory']})

    def lookup(self, product, vehicle_ref):
        if not self.requires_vehicle(product) or not isinstance(vehicle_ref, str) or not VEHICLE_REF.fullmatch(vehicle_ref):
            return None
        review = self._approvals.get((product['id'], vehicle_ref))
        if review is None:
            return None
        return {'confirmed': True, 'identity_hash': product['identity_hash'], 'vehicle_ref': vehicle_ref,
                'evidence_id': _hash({'artifact_sha256': self._artifact_sha, 'review': review})}
