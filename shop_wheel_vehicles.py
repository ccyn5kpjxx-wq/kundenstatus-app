"""Vehicle discovery over exact rim articles and reviewed report rows.

Supplier model/type listings enable browsing. Only a released, hash-bound PDF
row and a documented rim/tyre pair supply dimensional candidates for a type.
Neither route establishes approval for a particular vehicle or purchase.
"""
from collections import defaultdict
from collections.abc import Mapping
from hashlib import sha256
import json
from pathlib import Path
import re

from shop_catalog_public import CatalogError, _fold, _integer, _matches_vehicle, _text
from shop_partner_native.alzura_live_products import VEHICLE_BRANDS
from shop_tyre_recommendations import tyre_size_key


def _term(value):
    return ' '.join(_fold(value).split())


def _option(make, model, variant):
    # Same exact label maps across articles. No supplier/customer ID is exposed.
    raw = json.dumps([_term(v) for v in (make, model, variant)], ensure_ascii=False)
    return {'key': sha256(raw.encode()).hexdigest()[:24], 'make': make, 'model': model, 'variant': variant}


def _report_rows(name):
    path = Path(__file__).resolve().parent / 'resources' / name
    return json.loads(path.read_text(encoding='utf-8'))


def consistent_tyre_size(tyre):
    size = tyre_size_key(tyre)
    if not size:
        return None
    stated = re.findall(r'(?<![0-9])([0-9]{3})\s*/\s*([0-9]{2,3})\s*Z?R\s*([0-9]{2})(?![0-9])',
                        tyre['size'], re.I)
    if any(f'{w}/{a} R{d}' != size for w, a, d in stated):
        return None
    return size


def _matches_size(tyre, rule):
    if consistent_tyre_size(tyre) != rule['size']:
        return False
    # A service description in an older export can be part of the exact
    # article's size. A contradiction with structured LI/SI excludes it.
    width, tail = rule['size'].split('/')
    ratio, diameter = tail.split(' R')
    stated = re.search(r'(?<![0-9])' + width + r'/' + ratio + r'\s*Z?R\s*' + diameter
                       + r'\s+([0-9]{2,3})\s*([A-Z])(?:\b|$)', tyre['size'], re.I)
    index, speed = (tyre['geometry'].get(key) for key in ('load_index', 'speed_index'))
    if stated:
        if index and str(index) != stated[1] or speed and str(speed).upper() != stated[2].upper():
            return False
        index = stated[1]
    minimum = rule['min_load_index']
    return not minimum or (re.fullmatch(r'[0-9]{2,3}', str(index)) is not None and int(index) >= minimum)


class WheelVehicleIndex:
    def __init__(self, entries, source_rows, identities, documented_pairs, report_resources):
        self.makes = {_term(alias): make for alias, make in VEHICLE_BRANDS.items()}
        self.options, self.rims, self.tyres = {}, defaultdict(dict), defaultdict(dict)
        self._supplier_records = {}
        self.report_dimensions = defaultdict(list)
        self.model_aliases = defaultdict(lambda: defaultdict(set))
        self.rim_names = {}
        rules = _report_rows('shop_wheel_report_rows.json')
        mini_rules = _report_rows('shop_mini_report_rows.json')
        self.tyre_seasons = {pid: entry['public'].get('tyre_season') for pid, entry in entries.items()
                             if entry['public']['department'] == 'tyres'}
        for rid, entry in entries.items():
            rim = entry['public']
            if rim['department'] != 'wheels':
                continue
            source = source_rows[rid]
            self.rim_names[rid] = ' · '.join(x for x in (rim['name'], rim['size']) if x)
            native = source.get('identifiers')
            candidates = source.get('vehicle_candidates')
            if (isinstance(native, Mapping) and native.get('provider') == 'ALZURA'
                    and re.fullmatch(r'[1-9][0-9]{0,17}', str(native.get('provider_id', '')))
                    and isinstance(candidates, list) and len(candidates) <= 500):
                for v in candidates:
                    if (not isinstance(v, Mapping) or v.get('scope') != 'provider'
                            or not _integer(v.get('car_id'), 1, 10**18-1)):
                        continue
                    make = self.makes.get(_term(_text(v.get('brand'), 80)))
                    if make:
                        self._add(rid, make, _text(v.get('model'), 120), _text(v.get('variant'), 160))
            fitment = rim['fitment']
            for index, vehicle in enumerate(fitment['vehicles']):
                make, model = vehicle['make'], vehicle['model']
                self.makes.setdefault(_term(make), make)
                rule = next((r for r in rules.get(identities[rid], ())
                             if r['make'] == make and r['model'] == model and vehicle['conditions'][:1] == [r['condition']]
                             and '/shop/documents/' + r['report'] + '.pdf' in fitment['reports']), None)
                reports = (['/shop/documents/' + rule['report'] + '.pdf'] if rule else
                           fitment['reports'] if len(fitment['reports']) == 1 else [])
                public_vehicle = self._review(rid, vehicle, rule, reports, entries, documented_pairs)
                if public_vehicle:
                    fitment['vehicles'][index] = public_vehicle
            for rule in mini_rules.get(identities[rid], ()):
                asset = rule['report'] + '.pdf'
                public_url = '/shop/documents/' + asset
                resource = report_resources.get(asset, {})
                if (public_url not in rim.get('product_documents', {}).get('rim_report_urls', ())
                        or resource.get('kind') != 'wheel_report' or resource.get('sha256') != rule['pdf_sha256']
                        or resource.get('file') != asset):
                    continue
                vehicle = {'make': rule['make'], 'model': rule['model'], 'conditions': rule['conditions'],
                           'year_from': None, 'year_to': None, 'hsn': '', 'tsn': ''}
                public_vehicle = self._review(rid, vehicle, rule, [public_url], entries, documented_pairs)
                # Also publish the checked rows and their PDF when a visitor
                # opens the article directly, without first selecting a car.
                if fitment['status'] != 'reviewed_with_conditions':
                    fitment['status'] = 'reviewed_with_conditions'
                fitment['vehicles'].append(public_vehicle)
                if public_url not in fitment['reports']:
                    fitment['reports'].append(public_url)
                if rule['make'] not in rim['wheel_vehicle_makes']:
                    rim['wheel_vehicle_makes'].append(rule['make'])
        self.make_names = sorted({option['make'] for option in self.options.values()}, key=str.casefold)
        self.aliases = sorted(self.makes, key=len, reverse=True)

    def _review(self, rid, vehicle, rule, reports, entries, documented_pairs):
        record = self._add(rid, vehicle['make'], vehicle['model'], rule['variant'] if rule else '')
        if not reports:
            return
        if isinstance(record['vehicles'], tuple):
            # Many rims share the same supplier-only vehicle label. Allocate
            # mutable application data only for an actual reviewed article.
            record = {'option': record['option'], 'vehicles': [], 'reports': set(), 'tyres': set()}
            self.rims[rid][record['option']['key']] = record
        public_vehicle = dict(vehicle)
        if len(reports) == 1:
            public_vehicle['report_url'] = reports[0]
        if rule and rule.get('size_condition'):
            # These four original PDFs used a legacy summary that combined
            # different vehicles in the third paragraph. Use the reviewed
            # size/condition of this row and retain every general restriction.
            public_vehicle['conditions'] = [*vehicle['conditions'][:2], rule['size_condition'], *vehicle['conditions'][3:]]
        if rule:
            public_vehicle.update(variant=rule['variant'], tyre_size=rule['size'], report_page=rule['page'])
        record['vehicles'].append(public_vehicle)
        record['reports'].update(reports)
        self.options[record['option']['key']]['has_report'] = True
        if rule:
            for alias in rule.get('model_aliases', ()):
                self.model_aliases[record['option']['key']][alias].add(rid)
            self.report_dimensions[(record['option']['key'], rule['size'])].append((rid, record, rule))
            for tid in documented_pairs.get(rid, ()):
                if _matches_size(entries[tid]['public'], rule) and tid not in record['tyres']:
                    record['tyres'].add(tid)
                    self.tyres[tid].setdefault(record['option']['key'], []).append((rid, record))
        return public_vehicle

    def _add(self, rim, make, model, variant):
        label = (make, model, variant)
        record = self._supplier_records.get(label)
        if record is None:
            option = _option(make, model, variant)
            shared = self.options.setdefault(option['key'], option)
            record = self._supplier_records[label] = {'option': shared, 'vehicles': (), 'reports': (), 'tyres': ()}
        return self.rims[rim].setdefault(record['option']['key'], record)

    def parse_query(self, q):
        term = _term(q)
        for alias in self.aliases:
            if term == alias or term.startswith(alias + ' '):
                return self.makes[alias], term[len(alias):].split()
        return None, []

    def selection(self, *, wheel_make=None, wheel_vehicle=None, wheel_query=None,
                  make=None, model=None, year=None, hsn=None, tsn=None):
        for value in (wheel_make, wheel_query, make, model):
            if value is not None and (not isinstance(value, str) or not 1 <= len(value.strip()) <= 160):
                raise CatalogError('invalid_vehicle_filter')
        if (wheel_vehicle is not None and (not isinstance(wheel_vehicle, str) or not re.fullmatch(r'[0-9a-f]{24}', wheel_vehicle))
                or model is not None and make is None or year is not None and (model is None or not _integer(year, 1900, 2100))
                or (hsn is None) != (tsn is None) or hsn is not None and (not isinstance(hsn, str) or not isinstance(tsn, str)
                    or not re.fullmatch(r'[0-9]{4}', hsn) or not re.fullmatch(r'[A-Z0-9]{3}', tsn))):
            raise CatalogError('invalid_vehicle_filter')
        query_make, tokens = self.parse_query(wheel_query or '')
        if wheel_query is not None and query_make is None:
            raise CatalogError('invalid_vehicle_filter')
        makes = {_term(self.makes.get(_term(v), v)) for v in (wheel_make, query_make, make) if v is not None}
        if not makes and wheel_vehicle is not None:
            makes.add(_term(self.options.get(wheel_vehicle, {}).get('make', '')))
        options, alias_articles, exact_text = {}, {}, {}
        wanted = [w for w in re.findall(r'\w+', ' '.join(tokens)) if w != 'typ']
        for key, option in self.options.items():
            if makes - {_term(option['make'])} or model is not None and _term(model) != _term(option['model']):
                continue
            variant = [w for w in re.findall(r'\w+', _term(option['variant'])) if w != 'typ']
            words = re.findall(r'\w+', _term(option['model'])) + variant
            if not all(w in words for w in wanted):
                # The manufacturer's trading names apply only to the article
                # whose PDF states them, not to other supplier listings with
                # the same vehicle key. Never extend them to other trims.
                alias = next((name for name in self.model_aliases.get(key, {})
                              if wanted in (_term(name).split(), _term(name).split() + variant)), None)
                if alias is None:
                    continue
                alias_articles[key] = self.model_aliases[key][alias]
                option = {**option, 'model': alias}
                words = _term(alias).split() + variant
            options[key] = option
            if variant and wanted == words:
                exact_text[key] = option
        if exact_text:
            options = exact_text
        selected = set(options) if wheel_vehicle is None else {wheel_vehicle} & options.keys()
        # A typed query that unambiguously names an exact published type can
        # resolve a tyre size. A make or a bare model never supplies that type.
        if wheel_vehicle is None and len(exact_text) == 1:
            wheel_vehicle = next(iter(exact_text))
        criteria = [(_term(make), _term(model) if model else None, year)] if make else []
        return {'keys': selected, 'options': options, 'exact': wheel_vehicle,
                'query_make': query_make, 'alias_articles': alias_articles, 'criteria': criteria, 'hsn': hsn, 'tsn': tsn}

    @staticmethod
    def _vehicles(record, selection):
        return [v for v in record['vehicles'] if _matches_vehicle(
            v, selection['criteria'], selection['hsn'], selection['tsn'], False)]

    def rim_records(self, rid, selection):
        records = self.rims.get(rid, {})
        return [record for key, record in records.items() if key in selection['keys']
                and (key not in selection['alias_articles'] or rid in selection['alias_articles'][key]) and
                (not (selection['criteria'] or selection['hsn']) or self._vehicles(record, selection))]

    def project(self, entry, selection):
        product = entry['public']
        identifier, department = product['id'], product['department']
        if department == 'wheels':
            records = self.rim_records(identifier, selection)
            if not records:
                return None
            vehicles = [v for record in records for v in self._vehicles(record, selection)]
            reports = sorted({url for record in records for url in record['reports']})
            result = {**product, 'fitment': {'status': 'reviewed_with_conditions' if vehicles else 'not_confirmed',
                                            'vehicles': vehicles, 'reports': reports}}
            # A general provider document may include different applications
            # even when the API was called with this car ID. Only reviewed PDF
            # content above can be attributed to a filtered vehicle selection.
            result.pop('product_documents', None)
            if selection['exact']:
                result['vehicle_sizes'] = sorted({v['tyre_size'] for v in vehicles if v.get('tyre_size')})
                seasons = {self.tyre_seasons.get(tid) for record in records for tid in record['tyres']}
                result['tyre_seasons'] = [s for s in ('summer', 'winter', 'allseason') if s in seasons]
        elif department == 'tyres' and selection['exact'] in selection['keys']:
            size = tyre_size_key(product)
            paired = self.tyres.get(identifier, {}).get(selection['exact'], ())
            alias_articles = selection['alias_articles'].get(selection['exact'])
            sources = {rid: rec for rid, rec in paired if (alias_articles is None or rid in alias_articles)
                       and self._vehicles(rec, selection)}
            scope = 'documented_rim_tyre_candidate'
            if not sources:
                # A tyre may share the exact report row's nominal size without
                # a released pair for its specific SKU. Expose it only as an
                # inquiry; never feed this list into price/assembly authority.
                for rid, rec, rule in self.report_dimensions.get((selection['exact'], size), ()):
                    if ((alias_articles is None or rid in alias_articles)
                            and _matches_size(product, rule) and self._vehicles(rec, selection)):
                        sources[rid] = rec
                scope = 'report_size_inquiry'
            if not sources:
                return None
            result = {**product, 'wheel_fitment_scope': scope, 'wheel_fitment_sources': [
                {'rim_id': rid, 'rim_name': self.rim_names[rid], 'reports': sorted(rec['reports']),
                 'vehicles': [v for v in self._vehicles(rec, selection) if v.get('tyre_size') == size]}
                for rid, rec in sources.items()]}
            if scope == 'report_size_inquiry':
                result.update(purchase_status='inquiry_only', order_enabled=False)
        else:
            return None
        if selection['exact'] in selection['options']:
            result['wheel_vehicle_match'] = {**selection['options'][selection['exact']],
                                             'has_report': department == 'tyres' or bool(result['fitment']['reports'])}
        return {**entry, 'public': result}
