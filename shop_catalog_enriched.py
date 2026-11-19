"""Reviewed, source-bound browsing metadata; never purchasing authority.

No provider IO. Families retain full model and finish. Parts vehicle evidence
is separately reviewed against the exact variant. Wheel-photo coordinates bind
the original released image, and are never guessed from a similar design.
"""
from collections.abc import Mapping
from copy import deepcopy
import math
import re
import unicodedata

from shop_catalog_public import PublicCatalog, CatalogError, PUBLIC_ID, _text, _fold, _quote, _stamp, variant_fingerprint, _size_value
from shop_wheel_vehicles import WheelVehicleIndex, consistent_tyre_size
from shop_wheel_prices import published_wheel_tier, tyre_service_gross_cent


def _norm(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split()).casefold()


def _sale_dimensions(proof, row):
    """Project reviewed exact-variant dimensions, never vehicle approval."""
    if (not isinstance(proof, Mapping) or proof.get('confirmed') is not True
            or proof.get('supplier') != 'RSU'
            or proof.get('product_id') != row.get('selection_id', row.get('id'))
            or proof.get('identity_hash') != variant_fingerprint(row)
            or not _text(proof.get('evidence_id'), 200)):
        return None
    from shop_rsu_rim_evidence import RimEvidenceError, normalize_rim_geometry
    try:
        return normalize_rim_geometry(proof.get('geometry'))
    except (RimEvidenceError, TypeError, ValueError):
        return None


def _face(value):
    if not isinstance(value, Mapping) or value.get('reviewed') is not True:
        return None
    keys = ('width', 'height', 'cx', 'cy', 'rx', 'ry', 'rotation')
    if any(type(value.get(k)) not in (int, float) or not math.isfinite(value[k]) for k in keys):
        return None
    w, h = value['width'], value['height']
    if not (1 <= w <= 5000 and 1 <= h <= 5000 and 0 < value['rx'] <= w and 0 < value['ry'] <= h
            and 0 <= value['cx'] <= w and 0 <= value['cy'] <= h and -180 <= value['rotation'] <= 180):
        return None
    hub = value.get('hub')
    if (not isinstance(hub, Mapping) or any(type(hub.get(k)) not in (int, float) or not math.isfinite(hub[k]) for k in ('x', 'y'))
            or not 0 <= hub['x'] <= w or not 0 <= hub['y'] <= h):
        return None
    result = {k: value[k] for k in keys}
    result.update(reviewed=True, hub={k: hub[k] for k in ('x', 'y')})
    polygons = value.get('openings')
    if polygons is not None:
        if not isinstance(polygons, list) or len(polygons) > 48:
            return None
        count = 0
        for polygon in polygons:
            if not isinstance(polygon, list) or not 3 <= len(polygon) <= 128:
                return None
            count += len(polygon)
            for point in polygon:
                if (not isinstance(point, list) or len(point) != 2
                        or any(type(n) not in (int, float) or not math.isfinite(n) for n in point)
                        or not 0 <= point[0] <= w or not 0 <= point[1] <= h):
                    return None
        if count > 2048:
            return None
        result['openings'] = deepcopy(polygons)
    return result


def validated_parts_notices(rows, released_ids, parts_notices):
    """Validate exact released parts notes without IO or purchase authority.

    The integration also calls this before provisioning or seeding any store.
    No source/evidence/provider prices enter the returned public prose.
    """
    if parts_notices is None:
        return {}
    if (not isinstance(parts_notices, Mapping) or len(parts_notices) > 5000
            or not isinstance(rows, (list, tuple)) or len(rows) > 100000
            or not isinstance(released_ids, (set, frozenset, list, tuple))
            or len(released_ids) > 100000 or any(not isinstance(value, str) for value in released_ids)):
        raise CatalogError('invalid_parts_notices')
    released = frozenset(released_ids)
    index = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        identifier = row.get('selection_id', row.get('id'))
        if isinstance(identifier, str) and identifier in released:
            if identifier in index:
                raise CatalogError('invalid_parts_notices')
            index[identifier] = row
    result = {}
    for identifier, record in parts_notices.items():
        row = index.get(identifier) if isinstance(identifier, str) else None
        if (row is None or not re.fullmatch(r'parts:[a-f0-9]{24}', identifier)
                or row.get('department') != 'parts' or row.get('category') not in {'body_part', 'accessory'}
                or not isinstance(record, Mapping) or set(record) != {'identity_hash', 'notes'}
                or record.get('identity_hash') != variant_fingerprint(row)
                or not isinstance(record.get('notes'), list) or not 1 <= len(record['notes']) <= 12):
            raise CatalogError('invalid_parts_notices')
        notes = []
        for note in record['notes']:
            if not isinstance(note, str) or not 1 <= len(note) <= 2000:
                raise CatalogError('invalid_parts_notices')
            value = _text(note, 2000)
            if not value:
                raise CatalogError('invalid_parts_notices')
            notes.append(value)
        result[identifier] = {'identity_hash': record['identity_hash'], 'notes': notes}
    return result


def validated_inquiry_prices(rows, released_ids, procurement_ids, values):
    """Validate a separate display snapshot; it grants no supplier authority."""
    if values is None:
        return {}
    if not isinstance(values, Mapping) or len(values) > 5000:
        raise CatalogError('invalid_inquiry_prices')
    source = {r.get('selection_id', r.get('id')): r for r in rows if isinstance(r, Mapping)}
    released, purchasing = set(released_ids or ()), set(procurement_ids or ())
    result = {}
    for identifier, value in values.items():
        row = source.get(identifier)
        if (not isinstance(identifier, str) or not re.fullmatch(r'parts:[a-f0-9]{24}', identifier)
                or identifier not in released or identifier in purchasing or not isinstance(row, Mapping)
                or row.get('department') != 'parts' or row.get('category') not in {'body_part', 'accessory'}
                or not isinstance(value, Mapping) or set(value) != {'identity_hash', 'price'}
                or value['identity_hash'] != variant_fingerprint(row)
                or not isinstance(value['price'], Mapping)):
            raise CatalogError('invalid_inquiry_prices')
        price = value['price']
        if price.get('kind') == 'partner_inquiry_pricebook' and price.get('source') == 'ALZURA':
            from shop_parts_partner_inquiry import normalize_partner_inquiry_price
            try:
                quote = normalize_partner_inquiry_price(price)
            except ValueError:
                raise CatalogError('invalid_inquiry_prices') from None
        elif price.get('kind') == 'merchant_pricebook' and price.get('source') == 'RSU':
            quote = _quote({'category': row['category'], 'quote_public': price}, allow_published_prices=True)
        else:
            raise CatalogError('invalid_inquiry_prices')
        if quote is None:
            raise CatalogError('invalid_inquiry_prices')
        # Keep this detached projection valid across the bootstrap preflight
        # and the catalogue constructor's independent validation.
        for line in quote['prices_by_quantity'].values():
            line.update(currency='EUR', vat_percent='19.00')
        quote['stock_current'] = False
        result[identifier] = {'identity_hash': value['identity_hash'], 'price': quote}
    return result


class EnrichedPublicCatalog(PublicCatalog):
    def __init__(self, products, *, browsing_evidence=None, compatible_pairs=None,
                 procurement_ids=None, product_documents=None, standalone_rims=None,
                 released_rim_report_resources=None, rsu_only_prices=False, preferred_ids=None, shipping_ready_ids=None,
                 checkout_excluded_ids=None, parts_notices=None, inquiry_prices=None,
                 recommendation_ladder=False, recommendation_document_ids=None, wheel_pickup_freight=None, **kwargs):
        if type(rsu_only_prices) is not bool:
            raise CatalogError('invalid_price_policy')
        if type(recommendation_ladder) is not bool:
            raise CatalogError('invalid_recommendation_policy')
        self._recommendation_ladder = recommendation_ladder
        self._wheel_pickup_freight = wheel_pickup_freight
        rows = deepcopy(list(products))
        notices = validated_parts_notices(rows, kwargs.get('released_ids'), parts_notices)
        display_prices = validated_inquiry_prices(rows, kwargs.get('released_ids'), procurement_ids, inquiry_prices)
        super().__init__(rows, **kwargs)
        if (procurement_ids is not None and (not isinstance(procurement_ids, (set, frozenset, list, tuple))
                or any(not isinstance(i, str) for i in procurement_ids))):
            raise CatalogError('invalid_procurement_ids')
        procurement = frozenset(procurement_ids or ())
        self._rsu_only_prices, self._procurement_ids = rsu_only_prices, procurement
        self._inquiry_quotes = {identifier: value['price'] for identifier, value in display_prices.items()}
        if (checkout_excluded_ids is not None and
                (not isinstance(checkout_excluded_ids, (set, frozenset))
                 or not checkout_excluded_ids <= self._rows.keys())):
            raise CatalogError('invalid_checkout_excluded_ids')
        # A known unavailable mandatory document removes checkout eligibility,
        # not the actual source identity, procurement mapping or merchant price.
        self._checkout_excluded_ids = frozenset(checkout_excluded_ids or ())
        if (recommendation_document_ids is not None and
                (not isinstance(recommendation_document_ids, (set, frozenset))
                 or not recommendation_document_ids <= self._rows.keys())):
            raise CatalogError('invalid_recommendation_documents')
        self._recommendation_document_ids = (frozenset(recommendation_document_ids)
            if recommendation_document_ids is not None else None)
        if (shipping_ready_ids is not None and
                (not isinstance(shipping_ready_ids, (set, frozenset, list, tuple))
                 or any(not isinstance(i, str) for i in shipping_ready_ids)
                 or not set(shipping_ready_ids) <= procurement
                 or not set(shipping_ready_ids) <= self._rows.keys())):
            raise CatalogError('invalid_shipping_ready_ids')
        # The caller supplies IDs only after validating its private tariff.
        # This says nothing about the buyer's address or a final shipping quote.
        self._shipping_ready_ids = frozenset(shipping_ready_ids or ())
        if (preferred_ids is not None and (not isinstance(preferred_ids, (set, frozenset))
                or not preferred_ids <= procurement or not preferred_ids <= self._rows.keys())):
            raise CatalogError('invalid_preferred_ids')
        self._preferred_ids = frozenset(preferred_ids or ())
        if standalone_rims is not None and not isinstance(standalone_rims, Mapping):
            raise CatalogError('invalid_standalone_rims')
        sale_proofs = standalone_rims or {}
        documents = product_documents if isinstance(product_documents, Mapping) else {}
        released_documents = set((kwargs.get('document_urls') or {}).values())
        self._source_rows = {row.get('selection_id', row.get('id')): row for row in rows if isinstance(row, Mapping)}
        evidence = browsing_evidence if isinstance(browsing_evidence, Mapping) else {}
        self._families, self._family_for = {}, {}
        self._pairs = deepcopy(compatible_pairs) if isinstance(compatible_pairs, Mapping) else {}
        released_images = set((kwargs.get('image_urls') or {}).values())
        for row in rows:
            identifier = row.get('selection_id', row.get('id')) if isinstance(row, Mapping) else None
            entry = self._rows.get(identifier)
            if entry is None:
                continue
            product = entry['public']
            if identifier in notices:
                product['application_notes'] = deepcopy(notices[identifier]['notes'])
            product['purchase_status'] = ('supplier_check_required'
                if identifier in procurement and identifier not in self._checkout_excluded_ids else 'inquiry_only')
            product['price_label'] = '' if rsu_only_prices or identifier in procurement else 'Richtpreis'
            product['delivery'] = {'status': 'unknown', 'label': 'Lieferzeit wird bestätigt'}
            if (identifier in procurement and product['category'] in {'alloy_wheel', 'steel_wheel'}):
                dimensions = _sale_dimensions(sale_proofs.get(identifier), row)
                if dimensions is not None:
                    product['sale_dimensions'] = dimensions
                    if sale_proofs[identifier].get('model_photo_only') is True:
                        product['image_note'] = 'Modellfoto. Größe und genaue Ausführung können von der Abbildung abweichen.'
            doc = documents.get(identifier)
            if (product['category'] == 'tyre' and isinstance(doc, Mapping)
                    and doc.get('identity_hash') == variant_fingerprint(row)):
                original = {}
                for key, extensions in (('tyre_label_url', ('jpg', 'png')), ('tyre_sheet_url', ('pdf',))):
                    path = doc.get(key)
                    if (isinstance(path, str) and path in released_documents
                            and re.fullmatch(r'/shop/documents/[a-f0-9]{64}\.(' + '|'.join(extensions) + ')', path)):
                        original[key] = path
                eprel = doc.get('eprel_id')
                if original and isinstance(eprel, str) and re.fullmatch(r'[1-9][0-9]{0,11}', eprel):
                    product['product_documents'] = {**original, 'eprel_id': eprel}
            if product['category'] in {'alloy_wheel', 'steel_wheel'}:
                from shop_rim_reports import public_rim_documents
                rim_documents = public_rim_documents(row, doc, kwargs.get('document_urls') or {})
                if rim_documents:
                    product['product_documents'] = rim_documents
            facts = row.get('quality_facts')
            if isinstance(facts, Mapping):
                public_facts = {}
                for key in ('wet_grip', 'fuel_efficiency', 'noise_class', 'external_noise_db', 'vehicle_class'):
                    fact = facts.get(key)
                    value = _text(str(fact), 30) if type(fact) in (str, int) else ''
                    if value:
                        public_facts[key] = value
                if public_facts:
                    product['quality_facts'] = public_facts
            ev = evidence.get(identifier, {})
            if isinstance(ev, Mapping) and ev.get('variant_fingerprint') == variant_fingerprint(row):
                gallery = ev.get('gallery')
                if isinstance(gallery, list) and len(gallery) <= 12:
                    product['images'] = list(dict.fromkeys(product['images'] + [v for v in gallery
                        if isinstance(v, str) and v in released_images]))
                if ev.get('photo') == row.get('photo') and product['images']:
                    face = _face(ev.get('photo_face'))
                    if face:
                        product['photo_face'] = face
                vehicles = ev.get('provider_vehicles')
                if (ev.get('provider_reviewed') is True and product['department'] == 'parts'
                        and isinstance(vehicles, list) and 1 <= len(vehicles) <= 512):
                    clean = []
                    for vehicle in vehicles:
                        if not isinstance(vehicle, Mapping):
                            continue
                        make, model = _text(vehicle.get('make'), 80), _text(vehicle.get('model'), 120)
                        start, end = vehicle.get('year_from'), vehicle.get('year_to')
                        if (not make or not model or type(start) is not int or type(end) is not int
                                or not 1900 <= start <= end <= 2100):
                            continue
                        hsn, tsn = vehicle.get('hsn', ''), vehicle.get('tsn', '')
                        if not (isinstance(hsn, str) and isinstance(tsn, str)
                                and (hsn == tsn == '' or re.fullmatch(r'[0-9]{4}', hsn) and re.fullmatch(r'[A-Z0-9]{3}', tsn))):
                            continue
                        clean.append({'make': make, 'model': model, 'year_from': start, 'year_to': end,
                                      'hsn': hsn, 'tsn': tsn, 'label': _text(vehicle.get('label'), 240), 'conditions': []})
                    if clean:
                        product['fitment'] = {'status': 'provider_vehicle', 'vehicles': clean, 'reports': []}
                        entry['search'] += ' ' + _fold(' '.join(' '.join((v['make'], v['model'], v['label'])) for v in clean))
            if product['department'] == 'wheels':
                key = tuple(_norm(str(v)) for v in (product['category'], product['brand'], product['name'], product['specifications'].get('Farbe', '')))
                if not product['brand'] or not product['name']:
                    key = (identifier,)
                self._family_for[identifier] = key
                self._families.setdefault(key, []).append(identifier)

        # Fixed, reviewed catalogue evidence only. This does not query stock,
        # reserve goods or turn a width candidate into vehicle approval.
        identities = {identifier: variant_fingerprint(self._source_rows[identifier]) for identifier in self._rows}
        self._candidate_tyres = {}
        documented_pairs = {}
        for identifier, entry in self._rows.items():
            if entry['public']['department'] != 'wheels':
                continue
            candidates, seen = [], set()
            documented_pairs[identifier] = set()
            pairs = self._pairs.get(identifier, [])
            for pair in pairs[:1000] if isinstance(pairs, list) else ():
                if not isinstance(pair, Mapping) or pair.get('reviewed') is not True:
                    continue
                tyre_id = pair.get('tyre_id')
                if not isinstance(tyre_id, str) or tyre_id not in self._rows:
                    continue
                tyre = self._rows[tyre_id]['public']
                docs = tyre.get('product_documents', {})
                if self._recommendation_ladder and (
                        tyre_id in self._checkout_excluded_ids
                        or self._recommendation_document_ids is not None and tyre_id not in self._recommendation_document_ids
                        or not docs.get('tyre_label_url') or not docs.get('tyre_sheet_url')):
                    continue
                if (tyre['category'] != 'tyre' or pair.get('rim_identity') != identities[identifier]
                        or pair.get('tyre_identity') != identities[tyre_id]
                        or consistent_tyre_size(tyre) is None
                        or _size_value(tyre['geometry']['diameter']) != _size_value(entry['public']['geometry'].get('diameter'))):
                    continue
                source = pair.get('source')
                if (pair.get('scope') == 'rim_width_candidate' and isinstance(source, Mapping)
                        and re.fullmatch(r'[a-f0-9]{64}', str(source.get('sha256', '')))
                        and type(source.get('page')) is int and source['page'] >= 1 and _text(pair.get('evidence_id'), 200)
                        and tyre_id not in self._checkout_excluded_ids):
                    documented_pairs[identifier].add(tyre_id)
                if tyre_id in seen:
                    continue
                seen.add(tyre_id)
                candidates.append((tyre_id, tyre['tyre_season']))
            self._candidate_tyres[identifier] = candidates
            seasons = {season for _, season in candidates}
            entry['public']['tyre_seasons'] = [season for season in ('summer', 'winter', 'allseason') if season in seasons]

        self._wheel_index = WheelVehicleIndex(self._rows, self._source_rows, identities, documented_pairs,
            released_rim_report_resources or {})

    def _cached_entry(self, entry):
        identifier = entry['public']['id']
        if identifier in self._inquiry_quotes:
            return {**entry, 'quote': deepcopy(self._inquiry_quotes[identifier])}
        # An unbound article is outside this merchant's RSU offer, regardless
        # of an imported quote or a stale/misconfigured display cache. Do not
        # even consult that cache for an article we cannot procure.
        if self._rsu_only_prices and entry['public']['id'] not in self._procurement_ids:
            return {**entry, 'quote': None}
        return super()._cached_entry(entry)

    def _browse_order(self, entry):
        return (entry['public']['id'] not in self._preferred_ids,
                not bool(entry['public'].get('sale_dimensions')), *super()._browse_order(entry))

    def _offer_rank(self, entry, now):
        public = entry['public']
        identifier = public['id']
        if identifier not in self._procurement_ids or identifier in self._checkout_excluded_ids:
            if self._inquiry_quotes:
                quote = entry.get('quote')
                return 4 if identifier in self._inquiry_quotes and quote and _stamp(quote['observed_at']) <= now < _stamp(quote['expires_at']) else 5
            return 4
        price_rank = super()._offer_rank(entry, now)
        if price_rank == 2:
            return 3
        if price_rank == 1:
            return 2
        if public['category'] == 'tyre':
            docs = public.get('product_documents', {})
            obligations = all(docs.get(key) for key in ('tyre_label_url', 'tyre_sheet_url'))
        elif public['department'] == 'wheels':
            obligations = bool(public.get('sale_dimensions'))
        else:
            # Vehicle-specific parts still need the customer's exact vehicle.
            obligations = public['department'] != 'parts'
        return 0 if obligations and identifier in self._shipping_ready_ids else 1

    def _project(self, entry, now, *, detail=True):
        product = super()._project(entry, now, detail=detail)
        if product['id'] in self._inquiry_quotes:
            product.update(purchase_status='inquiry_only', order_enabled=False, price_label='')
            if product['price'] is not None:
                product['price']['stock_current'] = False
        elif self._rsu_only_prices and product['id'] not in self._procurement_ids:
            product.update(price=None, price_status='not_offered', price_label='')
        return product

    def _wheel_selection(self, filters, *, query='', department=None):
        options = {key: filters.pop(key) for key in ('wheel_make', 'wheel_vehicle', 'wheel_query') if key in filters}
        if not options.get('wheel_query') and isinstance(query, str) and query and department in (None, 'tyres', 'wheels'):
            if self._wheel_index.parse_query(query)[0]:
                options['wheel_query'] = query
                filters['q'] = ''
        legacy = ('make', 'model', 'year', 'hsn', 'tsn')
        if not any(v is not None for v in options.values()) and not (
                department in ('wheels', 'tyres') and any(filters.get(k) is not None for k in legacy)):
            return None
        if department not in (None, 'wheels', 'tyres'):
            raise CatalogError('invalid_vehicle_filter')
        options.update({key: filters.pop(key) for key in legacy if key in filters})
        return self._wheel_index.selection(**options)

    def get(self, product_id, *, now=None, **filters):
        if not filters:
            return super().get(product_id, now=now)
        self._validate_scope_fields(filters)
        selection = self._wheel_selection(filters, department='wheels')
        if selection is None:
            return super().get(product_id, now=now)
        if not isinstance(product_id, str) or not PUBLIC_ID.fullmatch(product_id):
            raise CatalogError('invalid_product_id')
        entry = self._rows.get(product_id)
        entry = self._wheel_index.project(entry, selection) if entry else None
        return self._project(self._cached_entry(entry), self._now(now)) if entry else None

    @staticmethod
    def _validate_scope_fields(filters):
        if set(filters) - {'wheel_make', 'wheel_vehicle', 'wheel_query', 'make', 'model', 'year', 'hsn', 'tsn'}:
            raise CatalogError('invalid_vehicle_filter')

    def _selection_metadata(self, result, selection, department, matching_rims):
        if department in ('wheels', 'tyres') or selection:
            result['wheel_makes'] = self._wheel_index.make_names.copy()
        if selection:
            choices = []
            for option in selection['options'].values():
                if not option['model'] or not option['variant']:
                    continue
                choice = dict(option)
                aliases = self._wheel_index.model_aliases.get(option['key'], {})
                names = {name for name, article_ids in aliases.items() if matching_rims is None or article_ids & matching_rims}
                if names:
                    names.add(self._wheel_index.options[option['key']]['model'])
                    choice['reviewed_model_names'] = sorted(names, key=str.casefold)
                choices.append(choice)
            result['wheel_vehicles'] = sorted(choices,
                key=lambda o: (not o.get('has_report', False), _norm(o['make']), _norm(o['model']), _norm(o['variant'])))
            if selection['query_make']:
                result['wheel_query_make'] = selection['query_make']

    def page(self, *, group=None, studio=None, complete=None, **kwargs):
        if group not in (None, 'family') or studio not in (None, '1') or complete not in (None, '1'):
            raise CatalogError('invalid_filter')
        page, size = kwargs.get('page', 1), kwargs.get('page_size', 24)
        department = kwargs.get('department')
        selection = self._wheel_selection(kwargs, query=kwargs.get('q'), department=department)
        if selection:
            kwargs['_entries'] = (match for entry in self._rows.values()
                if (department is None or entry['public']['department'] == department)
                and (match := self._wheel_index.project(entry, selection)) is not None)
        rows, current = self._page_entries(**kwargs)
        if selection and kwargs.get('sort') is None:
            def vehicle_rank(entry):
                product = entry['public']
                if product['department'] == 'wheels':
                    return 0 if product['fitment']['reports'] else 2
                return 1 if product.get('wheel_fitment_scope') == 'documented_rim_tyre_candidate' else 3
            rows.sort(key=vehicle_rank)
        matches, family_members = [], {}
        matching_rims = set() if selection and department != 'tyres' else None
        for entry in rows:
            product = entry['public']
            if complete == '1' and not (product.get('tyre_seasons') or product.get('vehicle_sizes')):
                continue
            if studio == '1' and not product.get('photo_face'):
                continue
            if matching_rims is not None and product['department'] == 'wheels' and product['fitment']['reports']:
                matching_rims.add(product['id'])
            if group == 'family':
                key = self._family_for.get(product['id'], (product['id'],))
                if key in family_members:
                    family_members[key].append(product)
                    continue
                family_members[key] = [product]
            matches.append(entry)
        result = self._page_result(matches, current, page=page, page_size=size, department=department)
        self._selection_metadata(result, selection, department, matching_rims)
        if group == 'family':
            for product in result['items']:
                members = family_members[self._family_for.get(product['id'], (product['id'],))]
                diameters = {p['geometry'].get('diameter', '') for p in members} - {''}
                product['family'] = {'variant_count': len(members), 'diameters': sorted(diameters, key=lambda n: (len(n), n))}
        return result

    def variants(self, identifier, *, page=1, page_size=24, diameter=None, complete=None, **filters):
        if type(page) is not int or not 1 <= page <= 10000 or type(page_size) is not int or not 1 <= page_size <= 48:
            raise CatalogError('invalid_pagination')
        if complete not in (None, '1') or diameter is not None and (not isinstance(diameter, str)
                or not re.fullmatch(r'(?:[89]|[12][0-9]|30)', diameter)):
            raise CatalogError('invalid_filter')
        self._validate_scope_fields(filters)
        selection = self._wheel_selection(filters, department='wheels')
        if self.get(identifier) is None:
            return None
        ids = self._families.get(self._family_for.get(identifier), [identifier])
        current, rows = self._now(None), []
        for i in ids:
            entry = self._rows[i]
            if selection:
                entry = self._wheel_index.project(entry, selection)
            if entry is None or complete and not (entry['public'].get('tyre_seasons') or entry['public'].get('vehicle_sizes')):
                continue
            if diameter is None or entry['public']['geometry'].get('diameter') == diameter:
                rows.append(self._cached_entry(entry))
        rows.sort(key=lambda entry: self._ranked_order(entry, current))
        offset = (page - 1) * page_size
        return {'items': [self._project(entry, current) for entry in rows[offset:offset + page_size]], 'page': page,
                'page_size': page_size, 'total': len(rows), 'has_more': offset + page_size < len(rows)}

    def compatible_tyres(self, identifier, *, quantity=4, season=None, tyre_size=None, **filters):
        if type(quantity) is not int or not 1 <= quantity <= 4 or season not in (None, 'summer', 'winter', 'allseason'):
            raise CatalogError('invalid_filter')
        rim = self.get(identifier)
        if rim is None or rim['department'] != 'wheels':
            return None
        result = {'rim_id': identifier, 'items': [], 'scope': 'rim_width_candidate',
                  'reason': 'Fahrzeugfreigabe und Reifenausführung sind vor der Bestellung zu prüfen.'}
        # Evidence is exact-ID AND exact-variant bound. A width/diameter match
        # alone never changes the scope to vehicle-approved.
        self._validate_scope_fields(filters)
        selection = self._wheel_selection(filters, department='wheels')
        allowed = None
        if selection:
            records = self._wheel_index.rim_records(identifier, selection)
            result['vehicle_sizes'] = sorted({v['tyre_size'] for record in records if record['option']['key'] == selection['exact']
                                              for v in record['vehicles'] if v.get('tyre_size')})
            allowed = {tid for record in records if record['option']['key'] == selection['exact'] for tid in record['tyres']}
            result['reason'] = 'Für diese Fahrzeugauswahl sind nur im Gutachten der konkreten Felge aufgeführte Reifengrößen berücksichtigt. Fahrzeugpapiere, Traglast, Geschwindigkeitsindex und Auflagen prüfen.'
        current = self._now(None)
        candidates = []
        for tyre_id, canonical_season in self._candidate_tyres.get(identifier, []):
            if (tyre_id in self._checkout_excluded_ids or season and canonical_season != season
                    or allowed is not None and tyre_id not in allowed):
                continue
            entry = self._rows[tyre_id]
            if selection:
                entry = self._wheel_index.project(entry, selection)
            if entry:
                candidates.append(self._cached_entry(entry))
        if self._recommendation_ladder:
            from shop_tyre_recommendations import select_priced_tyres
            try:
                comparison = select_priced_tyres([self._project(entry, current) for entry in candidates],
                    quantity=quantity, now=current, size_key=tyre_size)
            except ValueError:
                raise CatalogError('invalid_filter') from None
            result.update(comparison)
            return result
        if tyre_size is not None:
            raise CatalogError('invalid_filter')
        # Prefer actual merchant offers for the requested quantity. Do not pad
        # a short, priced recommendation list with unpriced enquiry cards.
        # This is display selection only: reported stock may be older than the
        # price, and neither this list nor its dimensional scope permits an order.
        offered = []
        for entry in candidates:
            tyre = self._project(entry, current)
            price = tyre.get('price')
            if (tyre['id'] in self._procurement_ids and tyre['price_status'] == 'current'
                    and price and str(quantity) in price['prices_by_quantity']
                    and price['stock_reported'] >= quantity):
                offered.append(entry)
        if offered:
            candidates = offered
        candidates.sort(key=lambda entry: self._ranked_order(entry, current))
        for entry in candidates:
            tyre = self._project(entry, current)
            result['items'].append(tyre)
            if len(result['items']) == 5:
                break
        return result

    def complete_wheel_price(self, rim_id, tyre_id, *, quantity, runflat=False, disposal=False, **filters):
        """Display the selected quantity at workshop pickup, without a buyer or supplier call."""
        if type(quantity) is not int or not 1 <= quantity <= 4 or type(runflat) is not bool or type(disposal) is not bool:
            raise CatalogError('invalid_set_options')
        self._validate_scope_fields(filters)
        selection = self._wheel_selection(filters, department='wheels')
        if selection and not any(record['option']['key'] == selection['exact'] and tyre_id in record['tyres']
                                 for record in self._wheel_index.rim_records(rim_id, selection)):
            return None
        if not any(candidate == tyre_id for candidate, _ in self._candidate_tyres.get(rim_id, ())):
            return None
        now = self._now(None)
        rim, tyre = self.get(rim_id, now=now), self.get(tyre_id, now=now)
        rim_line, tyre_line = (published_wheel_tier(product, quantity, now) for product in (rim, tyre))
        goods = None
        if rim_line is not None and tyre_line is not None:
            goods = rim_line['items_gross_cent'] + tyre_line['items_gross_cent']
        diameter = tyre['geometry'].get('diameter', '')
        mounting = None
        if diameter == rim['geometry'].get('diameter') and re.fullmatch(r'[0-9]{2}', diameter):
            mounting = tyre_service_gross_cent('mounting', int(diameter), quantity,
                                              {'runflat': runflat, 'disposal': disposal})
        freight = None
        if (self._wheel_pickup_freight is not None and rim_id in self._procurement_ids
                and tyre_id in self._procurement_ids):
            freight = self._wheel_pickup_freight(rim_id, tyre_id, quantity)
        total = goods + mounting + freight if None not in (goods, mounting, freight) else None
        return {'rim_id': rim_id, 'tyre_id': tyre_id, 'quantity': quantity, 'currency': 'EUR',
                'goods_gross_cent': goods, 'mounting_gross_cent': mounting, 'freight_gross_cent': freight,
                'total_gross_cent': total}

    def assembly_proposal(self, rim, tyre, vehicle_ref):
        """Return an exact documented nominal proposal, never vehicle approval.

        Legacy visual matches without source-backed dimensional evidence are
        intentionally excluded from the authorization-before-review route.
        """
        from shop_complete_wheels import valid_proposal
        if not isinstance(rim, Mapping) or not isinstance(tyre, Mapping):
            return None
        rim_id, tyre_id = rim.get('id'), tyre.get('id')
        if (rim_id not in self._source_rows or tyre_id not in self._source_rows
                or (tyre_id, self._rows[tyre_id]['public'].get('tyre_season'))
                    not in self._candidate_tyres.get(rim_id, [])):
            return None
        pairs = self._pairs.get(rim_id, [])
        for pair in pairs[:1000] if isinstance(pairs, list) else ():
            source = pair.get('source') if isinstance(pair, Mapping) else None
            if (not isinstance(pair, Mapping) or pair.get('reviewed') is not True
                    or pair.get('scope') != 'rim_width_candidate' or pair.get('tyre_id') != tyre_id
                    or pair.get('rim_identity') != rim.get('identity_hash')
                    or pair.get('tyre_identity') != tyre.get('identity_hash')
                    or not _text(pair.get('evidence_id'), 200) or not isinstance(source, Mapping)
                    or not isinstance(source.get('sha256'), str) or not re.fullmatch(r'[a-f0-9]{64}', source['sha256'])
                    or type(source.get('page')) is not int or source['page'] < 1):
                continue
            proposal = {'rim_id': rim_id, 'tyre_id': tyre_id,
                'rim_identity_hash': rim['identity_hash'], 'tyre_identity_hash': tyre['identity_hash'],
                'tyre_size': tyre.get('tyre_size'), 'rim_diameter': rim.get('diameter'),
                'evidence_id': pair['evidence_id']}
            if valid_proposal(proposal, rim, tyre):
                return proposal
        return None

    def _source_row(self, identifier):
        # Set by the reviewed release bootstrap; do not construct a source
        # fingerprint from the lossy public projection.
        return self._source_rows[identifier]
