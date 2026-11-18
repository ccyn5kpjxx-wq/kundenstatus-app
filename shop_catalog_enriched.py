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

from shop_catalog_public import PublicCatalog, CatalogError, _text, _fold, variant_fingerprint


def _norm(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split()).casefold()


def _tyre_season(product, source):
    """Use the exact released row's classification, never model-name guesses.

    Full exports carry the season in ``group`` while older detail snapshots
    use ``season``. Keep source geometry/fingerprints unchanged. Conflicting
    explicit classifications cannot satisfy a season filter.
    """
    seasons = {'Sommerreifen': 'summer', 'Winterreifen': 'winter',
               'Ganzjahresreifen': 'allseason', 'all_season': 'allseason',
               'summer': 'summer', 'winter': 'winter', 'allseason': 'allseason'}
    explicit = product.get('geometry', {}).get('season')
    classified = seasons.get(source.get('group'))
    if explicit:
        canonical = seasons.get(explicit)
        return canonical if canonical and classified in (None, canonical) else None
    return classified


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


class EnrichedPublicCatalog(PublicCatalog):
    def __init__(self, products, *, browsing_evidence=None, compatible_pairs=None,
                 procurement_ids=None, product_documents=None, standalone_rims=None,
                 rsu_only_prices=False, preferred_ids=None, shipping_ready_ids=None,
                 checkout_excluded_ids=None, parts_notices=None, **kwargs):
        if type(rsu_only_prices) is not bool:
            raise CatalogError('invalid_price_policy')
        rows = deepcopy(list(products))
        notices = validated_parts_notices(rows, kwargs.get('released_ids'), parts_notices)
        super().__init__(rows, **kwargs)
        if (procurement_ids is not None and (not isinstance(procurement_ids, (set, frozenset, list, tuple))
                or any(not isinstance(i, str) for i in procurement_ids))):
            raise CatalogError('invalid_procurement_ids')
        procurement = frozenset(procurement_ids or ())
        self._rsu_only_prices, self._procurement_ids = rsu_only_prices, procurement
        if (checkout_excluded_ids is not None and
                (not isinstance(checkout_excluded_ids, (set, frozenset))
                 or not checkout_excluded_ids <= self._rows.keys())):
            raise CatalogError('invalid_checkout_excluded_ids')
        # A known unavailable mandatory document removes checkout eligibility,
        # not the actual source identity, procurement mapping or merchant price.
        self._checkout_excluded_ids = frozenset(checkout_excluded_ids or ())
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
        for identifier, entry in self._rows.items():
            if entry['public']['department'] != 'wheels':
                continue
            candidates, seen = [], set()
            pairs = self._pairs.get(identifier, [])
            for pair in pairs[:1000] if isinstance(pairs, list) else ():
                if not isinstance(pair, Mapping) or pair.get('reviewed') is not True:
                    continue
                tyre_id = pair.get('tyre_id')
                if not isinstance(tyre_id, str) or tyre_id not in self._rows or tyre_id in seen:
                    continue
                tyre = self._rows[tyre_id]['public']
                if (tyre['category'] != 'tyre' or pair.get('rim_identity') != identities[identifier]
                        or pair.get('tyre_identity') != identities[tyre_id]):
                    continue
                seen.add(tyre_id)
                candidates.append((tyre_id, _tyre_season(tyre, self._source_rows[tyre_id])))
            self._candidate_tyres[identifier] = candidates
            seasons = {season for _, season in candidates}
            entry['public']['tyre_seasons'] = [season for season in ('summer', 'winter', 'allseason') if season in seasons]

    def _cached_entry(self, entry):
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
        if self._rsu_only_prices and product['id'] not in self._procurement_ids:
            product.update(price=None, price_status='not_offered', price_label='')
        return product

    def _family(self, product, *, complete=False):
        identifiers = self._families.get(self._family_for.get(product['id']), [product['id']])
        if complete:
            identifiers = [i for i in identifiers if self._rows[i]['public'].get('tyre_seasons')]
        diameters = sorted({self._rows[i]['public']['geometry'].get('diameter', '') for i in identifiers} - {''}, key=lambda n: (len(n), n))
        return {'variant_count': len(identifiers), 'diameters': diameters}

    def page(self, *, group=None, studio=None, complete=None, **kwargs):
        if group not in (None, 'family') or studio not in (None, '1') or complete not in (None, '1'):
            raise CatalogError('invalid_filter')
        # Validate/filter/rank once, before either family grouping or pagination.
        # Every cache access is local. A family uses its best matching variant,
        # so a late representative swap cannot leave it on the wrong page.
        page, size = kwargs.get('page', 1), kwargs.get('page_size', 24)
        rows, current = self._page_entries(**kwargs)
        matches, seen = [], set()
        for entry in rows:
            product = entry['public']
            if complete == '1' and not product.get('tyre_seasons'):
                continue
            if studio == '1' and not product.get('photo_face'):
                continue
            if group == 'family':
                key = self._family_for.get(product['id'], (product['id'],))
                if key in seen:
                    continue
                seen.add(key)
            matches.append(entry)
        result = self._page_result(matches, current, page=page, page_size=size)
        if group == 'family':
            for product in result['items']:
                product['family'] = self._family(product, complete=complete == '1')
        return result

    def variants(self, identifier, *, page=1, page_size=24, diameter=None, complete=None):
        if type(page) is not int or not 1 <= page <= 10000 or type(page_size) is not int or not 1 <= page_size <= 48:
            raise CatalogError('invalid_pagination')
        if complete not in (None, '1'):
            raise CatalogError('invalid_filter')
        if self.get(identifier) is None:
            return None
        ids = self._families.get(self._family_for.get(identifier), [identifier])
        if complete == '1':
            ids = [i for i in ids if self._rows[i]['public'].get('tyre_seasons')]
        if diameter is not None:
            import re
            if not isinstance(diameter, str) or not re.fullmatch(r'(?:[89]|[12][0-9]|30)', diameter):
                raise CatalogError('invalid_filter')
            ids = [i for i in ids if self._rows[i]['public']['geometry'].get('diameter') == diameter]
        current = self._now(None)
        rows = [self._cached_entry(self._rows[i]) for i in ids]
        rows.sort(key=lambda entry: self._ranked_order(entry, current))
        offset = (page - 1) * page_size
        return {'items': [self._project(entry, current) for entry in rows[offset:offset + page_size]], 'page': page,
                'page_size': page_size, 'total': len(ids), 'has_more': offset + page_size < len(ids)}

    def compatible_tyres(self, identifier, *, quantity=4, season=None, **filters):
        if type(quantity) is not int or not 1 <= quantity <= 4 or season not in (None, 'summer', 'winter', 'allseason'):
            raise CatalogError('invalid_filter')
        rim = self.get(identifier)
        if rim is None or rim['department'] != 'wheels':
            return None
        result = {'rim_id': identifier, 'items': [], 'scope': 'rim_width_candidate',
                  'reason': 'Fahrzeugfreigabe und Reifenausführung sind vor der Bestellung zu prüfen.'}
        # Evidence is exact-ID AND exact-variant bound. A width/diameter match
        # alone never changes the scope to vehicle-approved.
        # Validate vehicle filter syntax, but never infer a tyre's vehicle
        # approval from a rim-width candidate or filter presence.
        self.page(department='wheels', page_size=1, **filters)
        current = self._now(None)
        candidates = []
        for tyre_id, canonical_season in self._candidate_tyres.get(identifier, []):
            if tyre_id in self._checkout_excluded_ids or season and canonical_season != season:
                continue
            candidates.append(self._cached_entry(self._rows[tyre_id]))
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
                or (tyre_id, _tyre_season(self._rows[tyre_id]['public'], self._source_rows[tyre_id]))
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
