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


class EnrichedPublicCatalog(PublicCatalog):
    def __init__(self, products, *, browsing_evidence=None, compatible_pairs=None,
                 procurement_ids=None, product_documents=None, standalone_rims=None,
                 rsu_only_prices=False, preferred_ids=None, **kwargs):
        if type(rsu_only_prices) is not bool:
            raise CatalogError('invalid_price_policy')
        rows = deepcopy(list(products))
        super().__init__(rows, **kwargs)
        if (procurement_ids is not None and (not isinstance(procurement_ids, (set, frozenset, list, tuple))
                or any(not isinstance(i, str) for i in procurement_ids))):
            raise CatalogError('invalid_procurement_ids')
        procurement = frozenset(procurement_ids or ())
        self._rsu_only_prices, self._procurement_ids = rsu_only_prices, procurement
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
            product['purchase_status'] = 'supplier_check_required' if identifier in procurement else 'inquiry_only'
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
        return (entry['public']['id'] not in self._preferred_ids, *super()._browse_order(entry))

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
        # Base validation and filtering run before grouping. Bounded catalogue
        # iteration is local only, never a provider crawl.
        page, size = kwargs.get('page', 1), kwargs.get('page_size', 24)
        result = super().page(**kwargs)
        if group is None and studio is None and complete is None:
            return result
        matches, seen = [], {}
        scan = {**kwargs, 'page_size': 48}
        for number in range(1, (result['total'] + 47) // 48 + 1):
            scan['page'] = number
            for product in super().page(**scan)['items']:
                if complete == '1' and not product.get('tyre_seasons'):
                    continue
                if studio == '1' and not product.get('photo_face'):
                    continue
                key = self._family_for.get(product['id'], (product['id'],))
                if group == 'family':
                    if key in seen:
                        # Show the actual RSU variant of a design when a sparse
                        # display-only ALZURA row sorts first. Keep the exact
                        # filtered variant's ID, price, image and geometry.
                        old = matches[seen[key]]
                        preferred = product['id'] in self._preferred_ids
                        old_preferred = old['id'] in self._preferred_ids
                        if (preferred and not old_preferred or preferred == old_preferred
                                and product.get('sale_dimensions') and not old.get('sale_dimensions')):
                            product['family'] = self._family(product, complete=complete == '1')
                            matches[seen[key]] = product
                        continue
                    seen[key] = len(matches)
                    product['family'] = self._family(product, complete=complete == '1')
                matches.append(product)
        offset = (page - 1) * size
        return {**result, 'items': matches[offset:offset + size], 'total': len(matches), 'has_more': offset + size < len(matches)}

    def variants(self, identifier, *, page=1, page_size=24, diameter=None, complete=None):
        if type(page) is not int or not 1 <= page <= 10000 or type(page_size) is not int or not 1 <= page_size <= 48:
            raise CatalogError('invalid_pagination')
        if complete not in (None, '1'):
            raise CatalogError('invalid_filter')
        if self.get(identifier) is None:
            return None
        ids = sorted(self._families.get(self._family_for.get(identifier), [identifier]),
                     key=lambda pid: (pid not in self._preferred_ids, not bool(self._rows[pid]['public'].get('sale_dimensions')), pid))
        if complete == '1':
            ids = [i for i in ids if self._rows[i]['public'].get('tyre_seasons')]
        if diameter is not None:
            import re
            if not isinstance(diameter, str) or not re.fullmatch(r'(?:[89]|[12][0-9]|30)', diameter):
                raise CatalogError('invalid_filter')
            ids = [i for i in ids if self._rows[i]['public']['geometry'].get('diameter') == diameter]
        offset = (page - 1) * page_size
        return {'items': [self.get(i) for i in ids[offset:offset + page_size]], 'page': page,
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
        candidates = sorted(self._candidate_tyres.get(identifier, []), key=lambda item: item[0] not in self._preferred_ids)
        for tyre_id, canonical_season in candidates:
            if season and canonical_season != season:
                continue
            tyre = self.get(tyre_id)
            if tyre is None:
                continue
            result['items'].append(tyre)
            if len(result['items']) == 5:
                break
        return result

    def _source_row(self, identifier):
        # Set by the reviewed release bootstrap; do not construct a source
        # fingerprint from the lossy public projection.
        return self._source_rows[identifier]
