"""Offline exact rim source evidence; no IO, prices, fitment or activation.

The caller must independently review and approve IDs. Self-consistent hashes
are not supplier authentication. Native source products never inherit ALZURA
procurement, a vehicle approval or a historic CSV price/stock observation.
"""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import re
from collections import defaultdict

from shop_catalog_public import variant_fingerprint
from shop_supplier_catalog import RIM_FIELDS, _ean, _fold, _geometry_valid, _number, parse_rsu_rim_profile, PROFILE

CURRENT_RULE = 'exact_unique_ean_full_geometry_reviewed_trade_names_v1'
NATIVE_RULE = 'native_rsu_csv_full_rim_identity_reviewed_model_photo_v1'
RULES = frozenset({CURRENT_RULE, NATIVE_RULE})
HASH = re.compile(r'[a-f0-9]{64}\Z')
PID = re.compile(r'(alloy_wheel|steel_wheel):[a-f0-9]{24}\Z')
ARTICLE = re.compile(r'[1-9][0-9]{0,17}\Z')
PHOTO = re.compile(r'bilder/([a-f0-9]{64}\.(?:jpg|jpeg|png|webp))\Z')
RECORD_FIELDS = frozenset({'Artikelnummer','ean','Hersteller','Profil','Herstellerartikelnummer',
                         'Artikeltyp','Artikeluntertyp','Dot','Demo','Da'})
WIDTH_TABLE_SHA='61023026c64e09276eea492733cd2462e32c7a2a77a80949bc100540020ee4a9'
MAX_CANDIDATE_BYTES=48*1024*1024


class RimEvidenceError(ValueError):
    def __init__(self, code='invalid_rim_evidence'):
        self.code = code
        super().__init__(code)


def _hash(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _text(value, maximum=500):
    return (isinstance(value,str) and 1 <= len(value) <= maximum
        and all(ord(c) >= 32 and ord(c) != 127 for c in value))


def normalize_rim_geometry(value):
    if not isinstance(value,Mapping) or set(value) != RIM_FIELDS:
        raise RimEvidenceError('invalid_rim_geometry')
    normalized = {k:_number(v) for k,v in value.items()}
    if not _geometry_valid(normalized):
        raise RimEvidenceError('invalid_rim_geometry')
    return normalized


def _trade(value):
    if not _text(value):
        raise RimEvidenceError()
    for _ in range(2):
        if not any(marker in value for marker in ('Ã','Â')):
            break
        for encoding in ('cp1252','latin1'):
            try:
                fixed = value.encode(encoding).decode('utf-8')
            except (UnicodeError,LookupError):
                continue
            if sum(fixed.count(s) for s in ('Ã','Â')) < sum(value.count(s) for s in ('Ã','Â')):
                value = fixed
                break
        else:
            break
    return _fold(value)


# Reviewed spelling pairs are narrow. EAN, exact geometry, immutable source
# references and approved_ids are still required for every individual variant.
_BRANDS = {('aez leichtmetallräder','aez'), ('brock / rc','brock')}
_DESIGNS = {
    ('tr silver - silber','tr silver'), ('c27 gg titan glanz','c27 titan gloss'),
    ('c27 sr racing silber','c27 racing silver'),
    ('porto dark - gunmetal polished','porto gunmetal polished'),
    ('toronto black - schwarz glänzend','toronto black'),
    ('vidron (vn) schwarz','typ vn - vidron schwarz'),
    ('vidron (vn) brillantsilber','typ vn - vidron brillantsilber'),
    ('vidron (vn) schwarz poliert','typ vn - vidron schwarz poliert'),
    ('b24 schwarz glanz voll poliert (sgvp)','b24 schwarz front-poliert'),
    ('b24 schwarz glanz (sg)','b24 schwarz lackiert'),
    ('b26 schwarz matt voll poliert (smvp)','b26 schwarz-matt front-poliert'),
}


def _source(proof, category, expected_ean):
    record = proof['rsu_record']
    if (not isinstance(record,Mapping) or set(record) != RECORD_FIELDS
            or any(not isinstance(v,str) or len(v)>500 or any(ord(c)<32 or ord(c)==127 for c in v)
                   for v in record.values())
            or not ARTICLE.fullmatch(record['Artikelnummer'])
            or not _ean(record['ean']) or record['ean'] != expected_ean
            or record['Artikeltyp'] != 'Felge'
            or record['Artikeluntertyp'] != ('Alu' if category=='alloy_wheel' else 'Stahl')
            or any(record[k] for k in ('Dot','Demo','Da'))
            or not _text(record['Hersteller']) or not _text(record['Herstellerartikelnummer'])
            or not isinstance(proof['rsu_source_sha256'],str) or not HASH.fullmatch(proof['rsu_source_sha256'])
            or type(proof['rsu_row_number']) is not int or not 2 <= proof['rsu_row_number'] <= 300_001
            or proof['rsu_record_fingerprint'] != _hash(record)):
        raise RimEvidenceError()
    parsed = parse_rsu_rim_profile(record['Profil'])
    if not parsed:
        raise RimEvidenceError()
    return record, parsed


def _current(proof, product_id, category, expected_ean, identity_hash):
    required = {'rule','product_id','identity_hash','alzura_source_sha256','alzura','primary_sources',
        'source_reference','source_reference_equals_rsu_mpn','rsu_source_sha256','rsu_row_number',
        'rsu_record','rsu_record_fingerprint','rsu_geometry','unique_rsu_ean','reviewed_exact_ean_and_geometry',
        'manufacturer_text_equal','description_text_equal','vehicle_fitment_confirmed','procurement_enabled'}
    if (set(proof)!=required or proof['product_id']!=product_id or proof['identity_hash']!=identity_hash
            or not isinstance(proof['alzura_source_sha256'],str) or not HASH.fullmatch(proof['alzura_source_sha256'])
            or proof['unique_rsu_ean'] is not True or proof['reviewed_exact_ean_and_geometry'] is not True
            or proof['vehicle_fitment_confirmed'] is not False or proof['procurement_enabled'] is not False):
        raise RimEvidenceError()
    record, parsed = _source(proof,category,expected_ean)
    identity = proof['alzura']
    if (not isinstance(identity,Mapping) or set(identity) != {'product_id','category','manufacturer','description','ean','geometry','manufacturer_article_number'}
            or identity['product_id']!=product_id or identity['category']!=category or identity['ean']!=expected_ean
            or identity['manufacturer_article_number'] is not None
               and _trade(identity['manufacturer_article_number']) != _trade(record['Herstellerartikelnummer'])
            or normalize_rim_geometry(identity['geometry'])!=parsed['geometry']
            or normalize_rim_geometry(proof['rsu_geometry'])!=parsed['geometry']):
        raise RimEvidenceError()
    brands = (_trade(identity['manufacturer']),_trade(record['Hersteller']))
    designs = (_trade(identity['description']),_trade(parsed['description']))
    if (brands[0]!=brands[1] and brands not in _BRANDS
            or designs[0]!=designs[1] and designs not in _DESIGNS
            or proof['manufacturer_text_equal'] is not (identity['manufacturer']==_fold(record['Hersteller']))
            or proof['description_text_equal'] is not (identity['description']==parsed['description'])
            or proof['source_reference_equals_rsu_mpn'] is not
               (bool(proof['source_reference']) and proof['source_reference']==record['Herstellerartikelnummer'])):
        raise RimEvidenceError()
    sources = proof['primary_sources']
    if not isinstance(sources,list) or not 1 <= len(sources) <= 10:
        raise RimEvidenceError()
    original_details = False
    for source in sources:
        if (not isinstance(source,Mapping) or set(source)!={'provider','area','operation','run','body_file','body_sha256','local_original_verified'}
                or source['provider']!='ALZURA' or source['area']!='alloys'
                or not isinstance(source['body_sha256'],str) or not HASH.fullmatch(source['body_sha256'])
                or source['local_original_verified'] is not True
                or any(not _text(source[k],180) or '/' in source[k] or '\\' in source[k]
                       or '..' in source[k] for k in ('run','body_file'))):
            raise RimEvidenceError()
        original_details |= source['operation']=='details'
    if not original_details:
        raise RimEvidenceError()
    return record,parsed,{'alzura_source_sha256':proof['alzura_source_sha256']}


def _native(proof,product_id,category,expected_ean,identity_hash):
    if set(proof)!={'rule','product','identity_hash','rsu_source_sha256','rsu_row_number','rsu_record',
                   'rsu_record_fingerprint','photo_family','photo_proof_id'}:
        raise RimEvidenceError()
    record,parsed = _source(proof,category,expected_ean)
    expected_id = category+':'+sha256(('RSU:'+record['Artikelnummer']).encode()).hexdigest()[:24]
    product,photo = proof['product'],proof['photo_family']
    fields={'selection_id','category','department','name','manufacturer_name','article_number','width','diameter',
        'offset','bolt_count','pcd','centre_bore','ratio','size','dimensions_valid','photo','gallery','caption',
        'details','identifiers','price','quote_public','price_status','fitment_status','approval','technical_reports','order_enabled'}
    g=parsed['geometry']
    details={'EAN':expected_ean,'Herstellerartikelnummer':record['Herstellerartikelnummer'],
        'Einpresstiefe':g['offset']+' mm','Lochkreis':g['pcd'],'Lochzahl':g['bolt_count'],
        'Mittenloch':g['centre_bore'],'Fahrzeugpassung':'Für das konkrete Fahrzeug gesondert zu prüfen'}
    if isinstance(photo,Mapping) and 'finish_label' in photo:
        if not _text(photo['finish_label'],140):raise RimEvidenceError()
        details['Farbe']=photo['finish_label']
    if (not isinstance(product,Mapping) or set(product)!=fields or product_id!=expected_id
            or product['selection_id']!=product_id or product['category']!=category or product['department']!='wheels'
            or proof['identity_hash']!=identity_hash or variant_fingerprint(product)!=identity_hash
            or normalize_rim_geometry({k:product[k] for k in RIM_FIELDS})!=parsed['geometry']
            or product['manufacturer_name']!=record['Hersteller'] or product['article_number']!=record['Herstellerartikelnummer']
            or product['name']!=record['Hersteller']+' '+PROFILE.fullmatch(record['Profil'])['description']
            or product['details']!=details or product['ratio'] is not None
            or product['size']!=f"{g['width']} × {g['diameter']} Zoll · ET {g['offset']} · {g['bolt_count']}×{g['pcd']}"
            or product['caption']!='Modellfoto – genaue Größe und Ausführung der Auswahl beachten.'
            or product['identifiers']!={'provider':'RSU','provider_id':record['Artikelnummer'],
                'ean':expected_ean,'manufacturer_article_number':record['Herstellerartikelnummer']}
            or product['price'] is not None or product['quote_public'] is not None
            or product['approval'] is not None or product['technical_reports']!=[]
            or product['order_enabled'] is not False or product['fitment_status']!='not_confirmed'
            or product['price_status']!='update_required' or product['dimensions_valid'] is not True):
        raise RimEvidenceError()
    photo_fields={'rule','manufacturer','description','photo','image_url','source_bundle_sha256',
                  'source_products','model_photo_only','sku_geometry_or_vehicle_fitment_not_inferred'}
    if (not isinstance(photo,Mapping) or set(photo) not in (photo_fields,photo_fields|{'finish_label'})
            or photo['rule']!='exact_folded_manufacturer_full_design_finish_v1'
            or photo['manufacturer']!=_fold(record['Hersteller']) or photo['description']!=parsed['description']
            or not isinstance(photo['source_bundle_sha256'],str) or not HASH.fullmatch(photo['source_bundle_sha256'])
            or photo['model_photo_only'] is not True or photo['sku_geometry_or_vehicle_fitment_not_inferred'] is not True
            or not isinstance(product['photo'],str) or not PHOTO.fullmatch(product['photo'])
            or photo['photo']!=product['photo'] or photo['image_url']!='/shop/media/'+PHOTO.fullmatch(product['photo'])[1]
            or proof['photo_proof_id']!=_hash({'manufacturer':photo['manufacturer'],'description':photo['description']})
            or not isinstance(product['gallery'],list) or len(product['gallery'])>12
            or any(not isinstance(p,str) or not PHOTO.fullmatch(p) for p in product['gallery'])):
        raise RimEvidenceError()
    sources=photo['source_products']
    if not isinstance(sources,list) or not 1 <= len(sources) <= 1000:
        raise RimEvidenceError()
    for source in sources:
        if (not isinstance(source,Mapping) or set(source)!={'product_id','identity_hash','name','manufacturer'}
                or not isinstance(source['product_id'],str) or not PID.fullmatch(source['product_id'])
                or not isinstance(source['identity_hash'],str) or not HASH.fullmatch(source['identity_hash'])
                or _fold(source['manufacturer'])!=photo['manufacturer']
                or _fold(source['name'])!=photo['manufacturer']+' '+photo['description']):
            raise RimEvidenceError()
    return record,parsed,{'photo_source_bundle_sha256':photo['source_bundle_sha256'],'model_photo_only':True}


def validate_expansion_mapping(candidate, *, product_id, category, expected_ean, identity_hash):
    try:
        if (not isinstance(product_id,str) or not PID.fullmatch(product_id) or not product_id.startswith(category+':')
                or category not in {'alloy_wheel','steel_wheel'} or not _ean(expected_ean)
                or not isinstance(identity_hash,str) or not HASH.fullmatch(identity_hash)
                or not isinstance(candidate,Mapping) or set(candidate)!={'status','confirmed','evidence_id','evidence'}
                or candidate['confirmed'] is not False or candidate['status']!='reviewed_rim_source_candidate'
                or not isinstance(candidate['evidence'],Mapping)
                or candidate['evidence_id']!=_hash(candidate['evidence'])):
            raise RimEvidenceError()
        proof=candidate['evidence']
        check={CURRENT_RULE:_current,NATIVE_RULE:_native}.get(proof.get('rule'))
        if check is None:raise RimEvidenceError()
        record,parsed,extra=check(proof,product_id,category,expected_ean,identity_hash)
        return {'supplier_article_id':record['Artikelnummer'],'mapping_evidence_id':candidate['evidence_id'],
            'rsu_source_sha256':proof['rsu_source_sha256'],'rim_geometry':deepcopy(parsed['geometry']),**extra}
    except Exception:
        raise RimEvidenceError() from None


def _mapping(proof):
    return {'status':'reviewed_rim_source_candidate','confirmed':False,'evidence_id':_hash(proof),'evidence':deepcopy(proof)}


def derive_width_pairs(bundle, new_rims, *, width_registry=None):
    """Reuse independently reviewed nominal-width facts, never vehicle fitment.

    An exact original tyre fingerprint and manufacturer-table source are
    mandatory. Optional table rows permit only their listed rim widths for an
    already reviewed tyre/size/page. No new tyre or vehicle evidence is invented.
    """
    by={p.get('selection_id',p.get('id')):p for p in bundle['products']}
    fingerprints={pid:variant_fingerprint(product) for pid,product in by.items()}
    table=None
    if width_registry is not None:
        if (not isinstance(width_registry,Mapping)
                or width_registry.get('scope')!='continental_group_dimensional_proposals_only'
                or width_registry.get('is_vehicle_approval') is not False or width_registry.get('construction')!='R'
                or width_registry.get('source',{}).get('sha256')!=WIDTH_TABLE_SHA
                or not isinstance(width_registry.get('rows'),list) or len(width_registry['rows'])>1000):
            raise RimEvidenceError('invalid_width_table')
        table=width_registry
    pool=defaultdict(dict)
    for rid,pairs in bundle.get('compatible_pairs',{}).items():
        rim=by.get(rid)
        if not rim or rim.get('category') not in {'alloy_wheel','steel_wheel'} or not isinstance(pairs,list):continue
        old_width,diameter=_number(rim.get('width')),_number(rim.get('diameter'))
        if old_width is None or diameter is None:continue
        for pair in pairs:
            if not isinstance(pair,Mapping):continue
            tyre=by.get(pair.get('tyre_id'));source=pair.get('source')
            if (not tyre or tyre.get('category')!='tyre' or pair.get('reviewed') is not True
                    or pair.get('scope')!='rim_width_candidate' or pair.get('rim_identity')!=fingerprints[rid]
                    or pair.get('tyre_identity')!=fingerprints[pair['tyre_id']]
                    or not isinstance(source,Mapping) or source.get('sha256')!=WIDTH_TABLE_SHA
                    or not isinstance(source.get('url'),str) or not source['url'].startswith('https://www.continental-reifen.de/')
                    or type(source.get('page')) is not int or _number(tyre.get('diameter'))!=diameter):continue
            widths={old_width}
            if table is not None:
                widths=set()
                for row in table['rows']:
                    if (not isinstance(row,Mapping) or row.get('visually_reviewed') is not True
                            or source['url']!=table['source'].get('url') or source['page']!=row.get('pdf_page')
                            or _number(row.get('width_mm'))!=_number(tyre.get('width'))
                            or _number(row.get('ratio_percent'))!=_number(tyre.get('ratio'))
                            or _number(row.get('diameter_in'))!=diameter
                            or not isinstance(row.get('permitted_rim_widths_in'),list)):
                        continue
                    allowed={_number(w) for w in row['permitted_rim_widths_in']}
                    if old_width in allowed:
                        widths |= allowed-{None}
            for width in widths:
                pool[(width,diameter)][pair['tyre_id']]=pair
    result={}
    for rim in new_rims:
        pid=rim.get('selection_id',rim.get('id'));fingerprint=variant_fingerprint(rim)
        candidates=pool.get((_number(rim.get('width')),_number(rim.get('diameter'))),{})
        pairs=[]
        seasons={'Sommerreifen':'summer','Winterreifen':'winter','Ganzjahresreifen':'allseason',
                 'summer':'summer','winter':'winter','allseason':'allseason','all_season':'allseason'}
        selected=defaultdict(list)
        def priority(item):
            tid,_=item
            return (tid not in bundle.get('rsu_bindings',{}),str(by[tid].get('name','')),tid)
        for tid,original in sorted(candidates.items(),key=priority):
            tyre=by[tid]
            season=seasons.get(tyre.get('season')) or seasons.get(tyre.get('group'))
            if season is None or len(selected[season])>=5:continue
            selected[season].append((tid,original))
        for tid,original in [item for season in ('summer','winter','allseason') for item in selected[season]]:
            pair={'reviewed':True,'tyre_id':tid,'rim_identity':fingerprint,
                'tyre_identity':original['tyre_identity'],'scope':'rim_width_candidate',
                'evidence_id':'rim-width:'+_hash({'rim':pid,'rim_identity':fingerprint,
                    'tyre':tid,'tyre_identity':original['tyre_identity'],
                    'width':_number(rim.get('width')),'diameter':_number(rim.get('diameter')),
                    'source_evidence':original['evidence_id']})[:24], 'source':deepcopy(original['source'])}
            pairs.append(pair)
        if pairs:result[pid]=pairs
    return result


def prepare_expansion_bundle(bundle, patch, *, source_bundle_sha256, approved_current_ids, include_natives=False,
                             width_registry=None):
    """Pure candidate preparation. No private response prices/cache/quotas copied."""
    try:
        if (not isinstance(bundle,Mapping) or not isinstance(patch,Mapping)
                or not isinstance(approved_current_ids,(set,frozenset)) or type(include_natives) is not bool
                or not isinstance(source_bundle_sha256,str) or not HASH.fullmatch(source_bundle_sha256)
                or not any(isinstance(v,Mapping) and str(v.get('file','')).replace('\\','/').endswith('/bundle.json')
                    and v.get('sha256')==source_bundle_sha256 for v in patch['immutable_sources'])):
            raise RimEvidenceError('source_bundle_mismatch')
        out=deepcopy(dict(bundle)); products=out['products']; by={p.get('selection_id',p.get('id')):p for p in products}
        candidates={c['product_id']:c for c in patch['exact_current_id_candidates']}
        if not approved_current_ids <= candidates.keys() or len(by)!=len(products):
            raise RimEvidenceError('invalid_approved_ids')
        geometry=out.setdefault('rsu_rim_geometry',{}); bindings=out['rsu_bindings']; classes=out['shipping_classes']
        report={'current_ids_prepared':0,'native_added':0,'native_existing':0,'native_aliases':{}}
        new_rims=[]

        def add(product, proof):
            pid=product.get('selection_id',product.get('id')); identity=variant_fingerprint(product)
            mapping=_mapping(proof); category=product['category']; expected_ean=proof['rsu_record']['ean']
            verified=validate_expansion_mapping(mapping,product_id=pid,category=category,
                expected_ean=expected_ean,identity_hash=identity)
            bindings[pid]={'identity_hash':identity,'category':category,'mapping':mapping,'expected_ean':expected_ean}
            classes[pid]='pkw_rim'
            geometry[pid]={'variant_fingerprint':identity,'geometry':verified['rim_geometry'],
                'supplier':'RSU','supplier_article_id':verified['supplier_article_id'],
                'source_sha256':verified['rsu_source_sha256'],'evidence_id':mapping['evidence_id']}

        for pid in sorted(approved_current_ids):
            candidate=candidates[pid]; product=by[pid]
            if candidate['identity_hash']!=variant_fingerprint(product) or candidate['proof_sha256']!=_hash(candidate['proof']):
                raise RimEvidenceError('product_identity_changed')
            add(product,candidate['proof']);report['current_ids_prepared']+=1
        from shop_rsu_quotes import _binding
        existing={}
        for pid,binding in bindings.items():
            article=_binding(pid,binding)['supplier_article_id']
            if article in existing:
                raise RimEvidenceError('duplicate_supplier_identity')
            existing[article]=pid
        if include_natives:
            # A supplier SKU has one public purchase identity. Preserve existing
            # canonical IDs instead of silently replacing customer selections.
            for candidate in patch['native_rsu_candidates']:
                product=deepcopy(candidate['product']);pid=product['selection_id'];article=candidate['supplier_article_id']
                if article in existing:
                    report['native_existing']+=1;report['native_aliases'][pid]=existing[article];continue
                photo=deepcopy(patch['photo_family_proofs'][candidate['photo_proof_id']])
                if photo['source_bundle_sha256']!=source_bundle_sha256:
                    raise RimEvidenceError('photo_source_changed')
                for source in photo['source_products']:
                    original=by[source['product_id']]
                    if (source['identity_hash']!=variant_fingerprint(original) or original['photo']!=photo['photo']
                            or out['publication']['image_urls'].get(photo['photo'])!=photo['image_url']):
                        raise RimEvidenceError('photo_source_changed')
                colors={by[s['product_id']].get('details',{}).get('Farbe') for s in photo['source_products']}
                if len(colors)==1 and None not in colors and '' not in colors:
                    color=next(iter(colors))
                    if not _text(color,140):raise RimEvidenceError('photo_source_changed')
                    product['details']['Farbe']=color;photo['finish_label']=color
                fingerprint=variant_fingerprint(product)
                proof={'rule':NATIVE_RULE,'product':product,'identity_hash':fingerprint,
                    'rsu_source_sha256':candidate['source_sha256'],'rsu_row_number':candidate['source_row_number'],
                    'rsu_record':candidate['source_record'],'rsu_record_fingerprint':candidate['source_record_fingerprint'],
                    'photo_family':photo,'photo_proof_id':candidate['photo_proof_id']}
                if pid in by:raise RimEvidenceError('duplicate_product_identity')
                add(product,proof); products.append(deepcopy(product));by[pid]=products[-1]
                new_rims.append(products[-1])
                out['publication']['released_ids'].append(pid)
                out['browsing_evidence'][pid]={'variant_fingerprint':fingerprint,
                    'gallery':[out['publication']['image_urls'][p] for p in product['gallery']
                               if p in out['publication']['image_urls']]}
                existing[article]=pid;report['native_added']+=1
            derived=derive_width_pairs(bundle,new_rims,width_registry=width_registry)
            out['compatible_pairs'].update(derived)
            report.update(native_width_candidate_sets=len(derived),
                native_width_candidate_pairs=sum(map(len,derived.values())),
                native_without_reviewed_width_candidates=len(new_rims)-len(derived))
        report.update(products=len(products),bindings=len(bindings),rim_geometry=len(geometry),
            candidate_json_bytes=len(json.dumps(out,ensure_ascii=False,separators=(',',':')).encode()))
        if report['candidate_json_bytes']>MAX_CANDIDATE_BYTES:
            error=RimEvidenceError('candidate_bundle_too_large')
            error.preparation_report=report
            raise error
        return {'bundle':out,'report':report}
    except RimEvidenceError:
        raise
    except Exception:
        raise RimEvidenceError('invalid_expansion_input') from None
