"""Explicit, original-source-bound parts release; no provider or database IO."""
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import csv
import io
import json
import os
from pathlib import Path
import re
import stat

from shop_catalog_public import variant_fingerprint
from shop_cart_quotes import retail_unit_gross_cent
from shop_rsu_parts_evidence import CATEGORIES, parts_source_record, validate_parts_mapping
from shop_rsu_parts_source import dom_product, retained_inquiries

HASH=re.compile(r'[a-f0-9]{64}')
NAME=re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
MAX_FILE=64*1024*1024


class PartsReleaseError(ValueError):
    pass


def need(value,code='invalid_parts_release'):
    if not value:raise PartsReleaseError(code)


def decode(raw):
    def unique(items):
        value={}
        for key,item in items:
            need(key not in value,'duplicate_parts_release_key');value[key]=item
        return value
    try:return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
    except PartsReleaseError:raise
    except Exception:raise PartsReleaseError('invalid_parts_release_json') from None


def read(path,maximum):
    try:
        need(path.is_absolute() and not any(p.is_symlink() for p in (path,*path.parents)),'unsafe_parts_release_path')
        fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            meta=os.fstat(fd);need(stat.S_ISREG(meta.st_mode) and 0<meta.st_size<=maximum,'parts_release_size')
            with os.fdopen(fd,'rb') as stream:fd=None;raw=stream.read(maximum+1)
        finally:
            if fd is not None:os.close(fd)
        need(0<len(raw)<=maximum,'parts_release_size');return raw
    except PartsReleaseError:raise
    except Exception:raise PartsReleaseError('parts_release_file_unavailable') from None


def _observation(raw,number):
    need(type(number) is int and 1<=number<=300000)
    if raw.lstrip().startswith(b'{'):
        value=decode(raw)
        need(isinstance(value,dict) and value.get('supplier')=='RSU' and value.get('currency')=='EUR'
             and value.get('price_basis')=='net' and isinstance(value.get('records'),list)
             and len(value['records'])<=300000 and number<=len(value['records']))
        row=value['records'][number-1];need(isinstance(row,dict))
        return row
    try:
        reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')),delimiter=';')
        need(reader.fieldnames is not None and {'Artikelnummer','Hersteller','Herstellerartikelnummer','ean'}<=set(reader.fieldnames))
        for index,row in enumerate(reader,1):
            need(index<=300000 and None not in row)
            if index==number:
                stock=row.get('Lagerbestand','')
                return {'article_id':row['Artikelnummer'],'manufacturer':row['Hersteller'],
                    'mpn':row['Herstellerartikelnummer'],'ean':row['ean'] or None,
                    'unit_net':row.get('Einkaufspreis'),'stock':int(stock) if re.fullmatch(r'[0-9]{1,9}',stock) else None}
    except PartsReleaseError:raise
    except Exception:raise PartsReleaseError('invalid_parts_source') from None
    raise PartsReleaseError('parts_source_row_missing')


def _stamp(value):
    need(isinstance(value,str) and len(value)<=40)
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    need(result.tzinfo is not None and result.utcoffset() is not None)
    return result.astimezone(timezone.utc)


def _originals(sources):
    views, inquiries, visits = {}, [], {}
    for digest, raw in sources.items():
        if raw.lstrip().startswith(b'- '):
            views[digest] = dom_product(raw)
        elif raw.lstrip().startswith(b'{'):
            value = decode(raw)
            if isinstance(value, dict) and value.get('supplier') == 'RSU' and value.get('mode') == 'live':
                inquiries.append((digest, retained_inquiries(value)))
            if isinstance(value, dict) and 'captures' in value:
                need(type(value.get('schema_version')) is int and value['schema_version'] == 1
                     and isinstance(value['captures'], list) and len(value['captures']) <= 20, 'parts_capture_manifest_invalid')
                for capture in value['captures']:
                    need(isinstance(capture, dict) and set(capture) == {'file','sha256','observed_at'}
                         and isinstance(capture['file'], str) and isinstance(capture['sha256'], str)
                         and HASH.fullmatch(capture['sha256']) and capture['sha256'] in sources,
                         'parts_capture_receipt_invalid')
                    visits.setdefault(capture['sha256'], set()).add(_stamp(capture['observed_at']))
    return views, inquiries, visits


def _genuine_observation(row, supplier, views, inquiries):
    # A normalized row cannot authenticate itself: rebuild its maker/MPN/EAN
    # from an original DOM and its price/stock/time from a separate original GET.
    identity = supplier['identity']
    viewed = any(parts_source_record('rsu', source_sha256=digest, row_number=1,
        article_id=ui['article_id'], manufacturer=ui['manufacturer'], mpn=ui['mpn'], ean=ui['ean'])['identity'] == identity
        and ui['article_id'] == row.get('article_id') for digest, ui in views.items())
    need(viewed, 'parts_original_identity_missing')
    matches = [obs for _, records in inquiries for obs in records.values()
        if obs['supplier_article_id'] == row.get('article_id') and obs['ean'] == row.get('ean')
        and row.get('quantity') == 1 and row.get('stock') == obs['stock']
        and Decimal(str(row.get('unit_net'))) == Decimal(obs['unit_net'])
        and _stamp(row.get('observed_at')) == _stamp(obs['retrieved_at'])]
    need(matches, 'parts_original_inquiry_mismatch')


def _genuine_freight(proof, supplier, views, inquiries, visits):
    ui = views.get(proof['source_ref']['sha256'])
    need(ui is not None and ui['quantity'] == 1 and ui['article_id'] == supplier['article_id']
         and parts_source_record('rsu', source_sha256=proof['source_ref']['sha256'], row_number=1,
             article_id=ui['article_id'], manufacturer=ui['manufacturer'], mpn=ui['mpn'], ean=ui['ean'])['identity'] == supplier['identity'],
         'parts_freight_original_identity_mismatch')
    if proof['rule'] == 'merchant_fixed_parts_freight_v1':
        need(all(offer['freight_net'] is not None and Decimal(offer['freight_net']) == Decimal('6.00')
                 for offer in ui['offers']), 'parts_fixed_freight_not_standard')
        return
    need(proof['rule'] == 'rsu_ui_unique_q1_freight_v1' and len(ui['offers']) == 1, 'parts_freight_original_ambiguous')
    need(_stamp(proof['observed_at']) in visits.get(proof['source_ref']['sha256'], set()),
         'parts_freight_original_visit_missing')
    offer = ui['offers'][0]
    need(offer['freight_net'] is not None and offer['stock'] == proof['stock_reported']
         and Decimal(offer['unit_net']) == Decimal(proof['unit_net'])
         and Decimal(offer['freight_net']) == Decimal(proof['freight_net']), 'parts_freight_original_offer_mismatch')
    records = dict(inquiries).get(proof['inquiry_ref']['sha256'], {})
    obs = records.get(supplier['article_id'])
    need(obs is not None and obs['ean'] == proof['ean'] and obs['stock'] == proof['stock_reported']
         and Decimal(obs['unit_net']) == Decimal(proof['unit_net'])
         and _stamp(obs['retrieved_at']) == _stamp(proof['inquiry_observed_at']), 'parts_freight_original_inquiry_mismatch')


def load_parts_release(root,record,bundle,*,base_bundle_sha256):
    """Return an in-memory delta only after every pinned source row is checked.

    Absence is the unchanged legacy path. Candidate-only/empty overlays cannot
    activate purchasing. Parts shipping proofs remain separate from the old
    category tariffs, and their independent runtime verifier owns freshness.
    """
    if record is None:return bundle,{}
    try:
        need(isinstance(record,Mapping) and set(record)=={'path','sha256','base_bundle_sha256'}
             and isinstance(record['path'],str) and NAME.fullmatch(record['path'])
             and isinstance(record['sha256'],str) and HASH.fullmatch(record['sha256'])
             and record['base_bundle_sha256']==base_bundle_sha256)
        root=Path(root);need(root.is_absolute() and root.resolve()==root and root.is_dir())
        raw=read(root/record['path'],2*1024*1024);need(sha256(raw).hexdigest()==record['sha256'],'parts_release_hash_mismatch')
        release=decode(raw)
        need(isinstance(release,dict) and set(release)=={'schema_version','base_bundle_sha256','rsu_bindings','rsu_price_seeds','shipping_classes','source_refs'}
             and type(release['schema_version']) is int and release['schema_version']==1
             and release['base_bundle_sha256']==base_bundle_sha256)
        bindings,seeds,freight=release['rsu_bindings'],release['rsu_price_seeds'],release['shipping_classes']
        need(isinstance(bindings,dict) and 1<=len(bindings)<=5000 and isinstance(seeds,dict)
             and set(seeds)=={'records','evidence_id'} and isinstance(seeds['records'],dict)
             and set(seeds['records'])==set(bindings) and isinstance(seeds['evidence_id'],str) and HASH.fullmatch(seeds['evidence_id'])
             and isinstance(freight,dict) and set(freight)==set(bindings),'parts_release_not_complete')
        refs=release['source_refs'];need(isinstance(refs,list) and 1<=len(refs)<=30)
        sources={};names=set()
        for ref in refs:
            need(isinstance(ref,dict) and set(ref)=={'file','sha256'} and isinstance(ref['file'],str)
                 and NAME.fullmatch(ref['file']) and isinstance(ref['sha256'],str) and HASH.fullmatch(ref['sha256'])
                 and ref['file'] not in names)
            names.add(ref['file']);data=read(root/'parts-sources'/ref['file'],MAX_FILE)
            need(sha256(data).hexdigest()==ref['sha256'],'parts_source_hash_mismatch');sources[ref['sha256']]=data
        views, inquiries, visits = _originals(sources)
        products=bundle['products'];need(isinstance(products,list) and len(products)<=100000)
        index={row.get('selection_id',row.get('id')):(n,row) for n,row in enumerate(products,1) if isinstance(row,dict)}
        existing=bundle.get('rsu_bindings',{});need(isinstance(existing,dict) and not set(bindings)&set(existing),'parts_release_existing_binding')
        released=set(bundle['publication']['released_ids']);need(set(bindings)<=released)
        supplier_ids=set()
        for identifier,binding in bindings.items():
            need(identifier in index and isinstance(binding,dict) and set(binding)=={'category','identity_hash','mapping','expected_ean'})
            number,product=index[identifier];identity=variant_fingerprint(product)
            need(product.get('category') in CATEGORIES and binding['category']==product['category'] and binding['identity_hash']==identity)
            resolved=validate_parts_mapping(binding['mapping'],product_id=identifier,category=product['category'],expected_ean=binding['expected_ean'],identity_hash=identity)
            proof=binding['mapping']['evidence'];catalogue=proof['catalogue'];supplier=proof['rsu']
            details=product.get('details',{});ean=details.get('EAN')
            from shop_rsu_parts_evidence import _ean
            need(not ean or _ean(ean), 'parts_invalid_catalogue_ean')
            # Maker/MPN-only candidates remain useful for reconciliation, but
            # the current original-view contract does not prove their sale
            # unit/packing. They require that separate source before activation.
            need(_ean(ean), 'parts_sale_unit_source_required')
            catalogue_actual=parts_source_record('catalogue',source_sha256=base_bundle_sha256,row_number=number,article_id=identifier,
                manufacturer=product.get('manufacturer_name'),mpn=details.get('Herstellerartikelnummer'),ean=ean if _ean(ean) else None)
            need(catalogue==catalogue_actual,'parts_catalogue_row_mismatch')
            need(supplier['source_sha256'] in sources,'parts_supplier_source_missing')
            # This release supports original GET timestamps. CSV identity
            # candidates need a separately verified download-time receipt before
            # they can seed fixed retail prices; a caller-supplied date is not it.
            need(sources[supplier['source_sha256']].lstrip().startswith(b'{'), 'parts_original_inquiry_required')
            observation=_observation(sources[supplier['source_sha256']],supplier['row_number'])
            actual=parts_source_record('rsu',source_sha256=supplier['source_sha256'],row_number=supplier['row_number'],
                article_id=observation.get('article_id'),manufacturer=observation.get('manufacturer'),mpn=observation.get('mpn'),ean=observation.get('ean'))
            need(supplier==actual and resolved['supplier_article_id'] not in supplier_ids,'parts_supplier_row_mismatch')
            if sources[supplier['source_sha256']].lstrip().startswith(b'{'):
                _genuine_observation(observation, supplier, views, inquiries)
            supplier_ids.add(resolved['supplier_article_id'])
            seed=seeds['records'][identifier]
            need(isinstance(seed,dict) and seed.get('source')=='RSU' and seed.get('identity_hash')==identity
                 and type(seed.get('stock_reported')) is int and seed['stock_reported']>=0
                 and type(seed.get('unit_gross_cent')) is int and seed['unit_gross_cent']>0)
            # The seed may be backed by another newer genuine observation. It
            # must match an explicit original row, never an ALZURA market price.
            matched=False
            for source in sources.values():
                if not source.lstrip().startswith(b'{'):continue
                value=decode(source)
                if not isinstance(value,dict) or value.get('supplier')!='RSU' or value.get('price_basis')!='net' or value.get('currency')!='EUR':continue
                for obs in value.get('records',[]):
                    if (isinstance(obs,dict) and obs.get('article_id')==resolved['supplier_article_id']
                            and obs.get('ean')==binding['expected_ean'] and obs.get('quantity')==1
                            and _stamp(obs.get('observed_at'))==_stamp(seed.get('observed_at')) and obs.get('stock')==seed['stock_reported']):
                        _genuine_observation(obs, supplier, views, inquiries)
                        need(retail_unit_gross_cent(str(obs.get('unit_net')),product['category'])==seed['unit_gross_cent'],'parts_material_markup_mismatch');matched=True
            need(matched,'parts_price_source_missing')
            need(isinstance(freight[identifier],dict) and freight[identifier].get('product_id')==identifier
                 and freight[identifier].get('identity_hash')==identity,'parts_freight_missing')
            _genuine_freight(freight[identifier], supplier, views, inquiries, visits)
        # Validate the entire delivery contract BEFORE production provisions
        # or seeds a database. Constructor callbacks cannot perform any IO.
        from shop_parts_freight import PartsRetailFreight
        observed = {pid: p for pid,p in freight.items() if p['rule'] == 'rsu_ui_unique_q1_freight_v1'}
        fixed = {pid: p for pid,p in freight.items() if p['rule'] == 'merchant_fixed_parts_freight_v1'}
        need(len(observed)+len(fixed)==len(freight), 'parts_freight_rule_unknown')
        PartsRetailFreight(observed,bindings=bindings,mode='live',destination_eligibility=lambda *args:False,
            fresh_inquiry=lambda *args:None,source_refs=refs)
        from shop_parts_fixed_freight import FixedPartsRetailFreight
        if fixed:
            FixedPartsRetailFreight(fixed,bindings=bindings,mode='live',destination_eligibility=lambda *args:False,
                retained_inquiry=lambda *args:None,source_refs=refs)
        merged=deepcopy(bundle)
        merged['rsu_bindings'].update(deepcopy(bindings))
        merged['rsu_price_seeds']['records'].update(deepcopy(seeds['records']))
        merged['rsu_price_seeds']['evidence_id']=sha256((bundle['rsu_price_seeds']['evidence_id']+record['sha256']).encode()).hexdigest()
        return merged,{'ids':frozenset(bindings),'bindings':deepcopy(bindings),'freight':deepcopy(freight),'source_refs':deepcopy(refs)}
    except PartsReleaseError:raise
    except Exception:raise PartsReleaseError('invalid_parts_release') from None
