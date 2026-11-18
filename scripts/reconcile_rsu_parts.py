"""Offline, explicit-input parts reconciliation. Never reads providers or changes releases."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import csv
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import unicodedata

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from shop_catalog_public import variant_fingerprint
from shop_cart_quotes import retail_unit_gross_cent
from shop_rsu_parts_evidence import CATEGORIES, PartsEvidenceError, parts_source_record, build_parts_binding

MAX_BYTES=64*1024*1024
MAX_ROWS=300000
HASH=re.compile(r'[a-f0-9]{64}')
CSV_REQUIRED={'Artikelnummer','Hersteller','Herstellerartikelnummer','ean'}
READ_FIELDS={'article_id','manufacturer','mpn','ean','read_kind','observed_at','quantity','unit_net','stock',
             'category','unit_label','packing_quantity','condition','freight','supplier_offer','offer_count'}


class ReconciliationError(ValueError):pass


def require(value,code):
    if not value:raise ReconciliationError(code)


def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def decode(raw):
    def unique(items):
        result={}
        for key,value in items:
            require(key not in result,'duplicate_json_key');result[key]=value
        return result
    try:return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
    except ReconciliationError:raise
    except Exception:raise ReconciliationError('invalid_json') from None


def read_pinned(path,expected,maximum=MAX_BYTES):
    path=Path(path).absolute()
    require(isinstance(expected,str) and HASH.fullmatch(expected),'invalid_source_hash')
    require(not any(p.is_symlink() for p in (path,*path.parents)),'unsafe_source_path')
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    try:
        meta=os.fstat(fd);require(stat.S_ISREG(meta.st_mode) and 0<meta.st_size<=maximum,'source_size_limit')
        with os.fdopen(fd,'rb') as stream:fd=None;raw=stream.read(maximum+1)
    finally:
        if fd is not None:os.close(fd)
    require(len(raw)<=maximum and sha256(raw).hexdigest()==expected,'source_hash_mismatch')
    return raw


def stamp(value):
    try:
        result=datetime.fromisoformat(value.replace('Z','+00:00')) if isinstance(value,str) else None
        require(result is not None and result.tzinfo is not None,'invalid_timestamp')
        return result.astimezone(timezone.utc)
    except Exception:raise ReconciliationError('invalid_timestamp') from None


def valid_ean(value):
    return (isinstance(value,str) and bool(re.fullmatch(r'[0-9]{13}',value)) and set(value)!={'0'}
            and sum(int(c)*(1 if i%2==0 else 3) for i,c in enumerate(value[:12]))%10==(-int(value[-1]))%10)


def catalogue_records(bundle,bundle_sha):
    require(isinstance(bundle,dict) and isinstance(bundle.get('products'),list) and len(bundle['products'])<=100000,'invalid_catalogue')
    result={}
    for number,row in enumerate(bundle['products'],1):
        if not isinstance(row,dict) or row.get('category') not in CATEGORIES:continue
        identifier=row.get('selection_id',row.get('id'));require(identifier not in result,'duplicate_catalogue_id')
        details=row.get('details',{});ean=details.get('EAN') if isinstance(details,dict) else None
        # Missing/invalid GTIN stays unknown. Never decode vendor/native IDs.
        ean=ean if valid_ean(ean) else None
        try:
            source=parts_source_record('catalogue',source_sha256=bundle_sha,row_number=number,article_id=identifier,
                manufacturer=row.get('manufacturer_name'),mpn=details.get('Herstellerartikelnummer') if isinstance(details,dict) else None,ean=ean)
        except PartsEvidenceError:source=None
        result[identifier]={'row':row,'source':source,'identity_hash':variant_fingerprint(row)}
    require(result,'empty_parts_catalogue')
    return result


def source_rows(raw,record):
    if record['format']=='rsu_csv':
        try:reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')),delimiter=';')
        except UnicodeError:raise ReconciliationError('invalid_csv_encoding') from None
        require(reader.fieldnames is not None and len(set(reader.fieldnames))==len(reader.fieldnames)
                and CSV_REQUIRED<=set(reader.fieldnames),'not_rsu_csv_schema')
        for number,row in enumerate(reader,1):
            require(number<=MAX_ROWS and None not in row,'source_row_limit_or_columns')
            stock=row.get('Lagerbestand')
            # Native ALZURA offer IDs and prices have no role in this adapter.
            yield number,{'article_id':row['Artikelnummer'],'manufacturer':row['Hersteller'],
                'mpn':row['Herstellerartikelnummer'],'ean':row['ean'] or None,'read_kind':'supplier_csv',
                'observed_at':record.get('observed_at'),'quantity':1,'unit_net':row.get('Einkaufspreis'),
                'stock':int(stock) if isinstance(stock,str) and re.fullmatch(r'[0-9]{1,9}',stock) else None,
                'condition':'new' if all(row.get(k)==' ' or row.get(k)=='' for k in ['Dot','Demo','Da']) else None,
                'category':None,'unit_label':None,'packing_quantity':None,'freight':None,'supplier_offer':None,
                'source_category':row.get('Artikeltyp')}
    else:
        value=decode(raw)
        require(isinstance(value,dict) and set(value)<={'schema_version','supplier','currency','price_basis','records','source_receipts'}
                and value.get('schema_version')==1 and type(value.get('schema_version')) is int
                and value.get('supplier')=='RSU' and value.get('currency')=='EUR' and value.get('price_basis')=='net'
                and isinstance(value.get('records'),list) and len(value['records'])<=MAX_ROWS,'not_rsu_read_schema')
        receipts=value.get('source_receipts',[])
        require(isinstance(receipts,list) and len(receipts)<=20,'invalid_source_receipts')
        for receipt in receipts:
            require(isinstance(receipt,dict) and set(receipt)=={'file','sha256'},'invalid_source_receipts')
            read_pinned(receipt['file'],receipt['sha256'])
        for number,row in enumerate(value['records'],1):
            require(isinstance(row,dict) and set(row)<=READ_FIELDS
                    and {'article_id','manufacturer','mpn','ean','read_kind'}<=set(row)
                    and row['read_kind'] in {'inquiry','supplier_ui','supplier_csv'},'invalid_rsu_read_record')
            yield number,row


def load_observations(manifest,manifest_path,catalogue):
    require(isinstance(manifest,dict) and set(manifest)=={'schema_version','sources'}
            and type(manifest['schema_version']) is int and manifest['schema_version']==1
            and isinstance(manifest['sources'],list) and 1<=len(manifest['sources'])<=20,'invalid_sources_manifest')
    wanted={(item['source']['identity']['manufacturer'],item['source']['identity']['mpn']) for item in catalogue.values() if item['source']}
    observations=[];stats=Counter();seen_sources=set();all_identities=defaultdict(set)
    for record in manifest['sources']:
        require(isinstance(record,dict) and set(record)<={'file','sha256','format','observed_at'}
                and {'file','sha256','format'}<=set(record) and record['format'] in {'rsu_csv','rsu_read_json'},'invalid_source_record')
        path=Path(record['file']);path=path if path.is_absolute() else Path(manifest_path).parent/path
        require('..' not in path.parts and record['sha256'] not in seen_sources,'unsafe_or_duplicate_source')
        seen_sources.add(record['sha256']);raw=read_pinned(path,record['sha256'])
        refs=[{'file':path.name,'sha256':record['sha256']}]
        if record['format']=='rsu_read_json':
            refs.extend({'file':Path(receipt['file']).name,'sha256':receipt['sha256']}
                        for receipt in decode(raw).get('source_receipts',[]))
        for number,row in source_rows(raw,record):
            stats['rows_scanned']+=1;require(stats['rows_scanned']<=MAX_ROWS,'combined_row_limit')
            try:
                source=parts_source_record('rsu',source_sha256=record['sha256'],row_number=number,
                    article_id=row.get('article_id'),manufacturer=row.get('manufacturer'),mpn=row.get('mpn'),
                    ean=row.get('ean') if valid_ean(row.get('ean')) else None)
            except PartsEvidenceError:stats['invalid_source_identity']+=1;continue
            identity=source['identity'];key=(identity['manufacturer'],identity['mpn'])
            all_identities[source['article_id']].add(encode(identity))
            if key not in wanted:continue
            observations.append({'source':source,'row':row,'source_refs':refs});stats['potential_matching_rows']+=1
    conflicts={identifier for identifier,identities in all_identities.items() if len(identities)>1}
    stats['conflicting_supplier_article_ids']=len(conflicts)
    for item in observations:item['source_id_conflict']=item['source']['article_id'] in conflicts
    return observations,dict(stats)


def price_observation(item,binding,now):
    row=item['row'];kind=row.get('read_kind')
    if kind not in {'inquiry','supplier_csv'}:return None,'price_not_machine_observed'
    try:
        observed=stamp(row.get('observed_at'))
        require(observed<=now<observed+timedelta(days=5),'price_observation_stale_or_future')
        require(row.get('quantity')==1 and type(row.get('quantity')) is int,'price_quantity_scope_not_one')
        net=row.get('unit_net');require(isinstance(net,str) and re.fullmatch(r'[0-9]{1,6}(?:[.,][0-9]{1,4})?',net),'invalid_rsu_net_price')
        net=Decimal(net.replace(',','.'));stock=row.get('stock')
        require(type(stock) is int and 0<=stock<=1000000000,'invalid_rsu_stock')
        gross=retail_unit_gross_cent(net,binding['category'])
        return {'source':'RSU','identity_hash':binding['identity_hash'],'observed_at':observed.isoformat(),
                'unit_gross_cent':gross,'stock_reported':stock,'evidence_id':sha256(encode(item)).hexdigest()},None
    except (ReconciliationError,ValueError,InvalidOperation) as error:
        return None,str(error) if isinstance(error,ReconciliationError) else 'invalid_rsu_net_price'


def freight_observation(item,binding,product_id,now):
    """A unique observed q1 offer correlated with actual fresh REST facts."""
    row=item['row'];offer=row.get('freight')
    if offer is None:return None,'rsu_parts_freight_source_missing'
    try:
        keys={'rule','source_ref','inquiry_ref','observed_at','quantity','offer_count','supplier_article_id',
              'ean','manufacturer','mpn','unit_net','stock_reported','freight_net','currency','mainland_only'}
        require(isinstance(offer,dict) and set(offer)==keys and offer['rule']=='rsu_ui_unique_q1_freight_v1','invalid_parts_freight_proof')
        require(row.get('read_kind')=='inquiry' and type(offer['quantity']) is int and offer['quantity']==1
                and type(offer['offer_count']) is int and offer['offer_count']==1
                and type(row.get('quantity')) is int and row['quantity']==1,'freight_requires_unique_q1_offer')
        require(offer['currency']=='EUR' and offer['mainland_only'] is True,'invalid_parts_freight_scope')
        refs=item.get('source_refs',[])
        for key in ['source_ref','inquiry_ref']:
            ref=offer[key]
            require(isinstance(ref,dict) and set(ref)=={'file','sha256'} and ref in refs
                    and isinstance(ref['sha256'],str) and HASH.fullmatch(ref['sha256']),'freight_source_receipt_unverified')
        require(offer['source_ref']!=offer['inquiry_ref'],'freight_sources_not_independent')
        ui=stamp(offer['observed_at']);inquiry=stamp(row.get('observed_at'))
        expires=min(ui,inquiry)+timedelta(minutes=15)
        require(ui<=now and inquiry<=now and now<expires,'parts_freight_expired_or_future')
        source=parts_source_record('rsu',source_sha256=offer['source_ref']['sha256'],row_number=1,
            article_id=offer['supplier_article_id'],manufacturer=offer['manufacturer'],mpn=offer['mpn'],ean=offer['ean'])
        require(source['article_id']==item['source']['article_id'] and source['identity']==item['source']['identity'],
                'parts_freight_identity_mismatch')
        require(type(offer['stock_reported']) is int and type(row.get('stock')) is int
                and offer['stock_reported']==row['stock'] and row['stock']>=1,'parts_freight_stock_mismatch')
        for key in ['unit_net','freight_net']:
            require(isinstance(offer[key],str) and re.fullmatch(r'[0-9]{1,6}(?:[.][0-9]{1,4})?',offer[key]),'invalid_parts_freight_amount')
        net=Decimal(offer['unit_net']);freight=Decimal(offer['freight_net'])
        require(isinstance(row.get('unit_net'),str) and net==Decimal(row['unit_net'].replace(',','.'))
                and net>0 and Decimal('0')<=freight<=Decimal('100000'),'parts_freight_price_mismatch')
        proof={**offer,'product_id':product_id,'identity_hash':binding['identity_hash'],
            'binding_evidence_id':binding['mapping']['evidence_id'],
            'manufacturer':source['identity']['manufacturer'],'mpn':source['identity']['mpn'],
            'observed_at':ui.isoformat(),'inquiry_observed_at':inquiry.isoformat(),'expires_at':expires.isoformat(),
            'unit_net':format(net,'f'),'freight_net':format(freight,'f'),
            'retail_gross_cent':retail_unit_gross_cent(freight,binding['category']) if freight else 0}
        proof['proof_sha256']=sha256(encode(proof)).hexdigest()
        return proof,None
    except (ReconciliationError,PartsEvidenceError,ValueError,InvalidOperation) as error:
        return None,str(error) if isinstance(error,ReconciliationError) else 'invalid_parts_freight_proof'


def reconcile(bundle,bundle_sha,observations,*,as_of):
    now=stamp(as_of);catalogue=catalogue_records(bundle,bundle_sha)
    source_index=defaultdict(list);supplier_identities=defaultdict(set)
    for item in observations:
        identity=item['source']['identity'];key=(identity['manufacturer'],identity['mpn'])
        source_index[key].append(item);supplier_identities[item['source']['article_id']].add(encode(identity))
    catalogue_keys=defaultdict(list)
    for identifier,item in catalogue.items():
        if item['source']:
            identity=item['source']['identity'];catalogue_keys[(identity['manufacturer'],identity['mpn'],identity['ean'])].append(identifier)
    bindings,seeds,shipping,coverage={},{},{},[]
    for identifier,item in catalogue.items():
        row,source=item['row'],item['source'];reasons=[];matched=[]
        if source is None:reasons.append('catalogue_manufacturer_mpn_missing_or_invalid')
        else:
            identity=source['identity'];key=(identity['manufacturer'],identity['mpn']);candidates=source_index.get(key,[])
            if not candidates:reasons.append('rsu_identity_source_missing')
            elif identity['ean'] is not None:
                matched=[candidate for candidate in candidates if candidate['source']['identity']['ean']==identity['ean']]
                if not matched:reasons.append('rsu_ean_missing_or_conflicting')
            else:
                matched=[candidate for candidate in candidates if candidate['source']['identity']['ean'] is not None]
                if not matched:reasons.append('rsu_ean_missing')
                if matched:
                    unit=row.get('unit_label');packing=row.get('packing_quantity')
                    normal=lambda value:' '.join(unicodedata.normalize('NFKC',value).casefold().split()) if isinstance(value,str) else None
                    if (not normal(unit) or type(packing) is not int or packing<1
                            or any(normal(candidate['row'].get('unit_label'))!=normal(unit)
                                   or type(candidate['row'].get('packing_quantity')) is not int
                                   or candidate['row']['packing_quantity']!=packing for candidate in matched)):
                        reasons.append('mpn_only_sale_unit_or_pack_evidence_missing');matched=[]
            if matched and len({candidate['source']['article_id'] for candidate in matched})!=1:
                reasons.append('ambiguous_rsu_article_ids');matched=[]
            if matched and (len(supplier_identities[matched[0]['source']['article_id']])!=1
                            or any(candidate.get('source_id_conflict') is True for candidate in matched)):
                reasons.append('conflicting_supplier_article_identity');matched=[]
            if matched and len(catalogue_keys[(identity['manufacturer'],identity['mpn'],identity['ean'])])!=1:
                reasons.append('ambiguous_catalogue_identity');matched=[]
        binding=None;seed=None;observation=None
        if matched:
            matched.sort(key=lambda v:(v['row'].get('read_kind')!='inquiry',v['source']['source_sha256'],v['source']['row_number']))
            observation=matched[0]
            try:
                binding=build_parts_binding(product_id=identifier,identity_hash=item['identity_hash'],category=row['category'],
                    catalogue=source,rsu=observation['source'],supplier_offer=observation['row'].get('supplier_offer'))
                bindings[identifier]=binding
            except PartsEvidenceError:reasons.append('unusable_or_undocumented_supplier_offer')
        if binding:
            eligible=[];price_reasons=[]
            for candidate in matched:
                value,error=price_observation(candidate,binding,now)
                if value:eligible.append((candidate,value))
                elif error:price_reasons.append(error)
            if eligible:
                latest=max(v['observed_at'] for _,v in eligible)
                eligible=[pair for pair in eligible if pair[1]['observed_at']==latest]
                unique_prices={v['unit_gross_cent'] for _,v in eligible}
                if len(unique_prices)!=1:reasons.append('conflicting_observed_rsu_prices')
                else:
                    eligible.sort(key=lambda pair:pair[1]['observed_at'],reverse=True);observation,seed=eligible[0];seeds[identifier]=seed
            else:reasons.append(sorted(set(price_reasons))[0] if price_reasons else 'rsu_price_observation_missing')
            observed=observation['row']
            try:fresh=now-timedelta(minutes=15)<=stamp(observed.get('observed_at'))<=now
            except ReconciliationError:fresh=False
            if not fresh:reasons.append('fresh_exact_quantity_stock_required')
            amount,stock=observed.get('quantity'),observed.get('stock')
            if type(amount) is not int or type(stock) is not int or stock<amount:
                reasons.append('exact_quantity_stock_insufficient_or_unknown')
            freight,freight_error=freight_observation(observation,binding,identifier,now)
            if freight:shipping[identifier]=freight
            elif freight_error:reasons.append(freight_error)
            if row.get('department')=='parts':reasons.append('exact_vehicle_or_part_sale_policy_required')
        coverage.append({'product_id':identifier,'category':row['category'],'department':row.get('department'),
            'identity_hash':item['identity_hash'],'identity_binding_prepared':binding is not None,
            'merchant_price_seed_prepared':seed is not None,'supported_observed_quantities':sorted({m['row'].get('quantity') for m in matched if type(m['row'].get('quantity')) is int}),
            'checkout_ready':binding is not None and seed is not None and identifier in shipping and not reasons,
            'blocked_reasons':sorted(set(reasons)),
            **({'supplier_article_id':binding['mapping']['evidence']['rsu']['article_id']} if binding else {})})
    # An exact supplier article may not silently back two different public rows.
    assigned=defaultdict(list)
    for identifier,binding in bindings.items():assigned[binding['mapping']['evidence']['rsu']['article_id']].append(identifier)
    duplicates={pid for ids in assigned.values() if len(ids)>1 for pid in ids}
    for identifier in duplicates:bindings.pop(identifier,None);seeds.pop(identifier,None);shipping.pop(identifier,None)
    for item in coverage:
        if item['product_id'] in duplicates:
            item.update(identity_binding_prepared=False,merchant_price_seed_prepared=False,checkout_ready=False)
            item['blocked_reasons']=sorted(set(item['blocked_reasons'])|{'supplier_article_shared_by_catalogue_rows'})
    counts=Counter(reason for item in coverage for reason in item['blocked_reasons'])
    source_refs={}
    for item in observations:
        if any(binding['mapping']['evidence']['rsu']['source_sha256']==item['source']['source_sha256'] for binding in bindings.values()):
            for ref in item.get('source_refs',[]):
                require(isinstance(ref['file'],str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',ref['file']), 'invalid_source_ref_name')
                require(ref['file'] not in source_refs or source_refs[ref['file']]==ref['sha256'],'source_ref_name_collision')
                source_refs[ref['file']]=ref['sha256']
    return {'schema_version':1,'base_bundle_sha256':bundle_sha,'as_of':now.isoformat(),'markup_percent':15,
        'vat_percent':19,'active_release_modified':False,'provider_calls':0,'orders':0,'payments':0,
        'bindings':bindings,'rsu_price_seeds':seeds,'shipping_classes':shipping,'coverage':coverage,
        'source_refs':[{'file':file,'sha256':digest} for file,digest in sorted(source_refs.items())],
        'summary':{'products':len(catalogue),'departments':dict(Counter(item['row'].get('department') for item in catalogue.values())),
            'valid_catalogue_eans':sum(bool(item['source'] and item['source']['identity']['ean']) for item in catalogue.values()),
            'identity_bindings_prepared':len(bindings),'price_seeds_prepared':len(seeds),
            'checkout_ready':sum(item['checkout_ready'] for item in coverage),'blocked_reason_counts':dict(sorted(counts.items()))}}


def overlay(result,*,activation_ready):
    # A unique exact q1 offer needs no ambiguous warehouse selector. All other
    # articles retain their independent data/fitment blockers.
    identifiers={row['product_id'] for row in result['coverage'] if row['checkout_ready']} if activation_ready else set(result['bindings'])
    records={key:value for key,value in result['rsu_price_seeds'].items() if key in identifiers}
    refs=result['source_refs'] if identifiers else []
    evidence=sha256(encode({'base_bundle_sha256':result['base_bundle_sha256'],'records':records,'source_refs':refs})).hexdigest()
    return {'schema_version':1,'base_bundle_sha256':result['base_bundle_sha256'],
        'rsu_bindings':{key:value for key,value in result['bindings'].items() if key in identifiers},
        'rsu_price_seeds':{'records':records,'evidence_id':evidence},
        'shipping_classes':{key:value for key,value in result['shipping_classes'].items() if key in identifiers},'source_refs':refs}


def missing_identity_requests(catalogue,result):
    requests=[]
    for item in result['coverage']:
        if item['identity_binding_prepared']:continue
        source=catalogue[item['product_id']]['source'];row=catalogue[item['product_id']]['row']
        identity=source['identity'] if source else {'manufacturer':None,'mpn':None,'ean':None}
        requests.append({'product_id':item['product_id'],'identity_hash':item['identity_hash'],
            'category':item['category'],**identity,'unit_label':row.get('unit_label'),
            'packing_quantity':row.get('packing_quantity'),'requested_read_quantity':1})
    return {'schema_version':1,'base_bundle_sha256':result['base_bundle_sha256'],
            'requests':requests,'supplier_ids_guessed':False,'requests_executed':0}


def write_new(path,value):
    path=Path(path).absolute()
    data=encode(value)+b'\n'
    require('..' not in path.parts and not any(p.is_symlink() for p in (path,*path.parents)),'unsafe_output_path')
    if path.exists():require(path.is_file() and path.stat().st_size==len(data) and path.read_bytes()==data,'output_artifact_conflict')
    else:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
        with os.fdopen(fd,'wb') as target:target.write(data)
    return {'file':path.name,'bytes':len(data),'sha256':sha256(data).hexdigest()}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--catalogue',type=Path,required=True);p.add_argument('--catalogue-sha',required=True)
    p.add_argument('--sources',type=Path,required=True);p.add_argument('--sources-sha',required=True)
    p.add_argument('--as-of',required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    try:
        bundle=decode(read_pinned(args.catalogue,args.catalogue_sha));catalogue=catalogue_records(bundle,args.catalogue_sha)
        manifest=decode(read_pinned(args.sources,args.sources_sha,1024*1024))
        observations,source_stats=load_observations(manifest,args.sources,catalogue)
        result=reconcile(bundle,args.catalogue_sha,observations,as_of=args.as_of);result['source_statistics']=source_stats
        require('..' not in args.output.absolute().parts
                and not any(path.is_symlink() for path in (args.output.absolute(),*args.output.absolute().parents)),'unsafe_output_path')
        args.output.mkdir(exist_ok=True,parents=True)
        artifacts={name:write_new(args.output/name,value) for name,value in {
            'parts-rsu-reconciliation.private.json':result,'parts-rsu-bindings.private.json':result['bindings'],
            'parts-rsu-price-seeds-15pct.private.json':result['rsu_price_seeds'],
            'parts-rsu-candidates.private.json':overlay(result,activation_ready=False),
            'parts-rsu-direct-overlay.private.json':overlay(result,activation_ready=True),
            'parts-rsu-missing-identity-requests.private.json':missing_identity_requests(catalogue,result)}.items()}
        print(json.dumps({'summary':result['summary'],'sources':source_stats,'artifacts':artifacts},ensure_ascii=False))
    except Exception as error:
        code=str(error) if isinstance(error,ReconciliationError) else type(error).__name__
        print(json.dumps({'status':'error','reason':code}));raise SystemExit(1)


if __name__=='__main__':main()
