"""Correlate pinned original RSU DOM tables with retained successful GET results."""
import argparse
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import re
import json

from reconcile_rsu_parts import (catalogue_records,decode,read_pinned,require,stamp,
    write_new,parts_source_record)


def dom_product(raw):
    text=raw.decode('utf-8')
    def field(label):
        values=re.findall(r'^\s*- row "'+re.escape(label)+r' ([^"\r\n]+)":\s*$',text,re.M)
        require(len(values)==1,'ui_identity_field_not_unique');return values[0]
    headings=re.findall(r'^- heading "[^"\r\n]+" \[level=1\]:',text,re.M)
    require(len(headings)==1,'ui_product_heading_not_unique')
    header='row "Lieferoptionen Bestand EK Stück Versand EK Gesamt VK1":'
    require(text.count(header)==1,'ui_offer_table_not_unique')
    before,table=text.split(header)
    product=before.split(headings[0],1)[1]
    quantities=re.findall(r'^- generic: "([1-9][0-9]*)"\s*$',product,re.M)
    require(len(quantities)==1,'ui_selected_quantity_not_unique');quantity=int(quantities[0])
    table=table.split('- button ',1)[0]
    rows=re.findall(r'^\s*- row "([^"\r\n]+)":\s*$',table,re.M)
    require(1<=len(rows)<=50,'ui_offer_rows_missing')
    offers=[]
    for row in rows:
        prices=list(re.finditer(r'(?<![0-9.,])([0-9]+(?:\.[0-9]{3})*,[0-9]{2})\s*€',row))
        require(len(prices)==4,'ui_offer_price_fields_invalid')
        prefix=row[:prices[0].start()];stock=re.search(r'\s([0-9]+)\s+$',prefix)
        require(stock is not None,'ui_offer_stock_not_explicit')
        values=[Decimal(match[1].replace('.','').replace(',','.')) for match in prices]
        net,freight,total,retail=values
        require(net>0 and freight>=0 and net*quantity+freight==total,'ui_offer_total_mismatch')
        offers.append({'stock':int(stock[1]),'unit_net':format(net,'f'),'freight_net':format(freight,'f')})
    return {'article_id':field('RSU Artikelnummer'),'manufacturer':field('Marke'),
        'mpn':field('Hersteller Artikelnummer'),'ean':field('EAN'),'quantity':quantity,'offers':offers}


def normalize(bundle,bundle_sha,known,rest,rest_ref,captures,*,capture_root):
    require(isinstance(rest,dict) and type(rest.get('schema_version')) is int and rest['schema_version']==1
            and rest.get('supplier')=='RSU' and rest.get('mode')=='live'
            and type(rest.get('orders')) is int and rest['orders']==0
            and type(rest.get('payments')) is int and rest['payments']==0
            and isinstance(rest.get('records'),list) and type(rest.get('requests')) is int
            and rest['requests']==len(rest['records'])<=20,'invalid_retained_rest_envelope')
    records={};rest_index={}
    for row in rest['records']:
        require(row.get('ok') is True and row.get('mode')=='live' and row.get('operation')=='REST_INQUIRY_GET'
                and type(row.get('quantity')) is int and row['quantity']==1 and row.get('ean')==row.get('expected_ean')
                and type(row.get('stock')) is int and row['stock']>=0,'retained_rest_identity_or_quantity_invalid')
        require(row['supplier_article_id'] not in rest_index,'duplicate_retained_rest_id')
        stamp(row['retrieved_at']);rest_index[row['supplier_article_id']]=row
    catalogue=catalogue_records(bundle,bundle_sha)
    receipts=list(known.get('source_receipts',[])) if known else []
    def live_row(identity,observed,unit_label):
        return {'article_id':identity['article_id'],'manufacturer':identity['manufacturer'],'mpn':identity['mpn'],
            'ean':identity['ean'],'read_kind':'inquiry','observed_at':observed['retrieved_at'],'quantity':1,
            'unit_net':observed['unit_net'],'stock':observed['stock'],'category':None,'unit_label':unit_label,
            'packing_quantity':None,'condition':None,'freight':None,'supplier_offer':None}
    if known:
        require(known.get('supplier')=='RSU' and known.get('currency')=='EUR' and known.get('price_basis')=='net','invalid_known_identity_input')
        for identity in known['records']:
            observed=rest_index.get(identity['article_id'])
            if observed is None:continue
            require(identity['ean']==observed['ean'],'known_identity_rest_ean_conflict')
            records[identity['article_id']]=live_row(identity,observed,identity.get('unit_label'))
    require(isinstance(captures,dict) and set(captures) in ({'schema_version','captures'},{'captures'})
            and type(captures.get('schema_version',1)) is int and captures.get('schema_version',1)==1
            and isinstance(captures['captures'],list) and len(captures['captures'])<=20,'invalid_capture_manifest')
    diagnostics=[]
    for capture in captures['captures']:
        require(isinstance(capture,dict) and set(capture)=={'file','sha256','observed_at'},'invalid_capture_record')
        path=Path(capture['file']);path=path if path.is_absolute() else capture_root/path
        raw=read_pinned(path,capture['sha256'],1024*1024);ui=dom_product(raw);stamp(capture['observed_at'])
        observed=rest_index.get(ui['article_id']);require(observed is not None and ui['ean']==observed['ean'],'ui_rest_identity_mismatch')
        source=parts_source_record('rsu',source_sha256=capture['sha256'],row_number=1,
            article_id=ui['article_id'],manufacturer=ui['manufacturer'],mpn=ui['mpn'],ean=ui['ean'])
        matches=[value for value in catalogue.values() if value['source'] and value['source']['identity']==source['identity']]
        require(len(matches)==1,'ui_catalogue_identity_not_unique')
        record=live_row(ui,observed,matches[0]['row'].get('unit_label'))
        ui_ref={'file':path.name,'sha256':capture['sha256']}
        unique=ui['quantity']==1 and len(ui['offers'])==1
        correlated=unique and ui['offers'][0]['stock']==observed['stock'] and Decimal(ui['offers'][0]['unit_net'])==Decimal(observed['unit_net'])
        if correlated:
            offer=ui['offers'][0]
            record['freight']={'rule':'rsu_ui_unique_q1_freight_v1','source_ref':ui_ref,'inquiry_ref':rest_ref,
                'observed_at':capture['observed_at'],'quantity':1,'offer_count':1,'supplier_article_id':ui['article_id'],
                'ean':ui['ean'],'manufacturer':ui['manufacturer'],'mpn':ui['mpn'],
                'unit_net':offer['unit_net'],'stock_reported':offer['stock'],'freight_net':offer['freight_net'],
                'currency':'EUR','mainland_only':True}
        record['offer_count']=len(ui['offers']);records[ui['article_id']]=record
        receipts.append({'file':str(path.absolute()),'sha256':capture['sha256']})
        diagnostics.append({'supplier_article_id':ui['article_id'],'selected_quantity':ui['quantity'],
            'observed_offers':len(ui['offers']),'ui_rest_net_and_stock_correlated':correlated})
    require(records,'no_retained_observations')
    return {'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net',
        'source_receipts':receipts,'records':[records[key] for key in sorted(records)]},diagnostics


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['catalogue','rest','captures','known','output']:
        p.add_argument('--'+name,type=Path,required=name!='known')
    for name in ['catalogue-sha','rest-sha','captures-sha','known-sha']:
        p.add_argument('--'+name,required=name!='known-sha')
    args=p.parse_args()
    try:
        bundle=decode(read_pinned(args.catalogue,args.catalogue_sha));rest=decode(read_pinned(args.rest,args.rest_sha))
        captures=decode(read_pinned(args.captures,args.captures_sha));known=decode(read_pinned(args.known,args.known_sha)) if args.known else None
        result,diagnostics=normalize(bundle,args.catalogue_sha,known,rest,{'file':args.rest.name,'sha256':args.rest_sha},
            captures,capture_root=args.captures.absolute().parent)
        result['source_receipts'].extend({'file':str(path.absolute()),'sha256':digest} for path,digest in
            [(args.rest,args.rest_sha),(args.captures,args.captures_sha)]+([(args.known,args.known_sha)] if args.known else []))
        for receipt in result['source_receipts']:read_pinned(receipt['file'],receipt['sha256'])
        args.output.parent.mkdir(parents=True,exist_ok=True);artifact=write_new(args.output,result)
        print(json.dumps({'artifact':artifact,'observations':len(result['records']),'ui_checks':diagnostics,'provider_calls':0,'orders':0,'payments':0}))
    except Exception as error:
        print(json.dumps({'status':'error','reason':str(error) if isinstance(error,ValueError) else type(error).__name__}));raise SystemExit(1)


if __name__=='__main__':main()
