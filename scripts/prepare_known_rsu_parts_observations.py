"""Normalize two retained positive RSU sources offline; never discover or query IDs."""
import argparse
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import re
import sys

from reconcile_rsu_parts import (catalogue_records, decode, encode, read_pinned,
                                 require, write_new)

AIC_PROOF_SHA='efcd650dfbcc007b373b5aba25d7f6e70b42ee2291c0c02df47e225fb51dd4f0'
DIEDERICHS_UI_SHA='7d4151c8f566c7c47c4f7acacb16f6baa6ca4770720eeba695e40c5e75ffa0e5'


def prepare(work_root,bundle_raw,bundle_sha):
    bundle=decode(bundle_raw);catalogue=catalogue_records(bundle,bundle_sha)
    bind=work_root/'rsu-parts-binding-private';procurement=work_root/'rsu-parts-procurement-private'
    proof_path=bind/'aic-75983-identity-only-proof.private.json'
    proof_raw=read_pinned(proof_path,AIC_PROOF_SHA);proof=decode(proof_raw)
    inquiry_path=bind/'aic-75983-inquiry-20261117.private.json'
    inquiry_raw=read_pinned(inquiry_path,proof['inquiry_source']['sha256']);inquiry=decode(inquiry_raw)
    aic_path=procurement/'aic-75983-rsu-detail.private.txt'
    aic_raw=read_pinned(aic_path,proof['browser_source']['sha256'])
    require(proof.get('identity_confirmed') is True and proof.get('not_an_active_binding') is True
            and proof.get('supplier',{}).get('name')=='RSU' and inquiry.get('identity_confirmed') is True,'retained_aic_proof_invalid')
    product=proof['product'];supplier=proof['supplier'];observed=inquiry['result']
    require(product['id'] in catalogue and catalogue[product['id']]['identity_hash']==product['identity_hash']
            and observed['article_id']==supplier['article_id'] and observed['ean']==supplier['ean']==product['ean']
            and observed['amount']==1 and inquiry['requested_quantity']==1,'retained_aic_identity_changed')
    dom=aic_raw.decode('utf-8')
    for label,expected in [('Marke',supplier['manufacturer']),('Hersteller Artikelnummer',supplier['manufacturer_article_number']),
                           ('EAN',supplier['ean']),('RSU Artikelnummer',supplier['article_id'])]:
        values=re.findall(r'^\s*- row "'+re.escape(label)+r' ([^"\r\n]+)":\s*$',dom,re.M)
        require(values==[expected],'retained_aic_original_row_mismatch')
    # Its successful q1 observation never supplies q2 stock or tier data.
    aic={'article_id':supplier['article_id'],'manufacturer':supplier['manufacturer'],
         'mpn':supplier['manufacturer_article_number'],'ean':supplier['ean'],'read_kind':'inquiry',
         'observed_at':observed['retrieved_at'],'quantity':1,'unit_net':observed['unit_net'],'stock':observed['stock'],
         'category':None,'unit_label':'Stück','packing_quantity':None,'condition':None,'freight':None,
         'supplier_offer':None,'offer_count':2}
    d_path=procurement/'diederichs-1033106-rsu-ui-q2.private.txt';d_raw=read_pinned(d_path,DIEDERICHS_UI_SHA);text=d_raw.decode('utf-8')
    lines=[line.strip() for line in text.splitlines()]
    def after(label):
        positions=[i for i,line in enumerate(lines) if line==label]
        require(len(positions)==1 and positions[0]+1<len(lines),'retained_ui_label_ambiguous')
        return lines[positions[0]+1]
    manufacturer,mpn,ean,article=after('Marke'),after('Hersteller Artikelnummer'),after('EAN'),after('RSU Artikelnummer')
    quantity=re.search(r'Fahrzeug auswählen\s*\n([0-9]+)\s*\nabweichende Lieferadresse',text)
    require(quantity is not None,'retained_ui_quantity_unknown');quantity=int(quantity[1])
    table=text.split('LIEFEROPTIONEN',1)[1].split('In den Warenkorb',1)[0]
    prices=re.findall(r'([0-9]+,[0-9]{2})\s*€',table)
    stock=re.findall(r'\n\s*\t([0-9]+)\t',table)
    require(len(prices)==8 and prices[:4]==prices[4:] and len(stock)==2 and stock[0]==stock[1],'retained_ui_offers_changed')
    net,freight,total=[Decimal(value.replace(',','.')) for value in prices[:3]]
    require(net*quantity+freight==total,'retained_ui_total_mismatch')
    matches=[pid for pid,item in catalogue.items() if item['source'] and item['source']['identity']['manufacturer']==manufacturer.casefold()
        and item['source']['identity']['mpn']==mpn.casefold() and item['source']['identity']['ean']==ean]
    require(len(matches)==1,'retained_ui_catalogue_ambiguous')
    diederichs={'article_id':article,'manufacturer':manufacturer,'mpn':mpn,'ean':ean,'read_kind':'supplier_ui',
         'observed_at':None,'quantity':quantity,'unit_net':str(net),'stock':int(stock[0]),
         'category':None,'unit_label':'Stück','packing_quantity':None,'condition':None,
         'freight':{'confirmed':False,'observed_net_eur':str(freight),'offer_count':2},'supplier_offer':None,'offer_count':2}
    receipts=[(proof_path,proof_raw),(aic_path,aic_raw),(inquiry_path,inquiry_raw),(d_path,d_raw)]
    return {'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net','records':[aic,diederichs],
        'source_receipts':[{'file':str(path.absolute()),'sha256':sha256(data).hexdigest()} for path,data in receipts]}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work-root',type=Path,required=True);p.add_argument('--catalogue',type=Path,required=True)
    p.add_argument('--catalogue-sha',required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    try:
        value=prepare(args.work_root.absolute(),read_pinned(args.catalogue,args.catalogue_sha),args.catalogue_sha)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        report=write_new(args.output,value);print(json.dumps({'prepared_positive_observations':2,'artifact':report,'provider_calls':0,'orders':0,'payments':0}))
    except Exception as error:
        print(json.dumps({'status':'error','reason':str(error) if isinstance(error,ValueError) else type(error).__name__}));raise SystemExit(1)


if __name__=='__main__':main()
