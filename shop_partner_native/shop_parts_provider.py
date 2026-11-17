"""Private normalization and bounded product-only refresh for local shop parts.

No browser parameters, credentials, supplier costs or native IDs are returned.
Only the already reviewed private catalogue chooses provider requests.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import html
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit
from uuid import uuid4

from . import alzura_readonly_catalog as api
from . import shop_parts_catalog as catalog
from .shop_live_quotes import LOCK, _safe_error

WORK = Path(__file__).resolve().parent
PRIVATE = WORK/'shop-parts-private'
LIVE = WORK/'alzura-live-private'
RAW = WORK/'alzura-catalog-private'
ALLOWED = {('products', 'items'), ('wearparts', 'articles'), ('wearparts', 'articles/DIRECTSEARCH')}
SID = re.compile(r'parts:[a-f0-9]{24}')
HOSTS = {f'media{i}.tyre-shopping.com' for i in range(1, 6)}
HOSTS.add('cdn01.alzura.com')
PAINT_NAMES = {'kotflügel':'fender', 'motorhaube':'bonnet', 'tür':'door',
               'seitenwand':'side_panel', 'kofferraumdeckel':'boot_lid',
               'stoßfänger vorne':'front_bumper', 'stoßstange vorne':'front_bumper',
               'stoßfänger hinten':'rear_bumper', 'stoßstange hinten':'rear_bumper'}
# Exact category IDs inspected in Products/categories, not inferred by name.
# Keep category evidence reproducible when the quote service replays a request.
PRODUCT_CATEGORY_GROUPS = {
    (1,1091): ('oil','oil'),
    (1,91): ('care','care'), (1,92): ('care','care'), (1,962): ('care','care'),
    (1,7): ('accessory','valves'), (1,3): ('accessory','adhesive'),
    (1,4): ('accessory','clip'), (1,935): ('accessory','workshop'),
    (9,628): ('accessory','tools'), (9,338): ('accessory','tools'),
}
# A saved supplier response offered the same implausible amount across sixty
# unrelated tools, often with the same stock and no supplier article number.
# This is not proof of dummy data; quarantine this unresolved source until its
# commercial validity has been reviewed. Other typed supplier offers survive.
PRODUCT_PRICE_POLICY = 'products-supplier-review-20261116-1'
UNCONFIRMED_PRODUCT_DISTRIBUTORS = frozenset({'200946'})


def _unconfirmed_distributor(value):
    return isinstance(value, dict) and any(str(value.get(key, '')) in UNCONFIRMED_PRODUCT_DISTRIBUTORS
                                          for key in ('distributorId', 'wholesalerId'))


def _text(value, maximum=200):
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool): return ''
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]{0,2000}>', ' ', str(value)))).strip()[:maximum]


def _stamp(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.utcoffset() is None: raise ValueError('aware_timestamp_required')
    return value.astimezone(timezone.utc)


def _decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)) or not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,6})?', str(value)):
        return None
    try:
        result = Decimal(value)
        return result if result.is_finite() and 0 < result <= 100000 else None
    except InvalidOperation:
        return None


def _stock(value):
    if isinstance(value, str) and re.fullmatch(r'[0-9]{1,7}', value): value = int(value)
    return value if type(value) is int and 0 <= value <= 1000000 else None


def _request(value):
    if not isinstance(value, dict) or set(value) != {'area', 'operation', 'params'}:
        raise ValueError('trusted_product_request_required')
    if (value.get('area'), value.get('operation')) not in ALLOWED:
        raise ValueError('trusted_product_request_required')
    api.build_url(value['area'], value['operation'], value['params'])
    return json.loads(json.dumps(value))


def _attributes(value):
    entries = list(value.values()) if isinstance(value, dict) else value if isinstance(value, list) else []
    result = []
    for entry in entries[:100]:
        if not isinstance(entry, dict): continue
        translated = [_text(item.get('translated_value')) for item in entry.get('values', [])
                      if isinstance(item, dict)] if isinstance(entry.get('values'), list) else []
        result.append({'id':str(entry.get('attributeId', '')), 'name':_text(entry.get('name')),
                       'value':', '.join(v for v in translated if v) or _text(entry.get('value'), 500)})
    return result


def _attribute(entries, identifier, name):
    values = [item['value'] for item in entries if item['id'] == str(identifier)
              or item['name'].casefold() == name.casefold()]
    values = list(dict.fromkeys(value for value in values if value))
    return values[0] if len(values) == 1 else ''


def _image(value):
    if not isinstance(value, str) or len(value) > 2000: return ''
    if value.count('%s-%s') == 1: value = value.replace('%s-%s', 'h800-w800')
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != 'https' or parsed.hostname not in HOSTS or parsed.username or parsed.password
                or parsed.port not in (None,443) or parsed.query or parsed.fragment or '%s' in value
                or not re.fullmatch(r'/[A-Za-z0-9/_.%-]+\.(?:jpg|jpeg|png|webp)', parsed.path, re.I)
                or '..' in parsed.path or any(ord(c)<33 or ord(c)==127 for c in value)):
            return ''
        return value
    except ValueError:
        return ''


def _ean(values):
    if isinstance(values, dict): values = values.get('array', [])
    if not isinstance(values, list): return ''
    candidates = [_text(v.get('eanNumber') or v.get('ean')) if isinstance(v,dict) else _text(v) for v in values]
    return next((v for v in candidates if re.fullmatch(r'[0-9]{8,14}',v)), '')


def _sku_unit(attrs):
    packaging = _attribute(attrs,1,'Verpackungseinheit')
    container = _attribute(attrs,5,'Gebinde')
    content = _attribute(attrs,53,'Inhalt')
    # The price is for one whole SKU, even if that SKU contains 100 weights.
    label = ' · '.join(v for v in (container, content) if v)
    if packaging and packaging.casefold() not in {'1 stück','1 stueck','1 stk','1 stk.'}:
        label = (label+' · ' if label else '')+'Packung ('+packaging+')'
    return (label or packaging or 'Stück')[:60], packaging, content


def _volume(packaging, content):
    # Only a specific contents field can establish volume. Shipping weight,
    # model/name numbers and viscosity numbers are never volume evidence.
    match = re.fullmatch(r'([0-9]{1,4}(?:[.,][0-9]{1,3})?)\s*(l|liter|ml)', content, re.I)
    pack = re.fullmatch(r'([0-9]{1,4})\s*(?:stück|stueck|stk\.?)', packaging, re.I)
    if not match or not pack: return None
    volume = Decimal(match[1].replace(',','.')) / (1000 if match[2].lower()=='ml' else 1)
    count = int(pack[1])
    return volume * count if 0 < volume <= 10000 and 0 < count <= 1000 else None


def _product_offer(row):
    offers = []
    distributors = row.get('distributors', [])
    if not isinstance(distributors, list): return None
    for distributor in distributors[:100]:
        if not isinstance(distributor, dict): continue
        if _unconfirmed_distributor(distributor): continue
        stock = _stock(distributor.get('stock'))
        if stock is None: continue
        prices = []
        for price in distributor.get('prices', []) if isinstance(distributor.get('prices'),list) else []:
            if (not isinstance(price,dict) or str(price.get('type','')).upper()!='EK'
                    or str(price.get('country_code','')).upper()!='DE' or price.get('currency')!='EUR'):
                continue
            tiers = price.get('prices')
            tier = tiers.get('1') if isinstance(tiers,dict) else None
            value = _decimal(tier.get('base')) if isinstance(tier,dict) else None
            if value is not None: prices.append(value)
        unique = set(prices)
        if len(unique) == 1: offers.append((next(iter(unique)),stock))
    if not offers: return None
    available = [offer for offer in offers if offer[1]>0] or offers
    # The chosen stock belongs to this exact offer. Do not use aggregate stock
    # from other suppliers with a different purchase price.
    return sorted(available,key=lambda offer:(offer[0],-offer[1]))[0]


def normalize_products(data, observed_at, source_request):
    request = _request(source_request)
    if request['area']!='products': raise ValueError('products_request_required')
    observed = _stamp(observed_at).isoformat()
    rows = data.get('result',[]) if isinstance(data,dict) else []
    if not isinstance(rows,list): return []
    result = []
    for row in rows[:1000]:
        if not isinstance(row,dict) or row.get('isActive') is False: continue
        native = _text(row.get('itemId')); name = _text(row.get('name'))
        if not re.fullmatch(r'[1-9][0-9]{0,17}', native) or not name: continue
        attrs = _attributes(row.get('attributeValues'))
        unit,pack,content = _sku_unit(attrs)
        oil = bool(content and (re.search(r'motor[öo]l|partikelfilter[öo]l',name,re.I)
                   or any(a['id']=='74' and a['value'] for a in attrs)
                   and any(a['id'] in {'651','653'} and a['value'] for a in attrs)))
        care = bool(re.search(r'autoshampoo|karosseriereiniger|autopflege|fahrzeugpflege|lackreiniger|waschshampoo',
                               name+' '+_text(row.get('description'),1500),re.I)
                    or request['params'].get('search','').casefold() in {'autoshampoo','karosseriereiniger'})
        typed_group = PRODUCT_CATEGORY_GROUPS.get((request['params'].get('productAreaId'), request['params'].get('parentNodeId')))
        category = typed_group[0] if typed_group else 'oil' if oil else 'care' if care else 'accessory'
        images = [_image(m.get('imageLink')) for m in row.get('media',[])
                  if isinstance(m,dict) and m.get('isDeleted') is not True] if isinstance(row.get('media'),list) else []
        images = list(dict.fromkeys(v for v in images if v))
        description = _text(row.get('description'),1500)
        description = '\n'.join(([description] if description else []) +
                    [a['name']+': '+a['value'] for a in attrs if a['name'] and a['value']])[:3000]
        item = {'source':'ALZURA','provider_id':'products:'+native,'category':category,
                'name':name,'brand':_text(row.get('manufacturerName')),
                'article_number':_text(row.get('manufacturerItemNumber')),
                'ean':_ean(row.get('identifications',{}).get('EAN',[])) if isinstance(row.get('identifications'),dict) else '',
                'description':description,'unit_label':unit,'stock':_stock(row.get('stock')),
                'observed_at':observed,'photo':'','image_url':images[0] if images else '',
                'image_urls':images,'source_request':request,'price_basis':'per_item',
                'price_type':'','currency':'','country':'','unit_net_eur':None}
        if typed_group:
            item['accessory_group'] = typed_group[1]
        offer = _product_offer(row)
        if offer:
            item.update(unit_net_eur=str(offer[0]),stock=offer[1],price_type='EK',currency='EUR',country='DE')
        elif any(_unconfirmed_distributor(d) for d in (row.get('distributors') if isinstance(row.get('distributors'), list) else []) if isinstance(d, dict)):
            item.update(stock=None, price_review_required=True,
                        price_review_reason='supplier_offer_unconfirmed')
        volume = _volume(pack,content) if category in {'oil','care'} else None
        if volume is not None:
            item.update(package_volume_litres=str(volume),package_volume_reviewed=True)
        result.append(item)
    return result


def normalize_wearparts(data, observed_at, source_request, context=None):
    request = _request(source_request)
    if request['area']!='wearparts': raise ValueError('wearparts_request_required')
    observed = _stamp(observed_at).isoformat(); context = context if isinstance(context,dict) else {}
    rows = data.get('result',[]) if isinstance(data,dict) else []
    if not isinstance(rows,list): return []
    result = []
    for row in rows[:1000]:
        if not isinstance(row,dict): continue
        article = row.get('assignedArticle') or row.get('directArticle')
        market = row.get('assignedTyre24Data')
        if not isinstance(article,dict) or not isinstance(market,dict): continue
        native = _text(article.get('articleId') or market.get('articleId'))
        brand_id = _text(article.get('brandNo') or market.get('brandId') or market.get('tecdoc_brand_id'))
        normal_name = _text(article.get('articleName'))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}',native) or not re.fullmatch(r'[0-9]{1,18}',brand_id) or not normal_name:
            continue
        paint = PAINT_NAMES.get(normal_name.casefold())
        body = paint is not None or normal_name.casefold() in {'stoßfänger','stoßstange'}
        attrs_raw = row.get('articleAttributes',{})
        attrs_raw = attrs_raw.get('array',[]) if isinstance(attrs_raw,dict) else attrs_raw
        attrs = []
        for entry in attrs_raw if isinstance(attrs_raw,list) else []:
            if isinstance(entry,dict):
                attrs.append({'name':_text(entry.get('attrName') or entry.get('attributeName')),
                              'value':_text(entry.get('attrValue') or entry.get('attributeValue'))})
        if body and paint is None:
            side = next((a['value'].casefold() for a in attrs if a['name'].casefold() in {'einbauseite','einbauposition'}),'')
            paint = 'front_bumper' if side=='vorne' else 'rear_bumper' if side=='hinten' else None
        new = article.get('flagRemanufacturedPart') is False
        pack = _stock(article.get('quantityPerPackingUnit'))
        unit = 'Packung ('+str(pack)+' Stück)' if pack and pack>1 else 'Stück'
        images = market.get('images',[])
        images = [_image(v) for v in images] if isinstance(images,list) else []
        images = list(dict.fromkeys(v for v in images if v))
        display_name = 'Halteblech, '+normal_name if any('halteblech' in a['value'].casefold() for a in attrs) else normal_name
        name = ' '.join(v for v in (_text(article.get('brandName') or market.get('manufacturer_name')),
                                   display_name,_text(article.get('articleAddName')),
                                   _text(article.get('articleNo') or market.get('manufacturer_number'))) if v)
        item = {'source':'ALZURA','provider_id':'wearparts:'+native+':'+brand_id,
                'category':'body_part' if body else 'accessory','name':name[:200],
                'brand':_text(article.get('brandName') or market.get('manufacturer_name')),
                'article_number':_text(article.get('articleNo') or market.get('manufacturer_number')),
                'ean':_ean(row.get('eanNumber',[])),
                'description':'\n'.join(a['name']+': '+a['value'] for a in attrs if a['name'] and a['value'])[:3000],
                'unit_label':unit,'stock':_stock(market.get('wholesaler_quantity')),
                'observed_at':observed,'photo':'','image_url':images[0] if images else '',
                'image_urls':images,'source_request':request,'source_context':context,
                'price_basis':'unconfirmed','price_type':'','currency':'','country':'','unit_net_eur':None,
                'new_part':new,'paint_code':paint if new else None,'paint_mapping_reviewed':False,
                'normal_part_name':normal_name}
        # Wearparts' bare price field has no currency/type discriminator. The
        # same response's typed price list must establish DE/EUR/EK semantics.
        typed = market.get('priceList') or market.get('prices')
        if isinstance(typed,list):
            matching = [p for p in typed if isinstance(p,dict) and str(p.get('type','')).upper()=='EK'
                        and str(p.get('country_code','')).upper()=='DE' and p.get('currency')=='EUR']
            amounts = {_decimal(p.get('value') or p.get('base')) for p in matching}
            amounts.discard(None)
            deposit = market.get('depositPrice')
            deposit_free = deposit in (None,'','0','0.00',0,Decimal('0'))
            if len(amounts)==1 and deposit_free:
                item.update(unit_net_eur=str(next(iter(amounts))),price_basis='per_item',price_type='EK',currency='EUR',country='DE')
        elif (context.get('price_semantics_reviewed') is True and context.get('price_type')=='EK'
                and context.get('currency')=='EUR' and context.get('country')=='DE'
                and context.get('price_basis')=='per_item'):
            # Only an explicitly reviewed private schema mapping may type this
            # bare supplier price. A TecDoc UVP/evk price is never substituted.
            net = _decimal(market.get('wholesaler_price'))
            deposit = market.get('depositPrice')
            if net is not None and deposit in (None,'','0','0.00',0,Decimal('0')) and item['stock'] is not None:
                item.update(unit_net_eur=str(net),price_basis='per_item',price_type='EK',currency='EUR',country='DE')
        reviewed = context.get('reviewed_paint_mapping')
        reviewed = reviewed.get(native) if isinstance(reviewed,dict) else None
        is_component = any(re.search(r'halteblech|halter|reparaturblech',a['value'],re.I) for a in attrs)
        if new and body and paint and reviewed==paint and not is_component:
            item['paint_mapping_reviewed']=True
        if (context.get('vehicle_reviewed') is True and context.get('carId') == request['params'].get('carId')
                and request['operation']=='articles' and _text(context.get('vehicle_label'))):
            item.update(fitment_reviewed=True,vehicle_label=_text(context['vehicle_label']))
        result.append(item)
    return result


def _read_json(path, maximum):
    path = api._private_path(path)
    if path.stat().st_size > maximum: raise ValueError('private_input_too_large')
    return api._json(path.read_bytes())


def _identity(row):
    identity = {key:row.get(key,'') for key in ('provider_id','category','name','brand','article_number','ean',
            'unit_label','package_volume_litres','package_volume_reviewed')}
    if str(row.get('provider_id','')).startswith('products:'):
        identity['price_policy'] = PRODUCT_PRICE_POLICY
    return identity


class PartsQuoteService:
    def __init__(self, catalog_path=PRIVATE/'products.json', token_file=LIVE/'product-api-auth.json',
                 ledger_path=RAW/'budget-ledger.json', cache_path=PRIVATE/'quote-cache.json',
                 *, reader_factory=None,token_loader=None,clock=None,sleeper=None,output_root=None):
        self.catalog_path=api._private_path(catalog_path);self.token_file=api._private_path(token_file)
        self.ledger_path=api._private_path(ledger_path);self.cache_path=api._private_path(cache_path)
        self.output_root=api._private_path(output_root or self.cache_path.parent/'quote-read-runs')
        self.reader_factory=reader_factory or api.ReadOnlyCatalogClient;self.token_loader=token_loader or api.load_tokens
        self.clock=clock or (lambda:datetime.now(timezone.utc));self.sleeper=sleeper or time.sleep
        self._signature=None;self._mapping={};self._groups={};self._cache={};self._loaded_cache=False

    def __repr__(self): return '<LocalPartsQuoteService product_GETs_only credentials=private>'
    def _now(self): return _stamp(self.clock())
    def _fresh(self,value):
        quote=catalog.public_quote(value)
        return quote if quote and _stamp(quote['observed_at'])<=self._now()<_stamp(quote['expires_at']) else None

    def _load(self):
        stat=self.catalog_path.stat();signature=(stat.st_mtime_ns,stat.st_size)
        if signature==self._signature:return
        value=_read_json(self.catalog_path,12*1024*1024)
        if not isinstance(value,dict) or value.get('mode')!='reviewed_alzura_parts_local' or not isinstance(value.get('products'),list):
            raise ValueError('private_catalogue_invalid')
        mapping={};groups={}
        for row in value['products']:
            if not isinstance(row,dict):continue
            try:
                public=catalog.public_product(row,now=self._now());request=_request(row.get('source_request'))
                if not str(row.get('provider_id','')).startswith(request['area']+':'):continue
            except (ValueError,TypeError,KeyError):continue
            sid=public['id'];group=sha256(json.dumps(request,sort_keys=True).encode()).hexdigest()
            identity=_identity(row);fingerprint=sha256(json.dumps(identity,sort_keys=True,default=str).encode()).hexdigest()
            if sid in mapping:raise ValueError('catalogue_duplicate_identity')
            mapping[sid]={'row':row,'identity':identity,'request':request,'group':group,'fingerprint':fingerprint,'baseline':public.get('price')}
            groups.setdefault(group,[]).append(sid)
        self._mapping,self._groups,self._signature=mapping,groups,signature
        if not self._loaded_cache:
            self._loaded_cache=True
            try:
                saved=_read_json(self.cache_path,12*1024*1024)
                if saved.get('schema_version')==1 and isinstance(saved.get('entries'),dict):
                    for sid,entry in saved['entries'].items():
                        if not isinstance(sid,str) or not SID.fullmatch(sid) or not isinstance(entry,dict):continue
                        if not re.fullmatch(r'[a-f0-9]{64}',str(entry.get('fingerprint',''))):continue
                        observed=_stamp(entry.get('observed_at'))
                        if observed>self._now():continue
                        quote=catalog.public_quote(entry.get('quote'))
                        self._cache[sid]={'fingerprint':entry['fingerprint'],'quote':quote,'observed_at':observed.isoformat(),
                                          'error':entry.get('error') if entry.get('error') in {'provider_identity_mismatch','provider_variant_mismatch','quote_unavailable'} else 'quote_unavailable'}
            except Exception:pass

    def _cached(self,sid):
        mapping=self._mapping[sid]
        if mapping['row'].get('price_review_required') is True:return None
        entry=self._cache.get(sid);baseline=self._fresh(mapping['baseline'])
        if entry and entry.get('fingerprint')==mapping['fingerprint']:
            if baseline and _stamp(baseline['observed_at'])>_stamp(entry['observed_at']):return baseline
            return self._fresh(entry.get('quote'))
        return baseline

    def _record(self,sid,quote,observed,error=None):
        self._cache[sid]={'fingerprint':self._mapping[sid]['fingerprint'],'quote':quote,
                          'observed_at':observed,'error':error}

    def _wait(self):
        if not self.ledger_path.exists():return
        value=_read_json(self.ledger_path,8*1024*1024)
        attempts=value.get('attempts') if isinstance(value,dict) else None
        if not isinstance(attempts,list):raise api.ReadError('budget_ledger_invalid')
        stamps=[_stamp(entry['at']) for entry in attempts if isinstance(entry,dict) and 'at' in entry]
        if stamps and max(stamps)>self._now()+timedelta(seconds=1):raise api.ReadError('budget_ledger_clock_conflict')
        if stamps:
            delay=api.MIN_INTERVAL-(self._now()-max(stamps)).total_seconds()
            if delay>0:self.sleeper(delay+0.02)

    def get_quotes(self,ids):
        if (not isinstance(ids,list) or not 1<=len(ids)<=8
                or any(not isinstance(sid,str) or not SID.fullmatch(sid) for sid in ids)):
            return {'quotes':{},'errors':{'_request':'invalid_selection_ids'}}
        ids=list(dict.fromkeys(ids));result={'quotes':{sid:None for sid in ids},'errors':{}}
        with LOCK:
            try:self._load()
            except Exception:
                result['errors']={sid:'local_catalogue_unavailable' for sid in ids};return result
            pending=[]
            for sid in ids:
                if sid not in self._mapping:result['errors'][sid]='unknown_selection_id';continue
                if self._mapping[sid]['row'].get('price_review_required') is True:
                    result['errors'][sid]='quote_unavailable';continue
                quote=self._cached(sid)
                if quote:result['quotes'][sid]=quote;continue
                entry=self._cache.get(sid)
                if (entry and entry.get('fingerprint')==self._mapping[sid]['fingerprint'] and not entry.get('quote')
                        and self._now()<_stamp(entry['observed_at'])+timedelta(seconds=30)):
                    result['errors'][sid]=entry.get('error') or 'quote_unavailable';continue
                pending.append(sid)
            if not pending:return result
            try:
                tokens=self.token_loader(self.token_file)
                client=self.reader_factory(tokens,output_dir=self.output_root/uuid4().hex,
                    ledger=api.BudgetLedger(self.ledger_path,monthly_limit=9900,run_limit=8),
                    authorize=lambda area,operation,params:(area,operation) in ALLOWED,
                    clock=self.clock,sleeper=self.sleeper,run_id=uuid4().hex)
            except Exception:
                result['errors'].update({sid:'local_quote_access_unavailable' for sid in pending});return result
            done=set();stopped=None
            for sid in pending:
                mapping=self._mapping[sid];group=mapping['group']
                if group in done:continue
                done.add(group);targets=[key for key in pending if self._mapping[key]['group']==group]
                if stopped:
                    result['errors'].update({key:stopped for key in targets});continue
                observed=self._now().isoformat();found={};duplicates=set();failure=None
                try:
                    self._wait();request=mapping['request']
                    response=client.read(request['area'],request['operation'],request['params'])
                    observed=_stamp(response.summary.get('requested_at')).isoformat()
                    if response.summary.get('ok') is not True:
                        failure=stopped=_safe_error(response.summary.get('error'))
                    else:
                        normalized=(normalize_products(response.data,observed,request) if request['area']=='products'
                                    else normalize_wearparts(response.data,observed,request,mapping['row'].get('source_context')))
                        for row in normalized:
                            native=row['provider_id']
                            if native in found:duplicates.add(native)
                            found[native]=row
                except api.ReadError as error:failure=stopped=_safe_error(error.code)
                except Exception:failure=stopped='provider_response_unusable'
                for key in self._groups[group]:
                    current=self._mapping[key];native=current['row']['provider_id'];row=found.get(native);quote=None;error=failure
                    if not error:
                        if row is None or native in duplicates:error='provider_identity_mismatch'
                        elif _identity(row)!=current['identity']:error='provider_variant_mismatch'
                        else:
                            quote=self._fresh(catalog.public_product(row,now=self._now()).get('price'))
                            if not quote:error='quote_unavailable'
                    self._record(key,quote,observed,error)
                    if key in targets:
                        result['quotes'][key]=quote
                        if error:result['errors'][key]=error
            try:api._atomic_json(self.cache_path,{'schema_version':1,'entries':self._cache})
            except Exception:pass
            return result
