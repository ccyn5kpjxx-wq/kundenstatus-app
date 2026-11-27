"""Premium shop shell, deliberately unregistered and disabled by default.

No catalogue, private evidence, provider clients or customer data are embedded.
The browser uses bounded public projections and the independently gated buyer,
quote and checkout APIs. A return URL is never evidence of payment.
"""
import re
import unicodedata
from urllib.parse import urlencode, urlsplit

from flask import Blueprint, abort, current_app, redirect, render_template, request, url_for
from shop_service_requests import WORKSHOP_SERVICES

SHOP_ORIGIN = 'https://auto-lackierzentrum.de'
SHOP_ALIASES = {
    'https://kundenstatus-app.onrender.com': 'portal',
    'https://www.auto-lackierzentrum.de': 'www',
}

PAGES = {
    'home': ('Felgen & Reifen.', '', 'felgen.webp',
             'Stellen Sie Ihre Räder zusammen. Auf Wunsch mit Montage in unserer Werkstatt in Mosbach.'),
    'felgen': ('Felgen', 'wheels', 'felgen.webp',
               'Designs und Ausführungen entdecken. Fahrzeug auswählen und bestätigte Gutachten prüfen.'),
    'reifen': ('Reifen', 'tyres', 'reifen-groessen-1200.webp',
                'Sommer, Winter oder ganzjährig. Ihre Reifengröße steht seitlich auf dem Reifen.'),
    'komplettraeder': ('Kompletträder.', 'wheels', 'felgen.webp',
                       'Felge auswählen, Reifen vergleichen und Montage in Mosbach ergänzen.'),
    'ersatzteile': ('Das passende Teil. Ihr fertiger Plan.', 'parts', 'teile-werkstatt-hero.png',
                    'Karosserieteile und Fahrzeugkomponenten. Wunschlackierung direkt am Artikel auswählen.'),
    'zubehoer': ('Für Ihr Fahrzeug. Für Ihre Werkstatt.', 'accessories', 'teile-werkstatt-hero.png',
                  'Motoröl, Pflege, Ventile und Gewichte. Passende Werkstattleistung auf Wunsch dazu.'),
    'service': ('Ihr Fahrzeug. Unser Handwerk.', '', 'teile-werkstatt-hero.png',
                'Reifenservice, Ölwechsel, Lackierung und Fahrzeugpflege in Mosbach.'),
    'warenkorb': ('Ihre Auswahl.', '', '',
                   'Produkte, Werkstattleistungen und Versand vor dem Abschluss gemeinsam prüfen.'),
    'felgenstudio': ('Dein Auto. Deine nächste Felge.', '', '',
                     'Seitenfoto wählen, echte Felgendesigns ausprobieren und näher ansehen.'),
}
NAV = [('felgen', 'Felgen'), ('reifen', 'Reifen'), ('komplettraeder', 'Kompletträder'),
       ('ersatzteile', 'Ersatzteile'), ('zubehoer', 'Zubehör'), ('service', 'Werkstattservice')]

# Editorial category photos from the released shop media catalogue.
CATEGORY_PHOTOS = {
    'felgen': 'a17d616282aca5e07599d85f7215a1fba366983c6bb88972d23bfa6fec50049e.png',
    'reifen': '7d8cd2c44ea9fa06a11b6cd233ee4615b94c17972c853d2d571ddf9106c3c11c.jpg',
    'ersatzteile': '03cd824eeb5a9f59e9e29d0837bef5f17774d66ce964f9000c93b0e819e739fb.jpg',
    'zubehoer': '0833c603fa055e0fbe973a3af4e24e448cc058a2b09661ac78289b6c4fbb76b3.jpg',
}


def _link(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 300 or not value.startswith('/') or value.startswith('//'):
        raise ValueError('invalid_shop_link')
    parsed = urlsplit(value)
    if (parsed.scheme or parsed.netloc or parsed.fragment or '\\' in value
            or any(ord(c) < 33 or ord(c) == 127 for c in value)
            or any(part in ('.', '..') for part in parsed.path.split('/'))):
        raise ValueError('invalid_shop_link')
    return value


def _parts_vehicle_options(values):
    """Expose only exact validated public registry labels, never source records."""
    if not isinstance(values,(list,tuple)) or not 1<=len(values)<=512:
        return []
    def text(value,maximum):
        return isinstance(value,str) and 0<len(value)<=maximum and not any(unicodedata.category(c).startswith('C') for c in value)
    def month(value):
        return type(value) is int and 190001<=value<=210012 and 1<=value%100<=12
    result=[];seen=set()
    for vehicle in values:
        if (not isinstance(vehicle,dict) or not {'vehicle_ref','car_id','manufacturer_id','model_id',
                'make','model','engine','engine_codes','date_from_yyyymm','date_to_yyyymm','hsn_tsn',
                'power_kw_from','power_kw_to'}<=vehicle.keys()):return []
        identifier,ref=vehicle.get('car_id'),vehicle.get('vehicle_ref')
        if (type(identifier) is not int or not 1<=identifier<1000000000 or ref!='alzura-vehicle:'+str(identifier)
                or ref in seen or any(type(vehicle.get(key)) is not int or not 1<=vehicle[key]<1000000000 for key in ['manufacturer_id','model_id'])
                or not all(text(vehicle.get(key),200) for key in ['make','model','engine'])
                or not month(vehicle.get('date_from_yyyymm'))
                or vehicle.get('date_to_yyyymm') is not None and (not month(vehicle['date_to_yyyymm']) or vehicle['date_to_yyyymm']<vehicle['date_from_yyyymm'])):
            return []
        codes,keys=vehicle.get('engine_codes'),vehicle.get('hsn_tsn')
        if (not isinstance(codes,list) or not 1<=len(codes)<=20 or any(not text(code,80) for code in codes)
                or not isinstance(keys,list) or not 1<=len(keys)<=100
                or any(not isinstance(key,str) or re.fullmatch(r'[0-9]{4}[A-Z0-9]{3}',key) is None for key in keys)
                or len(set(keys))!=len(keys)):
            return []
        power=[vehicle.get(key) for key in ['power_kw_from','power_kw_to']]
        if any(value is not None and (type(value) is not int or not 1<=value<=2000) for value in power):return []
        if None not in power and power[1]<power[0]:return []
        date=lambda value:f'{value%100:02d}/{value//100:04d}'
        label=' · '.join(part for part in [vehicle['make'],vehicle['model'],vehicle['engine'],
            (' / '.join(codes)) if codes else '',date(vehicle['date_from_yyyymm'])+'–'+(date(vehicle['date_to_yyyymm']) if vehicle.get('date_to_yyyymm') is not None else 'offen'),
            'HSN/TSN '+', '.join(key[:4]+'/'+key[4:] for key in keys) if keys else '',
            (str(power[0])+('–'+str(power[1]) if power[1]!=power[0] else '')+' kW') if None not in power else ''] if part)
        result.append({'vehicle_ref':ref,'label':label});seen.add(ref)
    return result


def create_shop_frontend_blueprint(*, url_prefix='/shop', csrf_token=None,
                                   company_url='/', feature_links=None):
    """Create a shell to register only when replacing the legacy shop routes.

    SHOP_FRONTEND_ENABLED, SHOP_BUYER_ENABLED, SHOP_CART_QUOTE_ENABLED,
    SHOP_ORDER_DRAFT_ENABLED and SHOP_CHECKOUT_ENABLED must be literal True.
    Optional same-origin feature_links: studio, complete_wheels, account,
    service_request, cart_request, terms, privacy, withdrawal. Supply only actual reviewed routes.
    """
    prefix = _link(url_prefix).rstrip('/')
    if not prefix or '?' in prefix:
        raise ValueError('invalid_shop_prefix')
    company = _link(company_url)
    links = dict(feature_links or {})
    if set(links) - {'studio', 'complete_wheels', 'account', 'service_request', 'cart_request', 'terms', 'privacy', 'withdrawal'}:
        raise ValueError('invalid_feature_link')
    links = {key: _link(value) for key, value in links.items()}
    links.setdefault('studio', prefix + '/felgenstudio/')
    bp = Blueprint('shop_frontend', __name__, url_prefix=prefix, template_folder='templates')

    @bp.before_request
    def enabled():
        if current_app.config.get('SHOP_FRONTEND_ENABLED') is not True:
            abort(404)
        if current_app.config.get('SHOP_PUBLIC_ORIGIN') != SHOP_ORIGIN:
            return
        origin = request.host_url.rstrip('/')
        cart_path = url_for('shop_frontend.warenkorb')
        if origin in SHOP_ALIASES:
            if request.endpoint == 'shop_frontend.warenkorb':
                return render_template('shop_v2/cart_transfer.html', source=True, peer_origin=SHOP_ORIGIN,
                                       cart_url=SHOP_ORIGIN + cart_path, alias=SHOP_ALIASES[origin])
            target = SHOP_ORIGIN + url_for(request.endpoint, **(request.view_args or {}))
            # Preserve public catalogue selections and campaign attribution.
            allowed = ('artikel', 'gclid', 'gbraid', 'wbraid', 'utm_source', 'utm_medium', 'utm_campaign',
                       'utm_content', 'utm_term')
            if request.endpoint in {'shop_frontend.felgen', 'shop_frontend.komplettraeder', 'shop_frontend.reifen'}:
                allowed += ('wheel_make', 'wheel_vehicle', 'make', 'model', 'year', 'hsn', 'tsn',
                            'q', 'width', 'ratio', 'diameter', 'season')
            params = [(key, request.args[key]) for key in allowed
                      if key in request.args and len(request.args[key]) <= 256]
            if request.endpoint == 'shop_frontend.reifen' and request.args.getlist('menge') in (['1'], ['2'], ['3'], ['4']):
                params.append(('menge', request.args['menge']))
            if request.endpoint == 'shop_frontend.reifen' and request.args.getlist('montage') == ['1']:
                params.append(('montage', '1'))
            return redirect(target + ('?' + urlencode(params) if params else ''), code=302)
        if origin == SHOP_ORIGIN and request.endpoint == 'shop_frontend.warenkorb':
            sender = next((url for url, alias in SHOP_ALIASES.items() if alias == request.args.get('uebernahme')), None)
            if sender:
                return render_template('shop_v2/cart_transfer.html', source=False, peer_origin=sender,
                                       cart_url=cart_path, alias='')

    @bp.after_request
    def secure_response(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'same-origin'
        response.headers['Content-Security-Policy'] = (
            "default-src 'none'; style-src 'self'; "
            "img-src 'self' blob: https://www.googletagmanager.com https://www.googleadservices.com "
            "https://googleads.g.doubleclick.net https://pagead2.googlesyndication.com "
            "https://www.google.com https://www.google.de; "
            "script-src 'self' https://www.googletagmanager.com https://www.googleadservices.com https://www.google.com; "
            "connect-src 'self' https://www.googletagmanager.com https://www.googleadservices.com "
            "https://googleads.g.doubleclick.net https://pagead2.googlesyndication.com "
            "https://ad.doubleclick.net https://www.google.com https://www.google.de; "
            "frame-src https://www.googletagmanager.com; "
            "form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
        return response

    def page(key, *, order_id='', return_kind=''):
        token = ''
        if callable(csrf_token):
            try:
                token = csrf_token()
            except Exception:
                token = ''
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_\-.:]{16,256}', token):
            token = ''
        values = PAGES.get(key, ('Bestellstatus', '', '', 'Der Status wird ausschließlich vom Server abgerufen.'))
        return render_template('shop_v2/page.html', page_key=key, title=values[0], department=values[1],
                               hero=values[2], description=values[3], navigation=NAV, category_photos=CATEGORY_PHOTOS,
                               company_url=company, feature_links=links, csrf_token=token,
                               workshop_services=WORKSHOP_SERVICES if key == 'service' else {},
                               order_id=order_id, return_kind=return_kind,
                               studio_config={'products': [], 'demo': {
                                   'src': '/static/shop_v2/felgenstudio-demo.png',
                                   'front': {'x': .7798145933014353, 'y': .693836344314559, 'r': .06578947368421052,
                                             'face': {'rxRatio': .8165345454545454, 'ryRatio': .8508072727272727, 'rotation': 3.36}},
                                   'rear': {'x': .2621471291866029, 'y': .6947927736450584, 'r': .06578947368421052,
                                            'face': {'rxRatio': .8020872727272727, 'ryRatio': .83984, 'rotation': 2.71}}}},
                               indexable=(key not in {'warenkorb', 'order_status'}
                                          and current_app.config.get('SHOP_FRONTEND_INDEXABLE') is True),
                               buyer_enabled=current_app.config.get('SHOP_BUYER_ENABLED') is True,
                               mainland_delivery_enabled=current_app.config.get('SHOP_MAINLAND_DELIVERY_ENABLED') is True,
                               deferred_complete_wheels_enabled=current_app.config.get('SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED') is True,
                               parts_direct_enabled=current_app.config.get('SHOP_PARTS_DIRECT_ENABLED') is True,
                               parts_vehicle_options=_parts_vehicle_options(current_app.config.get('SHOP_PARTS_VEHICLE_OPTIONS'))
                                   if current_app.config.get('SHOP_PARTS_DIRECT_ENABLED') is True else [],
                               quote_enabled=current_app.config.get('SHOP_CART_QUOTE_ENABLED') is True,
                               order_enabled=current_app.config.get('SHOP_ORDER_DRAFT_ENABLED') is True,
                               checkout_enabled=current_app.config.get('SHOP_CHECKOUT_ENABLED') is True,
                               consumer_enabled=current_app.config.get('SHOP_CONSUMER_ENABLED') is True)

    @bp.get('/')
    def home():
        return page('home')

    for key in PAGES:
        if key != 'home':
            bp.add_url_rule('/' + key + '/', endpoint=key, view_func=lambda key=key: page(key), methods=['GET'])

    @bp.get('/orders/<string:order_id>/success')
    def order_success(order_id):
        if not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
            abort(404)
        return page('order_status', order_id=order_id, return_kind='success')

    @bp.get('/orders/<string:order_id>/cancel')
    def order_cancel(order_id):
        if not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
            abort(404)
        return page('order_status', order_id=order_id, return_kind='cancel')

    return bp
