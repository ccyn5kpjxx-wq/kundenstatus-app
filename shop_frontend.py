"""Premium shop shell, deliberately unregistered and disabled by default.

No catalogue, private evidence, provider clients or customer data are embedded.
The browser uses bounded public projections and the independently gated buyer,
quote and checkout APIs. A return URL is never evidence of payment.
"""
import re
from urllib.parse import urlsplit

from flask import Blueprint, abort, current_app, render_template


PAGES = {
    'home': ('Für Ihren Stil. Für jede Saison.', '', 'felgen.webp',
             'Reifen, Felgen und Teile. Persönlicher Service bei uns in Mosbach.'),
    'felgen': ('Felgen', 'wheels', 'felgen.webp',
               'Designs und Ausführungen entdecken. Fahrzeug auswählen und bestätigte Gutachten prüfen.'),
    'reifen': ('Reifen', 'tyres', 'reifen-groessen-hero.png',
                'Sommer, Winter oder ganzjährig. Ihre Reifengröße steht seitlich auf dem Reifen.'),
    'komplettraeder': ('Ihr Rad. Ihre Wahl.', 'wheels', 'felgen.webp',
                       'Mit der Felge beginnen. Reifen und Montage erst nach bestätigtem Abgleich ergänzen.'),
    'ersatzteile': ('Das passende Teil. Ihr fertiger Plan.', 'parts', 'teile-werkstatt-hero.png',
                    'Karosserieteile und Fahrzeugkomponenten. Wunschlackierung direkt am Artikel auswählen.'),
    'zubehoer': ('Für Ihr Fahrzeug. Für Ihre Werkstatt.', 'accessories', 'teile-werkstatt-hero.png',
                  'Motoröl, Pflege, Ventile und Gewichte. Passende Werkstattleistung auf Wunsch dazu.'),
    'service': ('Ihr Fahrzeug. Unser Handwerk.', '', 'teile-werkstatt-hero.png',
                'Reifenservice, Ölwechsel, Lackierung und Fahrzeugpflege in Mosbach.'),
    'warenkorb': ('Ihre Auswahl.', '', '',
                   'Produkte, Werkstattleistungen und Versand vor dem Abschluss gemeinsam prüfen.'),
}
NAV = [('felgen', 'Felgen'), ('reifen', 'Reifen'), ('komplettraeder', 'Kompletträder'),
       ('ersatzteile', 'Ersatzteile'), ('zubehoer', 'Zubehör'), ('service', 'Werkstattservice')]


def _link(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 300 or not value.startswith('/') or value.startswith('//'):
        raise ValueError('invalid_shop_link')
    parsed = urlsplit(value)
    if (parsed.scheme or parsed.netloc or parsed.fragment or '\\' in value
            or any(ord(c) < 33 or ord(c) == 127 for c in value)
            or any(part in ('.', '..') for part in parsed.path.split('/'))):
        raise ValueError('invalid_shop_link')
    return value


def create_shop_frontend_blueprint(*, url_prefix='/shop', csrf_token=None,
                                   company_url='/', feature_links=None):
    """Create a shell to register only when replacing the legacy shop routes.

    SHOP_FRONTEND_ENABLED, SHOP_BUYER_ENABLED, SHOP_CART_QUOTE_ENABLED,
    SHOP_ORDER_DRAFT_ENABLED and SHOP_CHECKOUT_ENABLED must be literal True.
    Optional same-origin feature_links: studio, complete_wheels, account,
    service_request, terms, privacy. Supply only actual reviewed routes.
    """
    prefix = _link(url_prefix).rstrip('/')
    if not prefix or '?' in prefix:
        raise ValueError('invalid_shop_prefix')
    company = _link(company_url)
    links = dict(feature_links or {})
    if set(links) - {'studio', 'complete_wheels', 'account', 'service_request', 'terms', 'privacy'}:
        raise ValueError('invalid_feature_link')
    links = {key: _link(value) for key, value in links.items()}
    bp = Blueprint('shop_frontend', __name__, url_prefix=prefix, template_folder='templates')

    @bp.before_request
    def enabled():
        if current_app.config.get('SHOP_FRONTEND_ENABLED') is not True:
            abort(404)

    @bp.after_request
    def secure_response(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'same-origin'
        response.headers['Content-Security-Policy'] = (
            "default-src 'none'; style-src 'self'; img-src 'self'; script-src 'self'; "
            "connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
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
                               hero=values[2], description=values[3], navigation=NAV,
                               company_url=company, feature_links=links, csrf_token=token,
                               order_id=order_id, return_kind=return_kind,
                               indexable=(key not in {'warenkorb', 'order_status'}
                                          and current_app.config.get('SHOP_FRONTEND_INDEXABLE') is True),
                               buyer_enabled=current_app.config.get('SHOP_BUYER_ENABLED') is True,
                               quote_enabled=current_app.config.get('SHOP_CART_QUOTE_ENABLED') is True,
                               order_enabled=current_app.config.get('SHOP_ORDER_DRAFT_ENABLED') is True,
                               checkout_enabled=current_app.config.get('SHOP_CHECKOUT_ENABLED') is True)

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
