"""Explicit shop registration, with no startup DB, credential or provider IO.

Production dependencies must be supplied by a reviewed bootstrap. Merely
setting an environment variable or reusing the portal's get_db() is not enough:
that connection may be PostgreSQL whereas the current shop journal is SQLite.
"""
from shop_public_api import create_shop_public_blueprint
from shop_buyer import create_shop_buyer_blueprint
from shop_frontend import create_shop_frontend_blueprint
from shop_media import create_shop_media_blueprint
from shop_consumer import create_consumer_blueprint
from shop_documents import create_shop_documents_blueprint


FLAGS = ('SHOP_PUBLIC_CATALOGUE_ENABLED', 'SHOP_CART_QUOTE_ENABLED',
         'SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED')


def register_shop_runtime(portal, *, catalogue=None, quote_service=None,
                          authorize_quote=None, quote_context=None,
                          quote_sink=None, order_store=None, checkout_service=None,
                          buyer_service=None, feature_links=None, catalogue_warmup=None,
                          media_service=None, consumer_service=None, documents_service=None,
                          workshop_order_view=None):
    app = portal.app
    if 'shop_v2' in app.extensions:
        raise ValueError('shop_already_registered')
    for key in FLAGS:
        app.config.setdefault(key, False)
    app.config.setdefault('SHOP_BUYER_ENABLED', False)
    app.config.setdefault('SHOP_FRONTEND_ENABLED', False)
    app.config.setdefault('SHOP_MEDIA_ENABLED', False)
    app.config.setdefault('SHOP_CONSUMER_ENABLED', False)
    app.config.setdefault('SHOP_DOCUMENTS_ENABLED', False)
    try:
        consumer_ready = consumer_service is not None and consumer_service.readiness()['checkout_ready'] is True
    except Exception:
        consumer_ready = False
    app.config['SHOP_CONSUMER_ENABLED'] = app.config['SHOP_CONSUMER_ENABLED'] is True and consumer_ready
    media_requested = app.config['SHOP_MEDIA_ENABLED'] is True
    buyer_requested = app.config['SHOP_BUYER_ENABLED'] is True
    try:
        buyer_ready = buyer_service is not None and buyer_service.readiness()['ready'] is True
    except Exception:
        buyer_ready = False
    app.config['SHOP_BUYER_ENABLED'] = buyer_requested and buyer_ready
    if app.config['SHOP_BUYER_ENABLED']:
        authorize_quote = buyer_service.authorize_quote
        quote_context = buyer_service.quote_context
    requested = {key: app.config.get(key) is True for key in FLAGS}
    prerequisites = {
        'SHOP_PUBLIC_CATALOGUE_ENABLED': catalogue is not None,
        'SHOP_CART_QUOTE_ENABLED': (quote_service is not None and callable(authorize_quote)
                                    and callable(quote_context) and callable(quote_sink)),
        'SHOP_ORDER_DRAFT_ENABLED': order_store is not None,
        'SHOP_CHECKOUT_ENABLED': checkout_service is not None and app.config['SHOP_CONSUMER_ENABLED'],
    }
    # Later stages require all previous stages; no accidental order/payment
    # activation when a catalogue or trusted session/context adapter is absent.
    previous = True
    for key in FLAGS:
        app.config[key] = previous and requested[key] and prerequisites[key]
        previous = app.config[key]
    app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'] = (
        app.config.get('SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED') is True
        and app.config['SHOP_CHECKOUT_ENABLED'] is True)
    app.register_blueprint(create_shop_public_blueprint(
        catalogue=catalogue, quote_service=quote_service,
        authorize_quote=authorize_quote, quote_context=quote_context,
        quote_sink=quote_sink, order_store=order_store,
        workshop_order_view=workshop_order_view,
        appointment_calendar=getattr(portal, 'freie_reifen_slots', None),
        checkout_service=checkout_service, catalogue_warmup=catalogue_warmup))
    app.config['SHOP_FRONTEND_ENABLED'] = (app.config['SHOP_FRONTEND_ENABLED'] is True
                                          and app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'])
    app.register_blueprint(create_shop_buyer_blueprint(buyer_service))
    app.register_blueprint(create_consumer_blueprint(consumer_service, buyer=buyer_service))
    app.register_blueprint(create_shop_frontend_blueprint(feature_links=feature_links))
    app.config['SHOP_MEDIA_ENABLED'] = (media_requested and media_service is not None
                                        and app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'])
    app.register_blueprint(create_shop_media_blueprint(media_service))
    app.config['SHOP_DOCUMENTS_ENABLED'] = (app.config['SHOP_DOCUMENTS_ENABLED'] is True
        and documents_service is not None and app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'])
    app.register_blueprint(create_shop_documents_blueprint(documents_service))
    state = {'enabled': {key: app.config[key] for key in (*FLAGS, 'SHOP_BUYER_ENABLED', 'SHOP_FRONTEND_ENABLED', 'SHOP_MEDIA_ENABLED', 'SHOP_CONSUMER_ENABLED', 'SHOP_DOCUMENTS_ENABLED')},
             'missing_dependencies': ([key for key in FLAGS if requested[key] and not prerequisites[key]]
                                      + (['SHOP_MEDIA_ENABLED'] if media_requested and media_service is None else [])),
             'startup_provider_calls': 0}
    app.extensions['shop_v2'] = state
    starter = app.extensions.get('shop_production', {}).get('start_workflow')
    if app.config['SHOP_CHECKOUT_ENABLED'] and callable(starter):
        starter()
        # Accounting has its own worker. A slow or unavailable Lexware connection
        # must never hold up supplier confirmation or the payment workflow.
        production = app.extensions.get('shop_production', {})
        if production.get('purchasing_enabled') is True and production.get('store') is not None:
            try:
                from shop_invoice_service import ShopInvoiceService
                from shop_lexware_invoices import LexwareShopInvoices
                invoices = ShopInvoiceService(production['store'], LexwareShopInvoices(portal))
                app.extensions['shop_invoice_service'] = invoices
                invoices.start()
            except Exception:
                app.logger.error('Die Rechnungsautomatik für Shopaufträge konnte nicht gestartet werden.')
    return state
