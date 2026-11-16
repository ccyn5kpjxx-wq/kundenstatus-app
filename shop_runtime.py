"""Explicit shop registration, with no startup DB, credential or provider IO.

Production dependencies must be supplied by a reviewed bootstrap. Merely
setting an environment variable or reusing the portal's get_db() is not enough:
that connection may be PostgreSQL whereas the current shop journal is SQLite.
"""
from shop_public_api import create_shop_public_blueprint


FLAGS = ('SHOP_PUBLIC_CATALOGUE_ENABLED', 'SHOP_CART_QUOTE_ENABLED',
         'SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED')


def register_shop_runtime(portal, *, catalogue=None, quote_service=None,
                          authorize_quote=None, quote_context=None,
                          quote_sink=None, order_store=None, checkout_service=None):
    app = portal.app
    if 'shop_v2' in app.extensions:
        raise ValueError('shop_already_registered')
    for key in FLAGS:
        app.config.setdefault(key, False)
    requested = {key: app.config.get(key) is True for key in FLAGS}
    prerequisites = {
        'SHOP_PUBLIC_CATALOGUE_ENABLED': catalogue is not None,
        'SHOP_CART_QUOTE_ENABLED': (quote_service is not None and callable(authorize_quote)
                                    and callable(quote_context) and callable(quote_sink)),
        'SHOP_ORDER_DRAFT_ENABLED': order_store is not None,
        'SHOP_CHECKOUT_ENABLED': checkout_service is not None,
    }
    # Later stages require all previous stages; no accidental order/payment
    # activation when a catalogue or trusted session/context adapter is absent.
    previous = True
    for key in FLAGS:
        app.config[key] = previous and requested[key] and prerequisites[key]
        previous = app.config[key]
    app.register_blueprint(create_shop_public_blueprint(
        catalogue=catalogue, quote_service=quote_service,
        authorize_quote=authorize_quote, quote_context=quote_context,
        quote_sink=quote_sink, order_store=order_store,
        checkout_service=checkout_service))
    state = {'enabled': {key: app.config[key] for key in FLAGS},
             'missing_dependencies': [key for key in FLAGS if requested[key] and not prerequisites[key]],
             'startup_provider_calls': 0}
    app.extensions['shop_v2'] = state
    return state
