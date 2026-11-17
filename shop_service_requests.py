"""Connect shop service buttons to the existing persistent workshop intake.

This adapter opens a form only. Its GET never reserves capacity or contacts a
customer. The existing /anfrage handler remains responsible for CSRF, rate
limits, explicit customer submission, slot reservation, leads, protected admin
views, confirmation notices and backups. Purchased-goods arrival is a separate
concern; a shop cart must never trigger an appointment implicitly.
"""

SERVICE_REQUEST_PATH = '/anfrage?anliegen=reifenservice'
CART_REQUEST_PATH = '/anfrage?anliegen=allgemein&shop_anfrage=1'


def prepare_service_request_links(portal, *, enabled=False):
    """Return frontend feature links only for the real persistent intake.

    Explicit activation is required. A stateless PUBLIC_SITE_ONLY service must
    not advertise its locally blocked POST route. This helper deliberately
    avoids credentials, databases, request/session values, external URLs and
    any provider or notification callback. No user-controlled URL is accepted.
    """
    if enabled is not True or getattr(portal, 'PUBLIC_SITE_ONLY', None) is not False:
        return {}
    app = getattr(portal, 'app', None)
    if app is None or not callable(getattr(app, 'view_functions', {}).get('website_anfrage')):
        return {}
    try:
        rules = app.url_map.iter_rules('website_anfrage')
        available = any(rule.rule == '/anfrage' and not rule.arguments
                        and {'GET', 'POST'} <= rule.methods for rule in rules)
    except (AttributeError, KeyError, TypeError):
        return {}
    return {'service_request': SERVICE_REQUEST_PATH, 'cart_request': CART_REQUEST_PATH} if available else {}
