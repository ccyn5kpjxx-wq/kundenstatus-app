"""Bounded public catalogue and authenticated quote boundary.

No provider clients, credentials, supplier orders or payment sessions are created here.
The hosting app must explicitly enable each route and inject its reviewed
catalogue, authoritative quote service and session/CSRF authorization callback.
"""
import json
import re
from urllib.parse import urlsplit

from flask import Blueprint, current_app, request, Response
from shop_order_store import OrderStoreError

PAGE_FIELDS = frozenset({'page', 'page_size', 'q', 'category', 'department',
                         'brand', 'make', 'model', 'year', 'hsn', 'tsn', 'group', 'studio', 'complete',
                         'width', 'ratio', 'diameter'})
MAX_REQUEST = 16384
MAX_RESPONSE = 1024 * 1024
MAX_DETAIL_RESPONSE = 4 * 1024 * 1024


def create_shop_public_blueprint(*, catalogue=None, quote_service=None, authorize_quote=None, quote_context=None, quote_sink=None, order_store=None, checkout_service=None, catalogue_warmup=None):
    """Return an unregistered blueprint; all features default off.

    ``authorize_quote(request)`` must verify the host session and CSRF token and
    return the bound principal ID, or None. Browser IDs/amounts are never used
    as identity or authoritative prices. ``quote_service.quote`` is the pure
    CartQuoteService; only its public projection crosses this boundary.
    ``quote_context(request, principal)`` supplies trusted server-held vehicle
    and delivery context. It cannot be replaced by caller-supplied hashes/IDs.
    Optional ``quote_sink`` persists a READY immutable quote and exact delivery
    context on the server before a later order lookup; it never starts payment.
    """
    bp = Blueprint('shop_public_v2', __name__, url_prefix='/shop/api/v2')

    def reply(body, status=200, *, maximum=MAX_RESPONSE):
        try:
            encoded = json.dumps(body, ensure_ascii=False, allow_nan=False,
                                 separators=(',', ':')).encode('utf-8')
            if len(encoded) > maximum:
                raise ValueError()
        except (TypeError, ValueError, OverflowError):
            encoded, status = b'{"error":"response_unavailable"}', 503
        response = Response(encoded, status=status, content_type='application/json; charset=utf-8')
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    def catalogue_enabled():
        return current_app.config.get('SHOP_PUBLIC_CATALOGUE_ENABLED') is True

    def authorized_principal(*, require_origin=True):
        try:
            origin = current_app.config.get('SHOP_PUBLIC_ORIGIN', '').rstrip('/')
            parsed = urlsplit(origin)
            if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                    or parsed.path or parsed.query or parsed.fragment
                    or request.scheme != 'https' or request.host_url.rstrip('/') != origin
                    or (require_origin and request.headers.get('Origin') != origin)
                    or request.headers.get('Origin') not in (None, origin)
                    or request.headers.get('Sec-Fetch-Site') not in (None, 'same-origin')):
                return None, reply({'error': 'request_not_allowed'}, 403)
        except (AttributeError, ValueError):
            return None, reply({'error': 'request_not_allowed'}, 403)
        if authorize_quote is None:
            return None, reply({'error': 'authorization_unavailable'}, 503)
        try:
            principal = authorize_quote(request)
        except Exception:
            principal = None
        if not isinstance(principal, str) or not re.fullmatch(r'[A-Za-z0-9_:@.-]{1,100}', principal):
            return None, reply({'error': 'authorization_required'}, 403)
        return principal, None

    def json_payload():
        if (request.headers.get('Transfer-Encoding') or request.mimetype != 'application/json'
                or request.content_length is None or not 0 < request.content_length <= MAX_REQUEST):
            raise ValueError()
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result
        payload = json.loads(request.get_data(cache=False), object_pairs_hook=unique,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(payload, dict):
            raise ValueError()
        return payload

    def warm_prices(items, *, quantities=(1, 4)):
        if not callable(catalogue_warmup):
            return False
        identifiers = [row['id'] for row in items[:24] if isinstance(row, dict)
                       and isinstance(row.get('id'), str)]
        try:
            return bool(catalogue_warmup(identifiers, quantities=quantities))
        except Exception:
            return False

    @bp.get('/products')
    def products():
        if not catalogue_enabled():
            return reply({'error': 'not_found'}, 404)
        if catalogue is None:
            return reply({'error': 'catalogue_unavailable'}, 503)
        if set(request.args) - PAGE_FIELDS or any(len(v) != 1 for _, v in request.args.lists()):
            return reply({'error': 'invalid_query'}, 400)
        params = request.args.to_dict()
        if any(len(value) > 160 for value in params.values()):
            return reply({'error': 'invalid_query'}, 400)
        if 'complete' in params and params['complete'] != '1':
            return reply({'error': 'invalid_query'}, 400)
        for field, minimum, maximum in (('width', 100, 455), ('ratio', 10, 100), ('diameter', 8, 30)):
            if field in params and (not re.fullmatch(r'[1-9][0-9]{0,2}', params[field])
                                    or not minimum <= int(params[field]) <= maximum):
                return reply({'error': 'invalid_query'}, 400)
        for field in ('page', 'page_size', 'year'):
            if field in params:
                if not re.fullmatch(r'[0-9]{1,5}', params[field]):
                    return reply({'error': 'invalid_query'}, 400)
                params[field] = int(params[field])
        try:
            result = catalogue.page(**params)
            if callable(catalogue_warmup) and isinstance(result, dict) and isinstance(result.get('items'), list):
                pending = warm_prices(result['items'])
                if not pending:
                    # The background worker may have completed between the
                    # initial cache projection and the warmup decision.
                    result = catalogue.page(**params)
                result['prices_refreshing'] = pending
            return reply(result)
        except ValueError:
            return reply({'error': 'invalid_query'}, 400)
        except Exception:
            return reply({'error': 'catalogue_unavailable'}, 503)

    @bp.get('/products/<string:product_id>')
    def product(product_id):
        if not catalogue_enabled():
            return reply({'error': 'not_found'}, 404)
        if catalogue is None:
            return reply({'error': 'catalogue_unavailable'}, 503)
        if (set(request.args) - {'quantity'} or any(len(v) != 1 for _, v in request.args.lists())
                or len(product_id) > 100):
            return reply({'error': 'invalid_query'}, 400)
        quantity = request.args.get('quantity')
        if quantity is not None and (not re.fullmatch(r'[0-9]{1,2}', quantity) or not 1 <= int(quantity) <= 20):
            return reply({'error': 'invalid_query'}, 400)
        try:
            value = catalogue.get(product_id)
        except ValueError:
            return reply({'error': 'invalid_product_id'}, 400)
        except Exception:
            return reply({'error': 'catalogue_unavailable'}, 503)
        if value is None:
            return reply({'error': 'product_not_found'}, 404)
        if quantity is not None and value.get('category') in {'tyre', 'alloy_wheel', 'steel_wheel'} and int(quantity) > 4:
            return reply({'error': 'invalid_query'}, 400)
        result = {'item': value}
        if callable(catalogue_warmup):
            pending = warm_prices([value], quantities=(int(quantity),) if quantity else (1, 4))
            if not pending:
                try:
                    value = catalogue.get(product_id)
                except Exception:
                    return reply({'error': 'catalogue_unavailable'}, 503)
                if value is None:
                    return reply({'error': 'product_not_found'}, 404)
                result['item'] = value
            result['prices_refreshing'] = pending
        return reply(result, maximum=MAX_DETAIL_RESPONSE)

    @bp.get('/products/<string:product_id>/variants')
    def product_variants(product_id):
        if not catalogue_enabled():
            return reply({'error': 'not_found'}, 404)
        if not callable(getattr(catalogue, 'variants', None)):
            return reply({'error': 'catalogue_unavailable'}, 503)
        if set(request.args) - {'page', 'page_size', 'diameter', 'complete'} or any(len(v) != 1 for _, v in request.args.lists()):
            return reply({'error': 'invalid_query'}, 400)
        try:
            params = {}
            for key, value in request.args.items():
                if key == 'complete':
                    if value != '1':
                        raise ValueError()
                    params[key] = value
                    continue
                if not re.fullmatch(r'[0-9]{1,5}', value):
                    raise ValueError()
                params[key] = value if key == 'diameter' else int(value)
            result = catalogue.variants(product_id, **params)
            if result is None:
                return reply({'error': 'product_not_found'}, 404)
            pending = warm_prices(result['items']) if callable(catalogue_warmup) else False
            if not pending:
                result = catalogue.variants(product_id, **params)
            result['prices_refreshing'] = pending
            return reply(result)
        except ValueError:
            return reply({'error': 'invalid_query'}, 400)
        except Exception:
            return reply({'error': 'catalogue_unavailable'}, 503)

    @bp.get('/products/<string:product_id>/compatible-tyres')
    def compatible_tyres(product_id):
        if not catalogue_enabled():
            return reply({'error': 'not_found'}, 404)
        if not callable(getattr(catalogue, 'compatible_tyres', None)):
            return reply({'rim_id': product_id, 'items': [], 'scope': 'blocked',
                          'reason': 'Passende Reifenausführungen werden geprüft.'})
        if (set(request.args) - {'quantity', 'season', 'tyre_size', 'make', 'model', 'year', 'hsn', 'tsn'}
                or any(len(v) != 1 for _, v in request.args.lists())
                or any(len(v) > 160 for v in request.args.values())):
            return reply({'error': 'invalid_query'}, 400)
        try:
            params = request.args.to_dict()
            for key in ('quantity', 'year'):
                if key in params:
                    if not re.fullmatch(r'[0-9]{1,5}', params[key]):
                        raise ValueError()
                    params[key] = int(params[key])
            result = catalogue.compatible_tyres(product_id, **params)
            if result is None:
                return reply({'error': 'product_not_found'}, 404)
            pending = warm_prices(result['items'], quantities=(params.get('quantity', 4),)) if callable(catalogue_warmup) else False
            if not pending:
                result = catalogue.compatible_tyres(product_id, **params)
            result['prices_refreshing'] = pending
            return reply(result, maximum=MAX_DETAIL_RESPONSE)
        except ValueError:
            return reply({'error': 'invalid_query'}, 400)
        except Exception:
            return reply({'error': 'catalogue_unavailable'}, 503)

    @bp.get('/products/<string:product_id>/complete-wheel-price')
    def complete_wheel_price(product_id):
        if not catalogue_enabled():
            return reply({'error': 'not_found'}, 404)
        if not callable(getattr(catalogue, 'complete_wheel_price', None)):
            return reply({'error': 'catalogue_unavailable'}, 503)
        args = request.args
        if (set(args) - {'tyre_id', 'quantity', 'runflat', 'disposal'}
                or any(len(values) != 1 for _, values in args.lists())
                or not re.fullmatch(r'(?:alloy_wheel|steel_wheel):[a-f0-9]{24}', product_id)
                or not re.fullmatch(r'tyre:[a-f0-9]{24}', args.get('tyre_id', ''))
                or not re.fullmatch(r'[1-4]', args.get('quantity', ''))
                or any(args[key] != '1' for key in ('runflat', 'disposal') if key in args)):
            return reply({'error': 'invalid_query'}, 400)
        try:
            result = catalogue.complete_wheel_price(product_id, args['tyre_id'], quantity=int(args['quantity']),
                                                    runflat='runflat' in args, disposal='disposal' in args)
        except Exception:
            return reply({'error': 'catalogue_unavailable'}, 503)
        if result is None:
            return reply({'error': 'combination_not_found'}, 404)
        return reply(result)

    @bp.post('/cart/quote')
    def cart_quote():
        if not catalogue_enabled() or current_app.config.get('SHOP_CART_QUOTE_ENABLED') is not True:
            return reply({'error': 'not_found'}, 404)
        principal, error = authorized_principal()
        if error is not None:
            return error
        if quote_service is None or quote_context is None:
            return reply({'error': 'quote_unavailable'}, 503)
        try:
            payload = json_payload()
            context = quote_context(request, principal)
            if (not isinstance(context, dict) or set(context) - {'vehicle_ref', 'delivery_context'}
                    or ('vehicle_ref' in context and not isinstance(context['vehicle_ref'], str))
                    or ('delivery_context' in context and not isinstance(context['delivery_context'], dict))):
                return reply({'error': 'quote_unavailable'}, 503)
            quote = quote_service.quote(payload, customer_id=principal, **context)
            result = quote.public()
            if not isinstance(result, dict):
                raise TypeError()
            retained = False
            if result.get('ready') is True and quote_sink is not None:
                quote_sink(quote, customer_id=principal, delivery_context=context.get('delivery_context', {}))
                retained = True
        except OrderStoreError:
            return reply({'error': 'quote_unavailable'}, 503)
        except (ValueError, UnicodeError):
            return reply({'error': 'invalid_request'}, 400)
        except Exception:
            return reply({'error': 'quote_unavailable'}, 503)
        # A validated quote is not an order, a payment or a supplier reservation.
        return reply({'quote': result, 'quote_retained': retained,
                      'order_created': False, 'payment_enabled': False})

    def orders_enabled():
        return catalogue_enabled() and current_app.config.get('SHOP_ORDER_DRAFT_ENABLED') is True

    def checkout_enabled():
        return orders_enabled() and current_app.config.get('SHOP_CHECKOUT_ENABLED') is True

    def checkout_ready():
        if checkout_service is None or checkout_service.mode != current_app.config.get('SHOP_ORDER_MODE'):
            return False
        ready = checkout_service.readiness()
        return (isinstance(ready, dict) and ready.get('ready') is True
                and ready.get('mode') == current_app.config.get('SHOP_ORDER_MODE')
                and ready.get('capture_method') == 'manual' and ready.get('payment_methods') == ['card'])

    def payment_available():
        try:
            return checkout_enabled() and checkout_ready()
        except Exception:
            return False

    def public_order(row):
        fields = ('id', 'mode', 'quote_hash', 'amount_gross_cent', 'currency',
                  'created_at', 'quote_expires_at', 'status')
        return {key: row[key] for key in fields}

    @bp.post('/orders')
    def create_order_draft():
        if not orders_enabled():
            return reply({'error': 'not_found'}, 404)
        principal, error = authorized_principal()
        if error is not None:
            return error
        mode = current_app.config.get('SHOP_ORDER_MODE')
        if order_store is None or mode not in {'test', 'live'}:
            return reply({'error': 'order_unavailable'}, 503)
        try:
            payload = json_payload()
            if (set(payload) != {'quote_hash', 'idempotency_key'}
                    or not isinstance(payload['quote_hash'], str)
                    or not re.fullmatch(r'[a-f0-9]{64}', payload['quote_hash'])
                    or not isinstance(payload['idempotency_key'], str)
                    or not re.fullmatch(r'[A-Za-z0-9_:-]{8,128}', payload['idempotency_key'])):
                raise ValueError()
        except (ValueError, UnicodeError, RecursionError):
            return reply({'error': 'invalid_request'}, 400)
        try:
            order = order_store.create_order(payload['quote_hash'], customer_id=principal,
                mode=mode, idempotency_key=payload['idempotency_key'])
            return reply({'order': public_order(order), 'payment_enabled': payment_available(),
                          'supplier_order_created': False})
        except OrderStoreError as error:
            if error.code in {'idempotency_conflict', 'quote_already_ordered', 'quote_not_found',
                              'quote_expired', 'quote_expired_or_order_closed'}:
                return reply({'error': 'quote_unavailable'}, 409)
            return reply({'error': 'order_unavailable'}, 503)
        except Exception:
            return reply({'error': 'order_unavailable'}, 503)

    @bp.get('/orders/<string:order_id>')
    def order_status(order_id):
        if not orders_enabled():
            return reply({'error': 'not_found'}, 404)
        principal, error = authorized_principal(require_origin=False)
        if error is not None:
            return error
        mode = current_app.config.get('SHOP_ORDER_MODE')
        if order_store is None or mode not in {'test', 'live'}:
            return reply({'error': 'order_unavailable'}, 503)
        if request.args:
            return reply({'error': 'invalid_query'}, 400)
        try:
            result = {'order': public_order(order_store.get_order(order_id, customer_id=principal, mode=mode)),
                      'payment_enabled': payment_available()}
            if getattr(checkout_service, 'mode', None) == mode and callable(getattr(checkout_service, 'status', None)):
                retained = checkout_service.status(order_id, customer_id=principal)
                technical = retained.get('technical_review') if isinstance(retained, dict) else None
                if (isinstance(technical, dict) and technical.get('required') is True
                        and technical.get('state') in {'pending', 'approved', 'rejected', 'expired'}
                        and (technical.get('expires_at') is None or
                             isinstance(technical.get('expires_at'), str) and len(technical['expires_at']) <= 50)):
                    result['technical_review'] = {key: technical.get(key) for key in ('required', 'state', 'expires_at')}
            return reply(result)
        except OrderStoreError as error:
            if error.code == 'order_not_found':
                return reply({'error': 'order_not_found'}, 404)
            return reply({'error': 'order_unavailable'}, 503)
        except Exception:
            return reply({'error': 'order_unavailable'}, 503)

    @bp.get('/orders/<string:order_id>/measurement')
    def order_measurement(order_id):
        if not orders_enabled():
            return reply({'error': 'not_found'}, 404)
        principal, error = authorized_principal(require_origin=False)
        if error is not None:
            return error
        mode = current_app.config.get('SHOP_ORDER_MODE')
        if (order_store is None or mode not in {'test', 'live'}
                or not callable(getattr(order_store, 'purchase_conversion', None))):
            return reply({'error': 'measurement_unavailable'}, 503)
        if request.args:
            return reply({'error': 'invalid_query'}, 400)
        try:
            purchase = order_store.purchase_conversion(order_id, customer_id=principal, mode=mode)
            # Only the bounded projection may cross the HTTP boundary. Never
            # expose payment IDs, a quote snapshot or a provider response.
            if purchase is not None:
                if (mode != 'live' or not isinstance(purchase, dict)
                        or set(purchase) != {'transaction_id', 'value', 'currency'}
                        or not isinstance(purchase['transaction_id'], str)
                        or not re.fullmatch(r'shop_ads_[a-f0-9]{64}', purchase['transaction_id'])
                        or type(purchase['value']) not in (int, float)
                        or not 0 < purchase['value'] <= 1_000_000 or purchase['currency'] != 'EUR'):
                    raise ValueError()
            return reply({'purchase': purchase})
        except OrderStoreError as error:
            if error.code == 'order_not_found':
                return reply({'error': 'order_not_found'}, 404)
            return reply({'error': 'measurement_unavailable'}, 503)
        except Exception:
            return reply({'error': 'measurement_unavailable'}, 503)

    @bp.post('/orders/<string:order_id>/checkout')
    def start_checkout(order_id):
        if not checkout_enabled():
            return reply({'error': 'not_found'}, 404)
        principal, error = authorized_principal()
        if error is not None:
            return error
        try:
            payload = json_payload()
            if payload or request.args or not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
                raise ValueError()
        except (ValueError, UnicodeError, RecursionError):
            return reply({'error': 'invalid_request'}, 400)
        try:
            if not checkout_ready():
                return reply({'error': 'checkout_unavailable'}, 503)
            result = checkout_service.start(order_id, customer_id=principal)
            if not isinstance(result, dict) or result.get('order_id') != order_id:
                raise ValueError()
            # Private order/delivery/SDK data never crosses this endpoint.
            fields = ('order_id', 'status', 'mode', 'currency', 'amount_gross_cent',
                      'checkout_url', 'payment_state', 'authorization_confirmed',
                      'payment_confirmed', 'capture_triggered', 'supplier_order_created')
            public = {key: result[key] for key in fields}
            url = public['checkout_url']
            if url is not None:
                parsed = urlsplit(url)
                if (not isinstance(url, str) or len(url) > 5000 or '\\' in url
                        or any(ord(c) < 33 for c in url) or parsed.scheme != 'https'
                        or parsed.hostname != 'checkout.stripe.com' or parsed.port not in (None, 443)
                        or parsed.username or parsed.password):
                    raise ValueError()
            if (public['mode'] != current_app.config.get('SHOP_ORDER_MODE') or public['currency'] != 'eur'
                    or type(public['amount_gross_cent']) is not int or not 0 < public['amount_gross_cent'] <= 100_000_000
                    or public['capture_triggered'] is not False or public['supplier_order_created'] is not False):
                raise ValueError()
            return reply({'checkout': public})
        except Exception:
            # A failed/unknown create is retained by ShopCheckout. Do not advise
            # the browser to retry a new payment or disclose provider details.
            return reply({'error': 'checkout_unavailable'}, 503)

    @bp.post('/payments/stripe/webhook')
    def stripe_webhook():
        if not checkout_enabled():
            return reply({'error': 'not_found'}, 404)
        origin = current_app.config.get('SHOP_PUBLIC_ORIGIN', '').rstrip('/')
        if request.scheme != 'https' or request.host_url.rstrip('/') != origin:
            return reply({'error': 'request_not_allowed'}, 403)
        if (request.args or request.headers.get('Transfer-Encoding')
                or request.mimetype != 'application/json' or request.content_length is None
                or not 0 < request.content_length <= 262144):
            return reply({'error': 'invalid_webhook'}, 400)
        signature = request.headers.get('Stripe-Signature')
        if not isinstance(signature, str) or not 1 <= len(signature) <= 4096:
            return reply({'error': 'invalid_webhook'}, 400)
        try:
            if not checkout_ready():
                return reply({'error': 'checkout_unavailable'}, 503)
            result = checkout_service.handle_webhook(request.get_data(cache=False), signature)
            if not isinstance(result, dict) or result.get('received') is not True or type(result.get('ignored')) is not bool:
                raise ValueError()
            # Acknowledge only after the signature and binding checks/ledger
            # operation. Payment acceptance is not inferred from this response.
            return reply({'received': True, 'ignored': result['ignored']})
        except Exception as error:
            if getattr(error, 'code', None) in {'invalid_webhook', 'invalid_signature'}:
                return reply({'error': 'invalid_webhook'}, 400)
            return reply({'error': 'checkout_unavailable'}, 503)

    return bp
