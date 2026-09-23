"""Run with python -m mos_booking.app. Off unless MOS_BOOKING_MODE is explicit.

This standalone, loopback-only app NEVER modifies production inventory.
"""
from pathlib import Path
import os
import secrets
from urllib.parse import urlsplit
from flask import Flask, abort, jsonify, redirect, render_template, request, session
import stripe
from .gateway import OfflineGateway, StripeTestGateway, verified_event
from .service import BookingService, BookingError, Conflict, ProviderUnavailable


def create_app(config=None):
    app = Flask(__name__)
    app.config.update(
        MODE=os.getenv('MOS_BOOKING_MODE', 'off'),
        DATABASE=os.getenv('MOS_BOOKING_TEST_DB', 'data/booking.mos-test.sqlite3'),
        BASE_URL='http://127.0.0.1:5084',
        SECRET_KEY=os.getenv('MOS_BOOKING_SESSION_SECRET') or secrets.token_hex(32),
        STRIPE_KEY=os.getenv('MOS_STRIPE_TEST_KEY', ''),
        WEBHOOK_SECRET=os.getenv('MOS_STRIPE_WEBHOOK_SECRET', ''),
        ADMIN_TOKEN=os.getenv('MOS_BOOKING_ADMIN_TOKEN', ''),
        # Lax permits the top-level GET back from Stripe; POSTs still require CSRF.
        SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Lax',
        SESSION_COOKIE_NAME='mos_booking_test', MAX_CONTENT_LENGTH=32768,
    )
    if config:
        app.config.update(config)
    if app.config['MODE'] == 'off':
        return app  # No public booking endpoint silently activated by importing this module.
    if app.config['MODE'] not in ('offline', 'stripe_test'):
        raise ValueError('Livebetrieb ist nicht implementiert und bleibt gesperrt.')
    base = urlsplit(app.config['BASE_URL'])
    if base.scheme != 'http' or base.hostname not in ('localhost', '127.0.0.1') or base.path:
        raise ValueError('Testanwendung ausschließlich auf Loopback ohne Pfad starten.')
    path = Path(app.config['DATABASE']).resolve()
    if not path.name.endswith('.mos-test.sqlite3'):
        raise ValueError('Eine getrennte *.mos-test.sqlite3 Datenbank ist Pflicht.')
    path.parent.mkdir(parents=True, exist_ok=True)
    if app.config['MODE'] == 'stripe_test':
        if not app.config['WEBHOOK_SECRET'].startswith('whsec_'):
            raise ValueError('Stripe-Test-Webhook-Secret fehlt.')
        gateway = StripeTestGateway(app.config['STRIPE_KEY'])
    else:
        app.config['WEBHOOK_SECRET'] = app.config['WEBHOOK_SECRET'] or secrets.token_urlsafe(32)
        gateway = OfflineGateway(path, app.config['BASE_URL'], app.config['WEBHOOK_SECRET'])
    service = BookingService(path, gateway, app.config['BASE_URL'])
    service.seed_demo()
    app.extensions['booking'] = service

    @app.before_request
    def guard():
        if request.remote_addr not in ('127.0.0.1', '::1') or request.host != base.netloc:
            abort(403)
        if request.endpoint == 'webhook':
            return
        session.setdefault('owner', secrets.token_urlsafe(32))
        session.setdefault('csrf', secrets.token_urlsafe(32))
        if request.method == 'POST':
            token = request.headers.get('X-CSRF-Token') or request.form.get('csrf', '')
            if not secrets.compare_digest(token, session['csrf']):
                abort(403)

    @app.after_request
    def headers(response):
        response.headers.update({'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer',
                                 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY'})
        return response

    @app.errorhandler(BookingError)
    def invalid(error):
        return jsonify(error=str(error)), 409 if isinstance(error, Conflict) else 400

    @app.errorhandler(ProviderUnavailable)
    def unavailable(error):
        return jsonify(error=str(error)), 503

    def input_data(checkout=False):
        data = request.get_json(silent=True)
        allowed = {'vehicle_id', 'start', 'end', 'km'} | ({'expected_quote_hash'} if checkout else set())
        if not isinstance(data, dict) or set(data) != allowed:
            raise BookingError('Nur Fahrzeug-ID, Zeitraum und Kilometer übermitteln; Preise berechnet der Server.')
        if checkout and (not isinstance(data['expected_quote_hash'], str) or len(data['expected_quote_hash']) != 64):
            raise BookingError('Zuerst den Testpreis prüfen.')
        if any(not isinstance(data[k], str) for k in ('vehicle_id', 'start', 'end')):
            raise BookingError('Fahrzeug-ID und ISO-Datumswerte müssen Text sein.')
        return data

    @app.get('/')
    def index():
        return render_template('test.html', vehicles=service.vehicles(), mode=app.config['MODE'])

    @app.post('/api/quote')
    def quote():
        service.sweep()
        return jsonify(service.preview_quote(**input_data()))

    @app.post('/api/checkout')
    def checkout():
        service.sweep()
        return jsonify(service.create(session['owner'], request.headers.get('Idempotency-Key'), **input_data(checkout=True)))

    @app.get('/booking/<bid>')
    def status(bid):
        booking = service.get(bid, session['owner'])
        return render_template('status.html', booking=booking)

    @app.get('/api/bookings/<bid>')
    def api_status(bid):
        return jsonify(service.get(bid, session['owner']))

    @app.post('/api/bookings/<bid>/cancel')
    def cancel(bid):
        service.get(bid, session['owner'])
        service.reconcile(bid, cancel=True)
        return jsonify(service.get(bid, session['owner']))

    def receive(body, signature):
        try:
            event = verified_event(body, signature, app.config['WEBHOOK_SECRET'])
        except (ValueError, stripe.SignatureVerificationError):
            abort(400)
        try:
            service.handle_event(event)
        except (KeyError, TypeError, AttributeError):
            abort(400)

    @app.post('/webhooks/stripe')
    def webhook():
        receive(request.get_data(cache=False), request.headers.get('Stripe-Signature'))
        return '', 204

    @app.route('/simulate/<sid>', methods=['GET', 'POST'])
    def simulate(sid):
        if app.config['MODE'] != 'offline':
            abort(404)
        try:
            s = gateway.retrieve(sid)
        except ValueError:
            abort(404)
        booking = service.get(s['metadata']['booking_id'], session['owner'])
        if request.method == 'POST':
            try:
                gateway.pay(sid)
            except ValueError as exc:
                raise BookingError(str(exc)) from exc
            receive(*gateway.signed_event(sid))
            return redirect('/booking/' + booking['id'], code=303)
        return render_template('simulate.html', booking=booking)

    @app.route('/admin', methods=['GET', 'POST'])
    def admin():
        if not app.config['ADMIN_TOKEN']:
            return 'Test-Admin ist ohne MOS_BOOKING_ADMIN_TOKEN deaktiviert.', 503
        if request.method == 'POST':
            if not secrets.compare_digest(request.form.get('password', ''), app.config['ADMIN_TOKEN']):
                abort(403)
            session['booking_admin'] = True
            return redirect('/admin', code=303)
        if not session.get('booking_admin'):
            return render_template('admin.html', bookings=None)
        service.sweep()
        return render_template('admin.html', bookings=service.admin_list())

    return app


if __name__ == '__main__':
    create_app().run(host='127.0.0.1', port=5084, debug=False)
