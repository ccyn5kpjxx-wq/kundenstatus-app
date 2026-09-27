"""Admin order management and opt-in durable dispatch worker.

Register once with register_orders(portal); it starts no thread and sends no mail.
ASSISTANT_ORDER_SEND_ENABLED and existing MAILBOX_SEND_ENABLED must both be true.
Weekly operation additionally needs ASSISTANT_ORDER_WORKER_ENABLED and a live
worker: flask --app app werkstatt-bestellungen-worker [--once] [--interval 30].
Use the same persistent DB and MAILBOX_OUTBOX_DIR on web and worker processes.
Existing portal IMAP/SMTP callbacks supply the unchanged mailbox identity.

There is no general portal scheduler. The existing MOS mail worker is isolated
to its own outbox, so this module exposes tick() and a dedicated opt-in CLI loop.
The worker only dispatches immutable, explicitly authorized queued requests.
Invoice imports may call propose_contact(), which NEVER verifies a contact.
No model-facing write API is registered. Employee-specific auth is not added;
this first management surface is exclusively for the existing admin session.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hmac
import os
from pathlib import Path
import re
import secrets
import time
import uuid

import click
from flask import Blueprint, abort, flash, g, has_request_context, redirect, render_template, request, session, url_for

from werkstatt_artikel_identity import parse_unit_price
from werkstatt_bestellausgang import build_order_dispatch
from werkstatt_bestellplan import BERLIN


def _now():
    return datetime.now(timezone.utc)


def _text(value, label, limit=300, optional=False):
    value = value.strip() if isinstance(value, str) else ''
    if (not value and not optional) or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ValueError(f'{label} fehlt oder ist ungültig.')
    return value


def _email(value, optional=False):
    value = _text(value, 'Bestelladresse', 254, optional)
    if not value and optional:
        return ''
    if not re.fullmatch(r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}", value):
        raise ValueError('Eine einzelne gültige Bestelladresse angeben.')
    local, domain = value.rsplit('@', 1)
    return local + '@' + domain.lower()


def _cents(value, label, positive=False):
    price = parse_unit_price(value)
    if price is None or price.as_tuple().exponent < -2 or price > Decimal('10000000') or (positive and price <= 0):
        raise ValueError(f'{label} eindeutig in EUR angeben; unbekannte Beträge bleiben gesperrt.')
    return int(price * 100)


class OrderManagement:
    def __init__(self, portal):
        self.p = portal
        self.app = portal.app
        for key in ('ASSISTANT_ORDER_SEND_ENABLED', 'ASSISTANT_ORDER_WORKER_ENABLED'):
            self.app.config.setdefault(key, os.environ.get(key, '').casefold() in {'1', 'true', 'yes'})
        with self.db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_bestellkontakte (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, recipient TEXT NOT NULL DEFAULT '',
                source_note TEXT NOT NULL DEFAULT '', verified_at TEXT NOT NULL DEFAULT '',
                verified_by TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 1)''')
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_bestellkonfiguration (
                setting_key TEXT PRIMARY KEY, setting_value TEXT NOT NULL)''')
            db.commit()
        storage = self.app.config.get('MAILBOX_OUTBOX_DIR') or str(Path(self.app.instance_path) / 'mail_outbox')
        self.storage_dir = Path(storage).resolve()
        self.dispatch = build_order_dispatch(portal.get_db, storage, portal.get_werkstatt_imap_config,
                                             portal.get_werkstatt_smtp_config, self.authorize_order,
                                             self.resolve_supplier, lambda: _now())

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def setting(self, key, default=''):
        with self.db() as db:
            row = db.execute('SELECT setting_value FROM assistent_bestellkonfiguration WHERE setting_key=?', (key,)).fetchone()
        return row['setting_value'] if row else default

    def set_setting(self, key, value):
        with self.db() as db:
            db.execute('''INSERT INTO assistent_bestellkonfiguration(setting_key,setting_value) VALUES(?,?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value''', (key, str(value)))
            db.commit()

    def cap(self):
        try:
            return max(0, int(self.setting('max_total_cents', '0')))
        except ValueError:
            return 0

    def contacts(self):
        with self.db() as db:
            return [dict(row) for row in db.execute('SELECT * FROM assistent_bestellkontakte ORDER BY name,id').fetchall()]

    def propose_contact(self, name, recipient='', source_note='', contact_id=None):
        """Manual or invoice-derived contact evidence always enters unverified."""
        name = _text(name, 'Lieferant')
        recipient = _email(recipient, optional=True)
        note = _text(source_note, 'Quelle', 500, optional=True)
        with self.db() as db:
            if contact_id:
                changed = db.execute('''UPDATE assistent_bestellkontakte SET name=?,recipient=?,source_note=?,
                    verified_at='',verified_by='',revision=revision+1 WHERE id=?''', (name, recipient, note, contact_id))
                if changed.rowcount != 1:
                    raise ValueError('Lieferantenkontakt nicht gefunden.')
            else:
                contact_id = str(uuid.uuid4())
                db.execute('INSERT INTO assistent_bestellkontakte(id,name,recipient,source_note) VALUES(?,?,?,?)',
                           (contact_id, name, recipient, note))
            db.commit()
        return contact_id

    def verify_contact(self, contact_id, revision):
        with self.db() as db:
            row = db.execute('SELECT * FROM assistent_bestellkontakte WHERE id=?', (contact_id,)).fetchone()
            if not row or str(row['revision']) != str(revision):
                raise ValueError('Kontakt wurde geändert. Bitte den aktuellen Vorschlag prüfen.')
            _email(row['recipient'])
            changed = db.execute('UPDATE assistent_bestellkontakte SET verified_at=?,verified_by=? WHERE id=? AND revision=?',
                                 (_now().isoformat(), 'admin', contact_id, row['revision']))
            if changed.rowcount != 1:
                raise ValueError('Kontakt wurde geändert. Bitte den aktuellen Vorschlag prüfen.')
            db.commit()

    def resolve_supplier(self, contact_id):
        with self.db() as db:
            row = db.execute('SELECT * FROM assistent_bestellkontakte WHERE id=?', (contact_id,)).fetchone()
        if not row:
            return None
        return {'id': row['id'], 'name': row['name'], 'recipient': row['recipient'],
                'verified': bool(row['verified_at'] and row['verified_by'])}

    def availability(self):
        smtp = self.p.get_werkstatt_smtp_config()
        imap = self.p.get_werkstatt_imap_config()
        mailbox_ready = bool(smtp.get('smtp_configured') and (smtp.get('smtp_ssl') or smtp.get('smtp_tls'))
                             and imap.get('configured') and imap.get('ssl') and imap.get('user')
                             and str(imap.get('user')).casefold() == str(smtp.get('smtp_user')).casefold())
        enabled = bool(self.app.config.get('ASSISTANT_ORDER_SEND_ENABLED') and self.app.config.get('MAILBOX_SEND_ENABLED'))
        configured_storage = self.app.config.get('MAILBOX_OUTBOX_DIR')
        storage_ready = bool(configured_storage and Path(configured_storage).is_absolute()
                             and Path(configured_storage).resolve() == self.storage_dir)
        worker_enabled = bool(self.app.config.get('ASSISTANT_ORDER_WORKER_ENABLED'))
        try:
            age = _now().timestamp() - float(self.setting('worker_last_ok', '0'))
            live = 0 <= age <= 180
        except ValueError:
            live = False
        return {'enabled': enabled, 'mailbox_ready': mailbox_ready, 'storage_ready': storage_ready,
                'sender': str(smtp.get('from_address') or ''),
                'worker_enabled': worker_enabled, 'worker_live': worker_enabled and live,
                'can_send': enabled and mailbox_ready and storage_ready}

    def authorize_order(self, actor_id, intent):
        # The trusted route creates this request-local permit after validating the
        # server nonce, admin session, deliberate submit and explicit price check.
        return bool(has_request_context() and actor_id == 'admin' and session.get('admin') and request.method == 'POST'
                    and request.endpoint == 'werkstatt_orders.create_order'
                    and getattr(g, 'assistant_order_request', None) == intent.get('id')
                    and 0 < intent.get('max_total_cents', 0) <= self.cap()
                    and self.availability()['can_send'])

    def tick(self, *, worker=False):
        state = self.availability()
        if not state['can_send']:
            raise ValueError('Bestellversand, bestehendes Postfach oder dauerhafter Ausgabespeicher sind noch nicht eingerichtet.')
        if worker and not state['worker_enabled']:
            raise ValueError('Der automatische Bestellworker ist nicht aktiviert.')
        result = self.dispatch.dispatch_due()
        self.set_setting('last_tick', _now().isoformat())
        if worker:
            self.set_setting('worker_last_ok', _now().timestamp())
        return result


def run_worker(manager, *, once=False, interval=30, sleeper=time.sleep):
    """Dedicated reentrant process; never started implicitly by Flask."""
    if not 10 <= interval <= 120:
        raise ValueError('Worker-Intervall muss zwischen 10 und 120 Sekunden liegen.')
    while True:
        manager.tick(worker=True)
        if once:
            return
        sleeper(interval)


def register_orders(portal):
    app = portal.app
    if 'werkstatt_orders' in app.extensions:
        return app.extensions['werkstatt_orders']
    manager = OrderManagement(portal)
    bp = Blueprint('werkstatt_orders', __name__, url_prefix='/admin/assistent-bestellungen')

    @bp.before_request
    def require_admin_and_csrf():
        if not session.get('admin'):
            abort(403)
        if request.method == 'POST':
            expected = session.get('csrf_token')
            supplied = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
            if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
                abort(400, description='Die Seite ist veraltet. Bitte neu öffnen.')

    def page(errors=None, form=None, code=200):
        form = dict(form or {})
        csrf = session.get('csrf_token')
        if not csrf:
            csrf = secrets.token_urlsafe(32)
            session['csrf_token'] = csrf
        pending = dict(session.get('assistant_order_requests') or {})
        request_id = form.get('request_id')
        if request_id not in pending:
            request_id = str(uuid.uuid4())
            pending[request_id] = True
            session['assistant_order_requests'] = dict(list(pending.items())[-20:])
        orders = manager.dispatch.list_orders()
        for order in orders:
            order['due_label'] = datetime.fromisoformat(order['due_at']).astimezone(BERLIN).strftime('%d.%m.%Y %H:%M')
        return render_template('assistent_bestellungen.html', contacts=manager.contacts(), availability=manager.availability(),
                               orders=orders, cap_cents=manager.cap(), csrf=csrf, request_id=request_id,
                               errors=errors or [], form=form), code

    @bp.get('')
    @portal.admin_required
    def index():
        return page()

    @bp.post('/kontakt')
    @portal.admin_required
    def contact_proposal():
        try:
            manager.propose_contact(request.form.get('name'), request.form.get('recipient'),
                                    request.form.get('source_note'), request.form.get('contact_id') or None)
        except ValueError as exc:
            return page([str(exc)], code=400)
        flash('Lieferantenkontakt als ungeprüfter Vorschlag gespeichert.', 'success')
        return redirect(url_for('werkstatt_orders.index'), code=303)

    @bp.post('/kontakt/<contact_id>/bestaetigen')
    @portal.admin_required
    def confirm_contact(contact_id):
        try:
            if request.form.get('contact_confirmed') != 'ja':
                raise ValueError('Bestätigen, dass die angezeigte Adresse Bestellungen für diesen Lieferanten annimmt.')
            manager.verify_contact(contact_id, request.form.get('revision'))
        except ValueError as exc:
            return page([str(exc)], code=400)
        flash('Bestelladresse durch den Admin bestätigt.', 'success')
        return redirect(url_for('werkstatt_orders.index'), code=303)

    @bp.post('/kostenrahmen')
    @portal.admin_required
    def set_cap():
        try:
            manager.set_setting('max_total_cents', _cents(request.form.get('max_total'), 'Positiven Kostenrahmen', positive=True))
        except ValueError as exc:
            return page([str(exc)], code=400)
        flash('Kostenrahmen für neue Bestellanforderungen gespeichert.', 'success')
        return redirect(url_for('werkstatt_orders.index'), code=303)

    @bp.post('/bestellen')
    @portal.admin_required
    def create_order():
        form = request.form
        try:
            request_id = form.get('request_id')
            if request_id not in (session.get('assistant_order_requests') or {}) or form.get('action') != 'bestellen':
                raise ValueError('Bestellung über das aktuelle Bestellformular ausdrücklich auslösen.')
            if form.get('price_confirmed') != 'ja':
                raise ValueError('Genauen Artikel, Brutto-Stückpreis und Versandkosten prüfen und bestätigen.')
            if form.get('urgency') not in {'urgent', 'weekly'}:
                raise ValueError('Dringlichkeit auswählen: sofort oder gesammelt am Montag.')
            available = manager.availability()
            if not available['can_send']:
                raise ValueError('Versand, bestehendes Postfach oder dauerhafter Ausgabespeicher sind noch nicht eingerichtet.')
            urgent = form['urgency'] == 'urgent'
            if not urgent and not available['worker_live']:
                raise ValueError('Der Montagsversand läuft noch nicht. Zuerst den Bestellworker starten.')
            supplier = manager.resolve_supplier(form.get('supplier_id'))
            if not supplier or not supplier['verified']:
                raise ValueError('Einen Lieferanten mit bestätigter Bestelladresse auswählen.')
            cap = _cents(form.get('max_total'), 'Kostenrahmen dieser Bestellung', positive=True)
            if not manager.cap() or cap > manager.cap():
                raise ValueError('Positiven Kostenrahmen konfigurieren; diese Bestellung darf ihn nicht überschreiten.')
            payload = {'order_requested': True, 'supplier_id': supplier['id'], 'recipient': supplier['recipient'],
                       'product_name': _text(form.get('product_name'), 'Produktname'),
                       'article_number': _text(form.get('article_number'), 'Lieferantenartikelnummer', 128),
                       'variant': _text(form.get('variant'), 'Genaue Variante / Größe / Farbe', 500),
                       'quantity': form.get('quantity'), 'unit': _text(form.get('unit'), 'Bestelleinheit', 60),
                       'max_total_cents': cap, 'urgent': urgent,
                       'unit_price_cents': _cents(form.get('unit_price'), 'Bestätigten Brutto-Stückpreis'),
                       'shipping_cents': _cents(form.get('shipping'), 'Bestätigte Versandkosten'),
                       'price_verified': True, 'price_basis': 'gross', 'currency': 'EUR'}
            g.assistant_order_request = request_id
            result = manager.dispatch.enqueue(payload, 'admin', request_id)
            if urgent:
                manager.tick()
                result = manager.dispatch.status(result['id'])
        except (ValueError, PermissionError) as exc:
            return page([str(exc)], form, code=400)
        flash(result['message'], 'success' if result['state'] in {'queued', 'sent'} else 'warning')
        return redirect(url_for('werkstatt_orders.index'), code=303)

    @app.cli.command('werkstatt-bestellungen-worker')
    @click.option('--once', is_flag=True, help='Genau einen fälligen Versandlauf verarbeiten.')
    @click.option('--interval', default=30, type=click.IntRange(10, 120))
    def worker_command(once, interval):
        try:
            run_worker(manager, once=once, interval=interval)
        except KeyboardInterrupt:
            return
        except Exception:
            raise click.ClickException('Bestellworker gestoppt. Aktivierung, Postfach und dauerhaften Speicher prüfen.') from None

    app.register_blueprint(bp)
    app.extensions['werkstatt_orders'] = manager
    return manager
