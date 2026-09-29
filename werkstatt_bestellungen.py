"""Admin order management and opt-in durable dispatch worker.

Register once with register_orders(portal); it starts no thread and sends no mail.
ASSISTANT_ORDER_SEND_ENABLED (or persisted send_enabled) and the existing
MAILBOX_SEND_ENABLED must both be true. Weekly operation additionally needs
ASSISTANT_ORDER_WORKER_ENABLED (or persisted worker_enabled) and a live
worker: flask --app app werkstatt-bestellungen-worker [--once] [--interval 30].
Use the same persistent DB and MAILBOX_OUTBOX_DIR on web and worker processes.
Existing portal IMAP/SMTP callbacks supply the unchanged mailbox identity.

There is no general portal scheduler. The existing MOS mail worker is isolated
to its own outbox, so this module exposes tick() and a dedicated opt-in CLI loop.
The worker only dispatches immutable, explicitly authorized queued requests.
Invoice imports may call propose_contact(), which NEVER verifies a contact.
No model-facing write API is registered. The management surface is admin-only;
submit_approved_action bridges separately confirmed avatar actions, checking
current employee sessions, rights, exact stored data and the shared budget.
start_order_worker is an explicit opt-in alternative in the existing web process.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import time
import threading
import uuid

import click
from flask import Blueprint, abort, flash, g, has_request_context, redirect, render_template, request, session, url_for

from werkstatt_artikel_identity import parse_unit_price
from werkstatt_bestellausgang import build_order_dispatch
from werkstatt_bestellplan import BERLIN
from werkstatt_bestelluebersicht import OrderOverview


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
                                             self.resolve_supplier, lambda: _now(),
                                             self.reserve_budget, self.check_batch)
        self._worker_lock = threading.Lock()
        self._worker = None
        self._worker_pid = None
        self._worker_stop = threading.Event()

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
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value RETURNING setting_key''', (key, str(value))).fetchall()
            db.commit()

    def cap(self):
        try:
            return max(0, int(self.setting('max_total_cents', '0')))
        except ValueError:
            return 0

    def configure_operations(self, enabled, cap_cents=None):
        """Persist the explicit admin operational switch in one transaction."""
        if type(enabled) is not bool:
            raise ValueError('Betriebsfreigabe eindeutig aktivieren oder pausieren.')
        if enabled and (type(cap_cents) is not int or not 0 < cap_cents <= 25000):
            raise ValueError('Freigegeben sind höchstens 250,00 EUR brutto je dringender Bestellung und je Sammelmail.')
        with self.db() as db:
            db.execute('''INSERT INTO app_settings(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at RETURNING key''',
                       ('ASSISTANT_OPERATIONS_ENABLED', '1' if enabled else '0', _now().isoformat())).fetchall()
            settings = {'send_enabled': '1' if enabled else '0', 'worker_enabled': '1' if enabled else '0',
                        'dispatch_paused': '0' if enabled else '1', 'worker_last_ok': '0'}
            if enabled:
                settings['max_total_cents'] = str(cap_cents)
            for key, value in settings.items():
                db.execute('''INSERT INTO assistent_bestellkonfiguration(setting_key,setting_value) VALUES(?,?)
                    ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value RETURNING setting_key''', (key, value)).fetchall()
            db.commit()
        if not enabled:
            self._worker_stop.set()

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
        paused = self.setting('dispatch_paused') == '1'
        enabled = bool(not paused and (self.app.config.get('ASSISTANT_ORDER_SEND_ENABLED') or self.setting('send_enabled') == '1')
                       and self.app.config.get('MAILBOX_SEND_ENABLED'))
        configured_storage = self.app.config.get('MAILBOX_OUTBOX_DIR')
        storage_ready = bool(configured_storage and Path(configured_storage).is_absolute()
                             and Path(configured_storage).resolve() == self.storage_dir)
        worker_enabled = bool(not paused and (self.app.config.get('ASSISTANT_ORDER_WORKER_ENABLED') or self.setting('worker_enabled') == '1'))
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
        if (has_request_context() and getattr(g, 'assistant_approved_order', None) == (actor_id, intent.get('id'))
                and 0 < intent.get('max_total_cents', 0) <= self.cap() and self.availability()['can_send']):
            return True
        # The trusted route creates this request-local permit after validating the
        # server nonce, admin session, deliberate submit and explicit price check.
        return bool(has_request_context() and actor_id == 'admin' and session.get('admin') and request.method == 'POST'
                    and request.endpoint == 'werkstatt_orders.create_order'
                    and getattr(g, 'assistant_order_request', None) == intent.get('id')
                    and 0 < intent.get('max_total_cents', 0) <= self.cap()
                    and self.availability()['can_send'])

    def reserve_budget(self, db, actor, request_key, intent, due_at):
        """Serialize reservations on the existing setting row (SQLite and PostgreSQL).

        Count immutable authorized ceilings, including already frozen batches, so
        a second request/worker cannot split a Monday supplier limit into mails.
        """
        db.execute("UPDATE assistent_bestellkonfiguration SET setting_value=setting_value WHERE setting_key='max_total_cents'")
        existing = db.execute('SELECT id FROM assistent_bestellanforderungen WHERE actor_id=? AND request_id=?',
                              (actor, request_key)).fetchone()
        if existing:
            return  # Dispatch verifies the exact immutable fingerprint next.
        row = db.execute("SELECT setting_value FROM assistent_bestellkonfiguration WHERE setting_key='max_total_cents'").fetchone()
        cap = int(row['setting_value']) if row and str(row['setting_value']).isdigit() else 0
        reserved = 0
        if not intent['urgent']:
            for saved in db.execute('SELECT snapshot_json FROM assistent_bestellanforderungen WHERE due_at=?', (due_at,)).fetchall():
                order = json.loads(saved['snapshot_json'])['order']
                if not order['urgent'] and order['supplier_id'] == intent['supplier_id']:
                    reserved += order['max_total_cents']
        if cap <= 0 or reserved + intent['max_total_cents'] > cap:
            raise ValueError('Brutto-Kostenrahmen überschritten: dringend je Bestellung, sonst insgesamt je Lieferant und Montagsversand. Nicht in weitere Bestellungen aufteilen.')

    def check_batch(self, payload):
        cap = self.cap()
        total = sum(entry['order']['max_total_cents'] for entry in payload['orders'])
        if not self.availability()['can_send'] or cap <= 0 or total > cap:
            raise ValueError('Bestellversand oder gesamter Brutto-Kostenrahmen nicht freigegeben.')
        for entry in payload['orders']:
            order = entry['order']
            supplier = self.resolve_supplier(order['supplier_id'])
            if not supplier or not supplier['verified'] or supplier['recipient'] != order['recipient']:
                raise ValueError('Bestellkontakt wurde geändert oder seine Bestätigung aufgehoben.')

    def _action_actor_limit(self, actor, *, write=False):
        if not has_request_context():
            raise PermissionError('Persönliche Anmeldung erforderlich.')
        if write:
            expected = session.get('csrf_token')
            supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token')
            if (request.method != 'POST' or not isinstance(expected, str) or not expected
                    or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied)):
                raise PermissionError('Bestellung braucht eine authentifizierte POST-Bestätigung mit CSRF-Schutz.')
        if session.get('admin') and actor == 'admin':
            return self.cap()
        mid = session.get('assistent_mid')
        if not mid or actor != f'mitarbeiter:{mid}':
            raise PermissionError('Bestellung gehört einem anderen Mitarbeiter.')
        with self.db() as db:
            row = db.execute('''SELECT r.*,m.aktiv FROM assistent_rechte r JOIN mitarbeiter m
                ON m.id=r.mitarbeiter_id WHERE r.mitarbeiter_id=?''', (mid,)).fetchone()
        if (not row or not row['aktiv'] or not row['lesen'] or not row['einkaufen']
                or row['version'] != session.get('assistent_version')):
            raise PermissionError('Aktuelle Mitarbeiterfreigabe für Bestellungen fehlt.')
        return min(self.cap(), max(0, int(row['limit_cent'])))

    def approved_action_status(self, actor_id, approved_action_id):
        self._action_actor_limit(actor_id)
        with self.db() as db:
            action = db.execute("SELECT id FROM assistent_aktionen WHERE id=? AND actor=? AND art='bestellung'",
                                (approved_action_id, actor_id)).fetchone()
            if not action:
                raise PermissionError('Eigene Einkaufsaktion nicht gefunden.')
            row = db.execute('SELECT id FROM assistent_bestellanforderungen WHERE actor_id=? AND request_id=?',
                             (actor_id, 'avatar:' + str(approved_action_id))).fetchone()
        return self.dispatch.status(row['id']) if row else None

    def submit_approved_action(self, actor_id, approved_action_id):
        """Bridge an owned, explicitly approved stored action; never trust model data.

        Called by a protected confirmation route, not directly by a model tool.
        Replays use actor + action ID, so lost HTTP replies never create new mail.
        This method can send an urgent order ONLY after all deployment gates pass.
        """
        limit = self._action_actor_limit(actor_id, write=True)
        action_id = _text(approved_action_id, 'Freigegebene Aktions-ID', 100)
        with self.db() as db:
            row = db.execute("SELECT * FROM assistent_aktionen WHERE id=? AND actor=? AND art='bestellung'",
                             (action_id, actor_id)).fetchone()
        if not row:
            raise PermissionError('Eigene Einkaufsaktion nicht gefunden.')
        def blocked(message, fields=()):
            return {'id': None, 'action_id': action_id, 'state': 'blocked', 'message': message,
                    'needs_review': True, 'missing_fields': list(fields)}
        if row['status'] != 'intern_freigegeben':
            return blocked('Bestellung noch nicht ausdrücklich bestätigt.', ['approval'])
        try:
            saved = json.loads(row['payload'])
            payload = dict(saved['versand'])
        except (ValueError, TypeError, KeyError):
            return blocked('Bestellung braucht eindeutige Artikel-, Varianten-, Lieferanten- und Preisangaben.', ['versand'])
        missing = [field for field in ('price_source', 'extra_costs_cents') if field not in payload]
        if missing:
            return blocked('Preisquelle und sämtliche Nebenkosten vor Bestellung ausdrücklich klären.', missing)
        request_key = 'avatar:' + action_id
        try:
            intent = self.dispatch._intent(payload, request_key)
        except (ValueError, TypeError) as exc:
            return blocked(str(exc))
        # Preserve the result even when configuration is disabled after sending.
        existing = self.approved_action_status(actor_id, action_id)
        if existing:
            if existing['order'] != intent:
                return blocked('Freigegebene Bestellung nach Übergabe verändert; nicht erneut bestellen.')
            return dict(existing, action_id=action_id)
        if limit <= 0 or intent['max_total_cents'] > limit:
            return blocked('Bestellung überschreitet den freigegebenen persönlichen oder betrieblichen Brutto-Kostenrahmen.', ['budget'])
        available = self.availability()
        if not available['can_send']:
            return blocked('Bestellversand, bestehendes Postfach oder dauerhafter Ausgabespeicher noch nicht eingerichtet.', ['configuration'])
        if not intent['urgent'] and not available['worker_live']:
            return blocked('Automatischer Montagsversand noch nicht betriebsbereit.', ['worker'])
        previous = getattr(g, 'assistant_approved_order', None)
        g.assistant_approved_order = (actor_id, request_key)
        try:
            result = self.dispatch.enqueue(payload, actor_id, request_key)
        except (ValueError, PermissionError) as exc:
            return blocked(str(exc))
        finally:
            g.assistant_approved_order = previous
        if intent['urgent']:
            # A dispatch/configuration failure must not turn an accepted durable
            # order into an apparent unsubmitted order; its same ID remains valid.
            try:
                self.tick()
            except Exception:
                result = self.dispatch.status(result['id'])
                return dict(result, action_id=action_id, needs_review=True,
                            message='Bestellung gespeichert; sofortiger Versand noch nicht bestätigt. Nicht erneut bestellen.')
            result = self.dispatch.status(result['id'])
        return dict(result, action_id=action_id)

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


def start_order_worker(manager, *, interval=30):
    """Explicit opt-in web-process worker, sharing the existing private outbox.

    Call after app initialization and after an admin enables operations. No
    import-time startup. Failures are retried without logging private mail data;
    the durable heartbeat expires while configuration/transport is unavailable.
    """
    if not 10 <= interval <= 120:
        raise ValueError('Worker-Intervall muss zwischen 10 und 120 Sekunden liegen.')
    if manager.app.config.get('TESTING') or not manager.availability()['worker_enabled']:
        return False
    with manager._worker_lock:
        if (manager._worker_pid == os.getpid() and manager._worker and manager._worker.is_alive()
                and not manager._worker_stop.is_set()):
            return False
        stop = threading.Event()
        manager._worker_stop = stop
        def work():
            while not stop.is_set():
                try:
                    with manager.app.app_context():
                        manager.tick(worker=True)
                except Exception:
                    manager.app.logger.warning('Bestellworker: Versandlauf ausstehend; Konfiguration und Status prüfen.')
                stop.wait(interval)
        manager._worker_pid = os.getpid()
        manager._worker = threading.Thread(target=work, name='werkstatt-bestellungen', daemon=True)
        manager._worker.start()
        return True


def register_orders(portal):
    app = portal.app
    if 'werkstatt_orders' in app.extensions:
        return app.extensions['werkstatt_orders']
    manager = OrderManagement(portal)
    bp = Blueprint('werkstatt_orders', __name__, url_prefix='/admin/assistent-bestellungen')

    @bp.after_request
    def private_order_response(response):
        response.headers['Cache-Control'] = 'no-store'
        return response

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
        overview = OrderOverview(portal.get_db).page(request.args)
        return render_template('assistent_bestellungen.html', contacts=manager.contacts(), availability=manager.availability(),
                               overview=overview, cap_cents=manager.cap(), csrf=csrf, request_id=request_id,
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
            cap = _cents(request.form.get('max_total'), 'Positiven Kostenrahmen', positive=True)
            if cap > 25000:
                raise ValueError('Der freigegebene Kostenrahmen beträgt höchstens 250,00 EUR brutto.')
            manager.set_setting('max_total_cents', cap)
        except ValueError as exc:
            return page([str(exc)], code=400)
        flash('Kostenrahmen für neue Bestellanforderungen gespeichert.', 'success')
        return redirect(url_for('werkstatt_orders.index'), code=303)

    @bp.post('/betrieb')
    @portal.admin_required
    def configure_operations():
        try:
            action = request.form.get('action')
            if action not in {'activate', 'deactivate'}:
                raise ValueError('Betrieb ausdrücklich aktivieren oder pausieren.')
            cap = _cents(request.form.get('max_total'), 'Kostenrahmen', positive=True) if action == 'activate' else None
            manager.configure_operations(action == 'activate', cap)
        except ValueError as exc:
            return page([str(exc)], code=400)
        if action == 'activate':
            start_order_worker(manager)
            flash('Statusaktionen und Bestellversand freigegeben. Persönliche Rechte, bestätigte Lieferanten und Versandbereitschaft werden weiter geprüft.', 'success')
        else:
            flash('Neue Statusaktionen und Versandläufe pausiert. Bereits übergebene Bestellungen bleiben im Versandprotokoll erhalten.', 'success')
        endpoint = 'assistent.rights' if 'assistent.rights' in app.view_functions else 'werkstatt_orders.index'
        return redirect(url_for(endpoint), code=303)

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
                       'extra_costs_cents': _cents(form.get('extra_costs'), 'Bestätigte Nebenkosten'),
                       'price_source': _text(form.get('price_source'), 'Geprüfte Preisquelle', 500),
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
