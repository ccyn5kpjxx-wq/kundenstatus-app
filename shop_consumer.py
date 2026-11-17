"""Frozen retail terms and public withdrawal intake; all activation is explicit.

No supplied draft is approved here. A mixed-retail pack must be reviewed and
injected by the operator. This module records declarations, not their legal
merits, and never cancels procurement, captures, refunds or sends on import.
Withdrawal receipt delivery is an explicitly invoked durable-outbox operation.
Primary statutory reference: https://www.gesetze-im-internet.de/bgb/__356a.html
"""
import base64
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from hashlib import sha256
import hmac
import json
import re
import secrets
import sqlite3
from urllib.parse import urlsplit
from uuid import UUID, uuid4, uuid5

from flask import Blueprint, Response, current_app, render_template_string, request
from reifen_rechtstexte import ANBIETER

COOKIE = '__Host-shop_withdrawal_v2'
TOKEN = re.compile(r'[A-Za-z0-9_-]{43}')
HASH = re.compile(r'[a-f0-9]{64}')
DOCS = ('agb', 'widerruf', 'widerrufsformular', 'lieferung', 'datenschutz')
MAX_BODY = 16 * 1024
NAMESPACE = UUID('4b12f4a7-e6c0-4a6e-a753-994350d3c52c')


class ConsumerError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise ConsumerError('consumer_unavailable') from None


def _text(value, maximum, *, empty=False):
    if (not isinstance(value, str) or not (0 if empty else 1) <= len(value.strip()) <= maximum
            or any(ord(c) < 32 and c not in '\n\t' or 0xD800 <= ord(c) <= 0xDFFF or ord(c) == 127 for c in value)):
        raise ConsumerError('invalid_declaration')
    return value.strip()


def _email(value):
    value = _text(value, 254)
    if not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}", value) or '..' in value:
        raise ConsumerError('invalid_declaration')
    return value


def _freeze_pack(pack):
    if (not isinstance(pack, Mapping) or pack.get('approved') is not True or pack.get('status') != 'approved'
            or pack.get('scope') != 'mixed_retail' or not isinstance(pack.get('texts'), Mapping)):
        return None
    try:
        version = _text(pack.get('version'), 100)
        if version.startswith(('entwurf:', 'demo:')):
            return None
        texts = {key: _text(pack['texts'].get(key), 65536) for key in DOCS}
        attachments = []
        for item in pack.get('attachments', []):
            if (not isinstance(item, Mapping) or not isinstance(item.get('filename'), str)
                    or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', item['filename'])
                    or item.get('mimetype') not in {'application/pdf', 'image/png', 'image/jpeg', 'text/plain'}
                    or not isinstance(item.get('content'), bytes) or not 0 < len(item['content']) <= 1024 * 1024):
                return None
            attachments.append({'filename': item['filename'], 'mimetype': item['mimetype'],
                                'content_b64': base64.b64encode(item['content']).decode('ascii')})
        if len(attachments) > 8:
            return None
        seller = pack.get('seller', ANBIETER)
        if not isinstance(seller, Mapping):
            return None
        seller = {key: _text(seller.get(key), 500) for key in ANBIETER}
        value = {'version': version, 'status': 'approved', 'approved': True, 'scope': 'mixed_retail',
                 'seller': seller, 'texts': texts, 'attachments': attachments}
        encoded = _json(value)
        if len(encoded.encode('utf-8')) > 4 * 1024 * 1024:
            return None
        return encoded
    except (ValueError, TypeError, UnicodeError, RecursionError):
        return None


def init_consumer_schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS shop_consumer_packs_v2 (
        hash TEXT PRIMARY KEY, pack_json TEXT NOT NULL);
      CREATE TRIGGER IF NOT EXISTS shop_consumer_pack_immutable_v2 BEFORE UPDATE ON shop_consumer_packs_v2
        BEGIN SELECT RAISE(ABORT,'immutable_consumer_pack'); END;
      CREATE TABLE IF NOT EXISTS shop_consumer_acceptances_v2 (
        quote_hash TEXT PRIMARY KEY,customer_id TEXT NOT NULL,mode TEXT NOT NULL,
        pack_hash TEXT NOT NULL,accepted_at TEXT NOT NULL,consumer INTEGER NOT NULL CHECK(consumer=1));
      CREATE TRIGGER IF NOT EXISTS shop_consumer_acceptance_immutable_v2 BEFORE UPDATE ON shop_consumer_acceptances_v2
        BEGIN SELECT RAISE(ABORT,'immutable_consumer_acceptance'); END;
      CREATE TABLE IF NOT EXISTS shop_withdrawal_sessions_v2 (
        key TEXT PRIMARY KEY,csrf_hash TEXT NOT NULL,mode TEXT NOT NULL,expires_at TEXT NOT NULL,
        declaration_json TEXT,confirm_hash TEXT,withdrawal_id TEXT);
      CREATE TABLE IF NOT EXISTS shop_withdrawals_v2 (
        id TEXT PRIMARY KEY,mode TEXT NOT NULL,declaration_json TEXT NOT NULL,received_at TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'manual_review',mail_token TEXT UNIQUE NOT NULL,
        mail_state TEXT NOT NULL DEFAULT 'queued',lease_until TEXT,lease_token TEXT);
      CREATE TRIGGER IF NOT EXISTS shop_withdrawal_immutable_v2
        BEFORE UPDATE OF id,mode,declaration_json,received_at,mail_token ON shop_withdrawals_v2
        BEGIN SELECT RAISE(ABORT,'immutable_withdrawal'); END;
    ''')
    db.commit()


class ShopConsumer:
    def __init__(self, store, connection_factory, *, mode, public_origin, pack=None, enabled=False,
                 withdrawals_enabled=False, delivery_enabled=False, outbox=None, smtp_config=None,
                 sender_address='info@auto-lackierzentrum.de', clock=None):
        try:
            parsed = urlsplit(public_origin)
            valid_origin = (isinstance(public_origin, str) and parsed.scheme == 'https' and parsed.hostname
                and not parsed.username and not parsed.password and parsed.path == ''
                and not parsed.query and not parsed.fragment and parsed.port in (None, 443)
                and '\\' not in public_origin and not any(ord(c) < 33 for c in public_origin))
        except (ValueError, TypeError):
            valid_origin = False
        if (not valid_origin or mode not in {'live', 'test'} or not callable(connection_factory)
                or any(type(v) is not bool for v in (enabled, withdrawals_enabled, delivery_enabled))):
            raise ConsumerError('invalid_consumer_configuration')
        self.store, self.factory, self.mode, self.public_origin = store, connection_factory, mode, public_origin
        self.enabled, self.withdrawals_enabled, self.delivery_enabled = enabled, withdrawals_enabled, delivery_enabled
        self.outbox, self.smtp_config, self.sender = outbox, smtp_config, _email(sender_address)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.pack_json = _freeze_pack(pack)
        self.pack_hash = sha256(self.pack_json.encode('utf-8')).hexdigest() if self.pack_json else None

    def readiness(self):
        legal = self.enabled and self.pack_json is not None
        withdrawal = self.enabled and self.withdrawals_enabled
        delivery = withdrawal and self.delivery_enabled and callable(self.smtp_config) and all(
            callable(getattr(self.outbox, method, None)) for method in ('send', 'status'))
        return {'legal_ready': legal, 'withdrawal_ready': withdrawal, 'confirmation_delivery_ready': delivery,
                'checkout_ready': legal and withdrawal and delivery, 'mode': self.mode}

    @contextmanager
    def _db(self, *, write=False):
        db = None
        try:
            db = self.factory()
            if not isinstance(db, sqlite3.Connection) or db.in_transaction:
                raise ValueError()
            db.row_factory = sqlite3.Row
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except ConsumerError:
            if isinstance(db, sqlite3.Connection):
                db.rollback()
            raise
        except Exception:
            if isinstance(db, sqlite3.Connection):
                db.rollback()
            raise ConsumerError('consumer_unavailable') from None
        finally:
            if isinstance(db, sqlite3.Connection):
                db.close()

    def terms(self):
        if not self.readiness()['legal_ready']:
            raise ConsumerError('legal_terms_unavailable')
        pack = json.loads(self.pack_json)
        return {'version': pack['version'], 'pack_hash': self.pack_hash, 'texts': pack['texts'],
                'seller': pack['seller'], 'consumer_only': True}

    def accept_quote(self, quote_hash, *, customer_id, pack_hash, terms_accepted, consumer=True):
        if (not self.readiness()['legal_ready'] or pack_hash != self.pack_hash
                or terms_accepted is not True or consumer is not True):
            raise ConsumerError('terms_acceptance_required')
        try:
            self.store.lookup_server_quote(quote_hash, customer_id=customer_id, mode=self.mode)
        except Exception:
            raise ConsumerError('quote_not_found') from None
        now = _stamp(self.clock()).isoformat()
        with self._db(write=True) as db:
            previous = db.execute('SELECT * FROM shop_consumer_acceptances_v2 WHERE quote_hash=?', (quote_hash,)).fetchone()
            if previous is not None:
                if (previous['customer_id'] != customer_id or previous['mode'] != self.mode
                        or previous['pack_hash'] != pack_hash):
                    raise ConsumerError('terms_changed_new_quote_required')
                now = previous['accepted_at']
            else:
                db.execute('INSERT OR IGNORE INTO shop_consumer_packs_v2 VALUES (?,?)', (pack_hash, self.pack_json))
                frozen = db.execute('SELECT pack_json FROM shop_consumer_packs_v2 WHERE hash=?', (pack_hash,)).fetchone()
                if frozen['pack_json'] != self.pack_json:
                    raise ConsumerError('consumer_unavailable')
                db.execute('INSERT INTO shop_consumer_acceptances_v2 VALUES (?,?,?,?,?,1)',
                           (quote_hash, customer_id, self.mode, pack_hash, now))
        return {'accepted': True, 'quote_hash': quote_hash, 'pack_hash': pack_hash, 'accepted_at': now}

    def require_acceptance(self, quote_hash, *, customer_id):
        if not self.readiness()['legal_ready']:
            raise ConsumerError('terms_acceptance_required')
        with self._db() as db:
            row = db.execute('''SELECT a.pack_hash,p.pack_json FROM shop_consumer_acceptances_v2 a
                JOIN shop_consumer_packs_v2 p ON p.hash=a.pack_hash
                WHERE a.quote_hash=? AND a.customer_id=? AND a.mode=?''', (quote_hash, customer_id, self.mode)).fetchone()
            if row is None or row['pack_hash'] != self.pack_hash or row['pack_json'] != self.pack_json:
                raise ConsumerError('terms_acceptance_required')
        return True

    def contract_pack(self, order_id, *, customer_id, mode):
        if mode != self.mode:
            raise ConsumerError('contract_not_found')
        try:
            order = self.store.get_order(order_id, customer_id=customer_id, mode=mode)
        except Exception:
            raise ConsumerError('contract_not_found') from None
        with self._db() as db:
            row = db.execute('''SELECT p.hash,p.pack_json FROM shop_consumer_acceptances_v2 a
                JOIN shop_consumer_packs_v2 p ON p.hash=a.pack_hash
                WHERE a.quote_hash=? AND a.customer_id=? AND a.mode=?''',
                (order['quote_hash'], customer_id, mode)).fetchone()
            if row is None or sha256(row['pack_json'].encode('utf-8')).hexdigest() != row['hash']:
                raise ConsumerError('contract_not_found')
            pack = json.loads(row['pack_json'])
        return {'version': pack['version'], 'texts': pack['texts'],
                'attachments': [{'filename': a['filename'], 'mimetype': a['mimetype'],
                                  'content': base64.b64decode(a['content_b64'], validate=True)} for a in pack['attachments']]}

    def _origin(self, req, *, mutation):
        if (req.scheme != 'https' or req.host_url.rstrip('/') != self.public_origin
                or req.headers.get('Origin') not in (None, self.public_origin)
                or mutation and req.headers.get('Origin') != self.public_origin
                or mutation and req.headers.get('Sec-Fetch-Site') not in (None, 'same-origin')):
            raise ConsumerError('request_not_allowed')

    def open_withdrawal(self, req):
        if not self.readiness()['withdrawal_ready']:
            raise ConsumerError('withdrawal_unavailable')
        self._origin(req, mutation=False)
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        now = _stamp(self.clock())
        with self._db(write=True) as db:
            db.execute('DELETE FROM shop_withdrawal_sessions_v2 WHERE expires_at < ?', (now.isoformat(),))
            if db.execute('SELECT COUNT(*) FROM shop_withdrawal_sessions_v2').fetchone()[0] >= 4096:
                raise ConsumerError('withdrawal_unavailable')
            db.execute('INSERT INTO shop_withdrawal_sessions_v2 (key,csrf_hash,mode,expires_at) VALUES (?,?,?,?)',
                       (sha256(token.encode()).hexdigest(), sha256(csrf.encode()).hexdigest(), self.mode,
                        (now + timedelta(minutes=30)).isoformat()))
        return {'cookie': token, 'csrf': csrf}

    def _challenge(self, db, req, csrf):
        if not self.readiness()['withdrawal_ready']:
            raise ConsumerError('withdrawal_unavailable')
        self._origin(req, mutation=True)
        cookies = req.cookies.getlist(COOKIE)
        if len(cookies) != 1 or not TOKEN.fullmatch(cookies[0]) or not isinstance(csrf, str) or not TOKEN.fullmatch(csrf):
            raise ConsumerError('request_not_allowed')
        key = sha256(cookies[0].encode()).hexdigest()
        row = db.execute('SELECT * FROM shop_withdrawal_sessions_v2 WHERE key=? AND mode=?', (key, self.mode)).fetchone()
        if (row is None or not hmac.compare_digest(row['csrf_hash'], sha256(csrf.encode()).hexdigest())
                or _stamp(row['expires_at']) <= _stamp(self.clock())):
            raise ConsumerError('request_not_allowed')
        return row

    def review_withdrawal(self, req, payload):
        if not isinstance(payload, Mapping) or set(payload) != {'csrf', 'name', 'email', 'contract_reference', 'scope'}:
            raise ConsumerError('invalid_declaration')
        declaration = {'name': _text(payload['name'], 160), 'email': _email(payload['email']),
                       'contract_reference': _text(payload['contract_reference'], 240),
                       'scope': _text(payload['scope'], 2000, empty=True) or 'Gesamter Vertrag'}
        confirm = secrets.token_urlsafe(32)
        with self._db(write=True) as db:
            row = self._challenge(db, req, payload['csrf'])
            if row['withdrawal_id'] is not None:
                raise ConsumerError('declaration_already_received')
            db.execute('UPDATE shop_withdrawal_sessions_v2 SET declaration_json=?,confirm_hash=? WHERE key=?',
                       (_json(declaration), sha256(confirm.encode()).hexdigest(), row['key']))
        return {'declaration': declaration, 'confirm': confirm, 'csrf': payload['csrf']}

    def confirm_withdrawal(self, req, *, csrf, confirm):
        if not isinstance(confirm, str) or not TOKEN.fullmatch(confirm):
            raise ConsumerError('request_not_allowed')
        with self._db(write=True) as db:
            row = self._challenge(db, req, csrf)
            if (not row['declaration_json'] or not row['confirm_hash']
                    or not hmac.compare_digest(row['confirm_hash'], sha256(confirm.encode()).hexdigest())):
                raise ConsumerError('request_not_allowed')
            identifier = row['withdrawal_id']
            if identifier is None:
                identifier = 'swd_' + uuid4().hex
                timestamp = _stamp(self.clock()).isoformat()
                db.execute('INSERT INTO shop_withdrawals_v2 (id,mode,declaration_json,received_at,mail_token) VALUES (?,?,?,?,?)',
                           (identifier, self.mode, row['declaration_json'], timestamp, str(uuid5(NAMESPACE, identifier))))
                db.execute('UPDATE shop_withdrawal_sessions_v2 SET withdrawal_id=? WHERE key=?', (identifier, row['key']))
            received = db.execute('SELECT * FROM shop_withdrawals_v2 WHERE id=? AND mode=?', (identifier, self.mode)).fetchone()
            return {'id': identifier, 'received_at': received['received_at'], 'state': 'manual_review',
                    'confirmation_state': received['mail_state'], 'declaration': json.loads(received['declaration_json'])}

    @staticmethod
    def _mail_state(value, row):
        if not isinstance(value, Mapping) or value.get('token') != row['mail_token']:
            return 'unknown'
        accepted = value.get('accepted')
        recipient = json.loads(row['declaration_json'])['email'].casefold()
        if (value.get('state') in {'sent', 'copy_pending', 'partial'} and isinstance(accepted, list)
                and recipient in [entry.casefold() for entry in accepted if isinstance(entry, str)]):
            return 'submitted'
        return 'not_sent' if value.get('state') == 'not_sent' else 'unknown'

    def deliver_once(self):
        """Explicit worker tick. Unknown SMTP is status-reconciled, never resent."""
        if not self.readiness()['confirmation_delivery_ready']:
            return False
        now = _stamp(self.clock())
        with self._db(write=True) as db:
            row = db.execute('''SELECT * FROM shop_withdrawals_v2 WHERE mode=? AND
                (mail_state='queued' OR (mail_state='sending' AND lease_until<=?)) ORDER BY received_at,id LIMIT 1''',
                (self.mode, now.isoformat())).fetchone()
            if row is None:
                return False
            row = dict(row)
            owner = uuid4().hex
            db.execute("UPDATE shop_withdrawals_v2 SET mail_state='sending',lease_until=?,lease_token=? WHERE id=?",
                       ((now + timedelta(seconds=120)).isoformat(), owner, row['id']))
        state = 'unknown'
        try:
            if row['mail_state'] == 'queued':
                declaration = json.loads(row['declaration_json'])
                message = EmailMessage()
                message['From'], message['To'] = self.sender, declaration['email']
                message['Subject'] = 'Eingang Ihres Widerrufs'
                message.set_content('Wir bestätigen den Eingang Ihrer Widerrufserklärung.\n\n'
                    + 'Name: ' + declaration['name'] + '\nVertrag / Bestellung: ' + declaration['contract_reference']
                    + '\nBetroffene Positionen: ' + declaration['scope'] + '\nBestätigungskontakt: ' + declaration['email']
                    + '\nEingang (UTC): ' + row['received_at'] + '\nReferenz: ' + row['id']
                    + '\n\nDies bestätigt den Zugang Ihrer Erklärung. Die weitere Bearbeitung erfolgt persönlich.\n')
                config = self.smtp_config()
                if not isinstance(config, Mapping) or config.get('from_address') != self.sender:
                    raise ConsumerError('consumer_unavailable')
                state = self._mail_state(self.outbox.send(row['mail_token'], message, dict(config), retry_not_sent=False), row)
            else:
                state = self._mail_state(self.outbox.status(row['mail_token']), row)
        except Exception:
            state = 'unknown'
        with self._db(write=True) as db:
            db.execute('UPDATE shop_withdrawals_v2 SET mail_state=?,lease_until=NULL WHERE id=? AND mail_state=\'sending\' AND lease_token=?',
                       (state, row['id'], owner))
        return True

    def pending_withdrawals(self, *, limit=50):
        """Private staff queue; caller must enforce the portal's admin auth."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ConsumerError('invalid_request')
        with self._db() as db:
            rows = db.execute('''SELECT id,declaration_json,received_at,state,mail_state
                FROM shop_withdrawals_v2 WHERE mode=? AND state='manual_review'
                ORDER BY received_at,id LIMIT ?''', (self.mode, limit)).fetchall()
            return [{'id': row['id'], 'declaration': json.loads(row['declaration_json']),
                     'received_at': row['received_at'], 'state': row['state'],
                     'confirmation_state': row['mail_state']} for row in rows]

    def reconcile_confirmation(self, identifier):
        """Private staff action: read existing outbox result, never resend."""
        if not self.readiness()['confirmation_delivery_ready']:
            raise ConsumerError('consumer_unavailable')
        with self._db() as db:
            row = db.execute('SELECT * FROM shop_withdrawals_v2 WHERE id=? AND mode=?', (identifier, self.mode)).fetchone()
            if row is None:
                raise ConsumerError('declaration_not_found')
            row = dict(row)
        state = self._mail_state(self.outbox.status(row['mail_token']), row)
        if state == 'submitted':
            with self._db(write=True) as db:
                db.execute("UPDATE shop_withdrawals_v2 SET mail_state='submitted',lease_until=NULL WHERE id=? AND mode=?",
                           (identifier, self.mode))
        return {'id': identifier, 'confirmation_state': 'submitted' if row['mail_state'] == 'submitted' else state}


def create_consumer_blueprint(service, *, buyer=None):
    bp = Blueprint('shop_consumer_v2', __name__)

    def reply(value, status=200):
        return Response(_json(value).encode('utf-8'), status=status, content_type='application/json; charset=utf-8')

    @bp.before_request
    def enabled():
        if current_app.config.get('SHOP_CONSUMER_ENABLED') is not True or service is None or service.enabled is not True:
            return reply({'error': 'not_found'}, 404)
        if request.args or request.headers.get('Transfer-Encoding') or request.content_length and request.content_length > MAX_BODY:
            raise ConsumerError('invalid_request')

    @bp.after_request
    def headers(response):
        response.headers.update({'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
            'Referrer-Policy': 'same-origin', 'X-Robots-Tag': 'noindex, nofollow',
            'Content-Security-Policy': "default-src 'none'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"})
        return response

    @bp.errorhandler(ConsumerError)
    def error(exc):
        status = 403 if exc.code == 'request_not_allowed' else 503 if exc.code.endswith('unavailable') else 400
        return reply({'error': exc.code}, status)

    @bp.get('/shop/api/v2/consumer/terms')
    def terms():
        return reply(service.terms())

    @bp.get('/shop/recht/<document>')
    def legal_document(document):
        if document not in DOCS:
            return reply({'error': 'not_found'}, 404)
        terms = service.terms()
        return render_template_string('<!doctype html><html lang="de"><meta charset="utf-8"><title>{{ title }}</title>'
            '<main><h1>{{ title }}</h1><p>Version {{ version }}</p><pre>{{ text }}</pre></main></html>',
            title=document, version=terms['version'], text=terms['texts'][document])

    @bp.post('/shop/api/v2/consumer/acceptance')
    def accept():
        principal = buyer.authorize_quote(request) if buyer is not None else None
        if not principal:
            raise ConsumerError('request_not_allowed')
        if request.mimetype != 'application/json' or request.content_length is None or not 0 < request.content_length <= MAX_BODY:
            raise ConsumerError('invalid_request')
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result
        try:
            value = json.loads(request.get_data(cache=False), object_pairs_hook=unique,
                               parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeError, RecursionError):
            raise ConsumerError('invalid_request') from None
        if not isinstance(value, dict) or set(value) != {'quote_hash', 'pack_hash', 'terms_accepted', 'consumer'}:
            raise ConsumerError('invalid_request')
        return reply(service.accept_quote(value['quote_hash'], customer_id=principal, pack_hash=value['pack_hash'],
                                         terms_accepted=value['terms_accepted'], consumer=value['consumer']))

    @bp.get('/shop/widerruf/')
    def withdrawal():
        state = service.open_withdrawal(request)
        response = Response(render_template_string('''<!doctype html><html lang="de"><meta charset="utf-8">
          <title>Vertrag widerrufen</title><main><h1>Vertrag widerrufen</h1>
          <p>Hier können Sie einen Vertrag oder einzelne Positionen widerrufen. Eine Anmeldung ist nicht erforderlich.</p>
          <form method="post" action="/shop/widerruf/pruefen"><input type="hidden" name="csrf" value="{{ csrf }}">
          <p><label>Name <input name="name" maxlength="160" autocomplete="name" required></label></p>
          <p><label>Bestellung oder Vertrag <input name="contract_reference" maxlength="240" required></label></p>
          <p><label>Bestätigung per E-Mail an <input type="email" name="email" maxlength="254" autocomplete="email" required></label></p>
          <p><label>Betroffene Positionen (leer für gesamten Vertrag) <textarea name="scope" maxlength="2000"></textarea></label></p>
          <button type="submit">Weiter zur Bestätigung</button></form></main></html>''', csrf=state['csrf']),
          content_type='text/html; charset=utf-8')
        response.set_cookie(COOKIE, state['cookie'], max_age=1800, secure=True, httponly=True, samesite='Lax', path='/')
        return response

    def form():
        if request.mimetype != 'application/x-www-form-urlencoded' or request.content_length is None or not 0 < request.content_length <= MAX_BODY:
            raise ConsumerError('invalid_request')
        if any(len(request.form.getlist(key)) != 1 for key in request.form):
            raise ConsumerError('invalid_request')
        return request.form.to_dict()

    @bp.post('/shop/widerruf/pruefen')
    def withdrawal_review():
        result = service.review_withdrawal(request, form())
        return render_template_string('''<!doctype html><html lang="de"><meta charset="utf-8"><title>Widerruf bestätigen</title>
          <main><h1>Widerruf bestätigen</h1><dl>{% for key,value in declaration.items() %}<dt>{{ key }}</dt><dd>{{ value }}</dd>{% endfor %}</dl>
          <form method="post" action="/shop/widerruf/bestaetigen"><input type="hidden" name="csrf" value="{{ csrf }}">
          <input type="hidden" name="confirm" value="{{ confirm }}"><button type="submit">Widerruf bestätigen</button></form></main></html>''', **result)

    @bp.post('/shop/widerruf/bestaetigen')
    def withdrawal_confirm():
        value = form()
        if set(value) != {'csrf', 'confirm'}:
            raise ConsumerError('invalid_request')
        result = service.confirm_withdrawal(request, **value)
        return render_template_string('''<!doctype html><html lang="de"><meta charset="utf-8"><title>Widerruf eingegangen</title>
          <main><h1>Widerruf eingegangen</h1><p>Eingang (UTC): {{ received_at }}</p><p>Referenz: {{ id }}</p>
          <p>Die Eingangsbestätigung wird per E-Mail versendet. Die weitere Bearbeitung erfolgt persönlich.</p></main></html>''', **result)

    return bp
