"""Personal, expiring setup links; employees choose their own password.

Tokens only travel in URL fragments and POST bodies. The database stores their
SHA-256 hashes, never links or clear tokens. Issuing does not replace passwords,
and redeeming changes authentication version, not pending material permissions.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from urllib.parse import urlsplit

from flask import Blueprint, flash, jsonify, redirect, render_template, request, session
from werkzeug.security import generate_password_hash


SETUP_PATH = '/werkstatt/zugang/einrichten'
TOKEN_RE = re.compile(r'[A-Za-z0-9_-]{64}')
INVITATION_SECONDS = 72 * 60 * 60
INVALID_LINK = 'Dieser Einrichtungslink ist ungültig, abgelaufen oder bereits verwendet. Bitte einen neuen Link bei der Werkstattleitung anfordern.'
RATE_MESSAGE = 'Zu viele Fehlversuche. Bitte später erneut versuchen.'


def _iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def _fingerprint(rights):
    fields = ('mitarbeiter_id', 'lesen', 'dokumentieren', 'einkaufen',
              'limit_cent', 'version', 'auth_version', 'passwort_hash')
    return hashlib.sha256(json.dumps({key: rights[key] for key in fields},
                                   sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _token_hash(token):
    if not isinstance(token, str) or not TOKEN_RE.fullmatch(token):
        raise ValueError(INVALID_LINK)
    return hashlib.sha256(token.encode('ascii')).hexdigest()


class EmployeeInvitations:
    def __init__(self, portal, clock=None):
        self.p = portal
        self.clock = clock or time.time

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            self.p.ensure_column(db, 'assistent_rechte', 'auth_version', 'INTEGER NOT NULL DEFAULT 1')
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_einladungen (
                mitarbeiter_id INTEGER PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
                issued_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
                used_at INTEGER, rights_fingerprint TEXT NOT NULL)''')

    def origin(self):
        # These are server configuration, never request Host/Forwarded headers.
        value = (self.p.app.config.get('EMPLOYEE_INVITE_PUBLIC_BASE_URL')
                 or getattr(self.p, 'PORTAL_BASE_URL', '')
                 or getattr(self.p, 'PUBLIC_BASE_URL', '')
                 or os.environ.get('RENDER_EXTERNAL_URL', '')).strip().rstrip('/')
        parsed = urlsplit(value)
        local_test = (self.p.app.config.get('TESTING') and parsed.scheme == 'http'
                      and parsed.hostname in {'localhost', '127.0.0.1'})
        if (not value or (parsed.scheme != 'https' and not local_test)
                or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path
                or parsed.query or parsed.fragment):
            raise ValueError('Die öffentliche HTTPS-Portaladresse muss zuerst in der Serverkonfiguration hinterlegt werden.')
        return value

    def _rights(self, db, employee_id):
        row = db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=?', (employee_id,)).fetchone()
        return dict(row) if row else None

    def _audit(self, db, employee_id, action, actor='admin'):
        db.execute('''INSERT INTO assistent_audit(actor,auftrag_id,aktion,details,zeit)
            VALUES(?,?,?,?,?)''', (actor, None, action,
                                   json.dumps({'mitarbeiter': employee_id}), self.p.now_str()))

    def _create(self, db, employee_id, origin, grant_material):
        if type(employee_id) is not int or not 1 <= employee_id <= 2147483647:
            raise ValueError('Aktiven Mitarbeiter auswählen.')
        # All issue/redeem operations share the restore lock; these row locks
        # additionally serialize password/rights changes on PostgreSQL.
        db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (employee_id,))
        employee = db.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (employee_id,)).fetchone()
        if not employee or not employee['aktiv']:
            raise ValueError('Aktiven Mitarbeiter auswählen.')
        rights = self._rights(db, employee_id)
        if not rights:
            if not grant_material:
                raise ValueError('Für diesen Mitarbeiter müssen persönliche Materialrechte ausdrücklich freigegeben werden.')
            cap = self.p.workshop_orders.cap()
            if type(cap) is not int or cap <= 0:
                raise ValueError('Ein freigegebener persönlicher Kostenrahmen fehlt.')
            db.execute('''INSERT INTO assistent_rechte
                (mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,limit_cent,version,auth_version)
                VALUES(?, ?, 1, 0, 1, ?, 1, 1) RETURNING mitarbeiter_id''',
                       (employee_id, '', min(cap, 25000))).fetchall()
            self._audit(db, employee_id, 'einrichtung_materialrechte')
        db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (employee_id,))
        rights = self._rights(db, employee_id)
        token = secrets.token_urlsafe(48)
        issued = int(self.clock())
        expires = issued + INVITATION_SECONDS
        db.execute('''INSERT INTO assistent_einladungen
            (mitarbeiter_id,token_hash,issued_at,expires_at,used_at,rights_fingerprint)
            VALUES(?,?,?,?,NULL,?) ON CONFLICT(mitarbeiter_id) DO UPDATE SET
            token_hash=excluded.token_hash,issued_at=excluded.issued_at,
            expires_at=excluded.expires_at,used_at=NULL,rights_fingerprint=excluded.rights_fingerprint
            RETURNING mitarbeiter_id''',
                   (employee_id, _token_hash(token), issued, expires, _fingerprint(rights))).fetchall()
        self._audit(db, employee_id, 'einrichtungslink_erstellt')
        return {'employee_id': employee_id, 'name': employee['name'],
                'url': origin + SETUP_PATH + '#token=' + token, 'expires_at': _iso(expires)}

    def issue(self, employee_id, *, grant_material=False):
        origin = self.origin()
        with self.p.portal_originals_operation_lock(), self.db() as db:
            return self._create(db, employee_id, origin, grant_material)

    def issue_all(self, *, grant_material=False):
        origin = self.origin()
        with self.p.portal_originals_operation_lock(), self.db() as db:
            ids = [row['id'] for row in db.execute('SELECT id FROM mitarbeiter WHERE aktiv=1 ORDER BY id').fetchall()]
            return [self._create(db, mid, origin, grant_material) for mid in ids]

    def employees(self):
        now = int(self.clock())
        with self.db() as db:
            rows = db.execute('''SELECT m.id,m.name,r.*,i.token_hash,i.issued_at,i.expires_at,
                i.used_at,i.rights_fingerprint FROM mitarbeiter m
                LEFT JOIN assistent_rechte r ON r.mitarbeiter_id=m.id
                LEFT JOIN assistent_einladungen i ON i.mitarbeiter_id=m.id
                WHERE m.aktiv=1 ORDER BY m.name''').fetchall()
        result = []
        for row in rows:
            value = dict(row)
            rights = value['mitarbeiter_id'] is not None
            state = 'none'
            if value['token_hash']:
                state = ('used' if value['used_at'] is not None else
                         'expired' if value['expires_at'] <= now else
                         'stale' if not rights or not hmac.compare_digest(value['rights_fingerprint'], _fingerprint(value)) else 'pending')
            result.append({'id': value['id'], 'name': value['name'], 'has_rights': rights,
                           'has_password': bool(value['passwort_hash']),
                           **{key: value[key] for key in ('lesen','dokumentieren','einkaufen','limit_cent')},
                           'invitation_state': state,
                           'invitation_expires_at': _iso(value['expires_at']) if value['expires_at'] else None})
        return result

    def _lookup(self, db, token_hash):
        row = db.execute('''SELECT i.*,m.name,m.aktiv FROM assistent_einladungen i
            JOIN mitarbeiter m ON m.id=i.mitarbeiter_id WHERE i.token_hash=?''', (token_hash,)).fetchone()
        if (not row or not row['aktiv'] or row['used_at'] is not None
                or row['expires_at'] <= int(self.clock())):
            raise ValueError(INVALID_LINK)
        rights = self._rights(db, row['mitarbeiter_id'])
        if not rights or not hmac.compare_digest(row['rights_fingerprint'], _fingerprint(rights)):
            raise ValueError(INVALID_LINK)
        return dict(row), rights

    def inspect(self, token):
        token_hash = _token_hash(token)
        with self.db() as db:
            invite, _ = self._lookup(db, token_hash)
        return {'valid': True, 'employee': {'id': invite['mitarbeiter_id'], 'name': invite['name']},
                'expires_at': _iso(invite['expires_at'])}

    def redeem(self, token, password, repeat):
        token_hash = _token_hash(token)
        if not isinstance(password, str) or not 12 <= len(password) <= 256:
            raise ValueError('Ein persönliches Passwort mit 12 bis 256 Zeichen wählen.')
        if not isinstance(repeat, str) or password != repeat:
            raise ValueError('Die beiden Passwörter stimmen nicht überein.')
        with self.db() as db:
            self._lookup(db, token_hash)
        password_hash = generate_password_hash(password)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            # The hash is not consumed until every server check is successful.
            invite, rights = self._lookup(db, token_hash)
            mid = invite['mitarbeiter_id']
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (mid,))
            invite, rights = self._lookup(db, token_hash)
            used = int(self.clock())
            consumed = db.execute('''UPDATE assistent_einladungen SET used_at=?
                WHERE token_hash=? AND used_at IS NULL AND expires_at>?
                AND rights_fingerprint=? RETURNING mitarbeiter_id''',
                                  (used, token_hash, used, _fingerprint(rights))).fetchall()
            if len(consumed) != 1:
                raise ValueError(INVALID_LINK)
            db.execute('''UPDATE assistent_rechte SET passwort_hash=?,auth_version=auth_version+1
                WHERE mitarbeiter_id=?''', (password_hash, mid))
            self._audit(db, mid, 'einrichtungslink_eingeloest', actor='mitarbeiter:' + str(mid))
        return {'mitarbeiter_id': mid, 'version': rights['version'],
                'auth_version': rights['auth_version'] + 1}


def register_employee_invitations(p):
    service = EmployeeInvitations(p)
    service.init_schema()
    bp = Blueprint('employee_invitations', __name__)

    def admin_page(created=None, error=None, status=200):
        return render_template('mitarbeiter_einrichtung_admin.html', employees=service.employees(),
                               created_invitations=created or [],
                               created_invitation=(created or [None])[0], error=error), status

    @bp.get('/admin/mitarbeiter/einrichtung')
    @p.admin_required
    def admin_index():
        return admin_page()

    @bp.post('/admin/mitarbeiter/<int:employee_id>/einrichtungslink')
    @p.admin_required
    def create(employee_id):
        try:
            return admin_page([service.issue(employee_id, grant_material=request.form.get('grant_material') == 'on')])
        except ValueError as exc:
            return admin_page(error=str(exc), status=400)

    @bp.post('/admin/mitarbeiter/einrichtung/alle')
    @p.admin_required
    def create_all():
        try:
            return admin_page(service.issue_all(grant_material=request.form.get('grant_material') == 'on'))
        except ValueError as exc:
            return admin_page(error=str(exc), status=400)

    @bp.route(SETUP_PATH, methods=['GET', 'POST'])
    def setup():
        error, status = None, 200
        setup_token, employee, expires_at = None, None, None
        if request.method == 'POST':
            limited, _ = p.login_rate_limit_status('assistent_setup', 'einrichtung')
            if limited:
                error, status = RATE_MESSAGE, 429
            else:
                try:
                    who = service.redeem(request.form.get('token'), request.form.get('password'), request.form.get('password_confirm'))
                    p.clear_login_attempts('assistent_setup', 'einrichtung')
                    # Explicitly switch to this employee, removing unrelated
                    # admin/partner/workshop state and stale confirmations.
                    session.clear()
                    session.permanent = True
                    session['assistent_mid'] = who['mitarbeiter_id']
                    session['assistent_version'] = who['version']
                    session['assistent_auth_version'] = who['auth_version']
                    session['csrf_token'] = secrets.token_urlsafe(32)
                    flash('Dein persönlicher Zugang ist eingerichtet. Dein persönliches Profil ist bereit.', 'success')
                    response = redirect('/werkstatt/mein-konto', code=303)
                    for scope in ('admin', 'partner'):
                        p.clear_remember_login_cookie(response, scope)
                    return response
                except ValueError as exc:
                    p.record_failed_login('assistent_setup', 'einrichtung')
                    error, status = str(exc), 400
                    try:
                        inspected = service.inspect(request.form.get('token'))
                        setup_token = request.form.get('token')
                        employee = inspected['employee']
                        expires_at = inspected['expires_at']
                    except ValueError:
                        pass
        return render_template('mitarbeiter_einrichtung.html', error=error,
                               setup_token=setup_token, valid=bool(setup_token),
                               employee=employee, expires_at=expires_at), status

    @bp.post(SETUP_PATH + '/pruefen')
    def check():
        limited, _ = p.login_rate_limit_status('assistent_setup', 'einrichtung')
        if limited:
            return jsonify(valid=False, error=RATE_MESSAGE), 429
        data = request.get_json(silent=True)
        try:
            return jsonify(service.inspect(data.get('token') if isinstance(data, dict) else None))
        except ValueError:
            p.record_failed_login('assistent_setup', 'einrichtung')
            return jsonify(valid=False, error=INVALID_LINK), 400

    @bp.after_request
    def private_response(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        return response

    p.app.register_blueprint(bp)
    return service
