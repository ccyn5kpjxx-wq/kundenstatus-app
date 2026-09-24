"""Opt-in public intake for a MOS termination declaration (§ 312k BGB).

Receipt is a declaration audit, never a rental cancellation or refund.  Register
the blueprint independently of the new-booking switch so existing contracts can
still be terminated if new sales are paused.  The operator must expressly enable
the route after legal review and a real mail acceptance test.
"""

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from hashlib import sha256
import hmac
import json
import os
import secrets
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import click
from flask import Blueprint, abort, current_app, make_response, render_template, request, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from mos_contract_delivery import _single_address, _smtp_send


MAX_BODY = 8192
HOURLY_IP_LIMIT = 30
RECEIPT_TOKEN_SECONDS = 30 * 24 * 60 * 60


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_terminations (
        id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
        customer_email TEXT NOT NULL, declaration_json TEXT NOT NULL,
        declaration_text TEXT NOT NULL, declaration_sha256 TEXT NOT NULL,
        submitted_at TEXT NOT NULL, received_at TEXT NOT NULL,
        mail_status TEXT NOT NULL, mail_attempts INTEGER NOT NULL DEFAULT 0,
        mail_attempted_at TEXT, mail_accepted_at TEXT, mail_detail TEXT NOT NULL DEFAULT '')''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_termination_limits (
        bucket TEXT PRIMARY KEY, attempts INTEGER NOT NULL)''')


def _now():
    return datetime.now(timezone.utc).isoformat()


def _field(form, key, maximum):
    value = str(form.get(key, '')).strip()
    if not value or len(value) > maximum or any(ord(ch) < 32 for ch in value):
        raise ValueError('Bitte alle erforderlichen Angaben prüfen.')
    return value


def _declaration(form):
    name = _field(form, 'name', 150)
    email = _field(form, 'email', 254).lower()
    contract = _field(form, 'contract_reference', 100)
    if not _single_address(email):
        raise ValueError('Bitte eine gültige E-Mail-Adresse angeben.')
    kind = form.get('kind', '')
    if kind not in {'ordinary', 'extraordinary'}:
        raise ValueError('Bitte die Art der Kündigung wählen.')
    reason = str(form.get('reason', '')).strip()
    if len(reason) > 1000 or any(ord(ch) < 32 and ch not in '\n\t' for ch in reason):
        raise ValueError('Bitte den Kündigungsgrund prüfen.')
    if kind == 'extraordinary' and not reason:
        raise ValueError('Bitte den Grund der außerordentlichen Kündigung angeben.')
    timing = form.get('timing', '')
    if timing not in {'earliest', 'date'}:
        raise ValueError('Bitte einen gewünschten Beendigungszeitpunkt wählen.')
    desired_date = ''
    if timing == 'date':
        desired_date = _field(form, 'desired_date', 10)
        try:
            if datetime.strptime(desired_date, '%Y-%m-%d').strftime('%Y-%m-%d') != desired_date:
                raise ValueError
        except ValueError:
            raise ValueError('Bitte ein gültiges Beendigungsdatum angeben.') from None
    return {'name': name, 'email': email, 'contract_reference': contract,
            'kind': kind, 'reason': reason if kind == 'extraordinary' else '',
            'timing': timing, 'desired_date': desired_date}


def _text(data, submitted_at, received_at):
    def local_stamp(value):
        moment = datetime.fromisoformat(value).astimezone(ZoneInfo('Europe/Berlin'))
        return moment.strftime('%d.%m.%Y %H:%M:%S Uhr (%z, Berlin)')

    kind = 'außerordentliche Kündigung' if data['kind'] == 'extraordinary' else 'ordentliche Kündigung'
    date = data['desired_date'] if data['timing'] == 'date' else 'zum frühestmöglichen Zeitpunkt'
    parts = ['MOS – Kündigungserklärung',
             'Ich erkläre die ' + kind + ' des folgenden Vertrags.',
             'Bezeichnung des Vertrags: ' + data['contract_reference'],
             'Name: ' + data['name'],
             'E-Mail für die Bestätigung: ' + data['email'],
             'Gewünschte Beendigung: ' + date]
    if data['kind'] == 'extraordinary':
        parts.append('Angegebener Grund: ' + data['reason'])
    parts.extend(['Abgabe über die Schaltfläche „jetzt kündigen“: ' + local_stamp(submitted_at),
                  'Zugang beim Vermieter: ' + local_stamp(received_at),
                  'Vermieter: Gärtner GmbH Karosserie + Lack',
                  'Dies bestätigt den Eingang der Erklärung, nicht bereits die rechtliche Wirksamkeit '
                  'oder eine Rückzahlung. Die Erklärung wird geprüft.'])
    return '\n'.join(parts) + '\n'


def _limit(db, secret, ip, now):
    """Throttle the sender, never the unverified address of the customer."""
    hour = now[:13]
    old = (datetime.fromisoformat(now) - timedelta(hours=48)).isoformat()[:13]
    db.execute('DELETE FROM miet_checkout_termination_limits WHERE bucket<?', (old,))
    digest = hmac.new(secret.encode('utf-8'), ('ip:' + (ip or 'unknown')).encode('utf-8'), 'sha256').hexdigest()
    bucket = hour + ':ip:' + digest
    db.execute('''INSERT INTO miet_checkout_termination_limits (bucket,attempts)
        VALUES (?,0) ON CONFLICT (bucket) DO NOTHING''', (bucket,))
    row = db.execute('SELECT attempts FROM miet_checkout_termination_limits WHERE bucket=?', (bucket,)).fetchone()
    if row['attempts'] >= HOURLY_IP_LIMIT:
        raise OverflowError('Zu viele Anfragen. Bitte wenden Sie sich direkt an den Vermieter.')
    db.execute('UPDATE miet_checkout_termination_limits SET attempts=attempts+1 WHERE bucket=?', (bucket,))


def submit(portal, data, request_key, *, ip, now=None):
    """Persist the exact declaration once. Never change a rental or Stripe state."""
    if len(request_key) > 100 or len(request_key) < 20:
        raise ValueError('Das Formular ist veraltet. Bitte die Seite neu laden.')
    now = now or _now()
    canonical = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = sha256(canonical.encode('utf-8')).hexdigest()
    db = portal.get_db()
    try:
        if not portal.USE_POSTGRES:
            db.execute('BEGIN IMMEDIATE')
        duplicate = db.execute('SELECT * FROM miet_checkout_terminations WHERE request_key=?', (request_key,)).fetchone()
        if duplicate:
            row = dict(duplicate)
            if sha256(row['declaration_json'].encode('utf-8')).hexdigest() != digest:
                raise ValueError('Dieses Formular wurde bereits mit anderen Angaben abgesendet.')
            db.rollback()
            return row, False
        _limit(db, str(portal.app.secret_key), ip, now)
        identifier = secrets.token_urlsafe(24)
        declaration_text = _text(data, now, now)
        inserted = db.execute('''INSERT INTO miet_checkout_terminations
            (id,request_key,customer_email,declaration_json,declaration_text,
             declaration_sha256,submitted_at,received_at,mail_status)
            VALUES (?,?,?,?,?,?,?,?,'queued')
            ON CONFLICT (request_key) DO NOTHING RETURNING id''',
            (identifier, request_key, data['email'], canonical, declaration_text,
             sha256(declaration_text.encode('utf-8')).hexdigest(), now, now))
        if not inserted.fetchone():
            duplicate = db.execute('SELECT * FROM miet_checkout_terminations WHERE request_key=?',
                                   (request_key,)).fetchone()
            if not duplicate or sha256(duplicate['declaration_json'].encode('utf-8')).hexdigest() != digest:
                raise ValueError('Dieses Formular wurde bereits mit anderen Angaben abgesendet.')
            db.rollback()
            return dict(duplicate), False
        db.commit()
        row = db.execute('SELECT * FROM miet_checkout_terminations WHERE id=?', (identifier,)).fetchone()
        return dict(row), True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _verified(row):
    if not row or sha256(row['declaration_text'].encode('utf-8')).hexdigest() != row['declaration_sha256']:
        raise ValueError('Kündigungserklärung muss manuell geprüft werden.')
    data = json.loads(row['declaration_json'])
    if data['email'] != row['customer_email'] or not _single_address(row['customer_email']):
        raise ValueError('Kündigungserklärung muss manuell geprüft werden.')
    return data


def _message(row, cfg):
    _verified(row)
    msg = EmailMessage()
    msg['To'] = row['customer_email']
    msg['From'] = formataddr((cfg.get('display_name', ''), cfg['from_address']))
    msg['Subject'] = 'Eingang Ihrer MOS-Kündigungserklärung'
    msg['Message-ID'] = ('<mos-termination-' + sha256(row['id'].encode()).hexdigest()[:32]
                         + '@' + cfg['from_address'].rsplit('@', 1)[1] + '>')
    msg.set_content('Wir bestätigen den Eingang Ihrer Erklärung. Der vollständige Inhalt '
                    'einschließlich Abgabe- und Zugangszeit steht nachfolgend.\n\n'
                    + row['declaration_text'])
    return msg


def deliver_one(portal, identifier, mail_cfg, *, enabled):
    """Attempt immediate SMTP acceptance once; ambiguous DATA requires review."""
    if (not enabled or not mail_cfg.get('smtp_configured')
            or not (mail_cfg.get('smtp_ssl') or mail_cfg.get('smtp_tls'))
            or not _single_address(mail_cfg.get('from_address'))
            or not mail_cfg.get('_smtp_password')):
        return 'disabled'
    db = portal.get_db()
    try:
        row = db.execute('SELECT * FROM miet_checkout_terminations WHERE id=?', (identifier,)).fetchone()
        row = dict(row) if row else None
        if not row:
            raise ValueError('Kündigungserklärung fehlt.')
        if row['mail_status'] != 'queued':
            return row['mail_status']
        message = _message(row, mail_cfg)
        claimed = db.execute('''UPDATE miet_checkout_terminations
            SET mail_status='sending',mail_attempts=mail_attempts+1,mail_attempted_at=?
            WHERE id=? AND mail_status='queued' ''', (_now(), identifier))
        db.commit()
        if claimed.rowcount != 1:
            return 'not_due'
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    outcome = _smtp_send(message, mail_cfg)
    db = portal.get_db()
    try:
        if outcome == 'sent':
            db.execute('''UPDATE miet_checkout_terminations
                SET mail_status='sent',mail_accepted_at=?,mail_detail=''
                WHERE id=? AND mail_status='sending' ''', (_now(), identifier))
        else:
            # No automatic retry after any uncertain SMTP outcome. An operator
            # must inspect the mailbox and the record before another attempt.
            db.execute('''UPDATE miet_checkout_terminations
                SET mail_status='review',mail_detail=?
                WHERE id=? AND mail_status='sending' ''',
                ('SMTP-Annahme unklar' if outcome == 'uncertain' else 'SMTP hat nicht angenommen', identifier))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return outcome


def unresolved_count(portal):
    db = portal.get_db()
    try:
        return db.execute("SELECT COUNT(*) AS n FROM miet_checkout_terminations WHERE mail_status!='sent'").fetchone()['n']
    finally:
        db.close()


def readiness_errors(portal):
    """Activation preflight, never a per-request availability switch."""
    errors = []
    if not portal.USE_POSTGRES:
        errors.append('Autoritative PostgreSQL-Datenbank fehlt.')
    if (getattr(portal, 'USING_EPHEMERAL_SECRET_KEY', True)
            or getattr(portal, 'USING_GENERATED_FLASK_SECRET_KEY', True)
            or not isinstance(portal.app.secret_key, str)
            or len(portal.app.secret_key) < 32):
        errors.append('Dauerhafter sicherer Flask-Schlüssel fehlt.')
    if not portal.app.config.get('SESSION_COOKIE_SECURE'):
        errors.append('Sichere HTTPS-Sitzungscookies fehlen.')
    origin = urlsplit(portal.app.config.get('MOS_TERMINATION_ORIGIN') or '')
    if (origin.scheme != 'https' or not origin.hostname or origin.username or origin.password
            or origin.path not in ('', '/') or origin.query or origin.fragment):
        errors.append('Öffentliche HTTPS-Origin für den Kündigungsweg fehlt.')
    if os.environ.get('MOS_TERMINATION_EMAIL_ENABLED') != '1':
        errors.append('Explizite Freigabe für sofortige E-Mail-Bestätigung fehlt.')
    cfg = portal.get_werkstatt_smtp_config()
    if (not cfg.get('smtp_configured') or not (cfg.get('smtp_ssl') or cfg.get('smtp_tls'))
            or not cfg.get('_smtp_password') or not _single_address(cfg.get('from_address'))):
        errors.append('Verschlüsselter SMTP-Versand ist nicht bereit.')
    return errors


def register(portal, *, enabled=False, test_mode=False):
    """Mount independent public routes; ``enabled`` is an explicit operator gate."""
    app = portal.app
    app.config['MOS_TERMINATION_ORIGIN'] = (os.environ.get('MOS_TERMINATION_ORIGIN') or '').rstrip('/')
    if enabled and not test_mode:
        errors = readiness_errors(portal)
        if errors:
            raise ValueError('MOS-Kündigungsweg kann nicht aktiviert werden: ' + '; '.join(errors))
    if enabled:
        db = portal.get_db()
        try:
            init_schema(db)
            db.commit()
        finally:
            db.close()
    bp = Blueprint('mos_termination', __name__, url_prefix='/mieten')

    @bp.before_request
    def guard():
        if not enabled:
            abort(404)
        if not test_mode:
            origin = urlsplit(app.config['MOS_TERMINATION_ORIGIN'])
            if (not request.is_secure or
                    (request.endpoint != 'mos_termination.admin_declarations'
                     and request.host.lower() != origin.netloc.lower())):
                abort(403)
        if request.content_length and request.content_length > MAX_BODY:
            abort(413)

    @bp.after_request
    def headers(response):
        response.headers.update({'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex, nofollow',
                                 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff',
                                 'X-Frame-Options': 'DENY'})
        return response

    def signer():
        return URLSafeTimedSerializer(app.secret_key, salt='mos-termination-receipt-v1')

    def receipt_link(row):
        token = signer().dumps({'id': row['id'], 'hash': row['declaration_sha256']})
        return url_for('mos_termination.receipt', token=token)

    @bp.route('/kuendigen', methods=['GET', 'POST'])
    def form():
        if request.method == 'GET':
            session['csrf_token'] = session.get('csrf_token') or secrets.token_urlsafe(32)
            session['mos_termination_form'] = secrets.token_urlsafe(32)
            return render_template('mos_termination/form.html', csrf=session['csrf_token'],
                                   form_token=session['mos_termination_form'])
        csrf = session.get('csrf_token')
        form_token = request.form.get('form_token', '')
        allowed = {session.get('mos_termination_form'), *session.get('mos_termination_recent', [])}
        if (not csrf or not hmac.compare_digest(csrf, request.form.get('csrf_token', ''))
                or not form_token or form_token not in allowed or request.form.get('website')):
            abort(400)
        try:
            data = _declaration(request.form)
            row, fresh = submit(portal, data, form_token, ip=request.remote_addr)
        except OverflowError:
            abort(429)
        except ValueError as exc:
            return render_template('mos_termination/form.html', csrf=csrf,
                                   form_token=form_token, error=str(exc)), 400
        if fresh:
            session['mos_termination_recent'] = ([form_token] + session.get('mos_termination_recent', []))[:3]
            session['mos_termination_form'] = secrets.token_urlsafe(32)
            try:
                cfg = portal.get_werkstatt_smtp_config() if not test_mode else app.config.get('MOS_TERMINATION_TEST_MAIL', {})
                outcome = deliver_one(portal, row['id'], cfg, enabled=True)
            except Exception:
                app.logger.exception('MOS Kündigungsbestätigung erfordert sofortige manuelle Prüfung')
                outcome = 'review'
        else:
            outcome = row['mail_status']
        status = 200 if outcome == 'sent' or (test_mode and outcome == 'disabled') else 202
        return render_template('mos_termination/received.html', row=row,
                               mail_sent=outcome == 'sent', receipt_url=receipt_link(row)), status

    @bp.get('/kuendigen/beleg/<token>')
    def receipt(token):
        try:
            identity = signer().loads(token, max_age=RECEIPT_TOKEN_SECONDS)
        except (BadSignature, SignatureExpired):
            abort(404)
        db = portal.get_db()
        try:
            row = db.execute('SELECT * FROM miet_checkout_terminations WHERE id=?',
                             (identity.get('id'),)).fetchone()
            row = dict(row) if row else None
        finally:
            db.close()
        if not row or not hmac.compare_digest(identity.get('hash', ''), row['declaration_sha256']):
            abort(404)
        _verified(row)
        response = make_response(row['declaration_text'])
        response.headers['Content-Type'] = 'text/plain; charset=utf-8'
        response.headers['Content-Disposition'] = 'attachment; filename="MOS-Kuendigungserklaerung.txt"'
        return response

    @bp.get('/admin/kuendigungen')
    @portal.admin_required
    def admin_declarations():
        db = portal.get_db()
        try:
            rows = [dict(row) for row in db.execute('''SELECT id,customer_email,declaration_text,
                received_at,mail_status,mail_attempted_at,mail_accepted_at,mail_detail
                FROM miet_checkout_terminations ORDER BY received_at DESC LIMIT 100''').fetchall()]
            unresolved = db.execute("SELECT COUNT(*) AS n FROM miet_checkout_terminations WHERE mail_status!='sent'").fetchone()['n']
        finally:
            db.close()
        return render_template('mos_termination/admin.html', rows=rows,
                               unresolved=unresolved)

    @app.cli.command('mos-termination-check')
    def check_termination_mail():
        if not enabled:
            click.echo('MOS-Kündigungsweg ist nicht aktiviert.', err=True)
            raise SystemExit(2)
        count = unresolved_count(portal)
        click.echo(f'Offene MOS-Kündigungsbestätigungen: {count}')
        if count:
            raise SystemExit(1)

    app.register_blueprint(bp)
    return bp
