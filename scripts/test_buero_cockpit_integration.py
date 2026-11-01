"""Fail-closed full-app office integration regression, safe for discovery.

Run: python scripts/test_buero_cockpit_integration.py
Environment, filesystem writes, SQLite access, workers, subprocesses and all
network operations are isolated BEFORE importing the real Flask application.
Guards apply only during that import and this class's own fixtures/tests. Other
discovered unit tests retain their normal environment and concurrency helpers.
No runtime server is started. Do not combine with an existing real-app fixture.
"""
from contextlib import ExitStack, closing, contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
import os
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if 'app' in sys.modules:
    raise RuntimeError('Run this integration regression in its own isolated Python process.')
SCRATCH = Path(tempfile.mkdtemp(prefix='office-cockpit-isolated-')).resolve()
BLOCKED_OPERATIONS = []
GUARD_ACTIVE = False
runtime = {key: value for key, value in os.environ.items()
           if key.upper() in {'PATH', 'SYSTEMROOT', 'WINDIR', 'SYSTEMDRIVE', 'COMSPEC', 'PATHEXT'}}
ISOLATED_ENV = {**runtime,
    'TEMP': str(SCRATCH), 'TMP': str(SCRATCH), 'TMPDIR': str(SCRATCH),
    'RENDER': 'isolated-office-integration', 'DATABASE_URL': '', 'REQUIRE_POSTGRES_ON_RENDER': '0',
    'DATA_DIR': str(SCRATCH / 'data'), 'SQLITE_DB_PATH': str(SCRATCH / 'synthetic.sqlite'),
    'UPLOAD_DIR': str(SCRATCH / 'uploads'), 'BACKUP_DIR': str(SCRATCH / 'backups'),
    'DELETED_UPLOAD_DIR': str(SCRATCH / 'deleted'), 'MAILBOX_OUTBOX_DIR': str(SCRATCH / 'outbox'),
    'AUTO_BACKUP_ENABLED': '0', 'AUTO_CHANGE_BACKUP_ENABLED': '0', 'AUTO_BACKUP_ON_STARTUP': '0',
    'GOOGLE_ADS_AUTO_SYNC_ENABLED': '0', 'ASSISTANT_ORDER_WORKER_ENABLED': '0',
    'ASSISTANT_ORDER_SEND_ENABLED': '0', 'PURCHASE_MONITOR_WORKER_ENABLED': '0',
    'MATERIAL_WHATSAPP_WORKER_ENABLED': '0', 'MATERIAL_WHATSAPP_ENABLED': '0',
    'MATERIAL_WHATSAPP_REPLIES_ENABLED': '0', 'CHECK24_WORKER_ENABLED': '0',
    'MAILBOX_SEND_ENABLED': '0', 'MAILBOX_MOVE_ENABLED': '0', 'MAILBOX_FLAGS_ENABLED': '0',
    'MAILBOX_WRITE_ENABLED': '0', 'WHATSAPP_ENABLED': '0', 'ENABLE_LOCAL_OCR': '0',
    'KI_ASSISTENT_ONLINE_ENABLED': '0', 'LOCAL_SPEECH_ENABLED': '0', 'CODEX_BRIDGE_ENABLED': '0',
    'OFFICE_LEXWARE_UPLOAD_ENABLED': '0', 'ASSISTANT_NATIVE_COCKPIT': '1',
    'OPENAI_API_KEY': '', 'LEXWARE_API_KEY': '', 'GOOGLE_APPLICATION_CREDENTIALS': '',
    'GOOGLE_DOC_AI_SERVICE_ACCOUNT_FILE': '', 'GOOGLE_DOC_AI_PROJECT_ID': '',
    'WHATSAPP_ACCESS_TOKEN': '', 'WHATSAPP_WORKSHOP_NUMBERS': '', 'MAIL_IMAP_PASS': '',
    'MAIL_SMTP_PASS': '', 'SCHADEN_IMAP_PASS': '', 'SCHADEN_SMTP_PASS': '', 'SMTP_PASSWORD': '',
    'FLASK_SECRET_KEY': 'synthetic-office-integration-only', 'ADMIN_PASS': 'synthetic-admin-only',
    'ADMIN_PASSWORD': '',
}


def inside(path):
    try:
        return Path(path).resolve().is_relative_to(SCRATCH)
    except (TypeError, ValueError):
        return False


def forbidden(event):
    frame = sys._getframe(1)
    callers = []
    for _ in range(10):
        if frame is None:
            break
        callers.append(Path(frame.f_code.co_filename).name + ':' + frame.f_code.co_name)
        frame = frame.f_back
    BLOCKED_OPERATIONS.append(event + ' [' + ' <- '.join(callers) + ']')
    raise AssertionError('Forbidden operation in isolated office regression: ' + event)


def audit(event, args):
    # Python audit hooks cannot be removed. Keep this hook inert whenever
    # another discovered module is importing or executing its own fixtures.
    if not GUARD_ACTIVE:
        return
    if event in {'socket.connect', 'socket.bind', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
        forbidden(event)
    if event == 'sqlite3.connect' and args[0] != ':memory:' and not inside(args[0]):
        forbidden('database outside test scratch')
    if event == 'open':
        path, mode, flags = args
        if not isinstance(path, int) and Path(path).name.lower() in {'.env', '.env.local'}:
            forbidden('environment-file read')
        writing = bool(mode and any(character in mode for character in 'wax+')) or bool(
            isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
        if writing and not isinstance(path, int) and not inside(path):
            forbidden('write outside test scratch')
    if event in {'os.mkdir', 'os.remove', 'os.rmdir', 'os.rename'}:
        for path in args[:2] if event == 'os.rename' else args[:1]:
            if not inside(path):
                forbidden('filesystem mutation outside test scratch')


sys.addaudithook(audit)


def deny_network(*args, **kwargs):
    forbidden('network call')


def deny_worker(*args, **kwargs):
    forbidden('background worker start')


original_exists = Path.exists


def no_env_files(path):
    return False if path in (ROOT / '.env', ROOT / '.env.local') else original_exists(path)


@contextmanager
def isolated_operations():
    """Restore every process-global patch when our own work has finished."""
    global GUARD_ACTIVE
    if GUARD_ACTIVE:
        raise RuntimeError('Office integration isolation must not be nested.')
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, ISOLATED_ENV, clear=True))
        stack.enter_context(patch.object(tempfile, 'tempdir', str(SCRATCH)))
        stack.enter_context(patch.object(sys, 'dont_write_bytecode', True))
        stack.enter_context(patch.object(sys, 'path', [str(ROOT), *sys.path]))
        stack.enter_context(patch('socket.socket.connect', deny_network))
        stack.enter_context(patch('socket.socket.connect_ex', deny_network))
        stack.enter_context(patch('socket.create_connection', deny_network))
        # Suppress urllib3's optional Windows IPv6 bind probe during import;
        # even loopback sockets remain forbidden inside our regression.
        stack.enter_context(patch('socket.has_ipv6', False))
        stack.enter_context(patch.object(threading.Thread, 'start', deny_worker))
        stack.enter_context(patch.object(Path, 'exists', no_env_files))
        GUARD_ACTIVE = True
        try:
            yield
        finally:
            GUARD_ACTIVE = False


with isolated_operations():
    import app as p  # noqa: E402 -- isolation must precede this import.
    from werkzeug.security import generate_password_hash
    from werkzeug.datastructures import MultiDict
    from pypdf import PdfWriter
    from werkstatt_buero_zugang import CATEGORY_LABELS
    from werkstatt_buero_sicherung import TABLES as OFFICE_BACKUP_TABLES
    import mailbox_client

BASE_URL = 'https://localhost'
MID = 910001
HID = 910001
SECRET_MARKER = 'SYNTHETIC-PRIVATE-HR-AND-FINANCE-NOT-FOR-OFFICE'


class RenderedForms(HTMLParser):
    """Read successful controls from the actual server HTML, keeping duplicates.

    Buttons/unchecked checkboxes are only submitted when a test explicitly
    activates them. Hidden controls include the central CSRF injection as well
    as any field supplied by the individual product template.
    """
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.forms, self.current, self.textarea, self.option, self.select = [], None, None, None, None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == 'form':
            self.current = {'action': attributes.get('action', ''),
                            'method': attributes.get('method', 'get').lower(),
                            'data': MultiDict(), 'buttons': []}
            self.forms.append(self.current)
        if self.current is None or 'disabled' in attributes:
            return
        name = attributes.get('name')
        if tag == 'input' and name:
            kind = attributes.get('type', 'text').lower()
            if kind in ('file', 'submit', 'button', 'reset'):
                return
            if kind in ('checkbox', 'radio') and 'checked' not in attributes:
                return
            self.current['data'].add(name, attributes.get('value', 'on' if kind in ('checkbox', 'radio') else ''))
        elif tag == 'button' and name:
            self.current['buttons'].append((name, attributes.get('value', '')))
        elif tag == 'textarea' and name:
            self.textarea = [name, '']
        elif tag == 'select' and name:
            self.select = {'name': name, 'values': [], 'selected': [], 'multiple': 'multiple' in attributes}
        elif tag == 'option' and self.select is not None:
            value = attributes.get('value', '')
            self.select['values'].append(value)
            if 'selected' in attributes:
                self.select['selected'].append(value)

    def handle_data(self, data):
        if self.textarea is not None:
            self.textarea[1] += data

    def handle_endtag(self, tag):
        if tag == 'textarea' and self.textarea is not None and self.current is not None:
            self.current['data'].add(*self.textarea)
            self.textarea = None
        elif tag == 'select' and self.select is not None and self.current is not None:
            chosen = self.select['selected'] or ([] if self.select['multiple'] else self.select['values'][:1])
            for value in chosen:
                self.current['data'].add(self.select['name'], value)
            self.select = None
        elif tag == 'form':
            self.current = None


class OfficeCockpitIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with isolated_operations():
            cls.seed_fixture()

    @classmethod
    def seed_fixture(cls):
        if BLOCKED_OPERATIONS:
            raise AssertionError('App import attempted forbidden operations: ' + repr(BLOCKED_OPERATIONS))
        if not inside(p.DB) or not inside(p.UPLOAD_DIR) or not inside(p.BACKUP_DIR):
            raise AssertionError('Application paths are not isolated')
        p.init_db()
        now = p.now_str()
        with closing(p.get_db()) as db:
            if db.execute('SELECT COUNT(*) FROM buero_zugaenge WHERE aktiv=1').fetchone()[0]:
                raise AssertionError('Registration must never enable a real or default office identity')
            db.execute('''INSERT INTO mitarbeiter(id,name,rolle,adresse,notiz,aktiv,erstellt_am,geaendert_am)
                VALUES(?,?,'Büro',?,?,1,?,?)''',
                (MID, 'Synthetische Büromitarbeiterin', SECRET_MARKER, SECRET_MARKER, now, now))
            db.execute('''INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,
                limit_cent,version,auth_version) VALUES(?,?,1,0,0,0,1,1)''',
                (MID, generate_password_hash('SyntheticOfficePassword2026!')))
            db.execute('''INSERT INTO autohaeuser(id,name,slug,portal_key,zugangscode,erstellt_am)
                VALUES(?,'SYNTH AUTOHAUS','synthetic-office-dealer','synthetic-office-key','synthetic-code',?)''', (HID, now))
            today = datetime.now(ZoneInfo('Europe/Berlin')).date()
            for oid, state, incoming, outgoing in ((910001, 2, today, today + timedelta(days=1)),
                                                    (910002, 4, today - timedelta(days=1), today)):
                db.execute('''INSERT INTO auftraege(id,autohaus_id,fahrzeug,kennzeichen,status,archiviert,
                    transport_art,annahme_datum,annahme_uhrzeit,abholtermin,abhol_uhrzeit,
                    abhol_adresse,fahrzeug_abholbereit,beschreibung,werkstatt_angebot_text,
                    erstellt_am,geaendert_am) VALUES(?,?,'SYNTH Fahrzeug',?,?,0,'hol_und_bring',?,'09:00',?,'16:00',
                    'SYNTH Testweg 1',1,?,?,?,?)''',
                    (oid, HID, 'SYNTH-' + str(oid), state, incoming.strftime('%d.%m.%Y'),
                     outgoing.strftime('%d.%m.%Y'), SECRET_MARKER, SECRET_MARKER, now, now))
            db.execute('''INSERT INTO kalender_notizen(datum,titel,notiz,kategorie,wiederholung,erstellt_am)
                VALUES(?,?,?,'privat','einmalig',?)''', (today.strftime('%d.%m.%Y'), SECRET_MARKER, SECRET_MARKER, now))
            db.commit()

    def setUp(self):
        self.enterContext(isolated_operations())
        self.enterContext(patch.dict(p.app.config, TESTING=True,
                                    OFFICE_CEO_MAIL_ADDRESS='chef@example.invalid'))
        self.enterContext(patch.object(p, 'get_werkstatt_imap_config',
                                      return_value={'user': 'info@example.invalid'}))
        # Register before fixture writes: even a caught forbidden operation in
        # setup must fail this test, and cleanup always restores global patches.
        self.addCleanup(self.assert_no_external_operations)
        self.admin = p.app.test_client()
        with self.admin.session_transaction() as state:
            state.update(admin=True, csrf_token='synthetic-admin-csrf')
        self.grant(list(CATEGORY_LABELS))
        self.staff = p.app.test_client()
        with self.staff.session_transaction() as state:
            state.update(assistent_mid=MID, assistent_version=1, assistent_auth_version=1,
                         csrf_token='synthetic-office-csrf')
        self.guest = p.app.test_client()

    def grant(self, categories, active=True):
        with closing(p.get_db()) as db:
            prior = db.execute('SELECT version FROM buero_zugaenge WHERE mitarbeiter_id=?', (MID,)).fetchone()
        with p.app.test_request_context():
            from flask import session
            session['admin'] = True
            p.office_access.set_access(MID, categories, active=active,
                                       expected_version=prior['version'] if prior else 0)

    def get(self, path, client=None):
        return (client or self.staff).get(path, base_url=BASE_URL)

    def post(self, path, data=None, client=None):
        return (client or self.staff).post(path, data=data or {}, base_url=BASE_URL)

    def assert_no_external_operations(self):
        self.assertEqual(BLOCKED_OPERATIONS, [], 'A forbidden operation was attempted, even if the app caught its exception')

    def rendered_form(self, response, **matches):
        self.assertEqual(response.status_code, 200)
        forms = [form for form in RenderedForms(response.get_data(as_text=True)).forms
                 if form['method'] == 'post' and all(form['data'].get(key) == str(value) for key, value in matches.items())]
        self.assertEqual(len(forms), 1, 'Expected one actual rendered POST form for ' + repr(matches))
        form = forms[0]
        tokens = form['data'].getlist('csrf_token')
        self.assertGreaterEqual(len(tokens), 2, 'Exercise both template token and central HTML injection')
        self.assertTrue(tokens[0])
        self.assertEqual(len(set(tokens)), 1, 'Every rendered CSRF copy must contain the same token value')
        return form

    def submit_rendered(self, form, page, *, client=None, button=None, **changes):
        data = MultiDict(form['data'])
        for name, value in changes.items():
            data.setlist(name, value if isinstance(value, list) else [value])
        if button is not None:
            self.assertIn(button, form['buttons'])
            data.add(*button)
        path = form['action'] or page
        self.assertTrue(path.startswith('/') and not path.startswith('//'))
        return self.post(path, data, client)

    def assert_limited_page(self, response):
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('Bürobereiche', html)
        self.assertIn('no-store', response.headers.get('Cache-Control', ''))
        self.assertNotIn(SECRET_MARKER, html)
        with p.app.test_request_context():
            from flask import url_for
            forbidden_paths = [url_for('admin_mitarbeiter'), url_for('werkstatt_orders.index')]
        for path in forbidden_paths:
            self.assertNotIn('href="' + path + '"', html)
        return html

    def test_01_full_app_registration_and_every_office_get_renders_without_full_navigation(self):
        for blueprint in ('office_access', 'office_cockpit', 'office_planning', 'office_sections',
                          'buero_belege', 'office_lexware', 'office_payroll'):
            self.assertIn(blueprint, p.app.blueprints)
        for table in ('buero_zugaenge', 'buero_zugang_audit', 'buero_arbeitszeit_pruefungen',
                      'buero_belege', 'buero_belege_audit', 'buero_belege_seen',
                      'buero_lexware_transfers', 'buero_posteingang_receipts', 'buero_posteingang_audit',
                      'buero_lohnlaeufe', 'buero_lohnzettel', 'buero_lohn_audit'):
            self.assertIn(table, p.BACKUP_TABLES)
        for path in ('/werkstatt/buero', '/werkstatt/buero/planung', '/werkstatt/buero/kalender',
                     '/werkstatt/buero/arbeitszeiten', '/werkstatt/buero/abwesenheit',
                     '/werkstatt/buero/mahnungen', '/werkstatt/buero/news', '/werkstatt/buero/bewerbungen',
                     '/werkstatt/buero/lieferanten', '/werkstatt/buero/postfach', '/werkstatt/buero/zugaenge',
                     '/werkstatt/buero/autohausportale', '/werkstatt/buero/belege',
                     '/werkstatt/buero/loehne', '/admin/mail'):
            with self.subTest(path=path):
                self.assert_limited_page(self.get(path))
        html = self.get('/werkstatt/buero/planung').get_data(as_text=True)
        self.assertIn('SYNTH-910001', html)
        self.assertIn('SYNTH-910002', html)
        self.assertIn('Bei Kunde oder Partner holen', html)
        self.assertIn('Zum Kunden oder Partner zurückbringen', html)

    def test_02_personal_login_uses_existing_identity_and_routes_to_office(self):
        client = p.app.test_client()
        self.assertEqual(self.get('/werkstatt/buero/login', client).status_code, 200)
        with client.session_transaction() as state:
            csrf = state['csrf_token']
        response = self.post('/werkstatt/assistent/login', {'csrf_token': csrf, 'mitarbeiter_id': str(MID),
            'password': 'SyntheticOfficePassword2026!', 'next': '/werkstatt/buero'}, client)
        self.assertIn(response.status_code, (302, 303))
        self.assertTrue(response.headers['Location'].endswith('/werkstatt/buero'))
        self.assert_limited_page(self.get('/werkstatt/buero', client))
        with client.session_transaction() as state:
            self.assertEqual(state.get('assistent_mid'), MID)
            self.assertFalse(state.get('admin'))

    def test_03_calendar_write_integrates_existing_table_without_order_changes(self):
        today = datetime.now(ZoneInfo('Europe/Berlin')).date().isoformat()
        with closing(p.get_db()) as db:
            before = tuple(db.execute('SELECT status,annahme_datum,abholtermin FROM auftraege WHERE id=910001').fetchone())
        response = self.post('/werkstatt/buero/kalender/termine', {'csrf_token': 'synthetic-office-csrf',
            'datum': today, 'uhrzeit': '11:30', 'titel': 'SYNTH Büro-Rückruf', 'notiz': 'SYNTH Terminabsprache'})
        self.assertEqual(response.status_code, 303)
        html = self.assert_limited_page(self.get('/werkstatt/buero/kalender?datum=' + today))
        self.assertIn('SYNTH Büro-Rückruf', html)
        with closing(p.get_db()) as db:
            note = db.execute("SELECT * FROM kalender_notizen WHERE kategorie='buero' AND titel LIKE '%SYNTH Büro-Rückruf'").fetchone()
            self.assertIsNotNone(note)
            self.assertEqual(tuple(db.execute('SELECT status,annahme_datum,abholtermin FROM auftraege WHERE id=910001').fetchone()), before)
        self.assertEqual(self.post('/werkstatt/buero/kalender/termine', {'datum': today, 'titel': 'no csrf'}).status_code, 400)

    def test_04_scan_submit_chef_card_and_approval_callback_use_current_revision(self):
        pdf = BytesIO()
        writer = PdfWriter(); writer.add_blank_page(width=100, height=100); writer.write(pdf)
        today = datetime.now(ZoneInfo('Europe/Berlin')).date().isoformat()
        with patch.object(p, 'extract_document_text_local', return_value='Rechnung\nSYNTH Lieferant\n119,00 EUR'):
            response = self.post('/werkstatt/buero/belege/erfassen', {'csrf_token': 'synthetic-office-csrf',
                'typ': 'rechnung', 'kategorie': 'rechnung', 'lieferant': 'SYNTH Lieferant',
                'belegnummer': 'SYNTH-INTEGRATION', 'belegdatum': today, 'betrag_brutto': '119.00',
                'faelligkeit': today, 'notiz': '', 'original': (BytesIO(pdf.getvalue()), 'synthetic.pdf')})
        self.assertEqual(response.status_code, 303)
        key = response.headers['Location'].rstrip('/').rsplit('/', 1)[-1]
        self.assert_limited_page(self.get('/werkstatt/buero/belege/' + key))
        self.assertEqual(self.get('/admin/buero/belege/' + key, self.admin).status_code, 200)
        self.assertEqual(self.get('/admin/buero/belege', self.admin).status_code, 200)
        with self.staff.session_transaction() as state:
            csrf = state['csrf_token']
        with closing(p.get_db()) as db:
            revision = db.execute('SELECT revision FROM buero_belege WHERE id=?', (key,)).fetchone()[0]
        response = self.post('/werkstatt/buero/belege/' + key + '/einreichen',
                             {'csrf_token': csrf, 'revision': str(revision), 'confirmed': 'ja'})
        self.assertEqual(response.status_code, 303)
        chef = self.get('/admin/cockpit', self.admin)
        self.assertEqual(chef.status_code, 200)
        self.assertIn('Rechnungen und Belege zur Freigabe öffnen', chef.get_data(as_text=True))
        with closing(p.get_db()) as db:
            revision = db.execute('SELECT revision FROM buero_belege WHERE id=?', (key,)).fetchone()[0]
        with patch.object(p.office_lexware, 'send_approved', return_value={'state': 'linked'}) as upload:
            response = self.post('/admin/buero/belege/' + key + '/freigeben',
                {'csrf_token': 'synthetic-admin-csrf', 'revision': str(revision), 'confirmed': 'ja',
                 'bereich': 'geschaeftlich', 'chef_notiz': 'Synthetisch geprüft'}, self.admin)
        self.assertEqual(response.status_code, 303)
        with closing(p.get_db()) as db:
            approved = dict(db.execute('SELECT status,revision,business_confirmed FROM buero_belege WHERE id=?', (key,)).fetchone())
        self.assertEqual(approved['status'], 'freigegeben')
        self.assertEqual(approved['business_confirmed'], 1)
        upload.assert_called_once_with(key, approved['revision'])

    def test_05_forbidden_direct_urls_and_assistant_order_routes_stay_closed(self):
        with p.app.test_request_context():
            from flask import url_for
            forbidden_paths = [url_for('admin_mitarbeiter'), url_for('werkstatt_orders.index'),
                               '/admin/cockpit', '/admin/buero-zugaenge', '/admin/buero/belege']
        for path in forbidden_paths:
            with self.subTest(path=path):
                self.assertEqual(self.get(path).status_code, 403)
        self.assertIn(self.get('/werkstatt/assistent').status_code, (302, 303))

    def test_06_mailbox_is_read_only_even_when_global_features_are_enabled(self):
        with patch.dict(p.app.config, MAILBOX_WRITE_ENABLED=True, MAILBOX_SEND_ENABLED=True,
                        MAILBOX_MOVE_ENABLED=True, MAILBOX_FLAGS_ENABLED=True):
            response = self.get('/admin/mail')
            self.assert_limited_page(response)
            html = response.get_data(as_text=True)
            self.assertIn('const WRITE=false, SEND=false, MOVE=false, FLAGS=false', html)
            with patch.object(mailbox_client.Mailbox, 'connect', side_effect=AssertionError('No provider mutation')) as connect:
                for path in ('/admin/mail/message/1/read', '/admin/mail/message/1/move',
                             '/admin/mail/draft', '/admin/mail/send', '/admin/mail/outbox/test/copy'):
                    with self.subTest(path=path):
                        self.assertEqual(self.post(path, {'csrf_token': 'synthetic-office-csrf'}).status_code, 403)
                connect.assert_not_called()
        with patch.object(mailbox_client.Mailbox, 'listing', return_value={'messages': [], 'folders': [], 'total': 0}) as listing:
            self.assertEqual(self.get('/admin/mail/messages').status_code, 200)
            listing.assert_called_once()

    def test_07_category_removal_and_full_revocation_apply_to_existing_session(self):
        self.grant(['termine'])
        html = self.assert_limited_page(self.get('/werkstatt/buero'))
        self.assertNotIn('href="/werkstatt/buero/belege"', html)
        self.assertEqual(self.get('/werkstatt/buero/kalender').status_code, 200)
        for path in ('/werkstatt/buero/belege', '/werkstatt/buero/planung', '/admin/mail', '/werkstatt/buero/lieferanten'):
            with self.subTest(path=path):
                self.assertEqual(self.get(path).status_code, 403)
        self.grant([], active=False)
        self.assertIn(self.get('/werkstatt/buero').status_code, (302, 303))
        self.assertEqual(self.get('/werkstatt/buero/kalender').status_code, 403)
        self.assertEqual(self.get('/werkstatt/buero/belege').status_code, 403)

    def test_08_access_save_posts_actual_rendered_controls_with_both_csrf_tokens(self):
        page = '/admin/buero-zugaenge'
        form = self.rendered_form(self.get(page, self.admin), mitarbeiter_id=MID)
        categories = ['termine', 'arbeitszeiten', 'belege', 'loehne']
        response = self.submit_rendered(form, page, client=self.admin, button=('action', 'save'),
                                        categories=categories)
        self.assertEqual(response.status_code, 303)
        self.assertIn('Bürofreigabe gespeichert.', self.get(response.headers['Location'], self.admin).get_data(as_text=True))
        self.assertEqual(self.get('/werkstatt/buero/kalender').status_code, 200)
        self.assertEqual(self.get('/werkstatt/buero/loehne').status_code, 200)
        self.assertEqual(self.get('/werkstatt/buero/lieferanten').status_code, 403)
        with closing(p.get_db()) as db:
            import json
            saved = db.execute('SELECT aktiv,kategorien_json FROM buero_zugaenge WHERE mitarbeiter_id=?', (MID,)).fetchone()
            self.assertEqual(saved['aktiv'], 1)
            self.assertEqual(set(json.loads(saved['kategorien_json'])), set(categories))

    def test_09_daily_time_review_posts_the_rendered_fingerprint_and_csrf_without_changing_stamps(self):
        berlin = ZoneInfo('Europe/Berlin')
        day = datetime.now(berlin).date() - timedelta(days=1)
        with closing(p.get_db()) as db:
            for index, hour, action in ((1, 8, 'kommen'), (2, 16, 'gehen')):
                when = datetime.combine(day, datetime.min.time().replace(hour=hour), berlin).astimezone(timezone.utc)
                db.execute('''INSERT INTO mitarbeiter_zeitstempel(id,mitarbeiter_id,aktion,zeit,request_id,revision)
                              VALUES(?,?,?,?,?,?)''', (f'html-time-{index}', MID, action, when.isoformat(), f'html-request-{index}', index))
            db.commit()
            before = [tuple(row) for row in db.execute('SELECT * FROM mitarbeiter_zeitstempel ORDER BY id')]
        page = '/werkstatt/buero/arbeitszeiten?tag=' + day.isoformat()
        form = self.rendered_form(self.get(page), mitarbeiter_id=MID, datum=day.isoformat())
        response = self.submit_rendered(form, page, button=('status', 'geprueft'), notiz='Synthetische Originalzeiten geprüft.')
        self.assertEqual(response.status_code, 303)
        with closing(p.get_db()) as db:
            row = db.execute('SELECT * FROM buero_arbeitszeit_pruefungen WHERE mitarbeiter_id=? AND datum=?', (MID, day.isoformat())).fetchone()
            self.assertEqual(row['status'], 'geprueft')
            self.assertEqual(row['geaendert_von'], f'mitarbeiter:{MID}')
            self.assertEqual(row['quell_fingerprint'], form['data']['fingerprint'])
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM mitarbeiter_zeitstempel ORDER BY id')], before)
        self.assertIn('Geprüft', self.get(response.headers['Location']).get_data(as_text=True))

    def test_10_payroll_html_create_assign_approve_publish_and_office_payment_task(self):
        def pdf(label):
            stream = BytesIO()
            writer = PdfWriter()
            writer.add_blank_page(width=150, height=200)
            writer.add_metadata({'/Title': 'SYNTHETIC ONLY: ' + label})
            writer.write(stream)
            return stream.getvalue()

        summary, single = pdf('monthly summary'), pdf('private payslip owner ' + str(MID))
        page = '/admin/buero/loehne'
        form = self.rendered_form(self.get(page, self.admin), action='create')
        response = self.submit_rendered(form, page, client=self.admin, period='2026-10',
            source_label='SYNTHETIC payroll provider', note='Only synthetic regression data.',
            original=(BytesIO(summary), 'synthetic-summary.pdf'))
        self.assertEqual(response.status_code, 303)
        form = self.rendered_form(self.get(page, self.admin), action='add')
        key = form['data']['key']
        self.assertNotIn('/' + key + '/auswertung', self.get('/werkstatt/buero/loehne').get_data(as_text=True))
        self.assertEqual(self.get('/werkstatt/buero/loehne/' + key + '/auswertung').status_code, 403)
        response = self.submit_rendered(form, page, client=self.admin, confirmed='1',
                                        original=(BytesIO(single), 'synthetic-personal.pdf'))
        self.assertEqual(response.status_code, 303)
        form = self.rendered_form(self.get(page, self.admin), action='assign', key=key)
        item = form['data']['item']
        response = self.submit_rendered(form, page, client=self.admin, confirmed='1', employee_id=str(MID))
        self.assertEqual(response.status_code, 303)
        form = self.rendered_form(self.get(page, self.admin), action='approve', key=key)
        self.assertEqual(self.submit_rendered(form, page, client=self.admin, confirmed='1').status_code, 303)
        form = self.rendered_form(self.get(page, self.admin), action='publish', key=key, item=item)
        self.assertEqual(self.submit_rendered(form, page, client=self.admin, confirmed='1').status_code, 303)
        with closing(p.get_db()) as db:
            published = dict(db.execute('SELECT * FROM buero_lohnzettel WHERE id=?', (item,)).fetchone())
            owner = db.execute('SELECT mitarbeiter_id FROM mitarbeiter_lohnzettel WHERE id=?', (published['portal_lohnzettel_id'],)).fetchone()[0]
        self.assertEqual(published['publikation'], 'bereitgestellt')
        self.assertEqual(owner, MID)
        own_url = '/werkstatt/mein-konto/lohnzettel/' + str(published['portal_lohnzettel_id'])
        self.assertEqual(self.get(own_url).data, single)
        self.assertEqual(self.get('/werkstatt/buero/loehne/' + key + '/auswertung').data, summary)
        response = self.get('/werkstatt/buero/loehne')
        html = self.assert_limited_page(response)
        self.assertNotIn('synthetic-personal.pdf', html)
        payment = self.rendered_form(response, action='payment', key=key)
        self.assertEqual(payment['data']['target'], 'zahlung_in_bearbeitung')
        home = self.assert_limited_page(self.get('/werkstatt/buero'))
        self.assertIn('<span>Freigegebene Löhne</span><strong>1</strong>', home)
        chef_preview = self.get('/werkstatt/buero', self.admin)
        self.assertEqual(chef_preview.status_code, 200)
        self.assertIn('href="/admin/buero/loehne"', chef_preview.get_data(as_text=True))

    def test_11_generic_backup_guard_accepts_identical_export_and_rejects_stale_missing_or_changed_identity(self):
        def snapshot():
            with closing(p.get_db()) as db:
                return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                                   for table in (*OFFICE_BACKUP_TABLES, 'mitarbeiter', 'assistent_rechte')}}
        before = snapshot()
        p.ensure_office_state_for_import(p, export=before)
        changed = deepcopy(before)
        changed['tables']['buero_zugaenge'][0]['version'] += 1
        cases = [changed, {'tables': {}}, deepcopy(before), deepcopy(before)]
        next(row for row in cases[2]['tables']['mitarbeiter'] if row['id'] == MID)['name'] = 'Different synthetic owner'
        next(row for row in cases[3]['tables']['assistent_rechte'] if row['mitarbeiter_id'] == MID)['auth_version'] += 1
        for index, incoming in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ValueError):
                p.ensure_office_state_for_import(p, export=incoming)
            self.assertEqual(snapshot(), before)

    def test_12_ceo_or_unconfigured_mailbox_is_never_exposed_to_office(self):
        with patch.object(mailbox_client.Mailbox, 'connect', side_effect=AssertionError('No mailbox connection')) as connect:
            for config in ({}, {'user': ''}, {'user': 'chef@example.invalid'}, {'user': ' CHEF@EXAMPLE.INVALID '}):
                with self.subTest(config=config), patch.object(p, 'get_werkstatt_imap_config', return_value=config):
                    self.assertEqual(self.get('/admin/mail').status_code, 403)
                    self.assertEqual(self.get('/admin/mail/messages').status_code, 403)
            with patch.dict(p.app.config, OFFICE_CEO_MAIL_ADDRESS=''):
                self.assertEqual(self.get('/admin/mail').status_code, 403)
            connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
