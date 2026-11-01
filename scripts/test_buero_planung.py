"""Office planning with synthetic SQLite only; never imports production app."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask, session
from jinja2 import ChoiceLoader, DictLoader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_buero_planung import register_office_planning, PlanningConflict, _day


class OfficePlanningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'synthetic.sqlite'
        root = Path(__file__).resolve().parents[1]
        app = Flask(__name__, template_folder=str(root / 'templates'), static_folder=str(root / 'static'))
        app.config.update(TESTING=True, SECRET_KEY='synthetic-test-only')
        app.jinja_loader = ChoiceLoader([DictLoader({'buero_nav.html': '<nav>Büro</nav>'}), app.jinja_loader])

        def connect():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            return db

        def identity():
            return {'mitarbeiter_id': 7, 'name': 'Test Büro', 'actor': 'mitarbeiter:7'} if session.get('office') else None

        self.p = SimpleNamespace(app=app, get_db=connect, now_str=lambda: '2026-11-01 10:00:00',
                                 office_access=SimpleNamespace(identity=identity),
                                 schedule_change_backup=Mock(), KALENDER_KATEGORIEN={})
        with closing(connect()) as db:
            db.executescript('''CREATE TABLE autohaeuser(id INTEGER PRIMARY KEY,name TEXT);
                INSERT INTO autohaeuser VALUES(1,'Testpartner');
                CREATE TABLE auftraege(id INTEGER PRIMARY KEY,autohaus_id INTEGER,fahrzeug TEXT,
                kennzeichen TEXT,status INTEGER,archiviert INTEGER DEFAULT 0,transport_art TEXT,
                annahme_datum TEXT,annahme_uhrzeit TEXT,abholtermin TEXT,abhol_uhrzeit TEXT,
                fertig_datum TEXT,fertig_uhrzeit TEXT,abhol_adresse TEXT,fahrzeug_abholbereit INTEGER,
                werkstatt_angebot_preis TEXT,beschreibung TEXT);
                CREATE TABLE kalender_notizen(id INTEGER PRIMARY KEY AUTOINCREMENT,datum TEXT,titel TEXT,
                notiz TEXT,kategorie TEXT,wiederholung TEXT,erstellt_am TEXT);
                INSERT INTO kalender_notizen(datum,titel,notiz,kategorie,wiederholung,erstellt_am)
                VALUES('01.11.2026','VERTRAULICHE GEHALTSLISTE','IBAN PRIVAT','privat','einmalig','alt');''')
            db.commit()
        self.service = register_office_planning(self.p)
        self.client = app.test_client()
        with self.client.session_transaction() as state:
            state.update(office=True, csrf_token='test-csrf')

    def order(self, oid, **values):
        row = dict(id=oid, autohaus_id=1, fahrzeug='Testfahrzeug', kennzeichen=f'TEST-{oid}', status=2,
                   archiviert=0, transport_art='hol_und_bring', annahme_datum='01.11.2026',
                   annahme_uhrzeit='09:00', abholtermin='01.11.2026', abhol_uhrzeit='17:00',
                   fertig_datum='', fertig_uhrzeit='', abhol_adresse='Testweg 1', fahrzeug_abholbereit=0,
                   werkstatt_angebot_preis='FINANZGEHEIMNIS', beschreibung='GEHALT BANKING ORIGINAL')
        row.update(values)
        with closing(self.p.get_db()) as db:
            db.execute('INSERT INTO auftraege (' + ','.join(row) + ') VALUES (' + ','.join('?' for _ in row) + ')', tuple(row.values()))
            db.commit()

    def view(self):
        with self.p.app.test_request_context():
            session['office'] = True
            return self.service.today('2026-11-01')

    def test_directions_times_minimal_fields_and_no_admin_links(self):
        self.order(1)
        self.order(2, transport_art='standard', annahme_uhrzeit='10:00', abhol_uhrzeit='18:00')
        data = self.view()
        self.assertEqual([len(group['items']) for group in data['gruppen'][:4]], [1, 1, 1, 1])
        self.assertEqual(data['gruppen'][0]['items'][0]['uhrzeit'], '09:00')
        self.assertEqual(data['gruppen'][1]['items'][0]['uhrzeit'], '17:00')
        self.assertEqual(data['gruppen'][0]['items'][0]['adresse'], 'Testweg 1')
        self.assertEqual(data['gruppen'][1]['items'][0]['adresse'], '')
        html = self.client.get('/werkstatt/buero/planung?datum=2026-11-01').get_data(as_text=True)
        for private in ('FINANZGEHEIMNIS', 'GEHALT BANKING', 'IBAN PRIVAT', 'VERTRAULICHE GEHALTSLISTE', '/admin/'):
            self.assertNotIn(private, html)
        self.assertIn('keine Abhol- oder Rückgabefreigabe', html)

    def test_missing_dates_times_address_unknown_transport_and_archives(self):
        self.order(1, annahme_uhrzeit='25:89', abhol_adresse='', abholtermin='')
        self.order(2, transport_art=None)
        self.order(3, archiviert=1)
        self.order(4, status=5)
        self.order(5, annahme_datum='')
        data = self.view()
        groups = {group['key']: group['items'] for group in data['gruppen']}
        self.assertIsNone(groups['abholung_durch_werkstatt_heute'][0]['uhrzeit'])
        self.assertTrue(any('Abholadresse fehlt' in hint for hint in groups['abholung_durch_werkstatt_heute'][0]['hinweise']))
        self.assertEqual(len(groups['transport_ungeklaert']), 2)
        self.assertTrue(any('Rückbringtermin fehlt' in item['hinweis'] for item in data['offene_hinweise']))
        self.assertTrue(any('Abholtermin fehlt' in item['hinweis'] for item in data['offene_hinweise']))
        self.assertFalse(any(item['auftrag_id'] in (3, 4) for group in groups.values() for item in group))

    def test_authorization_csrf_and_revocation_checked_for_every_route(self):
        guest = self.p.app.test_client()
        for path in ('planung', 'kalender'):
            response = guest.get('/werkstatt/buero/' + path)
            self.assertEqual(response.status_code, 403)
            self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine', data={'datum': '2026-11-01', 'titel': 'Test'}).status_code, 400)
        with self.client.session_transaction() as state:
            state['office'] = False
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine', data={'csrf_token': 'test-csrf'}).status_code, 403)
        with self.p.app.test_request_context():
            with self.assertRaises(PermissionError):
                self.service.today()
        admin = self.p.app.test_client()
        with admin.session_transaction() as state:
            state['admin'] = True
        self.assertEqual(admin.get('/werkstatt/buero/planung').status_code, 200)

    def test_notes_shared_table_allowed_category_only_stale_edit_and_unchanged_orders(self):
        self.order(1)
        new = {'csrf_token': 'test-csrf', 'datum': '2026-11-01', 'uhrzeit': '11:15', 'titel': 'Rückruf', 'notiz': '<script>unsafe</script>'}
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine', data=new).status_code, 303)
        note = self.view()['termine'][0]
        self.assertEqual(note['id'], 2)
        self.assertEqual((note['uhrzeit'], note['titel']), ('11:15', 'Rückruf'))
        with closing(self.p.get_db()) as db:
            self.assertEqual(db.execute('SELECT kategorie FROM kalender_notizen WHERE id=2').fetchone()[0], 'buero')
            self.assertEqual(db.execute('SELECT titel FROM kalender_notizen WHERE id=2').fetchone()[0], '11:15 · Rückruf')
        edit = dict(new, titel='Rückruf abgestimmt', version=note['version'])
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine/2', data=edit).status_code, 303)
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine/2', data=edit).status_code, 409)
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine/1', data=edit).status_code, 404)
        self.assertEqual(self.client.post('/werkstatt/buero/kalender/termine/2', data=dict(edit, status='5')).status_code, 400)
        html = self.client.get('/werkstatt/buero/kalender?datum=2026-11-01').get_data(as_text=True)
        self.assertIn('&lt;script&gt;unsafe&lt;/script&gt;', html)
        self.assertNotIn('<script>unsafe</script>', html)
        with closing(self.p.get_db()) as db:
            row = db.execute('SELECT status,werkstatt_angebot_preis FROM auftraege WHERE id=1').fetchone()
            self.assertEqual(tuple(row), (2, 'FINANZGEHEIMNIS'))
        self.assertEqual(self.p.schedule_change_backup.call_count, 2)

    def test_date_validation_week_boundary_and_berlin(self):
        self.assertEqual(_day(datetime(2026, 10, 31, 23, 30, tzinfo=timezone.utc)), date(2026, 11, 1))
        self.assertEqual(self.client.get('/werkstatt/buero/kalender?datum=not-a-day').status_code, 400)
        with self.p.app.test_request_context():
            session['office'] = True
            week = self.service.calendar('2026-12-29')
            self.assertEqual(week['tage'][-1]['datum'], '2027-01-04')
        response = self.client.post('/werkstatt/buero/kalender/termine', data={
            'csrf_token': 'test-csrf', 'datum': '2026-02-30', 'titel': 'Test'})
        self.assertEqual(response.status_code, 400)
        response = self.client.post('/werkstatt/buero/kalender/termine', data={
            'csrf_token': 'test-csrf', 'datum': '2026-11-01', 'uhrzeit': '25:15', 'titel': 'Test'})
        self.assertEqual(response.status_code, 400)

    def test_competing_edits_only_one_original_snapshot_can_win(self):
        with self.p.app.test_request_context():
            session['office'] = True
            note_id = self.service.save_note({'datum': '2026-11-01', 'titel': 'Abstimmung'})
        note = self.view()['termine'][0]
        barrier = threading.Barrier(2, timeout=5)
        version = self.service._version

        def same_snapshot(row):
            fingerprint = version(row)
            barrier.wait()
            return fingerprint

        def edit(title):
            with self.p.app.test_request_context():
                session['office'] = True
                try:
                    self.service.save_note({'datum': '2026-11-01', 'titel': title,
                                            'version': note['version']}, note_id)
                    return 'saved'
                except PlanningConflict:
                    return 'conflict'

        with patch.object(self.service, '_version', side_effect=same_snapshot):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(edit, ['Erste Änderung', 'Zweite Änderung']))
        self.assertCountEqual(results, ['saved', 'conflict'])


if __name__ == '__main__':
    unittest.main()
