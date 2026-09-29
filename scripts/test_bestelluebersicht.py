"""Synthetic read-only order-folder regressions; no network, files or live DB."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_bestellungen import FakePortal
from mailbox_outbox import _DDL as OUTBOX_DDL
from werkstatt_bestellungen import register_orders
from werkstatt_bestelluebersicht import OrderOverview


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class OverviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = FakePortal(Path(self.temp.name))
        self.manager = register_orders(self.p)
        self.now = datetime(2026, 9, 29, 10, tzinfo=timezone.utc)
        self.reader = OrderOverview(self.p.get_db)
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as state:
            state['admin'] = True
        with self.manager.db() as db:
            db.execute(OUTBOX_DDL)
            db.executescript('''CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,email TEXT);
                CREATE TABLE assistent_aktionen(id TEXT PRIMARY KEY,actor TEXT,auftrag_id INTEGER,art TEXT,
                payload TEXT,fingerprint TEXT,status TEXT,erstellt_am TEXT);
                CREATE TABLE assistent_audit(id INTEGER PRIMARY KEY,actor TEXT,auftrag_id INTEGER,aktion TEXT,details TEXT,zeit TEXT);''')
            db.execute('INSERT INTO mitarbeiter VALUES(1,?,?)', ('Testperson A', 'private-do-not-show@example.invalid'))
            db.execute('INSERT INTO mitarbeiter VALUES(2,?,?)', ('Testperson B', 'private-b@example.invalid'))
            db.execute('INSERT INTO assistent_bestellkontakte(id,name,recipient,verified_at,verified_by) VALUES(?,?,?,?,?)',
                       ('supplier-a', 'Heutiger Lieferantenname', 'orders@example.invalid', '2026-09-20', 'admin'))
            db.commit()

    def order(self, key, *, actor='mitarbeiter:1', request_id=None, batch='', urgent=False, created=None, **changes):
        intent = {'id': request_id or key, 'product_name': 'Grünes Band', 'article_number': 'TEST-50',
                  'product_id': None, 'variant': '50 mm', 'quantity': '2', 'unit': 'Rollen',
                  'supplier_id': 'supplier-a', 'recipient': 'orders@example.invalid', 'urgent': urgent,
                  'unit_price_cents': 1000, 'shipping_cents': 400, 'extra_costs_cents': 100,
                  'expected_total_cents': 2500, 'max_total_cents': 3000, 'price_verified': True,
                  'price_basis': 'gross', 'currency': 'EUR', 'order_requested': True,
                  'price_source': 'Geprüftes synthetisches Angebot A'}
        intent.update(changes)
        snapshot = {'order': intent, 'actor_id': actor, 'supplier_name': 'Historischer Lieferant A',
                    'recipient_verified': True, 'sender_account': 'private-mail-account', 'from_address': 'workshop@example.invalid',
                    'bank_data': 'NEVER_SHOW_BANK_DATA'}
        with self.manager.db() as db:
            db.execute('''INSERT INTO assistent_bestellanforderungen
                (id,actor_id,request_id,request_fingerprint,snapshot_json,due_at,created_at,batch_id) VALUES(?,?,?,?,?,?,?,?)''',
                (key, actor, request_id or key, hashlib.sha256(canonical(intent).encode()).hexdigest(), canonical(snapshot),
                 self.now.timestamp()-86400, created or self.now.timestamp(), batch))
            db.commit()
        return intent

    def batch(self, key, *, state='ready', outbox=None, cap=6000):
        payload = {'max_total_cents': cap, 'orders': []}
        with self.manager.db() as db:
            db.execute('''INSERT INTO assistent_bestellpakete(id,payload_json,fingerprint,due_at,created_at,state,attempts,result_json)
                VALUES(?,?,?,?,?,?,?,?)''', (key, canonical(payload), hashlib.sha256(canonical(payload).encode()).hexdigest(), self.now.timestamp()-86400,
                                           self.now.timestamp(), state, 1, canonical({'message': 'Kontakt wurde geändert.'})))
            if outbox:
                db.execute('''INSERT INTO mailbox_outbox(token,fingerprint,attempt,state,payload_name,message_id,recipients,
                    account,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)''',
                    (key, 'synthetic', 'synthetic', outbox, 'unread-private.eml', '<synthetic@example.invalid>',
                     '[]', 'private-account', self.now.timestamp(), self.now.timestamp()))
            db.commit()

    def action(self, key, *, actor='mitarbeiter:1', state='vorschlag', art='bestellung', when='29.09.2026 11:00'):
        payload = {'lieferant': 'Historischer Lieferant A', 'bezeichnung': 'Band', 'gesamt_cent': 2500,
                   'versand': {'supplier_id': 'supplier-a', 'product_name': 'Band', 'article_number': 'TEST-50',
                               'variant': '50 mm', 'quantity': '2', 'unit': 'Rollen', 'urgent': False,
                               'recipient': 'orders@example.invalid', 'max_total_cents': 3000}}
        with self.manager.db() as db:
            db.execute('INSERT INTO assistent_aktionen VALUES(?,?,?,?,?,?,?,?)',
                       (key, actor, 156, art, json.dumps(payload), 'synthetic:'+key, state, when))
            db.commit()

    def test_admin_only_and_readonly_no_dispatch_or_file_recovery(self):
        self.order('one')
        self.assertEqual(self.p.app.test_client().get('/admin/assistent-bestellungen').status_code, 403)
        employee = self.p.app.test_client()
        with employee.session_transaction() as state:
            state['assistent_mid'] = 1
        self.assertEqual(employee.get('/admin/assistent-bestellungen').status_code, 403)
        original = self.p.get_db
        statements = []
        def readonly():
            db = original()
            db.set_trace_callback(statements.append)
            forbidden = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                         sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE}
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        with patch.object(self.p, 'get_db', side_effect=readonly), patch.object(self.manager, 'tick', side_effect=AssertionError('no dispatch')), \
                patch.object(self.manager.dispatch, 'status', side_effect=AssertionError('no recovery')), \
                patch.object(self.manager.dispatch.outbox, 'status', side_effect=AssertionError('no private files')):
            response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertTrue(statements)
        html = response.get_data(as_text=True)
        self.assertIn('Testperson A', html)
        for private in ('NEVER_SHOW_BANK_DATA', 'private-mail-account', 'private-do-not-show'):
            self.assertNotIn(private, html)

    def test_saved_prices_and_identity_not_current_supplier_or_avatar_name(self):
        self.order('one')
        data = self.reader.page({'bestellung': 'one'}, now=self.now)
        item = data['selected']
        self.assertEqual(item['person'], 'Testperson A')
        self.assertEqual(item['supplier'], 'Historischer Lieferant A')
        self.assertEqual(item['total'], '25,00 €')
        self.assertEqual(item['cap'], '30,00 €')
        self.assertEqual(item['price_source'], 'Geprüftes synthetisches Angebot A')
        self.assertEqual(item['state'], 'queued')
        self.assertTrue(item['overdue'])
        self.order('deleted', actor='mitarbeiter:99')
        self.order('noncanonical', actor='mitarbeiter:01')
        self.assertIn('Name nicht mehr hinterlegt', self.reader.page({'bestellung':'deleted'}, now=self.now)['selected']['person'])
        self.assertEqual(self.reader.page({'bestellung':'noncanonical'}, now=self.now)['selected']['person'], 'Nicht zugeordneter Zugang')

    def test_pagination_covers_more_than_previous_100_without_combining_duplicates(self):
        for index in range(126):
            self.order(f'order-{index:03d}')
        seen = []
        for page in range(1, 7):
            data = self.reader.page({'page': str(page)}, now=self.now)
            self.assertEqual(data['count'], 126)
            self.assertEqual(data['pages'], 6)
            seen.extend(item['id'] for item in data['items'])
        self.assertEqual(len(set(seen)), 126)
        self.assertEqual(len(seen), 126)

    def test_parameterized_filters_literal_wildcards_and_berlin_date(self):
        self.order('early', actor='mitarbeiter:2', urgent=True, product_name='100% Band',
                   created=datetime(2026, 9, 28, 22, 15, tzinfo=timezone.utc).timestamp())
        self.order('other', supplier_id='supplier-other', created=datetime(2026, 9, 28, 20, tzinfo=timezone.utc).timestamp())
        for query in ({'person':'mitarbeiter:2'}, {'q':'%'}, {'supplier':'supplier-a'}, {'urgency':'urgent'},
                      {'from':'2026-09-29', 'to':'2026-09-29'}):
            self.assertEqual([row['id'] for row in self.reader.page(query, now=self.now)['items']], ['early'])
        self.assertEqual(self.reader.page({'person':"' OR 1=1 --"}, now=self.now)['count'], 0)
        self.assertTrue(self.reader.page({'from':'not-a-date'}, now=self.now)['errors'])
        self.assertTrue(self.reader.page({'to':'9999-12-31'}, now=self.now)['errors'])

    def test_batch_mail_receipt_states_and_shared_mail_trace(self):
        self.batch('batch', state='ready', outbox='copy_pending')
        self.order('first', batch='batch')
        self.order('second', batch='batch', actor='mitarbeiter:2')
        detail = self.reader.page({'bestellung':'first'}, now=self.now)['selected']
        self.assertEqual(detail['state'], 'copy_pending')
        self.assertEqual(detail['batch_cap'], '60,00 €')
        self.assertEqual(detail['sibling_count'], 2)
        self.assertEqual(self.reader.page({'batch':'batch'}, now=self.now)['count'], 2)
        self.assertEqual({item['person'] for item in detail['siblings']}, {'Testperson A', 'Testperson B'})
        self.assertEqual(detail['message_id'], '<synthetic@example.invalid>')
        for status in ('sent', 'uncertain', 'partial', 'sending', 'not_sent'):
            self.batch(status, state='ready', outbox=status)
            self.order('order-'+status, batch=status)
            data = self.reader.page({'state':status}, now=self.now)
            self.assertEqual(data['count'], 1)
            self.assertEqual(data['items'][0]['state'], status)
        self.batch('blocked', state='blocked', outbox='not_sent')
        self.order('order-blocked', batch='blocked')
        self.assertEqual(self.reader.page({'state':'blocked'}, now=self.now)['items'][0]['state'], 'blocked')
        self.order('missing-batch', batch='deleted-package')
        self.assertEqual(self.reader.page({'bestellung':'missing-batch'}, now=self.now)['selected']['state'], 'uncertain')

    def test_unsubmitted_actions_are_not_orders_or_duplicates(self):
        self.action('draft')
        self.action('approved', state='intern_freigegeben')
        self.action('legacy', art='einkauf')
        self.action('queued', state='intern_freigegeben')
        self.order('accepted', request_id='avatar:queued')
        self.action('same-request-other-actor', actor='mitarbeiter:2')
        self.order('other-request', request_id='avatar:same-request-other-actor')
        data = self.reader.page({}, now=self.now)
        self.assertEqual(data['count'], 2)
        self.assertEqual({row['id'] for row in data['drafts']}, {'draft', 'approved', 'same-request-other-actor'})
        draft = self.reader.page({'vorschlag':'approved'}, now=self.now)['selected']
        self.assertEqual(draft['state'], 'approved_pending')
        self.assertEqual(draft['due'], 'Noch nicht eingeplant')
        self.assertFalse(draft['verified'])
        self.assertEqual(self.reader.page({'state':'draft'}, now=self.now)['count'], 0)

    def test_source_audit_scoped_and_unsafe_or_unknown_amounts_not_invented(self):
        self.action('action', state='intern_freigegeben')
        self.order('accepted', request_id='avatar:action')
        with self.manager.db() as db:
            for actor, event in (('mitarbeiter:1', 'bestellung_bestaetigt'), ('mitarbeiter:2', 'bestellung_blockiert')):
                db.execute('INSERT INTO assistent_audit(actor,aktion,details,zeit) VALUES(?,?,?,?)',
                           (actor, event, 'action', '29.09.2026 11:00'))
            db.commit()
        detail = self.reader.page({'bestellung':'accepted'}, now=self.now)['selected']
        self.assertEqual(len(detail['events']), 1)
        self.assertEqual(detail['events'][0]['time'], '29.09.2026 11:00')
        self.assertEqual(detail['order_id'], 156)
        self.order('unknown-price', expected_total_cents=None)
        self.assertEqual(self.reader.page({'bestellung':'unknown-price'}, now=self.now)['selected']['total'], 'nicht belegt')
        with self.manager.db() as db:
            db.execute('UPDATE assistent_bestellanforderungen SET request_fingerprint=? WHERE id=?', ('changed', 'accepted'))
            db.commit()
        detail = self.reader.page({'bestellung':'accepted'}, now=self.now)['selected']
        self.assertEqual(detail['total'], 'nicht belegt')
        self.assertTrue(detail['warnings'])

    def test_real_portal_german_dates_sort_and_filter_with_iso_history(self):
        self.action('january', when='31.01.2026 15:20')
        self.action('february', when='01.02.2026 10:30')
        self.action('march-iso', when='2026-03-01T12:00:00')
        rows = self.reader.page({}, now=self.now)['drafts']
        self.assertEqual([row['id'] for row in rows], ['march-iso', 'february', 'january'])
        self.assertEqual(rows[1]['created'], '01.02.2026 10:30')
        selected = self.reader.page({'from':'2026-02-01','to':'2026-02-28'}, now=self.now)
        self.assertEqual([row['id'] for row in selected['drafts']], ['february'])


if __name__ == '__main__':
    unittest.main()
