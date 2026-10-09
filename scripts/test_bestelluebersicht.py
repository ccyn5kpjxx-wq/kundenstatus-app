"""Synthetic read-only order-folder regressions; no network, files or live DB."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_bestellungen import FakePortal
from mailbox_outbox import _DDL as OUTBOX_DDL
from werkstatt_bestellungen import register_orders
from werkstatt_bestelluebersicht import OrderOverview
from werkstatt_materialverwaltung import register_material_admin


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class OverviewTests(unittest.TestCase):
    def test_historical_lookup_uses_frozen_identity_once_without_writing_price_approval(self):
        self.order('one')
        self.order('two')
        self.order('unknown',article_number='')
        lookup=Mock(return_value={'status':'ok','amount':'47.74','currency':'EUR','tax_basis':'net','unit':'Rollen',
            'packaging':'6 Rollen','date':'2026-08-14','source':{'beleg':'Synthetische Rechnung TC-123','seite':2,'position':5},
            'historical':True,'verified':False,'dispatchable':False,'warnings':[]})
        snapshot={'items':[],'truncated':False}
        read=Mock(return_value=snapshot)
        self.p.cockpit_data=SimpleNamespace(catalog=SimpleNamespace(historical_price=lookup,knowledge_rows=read))
        original=self.p.get_db
        with self.manager.db() as db:
            before=[dict(row) for row in db.execute('SELECT * FROM assistent_bestellanforderungen ORDER BY id').fetchall()]
        def readonly():
            db=original()
            forbidden={sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE,sqlite3.SQLITE_CREATE_TABLE,sqlite3.SQLITE_DROP_TABLE}
            db.set_authorizer(lambda action,*args:sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        with patch.object(self.p,'get_db',side_effect=readonly),patch.object(self.manager,'tick',side_effect=AssertionError('no dispatch')):
            response=self.client.get('/admin/assistent-bestellungen')
        self.assertEqual(response.status_code,200)
        read.assert_called_once_with(limit=5000)
        lookup.assert_called_once_with('Historischer Lieferant A','TEST-50','Rollen',packaging='',snapshot=snapshot)
        self.assertIn('Synthetische Rechnung TC-123',response.get_data(as_text=True))
        with self.manager.db() as db:
            after=[dict(row) for row in db.execute('SELECT * FROM assistent_bestellanforderungen ORDER BY id').fetchall()]
        self.assertEqual(before,after)

    def test_main_overview_exposes_edit_form_checkboxes_and_restore_only_for_open_material(self):
        self.material(state='review')
        fields={'quantity':{'value':'3'},'unit':{'value':'Stück'}}
        current={'id':1,'revision':3,'state':'review','dispatch_id':'','fields':fields,'intake_id':None,'analysis':{}}
        self.p.material_dialog=SimpleNamespace(status=lambda key:current)
        register_material_admin(self.p)
        response=self.client.get('/admin/assistent-bestellungen?bestellung=material:1')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        for value in ('Anforderung bearbeiten','name="selection" value="1:1"','name="quantity" value="3"','name="unit" value="Stück"','Artikelnummer (optional)'):
            self.assertIn(value,html)
        current.update(state='cancelled',fields={'admin_removal':{'value':{'reason':'Doppelt'}}})
        with self.manager.db() as db:
            db.execute('UPDATE einkauf_material_dialoge SET state=?,fields_json=? WHERE id=1',('cancelled',canonical(current['fields'])));db.commit()
        html=self.client.get('/admin/assistent-bestellungen?bestellung=material:1').get_data(as_text=True)
        self.assertIn('Anforderung wiederherstellen',html)
        self.assertNotIn('<h3>Anforderung bearbeiten</h3>',html)

    def test_catalog_failure_is_visible_in_list_and_selected_detail_without_zero_or_old_price(self):
        self.order('one')
        self.p.cockpit_data = SimpleNamespace(catalog=SimpleNamespace(
            historical_price=Mock(), knowledge_rows=Mock(side_effect=RuntimeError('PRIVATE_DATABASE_DETAIL'))))
        for query in ('', '?bestellung=one'):
            with self.subTest(query=query), self.assertLogs('werkstatt_bestellungen', level='WARNING'):
                response = self.client.get('/admin/assistent-bestellungen' + query)
            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertIn('Historischer Rechnungspreis derzeit nicht verfügbar.', html)
            self.assertNotIn('PRIVATE_DATABASE_DETAIL', html)
            self.assertNotRegex(html, r'(?<![0-9])0,00\s*€')
        self.p.cockpit_data.catalog.historical_price.assert_not_called()

    def test_next_get_rechecks_real_catalog_source_quarantine_without_cached_old_price(self):
        import test_artikel_import as catalog_fixture
        from werkstatt_artikel_import import InvoiceCatalog
        self.order('one')
        source_portal = catalog_fixture.FakePortal(str(Path(self.temp.name) / 'catalog.sqlite'))
        source_portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = json.dumps(['Historischer Lieferant A'])
        catalog = InvoiceCatalog(source_portal)
        catalog_fixture.prepare_catalog(catalog, {'einkaufsbelege': [
            {'id': 1, 'lieferant': 'Historischer Lieferant A', 'original_name': 'SYNTHETIC-INVOICE.pdf'}],
            'lieferantenrechnungen': []})
        candidate = catalog_fixture.candidate(artikelnummer='TEST-50', ve='Rollen', gebinde='6 Rollen', preis='47.74',
            source={'page': 2, 'position': 5, 'date': '2026-08-14'},
            price_evidence={'value': '47.74', 'basis': 'gebindepreis_netto_abgeleitet', 'reconciled': True,
                            'currency': 'EUR', 'tax_basis': 'net', 'tax_rate': '19'})
        with patch('werkstatt_artikel_import.read_source', return_value=catalog_fixture.extracted(candidate)):
            catalog.process_next()
        self.p.cockpit_data = SimpleNamespace(catalog=catalog)
        with patch.object(catalog, 'knowledge_rows', wraps=catalog.knowledge_rows) as read:
            first = self.client.get('/admin/assistent-bestellungen')
            self.assertEqual(first.status_code, 200)
            self.assertIn('47,74 € netto / Rolle', first.get_data(as_text=True))
            db = source_portal.get_db()
            try:
                db.execute("UPDATE einkauf_belege SET beleg_typ='quarantaene' WHERE id=1")
                db.commit()
            finally:
                db.close()
            second = self.client.get('/admin/assistent-bestellungen')
            self.assertEqual(second.status_code, 200)
            self.assertNotIn('47,74', second.get_data(as_text=True))
            self.assertNotIn('SYNTHETIC-INVOICE.pdf', second.get_data(as_text=True))
            self.assertEqual(read.call_count, 2)

    def test_selected_comparison_12_candidates_share_one_catalog_read_with_history(self):
        import test_artikel_import as catalog_fixture
        from werkstatt_artikel_import import InvoiceCatalog
        from werkstatt_bestellvergleich import OrderPriceComparison
        from werkstatt_bestellvergleich_ui import comparison_context
        self.order('one')
        self.material(state='cancelled')  # Real comparison expects the material-link table.
        source_portal = catalog_fixture.FakePortal(str(Path(self.temp.name) / 'catalog.sqlite'))
        source_portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = json.dumps(['Historischer Lieferant A'])
        catalog = InvoiceCatalog(source_portal)
        catalog_fixture.prepare_catalog(catalog, {'einkaufsbelege': [
            {'id': 1, 'lieferant': 'Historischer Lieferant A', 'original_name': 'SYNTHETIC-INVOICE.pdf'}],
            'lieferantenrechnungen': []})
        candidates = [catalog_fixture.candidate(artikelnummer='TEST-50', ve='Rollen', gebinde='6 Rollen', preis='47.74',
            source={'page': 2, 'position': index + 1, 'date': '2026-08-14'},
            price_evidence={'value': '47.74', 'basis': 'gebindepreis_netto_abgeleitet', 'reconciled': True,
                            'currency': 'EUR', 'tax_basis': 'net', 'tax_rate': '19'}) for index in range(12)]
        with patch('werkstatt_artikel_import.read_source', return_value=catalog_fixture.extracted(*candidates)):
            catalog.process_next()
        self.p.cockpit_data = SimpleNamespace(catalog=catalog)
        self.p.order_price_comparison = OrderPriceComparison(self.p)
        contexts = []
        def capture_context(*args):
            result = comparison_context(*args)
            contexts.append(result)
            return result
        # Exercise the actual GET, real candidate resolution and permission checks.
        with patch.object(catalog, '_search', wraps=catalog._search) as read, \
                patch('werkstatt_bestellvergleich_ui.comparison_context', side_effect=capture_context):
            response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(read.call_count, 1)
            self.assertEqual(len(contexts[-1]['comparison_candidates']), 12)
            html = response.get_data(as_text=True)
            self.assertIn('SYNTHETIC-INVOICE.pdf', html)
            self.assertEqual(html.count('name="proposal_id"'), 12)
            source_portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = '[]'
            response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(read.call_count, 2)
            self.assertNotIn('SYNTHETIC-INVOICE.pdf', response.get_data(as_text=True))
            self.assertNotIn('name="proposal_id"', response.get_data(as_text=True))

    def comparison_fixture(self, count=2):
        """Real catalog and comparison services with synthetic invoice evidence."""
        import test_artikel_import as catalog_fixture
        from werkstatt_artikel_import import InvoiceCatalog
        from werkstatt_bestellvergleich import OrderPriceComparison
        self.order('one')
        self.material(state='cancelled')
        source_portal = catalog_fixture.FakePortal(str(Path(self.temp.name) / 'catalog.sqlite'))
        source_portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = json.dumps(['Historischer Lieferant A'])
        catalog = InvoiceCatalog(source_portal)
        catalog_fixture.prepare_catalog(catalog, {'einkaufsbelege': [
            {'id': 1, 'lieferant': 'Historischer Lieferant A', 'original_name': 'SYNTHETIC-INVOICE.pdf'}],
            'lieferantenrechnungen': []})
        candidates = [catalog_fixture.candidate(artikelnummer='TEST-50', ve='Rollen', gebinde='6 Rollen', preis='47.74',
            source={'page': 2, 'position': index + 1, 'date': '2026-08-14'},
            price_evidence={'value': '47.74', 'basis': 'gebindepreis_netto_abgeleitet', 'reconciled': True,
                            'currency': 'EUR', 'tax_basis': 'net', 'tax_rate': '19'}) for index in range(count)]
        with patch('werkstatt_artikel_import.read_source', return_value=catalog_fixture.extracted(*candidates)):
            catalog.process_next()
        self.p.cockpit_data = SimpleNamespace(catalog=catalog)
        service = self.p.order_price_comparison = OrderPriceComparison(self.p)
        return source_portal, catalog, service

    @staticmethod
    def comparison_rows(get_db):
        db = get_db()
        try:
            tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return {name: [tuple(row) for row in db.execute('SELECT * FROM "' + name.replace('"', '""') + '"')]
                    for name in tables}
        finally:
            db.close()

    @staticmethod
    def readonly_db(get_db):
        def connect():
            db = get_db()
            forbidden = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                         sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE}
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        return connect

    def test_selected_real_comparison_catalog_failure_is_visible_readonly_and_recovers(self):
        from werkstatt_bestellvergleich_ui import comparison_context
        source_portal, catalog, service = self.comparison_fixture()
        before = self.comparison_rows(self.p.get_db), self.comparison_rows(source_portal.get_db)
        contexts = []
        def capture(*args):
            result = comparison_context(*args)
            contexts.append(result)
            return result
        for error_type in (RuntimeError, sqlite3.OperationalError):
            with self.subTest(error=error_type.__name__), \
                    patch.object(catalog, 'knowledge_rows', side_effect=error_type('PRIVATE_DATABASE_DETAIL')), \
                    patch.object(self.p, 'get_db', side_effect=self.readonly_db(self.p.get_db)), \
                    patch.object(source_portal, 'get_db', side_effect=self.readonly_db(source_portal.get_db)), \
                    patch.object(self.manager, 'tick', side_effect=AssertionError('no dispatch')), \
                    patch('werkstatt_bestellvergleich_ui.comparison_context', side_effect=capture), \
                    self.assertLogs(level='WARNING') as logs:
                response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertIn('Rechnungskandidaten derzeit nicht verfügbar.', html)
            self.assertIn('Preisvergleich zur Bestellung', html)
            self.assertIn('Grünes Band', html)
            self.assertNotIn('PRIVATE_DATABASE_DETAIL', html + '\n'.join(logs.output))
            self.assertNotIn('name="proposal_id"', html)
            self.assertNotRegex(html, r'(?<![0-9])0,00\s*€')
            self.assertEqual(contexts[-1]['order_comparison']['order']['key'], 'order:one')
            self.assertEqual(contexts[-1]['comparison_candidates'], [])
            self.assertTrue(contexts[-1]['comparison_candidates_unavailable'])
            self.assertEqual(before, (self.comparison_rows(self.p.get_db), self.comparison_rows(source_portal.get_db)))
        # Failed reads do not leave an unavailable flag or stale data on the next GET.
        recovered = self.client.get('/admin/assistent-bestellungen?bestellung=one')
        self.assertEqual(recovered.status_code, 200)
        self.assertNotIn('Rechnungskandidaten derzeit nicht verfügbar.', recovered.get_data(as_text=True))
        self.assertEqual(recovered.get_data(as_text=True).count('name="proposal_id"'), 2)

    def test_selected_candidate_failure_discards_successful_partial_candidates(self):
        from werkstatt_bestellvergleich_ui import comparison_context
        source_portal, catalog, service = self.comparison_fixture()
        before = self.comparison_rows(self.p.get_db), self.comparison_rows(source_portal.get_db)
        contexts = []
        successful = []
        original = service._catalog_estimate
        def capture(*args):
            result = comparison_context(*args)
            contexts.append(result)
            return result
        for error_type in (RuntimeError, sqlite3.OperationalError):
            successful.clear()
            def estimate(order, payload):
                if successful:
                    raise error_type('PRIVATE_CANDIDATE_DETAIL')
                price = original(order, payload)
                successful.append(price)
                return price
            with self.subTest(error=error_type.__name__), \
                    patch.object(service, '_catalog_estimate', side_effect=estimate) as lookup, \
                    patch('werkstatt_bestellvergleich_ui.comparison_context', side_effect=capture), \
                    self.assertLogs('werkstatt_bestellvergleich_ui', level='WARNING') as logs:
                response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(lookup.call_count, 2)
            self.assertEqual(len(successful), 1)  # The first real candidate was valid before the second failed.
            html = response.get_data(as_text=True)
            self.assertIn('Rechnungskandidaten derzeit nicht verfügbar.', html)
            self.assertNotIn('PRIVATE_CANDIDATE_DETAIL', html + '\n'.join(logs.output))
            self.assertNotIn('name="proposal_id"', html)
            self.assertEqual(contexts[-1]['comparison_candidates'], [])
            self.assertTrue(contexts[-1]['comparison_candidates_unavailable'])
            self.assertEqual(contexts[-1]['order_comparison']['order']['key'], 'order:one')
            self.assertEqual(before, (self.comparison_rows(self.p.get_db), self.comparison_rows(source_portal.get_db)))

    def test_candidate_domain_rejections_keep_existing_behavior_and_other_valid_candidate(self):
        from werkstatt_bestellvergleich_ui import comparison_context
        source_portal, catalog, service = self.comparison_fixture()
        original = service._catalog_estimate
        for error_type in (PermissionError, ValueError, LookupError):
            calls = []
            def estimate(order, payload):
                calls.append(payload)
                if len(calls) == 1:
                    raise error_type('PRIVATE_REJECTION_DETAIL')
                return original(order, payload)
            with self.subTest(error=error_type.__name__), patch.object(service, '_catalog_estimate', side_effect=estimate):
                response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertEqual(len(calls), 2)
            self.assertEqual(html.count('name="proposal_id"'), 1)
            self.assertNotIn('Rechnungskandidaten derzeit nicht verfügbar.', html)
            self.assertNotIn('PRIVATE_REJECTION_DETAIL', html)
            with self.subTest(read_rejection=error_type.__name__), \
                    patch.object(catalog, 'knowledge_rows', side_effect=error_type('PRIVATE_REJECTION_DETAIL')), \
                    self.assertLogs('werkstatt_bestellungen', level='WARNING'):
                response = self.client.get('/admin/assistent-bestellungen?bestellung=one')
            self.assertEqual(response.status_code, 200)
            self.assertNotIn('Rechnungskandidaten derzeit nicht verfügbar.', response.get_data(as_text=True))
            self.assertNotIn('name="proposal_id"', response.get_data(as_text=True))

    def test_selected_detail_and_delivery_errors_are_not_masked_as_catalog_unavailability(self):
        source_portal, catalog, service = self.comparison_fixture()
        for target, attribute in ((service, 'detail'), ('werkstatt_liefereingang', 'delivery_context')):
            context = (patch.object(target, attribute, side_effect=RuntimeError('REQUIRED_DATA_ERROR'))
                       if not isinstance(target, str) else patch(target + '.' + attribute, side_effect=RuntimeError('REQUIRED_DATA_ERROR')))
            with self.subTest(attribute=attribute), context, self.assertRaisesRegex(RuntimeError, 'REQUIRED_DATA_ERROR'):
                self.client.get('/admin/assistent-bestellungen?bestellung=one')

    def test_dated_archives_keep_all_126_positions_and_new_open_work_separate(self):
        self.batch('monday',state='sent')
        with self.manager.db() as db:
            db.execute('UPDATE assistent_bestellpakete SET result_json=? WHERE id=?',(canonical({'sent_at':'2026-10-12T12:02:00+00:00'}),'monday'));db.commit()
        for index in range(126):self.order(f'archive-{index:03d}',batch='monday',actor='mitarbeiter:'+str(1+index%2))
        self.order('new-open')
        data=self.reader.page({},now=self.now)
        self.assertEqual([item['id'] for item in data['open']['items']],['new-open'])
        group=data['archive_groups'][0]
        self.assertEqual(group['label'],'Bestellung am 12.10.2026')
        self.assertEqual((group['count'],group['pages']),(126,6))
        seen=[]
        for page in range(1,7):
            block=self.reader.page({'archive_day':'sent:2026-10-12','archive_page':str(page)},now=self.now)['archive_groups'][0]
            self.assertTrue(block['expanded'])
            seen.extend(item['id'] for item in block['items'])
        self.assertEqual(len(set(seen)),126)
        self.assertEqual(len(seen),126)
        self.assertEqual({item['person'] for item in group['items']},{'Testperson A','Testperson B'})

    def test_legacy_copy_update_does_not_invent_a_send_date_or_archive_uncertain(self):
        self.batch('legacy',state='sent',outbox='sent')
        self.order('old',batch='legacy')
        self.batch('unknown',state='uncertain')
        self.order('uncertain',batch='unknown')
        with self.manager.db() as db:
            db.execute('UPDATE mailbox_outbox SET updated_at=?',(datetime(2026,10,15,tzinfo=timezone.utc).timestamp(),));db.commit()
        data=self.reader.page({},now=self.now)
        self.assertEqual(data['archive_groups'][0]['label'],'Bestelllauf am 28.09.2026')
        self.assertTrue(data['archive_groups'][0]['legacy'])
        self.assertEqual([item['id'] for item in data['open']['items']],['uncertain'])

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

    def material(self,key=1,*,state='external_sent',employee=1,created=None,dispatch='',broken=False,**changes):
        fields={name:{'value':value} for name,value in [('quantity','1'),('unit','Stück'),('urgent',True)]}
        snapshot=dict(kind='manual_external_order',draft_id=key,reservation_id=f'{key:032x}',employee_id=employee,
            quantity='1',unit='Stück',urgent=True,supplier_id='supplier-a',supplier_name='Historischer Materiallieferant',
            recipient='orders@example.invalid',product_name='Crystal Silver',article_number='TEST-4000',variant='0,5 Liter',
            max_total_cents=25000,reserved_by='admin',reserved_at='2026-09-29T08:00:00+00:00',
            sent_at='2026-09-29T08:05:00+00:00',recorded_by='admin',send_evidence='Synthetischer Gesendet-Nachweis',
            authorization_note='PRIVATE_AUTHORIZATION',body='PRIVATE_MAIL_BODY',bank_data='NEVER_SHOW_BANK_DATA')
        snapshot.update(changes)
        review=dict(supplier_id='supplier-a',product_name='Crystal Silver',article_number='TEST-4000',variant='0,5 Liter',recipient='orders@example.invalid')
        with self.manager.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS einkauf_material_nachrichten(id INTEGER PRIMARY KEY,employee_id INTEGER,caption TEXT,phone_number_id TEXT);
                CREATE TABLE IF NOT EXISTS einkauf_material_dialoge(id INTEGER PRIMARY KEY,message_id INTEGER,state TEXT,
                fields_json TEXT,review_json TEXT,analysis_json TEXT,snapshot_json TEXT,snapshot_hash TEXT,
                dispatch_id TEXT,created_at DOUBLE PRECISION);''')
            db.execute('INSERT INTO einkauf_material_nachrichten VALUES(?,?,?,?)',(key,employee,'PRIVATE_SOURCE_MESSAGE','synthetic-whatsapp'))
            db.execute('INSERT INTO einkauf_material_dialoge VALUES(?,?,?,?,?,?,?,?,?,?)',
                (key,key,state,canonical(fields),canonical(review),canonical({'merkmale':{'produkt':'Crystal Silver'}}),
                 canonical(snapshot),'broken' if broken else hashlib.sha256(canonical(snapshot).encode()).hexdigest(),dispatch,
                 created if created is not None else self.now.timestamp()))
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

    def test_recognized_photo_dimensions_name_pending_request_without_commercial_approval(self):
        self.material(state='review')
        with self.manager.db() as db:
            db.execute("UPDATE einkauf_material_dialoge SET review_json='{}',analysis_json=? WHERE id=1",
                       (canonical({'merkmale':{'materialtyp':'Folie','masse':'5 x 120 m','farbe':'gelb'}}),))
            db.commit()
        item = self.reader.page({'bestellung':'material:1'}, now=self.now)['selected']
        self.assertEqual(item['product'], 'Folie 5 x 120 m')
        self.assertEqual(item['state'], 'material_review')
        self.assertEqual(item['sku'], 'nicht belegt')
        self.assertEqual(item['total'], 'nicht belegt')
        self.assertFalse(item['verified'])

    def test_personal_picture_inquiry_is_visible_as_inquiry_and_description_searchable(self):
        self.material(state='review')
        description = 'Stoßstange rechts nach Bild anfragen'
        with self.manager.db() as db:
            db.execute("UPDATE einkauf_material_nachrichten SET phone_number_id='portal:personal' WHERE id=1")
            db.execute("UPDATE einkauf_material_dialoge SET fields_json=?,review_json='{}' WHERE id=1",
                (canonical({'vorgang':{'value':'anfrage'},'beschreibung':{'value':description},
                            'quantity':{'value':'1'},'unit':{'value':'Stück'},'urgent':{'value':True},
                            'order_requested':{'value':False}}),))
            db.commit()
        item = self.reader.page({'q':'Stoßstange','bestellung':'material:1'},now=self.now)['selected']
        self.assertEqual(item['state'],'material_inquiry')
        self.assertEqual(item['state_label'],'Teileanfrage – intern klären')
        self.assertEqual(item['product'],description)
        self.assertEqual(item['due'],'Interne Teileklärung')
        self.assertEqual(item['urgency'],'Dringende Anfrage')
        self.assertEqual(item['vorgang'],'anfrage')
        self.assertEqual(item['beschreibung'],description)
        self.assertTrue(any('Keine Bestellung' in warning for warning in item['warnings']))
        self.assertFalse(item['verified'])
        self.assertEqual(item['order_id'],0)

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

    def test_external_sent_is_visible_once_with_historical_identity_unknown_actual_and_separate_cap(self):
        self.material()
        data=self.reader.page({'bestellung':'material:1'},now=self.now)
        self.assertEqual(data['count'],1)
        self.assertEqual(data['counts'],{'external_sent':1})
        row=data['selected']
        self.assertEqual(row['source'],'material:1')
        self.assertEqual(row['detail_url'],'/admin/assistent-bestellungen/eingang/ansicht?material=1#materialdialog')
        self.assertEqual(row['channel'],'WhatsApp')
        self.assertEqual(row['person'],'Testperson A')
        self.assertEqual(row['supplier'],'Historischer Materiallieferant')
        self.assertEqual((row['quantity'],row['unit']),('1','Stück'))
        self.assertEqual(row['cap'],'250,00 €')
        self.assertEqual(row['total'],'nicht belegt')
        self.assertEqual(row['actual_price'],'nicht belegt')
        self.assertFalse(row['actual_price_known']);self.assertFalse(row['verified'])
        self.assertTrue(row['detail_url'].endswith('material=1#materialdialog'))
        self.assertEqual(row['state'],'external_sent')
        self.assertEqual(len(row['events']),2)
        self.assertTrue(row['send_evidence'])
        self.assertIn({'id':'mitarbeiter:1','name':'Testperson A'},data['people'])
        serialized=json.dumps(data)
        for private in ('PRIVATE_SOURCE_MESSAGE','PRIVATE_AUTHORIZATION','PRIVATE_MAIL_BODY','NEVER_SHOW_BANK_DATA','private-do-not-show'):
            self.assertNotIn(private,serialized)

    def test_external_pending_missing_or_tampered_proof_never_claims_sent(self):
        self.material(1,state='external_pending')
        self.material(2,broken=True)
        self.material(3,send_evidence='')
        self.material(4,draft_id=999)
        self.material(5,employee_id=2)
        self.material(6,sent_at='2026-09-29T08:05:00')
        self.material(7,reservation_id='wrong')
        self.material(8,quantity='2')
        self.material(9,sent_at='2026-09-28T00:00:00+00:00')
        self.material(10,state='external_pending',reserved_at='')
        self.material(11,max_total_cents=25001)
        self.material(12,supplier_name='')
        self.material(13,send_evidence='   ')
        self.material(14,send_evidence=123)
        data=self.reader.page({},now=self.now)
        self.assertEqual(data['counts'],{'external_pending':1,'unknown':13})
        pending=self.reader.page({'bestellung':'material:1'},now=self.now)['selected']
        self.assertEqual(pending['mail_updated'],'')
        self.assertEqual(pending['send_evidence'],'')
        self.assertEqual(len(pending['events']),1)
        for row in data['items']:
            if row['source']=='material:1':continue
            self.assertEqual(row['state'],'unknown')
            self.assertEqual(row['cap'],'nicht belegt')
            self.assertTrue(row['warnings'])
            self.assertEqual(row['send_evidence'],'')

    def test_material_filters_and_merged_pagination_preserve_every_request(self):
        self.material(1,employee=2,product_name='100% Crystal',created=datetime(2026,9,28,22,15,tzinfo=timezone.utc).timestamp())
        self.material(2,supplier_id='supplier-other',created=datetime(2026,9,28,20,tzinfo=timezone.utc).timestamp())
        for query in ({'person':'mitarbeiter:2'},{'q':'%'},{'supplier':'supplier-a'},
                      {'from':'2026-09-29','to':'2026-09-29'},{'q':'material:1'}):
            self.assertEqual([r['source'] for r in self.reader.page(query,now=self.now)['items']],['material:1'])
        self.assertEqual(self.reader.page({'urgency':'weekly'},now=self.now)['count'],0)
        self.assertEqual(self.reader.page({'batch':'unrelated'},now=self.now)['count'],0)
        self.assertEqual(self.reader.page({'state':'overdue'},now=self.now)['count'],0)
        self.assertEqual(self.reader.page({'person':"' OR 1=1 --"},now=self.now)['count'],0)
        for index in range(3,37):
            self.material(index,created=self.now.timestamp()+index)
            self.order(f'order-{index:03d}',created=self.now.timestamp()+index-.5)
        seen=[]
        for page in range(1,4):
            data=self.reader.page({'page':str(page)},now=self.now)
            self.assertEqual(data['count'],70)
            seen.extend(row['id'] for row in data['items'])
        self.assertEqual(len(seen),70);self.assertEqual(len(set(seen)),70)

    def test_durable_material_queue_owns_one_row_including_enqueue_acknowledgement_gap(self):
        self.material(1,state='accepted',dispatch='queue-1')
        self.order('queue-1',request_id='material:1')
        self.material(2,state='approved')
        self.order('queue-2',request_id='material:2')
        self.material(3,state='accepted',dispatch='queue-3')
        self.order('queue-3',request_id='historical-other-key')
        data=self.reader.page({},now=self.now)
        self.assertEqual(data['count'],3)
        self.assertEqual({row['id'] for row in data['items']},{'queue-1','queue-2','queue-3'})
        row=self.reader.page({'bestellung':'material:1'},now=self.now)['selected']
        self.assertEqual(row['id'],'queue-1')
        self.assertEqual(row['source'],'material:1')
        self.assertEqual(row['detail_url'],'/admin/assistent-bestellungen/eingang/ansicht?material=1#materialdialog')
        self.assertEqual(row['channel'],'WhatsApp')
        fallback=self.reader.page({'bestellung':'material:3'},now=self.now)
        self.assertEqual(fallback['errors'],[])
        self.assertEqual(fallback['selected']['id'],'queue-3')
        self.assertEqual(fallback['selected']['source'],'material:3')
        self.assertEqual(fallback['selected']['channel'],'WhatsApp')

    def test_unordered_and_orphaned_material_states_are_not_reported_as_sent(self):
        for key,state in enumerate(('open','review','approved','cancelled','accepted'),start=1):
            self.material(key,state=state)
        data=self.reader.page({},now=self.now)
        self.assertEqual(data['count'],5)
        self.assertEqual(set(data['counts']),{'material_open','material_review','material_ready','cancelled','material_accepted'})
        for row in data['items']:
            self.assertFalse(row['verified']);self.assertEqual(row['mail_updated'],'')
            self.assertEqual(row['total'],'nicht belegt');self.assertEqual(row['cap'],'nicht belegt')
        self.assertTrue(self.reader.page({'bestellung':'material:5'},now=self.now)['selected']['warnings'])

    def test_material_projection_only_selects_without_recovery_or_transport(self):
        self.material()
        original=self.p.get_db
        def readonly():
            db=original()
            forbidden={sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE,
                       sqlite3.SQLITE_CREATE_TABLE,sqlite3.SQLITE_DROP_TABLE}
            db.set_authorizer(lambda action,*args:sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        with patch.object(self.manager,'tick',side_effect=AssertionError('no dispatch')), \
                patch.object(self.manager.dispatch,'status',side_effect=AssertionError('no recovery')):
            data=OrderOverview(readonly).page({'bestellung':'material:1'},now=self.now)
        self.assertEqual(data['selected']['state'],'external_sent')

    def test_pending_duplicate_is_visible_until_matching_employee_confirmation(self):
        self.material(state='open')
        marker={'id':2,'signature':'synthetic-same-item'}
        with self.manager.db() as db:
            fields=json.loads(db.execute('SELECT fields_json FROM einkauf_material_dialoge WHERE id=1').fetchone()[0])
            fields['possible_duplicate']={'value':marker}
            db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=1',(canonical(fields),))
            db.commit()
        row=self.reader.page({'bestellung':'material:1'},now=self.now)['selected']
        self.assertIn('Doppelbestellung',row['state_label'])
        self.assertTrue(row['warnings'])
        with self.manager.db() as db:
            fields['duplicate_confirmation']={'value':marker}
            db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=1',(canonical(fields),))
            db.commit()
        row=self.reader.page({'bestellung':'material:1'},now=self.now)['selected']
        self.assertNotIn('Doppelbestellung',row['state_label'])


    def test_personal_photo_source_keeps_its_channel_before_and_after_handoff(self):
        self.material(1, state='review')
        self.material(2, state='accepted', dispatch='portal-queued')
        self.order('portal-queued', request_id='material:2')
        with self.manager.db() as db:
            db.execute("UPDATE einkauf_material_nachrichten SET phone_number_id='portal:personal'")
            db.commit()
        before = self.reader.page({'bestellung': 'material:1'}, now=self.now)['selected']
        after = self.reader.page({'bestellung': 'material:2'}, now=self.now)['selected']
        self.assertEqual(before['channel'], 'Fotoformular')
        self.assertEqual(after['channel'], 'Fotoformular')
        self.assertEqual(after['id'], 'portal-queued')
        self.assertEqual(self.reader.page({}, now=self.now)['count'], 2)


if __name__ == '__main__':
    unittest.main()
