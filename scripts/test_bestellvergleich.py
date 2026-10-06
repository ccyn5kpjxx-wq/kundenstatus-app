"""Frozen order-price appendices, real intake originals, synthetic orders only."""
from contextlib import nullcontext
from copy import deepcopy
import json
from pathlib import Path
import ast
import re
import socket
import sqlite3
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_einkaufseingang as fixtures
from werkstatt_bestellvergleich import OrderPriceComparison, TABLES
from werkstatt_einkaufseingang import IntakeConflict, _hash, _json
from werkstatt_rechnungsfreigabe import classify_invoice_source


class OrderPriceTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.IntakeTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.p = self.f.p
        self.p.workshop_intake = self.f.service
        self.p.portal_originals_operation_lock = nullcontext
        self.allowed = True
        self.p.cockpit_data.catalog.source_rule = lambda value: (classify_invoice_source(value, ['Testlieferant']) if self.allowed else {'allowed': False})
        self.guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network in price comparison'))
        self.guard.start()
        self.addCleanup(self.guard.stop)
        with self.f.service.db() as db:
            db.executescript('''
                CREATE TABLE einkauf_material_dialoge(id INTEGER PRIMARY KEY,state TEXT,snapshot_json TEXT,snapshot_hash TEXT,dispatch_id TEXT DEFAULT '');
                CREATE TABLE assistent_bestellanforderungen(id TEXT PRIMARY KEY,actor_id TEXT,request_id TEXT,
                    request_fingerprint TEXT,snapshot_json TEXT,created_at REAL,batch_id TEXT);
                CREATE TABLE assistent_bestellpakete(id TEXT PRIMARY KEY,state TEXT,result_json TEXT);
            ''')
        self.claim = {'kind': 'manual_external_order', 'reservation_id': 'a' * 32, 'draft_id': 1,
            'supplier_id': 'supplier-1', 'supplier_name': 'Testlieferant', 'recipient': 'orders@example.invalid',
            'article_number': 'TEST-50', 'product_name': 'Testband', 'variant': '50 mm', 'quantity': '2',
            'unit': 'Karton', 'urgent': True, 'max_total_cents': 25000,
            'reserved_at': '2026-10-06T12:00:00+00:00', 'sent_at': '2026-10-06T12:01:00+00:00', 'send_evidence': 'synthetic mail evidence'}
        self.put_claim()
        self.s = OrderPriceComparison(self.p)
        self.group = self.f.group('2')
        self.original = self.f.attach(self.group, kind='rechnung', color='blue')
        self.new_original = self.f.attach(self.group, kind='rechnung', color='red')
        self.price = {'group_id': self.group['id'], 'file_id': self.original['id'], 'page': 2, 'position': 15,
            'source_date': '2026-05-18', 'amount': '10.00', 'currency': 'EUR', 'tax_basis': 'net', 'tax_rate': '19',
            'discount_basis': 'nach Positionsrabatt, ohne Skonto, Versand separat', 'unit': 'Karton', 'pack': '6 Rollen',
            'identity': {'supplier': 'Testlieferant', 'sku': 'TEST-50', 'variant': '50 mm'}, 'reviewed': True}

    def sql(self, query, params=()):
        with self.f.service.db() as db:
            return [dict(row) for row in db.execute(query, params).fetchall()]

    def put_claim(self, state='external_sent'):
        self.sql('INSERT OR REPLACE INTO einkauf_material_dialoge(id,state,snapshot_json,snapshot_hash) VALUES(?,?,?,?)',
                 (1, state, _json(self.claim), _hash(self.claim)))

    def invoice(self, **changes):
        return dict(self.price, file_id=self.new_original['id'], source_date='2026-10-07', amount='12.00', quantity='1', **changes)

    def regular(self, ident='ordinary-1', material=False, state='queued'):
        intent = {key: self.claim[key] for key in ('article_number', 'product_name', 'variant', 'quantity', 'unit')}
        saved = {'actor_id': 'mitarbeiter:1', 'supplier_name': 'Testlieferant', 'order': intent}
        if state != 'queued':
            self.sql('INSERT INTO assistent_bestellpakete VALUES(?,?,?)', ('batch-' + ident, state, '{}'))
        self.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?,?,?,?,?)',
            (ident, 'mitarbeiter:1', 'material:2' if material else 'avatar:test', _hash(intent), _json(saved),
             1791288000, 'batch-' + ident if state != 'queued' else ''))
        return 'order:' + ident

    def catalog(self, **changes):
        row = {'vorschlag_id': 696, 'produkt_name': 'Testband', 'artikelnummer': 'TEST-50', 'lieferant': 'Testlieferant',
            've': 'Gebinde', 'gebinde': '6 Rollen', 'groesse': '50 mm',
            'quelle': {'art': 'einkauf', 'beleg_id': 10, 'beleg': 'Synthetische Rechnung', 'datum': '2026-05-18',
                       'seite': 2, 'position': 15, 'datei_sha256': 'a' * 64},
            'price_evidence': {'basis': 'gebindepreis_netto_abgeleitet', 'value': '10.00', 'reconciled': True}}
        row.update(changes)
        self.f.items.append(row)
        return {'proposal_id': 696, 'identity_confirmed': True, 'unit_matches_order': True}

    def test_external_sent_estimate_is_separate_immutable_idempotent_and_after_dispatch(self):
        before = self.sql('SELECT * FROM einkauf_material_dialoge')
        view = self.s.record_estimate('material:1', self.price)
        self.assertEqual(view['status'], 'awaiting_invoice')
        self.assertEqual(view['estimate']['amount'], '10')
        self.assertEqual(view['estimate']['estimated_article_total'], '20.00')
        self.assertTrue(view['estimate']['after_dispatch'])
        self.assertEqual(self.s.record_estimate('material:1', self.price), view)
        with self.assertRaises(IntakeConflict):
            self.s.record_estimate('material:1', dict(self.price, amount='11'))
        self.assertEqual(self.sql('SELECT * FROM einkauf_material_dialoge'), before)
        self.assertEqual(len(self.sql('SELECT * FROM assistent_bestellpreis_basis')), 1)
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellanforderungen'), [])
        self.assertFalse(view['can_dispatch'])

    def test_invoice_difference_is_per_unit_and_partial_quantity_is_not_complete_order(self):
        self.s.record_estimate('material:1', self.price)
        view = self.s.record_invoice('material:1', self.invoice())
        self.assertEqual(view['status'], 'difference')
        invoice = view['invoices'][0]
        self.assertEqual(invoice['comparison']['difference'], '2')
        self.assertEqual(invoice['comparison']['percent'], '20.00')
        self.assertIsNone(invoice['comparison']['cause'])
        self.assertEqual(invoice['article_total'], '12.00')
        self.assertNotIn('paid', view)
        self.assertNotIn('delivered', view)

    def test_negative_and_zero_baseline_differences(self):
        self.s.record_estimate('material:1', dict(self.price, amount='0'))
        view = self.s.record_invoice('material:1', self.invoice())
        self.assertIsNone(view['invoices'][0]['comparison']['percent'])
        estimate = deepcopy(view['estimate']); estimate['amount'] = '15'
        self.assertEqual(self.s.compare(estimate, view['invoices'][0])['difference'], '-3')
        self.assertEqual(self.s.compare(estimate, view['invoices'][0])['percent'], '-20.00')

    def test_receipt_assignment_dedup_across_aliases_files_groups_and_orders(self):
        key = self.regular(material=True)
        self.s.record_estimate(key, self.price)
        first = self.s.record_invoice(key, self.invoice())
        self.assertEqual(first['order']['key'], 'material:2')
        self.assertEqual(self.s.record_invoice('material:2', self.invoice()), first)
        with self.assertRaises(IntakeConflict):
            self.s.record_invoice('material:1', self.invoice())
        with self.assertRaises(IntakeConflict):
            self.s.record_invoice(key, dict(self.invoice(), amount='13'))
        self.assertEqual(len(self.sql('SELECT * FROM assistent_bestellpreis_rechnungen')), 1)

    def test_regular_order_and_pending_external_do_not_relabel_earlier_capture_after_send(self):
        key = self.regular()
        before = self.s.record_estimate(key, self.price)
        self.assertFalse(before['estimate']['after_dispatch'])
        self.sql('INSERT INTO assistent_bestellpakete VALUES(?,?,?)', ('batch-later', 'sent', '{}'))
        self.sql('UPDATE assistent_bestellanforderungen SET batch_id=? WHERE id=?', ('batch-later', key[6:]))
        self.assertFalse(self.s.detail(key)['estimate']['after_dispatch'])
        self.put_claim('external_pending')
        self.s.record_estimate('material:1', self.price)
        self.put_claim('external_sent')
        self.assertFalse(self.s.detail('material:1')['estimate']['after_dispatch'])

    def test_catalog_estimate_freezes_actual_value_and_keeps_unknown_currency_date_tax(self):
        payload = self.catalog()
        self.f.items[0]['quelle']['datum'] = None
        first = self.s.record_estimate('material:1', payload)
        self.assertFalse(first['estimate']['verified'])
        self.assertEqual(first['estimate']['currency'], 'unknown')
        self.assertIsNone(first['estimate']['tax_rate'])
        self.assertIsNone(first['estimate']['date'])
        self.assertIsNone(first['estimate']['estimated_article_total'])
        self.f.items[0]['price_evidence']['value'] = '999'
        self.assertEqual(self.s.detail('material:1')['estimate']['amount'], '10.00')
        with self.assertRaises(IntakeConflict):
            self.s.record_estimate('material:1', payload)
        view = self.s.record_invoice('material:1', self.invoice())
        self.assertEqual(view['status'], 'not_comparable')
        self.assertIn('Preiswährung', view['invoices'][0]['comparison']['reason'])

    def test_unreviewed_catalog_cannot_invent_basis_or_override_price(self):
        payload = self.catalog()
        for changes in ({'identity_confirmed': False}, {'unit_matches_order': False}, {'amount': '1'}):
            with self.assertRaises(ValueError):
                self.s.record_estimate('material:1', dict(payload, **changes))
        self.f.items[0]['price_evidence']['basis'] = 'unknown'
        with self.assertRaises(ValueError):
            self.s.record_estimate('material:1', payload)

    def test_unknown_or_mismatched_units_pack_tax_or_discount_give_specific_reasons(self):
        view = self.s.record_estimate('material:1', self.price)
        base = view['estimate']
        for field, value, word in [('unit', 'Liter', 'Preiseinheit'), ('pack', '12 Rollen', 'Gebinde'),
            ('tax_basis', 'gross', 'Netto'), ('tax_rate', None, 'Steuersatz'),
            ('discount_basis', 'vor Rabatt', 'Rabatt'), ('currency', 'CHF', 'Preiswährung')]:
            actual = dict(base, **{field: value})
            result = self.s.compare(base, actual)
            self.assertFalse(result['comparable'])
            self.assertIn(word, result['reason'])
        for change in ({'unit': 'Liter'}, {'currency': 'CHF'}, {'reviewed': False},
            {'identity': dict(self.price['identity'], sku='OTHER')}):
            with self.assertRaises(ValueError):
                self.s.record_invoice('material:1', dict(self.invoice(), **change))

    def test_source_revocation_before_capture_and_after_capture_hides_values_without_deletion(self):
        self.allowed = False
        with patch.object(self.f.service, 'original') as read, self.assertRaises(PermissionError):
            self.s.record_estimate('material:1', self.price)
        read.assert_not_called()
        self.allowed = True
        self.s.record_estimate('material:1', self.price)
        self.allowed = False
        view = self.s.detail('material:1')
        self.assertEqual(view['status'], 'source_blocked')
        self.assertNotIn('amount', view['estimate'])
        self.assertEqual(len(self.sql('SELECT * FROM assistent_bestellpreis_basis')), 1)

    def test_permission_revocation_during_original_read_prevents_publication(self):
        real = self.f.service.original
        def revoked(*args):
            result = real(*args)
            self.allowed = False
            return result
        with patch.object(self.f.service, 'original', side_effect=revoked), self.assertRaises(PermissionError):
            self.s.record_estimate('material:1', self.price)
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellpreis_basis'), [])

    def test_bank_named_invoice_and_delivery_note_are_not_price_sources(self):
        self.sql('UPDATE einkauf_eingang_dateien SET original_name=? WHERE id=?', ('Kontoauszug.pdf', self.original['id']))
        with patch.object(self.f.service, 'original') as read, self.assertRaises(PermissionError):
            self.s.record_estimate('material:1', self.price)
        read.assert_not_called()
        self.sql('UPDATE einkauf_eingang_dateien SET original_name=?,kind=? WHERE id=?', ('invoice.png', 'lieferschein', self.original['id']))
        with self.assertRaises(PermissionError):
            self.s.record_estimate('material:1', self.price)

    def test_actual_source_supplier_and_reference_are_checked_before_original_access(self):
        for supplier, reference in (('Sparkasse', 'Rechnung'), ('Fremder Lieferant', 'Rechnung'),
                                    ('Lieferant ungeklärt', 'Rechnung'), ('Testlieferant', 'Kontoauszug Bank')):
            with self.subTest(supplier=supplier, reference=reference):
                self.sql('UPDATE einkauf_eingang SET supplier=?,external_ref=? WHERE id=?',
                         (supplier, reference, self.group['id']))
                with patch.object(self.f.service, 'original') as read, self.assertRaises(PermissionError):
                    self.s.record_estimate('material:1', self.price)
                read.assert_not_called()
                self.assertEqual(self.sql('SELECT * FROM assistent_bestellpreis_basis'), [])

    def test_rehashed_external_claim_must_still_belong_to_its_material_order(self):
        for changes in ({'draft_id': 999}, {'reservation_id': 'not-a-reservation'},
                        {'sent_at': ''}, {'send_evidence': ''}):
            with self.subTest(changes=changes):
                original = deepcopy(self.claim)
                self.claim.update(changes)
                self.put_claim()
                with self.assertRaises(ValueError):
                    self.s.record_estimate('material:1', self.price)
                self.claim = original
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellpreis_basis'), [])

    def test_catalog_confirmation_cannot_override_known_size_color_or_package_conflicts(self):
        self.claim.update(variant='50 mm, grün, 6 Rollen')
        self.put_claim()
        payload = self.catalog(farbe='grün')
        for changes in ({'groesse': '30 mm'}, {'farbe': 'blau'}, {'gebinde': '12 Rollen'}):
            with self.subTest(changes=changes):
                original = deepcopy(self.f.items[0])
                self.f.items[0].update(changes)
                with self.assertRaisesRegex(ValueError, 'widerspricht'):
                    self.s.record_estimate('material:1', payload)
                self.f.items[0] = original
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellpreis_basis'), [])
        self.f.items[0]['groesse'] = '50.0 mm'
        self.assertTrue(self.s.record_estimate('material:1', payload)['estimate']['available'])

    def test_source_supplier_changed_after_capture_hides_price_without_changing_appendix(self):
        self.s.record_estimate('material:1', self.price)
        before = self.sql('SELECT * FROM assistent_bestellpreis_basis')
        self.sql('UPDATE einkauf_eingang SET supplier=? WHERE id=?', ('Sparkasse', self.group['id']))
        view = self.s.detail('material:1')
        self.assertFalse(view['estimate']['available'])
        self.assertNotIn('amount', view['estimate'])
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellpreis_basis'), before)

    def test_known_package_volume_conflict_cannot_hide_behind_different_units(self):
        self.claim.update(variant='500 ml')
        self.put_claim()
        payload = self.catalog(groesse='', gebinde='1 Liter')
        with self.assertRaisesRegex(ValueError, 'widerspricht'):
            self.s.record_estimate('material:1', payload)
        self.f.items[0]['gebinde'] = '0,5 Liter'
        result = self.s.record_estimate('material:1', payload)
        self.assertEqual(result['estimate']['unit'], 'Karton')
        self.assertEqual(result['estimate']['pack'], '0,5 Liter')

    def test_missing_original_and_tampered_order_or_estimate_are_not_trusted(self):
        self.s.record_estimate('material:1', self.price)
        self.sql("UPDATE einkauf_eingang_dateien SET sha256='changed' WHERE id=?", (self.original['id'],))
        self.assertFalse(self.s.detail('material:1')['estimate']['available'])
        self.sql("UPDATE assistent_bestellpreis_basis SET payload_hash='changed'")
        self.assertIn('unveränderten', self.s.detail('material:1')['estimate']['reason'])
        self.sql("UPDATE einkauf_material_dialoge SET snapshot_hash='changed'")
        with self.assertRaises(ValueError):
            self.s.detail('material:1')

    def test_future_baseline_and_old_actual_invoice_rejected(self):
        with self.assertRaises(ValueError):
            self.s.record_estimate('material:1', dict(self.price, source_date='2026-10-07'))
        with self.assertRaises(ValueError):
            self.s.record_invoice('material:1', dict(self.invoice(), source_date='2026-10-05'))
        with self.assertRaises(ValueError):
            self.s.record_estimate('material:1', dict(self.price, source_date='date missing'))

    def test_admin_only_and_no_fabricated_order_or_duplicate_material_reference(self):
        for method in (self.s.detail, self.s.record_estimate, self.s.record_invoice):
            args = ('material:1',) if method == self.s.detail else ('material:1', self.price)
            with self.assertRaises(PermissionError):
                method(*args, actor='mitarbeiter:1')
        with self.assertRaises(ValueError):
            self.s.detail('material:999')
        self.regular(ident='a', material=True)
        self.regular(ident='b', material=True)
        with self.assertRaises(ValueError):
            self.s.detail('material:2')

    def test_schema_restart_and_table_roundtrip_preserve_frozen_basis_and_receipt(self):
        self.s.record_estimate('material:1', self.price)
        before = self.s.record_invoice('material:1', self.invoice())
        self.s.init_schema()
        saved = {table: self.sql('SELECT * FROM ' + table) for table in TABLES}
        with self.s.db() as db:
            for table in reversed(TABLES):
                db.execute('DELETE FROM ' + table)
            for table, rows in saved.items():
                for row in rows:
                    columns = list(row)
                    db.execute('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')',
                        tuple(row[column] for column in columns))
        self.assertEqual(self.s.detail('material:1'), before)
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellanforderungen'), [])

    def test_dispatch_id_link_and_url_alias_share_one_canonical_price_appendix(self):
        key = self.regular(ident='linked-old')
        self.sql("INSERT INTO einkauf_material_dialoge VALUES(2,'accepted','{}','','linked-old')")
        first = self.s.record_estimate('material:2', self.price)
        self.assertEqual(first['order']['key'], 'material:2')
        self.assertEqual(self.s.record_estimate(key, self.price), first)
        self.assertEqual(self.s.record_estimate('dispatch:linked-old', self.price), first)
        self.assertEqual(len(self.sql('SELECT * FROM assistent_bestellpreis_basis')), 1)

    def test_historical_date_uses_berlin_order_day_at_midnight(self):
        self.claim.update(reserved_at='2026-10-05T22:30:00+00:00', sent_at='2026-10-05T22:31:00+00:00')
        self.put_claim()
        self.assertTrue(self.s.record_estimate('material:1', dict(self.price, source_date='2026-10-06'))['estimate']['available'])

    def test_real_postgres_adapter_inserts_both_tables_without_missing_returning_id(self):
        source = Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        names = {'DbRow', 'PostgresCursor', 'PostgresConnection', 'split_sql_script',
                 'get_insert_table_name', 'convert_sqlite_sql_to_postgres'}
        nodes = [node for node in ast.parse(source).body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<real-postgres-adapter>', 'exec'), namespace)
        statements = []
        class Cursor:
            def __init__(self, db):
                self.raw = db.cursor()
            def __enter__(self): return self
            def __exit__(self, *args): self.raw.close()
            def execute(self, sql, params):
                statements.append(sql)
                self.raw.execute(sql.replace('%s', '?').replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT'), params)
                self.rowcount = self.raw.rowcount
                self.description = [SimpleNamespace(name=item[0]) for item in self.raw.description] if self.raw.description else None
            def fetchall(self): return self.raw.fetchall()
        def get_db():
            db = sqlite3.connect(self.f.database)
            return namespace['PostgresConnection'](SimpleNamespace(cursor=lambda: Cursor(db), commit=db.commit, rollback=db.rollback, close=db.close))
        self.p.get_db = get_db
        self.s.init_schema()
        self.s.record_estimate('material:1', self.price)
        view = self.s.record_invoice('material:1', self.invoice())
        self.assertEqual(view['status'], 'difference')
        inserts = [sql for sql in statements if sql.startswith('INSERT INTO assistent_bestellpreis')]
        self.assertEqual(len(inserts), 2)
        self.assertTrue(all(sql.count('RETURNING id') == 1 for sql in inserts))


if __name__ == '__main__':
    unittest.main()
