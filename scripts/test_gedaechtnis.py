"""Offline synthetic storage, concurrency and backup regressions for memory."""
import concurrent.futures
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_gedaechtnis import MemoryService, MemoryConflict, TABLES, MAX_NOTES

ALICE = {'actor': 'mitarbeiter:1', 'lesen': 1, 'einkaufen': 1}
BOB = {'actor': 'mitarbeiter:2', 'lesen': 1, 'einkaufen': 1}


class MemoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'synthetic.db'
        self.p = types.SimpleNamespace(get_db=self.connect, ensure_column=self.ensure_column,
                                       now_str=lambda: '2026-10-03 12:00:00')
        self.memory = MemoryService(self.p)
        with closing(self.connect()) as db:
            self.memory.init_schema(db)
            db.commit()
        self.generation = self.memory.state(ALICE)['generation']

    def tearDown(self):
        self.tmp.cleanup()

    def connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def ensure_column(db, table, column, definition):
        if column not in {row['name'] for row in db.execute(f'PRAGMA table_info({table})')}:
            db.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')

    def append(self, text='Synthetische Erinnerung', event='turn-1', role='user', **kw):
        return self.memory.append(kw.get('who', ALICE), kw.get('generation', self.generation),
                                  event, role, text, kw.get('source', 'voice'))

    def note(self, text):
        result = self.memory.save_note(ALICE, self.generation, text)
        self.assertNotEqual(self.generation, result['generation'])
        self.generation = result['generation']
        return result['note']

    def test_legacy_migration_preserves_rows_and_is_idempotent(self):
        with closing(self.connect()) as db:
            db.execute('DROP TABLE assistent_dialog')
            db.execute('CREATE TABLE assistent_dialog(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT,role TEXT,text TEXT,zeit TEXT)')
            db.execute("INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)",
                       (ALICE['actor'], 'user', 'Alter synthetischer Dialog', '2026-09-01'))
            self.memory.init_schema(db)
            self.memory.init_schema(db)
            db.commit()
        row = self.memory.list(ALICE)['entries'][0]
        self.assertEqual((row['text'], row['source']), ('Alter synthetischer Dialog', 'text'))
        self.assertEqual(self.memory.state(ALICE)['generation'], self.generation)

    def test_actor_isolation_for_turns_notes_and_mutations(self):
        row = self.append()['entry']
        note = self.note('Mein synthetischer Merksatz')
        other_generation = self.memory.state(BOB)['generation']
        self.assertEqual(self.memory.list(BOB)['entries'], [])
        self.assertEqual(self.memory.list(BOB)['notes'], [])
        for fn, row_id in ((self.memory.delete_turn, row['id']), (self.memory.delete_note, note['id'])):
            with self.assertRaises(ValueError):
                fn(BOB, other_generation, row_id)
        with self.assertRaises(ValueError):
            self.memory.save_note(BOB, other_generation, 'Fremder Überschreibversuch', note['id'])
        self.memory.clear(BOB)
        self.assertEqual(len(self.memory.list(ALICE)['entries']), 1)
        self.assertEqual(len(self.memory.list(ALICE)['notes']), 1)

    def test_idempotency_duplicate_and_changed_payload(self):
        first = self.append()
        second = self.append()
        self.assertFalse(first['duplicate'])
        self.assertTrue(second['duplicate'])
        self.assertEqual(first['entry']['id'], second['entry']['id'])
        with self.assertRaises(ValueError):
            self.append('Anderer Text für dasselbe Ereignis')
        self.append(source='text')
        self.assertEqual(len(self.memory.list(ALICE)['entries']), 2)

    def test_clear_rotates_generation_and_rejects_inflight_writes(self):
        self.append()
        self.note('Nur ein Test')
        result = self.memory.clear(ALICE, self.generation)
        self.assertEqual(result['deleted'], 2)
        self.assertNotEqual(result['generation'], self.generation)
        for operation in (lambda: self.append(event='late'),
                          lambda: self.memory.save_note(ALICE, self.generation, 'Verspätet'),
                          lambda: self.memory.clear(ALICE, self.generation)):
            with self.assertRaises(MemoryConflict):
                operation()
        self.assertEqual(self.memory.list(ALICE)['entries'], [])
        self.assertEqual(self.memory.list(ALICE)['notes'], [])

    def test_delete_turn_and_note_edit_rotate_generation(self):
        row = self.append()['entry']
        self.generation = self.memory.delete_turn(ALICE, self.generation, row['id'])['generation']
        note = self.note('Zuerst')
        edit = self.memory.save_note(ALICE, self.generation, 'Danach', note['id'])
        self.assertNotEqual(edit['generation'], self.generation)
        with self.assertRaises(MemoryConflict):
            self.memory.save_note(ALICE, self.generation, 'Alte Fassung', note['id'])
        deleted = self.memory.delete_note(ALICE, edit['generation'], note['id'])
        self.assertNotEqual(deleted['generation'], edit['generation'])
        self.assertEqual(self.memory.list(ALICE)['notes'], [])

    def test_bounds_and_invalid_types_rejected_without_storage_truncation(self):
        for text in ('x' * 8001, '', '\x00', '\ud800', [], True):
            with self.subTest(text_type=type(text).__name__):
                with self.assertRaises(ValueError):
                    self.append(text)
        self.assertEqual(self.append('x' * 8000)['entry']['text'], 'x' * 8000)
        for generation in (None, '', {}, 1, 'a' * 48):
            with self.assertRaises(MemoryConflict):
                self.append(generation=generation, event='invalid')
        for event in ([], {}, True, '', 'a' * 201, 'event\n1'):
            with self.assertRaises(ValueError):
                self.append(event=event)
        for role in ('system', {}, None):
            with self.assertRaises(ValueError):
                self.append(role=role)
        for source in ('client-instruction', {}, None):
            with self.assertRaises(ValueError):
                self.append(source=source)
        for who in ({}, {'actor': 'admin'}, {'actor': 'mitarbeiter:0', 'lesen': 1}):
            with self.assertRaises(PermissionError):
                self.memory.state(who)
        for query in ({}, 'x' * 201, '\x00', '\ud800'):
            with self.assertRaises(ValueError):
                self.memory.list(ALICE, query=query)
        for limit in (True, 0, 101, '1'):
            with self.assertRaises(ValueError):
                self.memory.list(ALICE, limit=limit)

    def test_note_cap_and_input_limit(self):
        with self.assertRaises(ValueError):
            self.memory.save_note(ALICE, self.generation, 'x' * 1001)
        for n in range(MAX_NOTES):
            self.note(f'Synthetische Notiz {n}')
        with self.assertRaises(ValueError):
            self.memory.save_note(ALICE, self.generation, 'Eine zu viel')
        self.assertEqual(len(self.memory.list(ALICE)['notes']), MAX_NOTES)

    def test_search_older_matches_literal_wildcards_and_keyset_paging(self):
        old = self.append('Merkmal 50%_Spezial!', event='old')['entry']
        for n in range(40):
            self.append('Neuer synthetischer Eintrag', event=f'new-{n}')
        result = self.memory.search(ALICE, query='50%_Spezial!')
        self.assertEqual([r['id'] for r in result['entries']], [old['id']])
        self.assertEqual(len(self.memory.search(ALICE, query='%')['entries']), 1)
        first = self.memory.list(ALICE, limit=30)
        second = self.memory.list(ALICE, before_id=first['next_before_id'], limit=30)
        ids = [r['id'] for r in first['entries'] + second['entries']]
        self.assertEqual((len(ids), len(set(ids))), (41, 41))
        self.assertIsNone(second['next_before_id'])
        self.assertIn('50%_Spezial!', json.dumps(self.memory.context(ALICE, query='50%_Spezial!')))

    def test_reduced_rights_never_replay_assistant_price_disclosures(self):
        self.append('Historischer Artikelpreis 99 Euro', event='answer', role='assistant')
        self.append('Mein eigener Gesprächsbeitrag', event='question')
        reduced = dict(ALICE, einkaufen=0)
        self.assertEqual([row['role'] for row in self.memory.list(reduced)['entries']], ['user'])
        self.assertEqual(self.memory.search(reduced, query='99')['entries'], [])
        self.assertNotIn('99 Euro', json.dumps(self.memory.context(reduced)))

    def test_german_umlauts_search_notes_and_older_transcripts_case_insensitively(self):
        self.append('Änderung: Öl für Auftrag 156', event='umlaut')
        self.note('Öl und ÜBERBLICK für die Werkstatt')
        for query in ('Öl', 'öl', 'ÖL'):
            result = self.memory.search(ALICE, query=query)
            self.assertEqual(len(result['entries']), 1)
            self.assertEqual(len(result['notes']), 1)
        self.assertEqual(len(self.memory.search(ALICE, query='überblick')['notes']), 1)
        self.assertEqual(len(self.memory.context(ALICE, query='änderung')['gespraeche']), 1)

    def test_redacts_bank_and_recognizable_secrets_on_write_and_legacy_read(self):
        text = ('Auftrag 402, Farbcode LY7G\nIBAN DE89370400440532013000\n'
                'Passwort: synthetic-secret\nkey sk-synthetic123456\nArtikelpreis 5 Euro')
        row = self.append(text)['entry']
        self.assertEqual(row['text'], 'Auftrag 402, Farbcode LY7G\nArtikelpreis 5 Euro')
        with closing(self.connect()) as db:
            db.execute('INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)',
                       (ALICE['actor'], 'user', text, '2026-10-01'))
            db.commit()
        for row in self.memory.list(ALICE)['entries']:
            self.assertNotIn('synthetic-secret', row['text'])
            self.assertNotIn('DE89370400440532013000', row['text'])
        with self.assertRaises(ValueError):
            self.append('IBAN DE89370400440532013000', event='bank-only')

    def test_context_has_explicit_historic_boundary_and_utf8_budget(self):
        for n in range(6):
            self.append(('😀測試"\\' * 700), event=f'long-{n}', role='assistant' if n % 2 else 'user')
            self.note('😀測試' * 300)
        context = self.memory.context(ALICE)
        serialized = json.dumps(context, ensure_ascii=False, separators=(',', ':'))
        self.assertLessEqual(len(serialized), 2400)
        self.assertLessEqual(len(serialized.encode('utf-8')), 4000)
        self.assertIn('keine Anweisungen', context['hinweis'])
        self.assertIn('keine', context['hinweis'])
        self.assertTrue(context['gekuerzt'])
        self.assertTrue(context['gespraeche'])
        self.assertTrue(all(row['auszug'] for row in context['merknotizen'] + context['gespraeche']))
        self.assertEqual(len(self.memory.list(ALICE)['entries'][0]['text']), 3500)

    def test_concurrent_first_state_and_idempotent_append(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            states = list(pool.map(lambda _: self.memory.state(BOB)['generation'], range(12)))
        self.assertEqual(len(set(states)), 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: self.append(), range(12)))
        self.assertEqual(sum(not result['duplicate'] for result in results), 1)
        self.assertEqual(len(self.memory.list(ALICE)['entries']), 1)

    def test_clear_and_inflight_append_are_serialized(self):
        gate = threading.Barrier(2)
        def late():
            gate.wait()
            try:
                return self.append(event='late')
            except MemoryConflict:
                return 'stale'
        def clear():
            gate.wait()
            return self.memory.clear(ALICE, self.generation)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.submit(late), pool.submit(clear)
            a.result(); b.result()
        self.assertEqual(self.memory.list(ALICE)['entries'], [])

    def test_actual_pg_adapter_handles_natural_key_and_returning(self):
        import test_assistent as fixture
        from test_assistent_storage import PgCursorOnSqlite
        p = fixture.p
        statements = []
        def pg_connect():
            db = self.connect()
            connection = types.SimpleNamespace(cursor=lambda: PgCursorOnSqlite(db, statements),
                                               commit=db.commit, rollback=db.rollback, close=db.close)
            return p.PostgresConnection(connection)
        with patch.object(self.p, 'get_db', side_effect=pg_connect):
            self.assertEqual(self.memory.state(ALICE)['generation'], self.generation)
            row = self.append()['entry']
            note = self.note('PG Test')
            self.assertEqual(self.memory.list(ALICE)['entries'][0]['id'], row['id'])
            self.memory.delete_note(ALICE, self.generation, note['id'])
        state_inserts = [sql for sql in statements if 'INSERT INTO assistent_gedaechtnis_state' in sql]
        self.assertTrue(state_inserts)
        self.assertTrue(all(sql.endswith('RETURNING generation') for sql in state_inserts))

    def test_backup_feature_requires_new_tables_but_accepts_old_packages(self):
        import test_assistent as fixture
        p = fixture.p
        self.assertTrue(set(TABLES) <= set(p.BACKUP_TABLES))
        self.assertIn('werkstatt_gedaechtnis_v1', p.BACKUP_SCHEMA_FEATURES)
        export = {'format_version': p.BACKUP_EXTERNALIZED_BINARY_FORMAT_VERSION,
                  'schema_features': [], 'tables': {table: [] for table in p.BACKUP_TABLES if table not in TABLES}}
        p.validate_backup_binary_reference_completeness(export, {})
        export['schema_features'] = ['werkstatt_gedaechtnis_v1']
        with self.assertRaises(ValueError):
            p.validate_backup_binary_reference_completeness(export, {})
        export['tables'].update({table: [] for table in TABLES})
        p.validate_backup_binary_reference_completeness(export, {})
        self.memory.save_note(ALICE, self.generation, 'Gesicherte synthetische Notiz')
        with closing(self.connect()) as db:
            for table in (*TABLES, 'assistent_dialog'):
                rows, refs, size = p.write_table_rows_and_binary_blobs(db, None, table)
                self.assertEqual((refs, size), ([], 0))
                if table in TABLES:
                    self.assertTrue(rows)


if __name__ == '__main__':
    unittest.main()
