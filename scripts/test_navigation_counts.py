"""Compare the real legacy inbox with the one-query badge on synthetic data."""
import random
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from smoke_test import portal as p  # Isolated app import; external sockets denied.
from portal_navigation_counts import count_admin_postfach_items


SCHEMA = """
CREATE TABLE autohaeuser (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE auftraege (
    id INTEGER PRIMARY KEY, fahrzeug TEXT DEFAULT '', kennzeichen TEXT DEFAULT '',
    leistungsart TEXT DEFAULT '', schaden_aufnahme_json TEXT DEFAULT '',
    auftragsnummer TEXT DEFAULT '', versicherung_id INTEGER DEFAULT 0,
    autohaus_id INTEGER, archiviert INTEGER DEFAULT 0,
    angebotsphase INTEGER DEFAULT 0, angebot_abgesendet INTEGER DEFAULT 0,
    angebot_status TEXT DEFAULT '', geaendert_am TEXT DEFAULT '',
    erstellt_am TEXT DEFAULT '', analyse_text TEXT DEFAULT '',
    beschreibung TEXT DEFAULT '', quelle TEXT DEFAULT '', status INTEGER DEFAULT 2,
    analyse_pruefen INTEGER DEFAULT 0, analyse_werkstatt_geprueft INTEGER DEFAULT 0,
    analyse_hinweis TEXT DEFAULT ''
);
CREATE TABLE chat_nachrichten (
    id INTEGER PRIMARY KEY, auftrag_id INTEGER, absender TEXT,
    nachricht TEXT DEFAULT '', erstellt_am TEXT DEFAULT '', gelesen_admin INTEGER DEFAULT 0
);
CREATE TABLE versicherung_aufgaben (
    id INTEGER PRIMARY KEY, auftrag_id INTEGER, titel TEXT DEFAULT '',
    beschreibung TEXT DEFAULT '', erstellt_am TEXT DEFAULT '', status TEXT DEFAULT 'offen'
);
CREATE TABLE verzoegerungen (
    id INTEGER PRIMARY KEY, auftrag_id INTEGER, meldung TEXT DEFAULT '',
    erstellt_am TEXT DEFAULT '', uebernommen INTEGER DEFAULT 0
);
CREATE TABLE reklamationen (
    id INTEGER PRIMARY KEY, auftrag_id INTEGER, meldung TEXT DEFAULT '',
    erstellt_am TEXT DEFAULT '', bearbeitet INTEGER DEFAULT 0
);
CREATE TABLE rahmenvertrag_anfragen (
    id INTEGER PRIMARY KEY, autohaus_id INTEGER, nachricht TEXT DEFAULT '',
    erstellt_am TEXT DEFAULT '', status TEXT DEFAULT 'offen'
);
"""


class ReadConnection:
    """Borrow the fixture connection; reject accidental writes on both paths."""
    def __init__(self, db):
        self.db = db
        self.statements = []

    def execute(self, sql, params=()):
        if not sql.lstrip().upper().startswith(('SELECT', 'WITH')):
            raise AssertionError('Inbox counts must remain read-only')
        self.statements.append(sql)
        return self.db.execute(sql, params)

    def close(self):
        pass


class NavigationCountsTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.insert('autohaeuser', id=1, name='Synthetic dealer')
        self.order(1)

    def insert(self, table, **values):
        columns = ','.join(values)
        placeholders = ','.join('?' for _ in values)
        self.db.execute(f'INSERT INTO {table} ({columns}) VALUES ({placeholders})', tuple(values.values()))

    def order(self, order_id, **values):
        self.insert('auftraege', id=order_id, **values)

    def compare(self, hidden=(), limit=200, expected=None):
        old_db = ReadConnection(self.db)
        with patch.object(p, 'get_db', return_value=old_db), \
             patch.object(p, 'postfach_hidden_keys', return_value=set(hidden)), \
             p.app.test_request_context('/admin/cockpit'):
            old = p.list_admin_postfach_items(limit=limit)
        new_db = ReadConnection(self.db)
        actual = count_admin_postfach_items(new_db, hidden, limit=limit)
        self.assertEqual(actual, len(old))
        self.assertEqual(len(new_db.statements), 1)
        if expected is not None:
            self.assertEqual(actual, expected)
        return old

    def test_empty_and_global_slice_limits(self):
        self.compare(expected=0)
        for index in range(1, 91):
            self.insert('chat_nachrichten', id=index, auftrag_id=1, absender='autohaus')
        for limit, expected in [(0, 0), (1, 1), (8, 8), (200, 80), (None, 80), (-1, 79)]:
            with self.subTest(limit=limit):
                self.compare(limit=limit, expected=expected)

    def test_each_source_caps_before_hidden_items_are_removed(self):
        # Reversed timestamps catch substituting ID order for the legacy dates.
        for index in range(1, 101):
            stamp = f'2026-10-{101-index:02} 08:00:00'
            self.insert('chat_nachrichten', id=index, auftrag_id=1, absender='versicherung', erstellt_am=stamp)
            self.insert('versicherung_aufgaben', id=index, auftrag_id=1, erstellt_am=stamp)
            self.insert('verzoegerungen', id=index, auftrag_id=1, erstellt_am=stamp)
            self.insert('reklamationen', id=index, auftrag_id=1, erstellt_am=stamp)
            self.insert('rahmenvertrag_anfragen', id=index, autohaus_id=1, erstellt_am=stamp)
            self.order(1000+index, angebotsphase=1, angebot_abgesendet=1,
                       angebot_status='angefragt', geaendert_am=stamp)
            self.order(2000+index, quelle='autohaus', status=1, erstellt_am=stamp)
            self.order(3000+index, analyse_pruefen=1, geaendert_am=stamp)
        old = self.compare(limit=1000, expected=640)
        prefixes = {'admin-chat-': 0, 'admin-versicherung-aufgabe-': 0,
                    'admin-angebot-': 1000, 'admin-auftrag-': 2000,
                    'admin-verzoegerung-': 0, 'admin-reklamation-': 0,
                    'admin-dokument-': 3000, 'admin-rahmenvertrag-': 0}
        for prefix, offset in prefixes.items():
            keys = [item['item_key'] for item in old if item['item_key'].startswith(prefix)]
            self.assertEqual(len(keys), 80)
            # Hide 15 selected rows, plus 20 rows outside this source's cap.
            outside = {f'{prefix}{offset+number}' for number in range(1, 101)} - set(keys)
            self.assertEqual(len(outside), 20)
            hidden = set(keys[:15]) | outside
            with self.subTest(source=prefix):
                self.compare(hidden=hidden, limit=1000, expected=625)
        hidden = {item['item_key'] for item in old[::3]}
        self.compare(hidden=hidden, limit=200, expected=200)
        self.compare(hidden=hidden, limit=1000, expected=640-len(hidden))
        self.compare(hidden={item['item_key'] for item in old}, limit=1000, expected=0)

    def test_filters_missing_orders_archive_null_and_missing_dealers(self):
        self.order(2, archiviert=1)
        self.order(3, archiviert=None)
        self.order(4, autohaus_id=999, quelle='autohaus', status=1,
                   analyse_pruefen=1, schaden_aufnahme_json='{bad json', leistungsart='unknown')
        self.order(5, angebotsphase=1, angebot_abgesendet=1, angebot_status='angefragt')
        self.order(6, angebotsphase=1, angebot_abgesendet=0, angebot_status='angefragt')
        self.order(7, angebotsphase=1, angebot_abgesendet=1, angebot_status='angenommen')
        self.order(8, quelle='autohaus', status=2, analyse_pruefen=1, analyse_werkstatt_geprueft=1)
        for index, order_id in enumerate((1, 2, 3, 4, 999), 1):
            self.insert('chat_nachrichten', id=index, auftrag_id=order_id, absender='autohaus')
            self.insert('versicherung_aufgaben', id=index, auftrag_id=order_id)
            self.insert('verzoegerungen', id=index, auftrag_id=order_id)
            self.insert('reklamationen', id=index, auftrag_id=order_id)
        self.insert('chat_nachrichten', id=10, auftrag_id=1, absender='werkstatt')
        self.insert('chat_nachrichten', id=11, auftrag_id=1, absender='autohaus', gelesen_admin=1)
        self.insert('versicherung_aufgaben', id=10, auftrag_id=1, status='erledigt')
        self.insert('verzoegerungen', id=10, auftrag_id=1, uebernommen=1)
        self.insert('reklamationen', id=10, auftrag_id=1, bearbeitet=1)
        self.insert('rahmenvertrag_anfragen', id=1, autohaus_id=1)
        self.insert('rahmenvertrag_anfragen', id=2, autohaus_id=999)
        self.insert('rahmenvertrag_anfragen', id=3, autohaus_id=None)
        self.insert('rahmenvertrag_anfragen', id=4, autohaus_id=1, status='erledigt')
        old = self.compare(expected=14)
        keys = {item['item_key'] for item in old}
        self.assertIn('admin-reklamation-2', keys)  # Archived complaints still count.
        self.assertIn('admin-reklamation-3', keys)  # Unset archive flag also allowed here.
        self.assertIn('admin-auftrag-4', keys)  # LEFT JOIN must not require a dealer.
        self.assertNotIn('admin-rahmenvertrag-2', keys)  # This JOIN is intentionally inner.

    def test_same_order_in_several_sources_is_not_deduplicated(self):
        self.db.execute("UPDATE auftraege SET quelle='autohaus',status=1,analyse_pruefen=1 WHERE id=1")
        self.insert('chat_nachrichten', id=1, auftrag_id=1, absender='autohaus')
        self.insert('versicherung_aufgaben', id=1, auftrag_id=1)
        self.insert('verzoegerungen', id=1, auftrag_id=1)
        self.insert('reklamationen', id=1, auftrag_id=1)
        self.compare(expected=6)
        self.compare(hidden={'admin-chat-1'}, expected=5)

    def test_malformed_display_values_do_not_change_count(self):
        self.db.execute("UPDATE auftraege SET schaden_aufnahme_json=?,leistungsart=?,fahrzeug=? WHERE id=1",
                        ('[1,"bad"]', 'not-a-service', '<script>synthetic</script>'))
        for index, stamp in enumerate((None, '', 'not-a-date', '31.02.2026', '2026-10-26T12:34:56Z'), 1):
            self.insert('chat_nachrichten', id=index, auftrag_id=1, absender='autohaus',
                        erstellt_am=stamp, nachricht=None)
        self.compare(expected=5)
        self.compare(hidden={'admin-chat-1', 'admin-chat-4', 'unrelated'}, expected=3)

    def test_seeded_mixed_records_match_actual_legacy_list(self):
        rng = random.Random(20261026)
        for index in range(2, 152):
            self.order(index, archiviert=rng.choice((0, 0, 1, None)),
                       angebotsphase=rng.choice((0, 1)), angebot_abgesendet=rng.choice((0, 1)),
                       angebot_status=rng.choice(('angefragt', 'angenommen', '')),
                       quelle=rng.choice(('autohaus', 'intern')), status=rng.choice((1, 2, 5)),
                       analyse_pruefen=rng.choice((0, 1)), analyse_werkstatt_geprueft=rng.choice((0, 1)),
                       erstellt_am=str(index % 7), geaendert_am=str(index % 5))
        for index in range(1, 251):
            order_id = rng.randrange(1, 175)
            stamp = rng.choice(('', 'bad', '2026-10-26 12:00:00', '25.10.2026 08:00:00'))
            self.insert('chat_nachrichten', id=index, auftrag_id=order_id, erstellt_am=stamp,
                        absender=rng.choice(('autohaus', 'versicherung', 'werkstatt')), gelesen_admin=rng.choice((0, 0, 1)))
            self.insert('versicherung_aufgaben', id=index, auftrag_id=order_id, erstellt_am=stamp,
                        status=rng.choice(('offen', 'erledigt')))
            self.insert('verzoegerungen', id=index, auftrag_id=order_id, erstellt_am=stamp,
                        uebernommen=rng.choice((0, 1)))
            self.insert('reklamationen', id=index, auftrag_id=order_id, erstellt_am=stamp,
                        bearbeitet=rng.choice((0, 1)))
            self.insert('rahmenvertrag_anfragen', id=index, autohaus_id=rng.choice((1, 1, None, 999)),
                        erstellt_am=stamp, status=rng.choice(('offen', 'erledigt')))
        all_items = self.compare(limit=1000)
        hidden = {item['item_key'] for item in all_items if rng.random() < 0.45}
        for limit in (0, 1, 8, 80, 200, 1000, None, -5):
            with self.subTest(limit=limit):
                self.compare(hidden=hidden, limit=limit)

    def test_portal_postgres_adapter_uses_the_same_portable_query(self):
        # Exercise the production SQL conversion and DbRow adapter without any
        # network/server. The query uses standard CTEs, UNION ALL and LIMIT, with
        # no SQLite-only functions or implicit integer/string concatenation.
        self.insert('chat_nachrichten', id=1, auftrag_id=1, absender='autohaus')
        self.insert('chat_nachrichten', id=2, auftrag_id=1, absender='versicherung')
        read_db = ReadConnection(self.db)

        class CursorBridge:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.result.close()

            def execute(self, sql, params):
                self.result = read_db.execute(sql, params)

            @property
            def rowcount(self):
                return self.result.rowcount

            @property
            def description(self):
                return [SimpleNamespace(name=column[0]) for column in self.result.description]

            def fetchall(self):
                return self.result.fetchall()

        adapter = p.PostgresConnection(SimpleNamespace(cursor=CursorBridge))
        self.assertEqual(count_admin_postfach_items(adapter, {'admin-chat-1'}), 1)
        self.assertEqual(len(read_db.statements), 1)


if __name__ == '__main__':
    unittest.main()
