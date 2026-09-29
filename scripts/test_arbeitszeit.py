"""Own employee clock regression: isolated synthetic storage, no real bookings."""
import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_mitarbeiter_selfservice import Portal, PERSON, OTHER, ADMIN
from werkstatt_arbeitszeit import TimeTracking
from werkstatt_personal_assistent import PersonalActions


class TimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = Portal(Path(self.temp.name))
        self.clock = datetime(2026, 9, 29, 7, tzinfo=timezone.utc)
        self.s = TimeTracking(self.p, now=lambda:self.clock)

    def at(self, instant, action, *, who=PERSON, key=None):
        self.clock = datetime.fromisoformat(instant)
        revision = self.s.state(who)['revision']
        return self.s.stamp(who, action, key or f'test-request-{revision}', revision)

    def test_identity_active_and_input_types(self):
        for who in (ADMIN, dict(PERSON,actor='mitarbeiter:2'), dict(PERSON,lesen=0), dict(PERSON,mitarbeiter_id=True)):
            with self.assertRaises(ValueError):self.s.state(who)
        self.at('2026-09-29T07:00:00+00:00','kommen',who=OTHER)
        self.assertEqual(self.s.state(PERSON)['zustand'],'abwesend')
        self.assertEqual(self.s.summary(PERSON)['schichten'],[])
        with self.s.db() as db:db.execute('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        with self.assertRaises(ValueError):self.s.stamp(PERSON,'kommen','synthetic-request',0)

    def test_malformed_actions_fail_cleanly_without_changes(self):
        for action in ([],{},None,True,'unknown'):
            with self.assertRaises(ValueError):self.s.preview(PERSON,action)
            with self.assertRaises(ValueError):self.s.stamp(PERSON,action,'synthetic-request',0)
        self.assertEqual(self.s.state(PERSON)['revision'],0)

    def test_server_time_exact_retry_and_stale_revision(self):
        preview = self.s.preview(PERSON,'kommen')
        self.clock = datetime(2026,9,29,8,15,tzinfo=timezone.utc)
        first = self.s.stamp(PERSON,'kommen','synthetic-clock-1',preview['revision'])
        self.clock = datetime(2026,9,29,9,tzinfo=timezone.utc)
        repeat = self.s.stamp(PERSON,'kommen','synthetic-clock-1',preview['revision'])
        self.assertEqual(first['zeit'],'2026-09-29T08:15:00+00:00')
        self.assertEqual(repeat['zeit'],first['zeit']);self.assertTrue(repeat['wiederholt'])
        self.assertEqual(self.s.state(PERSON)['revision'],1)
        with self.assertRaises(ValueError):self.s.stamp(PERSON,'gehen','synthetic-clock-1',1)
        with self.assertRaises(ValueError):self.s.stamp(PERSON,'gehen','synthetic-clock-2',0)

    def test_atomic_rollback_when_event_insert_fails(self):
        with self.s.db() as db:
            db.execute("CREATE TRIGGER reject_stamp BEFORE INSERT ON mitarbeiter_zeitstempel BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.s.stamp(PERSON,'kommen','synthetic-clock-1',0)
        self.assertEqual(self.s.state(PERSON)['zustand'],'abwesend')
        self.assertEqual(self.s.state(PERSON)['revision'],0)

    def test_pauses_only_explicit_and_open_shifts_excluded(self):
        self.at('2026-09-29T07:00:00+00:00','kommen')
        self.at('2026-09-29T10:00:00+00:00','pause')
        self.clock=datetime(2026,9,29,11,tzinfo=timezone.utc)
        open_report=self.s.summary(PERSON)
        self.assertEqual(open_report['abgeschlossene_arbeitszeit'],'0:00 Stunden')
        self.assertTrue(open_report['schichten'][0]['offen'])
        self.assertEqual(open_report['schichten'][0]['pause'],'1:00 Stunden')
        self.at('2026-09-29T11:00:00+00:00','weiter')
        self.at('2026-09-29T15:00:00+00:00','gehen')
        result=self.s.summary(PERSON)
        self.assertEqual(result['abgeschlossene_arbeitszeit'],'7:00 Stunden')
        self.assertEqual(result['schichten'][0]['pause'],'1:00 Stunden')
        self.at('2026-09-30T07:00:00+00:00','kommen')
        self.at('2026-09-30T15:00:00+00:00','gehen')
        self.assertEqual(self.s.summary(PERSON)['abgeschlossene_arbeitszeit'],'15:00 Stunden')

    def test_month_boundary_closed_shift_portions(self):
        self.at('2026-09-30T20:00:00+00:00','kommen')  # 22:00 Berlin
        self.at('2026-10-01T04:00:00+00:00','gehen')  # 06:00 Berlin
        september=self.s.summary(PERSON,'2026-09')
        october=self.s.summary(PERSON,'2026-10')
        self.assertEqual(september['abgeschlossene_arbeitszeit'],'2:00 Stunden')
        self.assertFalse(september['schichten'][0]['offen'])
        self.assertEqual(october['abgeschlossene_arbeitszeit'],'6:00 Stunden')

    def test_month_boundary_pause_and_exact_midnight(self):
        self.at('2026-09-30T20:00:00+00:00','kommen')
        self.at('2026-09-30T21:30:00+00:00','pause')
        self.at('2026-09-30T22:30:00+00:00','weiter')
        self.at('2026-10-01T00:00:00+00:00','gehen')
        for month in ('2026-09','2026-10'):
            report=self.s.summary(PERSON,month)
            self.assertEqual(report['abgeschlossene_arbeitszeit'],'1:30 Stunden')
            self.assertEqual(report['schichten'][0]['pause'],'0:30 Stunden')
        self.at('2026-10-31T20:00:00+00:00','kommen')
        self.at('2026-10-31T23:00:00+00:00','gehen')  # Nov midnight, never a zero-length November shift.
        self.assertEqual(self.s.summary(PERSON,'2026-11')['schichten'],[])

    def test_both_dst_changes_use_elapsed_utc_time(self):
        self.at('2026-03-29T00:30:00+00:00','kommen')
        self.at('2026-03-29T02:30:00+00:00','gehen')
        self.assertEqual(self.s.summary(PERSON,'2026-03')['abgeschlossene_arbeitszeit'],'2:00 Stunden')
        self.at('2026-10-25T00:30:00+00:00','kommen')
        self.at('2026-10-25T02:30:00+00:00','gehen')
        self.assertEqual(self.s.summary(PERSON,'2026-10')['abgeschlossene_arbeitszeit'],'2:00 Stunden')

    def test_long_open_shift_is_not_accepted_as_completed_time(self):
        self.at('2026-09-29T07:00:00+00:00','kommen')
        self.clock=datetime(2026,10,2,7,tzinfo=timezone.utc)
        report=self.s.summary(PERSON,'2026-09')
        self.assertTrue(report['pruefen']);self.assertTrue(report['schichten'][0]['offen'])
        self.assertEqual(report['abgeschlossene_arbeitszeit'],'0:00 Stunden')

    def test_concurrent_same_confirmation_is_one_event_and_successful_replay(self):
        barrier=threading.Barrier(2)
        def stamp():
            barrier.wait(timeout=4)
            return self.s.stamp(PERSON,'kommen','synthetic-concurrent',0)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(stamp) for _ in range(2)]
            results=[future.result() for future in futures]
        self.assertEqual({result['zeit'] for result in results},{'2026-09-29T07:00:00+00:00'})
        self.assertEqual(sum(result['wiederholt'] for result in results),1)
        self.assertEqual(self.s.state(PERSON)['revision'],1)

    def test_personal_action_boundary_and_retry_after_interrupted_ack(self):
        with self.s.db() as db:
            db.executescript('''CREATE TABLE assistent_aktionen(id TEXT PRIMARY KEY,actor TEXT,auftrag_id INTEGER,
                art TEXT,payload TEXT,fingerprint TEXT UNIQUE,status TEXT DEFAULT 'vorschlag',erstellt_am TEXT);''')
        self.p.now_str=lambda:self.clock.strftime('%d.%m.%Y %H:%M')
        actions=PersonalActions(self.p,None,self.s,self.s.db,lambda *args:None)
        for extra in ({'mitarbeiter_id':2},{'zeit':'2020-01-01T00:00:00Z'}):
            with self.assertRaises(ValueError):actions.propose(PERSON,{'art':'arbeitszeit','aktion':'kommen',**extra})
        row=actions.propose(PERSON,{'art':'arbeitszeit','aktion':'kommen'})
        self.assertEqual(self.s.state(PERSON)['revision'],0,'model proposal cannot stamp')
        with self.assertRaises(ValueError):actions.confirm(OTHER,row)
        with self.s.db() as db:
            db.execute("CREATE TRIGGER reject_ack BEFORE UPDATE ON assistent_aktionen BEGIN SELECT RAISE(ABORT,'synthetic lost ack'); END")
        with self.assertRaises(sqlite3.IntegrityError):actions.confirm(PERSON,row)
        self.assertEqual(self.s.state(PERSON)['revision'],1)
        with self.s.db() as db:db.execute('DROP TRIGGER reject_ack')
        result=actions.confirm(PERSON,row)
        self.assertTrue(result['wiederholt']);self.assertEqual(self.s.state(PERSON)['revision'],1)
        with self.s.db() as db:
            self.assertEqual(db.execute('SELECT status FROM assistent_aktionen WHERE id=?',(row['id'],)).fetchone()['status'],'dokumentiert')

    def test_real_postgres_adapter_primary_key_contract(self):
        names={'DbRow','PostgresCursor','PostgresConnection','convert_sqlite_sql_to_postgres','get_insert_table_name','split_sql_script'}
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text(encoding='utf-8'))
        nodes=[node for node in tree.body if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in names]
        namespace={'re':re}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'app.py (adapter only)','exec'),namespace)
        statements,path=[],self.p.path
        class Cursor:
            def __init__(self,connection):self.cursor=connection.cursor()
            def __enter__(self):return self
            def __exit__(self,*args):self.cursor.close()
            def execute(self,sql,params):
                statements.append(sql)
                self.cursor.execute(sql.replace('%s','?').replace('SERIAL PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT'),params)
                self.rows=self.cursor.fetchall() if self.cursor.description else []
                self.rowcount=self.cursor.rowcount
                self.description=[type('Column',(),{'name':col[0]}) for col in self.cursor.description] if self.cursor.description else None
            def fetchall(self):return self.rows
        class Connection:
            def __init__(self):self.connection=sqlite3.connect(path)
            def cursor(self):return Cursor(self.connection)
            def commit(self):self.connection.commit()
            def rollback(self):self.connection.rollback()
            def close(self):self.connection.close()
        with patch.object(self.p,'get_db',side_effect=lambda:namespace['PostgresConnection'](Connection())):
            self.s.init_schema()
            self.at('2026-09-29T07:00:00+00:00','kommen')
            self.at('2026-09-29T15:00:00+00:00','gehen')
            self.assertEqual(self.s.summary(PERSON)['abgeschlossene_arbeitszeit'],'8:00 Stunden')
        self.assertTrue(any('INSERT INTO mitarbeiter_zeitstatus' in sql and sql.endswith('RETURNING mitarbeiter_id') for sql in statements))


if __name__=='__main__':unittest.main()
