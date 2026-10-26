"""Isolated navigation regressions; run with python scripts/test_navigation_readmodels.py.

All imports use smoke_test's synthetic portal. Network, worker starts and writes
outside this test's scratch are denied before application import. No live data.
"""
from contextlib import closing, contextmanager
from datetime import date, timedelta
from pathlib import Path
import base64
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch


SCRATCH = Path(tempfile.mkdtemp(prefix='portal-navigation-readmodels-')).resolve()
sys.dont_write_bytecode = True
runtime = {k:v for k,v in os.environ.items()
           if k.upper() in {'PATH','SYSTEMROOT','WINDIR','SYSTEMDRIVE','COMSPEC','PATHEXT'}}
os.environ.clear()
os.environ.update(runtime)
os.environ.update({
    'TEMP':str(SCRATCH), 'TMP':str(SCRATCH), 'TMPDIR':str(SCRATCH),
    'AUTO_BACKUP_ENABLED':'0', 'AUTO_CHANGE_BACKUP_ENABLED':'0', 'AUTO_BACKUP_ON_STARTUP':'0',
    'GOOGLE_ADS_AUTO_SYNC_ENABLED':'0', 'ASSISTANT_ORDER_WORKER_ENABLED':'0',
    'ASSISTANT_ORDER_SEND_ENABLED':'0', 'PURCHASE_MONITOR_WORKER_ENABLED':'0',
    'MATERIAL_WHATSAPP_WORKER_ENABLED':'0', 'MATERIAL_WHATSAPP_ENABLED':'0',
    'MATERIAL_WHATSAPP_REPLIES_ENABLED':'0', 'MAILBOX_SEND_ENABLED':'0',
    'MAILBOX_MOVE_ENABLED':'0', 'MAILBOX_FLAGS_ENABLED':'0', 'WHATSAPP_ENABLED':'0',
    'KI_ASSISTENT_ONLINE_ENABLED':'0', 'LOCAL_SPEECH_ENABLED':'0',
    'CODEX_BRIDGE_ENABLED':'0', 'ENABLE_LOCAL_OCR':'0',
})
tempfile.tempdir = str(SCRATCH)

def inside(path):
    try:
        return Path(path).resolve().is_relative_to(SCRATCH)
    except (TypeError, ValueError):
        return False

def audit(event, args):
    if event in {'socket.connect','socket.bind','socket.getaddrinfo','subprocess.Popen','os.system'}:
        raise RuntimeError('External operation forbidden in navigation tests: '+event)
    if event == 'sqlite3.connect' and args[0] != ':memory:' and not inside(args[0]):
        raise RuntimeError('Database outside test scratch forbidden')
    if event == 'open':
        path, mode, flags = args
        writing = (mode and any(c in mode for c in 'wax+')) or (
            isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        if writing and not isinstance(path,int) and not inside(path):
            raise RuntimeError('Write outside navigation test scratch forbidden: '+str(path))
    if event in {'os.mkdir','os.remove','os.rmdir','os.rename'}:
        for path in args[:2] if event == 'os.rename' else args[:1]:
            if not inside(path):
                raise RuntimeError('Filesystem mutation outside test scratch forbidden: '+str(path))

sys.addaudithook(audit)
def deny_thread(*args, **kwargs):
    raise RuntimeError('Background worker forbidden in navigation tests')
threading.Thread.start = deny_thread

from smoke_test import portal as p  # noqa: E402
p.app.config['TESTING'] = True


@contextmanager
def selects():
    statements = []
    original = p.get_db
    def traced():
        db = original()
        db.set_trace_callback(lambda sql: statements.append(' '.join(sql.split())))
        return db
    with patch.object(p, 'get_db', traced):
        yield statements

def read_count(statements):
    return sum(sql.upper().startswith('SELECT') for sql in statements)

def insert_order(order_id, **changes):
    fields = dict(id=order_id, fahrzeug='NAVREAD Prüfwagen', kennzeichen='NAV-Ä 10',
                  kunde_name='NAVREAD Müller & Söhne', auftragsnummer=f'NAVREAD-{order_id}',
                  erstellt_am='2026-10-01 08:00:00', geaendert_am='2026-10-01 08:00:00',
                  token=f'navread-order-{order_id}', kunden_status_token=f'navread-status-{order_id}',
                  leistungsart='lackierung', status=2, transport_art='standard')
    fields.update(changes)
    with closing(p.get_db()) as db:
        db.execute('INSERT INTO auftraege ('+','.join(fields)+') VALUES ('+','.join('?' for _ in fields)+')',
                   tuple(fields.values()))
        db.commit()

def admin_client():
    client = p.app.test_client()
    with client.session_transaction() as sess:
        sess['admin'] = True
    return client


class NavigationReadModelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        today = date.today()
        df = lambda d: d.strftime(p.DATE_FMT)
        variants = [
            dict(kennzeichen='NAV-Ä 10', annahme_datum=df(today), annahme_uhrzeit='08:30'),
            dict(kennzeichen='NAV-A 100', archiviert=1, status=5, fertig_datum=df(today-timedelta(days=2))),
            dict(kennzeichen='NAV-A 101', angebotsphase=1, angebot_abgesendet=1, status=1,
                 transport_art='hol_und_bring', annahme_datum=df(today+timedelta(days=1))),
            dict(kennzeichen='NAV-A 102', transport_art='hol_und_bring',
                 annahme_datum=df(today-timedelta(days=1)), abholtermin=df(today+timedelta(days=2))),
            dict(kennzeichen='NAV-A 103', transport_art='unknown', annahme_datum='bad-date',
                 leistungsart='reifenservice', schaden_aufnahme_json='{"anliegen":"reifenservice"}'),
            dict(kennzeichen='NAV-A 104', archiviert=1, status=4,
                 fertig_datum=df(today-timedelta(days=25)), leistungsart='mechanik'),
            dict(kennzeichen='NAV-A 105', archiviert=None, fahrzeug_abholbereit=1,
                 status=4, fertig_datum=df(today), quelle='versicherung'),
            dict(kennzeichen='NAV-A 106', status=1, quelle='',
                 schaden_aufnahme_json='{"anliegen":"lackanfrage"}', kunde_name='NAVREAD Groß-Öl'),
        ]
        cls.order_ids=[]
        for index, variant in enumerate(variants):
            order_id=62000+index
            insert_order(order_id, **variant)
            cls.order_ids.append(order_id)

    def test_search_matches_full_models_for_special_characters_offers_and_planning_order(self):
        def full_projection(*, include_angebote=False, planning=False):
            return p.list_auftraege(include_archived=True, include_angebote=include_angebote)
        with p.app.test_request_context('/admin/cockpit/auftraege-suche'):
            for query in ('NAV','NAVÄ10','NAV-A','Müller','Groß-Öl','NAVREAD','Reifenservice','no-result','N'):
                for limit in (1,8,50):
                    with self.subTest(query=query,limit=limit):
                        actual=p.list_admin_auftrag_suche(query,limit=limit)
                        with patch.object(p,'list_auftrag_navigation_rows',full_projection):
                            expected=p.list_admin_auftrag_suche(query,limit=limit)
                        self.assertEqual(actual,expected)
            offer_results=p.list_admin_auftrag_suche('NAV-A 101',limit=50)
            self.assertIn(62002,[row['id'] for row in offer_results])

    def test_statistics_matches_full_models_with_archive_dates_and_service_types(self):
        with p.app.test_request_context('/admin/lackier-statistik'):
            full=p.list_auftraege(include_archived=True)
            projected=p.list_auftrag_navigation_rows()
            self.assertEqual(p.build_lackier_statistik(projected),p.build_lackier_statistik(full))
            rows={row['id']:row for row in projected}
            self.assertNotIn(62002,rows)  # offers stay excluded from statistics
            self.assertNotIn('kunden_status_share',rows[62000])
            self.assertNotIn('werkstatt_preisvorschlag',rows[62000])

    def test_json_routes_have_constant_query_budgets_and_never_hydrate_full_orders(self):
        client=admin_client()
        for path,budget in (('/admin/cockpit/auftraege-suche?q=NAV',1),('/admin/lackier-statistik',2)):
            with self.subTest(path=path):
                with patch.object(p,'row_to_auftrag',side_effect=AssertionError('full hydration forbidden')):
                    with selects() as statements:
                        response=client.get(path)
                self.assertEqual(response.status_code,200)
                self.assertEqual(read_count(statements),budget,statements)
                self.assertFalse(any('serviceprozesse' in sql.lower() for sql in statements))


class ServiceBulkTests(unittest.TestCase):
    def setUp(self):
        self.path=SCRATCH / ('service-'+self._testMethodName+'.db')
        self.path.unlink(missing_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE serviceprozesse(auftrag_id INTEGER,lead_id INTEGER,fahrzeug_id TEXT)')
            db.executemany('INSERT INTO serviceprozesse VALUES(?,?,?)',[(7,8,'order-car'),(9,7,'lead-car')])
            db.commit()
        self.statements=[]
        def connect():
            db=sqlite3.connect(self.path)
            db.row_factory=sqlite3.Row
            db.set_trace_callback(self.statements.append)
            return db
        patcher=patch.object(p,'get_db',connect)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher=patch.object(p,'service_process',object())
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_bulk_chunks_positive_negative_keys_and_repeat_preload(self):
        with p.app.test_request_context('/admin'):
            p.preload_service_status_paths(list(range(1,1202))+[7,7,None])
            self.assertEqual(read_count(self.statements),3)
            self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/order-car')
            self.assertEqual(p.service_status_path(order_id=99),'')
            p.preload_service_status_paths([7,99])
            self.assertEqual(read_count(self.statements),3)
            self.assertEqual(p.service_status_path(lead_id=7),'/service/fahrzeug/lead-car')
            self.assertEqual(read_count(self.statements),4)
        bulk=[sql for sql in self.statements if ' IN (' in sql]
        self.assertEqual([sql.split(' IN (',1)[1].count(',')+1 for sql in bulk],[500,500,201])

    def test_next_get_refreshes_changed_positive_and_previously_missing_paths(self):
        with p.app.test_request_context('/admin'):
            p.preload_service_status_paths([7,99])
            self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/order-car')
            self.assertEqual(p.service_status_path(order_id=99),'')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE serviceprozesse SET fahrzeug_id='changed' WHERE auftrag_id=7")
            db.execute("INSERT INTO serviceprozesse VALUES(99,99,'new')")
            db.commit()
        with p.app.test_request_context('/admin'):
            p.preload_service_status_paths([7,99])
            self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/changed')
            self.assertEqual(p.service_status_path(order_id=99),'/service/fahrzeug/new')
        self.assertEqual(read_count(self.statements),2)

    def test_post_and_background_preloads_do_not_cache_mutable_status_paths(self):
        with p.app.test_request_context('/admin/auftrag/7',method='POST'):
            p.preload_service_status_paths([7])
            self.assertEqual(read_count(self.statements),0)
            self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/order-car')
            with closing(sqlite3.connect(self.path)) as db:
                db.execute("UPDATE serviceprozesse SET fahrzeug_id='post-fresh' WHERE auftrag_id=7")
                db.commit()
            self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/post-fresh')
        p.preload_service_status_paths([7])
        self.assertEqual(read_count(self.statements),2)
        self.assertEqual(p.service_status_path(order_id=7),'/service/fahrzeug/post-fresh')
        self.assertEqual(read_count(self.statements),3)


class FileReadModelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.order_id=63000
        insert_order(cls.order_id)
        cls.payloads={}
        cls.ids={}
        with closing(p.get_db()) as db:
            complaint=db.execute('''INSERT INTO reklamationen(auftrag_id,quelle,meldung,erstellt_am)
                                    VALUES(?,'intern','synthetic complaint','2026-10-10 08:00')''',(cls.order_id,))
            cls.complaint_id=complaint.lastrowid
            # Predicates intentionally cover SQL NULL/empty distinctions before normalization.
            variants=[
                ('local','standard','pruefen',None),('db-only','standard',None,None),
                ('missing','standard','pruefen',None),('null-category',None,'pruefen',None),
                ('empty-category','','pruefen',None),('archive-purpose','standard','ablage',None),
                ('empty-purpose','standard','',None),('complaint','standard','pruefen',cls.complaint_id),
                ('finish-photo','fertigbild','pruefen',None),('db-only-second','standard','pruefen',None),
                ('complaint-second','standard','pruefen',cls.complaint_id),
            ]
            for index,(name,category,purpose,complaint_id) in enumerate(variants):
                stored=f'navread-{name}.pdf'
                ai=json.dumps({'fahrzeug':'NAVREAD Original','kennzeichen':'NAV-Ä 10','analyse_pruefen':True})
                cursor=db.execute('''INSERT INTO dateien
                    (auftrag_id,original_name,stored_name,mime_type,kategorie,dokument_zweck,reklamation_id,
                     extrahierter_text,analyse_json,hochgeladen_am)
                    VALUES(?,?,?,'application/pdf',?,?,?,?,?,?)''',
                    (cls.order_id,stored,stored,category,purpose,complaint_id,
                     'Kennzeichen: NAV-Ä 10',ai,
                     f'2026-10-{11 if name=="complaint-second" else 10+index:02d} 08:00:00'))
                file_id=cursor.lastrowid
                cls.ids[name]=file_id
                payload=b'%PDF-1.4 synthetic '+name.encode('ascii')+b'\n%%EOF'
                cls.payloads[name]=payload
                if name=='local':
                    (p.UPLOAD_DIR/stored).write_bytes(payload)
                if name.startswith('db-only'):
                    db.execute('''INSERT INTO datei_backups(datei_id,file_base64,file_sha256,size,erstellt_am)
                                  VALUES(?,?,?,?,?)''',(file_id,base64.b64encode(payload).decode('ascii'),
                                                       hashlib.sha256(payload).hexdigest(),len(payload),'2026-10-10'))
            db.commit()

    def test_list_files_uses_one_query_and_preserves_local_backup_missing_availability(self):
        with selects() as statements:
            with patch.object(p,'datei_backup_exists',side_effect=AssertionError('per-file SELECT forbidden')):
                rows=p.list_dateien(self.order_id)
        self.assertEqual(read_count(statements),1,statements)
        self.assertIn('EXISTS',statements[0].upper())
        self.assertNotIn('file_base64',statements[0].lower())
        by_id={row['id']:row for row in rows}
        for name in ('local','db-only','db-only-second'):
            self.assertTrue(by_id[self.ids[name]]['original_available'],name)
        self.assertFalse(by_id[self.ids['missing']]['original_available'])
        self.assertTrue(all('_backup_available' not in row for row in rows))

    def test_get_and_grouped_files_keep_hints_private_without_fallback_queries(self):
        with patch.object(p,'datei_backup_exists',side_effect=AssertionError('per-file SELECT forbidden')):
            with selects() as statements:
                grouped=p.list_dateien_for_auftraege([self.order_id])
            self.assertEqual(read_count(statements),1)
            self.assertEqual(len(grouped[self.order_id]),len(self.ids))
            with selects() as statements:
                item=p.get_datei(self.ids['db-only'])
            self.assertEqual(read_count(statements),1)
            self.assertTrue(item['original_available'])
            self.assertNotIn('_backup_available',item)

    def test_preloaded_document_review_exactly_matches_sql_null_category_purpose_and_order(self):
        with p.app.test_request_context('/admin/auftrag/63000'):
            order=p.get_auftrag(self.order_id)
            loaded=p.list_dateien(self.order_id)
            for audience in (None,'admin','kunde'):
                with self.subTest(audience=audience):
                    with selects() as legacy_statements:
                        expected=p.list_document_review_items(self.order_id,order,audience)
                    with selects() as statements:
                        actual=p.list_document_review_items(self.order_id,order,audience,dateien=loaded)
                    self.assertEqual(actual,expected)
                    # Document parsing may still read partner-name reference data.
                    # Reusing files must eliminate the document SELECT specifically.
                    self.assertEqual(read_count(statements),read_count(legacy_statements)-1,statements)
                    self.assertFalse(any(' from dateien' in sql.lower() for sql in statements))
            reviews=p.list_document_review_items(self.order_id,order,dateien=loaded)
            ids=[row['datei_id'] for row in reviews]
            self.assertEqual(ids,sorted(ids,reverse=True))
            self.assertTrue(ids,'fixture must exercise returned review items')
            for name in ('null-category','empty-category','archive-purpose','empty-purpose','complaint','finish-photo'):
                self.assertNotIn(self.ids[name],ids,name)
            self.assertIn(self.ids['db-only'],ids)  # purpose=NULL means pruefen

    def test_preloaded_complaints_keep_attachment_order_and_avoid_duplicate_file_reads(self):
        loaded=p.list_dateien(self.order_id)
        expected=p.list_reklamationen(self.order_id)
        with patch.object(p,'list_dateien_by_reklamation',side_effect=AssertionError('duplicate file read forbidden')):
            with selects() as statements:
                actual=p.list_reklamationen(self.order_id,dateien=loaded)
        self.assertEqual(actual,expected)
        self.assertEqual(read_count(statements),1)
        self.assertEqual([item['id'] for item in actual[0]['dateien']],
                         [self.ids['complaint'],self.ids['complaint-second']])

    def test_original_downloads_return_exact_local_and_database_only_bytes(self):
        client=admin_client()
        for name in ('local','db-only','db-only-second'):
            with self.subTest(name=name):
                response=client.get(f'/admin/datei/{self.ids[name]}/download')
                self.assertEqual(response.status_code,200)
                self.assertEqual(response.data,self.payloads[name])
                self.assertIn('attachment',response.headers.get('Content-Disposition',''))
                response.close()
        for name in ('db-only','db-only-second'):
            self.assertFalse((p.UPLOAD_DIR/f'navread-{name}.pdf').exists())
        missing=client.get(f'/admin/datei/{self.ids["missing"]}/download')
        self.assertEqual(missing.status_code,404)


if __name__=='__main__':
    unittest.main(verbosity=2)
