"""Tenant/login, real camera-style uploads, immutable originals and restore offline."""
from contextlib import closing
from copy import deepcopy
from datetime import date
from io import BytesIO
import csv
import json
import sqlite3
import time
import unittest
from unittest.mock import patch
from uuid import uuid4
import zipfile
from urllib.parse import urlencode

import fitz
from PIL import Image
from werkzeug.datastructures import FileStorage
import smoke_test as isolated
import werkstatt_flottenportal as module

p=isolated.portal


def png():
    out=BytesIO()
    Image.new('RGB',(160,210),'#bed7e4').save(out,'PNG')
    return out.getvalue()


def pdf():
    with fitz.open() as doc:
        doc.new_page().insert_text((40,50),'Synthetic repair receipt - offline test')
        return doc.tobytes()


class FleetPortalTests(unittest.TestCase):
    def setUp(self):
        self.mode=patch.dict(p.app.config,TESTING=True,FLOTTENPORTAL_ENABLED=True)
        self.mode.start();self.addCleanup(self.mode.stop)
        self.fleet=p.kundenflotten;self.service=p.flottenportal
        self.company=self.fleet.create_company({'name':'Example firm '+uuid4().hex,'konditionen':'INTERNAL-CONDITIONS-NOT-PUBLIC'})
        self.other=self.fleet.create_company({'name':'Other firm '+uuid4().hex})
        self.vehicle=self.car(self.company,'MOS FP 1')
        self.other_vehicle=self.car(self.other,'MOS FP 2')
        self.user='firma-'+uuid4().hex[:12]
        self.password='Nur-Lokal-2026-Test'
        self.service.configure_access(self.company,self.credentials())
        self.customer=p.app.test_client();self.admin=p.app.test_client()
        with self.admin.session_transaction() as session:session.update(admin=True,csrf_token='test-csrf')
        self.login()

    def car(self,company,plate):
        return self.fleet.create_vehicle(company,{'kennzeichen':plate,'modell':'Synthetic fleet car','km':'15000','km_datum':date.today().isoformat()})

    def credentials(self):return dict(benutzername=self.user,passwort=self.password,passwort_wiederholen=self.password)

    def sql(self,text,args=()):
        with closing(p.get_db()) as db:
            rows=db.execute(text,args).fetchall();db.commit();return rows

    def csrf(self,client=None):
        with (client or self.customer).session_transaction() as session:return session.get('csrf_token')

    def login(self,client=None,**changes):
        client=client or self.customer
        client.get('/flotte/anmelden')
        data=dict(self.credentials(),csrf_token=self.csrf(client))
        data.update(changes)
        return client.post('/flotte/anmelden',data=data)

    def payload(self,**changes):
        values=dict(art='tank',titel='Synthetic fuel receipt',belegdatum=date.today().isoformat(),betrag='68,40',request_id=uuid4().hex)
        values.update(changes);return values

    def store(self,company=None,vehicle=None,internal=False,visible='0',raw=None,name='Beleg.png',**changes):
        data=self.payload(sichtbar=visible,**changes)
        return self.service.upload(company or self.company,vehicle or self.vehicle,data,[FileStorage(stream=BytesIO(raw if raw is not None else png()),filename=name)],internal=internal)

    def upload(self,vehicle=None,client=None,raw=None,name='tank.png',camera=False,**changes):
        client=client or self.customer
        data=self.payload(csrf_token=self.csrf(client))
        data.update(changes)
        data['kamera' if camera else 'datei']=(BytesIO(raw if raw is not None else png()),name)
        return client.post(f'/flotte/fahrzeug/{vehicle or self.vehicle}/beleg',data=data)

    def test_separate_login_rotates_identity_csrf_and_remembered_privileges(self):
        client=p.app.test_client()
        with client.session_transaction() as session:session.update(admin=True,assistent_mid=7,autohaus_id=4,csrf_token='old-token')
        self.assertEqual(client.get('/flotte').status_code,302)
        result=self.login(client)
        self.assertEqual(result.status_code,303)
        with client.session_transaction() as session:
            self.assertNotIn('admin',session);self.assertNotIn('assistent_mid',session);self.assertNotIn('autohaus_id',session)
            self.assertNotEqual(session['csrf_token'],'old-token')
            self.assertEqual(session['flotte_zugang'],self.service.access(self.company)['id'])
        cookie=' '.join(result.headers.getlist('Set-Cookie'))
        self.assertIn(p.remember_login_cookie_name('admin'),cookie)
        self.assertIn(p.remember_login_cookie_name('partner'),cookie)
        self.assertIn('scrypt:',self.sql('SELECT passwort_hash FROM kundenflotten_zugaenge WHERE firma_id=?',(self.company,))[0][0])

    def test_fleet_list_and_vehicle_documents_are_tenant_scoped(self):
        page=self.customer.get('/flotte')
        self.assertEqual(page.status_code,200)
        self.assertIn('MOS FP 1',page.text);self.assertNotIn('MOS FP 2',page.text)
        self.assertNotIn('INTERNAL-CONDITIONS-NOT-PUBLIC',page.text)
        self.assertEqual(self.customer.get(f'/flotte/fahrzeug/{self.other_vehicle}').status_code,404)
        other_doc=self.store(self.other,self.other_vehicle)
        for vid in (self.vehicle,self.other_vehicle):
            self.assertEqual(self.customer.get(f'/flotte/fahrzeug/{vid}/beleg/{other_doc}').status_code,404)
        self.assertEqual(self.upload(vehicle=self.other_vehicle).status_code,404)
        self.assertEqual(self.service.documents(self.other,self.other_vehicle)[0]['id'],other_doc)

    def test_camera_and_pdf_uploads_persist_exact_originals_without_ocr_or_send(self):
        with patch.object(p,'save_uploads',side_effect=AssertionError('No OCR/order mutation')),patch.object(p,'fuehre_auftrag_status_wechsel_aus',side_effect=AssertionError('No mail')):
            for raw,name,camera in [(png(),'Kamera.png',True),(pdf(),'Rechnung.pdf',False)]:
                response=self.upload(raw=raw,name=name,camera=camera)
                self.assertEqual(response.status_code,303)
                doc=next(d for d in self.service.documents(self.company,self.vehicle) if d['original_name']==name)
                file=self.customer.get(f'/flotte/fahrzeug/{self.vehicle}/beleg/{doc["id"]}')
                self.assertEqual(file.status_code,200)
                self.assertEqual(file.data,raw)
                self.assertIn('attachment',file.headers['Content-Disposition'])
                self.assertEqual(file.headers['X-Content-Type-Options'],'nosniff')
                self.assertIn('no-store',file.headers['Cache-Control'])
                self.assertEqual(doc['betrag_cent'],6840)

    def test_admin_repair_receipt_visibility_is_explicit_and_company_cannot_spoof_origin(self):
        private=self.store(internal=True,art='reparatur',raw=pdf(),name='Intern.pdf')
        public=self.store(internal=True,visible='1',art='reparatur',raw=pdf(),name='Reparatur.pdf')
        self.assertEqual([d['id'] for d in self.service.documents(self.company,self.vehicle)],[public])
        self.assertEqual(self.customer.get(f'/flotte/fahrzeug/{self.vehicle}/beleg/{private}').status_code,404)
        admin_url=f'/admin/flottenportal/firma/{self.company}/fahrzeug/{self.vehicle}/beleg/{private}'
        self.assertEqual(self.admin.get(admin_url).status_code,200)
        self.assertEqual(self.customer.get(admin_url).status_code,302)
        self.upload(quelle='werkstatt',sichtbar='0')
        uploaded=[d for d in self.service.documents(self.company,self.vehicle) if d['quelle']=='firma']
        self.assertEqual(len(uploaded),1);self.assertEqual(uploaded[0]['sichtbar'],1)

    def test_all_post_actions_require_csrf_and_replay_is_safe(self):
        token=self.csrf()
        self.assertEqual(self.upload(csrf_token='wrong').status_code,400)
        self.assertEqual(self.customer.post('/flotte/abmelden').status_code,400)
        rid=uuid4().hex
        for _ in range(2):self.assertEqual(self.upload(request_id=rid).status_code,303)
        self.assertEqual(len(self.service.documents(self.company,self.vehicle)),1)
        response=self.upload(request_id=rid,raw=pdf(),name='changed.pdf')
        self.assertEqual(response.status_code,303)
        self.assertEqual(len(self.service.documents(self.company,self.vehicle)),1)
        self.assertEqual(self.customer.post('/flotte/abmelden',data={'csrf_token':token}).status_code,303)
        self.assertEqual(self.customer.get('/flotte').status_code,302)

    def test_invalid_empty_truncated_mismatched_and_oversize_documents_rejected(self):
        for raw,name in [(b'','empty.png'),(b'<html>evil</html>','fake.pdf'),(png(),'wrong.jpg'),(b'%PDF-1.4 broken','broken.pdf'),(b'\x89PNG\r\n\x1a\n','broken.png'),(b'x'*(8*1024*1024+1),'large.pdf'),(b'x','evil.svg')]:
            with self.subTest(name=name),self.assertRaises(ValueError):self.store(raw=raw,name=name)
        self.assertEqual(self.service.documents(self.company,self.vehicle),[])
        for changes in ({'betrag':'-1'},{'betrag':'0.001'},{'belegdatum':'2100-01-01'},{'request_id':'bad'}):
            with self.assertRaises(ValueError):self.store(**changes)

    def test_revocation_password_rotation_expiry_and_feature_flag_deny_stale_sessions(self):
        self.assertEqual(self.admin.get(f'/admin/flottenportal/firma/{self.company}/zugang').status_code,200)
        self.service.configure_access(self.company,{'aktion':'sperren'})
        self.assertEqual(self.customer.get('/flotte').status_code,302)
        self.service.configure_access(self.company,self.credentials())
        self.login()
        self.service.configure_access(self.company,self.credentials())
        self.assertEqual(self.upload().status_code,403)
        self.login()
        with self.customer.session_transaction() as session:session['flotte_login_am']=time.time()-9*3600
        self.assertEqual(self.customer.get('/flotte').status_code,302)
        with patch.dict(p.app.config,FLOTTENPORTAL_ENABLED=False):
            self.assertEqual(self.customer.get('/flotte/anmelden').status_code,404)
            self.assertEqual(self.admin.get(f'/admin/flottenportal/firma/{self.company}/zugang').status_code,404)

    def test_login_throttle_and_no_csrf_password_or_weak_setup(self):
        anonymous=p.app.test_client()
        self.assertEqual(anonymous.post('/flotte/anmelden',data=self.credentials()).status_code,400)
        self.assertEqual(self.login(anonymous,passwort='wrong').status_code,401)
        with patch.object(p,'login_rate_limit_status',return_value=(True,30)):
            self.assertEqual(self.login(anonymous).status_code,429)
        with self.assertRaises(ValueError):self.service.configure_access(self.company,dict(self.credentials(),passwort='short',passwort_wiederholen='short'))
        with self.assertRaises(ValueError):self.service.configure_access(self.other,self.credentials())

    def test_integrity_tampering_blocks_original_download(self):
        doc=self.store()
        self.sql("UPDATE kundenflotten_belege SET original_base64='eA==' WHERE id=?",(doc,))
        self.assertEqual(self.customer.get(f'/flotte/fahrzeug/{self.vehicle}/beleg/{doc}').status_code,409)
        # Remove this intentionally corrupt test row so later backup tests only see valid originals.
        self.sql('DELETE FROM kundenflotten_belege WHERE id=?',(doc,))

    def test_real_backup_roundtrip_and_missing_original_rejected(self):
        doc=self.store()
        package=p.create_backup_package(reason='synthetic-fleet-documents')
        with zipfile.ZipFile(package) as archive:
            export=json.loads(archive.read('backup.json'))
            refs=p.backup_binary_reference_map(export)
            self.assertIn(('kundenflotten_belege',doc,'original_base64'),refs)
            broken=deepcopy(export)
            broken['binary_blobs']=[r for r in broken.get('binary_blobs',[]) if r.get('table')!='kundenflotten_belege']
            # Directly test missing field references regardless of manifest representation.
            missing={k:v for k,v in refs.items() if k[0]!='kundenflotten_belege'}
            with self.assertRaises(ValueError):p.validate_backup_binary_reference_completeness(export,missing)
            self.sql('DELETE FROM kundenflotten_belege WHERE id=?',(doc,))
            p.import_backup_json_rows_into_current_database(export,archive,archive.namelist())
            self.assertEqual(self.service.original(self.company,self.vehicle,doc)[1],png())
            source=isolated.TEMP_DIR/('fleet-doc-'+uuid4().hex+'.db')
            source.write_bytes(archive.read('auftraege.db'))
            p.hydrate_imported_sqlite_backup_blobs(source,archive,archive.namelist(),export)
            p.import_sqlite_rows_into_current_database(source)
            self.assertEqual(self.service.original(self.company,self.vehicle,doc)[1],png())

    def test_older_backup_cannot_erase_new_receipt_or_reenable_disabled_login(self):
        package=p.create_backup_package(reason='synthetic-fleet-before-new-receipt')
        doc=self.store()
        with zipfile.ZipFile(package) as archive:
            export=json.loads(archive.read('backup.json'))
            with self.assertRaisesRegex(ValueError,'Flottenbelege'):p.import_backup_json_rows_into_current_database(export,archive,archive.namelist())
            self.assertEqual(self.service.original(self.company,self.vehicle,doc)[1],png())
        package=p.create_backup_package(reason='synthetic-fleet-before-revocation')
        self.service.configure_access(self.company,{'aktion':'sperren'})
        with zipfile.ZipFile(package) as archive:
            export=json.loads(archive.read('backup.json'))
            with self.assertRaisesRegex(ValueError,'Zugangsstatus'):p.import_backup_json_rows_into_current_database(export,archive,archive.namelist())
        self.assertFalse(self.service.access(self.company)['aktiv'])

    def test_backup_cannot_move_vehicle_and_existing_documents_to_another_company(self):
        self.store()
        package=p.create_backup_package(reason='synthetic-owner-test')
        with zipfile.ZipFile(package) as archive:
            export=json.loads(archive.read('backup.json'))
            car=next(row for row in export['tables']['kundenflotten_fahrzeuge'] if row['id']==self.vehicle)
            car['firma_id']=self.other
            with self.assertRaisesRegex(ValueError,'Flottenbelege'):
                p.import_backup_json_rows_into_current_database(export,archive,archive.namelist())
        self.assertEqual(self.sql('SELECT firma_id FROM kundenflotten_fahrzeuge WHERE id=?',(self.vehicle,))[0][0],self.company)

    def test_upload_rechecks_revoked_identity_under_restore_lock(self):
        with self.assertRaises(PermissionError):
            self.service.upload(self.company,self.vehicle,self.payload(),[FileStorage(stream=BytesIO(png()),filename='Test.png')],authorize=lambda:False)
        self.assertEqual(self.service.documents(self.company,self.vehicle),[])

    def test_missing_sqlite_backup_source_is_never_created(self):
        source=isolated.TEMP_DIR/(uuid4().hex+'-missing.db')
        with self.assertRaises(sqlite3.OperationalError):p.ensure_kundenflotten_state_for_import(imported_db=source)
        self.assertFalse(source.exists())

    def test_employee_and_order_sessions_do_not_authorize_fleet_access(self):
        client=p.app.test_client()
        with client.session_transaction() as session:
            session.update(assistent_mid=1,assistent_version=1,kunden_status_token=uuid4().hex,csrf_token='test-csrf')
        self.assertEqual(client.get('/flotte').status_code,302)
        self.assertEqual(self.upload(client=client).status_code,403)

    def export(self,kind='csv',vehicle=None,**filters):
        return self.customer.get(f'/flotte/fahrzeug/{vehicle or self.vehicle}/belege/export/{kind}?'+urlencode(filters))

    def csv_rows(self,raw):
        self.assertTrue(raw.startswith(b'\xef\xbb\xbf'))
        return list(csv.DictReader(raw.decode('utf-8-sig').splitlines(),delimiter=';'))

    def test_filters_csv_and_zip_share_exact_visible_selection_and_original_bytes(self):
        raw=pdf()
        match=self.store(titel='Ölservice 100%_ Rechnung',art='rechnung',belegdatum='2026-01-15',raw=raw,name='Original.pdf')
        self.store(titel='Ölservice 100%_ Rechnung',art='tank',belegdatum='2026-01-15')
        self.store(titel='Ölservice 100%_ Rechnung',art='rechnung',belegdatum='2026-01-14')
        self.store(internal=True,titel='Ölservice 100%_ Rechnung',art='rechnung',belegdatum='2026-01-15')
        filters=dict(q='ÖLSERVICE 100%_',belegart='rechnung',von='2026-01-15',bis='2026-01-15')
        html=self.customer.get(f'/flotte/fahrzeug/{self.vehicle}?'+urlencode(filters))
        self.assertEqual(html.status_code,200)
        self.assertIn('1 Beleg</span>',html.text);self.assertIn('Original.pdf',html.text)
        self.assertNotIn('NUR INTERN',html.text)
        response=self.export(**filters)
        self.assertEqual(response.status_code,200)
        rows=self.csv_rows(response.data)
        self.assertEqual([r['Beleg-ID'] for r in rows],[str(match)])
        self.assertEqual(rows[0]['Betrag brutto EUR'],'68,40')
        self.assertEqual(rows[0]['Kennzeichen'],'MOS FP 1')
        self.assertIn('MOS_FP_1',response.headers['Content-Disposition'])
        result=self.export('zip',**filters)
        self.assertEqual(result.status_code,200)
        with zipfile.ZipFile(BytesIO(result.data)) as archive:
            manifest=self.csv_rows(archive.read('Beleguebersicht.csv'))
            self.assertEqual([r['Beleg-ID'] for r in manifest],[str(match)])
            self.assertEqual(archive.read(manifest[0]['Datei im ZIP']),raw)
            self.assertEqual(len(archive.namelist()),2)
        for response in (html,response,result): self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertEqual(self.csv_rows(self.export(q='nonexistent').data),[])
        with zipfile.ZipFile(BytesIO(self.export('zip',q='nonexistent').data)) as archive:
            self.assertEqual(archive.namelist(),['Beleguebersicht.csv'])

    def test_invalid_filters_never_broaden_list_or_exports(self):
        self.store()
        for query in ('belegart=invalid','von=2026-02-30','von=20260115','von=2026-W01-1','von=2026-02-01&bis=2026-01-01','q='+('x'*121),'q=a&q=b'):
            for path in (f'/flotte/fahrzeug/{self.vehicle}',f'/flotte/fahrzeug/{self.vehicle}/belege/export/csv',f'/flotte/fahrzeug/{self.vehicle}/belege/export/zip'):
                with self.subTest(path=path,query=query):self.assertEqual(self.customer.get(path+'?'+query).status_code,400)
        self.assertEqual(self.export('exe').status_code,404)

    def test_csv_formula_injection_and_zip_paths_are_safe_without_changing_originals(self):
        ids=[]
        for title in ('=HYPERLINK("http://example.invalid")','+1','-1','@SUM(1)','  =1'):
            ids.append(self.store(titel=title,name='same.png'))
        self.sql('UPDATE kundenflotten_belege SET original_name=? WHERE id=?',('../../=evil.png',ids[0]))
        response=self.export()
        rows=self.csv_rows(response.data)
        self.assertTrue(all(row['Bezeichnung'].startswith("'") for row in rows))
        result=self.export('zip')
        with zipfile.ZipFile(BytesIO(result.data)) as archive:
            self.assertEqual(len(archive.namelist()),len(ids)+1)
            self.assertEqual(len(set(archive.namelist())),len(archive.namelist()))
            for name in archive.namelist():
                self.assertNotIn('/',name);self.assertNotIn('\\',name);self.assertNotIn('..',name)
                if name.endswith('.png'):self.assertEqual(archive.read(name),png())
        # Control prefix can only occur in old/imported metadata, still escaped in CSV.
        self.sql('UPDATE kundenflotten_belege SET titel=? WHERE id=?',('\t=1',ids[0]))
        self.assertIn(b"'\t=1",self.export().data)

    def test_export_limits_and_corrupt_original_abort_whole_archive(self):
        first=self.store();self.store()
        with patch.object(module,'EXPORT_MAX_DOCUMENTS',1):
            self.assertEqual(self.export().status_code,413);self.assertEqual(self.export('zip').status_code,413)
        with patch.object(module,'EXPORT_MAX_BYTES',len(png())):
            self.assertEqual(self.export('zip').status_code,413)
        self.sql("UPDATE kundenflotten_belege SET original_base64='eA==' WHERE id=?",(first,))
        try:
            response=self.export('zip')
            self.assertEqual(response.status_code,409);self.assertFalse(response.data.startswith(b'PK'))
        finally:self.sql('DELETE FROM kundenflotten_belege WHERE id=?',(first,))

    def test_exports_status_and_admin_cookies_never_cross_tenants(self):
        self.store(self.other,self.other_vehicle)
        for kind in ('csv','zip'):self.assertEqual(self.export(kind,vehicle=self.other_vehicle).status_code,404)
        self.assertEqual(self.customer.get(f'/flotte/fahrzeug/{self.other_vehicle}/status').status_code,404)
        self.assertEqual(self.admin.get('/flotte/status').status_code,403)
        self.assertEqual(self.admin.get(f'/flotte/fahrzeug/{self.vehicle}/belege/export/csv').status_code,403)
        payload=self.customer.get('/flotte/status')
        self.assertEqual(payload.status_code,200)
        self.assertEqual([v['id'] for v in payload.json['vehicles']],[self.vehicle])
        self.assertNotIn('INTERNAL-CONDITIONS-NOT-PUBLIC',payload.text)
        self.assertNotIn('MOS FP 2',payload.text)
        self.assertIn('no-store',payload.headers['Cache-Control'])

    def test_revocation_during_export_and_status_build_is_rechecked(self):
        self.store()
        original=self.service.original
        def revoke(*args,**kwargs):
            result=original(*args,**kwargs)
            self.service.configure_access(self.company,{'aktion':'sperren'})
            return result
        with patch.object(self.service,'original',side_effect=revoke):self.assertEqual(self.export('zip').status_code,403)
        self.service.configure_access(self.company,self.credentials());self.login()
        original_company=self.service.safe_company
        def revoke_company(*args):
            result=original_company(*args)
            self.service.configure_access(self.company,{'aktion':'sperren'})
            return result
        with patch.object(self.service,'safe_company',side_effect=revoke_company):self.assertEqual(self.customer.get('/flotte/status').status_code,403)

    def test_status_json_uses_real_order_and_hides_removed_case_without_second_state(self):
        order=self.sql("INSERT INTO auftraege(fahrzeug,kennzeichen,status,archiviert,kunden_status_token,notiz_intern,erstellt_am,geaendert_am) VALUES('Synthetic','MOS FP 1',2,0,?,'INTERNAL-NOTE',?,?) RETURNING id",(uuid4().hex,p.now_str(),p.now_str()))[0][0]
        self.fleet.change_vehicle(self.company,self.vehicle,'auftrag',dict(auftrag_id=str(order),art='service',bestaetigt='1',request_id=uuid4().hex))
        path=f'/flotte/fahrzeug/{self.vehicle}/status'
        for status in (2,3,4):
            self.sql('UPDATE auftraege SET status=? WHERE id=?',(status,order))
            response=self.customer.get(path)
            self.assertEqual(response.status_code,200)
            case=response.json['cases'][0]
            self.assertEqual(case['status'],status)
            self.assertEqual(set(case),{'id','title','vehicle','plate','status','label','description','updated','stage','steps'})
            self.assertNotIn('INTERNAL-NOTE',response.text)
            self.assertEqual(response.json['view'],self.customer.get('/flotte/status').json['view'])
        self.sql('UPDATE auftraege SET archiviert=1 WHERE id=?',(order,))
        self.assertEqual(self.customer.get(path).json['cases'],[])
        self.sql('UPDATE kundenflotten_fahrzeuge SET firma_id=? WHERE id=?',(self.other,self.vehicle))
        self.assertEqual(self.customer.get(path).status_code,404)
        self.assertEqual(self.customer.get('/flotte/status').json['vehicles'],[])

    def test_expired_and_rotated_access_denies_json_and_exports_with_no_redirect(self):
        paths=['/flotte/status',f'/flotte/fahrzeug/{self.vehicle}/status',f'/flotte/fahrzeug/{self.vehicle}/belege/export/csv',f'/flotte/fahrzeug/{self.vehicle}/belege/export/zip']
        for reason in ('expiry','rotation','disabled'):
            self.service.configure_access(self.company,self.credentials());self.login()
            if reason=='expiry':
                with self.customer.session_transaction() as session:session['flotte_login_am']=time.time()-9*3600
            else:self.service.configure_access(self.company,{'aktion':'sperren'} if reason=='disabled' else self.credentials())
            for path in paths:
                response=self.customer.get(path)
                self.assertEqual(response.status_code,403)
                self.assertIsNone(response.headers.get('Location'))
                self.assertEqual(response.json,{'error':'Anmeldung erforderlich.'})
        with patch.dict(p.app.config,FLOTTENPORTAL_ENABLED=False):
            for path in paths:self.assertEqual(self.customer.get(path).status_code,404)


if __name__=='__main__':unittest.main()
