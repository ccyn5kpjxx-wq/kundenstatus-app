"""Real order/customer coupling on smoke_test's disposable offline database."""
from contextlib import closing
from datetime import date
import unittest
from unittest.mock import patch
from uuid import uuid4

import smoke_test as isolated
from werkstatt_kundenflotten import case_view, KINDS

portal = isolated.portal


class CustomerCaseTests(unittest.TestCase):
    def setUp(self):
        portal.app.config['TESTING'] = True
        self.service = portal.kundenflotten
        self.company = self.service.create_company({'name':'Synthetic customer case ' + uuid4().hex})
        self.vehicle = self.service.create_vehicle(self.company, {'modell':'Synthetic service car','kennzeichen':'MOS TEST 159','km':'12000','km_datum':date.today().isoformat()})
        self.token = uuid4().hex
        self.order = self.new_order(token=self.token)
        self.customer = portal.app.test_client()
        self.admin = portal.app.test_client()
        with self.admin.session_transaction() as session:
            session.update(admin=True,csrf_token='test-csrf')
        self.path = f'/admin/kundenflotten/firma/{self.company}/fahrzeug/{self.vehicle}'

    def new_order(self, token=None, plate='MOS TEST 159'):
        with closing(portal.get_db()) as db:
            number=db.execute("""INSERT INTO auftraege(fahrzeug,kennzeichen,status,archiviert,
                kunden_status_token,kunden_status_aktiv,notiz_intern,werkstatt_angebot_preis,erstellt_am,geaendert_am)
                VALUES('Synthetic service car',?,2,0,?,1,'PRIVATE-DO-NOT-EXPOSE',98765,?,?)""",(plate,token or uuid4().hex,portal.now_str(),portal.now_str())).lastrowid
            db.commit()
        return number

    def sql(self, text, args=()):
        with closing(portal.get_db()) as db:
            rows=db.execute(text,args).fetchall()
            db.commit()
            return rows

    def link(self, **changes):
        data=dict(auftrag_id=str(self.order),art='service',bestaetigt='1',request_id=uuid4().hex)
        data.update(changes)
        self.service.change_vehicle(self.company,self.vehicle,'auftrag',data)
        return data

    def public_status(self):
        return self.customer.get(f'/kunden-app/fall/{self.token}/status')

    def test_explicit_match_and_confirmation_are_required_and_do_not_change_order(self):
        original=dict(self.sql('SELECT * FROM auftraege WHERE id=?',(self.order,))[0])
        with self.assertRaises(ValueError): self.link(bestaetigt='')
        with self.assertRaises(ValueError): self.link(auftrag_id='0')
        with self.assertRaises(ValueError): self.link(auftrag_id='99999999')
        self.sql('UPDATE auftraege SET kennzeichen=? WHERE id=?',('OTHER 1',self.order))
        with self.assertRaises(ValueError): self.link()
        self.sql('UPDATE auftraege SET kennzeichen=? WHERE id=?',('MOS TEST 159',self.order))
        self.link()
        self.assertEqual(dict(self.sql('SELECT * FROM auftraege WHERE id=?',(self.order,))[0]),original)

    def test_vin_match_allows_changed_plate_but_conflicting_vin_rejects(self):
        self.sql('UPDATE kundenflotten_fahrzeuge SET fin=? WHERE id=?',('WVWZZZ1KZ6W000159',self.vehicle))
        self.sql('UPDATE auftraege SET fin_nummer=? WHERE id=?',('WVWZZZ1KZ6W000160',self.order))
        with self.assertRaises(ValueError): self.link()
        self.sql('UPDATE auftraege SET fin_nummer=?,kennzeichen=? WHERE id=?',('WVWZZZ1KZ6W000159','NEW PLATE',self.order))
        self.link()
        self.assertEqual(self.public_status().status_code,200)

    def test_mapping_is_idempotent_and_unique_across_vehicles(self):
        data=self.link()
        self.service.change_vehicle(self.company,self.vehicle,'auftrag',data)
        self.link()
        self.assertEqual(self.sql('SELECT COUNT(*) FROM kundenflotten_auftraege WHERE auftrag_id=?',(self.order,))[0][0],1)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM kundenflotten_historie WHERE fahrzeug_id=? AND art='auftrag'",(self.vehicle,))[0][0],1)
        with self.assertRaises(ValueError): self.link(art='reifen')
        other=self.service.create_company({'name':'Other synthetic fleet'})
        vehicle=self.service.create_vehicle(other,{'modell':'Other','kennzeichen':'MOS TEST 159','km':'1','km_datum':date.today().isoformat()})
        with self.assertRaises(ValueError):
            self.service.change_vehicle(other,vehicle,'auftrag',dict(data,request_id=uuid4().hex))

    def test_customer_single_case_has_allowlisted_data_and_private_headers(self):
        self.link()
        other=self.new_order()
        self.link(auftrag_id=str(other),art='reifen')
        response=self.public_status()
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.json['cases']),1)
        case=response.json['cases'][0]
        self.assertEqual(case['id'],self.order)
        self.assertEqual(set(case),{'id','title','vehicle','plate','status','label','description','updated','stage','steps'})
        self.assertNotIn('PRIVATE-DO-NOT-EXPOSE',response.text)
        self.assertNotIn(self.token,response.text)
        html=self.customer.get(f'/kunden-app/fall/{self.token}')
        self.assertEqual(html.status_code,200)
        self.assertIn('no-store',html.headers['Cache-Control'])
        self.assertEqual(html.headers['Referrer-Policy'],'no-referrer')
        self.assertNotIn('PRIVATE-DO-NOT-EXPOSE',html.text)
        self.assertNotIn('Kundensicht öffnen',html.text)

    def test_invalid_unmapped_disabled_archived_deleted_and_reassigned_are_hidden(self):
        self.assertEqual(self.public_status().status_code,404)
        self.link()
        self.assertEqual(self.customer.get('/kunden-app/fall/not-a-token/status').status_code,404)
        for field,value in [('kunden_status_aktiv',0),('archiviert',1),('kennzeichen','OTHER 1')]:
            before=self.sql(f'SELECT {field} FROM auftraege WHERE id=?',(self.order,))[0][0]
            self.sql(f'UPDATE auftraege SET {field}=? WHERE id=?',(value,self.order))
            self.assertEqual(self.public_status().status_code,404,field)
            self.assertEqual(self.customer.get(f'/kunden-app/fall/{self.token}').status_code,404,field)
            if field!='kunden_status_aktiv': self.assertEqual(self.service.linked_cases(self.company,self.vehicle),[])
            self.sql(f'UPDATE auftraege SET {field}=? WHERE id=?',(before,self.order))
        self.sql('DELETE FROM auftraege WHERE id=?',(self.order,))
        self.assertEqual(self.public_status().status_code,404)
        self.assertEqual(self.service.linked_cases(self.company,self.vehicle),[])

    def test_admin_preview_and_mapping_enforce_auth_scope_and_csrf(self):
        self.assertEqual(self.customer.get(self.path+'/kundenansicht').status_code,302)
        self.assertEqual(self.customer.get(self.path+'/kundenansicht/status').status_code,302)
        self.assertEqual(self.admin.get(self.path+'/kundenansicht').status_code,200)
        wrong=self.path.replace(self.company,uuid4().hex)
        self.assertEqual(self.admin.get(wrong+'/kundenansicht/status').status_code,404)
        data=dict(auftrag_id=str(self.order),art='service',bestaetigt='1',request_id=uuid4().hex)
        self.assertEqual(self.admin.post(self.path+'/auftrag',data=data).status_code,400)
        data['csrf_token']='test-csrf'
        self.assertIn(self.admin.post(self.path+'/auftrag',data=data).status_code,(302,303))
        case=self.admin.get(self.path+'/kundenansicht/status').json['cases'][0]
        self.assertNotIn('customer_url',case)

    def test_every_reason_reuses_status_without_paint_stages_for_service(self):
        self.link()
        self.sql("UPDATE auftraege SET status=3,produktion_schritt='lackierung' WHERE id=?",(self.order,))
        for kind in KINDS:
            self.sql('UPDATE kundenflotten_auftraege SET art=? WHERE auftrag_id=?',(kind,self.order))
            case=self.public_status().json['cases'][0]
            self.assertEqual(case['title'],KINDS[kind])
            self.assertEqual(case['status'],3)
            self.assertEqual(case['stage'],'Lackierung' if kind=='karosserie' else '')
        for value in (99,'invalid',None):
            case=case_view({'id':1,'status':value},'service')
            self.assertEqual(case['status'],0)
            self.assertFalse(any(s['done'] or s['current'] for s in case['steps']))

    def test_actual_personal_employee_posts_reflect_without_second_status_or_dispatch(self):
        self.link()
        with closing(portal.get_db()) as db:
            mid=db.execute("INSERT INTO mitarbeiter(name,aktiv,erstellt_am,geaendert_am) VALUES('Synthetic mechanic',1,?,?)",(portal.now_str(),portal.now_str())).lastrowid
            db.execute("INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,version,auth_version) VALUES(?,'test-only',1,0,1,1)",(mid,))
            db.commit()
        employee=portal.app.test_client()
        with employee.session_transaction() as session:
            session.update(assistent_mid=mid,assistent_version=1,assistent_auth_version=1,csrf_token='test-csrf')
        with patch.dict(portal.app.config,EMPLOYEE_ORDER_OPERATIONS_ENABLED=True,ASSISTANT_NATIVE_COCKPIT=True), patch.object(portal,'fuehre_auftrag_status_wechsel_aus',side_effect=AssertionError('No outgoing mail')):
            for action,status in [('in_arbeit_starten',3),('fertig_melden',4)]:
                with patch('werkstatt_mitarbeiter_auftraege.render_template',return_value='synthetic employee page') as render:
                    response=employee.get(f'/werkstatt/mein-konto/auftraege?nummer={self.order}')
                    self.assertEqual(response.status_code,200)
                    form=next(f for f in render.call_args.kwargs['action_forms'] if f['aktion']==action)
                data=dict(aktion=action,request_id=form['request_id'],confirmed='ja',csrf_token='test-csrf')
                path=f'/werkstatt/mein-konto/auftraege/{self.order}/status'
                self.assertEqual(employee.post(path,data=data).status_code,303)
                self.assertEqual(employee.post(path,data=data).status_code,303)
                case=self.public_status().json['cases'][0]
                self.assertEqual(case['status'],status)
                self.assertEqual(self.service.linked_cases(self.company,self.vehicle)[0]['status'],status)
                if status==4:
                    self.assertIn('Übergabe stimmen wir',case['description'])
            count=self.sql('SELECT COUNT(*) FROM status_log WHERE auftrag_id=?',(self.order,))[0][0]
            self.assertEqual(count,2,'Repeated employee POSTs must not repeat writes')
        anonymous=portal.app.test_client()
        self.assertIn(anonymous.post(path,data=data).status_code,(400,403))


if __name__=='__main__': unittest.main()
