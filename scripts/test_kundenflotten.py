"""Synthetic, isolated customer fleet tests; never open the workshop database."""
import sqlite3
import re
import shutil
import sys
import tempfile
import unittest
from datetime import date, timedelta
from functools import wraps
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, RLock, local, Event
from werkzeug.exceptions import ServiceUnavailable

from flask import Flask, abort, session
from jinja2 import ChoiceLoader, DictLoader
from markupsafe import Markup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_kundenflotten import BACKUP_TABLES, Kundenflotten, add_months, due_state, register_kundenflotten


class OperationLock:
    """Inject the same shared-lock contract the production portal provides."""
    def __init__(self):
        self.lock = RLock()
        self.state = local()
        self.calls = 0
        self.attempt = None

    def held(self):
        return getattr(self.state, 'depth', 0) > 0

    @contextmanager
    def __call__(self):
        if self.attempt:
            self.attempt.set()
        with self.lock:
            self.calls += 1
            self.state.depth = getattr(self.state, 'depth', 0) + 1
            try:
                yield
            finally:
                self.state.depth -= 1


class CustomerFleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "synthetic.db"
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.secret_key = "test-only-no-customer-data"
        self.app.config.update(TESTING=True, KUNDENFLOTTEN_DEMO_ENABLED=True)
        self.app.add_url_rule("/homepage", "oeffentliche_homepage", lambda: "Testhomepage")
        self.app.jinja_loader = ChoiceLoader([DictLoader({"base.html": "<html>{% for c,m in get_flashed_messages(with_categories=true) %}<p>{{ m }}</p>{% endfor %}{% block content %}{% endblock %}</html>"}), self.app.jinja_loader])
        self.app.jinja_env.globals["csrf_field"] = lambda: Markup('<input type="hidden" name="csrf_token" value="test-csrf">')
        def get_db():
            connection = sqlite3.connect(self.db_path)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        def protected(fn):
            @wraps(fn)
            def wrapped(*args, **kwargs):
                if not session.get("admin"):
                    abort(403)
                return fn(*args, **kwargs)
            return wrapped
        self.operation_lock = OperationLock()
        self.portal = SimpleNamespace(app=self.app, get_db=get_db, admin_required=protected,
                                      portal_originals_operation_lock=self.operation_lock)
        self.service = register_kundenflotten(self.portal)
        self.service.init_schema()
        self.service.init_schema()
        self.client = self.app.test_client()
        self.today = date.today().isoformat()
        self.company = self.service.create_company({"name":"Demo Betrieb", "kontakt":"Alex Muster", "freigabe_limit":"250,50", "konditionen":"Nur nach Freigabe"})
        self.vehicle = self.service.create_vehicle(self.company, {"kennzeichen":"mos demo", "modell":"Demofahrzeug", "km":"10000", "km_datum":self.today})

    def tearDown(self):
        self.tmp.cleanup()

    def admin(self):
        with self.client.session_transaction() as sess:
            sess["admin"] = True
            sess["csrf_token"] = "test-csrf"

    def plan(self, **changes):
        data = dict(art="service", titel="Service", faellig_datum=self.today, faellig_km="10000", intervall_monate="12", intervall_km="15000", quelle="Individuell bestätigter Wartungsplan", request_id=uuid4().hex)
        data.update(changes)
        self.service.change_vehicle(self.company, self.vehicle, "plan", data)
        return self.service.vehicle(self.company, self.vehicle)["plans"][0]

    def count(self, table):
        with closing(self.portal.get_db()) as db:
            return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def hu_plan(self, **changes):
        data = dict(art="hu", titel="HU / AU", faellig_monat="2030-02", quelle="Prüfbericht Beispiel 1 · externer Prüfpartner", request_id=uuid4().hex)
        data.update(changes)
        self.service.change_vehicle(self.company, self.vehicle, "plan", data)
        return next(p for p in self.service.vehicle(self.company, self.vehicle)["plans"] if p["art"] == "hu")

    def test_hu_month_preserves_month_end_and_leap_year(self):
        from werkstatt_kundenflotten import _month_end
        self.assertEqual(_month_end({"monat":"2028-02"}, "monat"), "2028-02-29")
        self.assertEqual(_month_end({"monat":"2100-02"}, "monat"), "2100-02-28")
        plan = self.hu_plan()
        self.assertEqual(plan["faellig_datum"], "2030-02-28")
        self.assertIsNone(plan["faellig_km"])
        self.assertIsNone(plan["intervall_monate"])
        self.assertEqual(due_state(plan, 3000000), "geplant")

    def test_hu_rejects_missing_date_and_guessed_intervals_without_writes(self):
        initial_history = self.count("kundenflotten_historie")
        for change in [dict(faellig_monat="",faellig_datum=""), dict(faellig_km="50000"), dict(intervall_monate="24"), dict(intervall_km="15000"), dict(faellig_monat="2030-13"), dict(faellig_monat="1999-12"), dict(quelle="")]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.hu_plan(**change)
            self.assertEqual(self.count("kundenflotten_plaene"),0)
            self.assertEqual(self.count("kundenflotten_anlagen"),0)
            self.assertEqual(self.count("kundenflotten_historie"),initial_history)

    def test_hu_replay_is_safe_and_second_open_plan_rejected(self):
        receipt = uuid4().hex
        first = self.hu_plan(request_id=receipt)
        self.hu_plan(request_id=receipt)
        with self.assertRaises(ValueError):
            self.hu_plan(faellig_monat="2031-01")
        self.assertEqual(self.count("kundenflotten_plaene"),1)
        self.assertEqual(self.count("kundenflotten_anlagen"),1)
        self.assertEqual(self.service.vehicle(self.company,self.vehicle)["plans"][0]["id"],first["id"])

    def test_hu_requires_passed_report_and_next_month_without_partial_completion(self):
        plan = self.hu_plan()
        base = dict(erledigt_am=self.today, km="10050", version="1", pruefung_bestanden="1", naechste_hu_monat="2031-04", pruefbericht="Neuer Prüfbericht Beispiel 2")
        before = self.count("kundenflotten_historie")
        for changes in [dict(pruefung_bestanden=""),dict(naechste_hu_monat=""),dict(naechste_hu_monat="2001-01"),dict(pruefbericht="")]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.service.complete_plan(self.company,self.vehicle,plan["id"],dict(base,**changes))
            record = self.service.vehicle(self.company,self.vehicle)
            self.assertEqual(record["vehicle"]["km"],10000)
            self.assertEqual(record["plans"][0]["version"],1)
            self.assertEqual(record["plans"][0]["faellig_datum"],"2030-02-28")
            self.assertEqual(self.count("kundenflotten_historie"),before)

    def test_hu_next_date_is_report_based_and_scoped(self):
        plan = self.hu_plan()
        data = dict(erledigt_am=self.today,km="10050",version="1",pruefung_bestanden="1",naechste_hu_monat="2031-04",pruefbericht="Prüfbericht 2 · externer Prüfpartner")
        other=self.service.create_company({"name":"Andere Testfirma"})
        with self.assertRaises(LookupError):
            self.service.complete_plan(other,self.vehicle,plan["id"],data)
        self.service.complete_plan(self.company,self.vehicle,plan["id"],data)
        record = self.service.vehicle(self.company,self.vehicle)
        updated = record["plans"][0]
        self.assertEqual(updated["faellig_datum"],"2031-04-30")
        self.assertEqual(updated["quelle"],data["pruefbericht"])
        completion = next(item["text"] for item in record["history"] if item["art"] == "abschluss")
        self.assertIn(plan["quelle"], completion)
        self.assertIn(data["pruefbericht"], completion)
        self.assertIn("2030-02-28", completion)
        self.assertEqual(updated["version"],2)
        self.assertEqual(record["vehicle"]["km"],10050)
        self.assertIsNone(updated["intervall_monate"])
        self.assertIsNone(updated["faellig_km"])
        with self.assertRaises(ValueError):
            self.service.complete_plan(self.company,self.vehicle,plan["id"],data)
        self.assertEqual(sum(h["art"]=="abschluss" for h in self.service.vehicle(self.company,self.vehicle)["history"]),1)

    def test_hu_http_form_and_completion_keep_real_report(self):
        self.admin()
        path=f"/admin/kundenflotten/firma/{self.company}/fahrzeug/{self.vehicle}"
        before=self.client.get(path).text
        self.assertIn('name="faellig_monat"',before)
        self.assertIn('name="art" value="hu"',before)
        response=self.client.post(path+'/plan',data=dict(csrf_token="test-csrf",request_id=uuid4().hex,art="hu",titel="HU / AU",faellig_monat="2030-02",quelle="Testprüfbericht"),follow_redirects=True)
        self.assertEqual(response.status_code,200)
        self.assertIn("28.02.2030",response.text)
        self.assertIn('name="naechste_hu_monat"',response.text)
        self.assertNotIn('name="faellig_monat"',response.text)
        plan=self.service.vehicle(self.company,self.vehicle)["plans"][0]
        response=self.client.post(path+f'/plan/{plan["id"]}/erledigt',data=dict(csrf_token="test-csrf",erledigt_am=self.today,km="10000",version="1",pruefung_bestanden="1",naechste_hu_monat="2031-04",pruefbericht="Folgebericht"),follow_redirects=True)
        self.assertEqual(response.status_code,200)
        self.assertIn("30.04.2031",response.text)
        self.assertIn("Folgebericht",response.text)

    def test_schema_persistent_and_backup_table_set(self):
        with closing(self.portal.get_db()) as db:
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        self.assertEqual(names, set(BACKUP_TABLES))
        self.assertEqual(self.service.companies()[0]["anzahl"], 1)
        self.assertEqual(self.service.companies()[0]["freigabe_limit_cent"], 25050)

    def test_date_or_kilometres_first_reached(self):
        future = (date.today() + timedelta(days=100)).isoformat()
        self.assertEqual(due_state({"faellig_datum":future, "faellig_km":10000}, 10001), "ueberfaellig")
        self.assertEqual(due_state({"faellig_datum":self.today,"faellig_km":20000}, 10000), "faellig")
        self.assertEqual(due_state({"faellig_datum":future,"faellig_km":10999}, 10000), "bald")
        self.assertEqual(due_state({"faellig_datum":future,"faellig_km":20000}, 10000), "geplant")
        self.assertEqual(due_state({"status":"abgeschlossen","faellig_datum":self.today}, 10000), "abgeschlossen")

    def test_invalid_intervals_not_inferred(self):
        for values in [dict(faellig_datum="",faellig_km=""),dict(intervall_monate="0"),dict(quelle=""),dict(faellig_datum="",intervall_monate="1"),dict(intervall_km="-2")]:
            with self.assertRaises(ValueError): self.plan(**values)
        self.assertEqual(self.count("kundenflotten_plaene"), 0)

    def test_vehicle_file_lists_urgent_maintenance_before_future_plans(self):
        self.plan(titel="Bald", faellig_datum=(date.today()+timedelta(days=10)).isoformat(), faellig_km="", intervall_km="")
        self.plan(titel="Später", faellig_datum=(date.today()+timedelta(days=120)).isoformat(), faellig_km="", intervall_km="")
        self.plan(titel="Heute", faellig_datum=self.today, faellig_km="", intervall_km="")
        self.plan(titel="Überfällig", faellig_datum=(date.today()-timedelta(days=5)).isoformat(), faellig_km="", intervall_km="")
        self.assertEqual([plan['titel'] for plan in self.service.vehicle(self.company, self.vehicle)['plans']], ["Überfällig", "Heute", "Bald", "Später"])

    def test_completion_rolls_over_and_replay_rejected(self):
        plan = self.plan()
        data = {"erledigt_am":self.today,"km":"10100","version":"1"}
        self.service.complete_plan(self.company,self.vehicle,plan["id"],data)
        current = self.service.vehicle(self.company,self.vehicle)
        self.assertEqual(current["plans"][0]["faellig_km"],25100)
        self.assertEqual(current["plans"][0]["faellig_datum"],add_months(date.today(),12).isoformat())
        self.assertEqual(current["vehicle"]["km"],10100)
        with self.assertRaises(ValueError): self.service.complete_plan(self.company,self.vehicle,plan["id"],data)
        self.assertEqual(sum(h["art"]=="abschluss" for h in self.service.vehicle(self.company,self.vehicle)["history"]),1)

    def test_oneoff_completion_no_invented_next_service(self):
        plan = self.plan(intervall_km="",intervall_monate="")
        self.service.complete_plan(self.company,self.vehicle,plan["id"],{"erledigt_am":self.today,"km":"10000","version":"1"})
        done = self.service.vehicle(self.company,self.vehicle)["plans"][0]
        self.assertEqual(done["status"],"abgeschlossen")
        self.assertIsNone(done["faellig_km"])
        self.assertEqual(done["faellig_datum"],"")

    def test_end_of_month_date_rollover(self):
        self.assertEqual(add_months(date(2024,1,31),1),date(2024,2,29))
        self.assertEqual(add_months(date(2024,2,29),12),date(2025,2,28))

    def test_company_vehicle_scope_blocks_mutation(self):
        other = self.service.create_company({"name":"Andere Firma"})
        with self.assertRaises(LookupError): self.service.vehicle(other,self.vehicle)
        with self.assertRaises(LookupError): self.service.change_vehicle(other,self.vehicle,"km",{"km":"12000","km_datum":self.today})
        self.assertEqual(self.service.vehicle(self.company,self.vehicle)["vehicle"]["km"],10000)
        plan = self.plan()
        with self.assertRaises(LookupError): self.service.complete_plan(other,self.vehicle,plan["id"],{"erledigt_am":self.today,"km":"10000","version":"1"})

    def test_plan_cannot_be_completed_for_another_vehicle(self):
        second = self.service.create_vehicle(self.company, {"kennzeichen":"mos demo 2","modell":"Zweites Demoauto","km":"10000","km_datum":self.today})
        plan = self.plan()
        with self.assertRaises(LookupError): self.service.complete_plan(self.company,second,plan["id"],{"erledigt_am":self.today,"km":"10000","version":"1"})

    def test_driver_consent_and_history(self):
        with self.assertRaises(ValueError): self.service.change_vehicle(self.company,self.vehicle,"fahrer",{"name":"Alex","kanal":"whatsapp","telefon":"012345"})
        self.service.change_vehicle(self.company,self.vehicle,"fahrer",{"name":"Alex","kanal":"keine"})
        self.service.change_vehicle(self.company,self.vehicle,"fahrer",{"name":"Sam","kanal":"email","email":"sam@example.invalid","einwilligung":"Synthetische Bestätigung im Test"})
        record = self.service.vehicle(self.company,self.vehicle)
        self.assertEqual(record["driver"]["name"],"Sam")
        self.assertEqual(sum(h["art"]=="fahrer" for h in record["history"]),2)

    def test_km_regression_and_future_rejected(self):
        for data in [{"km":"9000","km_datum":self.today},{"km":"12000","km_datum":(date.today()+timedelta(days=1)).isoformat()}]:
            with self.assertRaises(ValueError): self.service.change_vehicle(self.company,self.vehicle,"km",data)
        self.assertEqual(self.service.vehicle(self.company,self.vehicle)["vehicle"]["km"],10000)

    def test_company_form_replay_creates_one_company_and_preserves_data(self):
        self.admin()
        html = self.client.get('/admin/kundenflotten').get_data(as_text=True)
        token = re.search(r'name="request_id" value="([a-f0-9]{32})"', html).group(1)
        data = {'csrf_token': 'test-csrf', 'request_id': token, 'name': 'Neue Testfirma', 'kontakt': 'Alex'}
        for _ in range(2):
            self.assertEqual(self.client.post('/admin/kundenflotten/firma', data=data).status_code, 303)
        self.assertEqual(self.count('kundenflotten_firmen'), 2)
        self.assertEqual(self.service.company(token)[0]['kontakt'], 'Alex')
        with self.assertRaises(ValueError):
            self.service.create_company(dict(data, kontakt='Geänderter Inhalt'), require_submission=True)
        self.assertEqual(self.service.company(token)[0]['kontakt'], 'Alex')

    def test_company_http_creation_requires_a_valid_form_receipt(self):
        self.admin()
        for token in ('', 'not-a-form-token'):
            response = self.client.post('/admin/kundenflotten/firma', data={
                'csrf_token': 'test-csrf', 'name': 'Nicht anlegen', 'request_id': token})
            self.assertEqual(response.status_code, 303)
        self.assertEqual(self.count('kundenflotten_firmen'), 1)

    def test_parallel_company_form_replay_is_atomic(self):
        data = {'name': 'Parallele Testfirma', 'request_id': uuid4().hex}
        barrier = Barrier(2)
        def submit():
            barrier.wait(timeout=5)
            return self.service.create_company(data, require_submission=True)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result(timeout=10) for future in [pool.submit(submit), pool.submit(submit)]]
        self.assertEqual(results, [data['request_id'], data['request_id']])
        self.assertEqual(self.count('kundenflotten_firmen'), 2)
        another = self.service.create_company(dict(data, request_id=uuid4().hex), require_submission=True)
        self.assertNotEqual(another, data['request_id'])

    def test_plate_separator_variants_cannot_create_duplicate_vehicle_files(self):
        self.service.create_vehicle(self.company, {'kennzeichen': 'MOS-AB 123', 'modell': 'Testauto', 'km': '0', 'km_datum': self.today})
        for plate in ('MOS AB 123', 'mos-ab123', 'MOS–AB 123', 'MOSAB123'):
            with self.subTest(plate=plate), self.assertRaises(ValueError):
                self.service.create_vehicle(self.company, {'kennzeichen': plate, 'modell': 'Doppeltes Auto', 'km': '0', 'km_datum': self.today})
        self.assertEqual(self.count('kundenflotten_fahrzeuge'), 2)

    def test_distinct_district_boundaries_are_not_false_duplicates(self):
        for plate in ('B-AB 123', 'BA-B 123'):
            self.service.create_vehicle(self.company, {'kennzeichen': plate, 'modell': 'Eigenes Testauto', 'km': '0', 'km_datum': self.today})
        self.assertEqual(self.count('kundenflotten_fahrzeuge'), 3)
        for plate in ('B AB123', 'BA B123'):
            with self.subTest(plate=plate), self.assertRaises(ValueError):
                self.service.create_vehicle(self.company, {'kennzeichen': plate, 'modell': 'Dublette', 'km': '0', 'km_datum': self.today})

    def test_past_new_appointment_rolls_back_receipt_and_record(self):
        with self.assertRaises(ValueError):
            self.service.change_vehicle(self.company, self.vehicle, 'termin', {
                'art': 'reifen', 'datum': (date.today()-timedelta(days=1)).isoformat(),
                'transport': 'offen', 'request_id': uuid4().hex})
        self.assertEqual(self.count('kundenflotten_termine'), 0)
        self.assertEqual(self.count('kundenflotten_anlagen'), 0)

    def test_impossible_next_mileage_does_not_partially_complete_plan(self):
        plan = self.plan(faellig_km='3000000', intervall_km='200000', faellig_datum='', intervall_monate='')
        with self.assertRaises(ValueError):
            self.service.complete_plan(self.company, self.vehicle, plan['id'], {
                'erledigt_am': self.today, 'km': '3000000', 'version': '1'})
        record = self.service.vehicle(self.company, self.vehicle)
        self.assertEqual(record['vehicle']['km'], 10000)
        self.assertEqual(record['plans'][0]['version'], 1)
        self.assertEqual(record['plans'][0]['faellig_km'], 3000000)
        self.assertFalse(any(item['art']=='abschluss' for item in record['history']))

    def test_duplicate_vehicle_rejected(self):
        with self.assertRaises(ValueError): self.service.create_vehicle(self.company,{"kennzeichen":"MOS DEMO","modell":"Duplicate","km":"0","km_datum":self.today})

    def test_private_budget_validated(self):
        for value in ["NaN","Infinity","-10","1.555","1e99"]:
            with self.assertRaises(ValueError): self.service.create_company({"name":"Invalid","freigabe_limit":value})
        self.assertEqual(self.count("kundenflotten_firmen"),1)

    def test_tires_and_transport_do_not_order_or_confirm(self):
        self.service.change_vehicle(self.company,self.vehicle,"reifen",{"saison":"winter","lagerplatz":"A-01","beschreibung":"205/55 R16 Test","bestellbedarf":"Preis für zwei Reifen anfragen"})
        self.service.change_vehicle(self.company,self.vehicle,"termin",{"datum":self.today,"art":"reifen","transport":"abholen","ersatzwagen":"1","request_id":uuid4().hex})
        record = self.service.vehicle(self.company,self.vehicle)
        self.assertEqual(record["tires"]["lagerplatz"],"A-01")
        self.assertEqual(record["appointments"][0]["status"],"angefragt")
        self.assertEqual(record["appointments"][0]["ersatzwagen"],1)

    def test_admin_required_and_csrf(self):
        url=f"/admin/kundenflotten/firma/{self.company}/fahrzeug/{self.vehicle}/km"
        self.assertEqual(self.client.get("/admin/kundenflotten").status_code,403)
        with self.client.session_transaction() as sess: sess.update(csrf_token="test-csrf",mitarbeiter_id=1)
        self.assertEqual(self.client.post(url,data={"csrf_token":"test-csrf","km":"12000","km_datum":self.today}).status_code,403)
        self.admin()
        self.assertEqual(self.client.post(url,data={"km":"12000","km_datum":self.today}).status_code,400)
        self.assertEqual(self.client.post(url,data={"csrf_token":"wrong","km":"12000","km_datum":self.today}).status_code,400)
        self.assertEqual(self.client.post(url,data={"csrf_token":"test-csrf","km":"12000","km_datum":self.today}).status_code,303)
        self.assertEqual(self.service.vehicle(self.company,self.vehicle)["vehicle"]["km"],12000)

    def test_admin_pages_render_and_escape(self):
        self.admin()
        self.plan()
        self.service.change_vehicle(self.company,self.vehicle,"fahrer",{"name":"<script>alert(1)</script>","kanal":"keine"})
        for path in ["/admin/kundenflotten",f"/admin/kundenflotten/firma/{self.company}",f"/admin/kundenflotten/firma/{self.company}/fahrzeug/{self.vehicle}"]:
            response=self.client.get(path)
            self.assertEqual(response.status_code,200)
            self.assertIn("no-store",response.headers["Cache-Control"])
            self.assertNotIn(b"<script>alert(1)",response.data)

    def test_demo_never_reads_customer_tables(self):
        self.portal.get_db=lambda: (_ for _ in ()).throw(AssertionError("Demo must never query customer database"))
        response=self.client.get("/kunden-app/vorschau")
        self.assertEqual(response.status_code,200)
        self.assertNotIn(b"Demo Betrieb",response.data)
        self.assertIn(b"MOS",response.data)
        self.assertIn(b"Beispieldaten",response.data)
        self.assertEqual(self.client.post("/kunden-app/vorschau").status_code,405)

    def test_demo_closed_by_default_and_to_network(self):
        self.app.config["KUNDENFLOTTEN_DEMO_ENABLED"]=False
        self.assertEqual(self.client.get("/kunden-app/vorschau").status_code,404)
        self.app.config["KUNDENFLOTTEN_DEMO_ENABLED"]=True
        self.assertEqual(self.client.get("/kunden-app/vorschau",environ_overrides={"REMOTE_ADDR":"10.0.0.4"}).status_code,404)

    def test_no_outbound_client_or_employee_login(self):
        js=(ROOT/"static/kundenflotten-demo.js").read_text(encoding="utf8")
        for forbidden in ("fetch(","XMLHttpRequest","sendBeacon","serviceWorker.register","localStorage"):
            self.assertNotIn(forbidden,js)
        routes=[str(rule) for rule in self.app.url_map.iter_rules() if rule.endpoint.startswith("kundenflotten.")]
        self.assertTrue(all(path.startswith(("/admin/kundenflotten","/kunden-app/vorschau","/kunden-app/fall/<token>")) for path in routes))
        # The only non-demo customer routes are read-only single-order views.
        public=[rule for rule in self.app.url_map.iter_rules() if rule.endpoint.startswith('kundenflotten.') and str(rule).startswith('/kunden-app/fall/')]
        self.assertEqual(len(public),2)
        self.assertTrue(all(rule.methods <= {'GET','HEAD','OPTIONS'} for rule in public))

    def test_double_post_plan_and_visit_create_once(self):
        self.admin()
        base=f"/admin/kundenflotten/firma/{self.company}/fahrzeug/{self.vehicle}"
        html=self.client.get(base).get_data(as_text=True)
        forms={action:re.search(r'<form method="post" action="'+re.escape(base+'/'+action)+r'".*?</form>',html,re.S).group(0) for action in ('plan','termin')}
        data={
            'plan':{'art':'service','titel':'Testwartung','faellig_datum':self.today,'quelle':'Testvorgabe'},
            'termin':{'art':'reifen','datum':self.today,'transport':'abholen','ersatzwagen':'1'},
        }
        for action in data:
            token=re.search(r'name="request_id" value="([a-f0-9]{32})"',forms[action]).group(1)
            data[action].update(csrf_token='test-csrf',request_id=token)
            for _ in range(2):
                self.assertEqual(self.client.post(base+'/'+action,data=data[action]).status_code,303)
        self.assertEqual(self.count('kundenflotten_plaene'),1)
        self.assertEqual(self.count('kundenflotten_termine'),1)
        self.assertEqual(self.count('kundenflotten_anlagen'),2)
        history=self.service.vehicle(self.company,self.vehicle)['history']
        self.assertEqual(sum(h['art']=='plan' for h in history),1)
        self.assertEqual(sum(h['art']=='termin' for h in history),1)

    def test_fresh_request_allows_intentionally_identical_appointments(self):
        data={'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex}
        self.service.change_vehicle(self.company,self.vehicle,'termin',data)
        self.service.change_vehicle(self.company,self.vehicle,'termin',dict(data,request_id=uuid4().hex))
        self.assertEqual(self.count('kundenflotten_termine'),2)

    def test_changed_payload_with_used_token_is_rejected(self):
        data={'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex}
        self.service.change_vehicle(self.company,self.vehicle,'termin',data)
        with self.assertRaises(ValueError):
            self.service.change_vehicle(self.company,self.vehicle,'termin',dict(data,transport='abholen'))
        self.assertEqual(self.count('kundenflotten_termine'),1)
        self.assertEqual(self.count('kundenflotten_anlagen'),1)

    def test_submission_token_cannot_be_reused_across_vehicles(self):
        data={'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex}
        self.service.change_vehicle(self.company,self.vehicle,'termin',data)
        other=self.service.create_vehicle(self.company,{'kennzeichen':'TEST 2','modell':'Zweites Demoauto','km':'0','km_datum':self.today})
        with self.assertRaises(ValueError): self.service.change_vehicle(self.company,other,'termin',data)
        self.assertEqual(self.count('kundenflotten_termine'),1)

    def test_missing_or_invalid_submission_token_cannot_write(self):
        for token in ('','not-a-form-token','../'+'a'*29):
            with self.assertRaises(ValueError):
                self.service.change_vehicle(self.company,self.vehicle,'termin',{'art':'reifen','datum':self.today,'transport':'offen','request_id':token})
        self.assertEqual(self.count('kundenflotten_termine'),0)
        self.assertEqual(self.count('kundenflotten_anlagen'),0)

    def test_plan_replay_after_completion_does_not_recreate_it(self):
        token=uuid4().hex
        plan=self.plan(request_id=token)
        self.service.complete_plan(self.company,self.vehicle,plan['id'],{'erledigt_am':self.today,'km':'10000','version':'1'})
        self.plan(request_id=token)
        self.assertEqual(self.count('kundenflotten_plaene'),1)
        record=self.service.vehicle(self.company,self.vehicle)['plans'][0]
        self.assertEqual(record['version'],2)
        self.assertEqual(record['faellig_km'],25000)

    def test_failed_creation_rolls_back_submission_receipt(self):
        data={'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex}
        original=self.service.history
        def fail(*args): raise ValueError('synthetic write failure')
        self.service.history=fail
        with self.assertRaises(ValueError): self.service.change_vehicle(self.company,self.vehicle,'termin',data)
        self.service.history=original
        self.assertEqual(self.count('kundenflotten_termine'),0)
        self.assertEqual(self.count('kundenflotten_anlagen'),0)
        self.service.change_vehicle(self.company,self.vehicle,'termin',data)
        self.assertEqual(self.count('kundenflotten_termine'),1)

    def test_parallel_double_click_is_atomic(self):
        forms={
            'termin':{'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex},
            'plan':{'art':'service','titel':'Paralleler Testplan','faellig_datum':self.today,'quelle':'Testvorgabe','request_id':uuid4().hex},
        }
        for action,data in forms.items():
            barrier=Barrier(2)
            def submit():
                barrier.wait(timeout=5)
                self.service.change_vehicle(self.company,self.vehicle,action,data)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(submit) for _ in range(2)]
                for future in futures: future.result(timeout=10)
        self.assertEqual(self.count('kundenflotten_termine'),1)
        self.assertEqual(self.count('kundenflotten_plaene'),1)
        self.assertEqual(self.count('kundenflotten_anlagen'),2)

    def track_locked_database(self):
        original = self.portal.get_db
        events = []
        test = self
        class LockedConnection:
            def __init__(self, connection):
                self.connection = connection
            def __getattr__(self, key):
                return getattr(self.connection, key)
            def commit(self):
                test.assertTrue(test.operation_lock.held(), 'commit must remain inside shared lock')
                events.append('commit')
                self.connection.commit()
            def rollback(self):
                test.assertTrue(test.operation_lock.held(), 'rollback must remain inside shared lock')
                events.append('rollback')
                self.connection.rollback()
            def close(self):
                test.assertTrue(test.operation_lock.held(), 'close must precede shared lock release')
                events.append('close')
                self.connection.close()
        def guarded_open():
            self.assertTrue(self.operation_lock.held(), 'DB must open only after shared lock acquisition')
            events.append('open')
            return LockedConnection(original())
        self.portal.get_db = guarded_open
        return original, events

    def test_all_mutations_hold_restore_lock_through_connection_close(self):
        plan = self.plan()
        original, events = self.track_locked_database()
        calls = self.operation_lock.calls
        operations = [
            self.service.init_schema,
            lambda:self.service.create_company({'name':'Locked company'}),
            lambda:self.service.create_vehicle(self.company,{'kennzeichen':'LOCK 2','modell':'Locktest','km':'0','km_datum':self.today}),
            lambda:self.service.change_vehicle(self.company,self.vehicle,'km',{'km':'10001','km_datum':self.today}),
            lambda:self.service.change_vehicle(self.company,self.vehicle,'fahrer',{'name':'Locktest','kanal':'keine'}),
            lambda:self.service.change_vehicle(self.company,self.vehicle,'reifen',{'saison':'winter'}),
            lambda:self.service.change_vehicle(self.company,self.vehicle,'plan',{'art':'service','titel':'Lockplan','faellig_datum':self.today,'quelle':'Test','request_id':uuid4().hex}),
            lambda:self.service.change_vehicle(self.company,self.vehicle,'termin',{'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex}),
            lambda:self.service.complete_plan(self.company,self.vehicle,plan['id'],{'erledigt_am':self.today,'km':'10002','version':'1'}),
        ]
        try:
            for operation in operations:
                start = len(events)
                operation()
                self.assertEqual(events[start:], ['open','commit','close'])
                self.assertFalse(self.operation_lock.held())
        finally:
            self.portal.get_db = original
        self.assertEqual(self.operation_lock.calls-calls,len(operations))

    def test_failed_write_rolls_back_and_closes_before_unlock(self):
        original, events = self.track_locked_database()
        previous_history = self.service.history
        def fail(*args): raise ValueError('synthetic history failure')
        self.service.history = fail
        try:
            with self.assertRaises(ValueError):
                self.service.change_vehicle(self.company,self.vehicle,'termin',{'art':'reifen','datum':self.today,'transport':'offen','request_id':uuid4().hex})
            self.assertEqual(events,['open','rollback','close'])
            self.assertFalse(self.operation_lock.held())
        finally:
            self.portal.get_db = original
            self.service.history = previous_history
        self.assertEqual(self.count('kundenflotten_termine'),0)
        self.assertEqual(self.count('kundenflotten_anlagen'),0)

    def test_connection_open_failure_releases_shared_lock(self):
        original = self.portal.get_db
        def fail():
            self.assertTrue(self.operation_lock.held())
            raise sqlite3.OperationalError('synthetic opening failure')
        self.portal.get_db = fail
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.service.create_company({'name':'Will fail'})
            self.assertFalse(self.operation_lock.held())
        finally:
            self.portal.get_db = original
        self.service.create_company({'name':'Works after error'})

    def test_busy_restore_refuses_post_before_database_open(self):
        self.admin()
        @contextmanager
        def busy():
            raise ServiceUnavailable('Synthetic restore busy')
            yield
        original = self.portal.get_db
        self.portal.portal_originals_operation_lock = busy
        self.portal.get_db = lambda: (_ for _ in ()).throw(AssertionError('No DB may open while restore lock is busy'))
        try:
            response = self.client.post('/admin/kundenflotten/firma',data={'csrf_token':'test-csrf','name':'Not created','request_id':uuid4().hex})
            self.assertEqual(response.status_code,503)
        finally:
            self.portal.get_db = original
            self.portal.portal_originals_operation_lock = self.operation_lock
        self.assertEqual(self.count('kundenflotten_firmen'),1)

    def test_restore_replacement_finishes_before_waiting_write_opens_database(self):
        # A backup snapshot lacks the about-to-be-created company. Replacing the
        # SQLite file while the writer waits must not erase that later write.
        backup = Path(self.tmp.name) / 'restore-snapshot.db'
        shutil.copyfile(self.db_path,backup)
        attempted, opened = Event(), Event()
        original = self.portal.get_db
        def tracked_open():
            opened.set()
            return original()
        self.portal.get_db = tracked_open
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.operation_lock():
                self.operation_lock.attempt = attempted
                future=pool.submit(self.service.create_company,{'name':'Survives restore'})
                self.assertTrue(attempted.wait(5))
                self.assertFalse(opened.is_set(), 'Writer must not open the old file while restore owns the lock')
                backup.replace(self.db_path)
            key=future.result(timeout=10)
        self.portal.get_db = original
        self.operation_lock.attempt = None
        self.assertEqual(self.service.company(key)[0]['name'],'Survives restore')


if __name__ == "__main__":
    unittest.main(verbosity=2)
