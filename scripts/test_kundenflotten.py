"""Synthetic, isolated customer fleet tests; never open the workshop database."""
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, timedelta
from functools import wraps
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

from flask import Flask, abort, session
from jinja2 import ChoiceLoader, DictLoader
from markupsafe import Markup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_kundenflotten import BACKUP_TABLES, Kundenflotten, add_months, due_state, register_kundenflotten


class CustomerFleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "synthetic.db"
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.secret_key = "test-only-no-customer-data"
        self.app.config.update(TESTING=True, KUNDENFLOTTEN_DEMO_ENABLED=True)
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
        self.portal = SimpleNamespace(app=self.app, get_db=get_db, admin_required=protected)
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
        data = dict(art="service", titel="Service", faellig_datum=self.today, faellig_km="10000", intervall_monate="12", intervall_km="15000", quelle="Individuell bestätigter Wartungsplan")
        data.update(changes)
        self.service.change_vehicle(self.company, self.vehicle, "plan", data)
        return self.service.vehicle(self.company, self.vehicle)["plans"][0]

    def count(self, table):
        with closing(self.portal.get_db()) as db:
            return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_schema_persistent_and_backup_table_set(self):
        with closing(self.portal.get_db()) as db:
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
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

    def test_duplicate_vehicle_rejected(self):
        with self.assertRaises(ValueError): self.service.create_vehicle(self.company,{"kennzeichen":"MOS DEMO","modell":"Duplicate","km":"0","km_datum":self.today})

    def test_private_budget_validated(self):
        for value in ["NaN","Infinity","-10","1.555","1e99"]:
            with self.assertRaises(ValueError): self.service.create_company({"name":"Invalid","freigabe_limit":value})
        self.assertEqual(self.count("kundenflotten_firmen"),1)

    def test_tires_and_transport_do_not_order_or_confirm(self):
        self.service.change_vehicle(self.company,self.vehicle,"reifen",{"saison":"winter","lagerplatz":"A-01","beschreibung":"205/55 R16 Test","bestellbedarf":"Preis für zwei Reifen anfragen"})
        self.service.change_vehicle(self.company,self.vehicle,"termin",{"datum":self.today,"art":"reifen","transport":"abholen","ersatzwagen":"1"})
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
        self.assertTrue(all(path.startswith(("/admin/kundenflotten","/kunden-app/vorschau")) for path in routes))


if __name__ == "__main__":
    unittest.main(verbosity=2)
