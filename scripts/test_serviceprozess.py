"""Isolated synthetic policy tests. No production imports, accounts or transports."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
import unittest
from uuid import uuid4
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from werkstatt_serviceprozess import ServiceProcess, ensure_schema, ensure_serviceprocess_for_import, BACKUP_TABLES


class ServiceProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/"synthetic.sqlite3"
        self.lock=RLock()
        self.p=SimpleNamespace(get_db=self.db,portal_originals_operation_lock=lambda:self.lock,LEXWARE_TAX_RATE=19)
        @contextmanager
        def connection():
            db=self.db()
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally: db.close()
        self.fleet=SimpleNamespace(db=connection)
        self.service=ServiceProcess(self.p,self.fleet)
        with connection() as db:
            db.executescript('''CREATE TABLE kundenflotten_fahrzeuge(id TEXT PRIMARY KEY,firma_id TEXT,kennzeichen TEXT,fin TEXT);
                INSERT INTO kundenflotten_fahrzeuge VALUES('v1','f1','MOS-AA 100','');
                INSERT INTO kundenflotten_fahrzeuge VALUES('v2','f2','MOS-BB 200','');
                CREATE TABLE kundenflotten_auftraege(id TEXT PRIMARY KEY,fahrzeug_id TEXT,auftrag_id INTEGER UNIQUE,art TEXT,erstellt_am TEXT);
                CREATE TABLE leads(id INTEGER PRIMARY KEY,auftrag_id INTEGER,angebot_text TEXT,angebot_preis TEXT,angebot_status TEXT,kennzeichen TEXT,portal_json TEXT);
                INSERT INTO leads VALUES(1,NULL,'Bremsen vorne erneuern','200,00 EUR netto','angebot_abgegeben','MOS-AA 100','{"anliegen":"bremsen"}');
                INSERT INTO leads VALUES(2,NULL,'Ölwechsel','100,00 EUR netto','angebot_abgegeben','MOS-BB 200','{}');
                CREATE TABLE auftraege(id INTEGER PRIMARY KEY,werkstatt_angebot_text TEXT,werkstatt_angebot_preis TEXT,angebot_status TEXT,kennzeichen TEXT,fin_nummer TEXT,preisstand_json TEXT,reifen_rechnung_json TEXT);
                INSERT INTO auftraege VALUES(10,'Bremsen vorne erneuern','200,00 EUR netto','angenommen','MOS-AA 100','','{}','{}');
                INSERT INTO auftraege VALUES(11,'Altauftrag','100,00 EUR netto','angenommen','MOS-AA 100','','{}','{}');''')
            ensure_schema(db)
            ensure_schema(db)
        self.admin={"role":"werkstatt","id":"admin-synthetic"}
        self.manager={"role":"fuhrparkleitung","id":"manager-1","company_id":"f1"}
        self.driver={"role":"fahrer","id":"driver-1","company_id":"f1","vehicle_ids":["v1"]}
        self.parties={"auftraggeber":{"name":"Synthetische Firma"},"freigebender":{"name":"Test Leitung","identity_id":"manager-1"},
                      "rechnungsempfaenger":{"name":"Beispiel Rechnung GmbH","strasse":"Testweg 1","plz":"74821","ort":"Mosbach","land":"DE"},
                      "zahler":{"name":"Abweichender synthetischer Zahler"},"zahlungsweg":"firmenrechnung","fallart":"firma",
                      "selbstbeteiligung":"","selbstbeteiligung_quelle":""}
        self.case=self.service.bind_case(lead_id=1,company_id="f1",vehicle_id="v1",actor=self.admin)
        self.service.save_parties(self.case,self.parties,self.admin,1)
        self.offer=self.service.sync_offer(self.case,self.admin)

    def tearDown(self): self.tmp.cleanup()

    def db(self):
        db=sqlite3.connect(self.path)
        db.row_factory=sqlite3.Row
        return db

    def approve(self, **changes):
        args=dict(case_id=self.case,actor=self.manager,expected_revision=self.offer["revision"],snapshot_hash=self.offer["snapshot_hash"],request_id=uuid4().hex)
        args.update(changes)
        return self.service.approve(**args)

    def modify(self,sql,params=()):
        with self.fleet.db() as db: db.execute(sql,params)

    def bind_order(self):
        self.modify("UPDATE leads SET auftrag_id=10 WHERE id=1")
        return self.service.bind_order(1,10)

    def test_explicit_recipient_payer_and_vat_snapshot(self):
        snapshot=self.offer["snapshot"]
        self.assertEqual(snapshot["angebot"]["netto_cent"],20000)
        self.assertEqual(snapshot["angebot"]["mwst_cent"],3800)
        self.assertEqual(snapshot["angebot"]["brutto_cent"],23800)
        self.assertNotEqual(snapshot["parteien"]["rechnungsempfaenger"],snapshot["parteien"]["zahler"])

    def test_fresh_manager_approval_then_existing_order_link_and_invoice(self):
        self.approve()
        self.assertEqual(self.bind_order(),self.case)
        self.assertEqual(self.bind_order(),self.case)
        with self.fleet.db() as db:
            self.service.assert_can_start(db,10)
            self.assertEqual(self.service.invoice_recipient(db,10)["name"],"Beispiel Rechnung GmbH")
            link=db.execute("SELECT * FROM kundenflotten_auftraege WHERE auftrag_id=10").fetchall()
            self.assertEqual(len(link),1)
            self.assertEqual(link[0]["art"],"bremsen")

    def test_unapproved_start_and_accept_blocked_while_alt_order_unchanged(self):
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.assert_can_accept(db,lead_id=1)
            self.service.assert_can_start(db,11)
            self.service.assert_can_accept(db,order_id=11)
            self.assertIsNone(self.service.invoice_recipient(db,11))
        with self.assertRaises(ValueError): self.bind_order()

    def test_driver_cannot_approve_or_accept_even_with_current_snapshot(self):
        with self.assertRaises(PermissionError): self.approve(actor=self.driver)
        self.approve()
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.assert_can_accept(db,lead_id=1,actor=self.driver)
            self.service.assert_can_accept(db,lead_id=1,actor=None)

    def test_other_company_and_designated_other_manager_denied(self):
        for actor in ({"role":"fuhrparkleitung","id":"manager-2","company_id":"f2"},
                      {"role":"fuhrparkleitung","id":"manager-2","company_id":"f1"}):
            with self.assertRaises(PermissionError): self.approve(actor=actor)
        with self.assertRaises(PermissionError): self.service.view(self.case,{"role":"fahrer","id":"other","company_id":"f1","vehicle_ids":["v2"]})

    def test_driver_projection_has_no_price_parties_or_approval_identity(self):
        self.approve()
        view=self.service.view(self.case,self.driver)
        raw=json.dumps(view)
        self.assertNotIn("netto",raw)
        self.assertNotIn("snapshot",raw)
        self.assertNotIn("Testweg",raw)
        self.assertNotIn("manager-1",raw)
        self.assertFalse(view["can_approve"])
        self.assertFalse(view["online_payment_enabled"])
        self.assertEqual(len(self.service.for_vehicle("f1","v1",self.driver)),1)

    def test_workshop_requires_existing_written_evidence(self):
        with self.assertRaises(ValueError): self.approve(actor=self.admin)
        self.approve(actor=self.admin,external_reference="Synthetische Freigabe TEST-001 vom 21.10.2026")
        self.assertTrue(self.service.view(self.case,self.admin)["freigegeben"])

    def test_external_case_cannot_be_released_by_company_manager(self):
        self.parties["fallart"]="extern"
        self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        self.offer=self.service.sync_offer(self.case,self.admin)
        with self.assertRaises(PermissionError): self.approve()
        self.approve(actor=self.admin,external_reference="Geprüfte schriftliche externe Freigabe TEST-EXTERN")

    def test_same_request_is_idempotent_and_repurposed_request_is_rejected(self):
        key=uuid4().hex
        self.approve(request_id=key)
        self.approve(request_id=key)
        with self.fleet.db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) n FROM serviceprozess_ereignisse WHERE aktion='kosten_freigegeben'").fetchone()["n"],1)
        with self.assertRaises(ValueError): self.approve(request_id=key,external_reference="anderer Inhalt")

    def test_offer_change_invalidates_approval_even_before_sync(self):
        self.approve()
        self.modify("UPDATE leads SET angebot_text='Bremsen vorne und hinten',angebot_preis='400,00 EUR netto' WHERE id=1")
        self.assertFalse(self.service.view(self.case,self.manager)["freigegeben"])
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.assert_can_accept(db,lead_id=1)
        with self.assertRaises(ValueError): self.approve()
        new=self.service.sync_offer(self.case,self.admin)
        self.assertGreater(new["revision"],self.offer["revision"])
        self.offer=new
        self.approve()

    def test_party_change_invalidates_and_stale_forms_cannot_overwrite(self):
        self.approve()
        self.parties["zahler"]["name"]="Neuer Zahler"
        revision=self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        with self.assertRaises(ValueError): self.service.save_parties(self.case,self.parties,self.admin,revision-1)
        self.assertFalse(self.service.view(self.case,self.manager)["freigegeben"])

    def test_tax_change_invalidates_current_approval(self):
        self.approve()
        self.p.LEXWARE_TAX_RATE=7
        self.assertFalse(self.service.view(self.case,self.manager)["freigegeben"])
        with self.assertRaises(ValueError): self.approve()

    def test_current_vehicle_reassignment_revokes_all_financial_actions(self):
        self.approve()
        self.modify("UPDATE kundenflotten_fahrzeuge SET firma_id='f2' WHERE id='v1'")
        with self.assertRaises(ValueError): self.approve()
        self.assertFalse(self.service.view(self.case,self.manager)["freigegeben"])

    def test_corrected_order_identity_breaks_guard_without_silent_relink(self):
        self.approve();self.bind_order()
        self.modify("UPDATE auftraege SET kennzeichen='MOS-ZZ 999' WHERE id=10")
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.assert_can_start(db,10)

    def test_no_company_name_self_enrollment_or_second_vehicle_binding(self):
        with self.assertRaises(PermissionError): self.service.bind_case(lead_id=2,company_id="f1",vehicle_id="v1",actor=self.driver)
        with self.assertRaises(ValueError): self.service.bind_case(lead_id=1,company_id="f2",vehicle_id="v2",actor=self.admin)
        self.assertEqual(self.service.bind_case(lead_id=1,company_id="f1",vehicle_id="v1",actor=self.admin),self.case)

    def test_incomplete_address_and_online_method_are_rejected(self):
        for key in ("strasse","plz","ort"):
            data=json.loads(json.dumps(self.parties));data["rechnungsempfaenger"][key]=""
            with self.assertRaises(ValueError): self.service.save_parties(self.case,data,self.admin,self.offer["revision"])
        self.parties["zahlungsweg"]="mastercard"
        with self.assertRaises(ValueError): self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])

    def test_self_participation_requires_evidence_and_stays_separate(self):
        self.parties["selbstbeteiligung"]="150"
        with self.assertRaises(ValueError): self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        self.parties["selbstbeteiligung_quelle"]="Synthetischer Nachweis SB-1"
        self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        self.offer=self.service.sync_offer(self.case,self.admin)
        self.assertEqual(self.offer["snapshot"]["parteien"]["selbstbeteiligung_cent"],15000)
        self.assertEqual(self.offer["snapshot"]["angebot"]["brutto_cent"],23800)

    def test_unambiguous_gross_offer_and_rounding(self):
        self.modify("UPDATE leads SET angebot_preis='238,00 EUR brutto' WHERE id=1")
        result=self.service.sync_offer(self.case,self.admin)["snapshot"]["angebot"]
        self.assertEqual(result["netto_cent"],20000)
        self.assertEqual(result["brutto_cent"],23800)

    def test_ambiguous_or_nonfinal_offers_fail_closed(self):
        for price in ("ab 200 EUR","200 / 238 EUR","200 netto + Material","NaN","-2"):
            self.modify("UPDATE leads SET angebot_preis=? WHERE id=1",(price,))
            with self.assertRaises(ValueError): self.service.sync_offer(self.case,self.admin)
        self.modify("UPDATE leads SET angebot_preis='200',angebot_status='entwurf' WHERE id=1")
        with self.assertRaises(ValueError): self.service.sync_offer(self.case,self.admin)

    def test_no_provider_calls_for_entire_core_flow(self):
        with patch("socket.socket.connect",side_effect=AssertionError("No external connection permitted")):
            self.approve();self.bind_order()
            with self.fleet.db() as db: self.service.assert_can_start(db,10)
            self.assertFalse(self.service.view(self.case,self.manager)["online_payment_enabled"])

    def test_revocation_adapter_is_rechecked_inside_mutation_transaction(self):
        calls=[]
        def current(db,actor):
            calls.append((db,actor["id"]))
            return actor["id"] != "manager-1"
        self.p.service_actor_current=current
        with self.assertRaises(PermissionError): self.approve()
        self.assertEqual(len(calls),1)
        self.assertFalse(self.service.view(self.case,self.manager)["freigegeben"])

    def test_bound_order_price_edit_and_conflicting_confirmed_price_block(self):
        self.approve();self.bind_order()
        self.modify("UPDATE auftraege SET preisstand_json=? WHERE id=10",(json.dumps({"kunde":{"netto":"300","mwst_satz":"19"}}),))
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.assert_can_start(db,10)
            with self.assertRaises(ValueError): self.service.invoice_recipient(db,10)

    def test_unknown_tax_configuration_fails_with_validation_error(self):
        for rate in ("unknown",float("nan"),20):
            self.p.LEXWARE_TAX_RATE=rate
            with self.assertRaises(ValueError): self.service.sync_offer(self.case,self.admin)

    def test_atomic_invoice_snapshot_contains_exact_current_approved_state(self):
        self.approve();self.bind_order()
        with self.fleet.db() as db:
            invoice=self.service.approved_invoice_snapshot(db,10)
            self.assertEqual(invoice["case_id"],self.case)
            self.assertEqual(invoice["revision"],self.offer["revision"])
            self.assertEqual(invoice["snapshot_hash"],self.offer["snapshot_hash"])
            self.assertEqual(invoice["snapshot"],self.offer["snapshot"])
            self.assertEqual(invoice["recipient"],self.parties["rechnungsempfaenger"])
            self.assertEqual(invoice["snapshot"]["angebot"]["umfang"],"Bremsen vorne erneuern")
            self.assertEqual(invoice["snapshot"]["angebot"]["netto_cent"],20000)
            self.assertIsNone(self.service.approved_invoice_snapshot(db,11))

    def test_invoice_snapshot_uses_no_other_connection_or_ui_view(self):
        self.approve();self.bind_order()
        db=self.db()
        try:
            with patch.object(self.p,"get_db",side_effect=AssertionError("No second DB")), \
                 patch.object(self.fleet,"db",side_effect=AssertionError("No second transaction")), \
                 patch.object(self.service,"view",side_effect=AssertionError("No UI view")):
                invoice=self.service.approved_invoice_snapshot(db,10)
                self.assertEqual(invoice["recipient"]["strasse"],"Testweg 1")
                self.assertEqual(self.service.invoice_recipient(db,10),invoice["recipient"])
        finally:
            db.rollback();db.close()

    def test_invoice_snapshot_rejects_scope_edit_before_read(self):
        self.approve();self.bind_order()
        self.modify("UPDATE auftraege SET werkstatt_angebot_text='Zusätzliche Arbeiten' WHERE id=10")
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(db,10)

    def test_invoice_snapshot_rejects_changed_recipient_even_with_same_amount(self):
        self.approve();self.bind_order()
        self.parties["rechnungsempfaenger"]["name"]="Neue Rechnungsempfängerin"
        self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(db,10)

    def test_invoice_transaction_keeps_source_and_recipient_locked_until_end(self):
        self.approve();self.bind_order()
        invoice_db,other=self.db(),self.db()
        try:
            invoice=self.service.approved_invoice_snapshot(invoice_db,10)
            other.execute("PRAGMA busy_timeout=30")
            for query,args in (("UPDATE auftraege SET werkstatt_angebot_text=? WHERE id=10",("unapproved change",)),
                               ("UPDATE serviceprozesse SET parteien_json='{}' WHERE id=?",(self.case,))):
                with self.assertRaises(sqlite3.OperationalError): other.execute(query,args)
                other.rollback()
            self.assertEqual(invoice["snapshot"]["angebot"]["umfang"],"Bremsen vorne erneuern")
            invoice_db.commit()
            other.execute("UPDATE auftraege SET werkstatt_angebot_text='change after completion' WHERE id=10")
            other.commit()
            with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(invoice_db,10)
        finally:
            invoice_db.rollback();invoice_db.close();other.close()

    def test_tire_scope_swap_with_equal_totals_invalidates_invoice_approval(self):
        from reifenservice import berechne_reifenservice
        first=berechne_reifenservice([{"preis_id":"radwechsel_18_20","menge":1}])
        second=berechne_reifenservice([{"preis_id":"montage_17_18","menge":2}],lose_raeder=True)
        self.assertEqual(first["gesamt"],second["gesamt"])
        self.approve();self.bind_order()
        self.modify("UPDATE auftraege SET werkstatt_angebot_text='Reifenservice',werkstatt_angebot_preis='33,61 EUR netto',reifen_rechnung_json=? WHERE id=10",(json.dumps({"kalkulation":first}),))
        self.offer=self.service.sync_offer(self.case,self.admin)
        self.approve()
        with self.fleet.db() as db:
            original=self.service.approved_invoice_snapshot(db,10)
            self.assertEqual(original["snapshot"]["angebot"]["reifen_kalkulation"],first)
        self.modify("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=10",(json.dumps({"kalkulation":second}),))
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(db,10)
            with self.assertRaises(ValueError): self.service.assert_can_start(db,10)

    def test_first_actual_tire_selection_requires_new_approval(self):
        from reifenservice import berechne_reifenservice
        self.approve();self.bind_order()
        tire=berechne_reifenservice([{"preis_id":"radwechsel_18_20","menge":1}])
        self.modify("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=10",(json.dumps({"kalkulation":tire}),))
        with self.fleet.db() as db:
            with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(db,10)

    def test_tire_draft_formatting_and_timestamp_do_not_change_financial_scope(self):
        from reifenservice import berechne_reifenservice
        self.approve();self.bind_order()
        tire=berechne_reifenservice([{"preis_id":"radwechsel_18_20","menge":1}])
        self.modify("UPDATE auftraege SET werkstatt_angebot_preis='33,61 EUR netto',reifen_rechnung_json=? WHERE id=10",(json.dumps({"kalkulation":tire,"gespeichert_am":"first"}),))
        self.offer=self.service.sync_offer(self.case,self.admin);self.approve()
        self.modify("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=10",(json.dumps({"gespeichert_am":"later","kunde":{"name":"Not authoritative"},"kalkulation":tire},indent=2),))
        with self.fleet.db() as db:
            self.assertEqual(self.service.approved_invoice_snapshot(db,10)["snapshot_hash"],self.offer["snapshot_hash"])

    def test_corrupted_tire_selection_is_never_treated_as_empty(self):
        self.approve();self.bind_order()
        for value in ("{broken",'[]','{"kalkulation":{"positionen":[]}}','{"kalkulation":[]}','{"kalkulation":{}}'):
            self.modify("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=10",(value,))
            with self.fleet.db() as db:
                with self.assertRaises(ValueError): self.service.approved_invoice_snapshot(db,10)

    def test_tire_calculation_amount_must_match_the_offer_basis(self):
        from reifenservice import berechne_reifenservice
        self.approve();self.bind_order()
        tire=berechne_reifenservice([{"preis_id":"radwechsel_18_20","menge":1}])
        self.modify("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=10",(json.dumps({"kalkulation":tire}),))
        with self.assertRaises(ValueError): self.service.sync_offer(self.case,self.admin)
        self.modify("UPDATE auftraege SET werkstatt_angebot_preis='40,00 EUR brutto' WHERE id=10")
        self.offer=self.service.sync_offer(self.case,self.admin);self.approve()
        with self.fleet.db() as db:
            self.assertEqual(self.service.approved_invoice_snapshot(db,10)["snapshot"]["angebot"]["brutto_cent"],4000)

    def assert_bound_homepage_kind(self,intake,expected):
        self.modify("UPDATE leads SET portal_json=? WHERE id=1",(json.dumps(intake),))
        self.approve();self.bind_order()
        with self.fleet.db() as db:
            link=db.execute("SELECT art FROM kundenflotten_auftraege WHERE auftrag_id=10").fetchone()
            self.assertEqual(link["art"],expected)
            self.assertEqual(json.loads(db.execute("SELECT portal_json FROM leads WHERE id=1").fetchone()["portal_json"]),intake)

    def test_homepage_repair_request_is_bodywork_in_fleet_board(self):
        self.assert_bound_homepage_kind({"anliegen":"reparaturanfrage"},"karosserie")

    def test_homepage_mechanical_inspection_request_is_service(self):
        self.assert_bound_homepage_kind({"anliegen":"mechanikanfrage","mechanik_kategorien":["inspektion"]},"service")

    def test_homepage_explicit_mechanical_oil_change_is_oil_service(self):
        self.assert_bound_homepage_kind({"anliegen":"mechanikanfrage","mechanik_kategorien":["inspektion","oelwechsel"]},"oel")

    def test_homepage_explicit_brakes_prioritize_heading_without_losing_other_work(self):
        self.assert_bound_homepage_kind({"anliegen":"mechanikanfrage","mechanik_kategorien":["oelwechsel","bremsen"]},"bremsen")

    def test_homepage_tire_request_keeps_existing_tire_kind(self):
        self.assert_bound_homepage_kind({"anliegen":"reifenservice","reifen_leistungen":["radwechsel","einlagerung"]},"reifen")

    def test_restore_preserves_revisions_and_approval_history(self):
        self.approve()
        with self.fleet.db() as db:
            export={"tables":{table:[dict(r) for r in db.execute(f"SELECT * FROM {table}")] for table in BACKUP_TABLES}}
        ensure_serviceprocess_for_import(get_db=self.db,export=export)
        export["tables"]["serviceprozesse"][0]["revision"]-=1
        with self.assertRaises(ValueError): ensure_serviceprocess_for_import(get_db=self.db,export=export)

    def test_template_escapes_names_and_omits_finance_for_driver(self):
        from flask import Flask,render_template
        app=Flask(__name__,template_folder=str(ROOT/"templates"))
        app.jinja_env.globals["csrf_field"]=lambda:""
        self.parties["zahler"]["name"]="<script>synthetic</script>"
        self.service.save_parties(self.case,self.parties,self.admin,self.offer["revision"])
        with app.app_context():
            html=render_template("serviceprozess_status.html",service_case=self.service.view(self.case,self.manager))
            self.assertNotIn("<script>",html)
            self.assertIn("&lt;script&gt;",html)
            driver=render_template("serviceprozess_status.html",service_case=self.service.view(self.case,self.driver))
            self.assertNotIn("€",driver)
            self.assertNotIn("Kosten freigeben",driver)
            form=render_template("serviceprozess_verantwortung.html",service_case=self.service.view(self.case,self.admin),service_parties_action="/synthetic")
            self.assertIn('name="rechnungsempfaenger_strasse"',form)


if __name__=="__main__": unittest.main(verbosity=2)
