"""End-to-end regressions on the synthetic database from flow_test.

Run: python scripts/test_partner_lackierauftrag_reklamation.py
"""
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

import fitz

from flow_test import portal, with_csrf


def pdf_text(data):
    with fitz.open(stream=data, filetype="pdf") as document:
        return "\n".join(page.get_text() for page in document)


class PartnerWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()
        cls.partner = portal.get_autohaus_by_slug("autohaus-pfaff")

    def setUp(self):
        self.client = portal.app.test_client()
        with self.client.session_transaction() as session:
            session["partner_autohaus_id"] = self.partner["id"]
        db = portal.get_db()
        db.execute("DELETE FROM lackierauftrag_entwuerfe WHERE autohaus_id=?", (self.partner["id"],))
        db.commit()
        db.close()
        self.notification = patch.object(
            portal, "notify_workshop_whatsapp_for_new_order", return_value=(True, [])
        )
        self.notification.start()
        self.addCleanup(self.notification.stop)

    def post(self, path, data):
        return self.client.post(path, data=with_csrf(self.client, data))

    def test_download_and_create_keep_positions_and_attach_pdf(self):
        form = {
            "typ": "Synthetischer Testwagen",
            "kennzeichen": "TEST-LACK-1",
            "farb_nr": "TEST-FARBE",
            "position_1_teil": "Kotfluegel",
            "position_1_seite": "rechts",
            "position_1_bemerkung": "Neuteil, Oberflaeche lackieren",
            "position_2_bemerkung": "Weitere Arbeit ohne Bauteilname pruefen",
        }
        response = self.post("/partner/autohaus-pfaff/lackierauftrag", {**form, "aktion": "download"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Kotfluegel", pdf_text(response.data))
        self.assertIn("Weitere Arbeit ohne Bauteilname pruefen", pdf_text(response.data))
        self.assertFalse(any(a["kennzeichen"] == "TEST-LACK-1" for a in portal.list_auftraege()))

        response = self.post("/partner/autohaus-pfaff/lackierauftrag", {**form, "aktion": "fahrzeug_anlegen"})
        self.assertEqual(response.status_code, 302)
        order_id = int(response.location.rsplit("/", 1)[-1])
        order = portal.get_auftrag(order_id)
        self.assertIn("Kotfluegel (rechts): Neuteil, Oberflaeche lackieren", order["beschreibung"])
        self.assertIn("Position 2 (Bauteil noch offen): Weitere Arbeit", order["beschreibung"])
        self.assertEqual(order["farbcode"], "TEST-FARBE")

        files = portal.list_dateien(order_id)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["dokument_zweck"], "ablage")
        self.assertTrue(files[0]["original_name"].endswith(".pdf"))
        pdf_response = self.client.get(f"/partner/autohaus-pfaff/datei/{files[0]['id']}")
        self.assertEqual(pdf_response.status_code, 200)
        self.assertIn("Kotfluegel", pdf_text(pdf_response.data))
        self.assertIn("Weitere Arbeit ohne Bauteilname pruefen", pdf_text(pdf_response.data))
        pdf_response.close()
        with self.client.session_transaction() as session:
            session["admin"] = True
        admin_pdf_response = self.client.get(f"/admin/datei/{files[0]['id']}")
        self.assertEqual(admin_pdf_response.status_code, 200)
        admin_pdf_response.close()
        self.assertEqual(portal.get_lackierauftrag_entwurf(self.partner)["angelegter_auftrag_id"], order_id)
        repeated = self.post("/partner/autohaus-pfaff/lackierauftrag", {**form, "aktion": "fahrzeug_anlegen"})
        self.assertEqual(repeated.location, response.location)
        self.assertEqual(len([a for a in portal.list_auftraege() if a["kennzeichen"] == "TEST-LACK-1"]), 1)
        self.post("/partner/autohaus-pfaff/lackierauftrag", {"aktion": "neuer_entwurf"})
        self.assertEqual(portal.get_lackierauftrag_entwurf(self.partner)["id"], 0)

    def test_failed_pdf_storage_does_not_create_second_vehicle(self):
        form = {"typ": "Synthetischer Fehlerwagen", "kennzeichen": "TEST-LACK-2", "aktion": "fahrzeug_anlegen"}
        with patch.object(portal, "save_uploads", return_value=(0, {})):
            first = self.post("/partner/autohaus-pfaff/lackierauftrag", form)
        self.assertEqual(first.status_code, 302)
        first_id = int(first.location.rsplit("/", 1)[-1])
        draft = portal.get_lackierauftrag_entwurf(self.partner)
        self.assertEqual(draft["angelegter_auftrag_id"], first_id)
        self.assertIn("Erneutes Anlegen ist gesperrt", self.client.get("/partner/autohaus-pfaff/lackierauftrag").text)

        second = self.post("/partner/autohaus-pfaff/lackierauftrag", form)
        self.assertEqual(second.location, first.location)
        orders = [a for a in portal.list_auftraege() if a["kennzeichen"] == "TEST-LACK-2"]
        self.assertEqual(len(orders), 1)
        self.assertEqual(portal.list_dateien(first_id), [])
        reset = self.post("/partner/autohaus-pfaff/lackierauftrag", {"aktion": "neuer_entwurf"})
        self.assertEqual(reset.status_code, 302)
        self.assertEqual(portal.get_lackierauftrag_entwurf(self.partner)["id"], 0)
        self.assertIsNotNone(portal.get_auftrag(first_id))

    def test_concurrent_vehicle_claim_allows_only_one_submission(self):
        data = portal.save_lackierauftrag_entwurf(self.partner, {"typ": "Gleichzeitiger Testwagen"})
        with ThreadPoolExecutor(max_workers=2) as executor:
            claimed = list(executor.map(
                lambda _: portal.claim_lackierauftrag_anlage(self.partner, data), range(2)
            ))
        self.assertEqual(sorted(claimed), [False, True])
        self.assertEqual(portal.get_lackierauftrag_entwurf(self.partner)["angelegter_auftrag_id"], -1)

    def test_concurrent_first_save_keeps_one_draft(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(
                lambda name: portal.save_lackierauftrag_entwurf(self.partner, {"typ": name}),
                ("Erster Testwagen", "Zweiter Testwagen"),
            ))
        self.assertEqual(len(results), 2)
        db = portal.get_db()
        count = db.execute(
            "SELECT COUNT(*) FROM lackierauftrag_entwuerfe WHERE autohaus_id=?",
            (self.partner["id"],),
        ).fetchone()[0]
        db.close()
        self.assertEqual(count, 1)

    def test_stale_form_cannot_change_claimed_order_pdf(self):
        first = portal.save_lackierauftrag_entwurf(self.partner, {
            "typ": "Erster Testwagen", "position_1_teil": "Erster Kotfluegel",
        })
        self.assertTrue(portal.claim_lackierauftrag_anlage(self.partner, first))
        rejected = portal.save_lackierauftrag_entwurf(self.partner, {
            "typ": "Zweiter Testwagen", "position_1_teil": "Falsche Tuer",
        })
        self.assertIsNone(rejected)
        order_id = portal.create_auftrag(
            "autohaus", autohaus_id=self.partner["id"], fahrzeug="Erster Testwagen",
        )
        portal.mark_lackierauftrag_anlage(self.partner, order_id)
        draft = portal.get_lackierauftrag_entwurf(self.partner)
        self.assertEqual(draft["daten"]["typ"], "Erster Testwagen")
        response = self.client.get("/partner/autohaus-pfaff/lackierauftrag-vorlage.pdf")
        text = pdf_text(response.data)
        self.assertIn("Erster Kotfluegel", text)
        self.assertNotIn("Falsche Tuer", text)
        response.close()

    def test_old_tab_cannot_overwrite_new_draft(self):
        first = self.post("/partner/autohaus-pfaff/lackierauftrag", {
            "aktion": "speichern", "entwurf_id": "0", "typ": "Erster Testwagen",
        })
        self.assertEqual(first.status_code, 302)
        current = portal.get_lackierauftrag_entwurf(self.partner)
        self.assertGreater(current["id"], 0)
        stale = self.post("/partner/autohaus-pfaff/lackierauftrag", {
            "aktion": "speichern", "entwurf_id": "0", "typ": "Ueberschriebener Wagen",
        })
        self.assertEqual(stale.status_code, 302)
        self.assertEqual(portal.get_lackierauftrag_entwurf(self.partner)["daten"]["typ"], "Erster Testwagen")

    def test_archived_complaint_appears_as_admin_alarm(self):
        order_id = portal.create_auftrag(
            "autohaus", autohaus_id=self.partner["id"], fahrzeug="Synthetischer Hyundai",
            kennzeichen="TEST-ARCHIV-1",
        )
        portal.archive_auftrag(order_id, 1)
        response = self.post(
            f"/partner/autohaus-pfaff/auftrag/{order_id}/reklamation",
            {"meldung": "Stoßfaenger erneut beanstandet"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(portal.get_auftrag(order_id)["archiviert"])
        alarms = [item for item in portal.list_admin_postfach_items()
                  if item["typ"] == "Alarm" and item["kennzeichen"] == "TEST-ARCHIV-1"]
        self.assertEqual(len(alarms), 1)
        self.assertIn("Archiv", alarms[0]["titel"])

        with self.client.session_transaction() as session:
            session["admin"] = True
        dashboard = self.client.get("/admin")
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn("Offene Reklamationen", dashboard.text)
        self.assertIn("Reklamation offen (1)", dashboard.text)
        postfach = self.client.get("/admin/postfach")
        self.assertEqual(postfach.status_code, 200)
        self.assertIn("mail-item-alarm", postfach.text)
        self.assertIn("Reklamation offen · Archiv", postfach.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
