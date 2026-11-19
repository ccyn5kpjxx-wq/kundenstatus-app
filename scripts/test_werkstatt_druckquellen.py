"""Order originals and manual selection. The smoke fixture isolates all data."""

from contextlib import closing
import unittest
from uuid import uuid4

import smoke_test
from werkstatt_druckquellen import druckquelle_status, druckquellen, lade_druckquelle

portal = smoke_test.portal


class DruckquellenTest(unittest.TestCase):
    def setUp(self):
        self.client = portal.app.test_client()
        with self.client.session_transaction() as login:
            login.update(admin=True, csrf_token="testtoken")
        self.order = portal.create_auftrag("intern", fahrzeug="Testwagen")
        self.other = portal.create_auftrag("intern", fahrzeug="Anderer Testwagen")

    def upload(self, name, *, order=None, typ="", category="standard", content=b"test", text=""):
        stored = uuid4().hex + ".pdf"
        path = portal.UPLOAD_DIR / stored
        path.write_bytes(content)
        with closing(portal.get_db()) as db:
            file_id = db.execute("""INSERT INTO dateien(auftrag_id,original_name,stored_name,quelle,kategorie,
                dokument_typ,extrahierter_text,hochgeladen_am,size)
                VALUES(?,?,?,'autohaus',?,?,?,'2026-11-19',?) RETURNING id""",
                (order or self.order, name, stored, category, typ, text, len(content))).fetchone()[0]
            db.commit()
        return file_id, path

    def test_only_originals_for_this_job_in_upload_order(self):
        for name in ("RE0811.pdf", "Guenther_KVA_Auftrag.pdf", "Angebot_Auftrag.pdf", "Schadenfoto.png",
                     "Auftragsbestaetigung.pdf"):
            self.upload(name)
        self.upload("Scan003.pdf", typ="Reparaturauftrag", text="Kundennummer 123\nAuftragsnummer 987")
        self.upload("Scan004.pdf", typ="Reparaturauftrag", text="Rechnung Nr. 42\nReparaturauftrag Nr. 17")
        first, _ = self.upload("Lackauftrag9617.pdf")
        second, _ = self.upload("Lackauftrag_HHZ123-71.jpg")
        third, _ = self.upload("Scan006.pdf", typ="Reparaturauftrag", text="Autohaus Muster\nReparaturauftrag 42")
        self.upload("Lackierauftrag_haus_1.pdf", order=self.other)
        self.upload("Lackierauftrag_haus_1.pdf", category="fertigbild")
        self.assertEqual([item["id"] for item in druckquellen(portal, self.order, tafel=True)], [first, second, third])
        self.assertIsNone(lade_druckquelle(portal, self.other, first))
        self.assertEqual(lade_druckquelle(portal, self.order, first), b"test")

    def test_manual_choice_is_per_job_and_csrf_protected(self):
        scan, _ = self.upload("Scan_12.png")
        invoice, _ = self.upload("RE0811.pdf")
        def url(order, file_id):
            return f"/admin/auftrag/{order}/datei/{file_id}/druckauswahl"
        payload = {"csrf_token": "testtoken", "auswahl": "beilegen"}
        self.assertEqual(self.client.post(url(self.order, scan), data={"auswahl": "beilegen"}).status_code, 400)
        self.assertEqual(self.client.post(url(self.other, scan), data=payload).status_code, 404)
        self.assertEqual(self.client.post(url(self.order, invoice), data=payload).status_code, 404)
        self.assertEqual(druckquellen(portal, self.order), [])
        self.assertEqual(self.client.post(url(self.order, scan), data=payload).status_code, 303)
        self.assertEqual([item["id"] for item in druckquellen(portal, self.order)], [scan])
        self.assertEqual(self.client.post(url(self.order, scan), data={**payload, "auswahl": "weglassen"}).status_code, 303)
        self.assertIsNone(lade_druckquelle(portal, self.order, scan, tafel=True))
        with self.client.session_transaction() as login:
            login.pop("admin")
            login["partner_autohaus_id"] = 1
        self.assertIn(self.client.post(url(self.order, scan), data=payload).status_code, {302, 403})
        self.assertEqual(druckquellen(portal, self.order), [])

    def test_verified_database_original_reads_without_restoring_file(self):
        file_id, path = self.upload("Lackierauftrag_partner_77.pdf", content=b"original")
        with closing(portal.get_db()) as db:
            self.assertTrue(portal.store_datei_backup(db, file_id, path))
            db.commit()
        path.unlink()
        self.assertTrue(druckquellen(portal, self.order)[0]["original_available"])
        self.assertEqual(lade_druckquelle(portal, self.order, file_id), b"original")
        self.assertFalse(path.exists())
        with closing(portal.get_db()) as db:
            db.execute("UPDATE datei_backups SET file_sha256='invalid' WHERE datei_id=?", (file_id,))
            db.commit()
        self.assertIsNone(lade_druckquelle(portal, self.order, file_id))

    def test_explicit_choice_cannot_override_non_work_document(self):
        row = {"quelle": "autohaus", "original_name": "Auftrag.pdf", "kategorie": "standard", "druck_auswahl": 1}
        for excluded in ({"quelle": "versicherung"}, {"kategorie": "rechnung"}, {"reklamation_id": 3},
                         {"dokument_typ": "DAT-Kalkulation"}, {"prozess_key": "schaden_gutachten"},
                         {"original_name": "Auftragsrechnung_441.pdf"}):
            self.assertEqual(druckquelle_status(row | excluded), "")


if __name__ == "__main__":
    unittest.main()
