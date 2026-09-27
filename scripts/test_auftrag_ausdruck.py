"""Synthetic read-only workshop-sheet tests, with no app import or live data."""
import contextlib
from functools import wraps
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest

from flask import Flask, redirect, session

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_auftrag_ausdruck import register_auftrag_ausdruck, workshop_sheet


class SheetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name) / "orders.db"
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            db.executescript("""CREATE TABLE auftraege (
                id INTEGER PRIMARY KEY, fahrzeug TEXT, kennzeichen TEXT, auftragsnummer TEXT,
                beschreibung TEXT, status INTEGER, produktion_schritt TEXT, annahme_datum TEXT,
                annahme_uhrzeit TEXT, start_datum TEXT, fertig_datum TEXT, fertig_uhrzeit TEXT,
                abholtermin TEXT, abhol_uhrzeit TEXT, transport_art TEXT, archiviert INTEGER,
                kunde_name TEXT, kunde_email TEXT, kontakt_telefon TEXT, werkstatt_angebot_preis TEXT,
                kunden_status_token TEXT, analyse_text TEXT, farbcode TEXT, farbton TEXT, farbton_2 TEXT
            );""")
            db.execute("INSERT INTO auftraege VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                156, "Audi A4", "MOS AB 156", "EXTERN-9042", "Stoßfänger vorne rechts lackieren.",
                3, "lackierung", "2026-09-28", "9.30", "28.09.2026", "29.09.2026", "16:15",
                "2026-09-30", "", "hol_und_bring", 0, "PRIVATE CUSTOMER", "secret@example.invalid",
                "012345678901", "9922,33", "PRIVATE-STATUS-TOKEN", "UNREVIEWED OCR AND PRICES", "LY7W", "Silber", ""))
            db.commit()
        self.queries = []

        def get_db():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            db.set_trace_callback(self.queries.append)
            return db

        def admin_required(func):
            @wraps(func)
            def guarded(*args, **kwargs):
                if not session.get("admin"):
                    return redirect("/login")
                return func(*args, **kwargs)
            return guarded

        app = Flask("sheet-test", template_folder=str(pathlib.Path(__file__).resolve().parents[1] / "templates"))
        app.secret_key = "synthetic-test-secret"
        app.config["TESTING"] = True
        self.portal = types.SimpleNamespace(
            app=app, get_db=get_db, admin_required=admin_required,
            STATUSLISTE={3: {"label": "In Arbeit"}, 4: {"label": "Fertig"}},
            PRODUKTION_SCHRITTE=(("lackierung", "Lackierung", "In der Lackierung"),),
            get_auftrag=lambda *_: (_ for _ in ()).throw(AssertionError("Hydration must not be called")),
        )
        register_auftrag_ausdruck(self.portal)
        self.client = app.test_client()

    def login(self, **roles):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(roles)

    def update(self, **fields):
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE auftraege SET " + ",".join(key + "=?" for key in fields) + " WHERE id=156", tuple(fields.values()))
            db.commit()

    def test_only_admin_can_access_and_missing_order_is_404(self):
        for roles in ({}, {"partner_autohaus_id": 1}, {"werkstatt": True}):
            self.login(**roles)
            response = self.client.get("/admin/auftrag/156/werkstattzettel")
            self.assertEqual(response.status_code, 302)
        self.assertEqual(self.queries, [])
        self.login(admin=True)
        self.assertEqual(self.client.get("/admin/auftrag/999/werkstattzettel").status_code, 404)
        self.assertEqual(self.client.post("/admin/auftrag/156/werkstattzettel").status_code, 405)

    def test_stable_internal_number_and_minimal_read_only_fields(self):
        self.login(admin=True)
        response = self.client.get("/admin/auftrag/156/werkstattzettel")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('<h1 class="number">156</h1>', html)
        self.assertIn("Externe Referenz: EXTERN-9042", html)
        self.assertIn("Stoßfänger vorne rechts lackieren.", html)
        self.assertIn("Audi A4", html)
        self.assertIn("MOS AB 156", html)
        for denied in ("PRIVATE CUSTOMER", "secret@example.invalid", "012345678901", "9922,33", "PRIVATE-STATUS-TOKEN", "UNREVIEWED OCR"):
            self.assertNotIn(denied, html)
        self.assertTrue(all(query.startswith("SELECT ") for query in self.queries))
        self.assertTrue(all("kunde_name" not in query and "preis" not in query and "analyse_text" not in query for query in self.queries))
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.assertIn("noindex", response.headers["X-Robots-Tag"])

    def test_known_times_only_and_correct_transport_meaning(self):
        sheet = workshop_sheet(self.portal, 156)
        self.assertEqual(sheet["fertig"], {"label": "Geplante Fertigstellung", "datum": "29.09.2026", "uhrzeit": "16:15"})
        self.assertEqual(sheet["termine"][0]["label"], "Abholung durch uns")
        self.assertEqual(sheet["termine"][0]["uhrzeit"], "09:30")
        self.assertEqual(sheet["termine"][2]["label"], "Rückbringung")
        self.assertEqual(sheet["termine"][2]["uhrzeit"], "")
        self.update(transport_art="standard", fertig_uhrzeit="25:70", annahme_datum="", annahme_uhrzeit="08:00")
        sheet = workshop_sheet(self.portal, 156)
        self.assertEqual(sheet["termine"][0]["label"], "Kunde bringt")
        self.assertEqual(sheet["termine"][2]["label"], "Kunde holt")
        self.assertEqual(sheet["termine"][0]["uhrzeit"], "")
        self.assertEqual(sheet["fertig"]["uhrzeit"], "")

    def test_escaping_and_common_private_free_text_are_filtered(self):
        self.update(beschreibung='<script>alert(1)</script>\nStoßfänger lackieren 500,00 EUR\nTelefon: 01234567890\nKontakt test@example.invalid', fahrzeug='<img src=x onerror=alert(1)>')
        self.login(admin=True)
        html = self.client.get("/admin/auftrag/156/werkstattzettel").get_data(as_text=True)
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<img src=x", html)
        self.assertNotIn("500,00", html)
        self.assertNotIn("01234567890", html)
        self.assertNotIn("test@example.invalid", html)
        self.assertIn("Stoßfänger lackieren", html)

    def test_unknown_data_not_invented_and_archived_visible(self):
        self.update(beschreibung="", fertig_datum="invalid", transport_art="", archiviert=1, status=4)
        sheet = workshop_sheet(self.portal, 156)
        self.assertEqual(sheet["arbeit"], "")
        self.assertEqual(sheet["fertig"]["datum"], "")
        self.assertEqual(sheet["produktion"], "")
        self.assertEqual(sheet["transport"], "Transportart noch offen")
        self.login(admin=True)
        html = self.client.get("/admin/auftrag/156/werkstattzettel").get_data(as_text=True)
        self.assertIn("Archivierter Auftrag", html)
        self.assertIn("Keine Arbeitsbeschreibung hinterlegt", html)
        self.assertIn("Termin noch offen", html)

    def test_only_stored_paint_data_shown_and_second_color_supported(self):
        sheet = workshop_sheet(self.portal, 156)
        self.assertEqual(sheet["lackdaten"], [{"label": "Farbcode", "wert": "LY7W"}, {"label": "Farbton", "wert": "Silber"}])
        self.update(farbton_2="Schwarz")
        self.assertEqual(workshop_sheet(self.portal, 156)["lackdaten"][-1], {"label": "Zweiter Farbton", "wert": "Schwarz"})
        self.update(farbcode="", farbton="", farbton_2="")
        self.assertEqual(workshop_sheet(self.portal, 156)["lackdaten"], [])

    def test_print_affordance_and_registration_are_idempotent(self):
        register_auftrag_ausdruck(self.portal)
        self.login(admin=True)
        html = self.client.get("/admin/auftrag/156/werkstattzettel").get_data(as_text=True)
        self.assertIn('onclick="window.print()"', html)
        self.assertIn("@media print", html)
        self.assertIn("size: A4 portrait", html)
        self.assertIn("Zum Assistenten:", html)


if __name__ == "__main__":
    unittest.main()
