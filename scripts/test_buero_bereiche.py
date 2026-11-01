"""Office read models and invitation drafts: synthetic data, no live application."""
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
from threading import RLock
from unittest import TestCase, main
from unittest.mock import Mock, patch

from flask import Flask, session
from jinja2 import ChoiceLoader, DictLoader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_buero_bereiche import register_office_sections, SECTION_CATEGORIES, BACKUP_TABLES, ensure_office_sections_for_import
from werkzeug.datastructures import MultiDict

SECRET = "PRIVATE-FIELD-MUST-NOT-RENDER"


class OfficeSectionTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "synthetic.db"
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.secret_key = "synthetic-office-section-tests"
        self.app.config["TESTING"] = True
        self.app.jinja_loader = ChoiceLoader([DictLoader({"buero_nav.html": "<nav>Büro</nav>"}), self.app.jinja_loader])

        def db():
            value = sqlite3.connect(self.path)
            value.row_factory = sqlite3.Row
            return value

        def who(db=None):
            if not session.get("employee"):
                return None
            return {"actor": "mitarbeiter:1", "mitarbeiter_id": 1, "mitarbeiter_name": "Office person",
                    "office_categories": tuple(session.get("categories", []))}

        self.time = SimpleNamespace(
            admin_employees=Mock(return_value=[{"id": 1, "name": "Employee A", "aktiv": 1,
                "zeitstatus": {"label": "Angestempelt", "detail_berechnet": "Seit 09:00", "private": SECRET}, "private_profile": SECRET}]),
            report=Mock(return_value={"monat": "2026-11", "berechnete_abgeschlossene_arbeitszeit": "7:15 Stunden",
                "abgeschlossene_arbeitszeit": "8:00 Stunden", "pausenabzug": "0:45 Stunden", "hinweis": "Berechnet",
                "arbeitsplan": {"private_profile": SECRET}, "bank": SECRET,
                "schichten": [{"beginn": "01.11.2026 09:00", "ende": "01.11.2026 17:00", "arbeitszeit": "8:00 Stunden", "private": SECRET}]}))
        lock = RLock()
        self.p = SimpleNamespace(app=self.app, get_db=db, office_access=SimpleNamespace(identity=who), assistant_time=self.time,
            portal_originals_operation_lock=lambda: lock, schedule_change_backup=Mock(),
            mahnungen_uebersicht=Mock(return_value=([{"nummer": "RE-TEST", "kontakt": "Customer", "betrag_label": "100,00 €", "faellig_label": "01.10.2026", "tage_ueber": 31, "stufe_name": "Erinnerung", "iban": SECRET, "text": SECRET}], [])),
            list_werkstatt_news=Mock(return_value=[{"titel": "Betriebsinfo", "nachricht": "<script>unsafe()</script>", "zeitraum_text": "Heute", "kategorie": "Betrieb", "internal": SECRET}]),
            list_bewerbungen=Mock(return_value=[{"name": "Applicant", "telefon": "01234", "bereiche": "Büro", "status": "neu", "rueckrufwunsch": "Morgen", "notiz_intern": SECRET, "email": SECRET}]),
            list_lieferanten=Mock(return_value=[{"name": "Supplier", "kategorie": "Lack", "email": "supplier@example.invalid", "telefon": "01234", "adresse": "Example street", "ort": "Exampletown", "notiz": SECRET, "password": SECRET}]),
            list_autohaeuser=Mock(return_value=[{"name": "Dealer", "portal_key": "existing-key", "zugangscode": "CODE-TEST", "portal_url": "javascript:alert(1)", "versicherungscode": SECRET, "admin_password": SECRET}]),
            send_email=Mock(side_effect=AssertionError("No sending")), mark_mail_read=Mock(side_effect=AssertionError("No mutation")))
        with closing(db()) as value:
            value.executescript("""CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT);
                CREATE TABLE mitarbeiter_urlaub(id INTEGER PRIMARY KEY,mitarbeiter_id INTEGER,start_datum TEXT,end_datum TEXT,notiz TEXT);
                CREATE TABLE mitarbeiter_urlaubsantraege(id TEXT PRIMARY KEY,mitarbeiter_id INTEGER,kalender_id INTEGER,status TEXT,start_datum TEXT,end_datum TEXT,belastung_halbtage INTEGER);
                CREATE TABLE mitarbeiter_zeitstempel(id TEXT PRIMARY KEY,mitarbeiter_id INTEGER,aktion TEXT,zeit TEXT,revision INTEGER);
                INSERT INTO mitarbeiter_zeitstempel VALUES('start',1,'kommen','2026-11-01T08:00:00+00:00',1);
                INSERT INTO mitarbeiter_zeitstempel VALUES('end',1,'gehen','2026-11-01T16:00:00+00:00',2);
                INSERT INTO mitarbeiter VALUES(1,'Employee A');
                INSERT INTO mitarbeiter_urlaub VALUES(1,1,'02.11.2026','03.11.2026','PRIVATE-FIELD-MUST-NOT-RENDER');
                CREATE TABLE autohaeuser(id INTEGER PRIMARY KEY,name TEXT);
                CREATE TABLE auftraege(id INTEGER PRIMARY KEY,autohaus_id INTEGER,fahrzeug TEXT,kennzeichen TEXT,archiviert INTEGER);
                CREATE TABLE chat_nachrichten(id INTEGER PRIMARY KEY,auftrag_id INTEGER,erstellt_am TEXT,nachricht TEXT,absender TEXT,gelesen_admin INTEGER);
                INSERT INTO autohaeuser VALUES(1,'Dealer');
                INSERT INTO auftraege VALUES(1,1,'Test car','TEST-1',0);
                INSERT INTO chat_nachrichten VALUES(1,1,'2026-11-01','Dealer question','autohaus',0);
                INSERT INTO chat_nachrichten VALUES(2,1,'2026-11-01','PRIVATE-FIELD-MUST-NOT-RENDER','versicherung',0);""")
            value.commit()
        self.service = register_office_sections(self.p)
        self.service.init_schema()
        self.staff = self.app.test_client()
        self.admin = self.app.test_client()
        self.anonymous = self.app.test_client()
        with self.admin.session_transaction() as value:
            value.update(admin=True, csrf_token="csrf-test")
        self.enable(*set(SECTION_CATEGORIES.values()))

    def enable(self, *categories):
        with self.staff.session_transaction() as value:
            value.update(employee=True, categories=list(categories), csrf_token="csrf-test")

    def get(self, section, client=None, query=""):
        return (client or self.staff).get("/werkstatt/buero/" + section + query)

    def test_each_route_requires_current_category_before_loading_data(self):
        for section in SECTION_CATEGORIES:
            self.assertEqual(self.get(section, self.anonymous).status_code, 403)
            self.enable()
            self.assertEqual(self.get(section).status_code, 403)
        self.time.admin_employees.assert_not_called()
        self.p.list_bewerbungen.assert_not_called()
        self.p.list_autohaeuser.assert_not_called()

    def test_all_nine_sections_render_and_chef_can_preview_without_employee_grant(self):
        for section in SECTION_CATEGORIES:
            for client in (self.staff, self.admin):
                response = self.get(section, client)
                self.assertEqual(response.status_code, 200, (section, response.data.decode()))
                self.assertNotIn(SECRET.encode(), response.data, section)
                self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.assertEqual(self.p.send_email.call_count, 0)
        self.assertEqual(self.p.mark_mail_read.call_count, 0)

    def test_times_contains_only_projected_clock_data(self):
        data = self.service.times("1", "2026-11")
        self.assertEqual(data["selected_employee"], 1)
        self.assertNotIn("arbeitsplan", data["report"])
        self.assertNotIn("bank", data["report"])
        self.assertNotIn("private_profile", data["employees"][0])
        self.assertNotIn("private", data["report"]["schichten"][0])
        self.assertIn(((1, "2026-11"), {}), self.time.report.call_args_list)
        for query in ("?mitarbeiter=999", "?mitarbeiter=1%20OR%201=1", "?monat=invalid"):
            self.assertEqual(self.get("arbeitszeiten", query=query).status_code, 400)

    def test_absence_excludes_notes_and_personal_originals(self):
        self.assertEqual(self.service.absences(), {"items": [{"name": "Employee A", "start_datum": "02.11.2026", "end_datum": "03.11.2026",
            "status_label": "Kalendereintrag", "dauer_label": "Nicht als Urlaubstage berechnet", "belastung_halbtage": None}]})

    def test_absence_uses_approved_request_charge_without_calendar_day_arithmetic(self):
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO mitarbeiter_urlaubsantraege VALUES('approved',1,1,'genehmigt','2026-11-02','2026-11-03',1)")
            db.commit()
        item = self.service.absences()["items"][0]
        self.assertEqual((item["status_label"], item["dauer_label"], item["belastung_halbtage"]), ("Genehmigt", "0,5 Urlaubstage", 1))
        html = self.get("abwesenheit").get_data(as_text=True)
        self.assertIn("Eigenen Urlaub beantragen", html)
        self.assertNotIn("Eigenen Urlaub beantragen", self.get("abwesenheit", self.admin).get_data(as_text=True))
        for column, value in (("mitarbeiter_id", 2), ("start_datum", "2026-11-01")):
            with closing(self.p.get_db()) as db:
                db.execute("UPDATE mitarbeiter_urlaubsantraege SET " + column + "=? WHERE id='approved'", (value,))
                db.commit()
            item = self.service.absences()["items"][0]
            self.assertEqual((item["status_label"], item["belastung_halbtage"]), ("Zuordnung prüfen", None))
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE mitarbeiter_urlaubsantraege SET status='abgelehnt'")
            db.commit()
        self.assertEqual(self.service.absences()["items"], [])

    def test_reminders_suppliers_and_applicants_project_away_private_fields(self):
        self.assertNotIn("iban", self.service.reminders()["due"][0])
        self.assertNotIn("text", self.service.reminders()["due"][0])
        self.assertNotIn("notiz_intern", self.service.applications()["items"][0])
        self.assertNotIn("email", self.service.applications()["items"][0])
        self.assertNotIn("password", self.service.suppliers()["items"][0])
        self.p.list_lieferanten.assert_called_with(include_inactive=False)

    def test_news_is_visible_only_and_escaped(self):
        response = self.get("news")
        self.assertNotIn(b"<script>", response.data)
        self.assertIn(b"&lt;script&gt;", response.data)
        self.p.list_werkstatt_news.assert_called_with(limit=200, visible_only=True)

    def test_portal_links_are_constructed_from_existing_keys_and_codes_are_separately_scoped(self):
        self.assertEqual(self.service.portals()["items"], [{"name": "Dealer", "portal_url": "/portal/existing-key"}])
        response = self.get("autohausportale")
        self.assertNotIn(b"javascript:", response.data)
        self.assertNotIn(b"CODE-TEST", response.data)
        self.assertIn(b"CODE-TEST", self.get("zugaenge").data)
        self.p.list_autohaeuser.return_value = [{"name": "Bad link", "portal_key": "//evil.invalid"}]
        self.assertEqual(self.service.portals()["items"][0]["portal_url"], "")

    def test_inbox_does_not_mutate_read_state_and_omits_insurance_extras(self):
        response = self.get("postfach")
        self.assertIn(b"Dealer question", response.data)
        self.assertIn(b"/admin/mail", response.data)
        self.assertNotIn(SECRET.encode(), response.data)
        with closing(self.p.get_db()) as db:
            self.assertEqual([r[0] for r in db.execute("SELECT gelesen_admin FROM chat_nachrichten").fetchall()], [0, 0])

    def invitation(self, **changes):
        data = dict(csrf_token="csrf-test", recipient="entered@example.invalid", appointment="2026-11-03T10:00", message="Guten Tag, wir laden Sie ein.", confirmed="1")
        data.update(changes)
        return self.staff.post("/werkstatt/buero/bewerbungen", data=data)

    def test_invitation_is_user_entered_confirmed_unsent_mailto_only(self):
        response = self.invitation()
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"mailto:entered@example.invalid?", response.data)
        self.assertIn("noch nicht versendet".encode(), response.data)
        self.assertNotIn(SECRET.encode(), response.data)
        self.p.send_email.assert_not_called()

    def test_identical_csrf_copies_work_for_invitation_and_time_review_conflicts_fail_closed(self):
        invitation = MultiDict(dict(recipient="entered@example.invalid", appointment="2026-11-03T10:00",
                                    message="Einladung", confirmed="1"))
        invitation.setlist("csrf_token", ["csrf-test", "csrf-test"])
        response = self.staff.post("/werkstatt/buero/bewerbungen", data=invitation)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"mailto:entered@example.invalid?", response.data)
        invitation.setlist("csrf_token", ["wrong", "csrf-test"])
        self.assertEqual(self.staff.post("/werkstatt/buero/bewerbungen", data=invitation).status_code, 400)
        form = MultiDict(self.review_form())
        form.setlist("csrf_token", ["csrf-test", "wrong"])
        self.assertEqual(self.save_review(form).status_code, 400)
        self.assertEqual(self.service.daily_reviews("2026-11-01")[0]["status"], "offen")
        form.setlist("csrf_token", ["csrf-test", "csrf-test"])
        self.assertEqual(self.save_review(form).status_code, 303)
        form = MultiDict(self.review_form())
        form.add("mitarbeiter_id", "2")
        self.assertEqual(self.save_review(form).status_code, 409)
        self.p.send_email.assert_not_called()

    def test_invitation_rejects_missing_csrf_unconfirmed_invalid_recipient_and_date(self):
        for changes in ({"csrf_token": "wrong"}, {"confirmed": "0"},
                        {"recipient": "one@example.invalid,two@example.invalid"},
                        {"recipient": "one@example.invalid\r\nBcc:other@example.invalid"},
                        {"appointment": "2026-02-31T10:00"}, {"message": ""}):
            self.assertEqual(self.invitation(**changes).status_code, 400)
        self.enable("lieferanten")
        self.assertEqual(self.invitation().status_code, 403)
        self.p.send_email.assert_not_called()

    def test_read_only_sections_reject_posts(self):
        for section in set(SECTION_CATEGORIES) - {"bewerbungen"}:
            self.assertEqual(self.staff.post("/werkstatt/buero/" + section, data={"csrf_token": "csrf-test"}).status_code, 405)

    def review_form(self, **changes):
        item = self.service.daily_reviews("2026-11-01")[0]
        result = dict(csrf_token="csrf-test", mitarbeiter_id="1", datum=item["datum"],
                      fingerprint=item["fingerprint"], version=str(item["version"]), status="geprueft", notiz="")
        result.update(changes)
        return result

    def save_review(self, data=None, **changes):
        return self.staff.post("/werkstatt/buero/arbeitszeiten/pruefen", data=data or self.review_form(**changes))

    def stamp_rows(self):
        with closing(self.p.get_db()) as db:
            return [dict(row) for row in db.execute("SELECT * FROM mitarbeiter_zeitstempel ORDER BY revision").fetchall()]

    def test_daily_check_preserves_original_times_and_records_actor_audit(self):
        original = self.stamp_rows()
        self.assertEqual(self.service.daily_reviews("2026-11-01")[0]["status"], "offen")
        self.assertEqual(self.save_review().status_code, 303)
        self.assertEqual(self.service.daily_reviews("2026-11-01")[0]["status"], "geprueft")
        self.assertEqual(self.stamp_rows(), original)
        with closing(self.p.get_db()) as db:
            row = db.execute("SELECT actor FROM buero_arbeitszeit_pruefung_audit").fetchone()
            self.assertEqual(row["actor"], "mitarbeiter:1")

    def test_changed_source_reopens_day_and_rejects_stale_confirmation(self):
        form = self.review_form()
        self.assertEqual(self.save_review(form).status_code, 303)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE mitarbeiter_zeitstempel SET zeit='2026-11-01T16:05:00+00:00' WHERE id='end'")
            db.commit()
        day = self.service.daily_reviews("2026-11-01")[0]
        self.assertEqual(day["status"], "offen")
        self.assertTrue(day["source_changed"])
        self.assertEqual(self.save_review(form).status_code, 409)
        self.assertEqual(self.save_review().status_code, 303)

    def test_review_cas_csrf_permissions_and_question_note(self):
        form = self.review_form()
        self.assertEqual(self.save_review(form).status_code, 303)
        self.assertEqual(self.save_review(form).status_code, 409)
        self.assertEqual(self.save_review(status="rueckfrage", notiz="").status_code, 409)
        self.assertEqual(self.save_review(status="rueckfrage", notiz="Arbeitsende bitte prüfen").status_code, 303)
        self.assertEqual(self.service.daily_reviews("2026-11-01")[0]["status"], "rueckfrage")
        self.assertEqual(self.save_review(csrf_token="wrong").status_code, 400)
        self.enable("news")
        self.assertEqual(self.save_review().status_code, 403)

    def test_review_restore_does_not_drop_or_roll_back_audit(self):
        before = {"tables": {table: [] for table in BACKUP_TABLES}}
        self.save_review()
        with self.assertRaises(ValueError):
            ensure_office_sections_for_import(self.p, export=before)
        with closing(self.p.get_db()) as db:
            current = {"tables": {table: [dict(row) for row in db.execute("SELECT * FROM " + table).fetchall()] for table in BACKUP_TABLES}}
        ensure_office_sections_for_import(self.p, export=current)

    def test_daily_count_is_bound_to_office_permission(self):
        with self.app.test_request_context():
            session.update(employee=True, categories=["arbeitszeiten"])
            original = self.service._day
            with patch.object(self.service, "_day", side_effect=lambda value=None: original(value or "2026-11-01")):
                self.assertEqual(self.service.daily_time_review_count(), 1)
                self.service.save_time_review(MultiDict(self.review_form()))
                self.assertEqual(self.service.daily_time_review_count(), 0)
                self.service.save_time_review(MultiDict(self.review_form(status="rueckfrage", notiz="Ende prüfen")))
                self.assertEqual(self.service.daily_time_review_count(), 1)
            session["categories"] = []
            with self.assertRaises(Exception) as caught:
                self.service.daily_time_review_count()
            self.assertEqual(caught.exception.code, 403)

    def test_ticking_calculated_duration_does_not_change_original_event_fingerprint(self):
        before = self.review_form()["fingerprint"]
        self.time.report.return_value["schichten"][0]["arbeitszeit"] = "9:12 Stunden"
        self.assertEqual(self.review_form()["fingerprint"], before)

    def test_cross_midnight_closing_and_pause_events_are_bound_to_start_day(self):
        self.time.report.return_value["schichten"] = [{"arbeitstag": "2026-11-01", "beginn": "01.11.2026 22:00",
            "original_ende": "02.11.2026 06:00:00", "original_ende_zeitzone": "UTC+01:00"}]
        with closing(self.p.get_db()) as db:
            db.execute("DELETE FROM mitarbeiter_zeitstempel")
            db.executemany("INSERT INTO mitarbeiter_zeitstempel VALUES(?,1,?,?,?)", [
                ("night-start", "kommen", "2026-11-01T21:00:00+00:00", 1),
                ("night-pause", "pause", "2026-11-02T01:00:00+00:00", 2),
                ("night-end", "gehen", "2026-11-02T05:00:00+00:00", 3)])
            db.commit()
        before = self.review_form()["fingerprint"]
        self.assertEqual(len(self.service.daily_reviews("2026-11-01")[0]["events"]), 3)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE mitarbeiter_zeitstempel SET zeit='2026-11-02T01:05:00+00:00' WHERE id='night-pause'")
            db.commit()
        self.assertNotEqual(self.review_form()["fingerprint"], before)

    def test_unrelated_next_day_event_does_not_reopen_review(self):
        before = self.review_form()["fingerprint"]
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO mitarbeiter_zeitstempel VALUES('tomorrow',1,'kommen','2026-11-02T08:00:00+00:00',3)")
            db.commit()
        self.assertEqual(self.review_form()["fingerprint"], before)


if __name__ == "__main__":
    main()
