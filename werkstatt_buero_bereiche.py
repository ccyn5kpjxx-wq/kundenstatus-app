"""Office-only, explicitly projected read models and manually composed invitations."""
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import sqlite3
from urllib.parse import quote, urlencode
from uuid import uuid4
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request, session, url_for

BACKUP_TABLES = ("buero_arbeitszeit_pruefungen", "buero_arbeitszeit_pruefung_audit")
SCHEMA_FEATURE = "office_time_review_v1"

SECTION_CATEGORIES = {
    "arbeitszeiten": "arbeitszeiten", "abwesenheit": "urlaub", "mahnungen": "mahnungen",
    "news": "news", "bewerbungen": "bewerbungen", "lieferanten": "lieferanten",
    "postfach": "mailbox", "zugaenge": "partnerzugaenge", "autohausportale": "autohausportale",
}
SECTION_TITLES = {
    "arbeitszeiten": "Arbeitszeiten", "abwesenheit": "Urlaub und Abwesenheit", "mahnungen": "Mahnungen",
    "news": "News", "bewerbungen": "Bewerbungen", "lieferanten": "Lieferanten",
    "postfach": "Postfach und E-Mails", "zugaenge": "Autohauszugänge", "autohausportale": "Autohausportale",
}
OFFICE_SECTION_ENDPOINTS = {"office_sections." + key: frozenset({"GET", "HEAD"}) for key in SECTION_CATEGORIES}
OFFICE_SECTION_ENDPOINTS["office_sections.bewerbungen"] = frozenset({"GET", "HEAD", "POST"})
OFFICE_SECTION_ENDPOINTS["office_sections.arbeitszeit_pruefen"] = frozenset({"POST"})


def _pick(row, keys):
    value = dict(row or {})
    return {key: value.get(key, "") for key in keys}


def _text(value, limit=1000):
    return str(value or "").replace("\x00", "")[:limit]


class OfficeSections:
    def __init__(self, portal):
        self.p = portal

    def identity(self, section, db=None):
        if session.get("admin"):
            return {"actor": "admin", "mitarbeiter_name": "Chef-Vorschau", "office_categories": tuple(SECTION_CATEGORIES.values())}
        who = self.p.office_access.identity(db)
        if not who or SECTION_CATEGORIES[section] not in who.get("office_categories", ()):
            abort(403)
        return who

    @contextmanager
    def db(self):
        connection = self.p.get_db()
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS buero_arbeitszeit_pruefungen (
                mitarbeiter_id INTEGER NOT NULL, datum TEXT NOT NULL,
                status TEXT NOT NULL, notiz TEXT NOT NULL DEFAULT '',
                quell_fingerprint TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
                geaendert_am TEXT NOT NULL, geaendert_von TEXT NOT NULL,
                PRIMARY KEY(mitarbeiter_id,datum))""")
            db.execute("""CREATE TABLE IF NOT EXISTS buero_arbeitszeit_pruefung_audit (
                id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL, datum TEXT NOT NULL,
                actor TEXT NOT NULL, details_json TEXT NOT NULL, erstellt_am TEXT NOT NULL)""")

    @staticmethod
    def _day(value=None):
        value = value or datetime.now(ZoneInfo("Europe/Berlin")).date().isoformat()
        if not isinstance(value, str) or not re.fullmatch(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError("Prüftag als Datum auswählen.")
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Prüftag ungültig.") from None

    def day_snapshot(self, mid, day, db):
        """Hash original events, never durations ticking while a shift is open."""
        day = self._day(day)
        start = datetime.combine(day, datetime.min.time(), ZoneInfo("Europe/Berlin")).astimezone(timezone.utc)
        end = datetime.combine(day + timedelta(days=1), datetime.min.time(), ZoneInfo("Europe/Berlin")).astimezone(timezone.utc)
        error, shifts = "", []
        try:
            report = self.p.assistant_time.report(mid, day.strftime("%Y-%m"))
            for raw in report.get("schichten", []):
                workday = raw.get("arbeitstag")
                if not workday and raw.get("beginn"):
                    workday = datetime.strptime(raw["beginn"][:10], "%d.%m.%Y").date().isoformat()
                if workday != day.isoformat():
                    continue
                shifts.append(_pick(raw, ("berechneter_beginn", "berechnetes_ende", "beginn", "ende",
                                          "berechnete_arbeitszeit", "arbeitszeit", "offen", "pruefen")))
                # Include the closing event of a cross-midnight shift attributed
                # to this workday, plus all intervening original pause events.
                if raw.get("original_ende"):
                    offset = raw.get("original_ende_zeitzone", "")
                    if re.fullmatch(r"UTC[+-][0-9]{2}:[0-9]{2}", offset):
                        minutes = (int(offset[4:6]) * 60 + int(offset[7:9])) * (-1 if offset[3] == "-" else 1)
                        instant = datetime.strptime(raw["original_ende"], "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone(timedelta(minutes=minutes))).astimezone(timezone.utc)
                        end = max(end, instant + timedelta(microseconds=1))
        except ValueError as exc:
            error = str(exc)
        events = [dict(row) for row in db.execute("""SELECT id,aktion,zeit,revision FROM mitarbeiter_zeitstempel
            WHERE mitarbeiter_id=? AND zeit>=? AND zeit<? ORDER BY revision,id""", (mid, start.isoformat(), end.isoformat())).fetchall()]
        source = {"mitarbeiter_id": mid, "datum": day.isoformat(), "originalstempel": events, "auswertungsfehler": error}
        fingerprint = hashlib.sha256(json.dumps(source, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        display = []
        for event in events:
            try:
                when = datetime.fromisoformat(event["zeit"]).astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M:%S")
            except (TypeError, ValueError):
                when = "Zeitstempel ungültig"
            display.append({"action": {"kommen": "Beginn", "gehen": "Ende", "pause": "Pause", "weiter": "Weiter"}.get(event["aktion"], "Prüfen"), "time": when})
        return {"fingerprint": fingerprint, "shifts": shifts, "events": display, "error": error}

    def daily_reviews(self, day=None, *, db=None):
        day = self._day(day).isoformat()
        if db is None:
            with self.db() as connection:
                return self.daily_reviews(day, db=connection)
        items = []
        for employee in self.p.assistant_time.admin_employees():
            if not employee.get("aktiv"):
                continue
            mid = employee["id"]
            snapshot = self.day_snapshot(mid, day, db)
            row = db.execute("SELECT * FROM buero_arbeitszeit_pruefungen WHERE mitarbeiter_id=? AND datum=?", (mid, day)).fetchone()
            changed = bool(row and row["quell_fingerprint"] != snapshot["fingerprint"])
            status = row["status"] if row and not changed else "offen"
            items.append({"mitarbeiter_id": mid, "name": employee["name"], "datum": day,
                          "status": status, "version": row["version"] if row else 0,
                          "notiz": row["notiz"] if row and not changed else "",
                          "source_changed": changed, **snapshot})
        return items

    def daily_time_review_count(self):
        self.identity("arbeitszeiten")
        return sum(item["status"] != "geprueft" for item in self.daily_reviews())

    def save_time_review(self, form):
        for key in ("mitarbeiter_id", "datum", "status", "notiz", "fingerprint", "version"):
            if len(form.getlist(key)) != 1:
                raise ValueError("Prüfformular eindeutig ausfüllen.")
        if (not re.fullmatch(r"[1-9][0-9]{0,9}", form["mitarbeiter_id"])
                or not re.fullmatch(r"[0-9]{1,10}", form["version"])
                or not re.fullmatch(r"[a-f0-9]{64}", form["fingerprint"])):
            raise ValueError("Prüfstand ungültig. Seite neu laden.")
        mid, day, version = int(form["mitarbeiter_id"]), self._day(form["datum"]).isoformat(), int(form["version"])
        status, note = form["status"], form["notiz"].strip()
        if status not in {"geprueft", "rueckfrage"} or len(note) > 2000 or "\x00" in note or (status == "rueckfrage" and not note):
            raise ValueError("Geprüft oder Rückfrage wählen; für Rückfragen eine sachliche Notiz ergänzen.")
        now = datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="seconds")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity("arbeitszeiten", db)
            current = next((row for row in self.daily_reviews(day, db=db) if row["mitarbeiter_id"] == mid), None)
            if current is None or current["version"] != version or current["fingerprint"] != form["fingerprint"]:
                raise ValueError("Zeitstempel oder Prüfstand haben sich geändert. Neu laden und erneut prüfen.")
            old = db.execute("SELECT * FROM buero_arbeitszeit_pruefungen WHERE mitarbeiter_id=? AND datum=?", (mid, day)).fetchone()
            values = (status, note, current["fingerprint"], now, who["actor"])
            if old:
                changed = db.execute("""UPDATE buero_arbeitszeit_pruefungen SET status=?,notiz=?,quell_fingerprint=?,
                    geaendert_am=?,geaendert_von=?,version=version+1 WHERE mitarbeiter_id=? AND datum=? AND version=?
                    RETURNING version""", values + (mid, day, version)).fetchone()
            else:
                changed = db.execute("""INSERT INTO buero_arbeitszeit_pruefungen
                    (mitarbeiter_id,datum,status,notiz,quell_fingerprint,geaendert_am,geaendert_von,version)
                    VALUES(?,?,?,?,?,?,?,1) RETURNING version""", (mid, day) + values).fetchone()
            if not changed:
                raise ValueError("Prüfstand wurde inzwischen geändert. Seite neu laden.")
            details = {"vorher": dict(old) if old else None,
                       "nachher": {"status": status, "notiz": note, "fingerprint": current["fingerprint"], "version": changed["version"]}}
            db.execute("""INSERT INTO buero_arbeitszeit_pruefung_audit
                (id,mitarbeiter_id,datum,actor,details_json,erstellt_am) VALUES(?,?,?,?,?,?)""",
                       (uuid4().hex, mid, day, who["actor"], json.dumps(details, ensure_ascii=False), now))
        hook = getattr(self.p, "schedule_change_backup", None)
        if callable(hook):
            hook("buero-arbeitszeit-pruefung")
        return day

    def times(self, employee="", month="", day=None):
        month = month or datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m")
        if not re.fullmatch(r"20[0-9]{2}-(?:0[1-9]|1[0-2])", month):
            raise ValueError("Monat im Format Jahr-Monat auswählen.")
        employees = []
        for raw in self.p.assistant_time.admin_employees():
            row = _pick(raw, ("id", "name", "aktiv"))
            row["zeitstatus"] = _pick(dict(raw).get("zeitstatus"), ("label", "detail_berechnet"))
            employees.append(row)
        ids = {row["id"] for row in employees}
        if employee:
            if not re.fullmatch(r"[1-9][0-9]{0,9}", str(employee)) or int(employee) not in ids:
                raise ValueError("Mitarbeiter aus der angezeigten Liste auswählen.")
            mid = int(employee)
        else:
            mid = next((row["id"] for row in employees if row["aktiv"]), None)
        day = self._day(day).isoformat()
        result = {"employees": employees, "selected_employee": mid, "month": month, "report": None,
                  "review_day": day, "daily_reviews": self.daily_reviews(day)}
        if mid is not None:
            source = self.p.assistant_time.report(mid, month)
            report = _pick(source, ("monat", "berechnete_abgeschlossene_arbeitszeit", "abgeschlossene_arbeitszeit",
                                    "pausenabzug", "berechnung_pruefen", "pruefen", "hinweis"))
            report["schichten"] = [_pick(row, ("beginn", "ende", "berechneter_beginn", "berechnetes_ende",
                                               "berechnete_arbeitszeit", "arbeitszeit", "pause", "offen", "pruefen"))
                                     for row in source.get("schichten", [])]
            result["report"] = report
        return result

    def absences(self):
        # Dates only. Neither illness originals/diagnoses nor free-text notes
        # from personnel or vacation records enter the office read model.
        with closing(self.p.get_db()) as db:
            rows = db.execute("""SELECT m.name,u.start_datum,u.end_datum FROM mitarbeiter_urlaub u
                JOIN mitarbeiter m ON m.id=u.mitarbeiter_id ORDER BY u.id DESC LIMIT 300""").fetchall()
        return {"items": [_pick(row, ("name", "start_datum", "end_datum")) for row in rows]}

    def reminders(self):
        due, later = self.p.mahnungen_uebersicht()
        keys = ("nummer", "kontakt", "betrag_label", "faellig_label", "tage_ueber", "stufe_name",
                "bereits_name", "naechste_name", "naechste_in_tagen")
        return {"due": [_pick(row, keys) for row in due], "later": [_pick(row, keys) for row in later]}

    def news(self):
        return {"items": [_pick(row, ("titel", "nachricht", "zeitraum_text", "kategorie"))
                          for row in self.p.list_werkstatt_news(limit=200, visible_only=True)]}

    def applications(self):
        return {"items": [_pick(row, ("name", "telefon", "bereiche", "status", "rueckrufwunsch"))
                          for row in self.p.list_bewerbungen()]}

    def suppliers(self):
        return {"items": [_pick(row, ("name", "kategorie", "email", "telefon", "ort", "adresse"))
                          for row in self.p.list_lieferanten(include_inactive=False)]}

    def portals(self, *, show_codes=False):
        items = []
        for raw in self.p.list_autohaeuser():
            row = dict(raw)
            if row.get("aktiv", 1) in (False, 0):
                continue
            key = row.get("portal_key")
            # Construct a same-origin, existing portal path. Never trust an
            # arbitrary URL from an editable row as an href.
            path = "/portal/" + key if isinstance(key, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", key) else ""
            item = {"name": _text(row.get("name"), 200), "portal_url": path}
            if show_codes:
                item["zugangscode"] = _text(row.get("zugangscode"), 120)
            items.append(item)
        return {"items": items}

    def inbox(self):
        # Existing dealer chats are read without opening/marking/hiding them.
        # Do not reuse the admin aggregate containing insurance/bank extras.
        with closing(self.p.get_db()) as db:
            rows = db.execute("""SELECT c.erstellt_am,c.nachricht,a.fahrzeug,a.kennzeichen,h.name AS autohaus_name
                FROM chat_nachrichten c JOIN auftraege a ON a.id=c.auftrag_id
                LEFT JOIN autohaeuser h ON h.id=a.autohaus_id
                WHERE c.absender='autohaus' AND c.gelesen_admin=0 AND a.archiviert=0
                ORDER BY c.id DESC LIMIT 80""").fetchall()
        return {"items": [{key: _text(dict(row).get(key), 500 if key == "nachricht" else 200)
                           for key in ("erstellt_am", "nachricht", "fahrzeug", "kennzeichen", "autohaus_name")}
                          for row in rows], "mail_url": "/admin/mail"}

    @staticmethod
    def invitation(form):
        for key in ("recipient", "appointment", "message", "confirmed"):
            if len(form.getlist(key)) != 1:
                raise ValueError("Alle Einladungsfelder eindeutig ausfüllen.")
        recipient = form["recipient"].strip()
        if len(recipient) > 254 or not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", recipient):
            raise ValueError("Eine gültige Empfänger-E-Mail-Adresse eingeben.")
        if any(ord(char) < 32 for char in recipient):
            raise ValueError("Empfänger ungültig.")
        when = form["appointment"]
        if not re.fullmatch(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}", when):
            raise ValueError("Termin mit Datum und Uhrzeit eingeben.")
        try:
            parsed = datetime.strptime(when, "%Y-%m-%dT%H:%M")
        except ValueError:
            raise ValueError("Termin ungültig.") from None
        text = form["message"].strip()
        if not text or len(text) > 4000 or "\x00" in text or form["confirmed"] != "1":
            raise ValueError("Einladungstext und Empfänger/Termin prüfen und ausdrücklich bestätigen.")
        label = parsed.strftime("%d.%m.%Y um %H:%M Uhr")
        subject = "Einladung zum Vorstellungsgespräch"
        body = text + "\n\nTermin: " + label + " (deutsche Ortszeit)"
        url = "mailto:" + quote(recipient, safe="@") + "?" + urlencode({"subject": subject, "body": body}, quote_via=quote)
        return {"recipient": recipient, "appointment": label, "message": body, "mailto_url": url}


def register_office_sections(p):
    service = OfficeSections(p)
    p.office_sections = service
    bp = Blueprint("office_sections", __name__)

    def page(section, model):
        who = service.identity(section)
        error, status, draft = "", 200, None
        if not session.get("csrf_token"):
            session["csrf_token"] = secrets.token_urlsafe(32)
        if request.method == "POST":
            tokens = request.form.getlist("csrf_token")
            expected = session.get("csrf_token")
            if not tokens or not isinstance(expected, str) or any(not hmac.compare_digest(expected, str(token)) for token in tokens):
                abort(400)
            try:
                draft = service.invitation(request.form)
            except ValueError as exc:
                error, status = str(exc), 400
        try:
            data = model()
        except ValueError as exc:
            data, error, status = {}, str(exc), 400
        return render_template("buero_bereiche.html", section=section, title=SECTION_TITLES[section],
                               data=data, error=error, draft=draft, csrf_token=session["csrf_token"],
                               office_user=who, buero_user=who, office_categories=who.get("office_categories", ()),
                               office_preview=bool(session.get("admin")), active_area=section), status

    @bp.get("/werkstatt/buero/arbeitszeiten")
    def arbeitszeiten():
        return page("arbeitszeiten", lambda: service.times(request.args.get("mitarbeiter", ""), request.args.get("monat", ""), request.args.get("tag")))

    @bp.post("/werkstatt/buero/arbeitszeiten/pruefen")
    def arbeitszeit_pruefen():
        service.identity("arbeitszeiten")
        tokens, expected = request.form.getlist("csrf_token"), session.get("csrf_token")
        if not tokens or not isinstance(expected, str) or any(not hmac.compare_digest(expected, str(token)) for token in tokens):
            abort(400)
        try:
            day = service.save_time_review(request.form)
        except ValueError as exc:
            return str(exc), 409
        return redirect(url_for("office_sections.arbeitszeiten", tag=day), code=303)

    @bp.get("/werkstatt/buero/abwesenheit")
    def abwesenheit(): return page("abwesenheit", service.absences)

    @bp.get("/werkstatt/buero/mahnungen")
    def mahnungen(): return page("mahnungen", service.reminders)

    @bp.get("/werkstatt/buero/news")
    def news(): return page("news", service.news)

    @bp.route("/werkstatt/buero/bewerbungen", methods=["GET", "POST"])
    def bewerbungen(): return page("bewerbungen", service.applications)

    @bp.get("/werkstatt/buero/lieferanten")
    def lieferanten(): return page("lieferanten", service.suppliers)

    @bp.get("/werkstatt/buero/postfach")
    def postfach(): return page("postfach", service.inbox)

    @bp.get("/werkstatt/buero/zugaenge")
    def zugaenge(): return page("zugaenge", lambda: service.portals(show_codes=True))

    @bp.get("/werkstatt/buero/autohausportale")
    def autohausportale(): return page("autohausportale", service.portals)

    @bp.after_request
    def private(response):
        response.headers.update({"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer",
                                 "X-Content-Type-Options": "nosniff", "X-Robots-Tag": "noindex, nofollow"})
        return response

    p.app.register_blueprint(bp)
    return service


def ensure_office_sections_for_import(p, *, export=None, imported_db=None, target=None, **_):
    """Preserve daily reviews and their audit during destructive restore."""
    own, source = target is None, None
    db = target if target is not None else p.get_db()
    try:
        with p.portal_originals_operation_lock():
            if imported_db is not None:
                source = sqlite3.connect(Path(imported_db).resolve().as_uri() + "?mode=ro", uri=True)
                source.row_factory = sqlite3.Row
            for table in BACKUP_TABLES:
                current = [dict(row) for row in db.execute("SELECT * FROM " + table).fetchall()]
                if source is not None:
                    exists = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    incoming = [dict(row) for row in source.execute("SELECT * FROM " + table).fetchall()] if exists else []
                else:
                    incoming = (export or {}).get("tables", {}).get(table, [])
                if not isinstance(incoming, list) or any(not isinstance(row, dict) for row in incoming):
                    raise ValueError()
                def indexed(rows):
                    return {(row.get("mitarbeiter_id"), row.get("datum")) if table == BACKUP_TABLES[0] else row.get("id"): row for row in rows}
                if len(indexed(incoming)) != len(incoming) or indexed(current) != indexed(incoming):
                    raise ValueError()
    except (TypeError, KeyError, ValueError, sqlite3.Error):
        raise ValueError("Import gestoppt: Arbeitszeit-Tagesprüfungen oder deren Audit würden verändert. Aktuelle vollständige Sicherung verwenden.") from None
    finally:
        if source is not None:
            source.close()
        if own:
            db.close()
