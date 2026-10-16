"""Customer vehicle files and an explicitly local, synthetic customer-app demo.

No outbound messaging or customer authentication is introduced here. The demo
never reads these tables; employee credentials are never accepted as customers.
"""
from __future__ import annotations

import calendar
import hmac
import hashlib
import json
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import quote
from uuid import uuid4
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, session, url_for

CORE_BACKUP_TABLES = (
    "kundenflotten_firmen", "kundenflotten_fahrzeuge", "kundenflotten_fahrer",
    "kundenflotten_plaene", "kundenflotten_termine", "kundenflotten_reifen",
    "kundenflotten_historie", "kundenflotten_anlagen",
)
ORDER_SCHEMA_FEATURE = "kundenflotten_auftraege_v1"
BACKUP_TABLES = CORE_BACKUP_TABLES + ("kundenflotten_auftraege",)
SCHEMA_FEATURE = "kundenflotten_v1"
KINDS = {"service": "Service / Inspektion", "oel": "Ölwechsel", "reifen": "Reifen",
         "hu": "HU / AU", "bremsen": "Bremsen", "karosserie": "Karosserie / Lack", "sonstiges": "Sonstiges"}



CASE_LABELS = {1: "Auftrag erfasst", 2: "Eingeplant", 3: "In Arbeit", 4: "Arbeiten abgeschlossen", 5: "Zurückgegeben"}
CASE_COPY = {1: "Wir stimmen die nächsten Schritte mit Ihnen ab.", 2: "Ihr Werkstatttermin ist eingeplant.", 3: "Unser Team arbeitet an Ihrem Fahrzeug.", 4: "Die Arbeiten sind abgeschlossen. Die Übergabe stimmen wir mit Ihnen ab.", 5: "Das Fahrzeug wurde zurückgegeben."}


def case_view(order, kind):
    """Explicit customer-safe allowlist. Never return the raw order or tokens."""
    try:
        status = int(order.get("status") or 0)
    except (ValueError, TypeError):
        status = 0
    if status not in CASE_LABELS:
        status = 0
    stamp = str(order.get("geaendert_am") or "")
    try:
        parsed = datetime.fromisoformat(stamp)
        if parsed.tzinfo:
            parsed = parsed.astimezone(ZoneInfo("Europe/Berlin"))
        stamp = parsed.strftime("%d.%m.%Y · %H:%M")
    except ValueError:
        pass  # Legacy portal timestamps are already a human-readable local time.
    stage = {"vorarbeit":"Vorbereitung", "karosserie":"Karosseriearbeiten", "lackierung":"Lackierung", "finish":"Finish"}.get(order.get("produktion_schritt"), "") if kind == "karosserie" and status == 3 else ""
    return {"id":order["id"], "title":KINDS.get(kind,"Werkstattauftrag"), "vehicle":str(order.get("fahrzeug") or "Ihr Fahrzeug"),
            "plate":str(order.get("kennzeichen") or ""), "status":status if status in CASE_LABELS else 0,
            "label":CASE_LABELS.get(status,"Status wird geprüft"), "description":CASE_COPY.get(status,"Bitte stimmen Sie den aktuellen Stand mit uns ab."),
            "updated":stamp or "Noch kein Zeitstempel", "stage":stage,
            "steps":[{"label":label,"done":value < status,"current":value == status} for value,label in CASE_LABELS.items()]}


def ensure_kundenflotten_schema(db):
    statements = (
        """CREATE TABLE IF NOT EXISTS kundenflotten_firmen (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, kontakt TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '', telefon TEXT NOT NULL DEFAULT '',
            konditionen TEXT NOT NULL DEFAULT '', freigabe_limit_cent INTEGER,
            erstellt_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_fahrzeuge (
            id TEXT PRIMARY KEY, firma_id TEXT NOT NULL REFERENCES kundenflotten_firmen(id),
            kennzeichen TEXT NOT NULL, fin TEXT NOT NULL DEFAULT '', modell TEXT NOT NULL,
            km INTEGER NOT NULL DEFAULT 0, km_datum TEXT NOT NULL, erstellt_am TEXT NOT NULL,
            UNIQUE(firma_id, kennzeichen))""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_fahrer (
            fahrzeug_id TEXT PRIMARY KEY REFERENCES kundenflotten_fahrzeuge(id),
            name TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '', telefon TEXT NOT NULL DEFAULT '',
            kanal TEXT NOT NULL DEFAULT 'keine', einwilligung TEXT NOT NULL DEFAULT '',
            aktualisiert_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_plaene (
            id TEXT PRIMARY KEY, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
            art TEXT NOT NULL, titel TEXT NOT NULL, faellig_datum TEXT NOT NULL DEFAULT '', faellig_km INTEGER,
            intervall_monate INTEGER, intervall_km INTEGER, quelle TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'offen', version INTEGER NOT NULL DEFAULT 1,
            erstellt_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_termine (
            id TEXT PRIMARY KEY, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
            datum TEXT NOT NULL, art TEXT NOT NULL, transport TEXT NOT NULL,
            ersatzwagen INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'angefragt',
            notiz TEXT NOT NULL DEFAULT '', erstellt_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_reifen (
            fahrzeug_id TEXT PRIMARY KEY REFERENCES kundenflotten_fahrzeuge(id),
            saison TEXT NOT NULL, beschreibung TEXT NOT NULL DEFAULT '', lagerplatz TEXT NOT NULL DEFAULT '',
            bestellbedarf TEXT NOT NULL DEFAULT '', aktualisiert_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_historie (
            id TEXT PRIMARY KEY, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
            art TEXT NOT NULL, text TEXT NOT NULL, erstellt_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_anlagen (
            id TEXT PRIMARY KEY, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
            art TEXT NOT NULL, fingerprint TEXT NOT NULL, erstellt_am TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS kundenflotten_auftraege (
            id TEXT PRIMARY KEY, fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
            auftrag_id INTEGER NOT NULL UNIQUE, art TEXT NOT NULL, erstellt_am TEXT NOT NULL)""",
        "CREATE INDEX IF NOT EXISTS idx_kundenflotten_auftrag_fahrzeug ON kundenflotten_auftraege(fahrzeug_id)",
        "CREATE INDEX IF NOT EXISTS idx_kundenflotten_firma ON kundenflotten_fahrzeuge(firma_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_kundenflotten_fin ON kundenflotten_fahrzeuge(fin) WHERE fin <> ''",
        "CREATE INDEX IF NOT EXISTS idx_kundenflotten_plan ON kundenflotten_plaene(fahrzeug_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_kundenflotten_historie ON kundenflotten_historie(fahrzeug_id, erstellt_am)",
    )
    for statement in statements:
        db.execute(statement)


def _text(data, field, limit=200, required=False):
    value = str(data.get(field) or "").strip()
    if len(value) > limit or "\x00" in value:
        raise ValueError("Eine Eingabe ist zu lang oder enthält ungültige Zeichen.")
    if required and not value:
        raise ValueError("Bitte alle Pflichtfelder ausfüllen.")
    return value


def _number(data, field, maximum=3000000, required=False):
    raw = _text(data, field, 12, required)
    if not raw:
        return None
    if not re.fullmatch(r"\d+", raw) or not 0 <= int(raw) <= maximum:
        raise ValueError("Bitte eine gültige, nicht negative ganze Zahl eintragen.")
    return int(raw)


def _date(data, field, required=False):
    raw = _text(data, field, 10, required)
    try:
        value = date.fromisoformat(raw) if raw else None
        if value and not date(2000, 1, 1) <= value <= date(2100, 12, 31):
            raise ValueError()
        return value.isoformat() if value else ""
    except ValueError:
        raise ValueError("Bitte ein gültiges Datum zwischen 2000 und 2100 angeben.") from None


def _month_end(data, field):
    raw = _text(data, field, 7, True)
    if not re.fullmatch(r"(?:20[0-9]{2}|2100)-(?:0[1-9]|1[0-2])", raw):
        raise ValueError("Bitte einen gültigen Prüfmonat zwischen 2000 und 2100 angeben.")
    year, month = map(int, raw.split("-"))
    return date(year, month, calendar.monthrange(year, month)[1]).isoformat()


def _email(data):
    value = _text(data, "email", 254)
    if value and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value):
        raise ValueError("Bitte die E-Mail-Adresse prüfen.")
    return value


def _choice(data, field, choices):
    value = _text(data, field, 40, True)
    if value not in choices:
        raise ValueError("Bitte einen gültigen Eintrag auswählen.")
    return value


def _same_plate(left, right):
    # A separator also marks the district boundary: B-AB 123 and BA-B 123
    # are different vehicles. Preserve it whenever both inputs are structured.
    pattern = r"^([A-ZÄÖÜ]{1,3})[\s\-\u2010-\u2015]+([A-Z]{1,2})\s*(\d{1,4}[EH]?)$"
    left, right = left.upper(), right.upper()
    a, b = re.fullmatch(pattern, left), re.fullmatch(pattern, right)
    if a and b:
        return a.groups() == b.groups()
    # Legacy compact/free-form entries have no reliable district boundary.
    # Flag matching text for review rather than creating an accidental copy.
    compact = lambda value: re.sub(r"[\s\-\u2010-\u2015]+", "", value)
    return compact(left) == compact(right)


def _same_vehicle(vehicle, order):
    """Recheck a link when reading too: an order can be corrected later."""
    vin = str(vehicle["fin"] or "").strip().upper()
    order_vin = str(order["fin_nummer"] or "").strip().upper()
    if vin and order_vin:
        return vin == order_vin
    return bool(vehicle["kennzeichen"] and order["kennzeichen"]) and _same_plate(vehicle["kennzeichen"], order["kennzeichen"])


def _timestamp():
    return datetime.now().isoformat(timespec="seconds")


def due_state(plan, km, today=None):
    """First reached due date OR mileage; intervals are never inferred."""
    today = today or date.today()
    if plan.get("status") == "abgeschlossen":
        return "abgeschlossen"
    target = date.fromisoformat(plan["faellig_datum"]) if plan.get("faellig_datum") else None
    target_km = plan.get("faellig_km")
    if (target and target < today) or (target_km is not None and km > target_km):
        return "ueberfaellig"
    if (target and target == today) or (target_km is not None and km == target_km):
        return "faellig"
    if (target and target <= today + timedelta(days=30)) or (target_km is not None and target_km - km <= 1000):
        return "bald"
    return "geplant"


def add_months(value, months):
    absolute = value.year * 12 + value.month - 1 + months
    year, month = absolute // 12, absolute % 12 + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


class Kundenflotten:
    def __init__(self, portal):
        self.p = portal

    @contextmanager
    def db(self):
        # Mutators acquire the shared originals/restore lock BEFORE entering
        # this context. Keep it until commit/rollback and close have finished,
        # otherwise a SQLite file replacement could discard a successful write.
        conn = self.p.get_db()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            ensure_kundenflotten_schema(db)

    @staticmethod
    def row(db, table, key):
        # Table names are exclusively hard-coded by callers.
        row = db.execute(f"SELECT * FROM {table} WHERE id=?", (key,)).fetchone()
        if not row:
            raise LookupError("Datensatz nicht gefunden.")
        return dict(row)

    @staticmethod
    def history(db, vehicle_id, kind, text):
        db.execute("INSERT INTO kundenflotten_historie VALUES (?,?,?,?,?)", (uuid4().hex, vehicle_id, kind, text, _timestamp()))

    @staticmethod
    def claim_submission(db, vehicle_id, kind, data, values):
        """Durable per-form receipt, committed atomically with its record/history.

        Replays keep the first successful result even after a plan is completed.
        A fresh form can intentionally create another identical service/visit.
        """
        request_id = _text(data, "request_id", 32, True)
        if not re.fullmatch(r"[a-f0-9]{32}", request_id):
            raise ValueError("Das Formular ist veraltet. Bitte die Seite neu laden.")
        fingerprint = hashlib.sha256(json.dumps(values, sort_keys=True, ensure_ascii=False).encode("utf8")).hexdigest()
        result = db.execute("""INSERT INTO kundenflotten_anlagen (id,fahrzeug_id,art,fingerprint,erstellt_am)
            VALUES (?,?,?,?,?) ON CONFLICT(id) DO NOTHING""", (request_id, vehicle_id, kind, fingerprint, _timestamp()))
        if result.rowcount == 1:
            return True
        previous = db.execute("SELECT * FROM kundenflotten_anlagen WHERE id=?", (request_id,)).fetchone()
        if previous and previous["fahrzeug_id"] == vehicle_id and previous["art"] == kind and hmac.compare_digest(previous["fingerprint"], fingerprint):
            return False
        raise ValueError("Dieses Formular wurde bereits mit anderen Angaben verwendet. Bitte neu laden und den neuen Eintrag prüfen.")

    def companies(self):
        with self.db() as db:
            return [dict(r) for r in db.execute("""SELECT f.*, COUNT(v.id) AS anzahl
                FROM kundenflotten_firmen f LEFT JOIN kundenflotten_fahrzeuge v ON v.firma_id=f.id
                GROUP BY f.id ORDER BY f.name""").fetchall()]

    def company(self, key):
        with self.db() as db:
            company = self.row(db, "kundenflotten_firmen", key)
            vehicles = [dict(r) for r in db.execute("SELECT * FROM kundenflotten_fahrzeuge WHERE firma_id=? ORDER BY kennzeichen", (key,)).fetchall()]
            for vehicle in vehicles:
                plans = [dict(r) for r in db.execute("SELECT * FROM kundenflotten_plaene WHERE fahrzeug_id=? AND status='offen'", (vehicle["id"],)).fetchall()]
                vehicle["offen"] = sum(due_state(p, vehicle["km"]) in {"ueberfaellig", "faellig", "bald"} for p in plans)
            return company, vehicles

    def vehicle(self, company_id, key):
        with self.db() as db:
            company = self.row(db, "kundenflotten_firmen", company_id)
            vehicle = self.row(db, "kundenflotten_fahrzeuge", key)
            if vehicle["firma_id"] != company_id:
                raise LookupError("Fahrzeug gehört nicht zu dieser Firma.")
            plans = [dict(r) for r in db.execute("SELECT * FROM kundenflotten_plaene WHERE fahrzeug_id=? ORDER BY erstellt_am DESC,id", (key,)).fetchall()]
            for plan in plans:
                plan["due"] = due_state(plan, vehicle["km"])
            urgency = {"ueberfaellig": 0, "faellig": 1, "bald": 2, "geplant": 3, "abgeschlossen": 4}
            plans.sort(key=lambda plan: (urgency[plan["due"]], plan["faellig_datum"] or "9999-12-31", plan["faellig_km"] if plan["faellig_km"] is not None else 3000001, plan["id"]))
            def single(table):
                row = db.execute(f"SELECT * FROM {table} WHERE fahrzeug_id=?", (key,)).fetchone()
                return dict(row) if row else {}
            return dict(company=company, vehicle=vehicle, driver=single("kundenflotten_fahrer"), tires=single("kundenflotten_reifen"), plans=plans,
                        appointments=[dict(r) for r in db.execute("SELECT * FROM kundenflotten_termine WHERE fahrzeug_id=? ORDER BY datum DESC,id", (key,)).fetchall()],
                        history=[dict(r) for r in db.execute("SELECT * FROM kundenflotten_historie WHERE fahrzeug_id=? ORDER BY erstellt_am DESC,id DESC LIMIT 80", (key,)).fetchall()])

    @staticmethod
    def order_row(db, number):
        row = db.execute("SELECT id,fahrzeug,kennzeichen,fin_nummer,status,archiviert,produktion_schritt,geaendert_am,kunden_status_token,kunden_status_aktiv FROM auftraege WHERE id=?", (number,)).fetchone()
        return dict(row) if row else None

    def linked_cases(self, company_id, vehicle_id):
        with self.db() as db:
            vehicle = self.row(db,"kundenflotten_fahrzeuge",vehicle_id)
            if vehicle["firma_id"] != company_id:
                raise LookupError()
            cases=[]
            for link in db.execute("SELECT * FROM kundenflotten_auftraege WHERE fahrzeug_id=? ORDER BY erstellt_am DESC,id",(vehicle_id,)).fetchall():
                order=self.order_row(db,link["auftrag_id"])
                if not order or order["archiviert"] or not _same_vehicle(vehicle,order):
                    continue
                view=case_view(order,link["art"])
                # These links exist only in the authenticated internal vehicle file.
                view["customer_url"] = "/kunden-app/fall/"+quote(order["kunden_status_token"],safe="") if order["kunden_status_token"] and order["kunden_status_aktiv"] else ""
                cases.append(view)
            return cases

    def case_for_order(self, number):
        with self.db() as db:
            link=db.execute("SELECT art,fahrzeug_id FROM kundenflotten_auftraege WHERE auftrag_id=?",(number,)).fetchone()
            if not link:
                return None
            order=self.order_row(db,number)
            vehicle=self.row(db,"kundenflotten_fahrzeuge",link["fahrzeug_id"])
            return case_view(order,link["art"]) if order and not order["archiviert"] and _same_vehicle(vehicle,order) else None

    def create_company(self, data, *, require_submission=False):
        name = _text(data, "name", required=True)
        limit = _text(data, "freigabe_limit", 16)
        try:
            amount = Decimal(limit.replace(",", ".")) if limit else None
            if amount is not None and (not amount.is_finite() or not 0 <= amount <= 1000000 or amount != amount.quantize(Decimal("0.01"))):
                raise ValueError()
            cents = int(amount * 100) if amount is not None else None
        except (InvalidOperation, ValueError):
            raise ValueError("Die Freigabegrenze bitte als gültigen Eurobetrag mit höchstens zwei Nachkommastellen angeben.") from None
        request_id = _text(data, "request_id", 32, require_submission)
        if request_id and not re.fullmatch(r"[a-f0-9]{32}", request_id):
            raise ValueError("Das Formular ist veraltet. Bitte die Seite neu laden.")
        # Use the form's random receipt as the stable company ID. This keeps
        # retries atomic and survives backups without a separate receipt table.
        key = request_id or uuid4().hex
        fields = ("name", "kontakt", "email", "telefon", "konditionen", "freigabe_limit_cent")
        values = (name, _text(data, "kontakt"), _email(data), _text(data, "telefon", 60), _text(data, "konditionen", 2000), cents)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            result = db.execute("""INSERT INTO kundenflotten_firmen
                (id,name,kontakt,email,telefon,konditionen,freigabe_limit_cent,erstellt_am)
                VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING""", (key, *values, _timestamp()))
            if result.rowcount != 1:
                previous = self.row(db, "kundenflotten_firmen", key)
                if tuple(previous[field] for field in fields) != values:
                    raise ValueError("Dieses Firmenformular wurde bereits mit anderen Angaben verwendet. Bitte neu laden.")
        return key

    def create_vehicle(self, company_id, data):
        plate = re.sub(r"\s+", " ", _text(data, "kennzeichen", 20, True).upper())
        vin = _text(data, "fin", 17).upper()
        if vin and not re.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", vin):
            raise ValueError("Eine FIN muss aus 17 gültigen Zeichen bestehen; sonst das Feld zunächst leer lassen.")
        km = _number(data, "km", required=True)
        recorded = _date(data, "km_datum", True)
        if recorded > date.today().isoformat():
            raise ValueError("Der Kilometerstand darf nicht in der Zukunft liegen.")
        key = uuid4().hex
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self.row(db, "kundenflotten_firmen", company_id)
            plate_key = re.sub(r"[\s\-\u2010-\u2015]+", "", plate)
            if not plate_key or not any(char.isalnum() for char in plate_key):
                raise ValueError("Bitte ein gültiges Kennzeichen angeben.")
            existing_plates = db.execute("SELECT kennzeichen FROM kundenflotten_fahrzeuge WHERE firma_id=?", (company_id,)).fetchall()
            if any(_same_plate(item["kennzeichen"], plate) for item in existing_plates):
                raise ValueError("Dieses Kennzeichen ist bei der Firma bereits angelegt, möglicherweise in einer anderen Schreibweise.")
            if vin and db.execute("SELECT id FROM kundenflotten_fahrzeuge WHERE fin=?", (vin,)).fetchone():
                raise ValueError("Für diese FIN gibt es bereits eine Fahrzeugakte. Bitte die vorhandene Akte prüfen.")
            db.execute("INSERT INTO kundenflotten_fahrzeuge VALUES (?,?,?,?,?,?,?,?)", (key, company_id, plate, vin, _text(data, "modell", 120, True), km, recorded, _timestamp()))
            self.history(db, key, "anlage", "Fahrzeugakte angelegt. Herstellerintervalle sind individuell zu hinterlegen.")
        return key

    def change_vehicle(self, company_id, key, action, data):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            vehicle = self.row(db, "kundenflotten_fahrzeuge", key)
            if vehicle["firma_id"] != company_id:
                raise LookupError()
            if action == "km":
                km, recorded = _number(data, "km", required=True), _date(data, "km_datum", True)
                if km < vehicle["km"] or recorded < vehicle["km_datum"] or recorded > date.today().isoformat():
                    raise ValueError("Kilometerstand und Ablesedatum dürfen nicht zurückgehen oder in der Zukunft liegen.")
                changed = db.execute("UPDATE kundenflotten_fahrzeuge SET km=?,km_datum=? WHERE id=? AND km=? AND km_datum=?", (km, recorded, key, vehicle["km"], vehicle["km_datum"]))
                if changed.rowcount != 1:
                    raise ValueError("Der Kilometerstand wurde zwischenzeitlich geändert. Bitte neu laden.")
                self.history(db, key, "km", f"Kilometerstand {km:,} km am {recorded} erfasst.")
            elif action == "fahrer":
                name, email, phone = _text(data, "name", required=True), _email(data), _text(data, "telefon", 60)
                channel = _choice(data, "kanal", {"keine", "email", "telefon", "whatsapp"})
                consent = _text(data, "einwilligung", 500)
                if channel != "keine" and (not consent or (channel == "email" and not email) or (channel in {"telefon", "whatsapp"} and not phone)):
                    raise ValueError("Für den Kontaktkanal bitte Kontaktdaten und die dokumentierte Zustimmung angeben.")
                previous = db.execute("SELECT name FROM kundenflotten_fahrer WHERE fahrzeug_id=?", (key,)).fetchone()
                db.execute("DELETE FROM kundenflotten_fahrer WHERE fahrzeug_id=?", (key,))
                db.execute("INSERT INTO kundenflotten_fahrer VALUES (?,?,?,?,?,?,?)", (key, name, email, phone, channel, consent, _timestamp()))
                self.history(db, key, "fahrer", f"Fahrerzuordnung: {previous['name'] if previous else 'noch keine'} → {name}; Kontaktkanal {channel}. Kein Versand.")
            elif action == "auftrag":
                number, kind = _number(data,"auftrag_id",2147483647,True), _choice(data,"art",KINDS)
                if not number or data.get("bestaetigt") != "1":
                    raise ValueError("Bitte den Auftrag ausdrücklich mit der Fahrzeugakte abgleichen und die Zuordnung bestätigen.")
                order=self.order_row(db,number)
                if not order or order["archiviert"] or order["status"] not in (1,2,3,4):
                    raise ValueError("Bitte einen vorhandenen, nicht archivierten Werkstattauftrag auswählen.")
                if not _same_vehicle(vehicle,order):
                    raise ValueError("Kennzeichen oder FIN müssen die Zuordnung zum selben Fahrzeug bestätigen.")
                if not self.claim_submission(db,key,action,data,(number,kind)):
                    return
                previous=db.execute("SELECT fahrzeug_id,art FROM kundenflotten_auftraege WHERE auftrag_id=?",(number,)).fetchone()
                if previous:
                    if previous["fahrzeug_id"] == key and previous["art"] == kind:
                        return
                    raise ValueError("Dieser Werkstattauftrag ist bereits einer Fahrzeugakte oder einem anderen Leistungsbereich zugeordnet.")
                db.execute("INSERT INTO kundenflotten_auftraege VALUES (?,?,?,?,?)",(uuid4().hex,key,number,kind,_timestamp()))
                self.history(db,key,"auftrag",f"Werkstattauftrag {number} für {KINDS[kind]} ausdrücklich zugeordnet. Status wird direkt aus dem Auftrag gelesen. Kein Versand.")
            elif action == "plan":
                kind = _choice(data, "art", KINDS)
                due = _month_end(data, "faellig_monat") if kind == "hu" and data.get("faellig_monat") else _date(data, "faellig_datum")
                km = _number(data, "faellig_km")
                months, interval_km = _number(data, "intervall_monate", 120), _number(data, "intervall_km", 200000)
                if kind == "hu" and (not due or km is not None or months is not None or interval_km is not None):
                    raise ValueError("HU/AU bitte mit belegtem Fälligkeitsdatum erfassen, ohne Kilometerziel oder pauschales Wiederholungsintervall.")
                if not due and km is None:
                    raise ValueError("Mindestens ein Fälligkeitsdatum oder ein Kilometerziel ist erforderlich.")
                if months == 0 or interval_km == 0:
                    raise ValueError("Ein Wiederholungsintervall muss größer als null sein; für einmalige Arbeiten leer lassen.")
                if months and not due or interval_km and km is None:
                    raise ValueError("Zum Intervall muss auch das entsprechende erste Datum bzw. Kilometerziel angegeben werden.")
                title = _text(data, "titel", 160, True)
                source = _text(data, "quelle", 500, True)
                values = (kind, title, due, km, months, interval_km, source)
                if not self.claim_submission(db, key, action, data, values):
                    return
                if kind == "hu" and db.execute("SELECT id FROM kundenflotten_plaene WHERE fahrzeug_id=? AND art='hu' AND status='offen'", (key,)).fetchone():
                    raise ValueError("Für dieses Fahrzeug ist bereits eine offene HU/AU-Fälligkeit hinterlegt. Bitte zuerst den bestehenden Prüfplan prüfen.")
                db.execute("INSERT INTO kundenflotten_plaene VALUES (?,?,?,?,?,?,?,?,?,'offen',1,?)", (uuid4().hex, key, *values, _timestamp()))
                self.history(db, key, "plan", f"Plan angelegt: {title}.")
            elif action == "termin":
                day = _date(data, "datum", True)
                kind = _choice(data, "art", KINDS)
                transport = _choice(data, "transport", {"bringt", "abholen", "offen"})
                replacement, note = int(data.get("ersatzwagen") == "1"), _text(data, "notiz", 1200)
                if not self.claim_submission(db, key, action, data, (day, kind, transport, replacement, note)):
                    return
                if day < date.today().isoformat():
                    raise ValueError("Ein neuer Terminwunsch darf nicht in der Vergangenheit liegen.")
                db.execute("INSERT INTO kundenflotten_termine VALUES (?,?,?,?,?,?, 'angefragt',?,?)", (uuid4().hex, key, day, kind, transport, replacement, note, _timestamp()))
                self.history(db, key, "termin", f"Terminwunsch {day} für {KINDS[kind]} intern erfasst. Bestätigung und Kostenfreigabe offen.")
            elif action == "reifen":
                season = _choice(data, "saison", {"sommer", "winter", "ganzjahr"})
                db.execute("DELETE FROM kundenflotten_reifen WHERE fahrzeug_id=?", (key,))
                db.execute("INSERT INTO kundenflotten_reifen VALUES (?,?,?,?,?,?)", (key, season, _text(data, "beschreibung", 500), _text(data, "lagerplatz", 120), _text(data, "bestellbedarf", 500), _timestamp()))
                self.history(db, key, "reifen", "Reifensatz, Lagerort und interner Bestellbedarf aktualisiert. Keine Bestellung ausgelöst.")
            else:
                raise ValueError("Unbekannte Aktion.")

    def complete_plan(self, company_id, vehicle_id, plan_id, data):
        completed, km = _date(data, "erledigt_am", True), _number(data, "km", required=True)
        version = _number(data, "version", required=True)
        if completed > date.today().isoformat():
            raise ValueError("Arbeiten können nicht für die Zukunft abgeschlossen werden.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            vehicle = self.row(db, "kundenflotten_fahrzeuge", vehicle_id)
            plan = self.row(db, "kundenflotten_plaene", plan_id)
            if vehicle["firma_id"] != company_id or plan["fahrzeug_id"] != vehicle_id:
                raise LookupError()
            if plan["version"] != version or plan["status"] != "offen":
                raise ValueError("Dieser Stand wurde bereits geändert. Bitte die Seite neu laden.")
            if km < vehicle["km"] or completed < vehicle["km_datum"]:
                raise ValueError("Für historische Arbeiten bitte zuerst die Fahrzeugakte prüfen; der aktuelle Kilometerstand wird nicht zurückgesetzt.")
            source, months, interval_km = plan["quelle"], plan["intervall_monate"], plan["intervall_km"]
            if plan["art"] == "hu":
                if data.get("pruefung_bestanden") != "1":
                    raise ValueError("Bitte nur eine bestandene HU/AU anhand des Prüfberichts abschließen. Bei Mängeln bleibt die Fälligkeit offen.")
                next_date = _month_end(data, "naechste_hu_monat")
                source = _text(data, "pruefbericht", 500, True)
                if next_date <= completed:
                    raise ValueError("Die nächste HU/AU-Fälligkeit aus dem Prüfbericht muss nach dem Prüftag liegen.")
                next_km, months, interval_km = None, None, None
            else:
                next_date = add_months(date.fromisoformat(completed), months).isoformat() if months else ""
                next_km = km + interval_km if interval_km else None
            if next_km is not None and next_km > 3000000:
                raise ValueError("Das nächste Kilometerziel liegt über 3.000.000 km. Bitte Kilometerstand und Wartungsintervall prüfen; es wurde nichts gespeichert.")
            if next_date and next_date > "2100-12-31":
                raise ValueError("Das nächste Fälligkeitsdatum liegt außerhalb des unterstützten Zeitraums. Bitte das Wartungsintervall prüfen.")
            status = "offen" if next_date or next_km is not None else "abgeschlossen"
            changed = db.execute("UPDATE kundenflotten_plaene SET faellig_datum=?,faellig_km=?,status=?,quelle=?,intervall_monate=?,intervall_km=?,version=version+1 WHERE id=? AND version=? AND status='offen'", (next_date, next_km, status, source, months, interval_km, plan_id, version))
            if changed.rowcount != 1:
                raise ValueError("Der Plan wurde zwischenzeitlich geändert. Bitte neu laden.")
            changed_vehicle = db.execute("UPDATE kundenflotten_fahrzeuge SET km=?,km_datum=? WHERE id=? AND km=? AND km_datum=?", (km, completed, vehicle_id, vehicle["km"], vehicle["km_datum"]))
            if changed_vehicle.rowcount != 1:
                raise ValueError("Die Fahrzeugakte wurde zwischenzeitlich geändert. Bitte neu laden; es wurde kein Abschluss gespeichert.")
            report_note = f" Bisherige HU/AU-Frist: {plan['faellig_datum']}; bisherige Quelle: {plan['quelle']}; neuer Prüfbericht: {source}." if plan["art"] == "hu" else ""
            self.history(db, vehicle_id, "abschluss", f"{plan['titel']} am {completed} bei {km} km erledigt. Nächster Termin: {next_date or 'kein Zeitintervall'}; Kilometerziel: {next_km if next_km is not None else 'kein KM-Intervall'}.{report_note} Keine Nachricht versendet.")


def register_kundenflotten(portal):
    service = Kundenflotten(portal)
    bp = Blueprint("kundenflotten", __name__)

    @bp.before_request
    def csrf():
        if request.method == "POST":
            expected, supplied = session.get("csrf_token"), request.form.getlist("csrf_token")
            if not expected or not supplied or any(not hmac.compare_digest(str(expected), str(x)) for x in supplied):
                abort(400)
            if request.files or request.content_length and request.content_length > 32768:
                abort(413)

    @bp.after_request
    def private(response):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.vary.add("Cookie")
        return response

    def save(action, target):
        try:
            action()
            flash("Gespeichert. Es wurde keine Nachricht oder Bestellung versendet.", "success")
        except LookupError:
            abort(404)
        except ValueError as exc:
            flash(str(exc), "warning")
        return redirect(target, code=303)

    @bp.get("/admin/kundenflotten")
    @portal.admin_required
    def index():
        return render_template("kundenflotten_index.html", companies=service.companies(), request_token=uuid4().hex)

    @bp.post("/admin/kundenflotten/firma")
    @portal.admin_required
    def create_company():
        return save(lambda: service.create_company(request.form, require_submission=True), url_for("kundenflotten.index"))

    @bp.get("/admin/kundenflotten/firma/<company_id>")
    @portal.admin_required
    def company(company_id):
        try:
            company, vehicles = service.company(company_id)
        except LookupError:
            abort(404)
        return render_template("kundenflotten_firma.html", company=company, vehicles=vehicles, today=date.today().isoformat())

    @bp.post("/admin/kundenflotten/firma/<company_id>/fahrzeug")
    @portal.admin_required
    def create_vehicle(company_id):
        return save(lambda: service.create_vehicle(company_id, request.form), url_for("kundenflotten.company", company_id=company_id))

    @bp.get("/admin/kundenflotten/firma/<company_id>/fahrzeug/<vehicle_id>")
    @portal.admin_required
    def vehicle(company_id, vehicle_id):
        try:
            data = service.vehicle(company_id, vehicle_id)
        except LookupError:
            abort(404)
        return render_template("kundenflotten_fahrzeug.html", **data, cases=service.linked_cases(company_id,vehicle_id), kinds=KINDS, today=date.today().isoformat(), request_tokens={"plan": uuid4().hex, "termin": uuid4().hex, "auftrag":uuid4().hex})

    @bp.post("/admin/kundenflotten/firma/<company_id>/fahrzeug/<vehicle_id>/<action>")
    @portal.admin_required
    def change(company_id, vehicle_id, action):
        return save(lambda: service.change_vehicle(company_id, vehicle_id, action, request.form), url_for("kundenflotten.vehicle", company_id=company_id, vehicle_id=vehicle_id))

    @bp.post("/admin/kundenflotten/firma/<company_id>/fahrzeug/<vehicle_id>/plan/<plan_id>/erledigt")
    @portal.admin_required
    def complete(company_id, vehicle_id, plan_id):
        return save(lambda: service.complete_plan(company_id, vehicle_id, plan_id, request.form), url_for("kundenflotten.vehicle", company_id=company_id, vehicle_id=vehicle_id))

    @bp.get("/admin/kundenflotten/firma/<company_id>/fahrzeug/<vehicle_id>/kundenansicht")
    @portal.admin_required
    def vehicle_preview(company_id,vehicle_id):
        try:
            data=service.vehicle(company_id,vehicle_id)
            cases=service.linked_cases(company_id,vehicle_id)
        except LookupError:
            abort(404)
        return render_template("kundenflotten_status.html",cases=cases,vehicle_title=data["vehicle"]["modell"],plate=data["vehicle"]["kennzeichen"],internal=True,status_url=url_for("kundenflotten.vehicle_status",company_id=company_id,vehicle_id=vehicle_id),back_url=url_for("kundenflotten.vehicle",company_id=company_id,vehicle_id=vehicle_id))

    @bp.get("/admin/kundenflotten/firma/<company_id>/fahrzeug/<vehicle_id>/kundenansicht/status")
    @portal.admin_required
    def vehicle_status(company_id,vehicle_id):
        try:
            cases=service.linked_cases(company_id,vehicle_id)
        except LookupError:
            abort(404)
        return jsonify(cases=[{k:v for k,v in c.items() if k != "customer_url"} for c in cases])

    def authorized_case(token):
        order=portal.get_auftrag_by_kunden_status_token(token)
        case=service.case_for_order(order["id"]) if order else None
        if not case:
            abort(404)
        return case

    @bp.get("/kunden-app/fall/<token>")
    def customer_case(token):
        case=authorized_case(token)
        return render_template("kundenflotten_status.html",cases=[case],vehicle_title=case["vehicle"],plate=case["plate"],internal=False,status_url=url_for("kundenflotten.customer_case_status",token=token),back_url=url_for("kunden_status",token=token))

    @bp.get("/kunden-app/fall/<token>/status")
    def customer_case_status(token):
        return jsonify(cases=[authorized_case(token)])

    @bp.get("/kunden-app/vorschau")
    def demo():
        if not portal.app.config.get("KUNDENFLOTTEN_DEMO_ENABLED", False) or request.remote_addr not in {"127.0.0.1", "::1"}:
            abort(404)
        example=case_view({"id":0,"fahrzeug":"Hyundai Kona","kennzeichen":"MOS · DEMO","status":3,"geaendert_am":datetime.now().strftime("%d.%m.%Y")+" · 09:42"},"service")
        return render_template("kundenflotten_demo.html",case=example)

    portal.app.register_blueprint(bp)
    portal.kundenflotten = service
    return service
