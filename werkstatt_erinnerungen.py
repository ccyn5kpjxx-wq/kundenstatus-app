"""Vehicle-scoped reminder projections and a strictly local test receipt.

This module has no HTTP routes, scheduler, SMTP, WhatsApp or Web Push provider.
The integrating service-app controller owns authentication, current assignment
authorization and CSRF. GET-style methods only read the existing fleet tables.
Test receipts are possible only in a Flask TESTING app with the additional
SERVICE_APP_TEST_TRANSPORT_ENABLED flag; they never represent a sent message.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from werkstatt_kundenflotten import KINDS, due_state


BACKUP_TABLES = ("kundenflotten_erinnerung_tests",)
SCHEMA_FEATURE = "kundenflotten_erinnerungen_v1"
_URGENCY = {"ueberfaellig": 0, "faellig": 1, "bald": 2}
_CHANNELS = {"email": "E-Mail", "whatsapp": "WhatsApp", "telefon": "Telefon", "keine": "Keine Kontaktfreigabe"}


class StaleReminder(ValueError):
    """The plan, appointment, assignment or contact approval has changed."""


def ensure_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS kundenflotten_erinnerung_tests (
        id TEXT PRIMARY KEY, firma_id TEXT NOT NULL REFERENCES kundenflotten_firmen(id),
        fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
        item_id TEXT NOT NULL, snapshot TEXT NOT NULL, empfaenger TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status='lokal_getestet'), erstellt_am TEXT NOT NULL)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_erinnerung_tests_fahrzeug ON kundenflotten_erinnerung_tests(fahrzeug_id,erstellt_am)")


def _today(value):
    if value is None:
        return datetime.now(ZoneInfo("Europe/Berlin")).date()
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError("Das Prüfdatum muss ein Kalendertag sein.")
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf8")).hexdigest()


class LocalTestTransport:
    """Validation only: no configurable destination or outbound implementation."""

    @staticmethod
    def require_enabled(p, recipient):
        if not (p.app.config.get("TESTING") is True and p.app.config.get("SERVICE_APP_TEST_TRANSPORT_ENABLED") is True):
            raise ValueError("Der Testtransport ist im Produktbetrieb deaktiviert.")
        if not isinstance(recipient, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._+\-]{0,63}@example\.invalid", recipient):
            raise ValueError("Nur synthetische Testempfänger unter @example.invalid sind erlaubt.")


class ReminderService:
    def __init__(self, p, fleet, *, assignment_context=None):
        self.p, self.fleet = p, fleet
        # Optional integration hook: read the current durable assignment/version
        # through the SAME connection. Its result is hashed, never exposed.
        self.assignment_context = assignment_context

    def _context(self, db, company_id, vehicle_id):
        company = self.fleet.row(db, "kundenflotten_firmen", company_id)
        vehicle = self.fleet.row(db, "kundenflotten_fahrzeuge", vehicle_id)
        if vehicle["firma_id"] != company_id:
            raise LookupError("Fahrzeug nicht gefunden.")
        row = db.execute("SELECT * FROM kundenflotten_fahrer WHERE fahrzeug_id=?", (vehicle_id,)).fetchone()
        driver = dict(row) if row else {}
        assignment = self.assignment_context(db, company_id, vehicle_id) if self.assignment_context else None
        if assignment is not None and not isinstance(assignment, dict):
            raise ValueError("Die aktuelle Fahrerzuordnung konnte nicht geprüft werden.")
        return company, vehicle, driver, assignment

    @staticmethod
    def _items(db, vehicle, today):
        items = []
        plans = db.execute("SELECT * FROM kundenflotten_plaene WHERE fahrzeug_id=? AND status='offen'", (vehicle["id"],)).fetchall()
        for row in plans:
            plan = dict(row)
            try:
                state = due_state(plan, vehicle["km"], today)
            except (ValueError, TypeError):
                continue  # Invalid legacy data requires a vehicle-file correction.
            if state not in _URGENCY:
                continue
            items.append({"id": "plan:" + plan["id"], "source_type": "plan", "source": plan,
                          "title": plan["titel"], "kind": KINDS.get(plan["art"], "Werkstattservice"),
                          "due": state, "date": plan["faellig_datum"], "km": plan["faellig_km"]})
        # A request is never a confirmed appointment. Only this explicit status
        # qualifies; no date, interval or arrival time is inferred.
        for row in db.execute("SELECT * FROM kundenflotten_termine WHERE fahrzeug_id=? AND status='bestaetigt'", (vehicle["id"],)).fetchall():
            appointment = dict(row)
            try:
                day = date.fromisoformat(appointment["datum"])
            except (ValueError, TypeError):
                continue
            if day > today + timedelta(days=30):
                continue
            state = "ueberfaellig" if day < today else "faellig" if day == today else "bald"
            items.append({"id": "termin:" + appointment["id"], "source_type": "termin", "source": appointment,
                          "title": "Bestätigter Werkstatttermin", "kind": KINDS.get(appointment["art"], "Werkstattservice"),
                          "due": state, "date": day.isoformat(), "km": None})
        return sorted(items, key=lambda item: (_URGENCY[item["due"]], item["date"] or "9999-12-31", item["id"]))

    @staticmethod
    def _public_item(item, vehicle):
        return {"id": item["id"], "vehicle_id": vehicle["id"], "plate": vehicle["kennzeichen"],
                "vehicle": vehicle["modell"], "title": item["title"], "kind": item["kind"],
                "due": item["due"], "due_date": item["date"], "due_km": item["km"],
                "km_recorded_at": vehicle["km_datum"], "source_type": item["source_type"]}

    def list_due(self, company_id, today=None):
        today = _today(today)
        with self.fleet.db() as db:
            self.fleet.row(db, "kundenflotten_firmen", company_id)
            vehicles = [dict(row) for row in db.execute("SELECT * FROM kundenflotten_fahrzeuge WHERE firma_id=? ORDER BY kennzeichen,id", (company_id,)).fetchall()]
            result = [self._public_item(item, vehicle) for vehicle in vehicles for item in self._items(db, vehicle, today)]
        return sorted(result, key=lambda item: (_URGENCY[item["due"]], item["due_date"] or "9999-12-31", item["plate"], item["id"]))

    def vehicle_inbox(self, company_id, vehicle_id, today=None):
        today = _today(today)
        with self.fleet.db() as db:
            self.fleet.row(db, "kundenflotten_firmen", company_id)
            vehicle = self.fleet.row(db, "kundenflotten_fahrzeuge", vehicle_id)
            if vehicle["firma_id"] != company_id:
                raise LookupError("Fahrzeug nicht gefunden.")
            return [self._public_item(item, vehicle) for item in self._items(db, vehicle, today)]

    @staticmethod
    def _contact(driver):
        channel = driver.get("kanal", "keine")
        consent = bool(str(driver.get("einwilligung") or "").strip())
        recipient = str(driver.get("email" if channel == "email" else "telefon") or "").strip()
        valid = bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", recipient)) if channel == "email" else bool(re.fullmatch(r"\+?[0-9][0-9 ()/\-]{5,59}", recipient)) if channel == "whatsapp" else False
        ready = bool(driver.get("name") and consent and valid and channel in {"email", "whatsapp"})
        reason = "Kontaktfreigabe dokumentiert; echter Versand bleibt deaktiviert." if ready else "Kein freigegebener automatischer Kontaktkanal. Bitte Fahrerzuordnung und Zustimmung prüfen."
        if channel == "telefon":
            reason = "Telefonischer Kontakt vereinbart; es gibt keinen automatischen Telefontransport."
        return {"channel": channel if channel in _CHANNELS else "keine", "channel_label": _CHANNELS.get(channel, _CHANNELS["keine"]),
                "contact_ready": ready, "consent_documented": consent, "contact_note": reason}

    def _preview(self, db, company_id, vehicle_id, item_id, today):
        company, vehicle, driver, assignment = self._context(db, company_id, vehicle_id)
        item = next((item for item in self._items(db, vehicle, today) if item["id"] == item_id), None)
        if item is None:
            raise StaleReminder("Diese Erinnerung ist nicht mehr aktuell. Bitte die Übersicht neu öffnen.")
        context = {"company": company, "vehicle": vehicle, "driver": driver,
                   "assignment": assignment, "item": item, "today": today.isoformat()}
        result = self._public_item(item, vehicle) | self._contact(driver)
        if self.assignment_context and (assignment is None or assignment.get("active") is not True):
            result.update(contact_ready=False, contact_note="Für dieses Fahrzeug ist keine aktive Fahrerzuordnung freigegeben.")
        date_text = f" am {item['date'][8:10]}.{item['date'][5:7]}.{item['date'][:4]}" if item["date"] else ""
        km_text = f" bei {item['km']:,} km".replace(",", ".") if item["km"] is not None else ""
        if item["source_type"] == "termin":
            message = f"Für Ihr Fahrzeug {vehicle['kennzeichen']} ist ein {item['kind']}-Termin{date_text} bestätigt. Bitte beachten Sie die persönlich vereinbarten Ankunftsdetails."
        else:
            message = f"Für Ihr Fahrzeug {vehicle['kennzeichen']} steht {item['title']}{date_text}{' oder' if date_text and km_text else ''}{km_text} an. Bitte stimmen Sie die nächsten Schritte mit Ihrer Werkstatt ab."
        result.update(snapshot=_digest(context), message=message, transport="deaktiviert", delivery_status="nicht_versendet")
        return result

    def preview(self, company_id, vehicle_id, item_id, *, today=None):
        today = _today(today)
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            return self._preview(db, company_id, vehicle_id, item_id, today)

    def record_test(self, company_id, vehicle_id, item_id, *, snapshot, request_id,
                    recipient="test@example.invalid", today=None, authorize=None):
        """Record one synthetic local receipt, never a message or a dispatch job.

        ``authorize`` may recheck the integrating controller's current principal
        under the restore lock, immediately before source/assignment validation.
        """
        LocalTestTransport.require_enabled(self.p, recipient)
        if not isinstance(request_id, str) or not re.fullmatch(r"[a-f0-9]{32}", request_id):
            raise ValueError("Bitte eine neue Testvorschau öffnen.")
        if not isinstance(snapshot, str) or not re.fullmatch(r"[a-f0-9]{64}", snapshot):
            raise StaleReminder("Die Vorschau ist ungültig. Bitte neu öffnen.")
        today = _today(today)
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            if authorize:
                authorize()
            current = self._preview(db, company_id, vehicle_id, item_id, today)
            if not hmac.compare_digest(current["snapshot"], snapshot):
                raise StaleReminder("Fälligkeit, Fahrerzuordnung oder Kontaktfreigabe wurde geändert. Bitte die Vorschau neu prüfen.")
            if not current["contact_ready"]:
                raise ValueError("Der ausgewählte Kontaktkanal ist nicht freigegeben. Es wurde nichts getestet oder versendet.")
            receipt = (company_id, vehicle_id, item_id, snapshot, recipient)
            stamp = datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="seconds")
            db.execute("""INSERT INTO kundenflotten_erinnerung_tests
                (id,firma_id,fahrzeug_id,item_id,snapshot,empfaenger,status,erstellt_am)
                VALUES (?,?,?,?,?,?,'lokal_getestet',?) ON CONFLICT(id) DO NOTHING""",
                       (request_id, *receipt, stamp))
            row = dict(db.execute("SELECT * FROM kundenflotten_erinnerung_tests WHERE id=?", (request_id,)).fetchone())
            if tuple(row[key] for key in ("firma_id", "fahrzeug_id", "item_id", "snapshot", "empfaenger")) != receipt:
                raise ValueError("Dieser Testschlüssel wurde bereits für andere Angaben verwendet.")
            return {"id": row["id"], "status": row["status"], "recipient": row["empfaenger"],
                    "created_at": row["erstellt_am"], "transport": "lokaler_test", "sent": False}
