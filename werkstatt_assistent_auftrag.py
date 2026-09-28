"""Narrow, explicitly confirmed workshop intake and paint-data mutations.

WorkshopOrderActions.preview_color/preview_create return JSON-safe previews;
the authenticated caller persists them server-side and obtains a separate human
confirmation. confirm accepts that saved preview and a stable server action key,
never model-supplied authorization. Fresh identity must include dokumentieren.
No app import, OCR interpretation, customer messaging, financial data, automatic
work approval or guessed order number. Existing auftraege.id is the workshop ID.

Intake writes use one transaction with the existing assistent_fortschritt_audit
replay table and status_log. A trusted attach_source(db, new_order_id) callback
can attach an actor-owned, server-validated staged upload in that transaction;
it must not commit or close db. Replays skip it. Database rollback cannot undo
external file/network side effects, so attachment adapters must manage those.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from werkstatt_fortschritt import ProgressError, init_schema as init_progress_schema


BERLIN = ZoneInfo("Europe/Berlin")
COLOR_FIELDS = {"farbcode": 80, "farbton": 120, "farbton_2": 120}
CONTACT_FIELDS = {"kunde_name": 180, "kontakt_telefon": 50, "kunde_email": 254}
CREATE_FIELDS = {
    **CONTACT_FIELDS,
    "fahrzeug": 180, "kennzeichen": 24, "fin_nummer": 17,
    "hsn_nummer": 4, "tsn_nummer": 8, "beschreibung": 3000, **COLOR_FIELDS,
}
COLOR_SELECT = "id,fahrzeug,kennzeichen,fin_nummer,status,archiviert,geaendert_am,farbcode,farbton,farbton_2"
CONTACT_SELECT = "id,fahrzeug,kennzeichen,fin_nummer,status,archiviert,geaendert_am,kunde_name,kontakt_telefon,kunde_email"
COLOR_LABELS = {"farbcode": "Farbcode", "farbton": "Farbton", "farbton_2": "Zweiter Farbton"}
CONTACT_LABELS = {"kunde_name": "Kundenname", "kontakt_telefon": "Telefon", "kunde_email": "Kunden-E-Mail"}
UNKNOWN = frozenset({"unbekannt", "offen", "unknown", "n/a", "none", "null", "nicht bekannt"})


class OrderActionError(ProgressError):
    def __init__(self, code, message, status_code=409, details=None):
        super().__init__(code, message, status_code)
        self.details = details or {}


def _text(value, name, maximum, required=False, multiline=False):
    if not isinstance(value, str):
        raise OrderActionError("invalid_fields", f"{name} als Text angeben.", 400)
    value = value.strip()
    if len(value) > maximum or any(ord(c) < 32 and not (multiline and c in "\r\n") for c in value):
        raise OrderActionError("invalid_fields", f"{name} ist zu lang oder enthält ungültige Zeichen.", 400)
    if value.casefold() in UNKNOWN or (required and not value):
        raise OrderActionError("missing_fields", f"{name} fehlt. Bitte eindeutig ergänzen.", 400)
    return value


def _actor(who):
    if not isinstance(who, dict) or type(who.get("dokumentieren")) not in (bool, int) or who["dokumentieren"] != 1:
        raise OrderActionError("permission_denied", "Die Berechtigung zum Dokumentieren fehlt.", 403)
    return _text(who.get("actor"), "Mitarbeiterkennung", 200, required=True)


def _id(value):
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise OrderActionError("invalid_order", "Eine vorhandene interne Auftragsnummer angeben.", 400)
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _colors(fields):
    if not isinstance(fields, dict) or not fields or not set(fields).issubset(COLOR_FIELDS):
        raise OrderActionError("invalid_fields", "Nur Farbcode, Farbton oder zweiten Farbton ergänzen.", 400)
    return {key: _text(fields[key], COLOR_LABELS[key], maximum, required=True)
            for key, maximum in COLOR_FIELDS.items() if key in fields}


def _create_fields(fields):
    if not isinstance(fields, dict) or not set(fields).issubset(CREATE_FIELDS):
        raise OrderActionError("invalid_fields", "Die Neuanlage enthält nicht erlaubte Felder.", 400)
    result = {key: _text(fields.get(key, ""), key, maximum,
                         required=key in ("kunde_name", "fahrzeug"), multiline=key == "beschreibung")
              for key, maximum in CREATE_FIELDS.items()}
    result["kennzeichen"] = result["kennzeichen"].upper()
    result["fin_nummer"] = result["fin_nummer"].upper()
    result["tsn_nummer"] = result["tsn_nummer"].upper()
    result["kunde_email"] = result["kunde_email"].lower()
    if not result["kennzeichen"] and not result["fin_nummer"]:
        raise OrderActionError("missing_fields", "Kennzeichen oder FIN für die eindeutige Fahrzeugzuordnung ergänzen.", 400)
    if result["kennzeichen"] and not re.fullmatch(r"[A-ZÄÖÜ0-9 -]{2,24}", result["kennzeichen"]):
        raise OrderActionError("invalid_fields", "Kennzeichen prüfen; keine freien Erläuterungen im Kennzeichenfeld.", 400)
    if result["fin_nummer"] and not re.fullmatch(r"[A-HJ-NPR-Z0-9]{17}", result["fin_nummer"]):
        raise OrderActionError("invalid_fields", "Die FIN muss genau 17 gültige Zeichen enthalten; unklare Zeichen prüfen.", 400)
    if result["hsn_nummer"] and not re.fullmatch(r"[0-9]{4}", result["hsn_nummer"]):
        raise OrderActionError("invalid_fields", "Die HSN muss vier Ziffern enthalten.", 400)
    if result["tsn_nummer"] and not re.fullmatch(r"[A-Z0-9]{3,8}", result["tsn_nummer"]):
        raise OrderActionError("invalid_fields", "Die TSN muss drei bis acht Buchstaben/Ziffern enthalten.", 400)
    _validate_contact(result)
    return result


def _validate_contact(result):
    phone, email = result.get("kontakt_telefon", ""), result.get("kunde_email", "")
    if phone and (not re.fullmatch(r"[+0-9 ()/.-]+", phone)
                  or not 6 <= len(re.sub(r"\D", "", phone)) <= 20):
        raise OrderActionError("invalid_fields", "Eine vollständige Telefonnummer angeben oder das Feld leer lassen.", 400)
    if email and not re.fullmatch(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+", email):
        raise OrderActionError("invalid_fields", "Kunden-E-Mail prüfen oder das Feld leer lassen.", 400)


def _contacts(fields):
    if not isinstance(fields, dict) or not fields or not set(fields).issubset(CONTACT_FIELDS):
        raise OrderActionError("invalid_fields", "Nur Kundenname, Kunden-E-Mail oder Telefonnummer ergänzen.", 400)
    result = {key: _text(fields[key], CONTACT_LABELS[key], maximum, required=True)
              for key, maximum in CONTACT_FIELDS.items() if key in fields}
    if "kunde_email" in result:
        result["kunde_email"] = result["kunde_email"].lower()
    _validate_contact(result)
    return result


def _vehicle_key(value):
    return re.sub(r"[^A-ZÄÖÜ0-9]", "", str(value or "").upper())


def _update_view(row, kind):
    fields = COLOR_FIELDS if kind == "farbe" else CONTACT_FIELDS
    return {key: row.get(key) or "" for key in ("fahrzeug", "kennzeichen", *fields)} | {"auftrag_id": row["id"]}


def _active(row):
    if row is None:
        raise OrderActionError("not_found", "Auftrag nicht gefunden. Keine Nummer erfinden.", 404)
    if row.get("archiviert") or row.get("status") not in (1, 2, 3, 4):
        raise OrderActionError("inactive_order", "Nur bestehende aktive Aufträge können hier ergänzt werden.")


class WorkshopOrderActions:
    def __init__(self, portal, initialize=True):
        self.p = portal
        if initialize:
            # Already included in the portal backup/restore contract.
            init_progress_schema(portal)

    def _update_row(self, db, order_id, kind, lock=False):
        sql = f"SELECT {COLOR_SELECT if kind == 'farbe' else CONTACT_SELECT} FROM auftraege WHERE id=?"
        if lock and getattr(self.p, "USE_POSTGRES", False):
            sql += " FOR UPDATE"
        row = db.execute(sql, (order_id,)).fetchone()
        return dict(row) if row is not None else None

    def _duplicates(self, db, fields):
        # Read only vehicle identity; never hydrate via get_auftrag/OCR helpers.
        rows = db.execute("SELECT id,fahrzeug,kennzeichen,fin_nummer FROM auftraege WHERE COALESCE(archiviert,0)=0 AND status IN (1,2,3,4) ORDER BY id").fetchall()
        plate, vin = _vehicle_key(fields["kennzeichen"]), fields["fin_nummer"]
        matches = [dict(row) for row in rows if (plate and plate == _vehicle_key(row["kennzeichen"]))
                   or (vin and vin == _vehicle_key(row["fin_nummer"]))]
        if matches:
            ids = ", ".join(str(row["id"]) for row in matches[:10])
            raise OrderActionError("existing_order", f"Für dieses Fahrzeug gibt es bereits aktive Aufträge: {ids}. Vorhandene Auftragsnummer prüfen und verwenden.",
                                   details={"auftraege": [{key: row[key] for key in ("id", "fahrzeug", "kennzeichen")} for row in matches[:10]]})

    def preview_color(self, order_id, fields, who):
        return self._preview_update(order_id, _colors(fields), who, "farbe")

    def preview_contact(self, order_id, fields, who):
        return self._preview_update(order_id, _contacts(fields), who, "kontakt")

    def _preview_update(self, order_id, fields, who, kind):
        actor, order_id = _actor(who), _id(order_id)
        labels = COLOR_LABELS if kind == "farbe" else CONTACT_LABELS
        db = self.p.get_db()
        try:
            row = self._update_row(db, order_id, kind)
        finally:
            db.close()
        _active(row)
        changes = [{"feld": key, "vorher": row.get(key) or "", "nachher": value}
                   for key, value in fields.items() if (row.get(key) or "") != value]
        summary = f"Auftrag {order_id}, {row.get('fahrzeug') or 'Fahrzeug'}, Kennzeichen {row.get('kennzeichen') or 'nicht hinterlegt'}: "
        summary += "; ".join(f"{labels[c['feld']]} von {c['vorher'] or 'nicht hinterlegt'} auf {c['nachher']}" for c in changes) if changes else "Daten bereits so hinterlegt. Keine Änderung."
        summary += " Auftragsstatus unverändert. Keine Kundenbenachrichtigung."
        return {"version": 1, "art": kind, "actor": actor, "auftrag_id": order_id,
                "fields": fields, "expected_snapshot": _digest(row), "auftrag": _update_view(row, kind),
                "aenderungen": changes, "unveraendert": not changes, "zusammenfassung": summary}

    def preview_create(self, fields, who, source_id=None):
        actor, fields = _actor(who), _create_fields(fields)
        if source_id is not None:
            source_id = _text(source_id, "Uploadkennung", 128, required=True)
        db = self.p.get_db()
        try:
            self._duplicates(db, fields)
        finally:
            db.close()
        warnings = ["Halter im Fahrzeugschein kann vom Auftraggeber abweichen; Kundennamen prüfen.",
                    "Büroprüfung und Einplanung bleiben offen. Es entsteht keine Reparaturfreigabe."]
        if not fields["kontakt_telefon"]:
            warnings.append("Telefonnummer fehlt und wird nicht erfunden.")
        if not fields["kunde_email"]:
            warnings.append("Kunden-E-Mail fehlt; vor einem späteren E-Mail-Versand ergänzen.")
        if not fields["beschreibung"]:
            warnings.append("Arbeitsumfang noch offen; keine Arbeiten aus dem Fahrzeugschein ableiten.")
        summary = (f"Neuen Auftrag für {fields['kunde_name']} anlegen: {fields['fahrzeug']}, "
                   f"Kennzeichen {fields['kennzeichen'] or 'nicht hinterlegt'}, FIN {fields['fin_nummer'] or 'nicht hinterlegt'}. "
                   "Die eindeutige interne Auftragsnummer wird erst beim Speichern vergeben. Status Angelegt; keine Termine, keine Kundenportal-Freigabe und kein Versand. ")
        labels = {"kontakt_telefon": "Telefon", "kunde_email": "Kunden-E-Mail", "hsn_nummer": "HSN",
                  "tsn_nummer": "TSN", "beschreibung": "Arbeitsumfang", **COLOR_LABELS}
        summary += " ".join(f"{label}: {fields[key]}." for key, label in labels.items() if fields[key]) + " "
        return {"version": 1, "art": "auftrag_neu", "actor": actor, "auftrag_id": None,
                "fields": fields, "source_id": source_id, "hinweise": warnings,
                "zusammenfassung": summary + " ".join(warnings)}

    def _validate_preview(self, preview, actor):
        if not isinstance(preview, dict) or type(preview.get("version")) is not int or preview["version"] != 1:
            raise OrderActionError("invalid_preview", "Gespeicherte Vorschau fehlt oder ist ungültig.", 400)
        if preview.get("actor") != actor:
            raise OrderActionError("permission_denied", "Diese Vorschau gehört zu einem anderen Zugang.", 403)
        kind = preview.get("art")
        if kind in ("farbe", "kontakt"):
            order_id = _id(preview.get("auftrag_id"))
            fields = (_colors if kind == "farbe" else _contacts)(preview.get("fields"))
            snapshot = preview.get("expected_snapshot")
            if not isinstance(snapshot, str) or not re.fullmatch(r"[0-9a-f]{64}", snapshot):
                raise OrderActionError("invalid_preview", "Der geprüfte Datenstand fehlt.", 400)
            return kind, order_id, fields, snapshot
        if kind == "auftrag_neu":
            fields = _create_fields(preview.get("fields"))
            if preview.get("auftrag_id") is not None:
                raise OrderActionError("invalid_preview", "Die interne Nummer wird ausschließlich beim Speichern vergeben.", 400)
            if preview.get("source_id") is not None:
                _text(preview["source_id"], "Uploadkennung", 128, required=True)
            return kind, 0, fields, "neu"
        raise OrderActionError("invalid_preview", "Diese Auftragsaktion ist nicht erlaubt.", 400)

    def confirm(self, preview, who, request_id, attach_source=None):
        actor = _actor(who)
        request_id = _text(request_id, "Anforderungskennung", 128, required=True)
        kind, order_id, fields, snapshot = self._validate_preview(preview, actor)
        fingerprint = _digest(["order-actions-v1", actor, request_id, kind, order_id, fields,
                               snapshot, preview.get("source_id")])
        db = self.p.get_db()
        try:
            postgres = bool(getattr(self.p, "USE_POSTGRES", False))
            if not postgres:
                db.execute("BEGIN IMMEDIATE")
            now = datetime.now(timezone.utc)
            timestamp = now.isoformat(timespec="microseconds")
            inserted = db.execute("""INSERT INTO assistent_fortschritt_audit
                (actor,request_id,payload_fingerprint,order_id,action,expected_status,expected_changed_at,created_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(actor,request_id) DO NOTHING""",
                (actor, request_id, fingerprint, order_id, kind, 0, snapshot, timestamp))
            if inserted.rowcount == 0:
                row = db.execute("SELECT payload_fingerprint,result_json FROM assistent_fortschritt_audit WHERE actor=? AND request_id=?", (actor, request_id)).fetchone()
                if row is None or row["payload_fingerprint"] != fingerprint or not row["result_json"]:
                    raise OrderActionError("request_conflict", "Die Anforderungskennung gehört zu anderen Angaben oder muss geprüft werden.")
                result = json.loads(row["result_json"])
                result["wiederholt"] = True
                db.commit()
                return result
            if kind in ("farbe", "kontakt"):
                row = self._update_row(db, order_id, kind, lock=True)
                _active(row)
                if _digest(row) != snapshot:
                    raise OrderActionError("stale_state", "Auftrag oder Daten wurden inzwischen geändert. Neue Vorschau prüfen.")
                changes = {key: value for key, value in fields.items() if (row.get(key) or "") != value}
                unchanged = not changes
                if changes:
                    changes["geaendert_am"] = timestamp
                    assignments = ",".join(key + "=?" for key in changes)
                    db.execute(f"UPDATE auftraege SET {assignments} WHERE id=?", (*changes.values(), order_id))
                    row.update(changes)
                result = {"auftrag_id": order_id, "art": kind, "auftrag": _update_view(row, kind),
                          "unveraendert": unchanged, "wiederholt": False,
                          "hinweis": f"{'Farbdaten' if kind == 'farbe' else 'Kontaktdaten'} zu Auftrag {order_id} {'bereits vorhanden' if unchanged else 'gespeichert'}. Status unverändert; keine Kundenbenachrichtigung."}
            else:
                if preview.get("source_id") is not None and not callable(attach_source):
                    raise OrderActionError("source_attachment_missing", "Die geprüfte Unterlage kann noch nicht sicher zugeordnet werden.")
                if postgres:
                    # Serialize this service's intake of an equal plate/FIN. The
                    # existing portal allows later jobs for the same vehicle, so
                    # no global unique constraint on vehicle identity is added.
                    keys = {_vehicle_key(fields[key]) for key in ("kennzeichen", "fin_nummer") if fields[key]}
                    for key in sorted(keys):
                        lock = int.from_bytes(hashlib.sha256(("assistant-intake:" + key).encode()).digest()[:8], "big", signed=True)
                        db.execute("SELECT pg_advisory_xact_lock(?)", (lock,)).fetchall()
                self._duplicates(db, fields)
                event_time = now.astimezone(BERLIN).strftime("%d.%m.%Y %H:%M")
                values = dict(fields, status=1, quelle="werkstatt", werkstatt_neu=1,
                              token="", kunden_status_token="", kunden_status_aktiv=0,
                              transport_art="", angebotsphase=0, angebot_abgesendet=0, angebot_status="",
                              analyse_pruefen=1, analyse_hinweis="Avatar-Vorerfassung: Kunden- und Fahrzeugdaten sowie Arbeitsumfang im Büro prüfen.",
                              notiz_intern="Nach ausdrücklicher Bestätigung durch den Werkstattassistenten vorerfasst. Büroprüfung und Einplanung offen.",
                              erstellt_am=event_time, geaendert_am=timestamp)
                columns = ",".join(values)
                params = ",".join("?" for _ in values)
                rows = db.execute(f"INSERT INTO auftraege({columns}) VALUES({params}) RETURNING id", tuple(values.values())).fetchall()
                if len(rows) != 1:
                    raise OrderActionError("create_failed", "Die Auftragsnummer konnte nicht sicher ermittelt werden.")
                order_id = _id(rows[0]["id"])
                db.execute("INSERT INTO status_log(auftrag_id,status,zeitstempel) VALUES(?,1,?)", (order_id, event_time))
                if preview.get("source_id") is not None:
                    attach_source(db, order_id)
                result = {"auftrag_id": order_id, "art": kind, "status": 1, "wiederholt": False,
                          "hinweis": f"Auftrag {order_id} angelegt. Büroprüfung offen; keine Termine oder Reparaturfreigabe, kein Kundenportal und keine Nachricht versendet."}
            db.execute("UPDATE assistent_fortschritt_audit SET order_id=?,result_json=? WHERE actor=? AND request_id=?",
                       (order_id, json.dumps(result, ensure_ascii=False, sort_keys=True), actor, request_id))
            db.commit()
            return result
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
