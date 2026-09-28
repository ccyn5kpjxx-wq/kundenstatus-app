"""Explicit internal workshop progress updates with CAS and durable replay.

No status notification, message delivery, AI or network access is performed.
The caller must authenticate the actor, check workshop write permissions and
obtain the explicit requested action. Actor/request_id must be server-controlled
identity and a stable retry key, not model-generated authorization evidence.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


ACTIONS = frozenset({"lackierbereit", "lackierung_starten", "finish_starten"})
NATIVE_ACTION_LABELS = {
    "in_arbeit_starten": "In Arbeit",
    "lackierbereit": "Lackierbereit",
    "lackierung_starten": "Lackierung",
    "finish_starten": "Finish",
    "fertig_melden": "Fahrzeug fertig",
}
STATUS_LABELS = {1: "Angelegt", 2: "Eingeplant", 3: "In Arbeit", 4: "Fertig", 5: "Zurückgegeben"}
STAGE_LABELS = {"": "Nicht hinterlegt", "vorarbeit": "Vorarbeit", "karosserie": "Karosserie",
                "lackierung": "Lackierung", "finish": "Finish"}
SELECT_FIELDS = "id,fahrzeug,kennzeichen,status,archiviert,produktion_schritt,lackierbereit,lackierbereit_am,geaendert_am"
NATIVE_FIELDS = SELECT_FIELDS + ",start_datum,fertig_datum,fahrzeug_abholbereit,fahrzeug_abholbereit_am"
BERLIN = ZoneInfo("Europe/Berlin")


class ProgressError(ValueError):
    def __init__(self, code, message, status_code=409):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def init_schema(portal):
    """Idempotently add readiness fields and the separate internal audit table."""
    db = portal.get_db()
    try:
        portal.ensure_column(db, "auftraege", "lackierbereit", "INTEGER DEFAULT 0")
        portal.ensure_column(db, "auftraege", "lackierbereit_am", "TEXT DEFAULT ''")
        db.execute("""CREATE TABLE IF NOT EXISTS assistent_fortschritt_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT NOT NULL, request_id TEXT NOT NULL,
            payload_fingerprint TEXT NOT NULL, order_id INTEGER NOT NULL,
            action TEXT NOT NULL, expected_status INTEGER NOT NULL,
            expected_changed_at TEXT NOT NULL, created_at TEXT NOT NULL,
            result_json TEXT NOT NULL DEFAULT '', UNIQUE(actor, request_id)
        )""")
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _identifier(value, field):
    if type(value) is not int or value <= 0:
        raise ProgressError("invalid_request", f"{field} muss eine positive Ganzzahl sein.", 400)
    return value


def _required_text(value, field, maximum=200):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ProgressError("invalid_request", f"{field} fehlt oder ist ungültig.", 400)
    return value


def _view(row):
    row = dict(row)
    status = int(row["status"])
    stage = row.get("produktion_schritt") or ""
    ready = row.get("lackierbereit") == 1
    return {
        "auftrag_id": row["id"], "fahrzeug": row.get("fahrzeug") or "",
        "kennzeichen": row.get("kennzeichen") or "", "status": status,
        "status_label": STATUS_LABELS.get(status, "Unbekannt"),
        "archiviert": bool(row.get("archiviert")),
        "produktion_schritt": stage, "produktion_schritt_label": STAGE_LABELS.get(stage, "Unbekannt"),
        "lackierbereit": ready,
        "lackierbereit_label": "Lackierbereit" if ready else "Nicht als lackierbereit markiert",
        "lackierbereit_am": row.get("lackierbereit_am") or "",
        "fahrzeug_fertig": status in (4, 5), "geaendert_am": row["geaendert_am"],
        "quelle": f"/admin/auftrag/{row['id']}",
        "hinweis": "Interner Bearbeitungsstand. Lackierbereit ist keine Fertigmeldung des Fahrzeugs.",
    }


def _native_actor(who):
    # The caller resolves this identity freshly from its authenticated session;
    # model tool arguments and saved previews are never authorization evidence.
    if not isinstance(who, dict) or who.get("dokumentieren") not in (1, True):
        raise ProgressError("permission_denied", "Die Berechtigung zum Dokumentieren fehlt.", 403)
    return _required_text(who.get("actor"), "Ausführender Mitarbeiter")


def _snapshot(row):
    values = [row.get(key) for key in NATIVE_FIELDS.split(",")]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def _native_plan(row, action, day):
    """Pure field-level plan; repeated commands never toggle a production step."""
    if row.get("archiviert"):
        raise ProgressError("archived", "Archivierte Aufträge können hier nicht geändert werden.")
    status = row["status"]
    if status == 4 and action == "fertig_melden":
        return {}
    if status not in (2, 3):
        raise ProgressError("status_not_allowed", "Nur eingeplante oder laufende Aufträge dürfen geändert werden. Fertige Fahrzeuge werden hier nicht reaktiviert.")
    if action == "in_arbeit_starten":
        if status == 3:
            return {}
        return {"status": 3, "start_datum": row.get("start_datum") or day,
                "fahrzeug_abholbereit": 0, "fahrzeug_abholbereit_am": ""}
    if action == "lackierbereit":
        return {} if row.get("lackierbereit") == 1 else {"lackierbereit": 1}
    if status != 3:
        raise ProgressError("status_not_allowed", "Lackierung, Finish und Fertigmeldung setzen einen Auftrag in Arbeit voraus.")
    if action == "fertig_melden":
        changes = {"status": 4, "start_datum": row.get("start_datum") or day,
                   "fertig_datum": row.get("fertig_datum") or day,
                   "fahrzeug_abholbereit": 0, "fahrzeug_abholbereit_am": "",
                   "lackierbereit": 0, "lackierbereit_am": ""}
    else:
        changes = {"produktion_schritt": "lackierung" if action == "lackierung_starten" else "finish",
                   "lackierbereit": 0, "lackierbereit_am": ""}
    return {key: value for key, value in changes.items() if row.get(key) != value}


class WorkshopProgress:
    def __init__(self, portal, initialize=True):
        self.p = portal
        if initialize:
            init_schema(portal)

    def read(self, order_id):
        order_id = _identifier(order_id, "Auftrags-ID")
        db = self.p.get_db()
        try:
            row = db.execute(f"SELECT {SELECT_FIELDS} FROM auftraege WHERE id=?", (order_id,)).fetchone()
            if row is None:
                raise ProgressError("not_found", "Auftrag nicht gefunden.", 404)
            return _view(row)
        finally:
            db.close()

    def preview(self, order_id, action, who):
        """Return a JSON-safe native preview without writing anything.

        The caller must persist this exact result server-side, show/read its
        zusammenfassung and obtain deterministic user confirmation before calling
        confirm(). It must not accept a model/client-supplied preview as proof of
        confirmation. No new bearer/API scope is enabled by these native methods.
        """
        actor = _native_actor(who)
        order_id = _identifier(order_id, "Auftrags-ID")
        if not isinstance(action, str) or action not in NATIVE_ACTION_LABELS:
            raise ProgressError("invalid_action", "Diese Statusaktion ist nicht erlaubt.", 400)
        db = self.p.get_db()
        try:
            row = db.execute(f"SELECT {NATIVE_FIELDS} FROM auftraege WHERE id=?", (order_id,)).fetchone()
        finally:
            db.close()
        if row is None:
            raise ProgressError("not_found", "Auftrag nicht gefunden.", 404)
        row = dict(row)
        expected_changed_at = _required_text(row["geaendert_am"], "Erwarteter Datenstand", 100)
        day = datetime.now(BERLIN).strftime("%d.%m.%Y")
        changes = _native_plan(row, action, day)
        label = NATIVE_ACTION_LABELS[action]
        text = f"Auftrag {order_id}: {label} speichern."
        if not changes:
            text = f"Auftrag {order_id} ist bereits als {label} gespeichert. Keine Änderung."
        elif action in ("lackierbereit", "lackierung_starten", "finish_starten"):
            text += " Der Fahrzeugstatus bleibt unverändert; das Fahrzeug wird nicht fertiggemeldet."
        elif action == "fertig_melden":
            text += " Das gesamte Fahrzeug wird als fertig gemeldet, nicht als zurückgegeben."
        if "start_datum" in changes and not row.get("start_datum"):
            text += f" Fehlenden Arbeitsbeginn auf {day} setzen."
        if "fertig_datum" in changes and not row.get("fertig_datum"):
            text += f" Fehlendes Fertigdatum auf {day} setzen."
        text += " Kein E-Mail- oder WhatsApp-Versand."
        return {"version": 1, "actor": actor, "auftrag_id": order_id, "aktion": action,
                "aktion_label": label, "expected_status": row["status"],
                "expected_changed_at": expected_changed_at, "expected_snapshot": _snapshot(row),
                "werkstatttag": day, "fortschritt": _view(row), "unveraendert": not bool(changes),
                "zusammenfassung": text}

    def confirm(self, preview, who, request_id):
        """Apply a previously saved, explicitly confirmed native preview.

        Resolve who afresh even on retries. request_id must be the persisted
        assistant action ID, namespaced by the caller; it is a durable replay key.
        Snapshot comparison also detects production/date changes made within the
        same minute by legacy writers. No external notification helpers run.
        """
        actor = _native_actor(who)
        if not isinstance(preview, dict) or type(preview.get("version")) is not int or preview["version"] != 1:
            raise ProgressError("invalid_preview", "Die Statusvorschau fehlt oder ist ungültig.", 400)
        if preview.get("actor") != actor:
            raise ProgressError("permission_denied", "Diese Vorschau gehört zu einem anderen Zugang.", 403)
        snapshot = preview.get("expected_snapshot")
        if not isinstance(snapshot, str) or len(snapshot) != 64 or any(c not in "0123456789abcdef" for c in snapshot):
            raise ProgressError("invalid_preview", "Die Statusvorschau enthält keinen gültigen Datenstand.", 400)
        day = _required_text(preview.get("werkstatttag"), "Werkstatttag", 10)
        return self._update(preview.get("auftrag_id"), preview.get("aktion"), preview.get("expected_status"),
                            preview.get("expected_changed_at"), actor, request_id,
                            native={"snapshot": snapshot, "day": day})

    def update(self, order_id, action, expected_status, expected_changed_at, actor, request_id):
        """Apply one authorized action atomically, or replay its saved result.

        Returns {aktion, fortschritt, wiederholt}; replay returns the historical
        action result, NOT a claim about newer current state. Call read() for the
        current state. Reusing a key with any changed payload is a 409 conflict.
        Readiness is available at overall status 2 or 3; starting painting/finish
        requires overall status 3. Overall status and status_log never change.
        geaendert_am uses a precise UTC timestamp instead of minute-resolution
        portal.now_str(), so this service's consecutive writes have distinct CAS
        tokens. Other portal writers retain their existing concurrency behavior.
        """
        return self._update(order_id, action, expected_status, expected_changed_at, actor, request_id)

    def _update(self, order_id, action, expected_status, expected_changed_at, actor, request_id, native=None):
        order_id = _identifier(order_id, "Auftrags-ID")
        allowed = NATIVE_ACTION_LABELS if native is not None else ACTIONS
        if not isinstance(action, str) or action not in allowed:
            raise ProgressError("invalid_action", "Diese Fortschrittsaktion ist nicht erlaubt.", 400)
        if type(expected_status) is not int or expected_status not in STATUS_LABELS:
            raise ProgressError("invalid_request", "Erwarteter Auftragsstatus fehlt oder ist ungültig.", 400)
        expected_changed_at = _required_text(expected_changed_at, "Erwarteter Datenstand", 100)
        actor = _required_text(actor, "Ausführender Mitarbeiter")
        request_id = _required_text(request_id, "Anforderungs-ID", 128)
        payload = [order_id, action, expected_status, expected_changed_at, actor, request_id]
        if native is not None:
            payload += ["native-v1", native["snapshot"], native["day"]]
        fingerprint = hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        db = self.p.get_db()
        try:
            is_postgres = bool(getattr(self.p, "USE_POSTGRES", False))
            if not is_postgres:
                db.execute("BEGIN IMMEDIATE")
            now = datetime.now(timezone.utc)
            timestamp = now.isoformat(timespec="microseconds")
            if timestamp == expected_changed_at:
                timestamp = (now + timedelta(microseconds=1)).isoformat(timespec="microseconds")
            # A concurrent equal key waits on the database unique constraint.
            # Reservations and mutations commit together; failures leave neither.
            inserted = db.execute("""INSERT INTO assistent_fortschritt_audit
                (actor,request_id,payload_fingerprint,order_id,action,expected_status,expected_changed_at,created_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(actor,request_id) DO NOTHING""",
                (actor, request_id, fingerprint, order_id, action, expected_status, expected_changed_at, timestamp))
            if inserted.rowcount == 0:
                previous = db.execute("SELECT payload_fingerprint,result_json FROM assistent_fortschritt_audit WHERE actor=? AND request_id=?", (actor, request_id)).fetchone()
                if previous is None or previous["payload_fingerprint"] != fingerprint:
                    raise ProgressError("request_conflict", "Die Anforderungs-ID wurde bereits für andere Angaben verwendet.")
                if not previous["result_json"]:
                    raise ProgressError("request_incomplete", "Die vorherige Anforderung muss geprüft werden.")
                result = json.loads(previous["result_json"])
                result["wiederholt"] = True
                db.commit()
                return result
            sql = f"SELECT {NATIVE_FIELDS if native is not None else SELECT_FIELDS} FROM auftraege WHERE id=?"
            if is_postgres:
                sql += " FOR UPDATE"
            row = db.execute(sql, (order_id,)).fetchone()
            if row is None:
                raise ProgressError("not_found", "Auftrag nicht gefunden.", 404)
            row = dict(row)
            if row.get("archiviert"):
                raise ProgressError("archived", "Archivierte Aufträge können hier nicht geändert werden.")
            if row["status"] != expected_status or row["geaendert_am"] != expected_changed_at:
                raise ProgressError("stale_state", "Der Auftrag wurde inzwischen geändert. Aktuellen Stand neu laden.")
            if native is not None:
                if _snapshot(row) != native["snapshot"] or datetime.now(BERLIN).strftime("%d.%m.%Y") != native["day"]:
                    raise ProgressError("stale_state", "Auftragsstand oder Werkstatttag haben sich geändert. Neue Vorschau anfordern.")
                result = self._update_native(db, row, action, native["day"], timestamp, now)
            else:
                result = self._update_progress(db, row, action, expected_status, expected_changed_at, timestamp)
            db.execute("UPDATE assistent_fortschritt_audit SET result_json=? WHERE actor=? AND request_id=?",
                       (json.dumps(result, ensure_ascii=False, separators=(",", ":")), actor, request_id))
            db.commit()
            return result
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _update_native(self, db, row, action, day, timestamp, now):
        changes = _native_plan(row, action, day)
        unchanged = not bool(changes)
        order_id = row["id"]
        if changes:
            if changes.get("lackierbereit") == 1:
                changes["lackierbereit_am"] = timestamp
            changes["geaendert_am"] = timestamp
            # Keys are produced only by the internal plan, never by the
            # preview/model/client. Row lock + CAS cover both DB adapters.
            assignments = ",".join(key + "=?" for key in changes)
            updated = db.execute(f"""UPDATE auftraege SET {assignments}
                WHERE id=? AND status=? AND COALESCE(archiviert,0)=0 AND geaendert_am=?""",
                (*changes.values(), order_id, row["status"], row["geaendert_am"]))
            if updated.rowcount != 1:
                raise ProgressError("stale_state", "Der Auftrag wurde inzwischen geändert. Aktuellen Stand neu laden.")
            if changes.get("status") is not None and changes["status"] != row["status"]:
                event_time = now.astimezone(BERLIN).strftime("%d.%m.%Y %H:%M")
                db.execute("INSERT INTO status_log (auftrag_id,status,zeitstempel) VALUES(?,?,?)",
                           (order_id, changes["status"], event_time))
            row.update(changes)
        label = NATIVE_ACTION_LABELS[action]
        return {"aktion": action, "aktion_label": label, "fortschritt": _view(row),
                "wiederholt": False, "unveraendert": unchanged,
                "hinweis": f"Auftrag {order_id}: {label} {'bereits gespeichert' if unchanged else 'gespeichert'}. Kein E-Mail- oder WhatsApp-Versand."}

    def _update_progress(self, db, row, action, expected_status, expected_changed_at, timestamp):
        """Existing three-action bearer behavior, intentionally not expanded."""
        order_id = row["id"]
        if row["status"] not in (2, 3):
            raise ProgressError("status_not_allowed", "Fortschritt ist nur bei eingeplanten oder laufenden Aufträgen erlaubt.")
        if action != "lackierbereit" and row["status"] != 3:
            raise ProgressError("status_not_allowed", "Lackierung und Finish können erst bei einem Auftrag in Arbeit gestartet werden.")
        ready = 1 if action == "lackierbereit" else 0
        ready_at = timestamp if ready else ""
        stage = row.get("produktion_schritt")
        if action == "lackierung_starten":
            stage = "lackierung"
        elif action == "finish_starten":
            stage = "finish"
        updated = db.execute("""UPDATE auftraege
            SET lackierbereit=?,lackierbereit_am=?,produktion_schritt=?,geaendert_am=?
            WHERE id=? AND status=? AND COALESCE(archiviert,0)=0 AND geaendert_am=?""",
            (ready, ready_at, stage, timestamp, order_id, expected_status, expected_changed_at))
        if updated.rowcount != 1:
            raise ProgressError("stale_state", "Der Auftrag wurde inzwischen geändert. Aktuellen Stand neu laden.")
        row.update(lackierbereit=ready, lackierbereit_am=ready_at,
                   produktion_schritt=stage, geaendert_am=timestamp)
        return {"aktion": action, "fortschritt": _view(row), "wiederholt": False}
