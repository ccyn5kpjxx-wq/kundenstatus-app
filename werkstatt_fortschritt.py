"""Explicit internal workshop progress updates with CAS and durable replay.

No status notification, message delivery, AI or network access is performed.
The caller must authenticate the actor, check workshop write permissions and
obtain the explicit requested action. Actor/request_id must be server-controlled
identity and a stable retry key, not model-generated authorization evidence.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone


ACTIONS = frozenset({"lackierbereit", "lackierung_starten", "finish_starten"})
STATUS_LABELS = {1: "Angelegt", 2: "Eingeplant", 3: "In Arbeit", 4: "Fertig", 5: "Zurückgegeben"}
STAGE_LABELS = {"": "Nicht hinterlegt", "vorarbeit": "Vorarbeit", "karosserie": "Karosserie",
                "lackierung": "Lackierung", "finish": "Finish"}
SELECT_FIELDS = "id,fahrzeug,kennzeichen,status,archiviert,produktion_schritt,lackierbereit,lackierbereit_am,geaendert_am"


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
        order_id = _identifier(order_id, "Auftrags-ID")
        if not isinstance(action, str) or action not in ACTIONS:
            raise ProgressError("invalid_action", "Diese Fortschrittsaktion ist nicht erlaubt.", 400)
        if type(expected_status) is not int or expected_status not in STATUS_LABELS:
            raise ProgressError("invalid_request", "Erwarteter Auftragsstatus fehlt oder ist ungültig.", 400)
        expected_changed_at = _required_text(expected_changed_at, "Erwarteter Datenstand", 100)
        actor = _required_text(actor, "Ausführender Mitarbeiter")
        request_id = _required_text(request_id, "Anforderungs-ID", 128)
        payload = [order_id, action, expected_status, expected_changed_at, actor, request_id]
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
            sql = f"SELECT {SELECT_FIELDS} FROM auftraege WHERE id=?"
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
            result = {"aktion": action, "fortschritt": _view(row), "wiederholt": False}
            db.execute("UPDATE assistent_fortschritt_audit SET result_json=? WHERE actor=? AND request_id=?",
                       (json.dumps(result, ensure_ascii=False, separators=(",", ":")), actor, request_id))
            db.commit()
            return result
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
