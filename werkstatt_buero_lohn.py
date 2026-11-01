"""Monthly payroll review, explicit private payslip publication, no banking."""
import base64
from contextlib import contextmanager
from datetime import datetime
import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import secrets
import sqlite3
from uuid import uuid4
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request, send_file, session, url_for
from werkzeug.datastructures import FileStorage
from werkstatt_mitarbeiter_portal import _document, MAX_DOCUMENT_BYTES

BACKUP_TABLES = ("buero_lohnlaeufe", "buero_lohnzettel", "buero_lohn_audit")
SCHEMA_FEATURE = "office_payroll_v1"
STATES = {"zur_pruefung": "Zur Chefprüfung", "freigegeben": "Freigegeben – Überweisung vorbereiten",
          "zahlung_in_bearbeitung": "Externe Überweisung in Bearbeitung", "dokumentiert": "Zahlung dokumentiert"}
_PERIOD = re.compile(r"20[0-9]{2}-(?:0[1-9]|1[0-2])")
_KEY = re.compile(r"[a-f0-9]{32}")
_RUN_META = "id,periode,quellenlabel,filename,mime,size_bytes,sha256,notiz,status,version,created_at,updated_at,approved_at,approved_by,paid_date,payment_reference"
_ITEM_META = "id,lauf_id,filename,mime,size_bytes,sha256,mitarbeiter_id,zuordnung_bestaetigt,version,publikation,portal_lohnzettel_id,created_at,updated_at"


class PayrollConflict(ValueError):
    pass


def _now():
    return datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="seconds")


def _key(value):
    if not isinstance(value, str) or not _KEY.fullmatch(value):
        raise ValueError("Ungültiger Lohnlauf oder Lohnzettel.")
    return value


def _version(value):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,9}", str(value)):
        raise ValueError("Prüfstand ungültig. Seite neu laden.")
    return int(value)


def _pdf(upload):
    if upload is None or not hasattr(upload, "stream"):
        raise ValueError("Eine PDF-Datei auswählen.")
    raw, filename, mime = _document(upload)
    if mime != "application/pdf":
        raise ValueError("Eine PDF-Datei bereitstellen.")
    return raw, filename, mime, hashlib.sha256(raw).hexdigest()


class OfficePayroll:
    def __init__(self, portal):
        self.p = portal

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def identity(self, *, admin=False, db=None):
        if admin:
            if not session.get("admin"):
                raise PermissionError("Nur die Werkstattleitung darf Lohnauswertungen prüfen und Lohnzettel bereitstellen.")
            return {"actor": "admin"}
        who = self.p.office_access.identity(db)
        if not who or "loehne" not in who.get("office_categories", ()):
            raise PermissionError("Persönlicher Bürozugang mit ausdrücklicher Lohnberechtigung erforderlich.")
        return who

    def init_schema(self):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS buero_lohnlaeufe (
                id TEXT PRIMARY KEY,periode TEXT NOT NULL UNIQUE,quellenlabel TEXT NOT NULL,
                filename TEXT NOT NULL,mime TEXT NOT NULL,size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL UNIQUE,
                original_base64 TEXT NOT NULL,notiz TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'zur_pruefung',
                version INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                approved_at TEXT NOT NULL DEFAULT '',approved_by TEXT NOT NULL DEFAULT '',
                paid_date TEXT NOT NULL DEFAULT '',payment_reference TEXT NOT NULL DEFAULT '')""")
            db.execute("""CREATE TABLE IF NOT EXISTS buero_lohnzettel (
                id TEXT PRIMARY KEY,lauf_id TEXT NOT NULL,filename TEXT NOT NULL,mime TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,original_base64 TEXT NOT NULL,
                mitarbeiter_id INTEGER,zuordnung_bestaetigt INTEGER NOT NULL DEFAULT 0,
                version INTEGER NOT NULL DEFAULT 1,publikation TEXT NOT NULL DEFAULT 'vorbereitet',
                portal_lohnzettel_id INTEGER,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(lauf_id,sha256))""")
            db.execute("""CREATE TABLE IF NOT EXISTS buero_lohn_audit (
                id TEXT PRIMARY KEY,lauf_id TEXT NOT NULL,actor TEXT NOT NULL,aktion TEXT NOT NULL,
                details_json TEXT NOT NULL,created_at TEXT NOT NULL)""")

    @staticmethod
    def _run(db, key):
        row = db.execute("SELECT * FROM buero_lohnlaeufe WHERE id=?", (_key(key),)).fetchone()
        if not row:
            raise LookupError("Lohnlauf nicht gefunden.")
        return dict(row)

    @staticmethod
    def _item(db, key, item):
        row = db.execute("SELECT * FROM buero_lohnzettel WHERE id=? AND lauf_id=?", (_key(item), _key(key))).fetchone()
        if not row:
            raise LookupError("Einzellohnzettel nicht gefunden.")
        return dict(row)

    @staticmethod
    def _current(row, version):
        if row["version"] != _version(version):
            raise PayrollConflict("Dieser Prüfstand wurde bereits geändert. Neu laden und erneut prüfen.")

    def _audit(self, db, key, who, action, details):
        db.execute("INSERT INTO buero_lohn_audit(id,lauf_id,actor,aktion,details_json,created_at) VALUES(?,?,?,?,?,?)",
                   (uuid4().hex, key, who["actor"], action, json.dumps(details, ensure_ascii=False), _now()))

    def _backup(self):
        hook = getattr(self.p, "schedule_change_backup", None)
        if callable(hook):
            hook("buero-lohn")

    def create(self, period, upload, *, source_label="Katharina Seber", note=""):
        self.identity(admin=True)
        if not isinstance(period, str) or not _PERIOD.fullmatch(period):
            raise ValueError("Abrechnungsmonat gültig auswählen.")
        if not isinstance(source_label, str) or not isinstance(note, str) or len(source_label) > 160 or len(note) > 2000:
            raise ValueError("Quellenbezeichnung oder Notiz zu lang.")
        raw, name, mime, digest = _pdf(upload)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db)
            existing = db.execute("SELECT id,periode,sha256 FROM buero_lohnlaeufe WHERE periode=? OR sha256=?", (period, digest)).fetchall()
            if existing:
                if len(existing) == 1 and existing[0]["periode"] == period and existing[0]["sha256"] == digest:
                    return existing[0]["id"], True
                raise ValueError("Für diesen Monat oder dieses Original besteht bereits ein Lohnlauf. Vorhandenen Lauf prüfen.")
            key, now = uuid4().hex, _now()
            db.execute("""INSERT INTO buero_lohnlaeufe
                (id,periode,quellenlabel,filename,mime,size_bytes,sha256,original_base64,notiz,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (key, period, source_label, name, mime, len(raw), digest,
                base64.b64encode(raw).decode("ascii"), note, now, now))
            self._audit(db, key, who, "auswertung_erfasst", {"periode": period, "sha256": digest})
        self._backup()
        return key, False

    def update_note(self, key, version, note):
        if not isinstance(note, str) or len(note) > 2000:
            raise ValueError("Notiz zu lang.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db); row = self._run(db, key); self._current(row, version)
            if row["status"] != "zur_pruefung":
                raise ValueError("Freigegebene Auswertung bleibt unverändert.")
            db.execute("UPDATE buero_lohnlaeufe SET notiz=?,version=version+1,updated_at=? WHERE id=? AND version=?", (note, _now(), key, row["version"]))
            self._audit(db, key, who, "notiz_bearbeitet", {"vorher": row["notiz"], "nachher": note})
        self._backup()

    def add_payslip(self, key, version, upload, *, confirmed_individual=False):
        self.identity(admin=True)
        if confirmed_individual is not True:
            raise ValueError("Ausdrücklich bestätigen: nur der Lohnzettel eines einzelnen Mitarbeiters, keine Gesamtauswertung.")
        raw, name, mime, digest = _pdf(upload)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db); row = self._run(db, key); self._current(row, version)
            if digest == row["sha256"]:
                raise ValueError("Die Gesamtauswertung darf nicht als Einzellohnzettel verteilt werden.")
            prior = db.execute("SELECT id FROM buero_lohnzettel WHERE lauf_id=? AND sha256=?", (key, digest)).fetchone()
            if prior:
                return prior["id"], True
            item, now = uuid4().hex, _now()
            db.execute("""INSERT INTO buero_lohnzettel(id,lauf_id,filename,mime,size_bytes,sha256,original_base64,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)""", (item, key, name, mime, len(raw), digest, base64.b64encode(raw).decode("ascii"), now, now))
            db.execute("UPDATE buero_lohnlaeufe SET version=version+1,updated_at=? WHERE id=?", (now, key))
            self._audit(db, key, who, "einzelpdf_erfasst", {"item": item, "sha256": digest})
        self._backup(); return item, False

    def assign(self, key, item, version, employee_id, *, confirmed=False):
        if type(employee_id) is not int or employee_id < 1 or confirmed is not True:
            raise ValueError("Mitarbeiter-ID auswählen und Zuordnung/Monat am Original ausdrücklich prüfen.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db); run = self._run(db, key); row = self._item(db, key, item); self._current(row, version)
            if row["publikation"] != "vorbereitet":
                raise ValueError("Eine laufende oder abgeschlossene Bereitstellung darf nicht neu zugeordnet werden.")
            employee = db.execute("SELECT id,name,aktiv FROM mitarbeiter WHERE id=?", (employee_id,)).fetchone()
            if not employee or not employee["aktiv"]:
                raise ValueError("Aktiven Mitarbeiter über die stabile ID auswählen.")
            db.execute("UPDATE buero_lohnzettel SET mitarbeiter_id=?,zuordnung_bestaetigt=1,version=version+1,updated_at=? WHERE id=? AND version=?", (employee_id, _now(), item, row["version"]))
            db.execute("UPDATE buero_lohnlaeufe SET version=version+1,updated_at=? WHERE id=?", (_now(), key))
            self._audit(db, key, who, "zuordnung_geprueft", {"item": item, "mitarbeiter_id": employee_id, "periode": run["periode"]})
        self._backup()

    def approve(self, key, version, *, confirmed=False):
        if confirmed is not True:
            raise ValueError("Die geprüfte Lohnauswertung und Zahlungsaufgabe ausdrücklich freigeben.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db); row = self._run(db, key); self._current(row, version)
            if row["status"] != "zur_pruefung":
                raise ValueError("Dieser Lauf wurde bereits freigegeben.")
            self._raw(row)
            db.execute("UPDATE buero_lohnlaeufe SET status='freigegeben',version=version+1,approved_at=?,approved_by='admin',updated_at=? WHERE id=? AND version=?", (_now(), _now(), key, row["version"]))
            self._audit(db, key, who, "chef_freigabe", {"sha256": row["sha256"], "periode": row["periode"]})
        self._backup()

    def publish(self, key, item, version, *, confirmed=False):
        if confirmed is not True:
            raise ValueError("Einzelbeleg, Mitarbeiter und Monat vor der Bereitstellung bestätigen.")
        with self.p.portal_originals_operation_lock():
            with self.db() as db:
                who = self.identity(admin=True, db=db); run = self._run(db, key); row = self._item(db, key, item); self._current(row, version)
                if run["status"] == "zur_pruefung" or not run["approved_at"] or not row["zuordnung_bestaetigt"] or not row["mitarbeiter_id"]:
                    raise ValueError("Zuerst den Lauf freigeben und den Einzelbeleg ausdrücklich einem Mitarbeiter zuordnen.")
                if row["portal_lohnzettel_id"]:
                    return row["portal_lohnzettel_id"]
                employee = db.execute("SELECT id,aktiv FROM mitarbeiter WHERE id=?", (row["mitarbeiter_id"],)).fetchone()
                if not employee or not employee["aktiv"]:
                    raise ValueError("Mitarbeiter ist nicht mehr aktiv.")
                raw = self._raw(row)
                db.execute("UPDATE buero_lohnzettel SET publikation='in_bereitstellung',version=version+1,updated_at=? WHERE id=? AND version=?", (_now(), item, row["version"]))
                self._audit(db, key, who, "bereitstellung_reserviert", {"item": item, "mitarbeiter_id": row["mitarbeiter_id"], "sha256": row["sha256"]})
            # Existing owner-scoped API opens its own transaction. Keep the
            # originals lock throughout; retry is safe by owner+period+SHA.
            upload = FileStorage(stream=io.BytesIO(raw), filename=row["filename"], content_type="application/pdf")
            portal_id = self.p.employee_portal.upload_payroll(row["mitarbeiter_id"], run["periode"], upload)
            with self.db() as db:
                self.identity(admin=True, db=db)
                db.execute("UPDATE buero_lohnzettel SET publikation='bereitgestellt',portal_lohnzettel_id=?,version=version+1,updated_at=? WHERE id=?", (portal_id, _now(), item))
                self._audit(db, key, who, "persoenlich_bereitgestellt", {"item": item, "mitarbeiter_id": row["mitarbeiter_id"], "portal_lohnzettel_id": portal_id})
        self._backup(); return portal_id

    def payment(self, key, version, target, *, confirmed=False, paid_date="", reference="", admin=False):
        if confirmed is not True:
            raise ValueError("Den tatsächlich außerhalb des Portals ausgeführten Zahlungsschritt bestätigen.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=admin, db=db); row = self._run(db, key); self._current(row, version)
            if not row["approved_at"] or {"freigegeben": "zahlung_in_bearbeitung", "zahlung_in_bearbeitung": "dokumentiert"}.get(row["status"]) != target:
                raise ValueError("Chef-Freigabe und richtige Reihenfolge der Zahlungsdokumentation erforderlich.")
            if target == "dokumentiert":
                try:
                    actual_date = datetime.strptime(paid_date, "%Y-%m-%d").date()
                    if (actual_date.isoformat() != paid_date or actual_date > datetime.now(ZoneInfo("Europe/Berlin")).date()
                            or not isinstance(reference, str) or not reference.strip() or len(reference) > 200):
                        raise ValueError()
                except (TypeError, ValueError):
                    raise ValueError("Ausführungsdatum (nicht in der Zukunft) und Zahlungsnachweis eintragen.") from None
            else:
                paid_date, reference = "", ""
            db.execute("UPDATE buero_lohnlaeufe SET status=?,paid_date=?,payment_reference=?,version=version+1,updated_at=? WHERE id=? AND version=?", (target, paid_date, reference, _now(), key, row["version"]))
            self._audit(db, key, who, target, {"datum": paid_date, "nachweis": reference})
        self._backup()

    @staticmethod
    def _raw(row):
        try:
            raw = base64.b64decode(row["original_base64"], validate=True)
            if (not 0 < len(raw) <= MAX_DOCUMENT_BYTES or len(raw) != row["size_bytes"]
                    or row["mime"] != "application/pdf" or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), row["sha256"])):
                raise ValueError()
            return raw
        except (ValueError, TypeError):
            raise LookupError("Original fehlt oder stimmt nicht mit dem gespeicherten Prüfstand überein.") from None

    def original(self, key, *, admin=False, item=None):
        with self.db() as db:
            self.identity(admin=admin, db=db); run = self._run(db, key)
            if not admin and (item is not None or run["status"] == "zur_pruefung" or not run["approved_at"]):
                raise PermissionError("Dieses Lohnoriginal ist für den Bürozugang nicht freigegeben.")
            row = self._item(db, key, item) if item is not None else run
            raw = self._raw(row)
            return {"filename": row["filename"], "sha256": row["sha256"], "mime": row["mime"]}, raw

    def list(self, *, admin=False):
        self.identity(admin=admin)
        with self.db() as db:
            query = "SELECT " + _RUN_META + " FROM buero_lohnlaeufe"
            if not admin:
                query += " WHERE status<>'zur_pruefung' AND approved_at<>''"
            runs = [dict(row) for row in db.execute(query + " ORDER BY periode DESC").fetchall()]
            employees = [dict(row) for row in db.execute("SELECT id,name FROM mitarbeiter WHERE aktiv=1 ORDER BY name,id").fetchall()] if admin else []
            for run in runs:
                run["items"] = [dict(row) for row in db.execute("SELECT " + _ITEM_META + " FROM buero_lohnzettel WHERE lauf_id=? ORDER BY created_at,id", (run["id"],)).fetchall()] if admin else []
        return {"runs": runs, "employees": employees}

    def pending_review_count(self):
        self.identity(admin=True)
        with self.db() as db:
            return db.execute("SELECT COUNT(*) AS n FROM buero_lohnlaeufe WHERE status='zur_pruefung'").fetchone()["n"]

    def pending_payment_count(self, *, admin=None):
        self.identity(admin=bool(session.get("admin")) if admin is None else admin)
        with self.db() as db:
            return db.execute("SELECT COUNT(*) AS n FROM buero_lohnlaeufe WHERE status IN ('freigegeben','zahlung_in_bearbeitung') AND approved_at<>''").fetchone()["n"]


def register_office_payroll(p):
    service = OfficePayroll(p); p.office_payroll = service
    bp = Blueprint("office_payroll", __name__)

    def csrf():
        values, expected = request.form.getlist("csrf_token"), session.get("csrf_token")
        if not values or not isinstance(expected, str) or any(not hmac.compare_digest(expected, str(value)) for value in values):
            abort(400)

    def page(admin):
        try:
            service.identity(admin=admin)
        except PermissionError:
            abort(403)
        session.setdefault("csrf_token", secrets.token_urlsafe(32))
        error = ""
        if request.method == "POST":
            csrf()
            try:
                form = request.form
                if any(len(form.getlist(k)) != 1 for k in form if k != "csrf_token"):
                    raise ValueError("Formularfelder eindeutig ausfüllen.")
                if set(request.files) - {"original"} or len(request.files.getlist("original")) > 1:
                    raise ValueError("Genau eine ausdrücklich gewählte PDF-Datei hochladen.")
                action = form.get("action", "")
                confirmed = form.get("confirmed") == "1"
                key, version = form.get("key", ""), form.get("version", "")
                if admin and action == "create":
                    service.create(form.get("period", ""), request.files.get("original"), source_label=form.get("source_label", "Katharina Seber"), note=form.get("note", ""))
                elif admin and action == "note": service.update_note(key, version, form.get("note", ""))
                elif admin and action == "add": service.add_payslip(key, version, request.files.get("original"), confirmed_individual=confirmed)
                elif admin and action == "assign": service.assign(key, form.get("item", ""), version, int(form.get("employee_id", "0")), confirmed=confirmed)
                elif admin and action == "approve": service.approve(key, version, confirmed=confirmed)
                elif admin and action == "publish": service.publish(key, form.get("item", ""), version, confirmed=confirmed)
                elif action == "payment": service.payment(key, version, form.get("target", ""), confirmed=confirmed, paid_date=form.get("paid_date", ""), reference=form.get("reference", ""), admin=admin)
                else: raise ValueError("Diese Aktion ist hier nicht erlaubt.")
                return redirect(url_for("office_payroll.admin" if admin else "office_payroll.office"), code=303)
            except PayrollConflict as exc:
                return render_template("buero_lohn.html", data=service.list(admin=admin), admin_view=admin,
                                       states=STATES, csrf_token=session["csrf_token"], error=str(exc)), 409
            except (ValueError, LookupError, PermissionError) as exc:
                error = str(exc)
        return render_template("buero_lohn.html", data=service.list(admin=admin), admin_view=admin,
                               states=STATES, csrf_token=session["csrf_token"], error=error), 400 if error else 200

    @bp.route("/admin/buero/loehne", methods=["GET", "POST"])
    def admin(): return page(True)

    @bp.route("/werkstatt/buero/loehne", methods=["GET", "POST"])
    def office(): return page(False)

    def download(key, *, admin=False, item=None):
        try: meta, raw = service.original(key, admin=admin, item=item)
        except PermissionError: abort(403)
        except (ValueError, LookupError): abort(404)
        return send_file(io.BytesIO(raw), mimetype="application/pdf", as_attachment=True, download_name=meta["filename"], etag=False, conditional=False)

    @bp.get("/admin/buero/loehne/<key>/auswertung")
    def admin_original(key): return download(key, admin=True)

    @bp.get("/werkstatt/buero/loehne/<key>/auswertung")
    def office_original(key): return download(key)

    @bp.get("/admin/buero/loehne/<key>/lohnzettel/<item>")
    def single_original(key, item): return download(key, admin=True, item=item)

    @bp.before_request
    def limit_upload(): request.max_content_length = MAX_DOCUMENT_BYTES + 128*1024

    @bp.after_request
    def private(response):
        response.headers.update({"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff", "X-Robots-Tag": "noindex, nofollow, noarchive"})
        return response
    p.app.register_blueprint(bp)
    return service


def ensure_office_payroll_for_import(p, *, export=None, imported_db=None, target=None, **_):
    own, source = target is None, None; db = target if target is not None else p.get_db()
    try:
        with p.portal_originals_operation_lock():
            if imported_db is not None:
                source = sqlite3.connect(Path(imported_db).resolve().as_uri()+"?mode=ro", uri=True); source.row_factory = sqlite3.Row
            for table in BACKUP_TABLES:
                current = [dict(row) for row in db.execute("SELECT * FROM "+table).fetchall()]
                if source:
                    exists = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    incoming = [dict(row) for row in source.execute("SELECT * FROM "+table).fetchall()] if exists else []
                else: incoming = (export or {}).get("tables", {}).get(table, [])
                if not isinstance(incoming, list) or any(not isinstance(row, dict) for row in incoming): raise ValueError()
                values = {row.get("id"): row for row in incoming}
                if len(values) != len(incoming) or values != {row["id"]: row for row in current}: raise ValueError()
            # Assignment ownership must survive a restore as well as the PDFs:
            # an old staff table must not turn the same numeric ID into someone else.
            mids = {row["mitarbeiter_id"] for row in db.execute("SELECT mitarbeiter_id FROM buero_lohnzettel WHERE mitarbeiter_id IS NOT NULL").fetchall()}
            for mid in mids:
                for table, key, fields in (("mitarbeiter", "id", ("id", "name", "aktiv")),
                                           ("assistent_rechte", "mitarbeiter_id", None)):
                    expected = db.execute("SELECT * FROM " + table + " WHERE " + key + "=?", (mid,)).fetchone()
                    if source:
                        candidate = source.execute("SELECT * FROM " + table + " WHERE " + key + "=?", (mid,)).fetchone()
                        candidate = dict(candidate) if candidate else None
                    else:
                        matches = [row for row in (export or {}).get("tables", {}).get(table, []) if row.get(key) == mid]
                        if len(matches) > 1: raise ValueError()
                        candidate = matches[0] if matches else None
                    expected = dict(expected) if expected else None
                    if fields and expected is not None:
                        expected = {name: expected[name] for name in fields}
                        candidate = {name: candidate.get(name) for name in fields} if candidate else None
                    if candidate != expected: raise ValueError()
    except (ValueError, TypeError, KeyError, sqlite3.Error):
        raise ValueError("Import gestoppt: Lohnläufe, Einzeloriginale oder deren Freigabe-/Bereitstellungsaudit würden verändert.") from None
    finally:
        if source: source.close()
        if own: db.close()
