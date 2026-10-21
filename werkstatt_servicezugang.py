"""Personal driver access. Explicit assignment, no messaging or automatic company membership."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from uuid import uuid4

from flask import Blueprint, abort, redirect, render_template, request, session
from werkzeug.security import check_password_hash, generate_password_hash

BACKUP_TABLES = ("service_zugaenge", "service_zuordnungen", "service_einladungen")
SCHEMA_FEATURE = "service_personal_access_v1"
SESSION_SECONDS = 8 * 3600
INVITATION_SECONDS = 24 * 3600


def ensure_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS service_zugaenge (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE,
        passwort_hash TEXT NOT NULL DEFAULT '', aktiv INTEGER NOT NULL DEFAULT 0,
        version INTEGER NOT NULL DEFAULT 1, erstellt_am INTEGER NOT NULL, geaendert_am INTEGER NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS service_zuordnungen (
        id TEXT PRIMARY KEY, zugang_id TEXT NOT NULL REFERENCES service_zugaenge(id),
        firma_id TEXT NOT NULL REFERENCES kundenflotten_firmen(id),
        fahrzeug_id TEXT NOT NULL REFERENCES kundenflotten_fahrzeuge(id),
        fahrer_fingerprint TEXT NOT NULL, aktiv INTEGER NOT NULL DEFAULT 1,
        version INTEGER NOT NULL DEFAULT 1, erstellt_am INTEGER NOT NULL, beendet_am INTEGER,
        UNIQUE(zugang_id,fahrzeug_id))""")
    db.execute("""CREATE TABLE IF NOT EXISTS service_einladungen (
        id TEXT PRIMARY KEY, zugang_id TEXT NOT NULL REFERENCES service_zugaenge(id),
        token_hash TEXT NOT NULL UNIQUE, zugang_version INTEGER NOT NULL,
        gueltig_bis INTEGER NOT NULL, benutzt_am INTEGER, widerrufen_am INTEGER,
        request_id TEXT NOT NULL UNIQUE, erstellt_am INTEGER NOT NULL)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_service_assignment_vehicle ON service_zuordnungen(fahrzeug_id,aktiv)")


def driver_fingerprint(row):
    if not row:
        return ""
    fields = ("fahrzeug_id", "name", "email", "telefon", "kanal", "einwilligung", "aktualisiert_am")
    return hashlib.sha256(json.dumps([row[k] for k in fields], ensure_ascii=False).encode()).hexdigest()


def revoke_vehicle(db, vehicle_id):
    """Call inside the same transaction as a driver change, before replacing driver data."""
    now = int(time.time())
    ids = [r[0] for r in db.execute("SELECT DISTINCT zugang_id FROM service_zuordnungen WHERE fahrzeug_id=? AND aktiv=1", (vehicle_id,))]
    db.execute("UPDATE service_zuordnungen SET aktiv=0,version=version+1,beendet_am=? WHERE fahrzeug_id=? AND aktiv=1", (now, vehicle_id))
    for key in ids:
        db.execute("UPDATE service_zugaenge SET version=version+1,geaendert_am=? WHERE id=?", (now, key))
        db.execute("UPDATE service_einladungen SET widerrufen_am=? WHERE zugang_id=? AND benutzt_am IS NULL AND widerrufen_am IS NULL", (now, key))


def ensure_state_for_import(*, get_db, export=None, imported_db=None, target=None):
    """A restore cannot erase accounts, reopen invitations, or undo access revocation."""
    own = target is None
    db = target if target is not None else get_db()
    source = None
    try:
        if imported_db is not None:
            from pathlib import Path
            source = sqlite3.connect(Path(imported_db).resolve().as_uri() + "?mode=ro", uri=True)
            source.row_factory = sqlite3.Row
        for table in BACKUP_TABLES:
            current = [dict(r) for r in db.execute(f"SELECT * FROM {table}")]
            if not current:
                continue
            incoming = [dict(r) for r in source.execute(f"SELECT * FROM {table}")] if source else (export or {}).get("tables", {}).get(table, [])
            if not isinstance(incoming, list) or any(not isinstance(r, dict) for r in incoming):
                raise ValueError()
            indexed = {r.get("id"): r for r in incoming}
            if len(indexed) != len(incoming) or any(indexed.get(row["id"]) != row for row in current):
                raise ValueError()
    except (ValueError, TypeError, KeyError, sqlite3.Error):
        raise ValueError("Import gestoppt: Persönliche Servicezugänge, Einladungen oder Fahrzeugberechtigungen würden zurückgesetzt. Bitte eine aktuelle vollständige Sicherung verwenden.") from None
    finally:
        if source is not None:
            source.close()
        if own:
            db.close()


class ServiceAccess:
    def __init__(self, p, fleet):
        self.p, self.fleet = p, fleet

    @staticmethod
    def safe_user(row):
        return {key: row[key] for key in ("id", "name", "email", "version")} | {"rolle": "fahrer"}

    def identity(self):
        if not self.p.app.config.get("SERVICE_ACCESS_ENABLED"):
            return None
        key, version, started = (session.get(k) for k in ("service_zugang", "service_version", "service_login_am"))
        if not isinstance(key, str) or type(version) is not int or type(started) not in (int, float) or not 0 <= time.time() - started < SESSION_SECONDS:
            return None
        with self.fleet.db() as db:
            row = db.execute("SELECT * FROM service_zugaenge WHERE id=? AND aktiv=1 AND version=?", (key, version)).fetchone()
        return self.safe_user(row) if row else None

    @staticmethod
    def _current(db, who):
        if not isinstance(who, dict) or who.get("rolle") != "fahrer" or not isinstance(who.get("id"), str) or type(who.get("version")) is not int:
            return None
        return db.execute("SELECT id FROM service_zugaenge WHERE id=? AND version=? AND aktiv=1", (who["id"], who["version"])).fetchone()

    def authorize(self, who, company, vehicle):
        if not self.p.app.config.get("SERVICE_ACCESS_ENABLED"):
            return False
        with self.fleet.db() as db:
            if not self._current(db, who):
                return False
            row = db.execute("""SELECT a.fahrer_fingerprint FROM service_zuordnungen a
                JOIN kundenflotten_fahrzeuge v ON v.id=a.fahrzeug_id AND v.firma_id=a.firma_id
                WHERE a.zugang_id=? AND a.firma_id=? AND a.fahrzeug_id=? AND a.aktiv=1""", (who["id"], company, vehicle)).fetchone()
            driver = db.execute("SELECT * FROM kundenflotten_fahrer WHERE fahrzeug_id=?", (vehicle,)).fetchone()
            return bool(row and driver and hmac.compare_digest(row["fahrer_fingerprint"], driver_fingerprint(driver)))

    def vehicles(self, who):
        if not self.p.app.config.get("SERVICE_ACCESS_ENABLED"):
            return []
        with self.fleet.db() as db:
            if not self._current(db, who):
                return []
            rows = [dict(r) for r in db.execute("""SELECT v.id,v.firma_id,f.name AS firma_name,v.kennzeichen,v.modell,v.km,v.km_datum
                FROM service_zuordnungen a JOIN kundenflotten_fahrzeuge v ON v.id=a.fahrzeug_id AND v.firma_id=a.firma_id
                JOIN kundenflotten_firmen f ON f.id=v.firma_id WHERE a.zugang_id=? AND a.aktiv=1 ORDER BY v.kennzeichen""", (who["id"],))]
        return [r for r in rows if self.authorize(who, r["firma_id"], r["id"])]

    def prepare_invitation(self, data):
        """Admin-only caller. Returned bearer token is displayed once and never persisted."""
        name, email = str(data.get("name") or "").strip(), str(data.get("email") or "").strip().lower()
        company = str(data.get("firma_id") or "")
        request_id = str(data.get("request_id") or "")
        values = data.getlist("fahrzeug_ids") if hasattr(data, "getlist") else data.get("fahrzeug_ids", [])
        if isinstance(values, str):
            values = [values]
        vehicles = list(dict.fromkeys(values))
        if not name or len(name) > 160 or any(ord(c) < 32 for c in name) or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email) or len(email) > 200:
            raise ValueError("Bitte Namen und gültige persönliche E-Mail-Adresse angeben.")
        if data.get("bestaetigt") != "1" or not re.fullmatch(r"[a-f0-9]{32}", request_id) or not 1 <= len(vehicles) <= 50:
            raise ValueError("Bitte ein bis 50 Fahrzeuge auswählen und die geprüfte Fahrerzuordnung bestätigen.")
        now, token = int(time.time()), secrets.token_urlsafe(32)
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            if db.execute("SELECT id FROM service_einladungen WHERE request_id=?", (request_id,)).fetchone():
                raise ValueError("Dieses Formular wurde bereits verwendet. Der Zugangscode wird nur einmal angezeigt; bei Bedarf einen neuen vorbereiten.")
            fingerprints = {}
            for vehicle in vehicles:
                car = db.execute("SELECT id FROM kundenflotten_fahrzeuge WHERE id=? AND firma_id=?", (vehicle, company)).fetchone()
                driver = db.execute("SELECT * FROM kundenflotten_fahrer WHERE fahrzeug_id=?", (vehicle,)).fetchone()
                matches = driver and (driver["email"].strip().lower() == email if driver["email"].strip() else driver["name"].strip().casefold() == name.casefold())
                if not car or not matches:
                    raise ValueError("Bitte die persönliche Fahrerzuordnung in der Fahrzeugakte zuerst prüfen und aktualisieren. Alle ausgewählten Fahrzeuge müssen zur Firma und zur Person gehören.")
                fingerprints[vehicle] = driver_fingerprint(driver)
            old = db.execute("SELECT * FROM service_zugaenge WHERE email=?", (email,)).fetchone()
            key, version = (old["id"], old["version"] + 1) if old else (uuid4().hex, 1)
            if old:
                db.execute("UPDATE service_zugaenge SET name=?,passwort_hash='',aktiv=0,version=?,geaendert_am=? WHERE id=?", (name, version, now, key))
                db.execute("UPDATE service_zuordnungen SET aktiv=0,version=version+1,beendet_am=? WHERE zugang_id=? AND aktiv=1", (now, key))
                db.execute("UPDATE service_einladungen SET widerrufen_am=? WHERE zugang_id=? AND benutzt_am IS NULL AND widerrufen_am IS NULL", (now, key))
            else:
                db.execute("INSERT INTO service_zugaenge VALUES(?,?,?,'',0,1,?,?)", (key, name, email, now, now))
            for vehicle in vehicles:
                db.execute("""INSERT INTO service_zuordnungen VALUES(?,?,?,?,?,1,1,?,NULL)
                    ON CONFLICT(zugang_id,fahrzeug_id) DO UPDATE SET firma_id=excluded.firma_id,
                    fahrer_fingerprint=excluded.fahrer_fingerprint,aktiv=1,version=service_zuordnungen.version+1,beendet_am=NULL""", (uuid4().hex, key, company, vehicle, fingerprints[vehicle], now))
            db.execute("INSERT INTO service_einladungen VALUES(?,?,?,?,?,NULL,NULL,?,?)", (uuid4().hex, key, hashlib.sha256(token.encode()).hexdigest(), version, now + INVITATION_SECONDS, request_id, now))
        return {"token": token, "gueltig_bis": now + INVITATION_SECONDS, "name": name, "email": email, "zugang_id": key}

    @staticmethod
    def _invitation(db, token):
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}", str(token)):
            return None
        return db.execute("""SELECT i.*,z.name,z.email FROM service_einladungen i JOIN service_zugaenge z ON z.id=i.zugang_id
            WHERE i.token_hash=? AND i.benutzt_am IS NULL AND i.widerrufen_am IS NULL
            AND i.gueltig_bis>? AND i.zugang_version=z.version""", (hashlib.sha256(token.encode()).hexdigest(), int(time.time()))).fetchone()

    def invitation(self, token):
        with self.fleet.db() as db:
            row = self._invitation(db, token)
            return {"name": row["name"], "email": row["email"]} if row else None

    def accept(self, token, password, repeated):
        if not isinstance(password, str) or not 12 <= len(password) <= 128 or password != repeated:
            raise ValueError("Bitte ein Passwort mit 12–128 Zeichen zweimal übereinstimmend eingeben.")
        hashed, now = generate_password_hash(password), int(time.time())
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            row = self._invitation(db, token)
            if not row:
                raise ValueError("Dieser Zugangscode ist abgelaufen, widerrufen oder bereits verwendet. Bitte einen neuen anfordern.")
            db.execute("UPDATE service_einladungen SET benutzt_am=? WHERE id=?", (now, row["id"]))
            db.execute("UPDATE service_zugaenge SET passwort_hash=?,aktiv=1,geaendert_am=? WHERE id=? AND version=?", (hashed, now, row["zugang_id"], row["zugang_version"]))


def register_service_access(p, fleet):
    service = ServiceAccess(p, fleet)
    p.service_access = service
    p.app.config.setdefault("SERVICE_ACCESS_ENABLED", os.environ.get("SERVICE_ACCESS_ENABLED", "0") == "1")
    bp = Blueprint("servicezugang", __name__)
    dummy = generate_password_hash(secrets.token_urlsafe(24))

    def csrf_token():
        if not session.get("csrf_token"):
            session["csrf_token"] = secrets.token_urlsafe(32)
        return session["csrf_token"]

    @bp.before_request
    def protect():
        if not p.app.config.get("SERVICE_ACCESS_ENABLED"):
            abort(404)
        request.max_content_length = 32768
        if request.method == "POST":
            supplied = request.form.getlist("csrf_token")
            expected = session.get("csrf_token")
            if not expected or len(supplied) != 1 or not hmac.compare_digest(str(expected), str(supplied[0])):
                abort(400)

    @bp.after_request
    def private(response):
        response.headers.update({"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff", "X-Robots-Tag": "noindex, nofollow, noarchive"})
        response.vary.add("Cookie")
        return response

    @bp.route("/service/anmelden", methods=["GET", "POST"])
    def login():
        error, status = "", 200
        if request.method == "POST":
            limited, _ = p.login_rate_limit_status("servicezugang", "login")
            if limited:
                error, status = "Zu viele Fehlversuche. Bitte später erneut versuchen.", 429
            else:
                email = str(request.form.get("email") or "").strip().lower()[:201]
                password = str(request.form.get("passwort") or "")
                with fleet.db() as db:
                    row = db.execute("SELECT * FROM service_zugaenge WHERE email=?", (email,)).fetchone()
                good = check_password_hash(row["passwort_hash"] if row and row["passwort_hash"] else dummy, password[:129])
                if not row or row["aktiv"] != 1 or len(password) > 128 or not good:
                    p.record_failed_login("servicezugang", "login")
                    error, status = "Die Anmeldung hat nicht geklappt. Bitte Zugangsdaten prüfen.", 401
                else:
                    p.clear_login_attempts("servicezugang", "login")
                    session.clear()
                    session.update(service_zugang=row["id"], service_version=row["version"], service_login_am=time.time(), csrf_token=secrets.token_urlsafe(32))
                    response = redirect("/service", code=303)
                    for scope in ("admin", "partner"):
                        p.clear_remember_login_cookie(response, scope)
                    return response
        return render_template("servicezugang_login.html", error=error, csrf_token=csrf_token()), status

    @bp.post("/service/abmelden")
    def logout():
        session.clear()
        return redirect("/service/anmelden", code=303)

    @bp.route("/service/einladung/<token>", methods=["GET", "POST"])
    def invitation(token):
        data = service.invitation(token)
        if not data:
            return render_template("servicezugang_einladung.html", invitation=None, error="Dieser Zugangscode ist abgelaufen, widerrufen oder bereits verwendet.", csrf_token=csrf_token()), 410
        error, status = "", 200
        if request.method == "POST":
            limited, _ = p.login_rate_limit_status("servicezugang", "einladung")
            if limited:
                error, status = "Zu viele Versuche. Bitte später erneut versuchen.", 429
            else:
                try:
                    service.accept(token, request.form.get("passwort"), request.form.get("passwort_wiederholen"))
                    p.clear_login_attempts("servicezugang", "einladung")
                    session.clear()
                    return redirect("/service/anmelden?eingerichtet=1", code=303)
                except ValueError as exc:
                    p.record_failed_login("servicezugang", "einladung")
                    error, status = str(exc), 400
        return render_template("servicezugang_einladung.html", invitation=data, error=error, csrf_token=csrf_token()), status

    @bp.route("/admin/service-zugaenge", methods=["GET", "POST"])
    @p.admin_required
    def admin():
        result, error, status = None, "", 200
        if request.method == "POST":
            try:
                if request.form.get("aktion") == "sperren":
                    key = str(request.form.get("zugang_id") or "")
                    with p.portal_originals_operation_lock(), fleet.db() as db:
                        now = int(time.time())
                        db.execute("UPDATE service_zugaenge SET aktiv=0,version=version+1,geaendert_am=? WHERE id=?", (now, key))
                        db.execute("UPDATE service_zuordnungen SET aktiv=0,version=version+1,beendet_am=? WHERE zugang_id=? AND aktiv=1", (now, key))
                        db.execute("UPDATE service_einladungen SET widerrufen_am=? WHERE zugang_id=? AND benutzt_am IS NULL AND widerrufen_am IS NULL", (now, key))
                    return redirect("/admin/service-zugaenge", code=303)
                result = service.prepare_invitation(request.form)
            except ValueError as exc:
                error, status = str(exc), 400
        with fleet.db() as db:
            users = [dict(r) for r in db.execute("SELECT id,name,email,aktiv,version FROM service_zugaenge ORDER BY name")]
            cars = [dict(r) for r in db.execute("""SELECT v.id,v.firma_id,v.kennzeichen,v.modell,f.name AS firma_name,d.name AS fahrer_name,d.email AS fahrer_email
                FROM kundenflotten_fahrzeuge v JOIN kundenflotten_firmen f ON f.id=v.firma_id
                LEFT JOIN kundenflotten_fahrer d ON d.fahrzeug_id=v.id ORDER BY f.name,v.kennzeichen""")]
        return render_template("servicezugang_admin.html", users=users, cars=cars, companies=fleet.companies(), result=result, error=error, request_id=uuid4().hex, csrf_token=csrf_token()), status

    p.app.register_blueprint(bp)
    return service
