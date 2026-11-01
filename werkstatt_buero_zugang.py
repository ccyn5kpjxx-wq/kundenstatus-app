"""Explicit office grants layered over existing personal employee identities.

No employee is enabled by default. This module never changes assistant rights
or promotes an employee to an administrator. Endpoint names and HTTP methods
must be listed explicitly; an empty category grants no route access.
"""
from contextlib import contextmanager
from datetime import datetime
import hmac
import json
from pathlib import Path
import re
import secrets
import sqlite3
from uuid import uuid4
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template_string, request, session, url_for


BACKUP_TABLES = ("buero_zugaenge", "buero_zugang_audit")
SCHEMA_FEATURE = "office_access_v1"
CATEGORY_LABELS = {
    "termine": "Heute und Termine",
    "hol_bring": "Hol- und Bringservice",
    "arbeitszeiten": "Arbeitszeiten",
    "urlaub": "Urlaub und Abwesenheiten",
    "mahnungen": "Mahnungen",
    "news": "News",
    "bewerbungen": "Bewerbungen und Einladungen",
    "lieferanten": "Lieferanten",
    "mailbox": "Mailbox und E-Mails",
    "partnerzugaenge": "Lieferanten- und Autohauszugänge",
    "autohausportale": "Autohausportale",
    "belege": "Eingangsbelege vorbereiten",
    "loehne": "Freigegebene Löhne zur Überweisung",
    "preise": "Interne Preisliste",
}
# Populated with audited, exact Flask endpoint names by application integration.
# Format: category -> {endpoint: frozenset({"GET", "POST", ...})}.
OFFICE_ALLOWED_ENDPOINTS = {category: {} for category in CATEGORY_LABELS}
_ENDPOINT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


def _categories(value):
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("Mindestens einen Bürobereich auswählen.")
    if any(not isinstance(item, str) or item not in CATEGORY_LABELS for item in value):
        raise ValueError("Unbekannter Bürobereich.")
    if len(set(value)) != len(value):
        raise ValueError("Bürobereiche nur einmal auswählen.")
    return sorted(value)


def _positive_id(value):
    if type(value) is not int or not 1 <= value <= 2147483647:
        raise ValueError("Gültigen Mitarbeiter auswählen.")
    return value


class OfficeAccess:
    def __init__(self, portal):
        self.p = portal

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
            db.execute("""CREATE TABLE IF NOT EXISTS buero_zugaenge (
                mitarbeiter_id INTEGER PRIMARY KEY,
                aktiv INTEGER NOT NULL DEFAULT 0,
                kategorien_json TEXT NOT NULL DEFAULT '[]',
                version INTEGER NOT NULL DEFAULT 1,
                rechte_version INTEGER NOT NULL,
                auth_version INTEGER NOT NULL,
                geaendert_am TEXT NOT NULL,
                geaendert_von TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS buero_zugang_audit (
                id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL,
                actor TEXT NOT NULL, aktion TEXT NOT NULL,
                details_json TEXT NOT NULL, erstellt_am TEXT NOT NULL)""")

    def identity(self, db=None):
        if db is None:
            with self.db() as connection:
                return self.identity(connection)
        who = self.p.employee_portal.identity(db)
        if not isinstance(who, dict):
            return None
        mid = who.get("mitarbeiter_id")
        if (type(mid) is not int or mid < 1 or who.get("actor") != f"mitarbeiter:{mid}"
                or not who.get("aktiv") or not who.get("lesen")):
            return None
        row = db.execute("SELECT * FROM buero_zugaenge WHERE mitarbeiter_id=?", (mid,)).fetchone()
        if (not row or row["aktiv"] != 1 or row["rechte_version"] != who.get("version")
                or row["auth_version"] != who.get("auth_version")):
            return None
        try:
            categories = _categories(json.loads(row["kategorien_json"]))
        except (TypeError, ValueError):
            return None
        return dict(who, office_categories=tuple(categories), office_version=row["version"])

    def is_office_user(self):
        return self.identity() is not None

    def allows(self, endpoint, method):
        # Refuse prefix strings, URLs, wildcard names and administrative grants.
        if (not isinstance(endpoint, str) or not _ENDPOINT.fullmatch(endpoint)
                or endpoint == "office_access.admin" or not isinstance(method, str) or method not in _METHODS):
            return False
        who = self.identity()
        if who is None:
            return False
        for category in who["office_categories"]:
            rules = OFFICE_ALLOWED_ENDPOINTS.get(category, {})
            methods = rules.get(endpoint) if isinstance(rules, dict) else None
            if isinstance(methods, (set, frozenset, tuple, list)) and method in methods:
                return True
        return False

    @staticmethod
    def _chef():
        # Deliberately independent from admin_required: that decorator may allow
        # office routes later, while modifying grants must remain chef-only.
        if not session.get("admin"):
            raise PermissionError("Nur die Werkstattleitung darf Bürozugänge verwalten.")

    def set_access(self, employee_id, categories, *, active=True, expected_version):
        self._chef()
        mid = _positive_id(employee_id)
        if type(active) is not bool or type(expected_version) is not int or expected_version < 0:
            raise ValueError("Freigabeversion ungültig. Seite neu laden.")
        selected = _categories(categories) if active else []
        now = datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="seconds")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            # A row lock across PostgreSQL workers; SQLite writes serialize too.
            db.execute("UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?", (mid,))
            employee = db.execute("SELECT id,aktiv FROM mitarbeiter WHERE id=?", (mid,)).fetchone()
            if not employee:
                raise ValueError("Mitarbeiter wurde nicht gefunden.")
            rights = db.execute("SELECT version,auth_version,lesen FROM assistent_rechte WHERE mitarbeiter_id=?", (mid,)).fetchone()
            old = db.execute("SELECT * FROM buero_zugaenge WHERE mitarbeiter_id=?", (mid,)).fetchone()
            version = old["version"] if old else 0
            if version != expected_version:
                raise ValueError("Die Bürofreigabe wurde inzwischen geändert. Seite neu laden.")
            if active and (not employee["aktiv"] or not rights or not rights["lesen"]):
                raise ValueError("Zuerst einen aktiven persönlichen Mitarbeiterzugang einrichten.")
            rv = rights["version"] if rights else (old["rechte_version"] if old else 0)
            av = rights["auth_version"] if rights else (old["auth_version"] if old else 0)
            encoded = json.dumps(selected, ensure_ascii=False)
            values = (int(active), encoded, rv, av, now, "admin")
            if old:
                changed = db.execute("""UPDATE buero_zugaenge SET aktiv=?,kategorien_json=?,
                    rechte_version=?,auth_version=?,geaendert_am=?,geaendert_von=?,version=version+1
                    WHERE mitarbeiter_id=? AND version=? RETURNING version""", values + (mid, version)).fetchone()
            else:
                changed = db.execute("""INSERT INTO buero_zugaenge(mitarbeiter_id,aktiv,kategorien_json,
                    rechte_version,auth_version,geaendert_am,geaendert_von,version)
                    VALUES(?,?,?,?,?,?,?,1) RETURNING version""", (mid,) + values).fetchone()
            if not changed:
                raise ValueError("Die Bürofreigabe wurde inzwischen geändert. Seite neu laden.")
            details = {"vorher": dict(old) if old else None,
                       "nachher": {"aktiv": int(active), "kategorien": selected, "version": changed["version"]}}
            db.execute("""INSERT INTO buero_zugang_audit
                (id,mitarbeiter_id,actor,aktion,details_json,erstellt_am) VALUES(?,?,?,?,?,?)""",
                       (uuid4().hex, mid, "admin", "freigegeben" if active else "entzogen",
                        json.dumps(details, ensure_ascii=False), now))
        hook = getattr(self.p, "schedule_change_backup", None)
        if callable(hook):
            hook("buero-zugang")
        return changed["version"]

    def admin_rows(self):
        self._chef()
        with self.db() as db:
            rows = db.execute("""SELECT m.id,m.name,m.aktiv,r.lesen,
                b.aktiv AS buero_aktiv,b.kategorien_json,b.version AS buero_version
                FROM mitarbeiter m LEFT JOIN assistent_rechte r ON r.mitarbeiter_id=m.id
                LEFT JOIN buero_zugaenge b ON b.mitarbeiter_id=m.id ORDER BY m.name,m.id""").fetchall()
        result = []
        for raw in rows:
            row = dict(raw)
            try:
                row["categories"] = _categories(json.loads(row["kategorien_json"]))
            except (TypeError, ValueError):
                row["categories"] = []
            result.append(row)
        return result


def ensure_office_access_for_import(p, *, export=None, imported_db=None, target=None, **_):
    """Do not resurrect revoked office grants or forget their audit on restore.

    The application must include BACKUP_TABLES in exports and call this guard
    under the same re-entrant originals lock before replacing live tables.
    """
    own = target is None
    source = None
    db = target if target is not None else p.get_db()
    try:
        with p.portal_originals_operation_lock():
            if imported_db is not None:
                source = sqlite3.connect(Path(imported_db).resolve().as_uri() + "?mode=ro", uri=True)
                source.row_factory = sqlite3.Row
            for table in BACKUP_TABLES:
                current = [dict(r) for r in db.execute("SELECT * FROM " + table).fetchall()]
                if source is not None:
                    exists = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    incoming = [dict(r) for r in source.execute("SELECT * FROM " + table).fetchall()] if exists else []
                else:
                    incoming = (export or {}).get("tables", {}).get(table, [])
                if not isinstance(incoming, list) or any(not isinstance(row, dict) for row in incoming):
                    raise ValueError()
                key = "mitarbeiter_id" if table == "buero_zugaenge" else "id"
                actual = {row.get(key): row for row in incoming}
                expected = {row[key]: row for row in current}
                if len(actual) != len(incoming) or actual != expected:
                    raise ValueError()
    except (TypeError, KeyError, ValueError, sqlite3.Error):
        raise ValueError("Import gestoppt: Bürofreigaben oder deren Widerrufe/Audit würden verändert. Eine aktuelle vollständige Sicherung verwenden.") from None
    finally:
        if source is not None:
            source.close()
        if own:
            db.close()


_ADMIN_TEMPLATE = """<!doctype html><html lang="de"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bürozugänge</title><main style="max-width:980px;margin:2rem auto;padding:0 1rem;font:16px/1.5 system-ui">
<a href="/admin">Zum Chefbereich</a><h1>Persönliche Bürozugänge</h1>
<p>Nur ausdrücklich ausgewählte Bereiche sind erreichbar. Persönliche Anmeldung erforderlich.
Assistent, Bestellordner und Mitarbeiterverwaltung werden hier nicht freigegeben.</p>
{% if error %}<p role="alert">{{ error }}</p>{% endif %}
{% if saved %}<p role="status">Bürofreigabe gespeichert.</p>{% endif %}
{% for person in people %}<section style="border:1px solid #ccc;padding:1rem;margin:1rem 0">
<h2>{{ person.name }}</h2><p>{{ 'Freigegeben' if person.buero_aktiv else 'Kein Bürozugang' }}
{% if not person.aktiv or not person.lesen %} · Persönlicher Zugang nicht aktiv{% endif %}</p>
<form method="post"><input type="hidden" name="csrf_token" value="{{ csrf }}">
<input type="hidden" name="mitarbeiter_id" value="{{ person.id }}">
<input type="hidden" name="version" value="{{ person.buero_version or 0 }}">
<fieldset><legend>Erlaubte Bürobereiche</legend>{% for key,label in categories.items() %}
<label style="display:block"><input type="checkbox" name="categories" value="{{ key }}"
{% if key in person.categories %}checked{% endif %}> {{ label }}</label>{% endfor %}</fieldset>
<button name="action" value="save">Ausgewählte Bereiche freigeben</button>
{% if person.buero_version %}<button name="action" value="revoke">Bürozugang entziehen</button>{% endif %}
</form></section>{% endfor %}</main></html>"""


def register_office_access(p):
    service = OfficeAccess(p)
    p.office_access = service
    bp = Blueprint("office_access", __name__)

    @bp.route("/admin/buero-zugaenge", methods=["GET", "POST"])
    @p.admin_required
    def admin():
        if not session.get("admin"):
            abort(403)
        if not session.get("csrf_token"):
            session["csrf_token"] = secrets.token_urlsafe(32)
        error = ""
        if request.method == "POST":
            tokens = request.form.getlist("csrf_token")
            if (not tokens or not isinstance(session["csrf_token"], str)
                    or any(not hmac.compare_digest(session["csrf_token"], str(token)) for token in tokens)):
                abort(400)
            try:
                for key in ("mitarbeiter_id", "version", "action"):
                    if len(request.form.getlist(key)) != 1:
                        raise ValueError("Formular ungültig. Seite neu laden.")
                mid, version = request.form["mitarbeiter_id"], request.form["version"]
                if not re.fullmatch(r"[1-9][0-9]{0,9}", mid) or not re.fullmatch(r"[0-9]{1,10}", version):
                    raise ValueError("Mitarbeiter oder Freigabeversion ungültig.")
                action = request.form["action"]
                if action not in {"save", "revoke"}:
                    raise ValueError("Unbekannte Aktion.")
                service.set_access(int(mid), request.form.getlist("categories"),
                                   active=action == "save", expected_version=int(version))
                return redirect(url_for("office_access.admin", saved="1"), code=303)
            except ValueError as exc:
                error = str(exc)
        return render_template_string(_ADMIN_TEMPLATE, people=service.admin_rows(), categories=CATEGORY_LABELS,
                                      csrf=session["csrf_token"], error=error,
                                      saved=request.args.get("saved") == "1"), 400 if error else 200

    @bp.after_request
    def private(response):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    p.app.register_blueprint(bp)
    return service
