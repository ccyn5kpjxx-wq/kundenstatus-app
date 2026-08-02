from __future__ import annotations

import os
import re
import secrets
import sqlite3
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import wraps
from pathlib import Path
from urllib.parse import quote, urlparse
from uuid import uuid4

from flask import (
    Flask,
    abort,
    current_app,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

try:
    from tomorrowworks_dashboard.integration import (
        git_projektstand,
        netzwerk_adressen,
        vorschau_aktiv,
        vorschau_starten,
        vorschau_stoppen,
    )
except ModuleNotFoundError:  # Erlaubt weiterhin: python tomorrowworks_dashboard/app.py
    from integration import (
        git_projektstand,
        netzwerk_adressen,
        vorschau_aktiv,
        vorschau_starten,
        vorschau_stoppen,
    )

try:
    from tomorrowworks_dashboard.notifications import mail_senden
except ModuleNotFoundError:
    from notifications import mail_senden

try:
    from tomorrowworks_dashboard.sales_video import (
        SalesVideoError,
        SalesVideoSource,
        create_sales_video,
        validate_sales_video_mp4,
    )
except ModuleNotFoundError:
    from sales_video import SalesVideoError, SalesVideoSource, create_sales_video, validate_sales_video_mp4


BASE_DIR = Path(__file__).resolve().parent
REPOSITORY_DIR = BASE_DIR.parent

PROJEKT_STATUS = [
    ("neu", "Neu"),
    ("planung", "Planung"),
    ("in_arbeit", "In Arbeit"),
    ("interne_pruefung", "Interne Prüfung"),
    ("kundenfreigabe", "Beim Kunden"),
    ("aenderungen", "Änderungen"),
    ("wartet_auf_kunde", "Wartet auf Kunde"),
    ("blockiert", "Blockiert"),
    ("veroeffentlicht", "Veröffentlicht"),
    ("abgeschlossen", "Abgeschlossen"),
    ("pausiert", "Pausiert"),
]
PROJEKT_STATUS_LABELS = dict(PROJEKT_STATUS)

PROJEKT_TYPEN = [
    ("website", "Website"),
    ("app", "App"),
    ("webapp", "Web-App / Portal"),
    ("branding", "Logo & Branding"),
    ("print", "Print & Visitenkarten"),
    ("marketing", "Marketing"),
    ("automatisierung", "Automatisierung"),
    ("sonstiges", "Sonstiges"),
]
PROJEKT_TYP_LABELS = dict(PROJEKT_TYPEN)

PRIORITAETEN = [
    ("niedrig", "Niedrig"),
    ("normal", "Normal"),
    ("hoch", "Hoch"),
    ("dringend", "Dringend"),
]

TEAM_FARBEN = ["#ff641a", "#5b8def", "#8b5cf6", "#16a085", "#d35400", "#be3f75"]
ERLAUBTE_DATEIEN = {"pdf", "png", "jpg", "jpeg", "webp"}
VIDEO_QUELLDATEIEN = {"pdf", "png", "jpg", "jpeg", "webp"}
VIDEO_DATEIEN = {"mp4"}
VIDEO_GENERATION_LOCK = threading.Lock()
PORTAL_DATEIEN = {"pdf", "png", "jpg", "jpeg", "webp", "docx", "xlsx", "zip", "ai", "eps", "svg"}
ANGEBOT_STATUS_LABELS = {
    "entwurf": "Entwurf",
    "gesendet": "Beim Kunden",
    "freigegeben": "Vom Kunden freigegeben",
    "angenommen": "Vertrag angenommen",
    "abgelehnt": "Abgelehnt",
}
TICKET_PHASE_LABELS = {
    "kennenlernen": "Kennenlernen",
    "konzept": "Konzept & Material",
    "angebot": "Angebot & Vertrag",
    "umsetzung": "Umsetzung",
    "betreuung": "Laufende Betreuung",
}
GIT_STATUS_LABELS = {
    "nicht_verbunden": "Nicht verbunden",
    "pfad_fehlt": "Ordner fehlt",
    "kein_git": "Noch kein Git",
    "kein_commit": "Noch kein Commit",
    "lokale_aenderungen": "Lokale Änderungen",
    "push_ausstehend": "Push ausstehend",
    "pull_ausstehend": "Neue Version verfügbar",
    "abweichung": "Abgleich nötig",
    "kein_upstream": "Noch nicht veröffentlicht",
    "aktuell": "Git aktuell",
}


SCHEMA = """
CREATE TABLE IF NOT EXISTS kunden (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firma TEXT NOT NULL,
    ansprechpartner TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    telefon TEXT NOT NULL DEFAULT '',
    adresse TEXT NOT NULL DEFAULT '',
    website TEXT NOT NULL DEFAULT '',
    keine_website INTEGER NOT NULL DEFAULT 0,
    whatsapp_freigabe INTEGER NOT NULL DEFAULT 0,
    notizen TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'aktiv',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS teammitglieder (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    rolle TEXT NOT NULL DEFAULT 'team',
    password_hash TEXT NOT NULL,
    farbe TEXT NOT NULL DEFAULT '#ff641a',
    aktiv INTEGER NOT NULL DEFAULT 1,
    fokus_projekt_id INTEGER,
    fokus_notiz TEXT NOT NULL DEFAULT '',
    fokus_aktualisiert_am TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projekte (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kunde_id INTEGER NOT NULL REFERENCES kunden(id) ON DELETE CASCADE,
    titel TEXT NOT NULL,
    typ TEXT NOT NULL DEFAULT 'website',
    beschreibung TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'neu',
    prioritaet TEXT NOT NULL DEFAULT 'normal',
    startdatum TEXT,
    zieldatum TEXT,
    fortschritt INTEGER NOT NULL DEFAULT 0,
    aktuelle_aufgabe TEXT NOT NULL DEFAULT '',
    blockiert_grund TEXT NOT NULL DEFAULT '',
    vorschau_url TEXT NOT NULL DEFAULT '',
    repo_url TEXT NOT NULL DEFAULT '',
    lokaler_pfad TEXT NOT NULL DEFAULT '',
    preview_pfad TEXT NOT NULL DEFAULT '.',
    preview_port INTEGER,
    preview_aktiv INTEGER NOT NULL DEFAULT 0,
    agent_token TEXT NOT NULL DEFAULT '',
    git_status TEXT NOT NULL DEFAULT 'nicht_verbunden',
    git_branch TEXT NOT NULL DEFAULT '',
    git_commit TEXT NOT NULL DEFAULT '',
    git_kurz TEXT NOT NULL DEFAULT '',
    git_author TEXT NOT NULL DEFAULT '',
    git_author_email TEXT NOT NULL DEFAULT '',
    git_nachricht TEXT NOT NULL DEFAULT '',
    git_geaendert_am TEXT NOT NULL DEFAULT '',
    git_dirty INTEGER NOT NULL DEFAULT 0,
    git_ahead INTEGER NOT NULL DEFAULT 0,
    git_behind INTEGER NOT NULL DEFAULT 0,
    git_last_checked TEXT NOT NULL DEFAULT '',
    git_fehler TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projekt_team (
    projekt_id INTEGER NOT NULL REFERENCES projekte(id) ON DELETE CASCADE,
    teammitglied_id INTEGER NOT NULL REFERENCES teammitglieder(id) ON DELETE CASCADE,
    ist_hauptverantwortlich INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (projekt_id, teammitglied_id)
);

CREATE TABLE IF NOT EXISTS projekt_dateien (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    projekt_id INTEGER NOT NULL REFERENCES projekte(id) ON DELETE CASCADE,
    original_name TEXT NOT NULL,
    gespeichert_name TEXT NOT NULL,
    mimetype TEXT NOT NULL DEFAULT 'application/octet-stream',
    hochgeladen_von INTEGER REFERENCES teammitglieder(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aktivitaeten (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    projekt_id INTEGER NOT NULL REFERENCES projekte(id) ON DELETE CASCADE,
    teammitglied_id INTEGER REFERENCES teammitglieder(id) ON DELETE SET NULL,
    aktion TEXT NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kunden_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kunde_id INTEGER NOT NULL UNIQUE REFERENCES kunden(id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'aktiv',
    phase TEXT NOT NULL DEFAULT 'kennenlernen',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_activity_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portal_nachrichten (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id INTEGER NOT NULL REFERENCES kunden_tickets(id) ON DELETE CASCADE,
    projekt_id INTEGER REFERENCES projekte(id) ON DELETE SET NULL,
    teammitglied_id INTEGER REFERENCES teammitglieder(id) ON DELETE SET NULL,
    absender TEXT NOT NULL,
    art TEXT NOT NULL DEFAULT 'nachricht',
    text TEXT NOT NULL DEFAULT '',
    gelesen_team INTEGER NOT NULL DEFAULT 0,
    gelesen_kunde INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portal_dateien (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id INTEGER NOT NULL REFERENCES kunden_tickets(id) ON DELETE CASCADE,
    nachricht_id INTEGER NOT NULL REFERENCES portal_nachrichten(id) ON DELETE CASCADE,
    original_name TEXT NOT NULL,
    gespeichert_name TEXT NOT NULL,
    mimetype TEXT NOT NULL DEFAULT 'application/octet-stream',
    groesse INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portal_benachrichtigungen (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id INTEGER NOT NULL REFERENCES kunden_tickets(id) ON DELETE CASCADE,
    nachricht_id INTEGER REFERENCES portal_nachrichten(id) ON DELETE SET NULL,
    richtung TEXT NOT NULL,
    empfaenger TEXT NOT NULL DEFAULT '',
    betreff TEXT NOT NULL,
    status TEXT NOT NULL,
    fehler TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS ticket_angebote (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id INTEGER NOT NULL UNIQUE REFERENCES kunden_tickets(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'entwurf',
    paket_name TEXT NOT NULL DEFAULT 'Website & digitales Wachstumssystem',
    einmalig_cent INTEGER NOT NULL DEFAULT 250000,
    monatlich_cent INTEGER NOT NULL DEFAULT 30000,
    einrichtung_cent INTEGER NOT NULL DEFAULT 0,
    laufzeit_monate INTEGER NOT NULL DEFAULT 12,
    kuendigungsfrist_monate INTEGER NOT NULL DEFAULT 3,
    leistungsumfang TEXT NOT NULL DEFAULT '',
    hinweise TEXT NOT NULL DEFAULT '',
    gueltig_bis TEXT,
    erstellt_von INTEGER REFERENCES teammitglieder(id) ON DELETE SET NULL,
    sent_at TEXT,
    accepted_at TEXT,
    accepted_name TEXT NOT NULL DEFAULT '',
    accepted_email TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_projekte_kunde ON projekte(kunde_id);
CREATE INDEX IF NOT EXISTS idx_projekte_status ON projekte(status);
CREATE INDEX IF NOT EXISTS idx_projekte_zieldatum ON projekte(zieldatum);
CREATE INDEX IF NOT EXISTS idx_aktivitaeten_projekt ON aktivitaeten(projekt_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_portal_nachrichten_ticket ON portal_nachrichten(ticket_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_portal_nachrichten_ungelesen ON portal_nachrichten(gelesen_team, absender);
CREATE INDEX IF NOT EXISTS idx_portal_dateien_nachricht ON portal_dateien(nachricht_id);
"""


def jetzt() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def _lade_oder_erzeuge_secret(data_dir: Path) -> str:
    env_secret = os.getenv("TW_DASHBOARD_SECRET_KEY", "").strip()
    if env_secret:
        return env_secret
    secret_path = data_dir / "session_secret.txt"
    if secret_path.exists():
        return secret_path.read_text(encoding="utf-8").strip()
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    value = secrets.token_urlsafe(48)
    secret_path.write_text(value, encoding="utf-8")
    return value


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    data_dir = Path(os.getenv("TW_DASHBOARD_DATA_DIR", REPOSITORY_DIR / "data" / "tomorrowworks_dashboard"))
    database = Path(os.getenv("TW_DASHBOARD_DB_PATH", data_dir / "dashboard.db"))
    upload_folder = Path(os.getenv("TW_DASHBOARD_UPLOAD_DIR", data_dir / "uploads"))
    application_root = os.getenv("TW_APPLICATION_ROOT", "/").strip() or "/"
    if not application_root.startswith("/"):
        application_root = f"/{application_root}"
    application_root = application_root.rstrip("/") or "/"
    secure_cookie = os.getenv(
        "TW_SESSION_COOKIE_SECURE",
        "1" if os.getenv("RENDER") else "0",
    ) == "1"

    app.config.update(
        DATABASE=str(database),
        UPLOAD_FOLDER=str(upload_folder),
        MAX_CONTENT_LENGTH=20 * 1024 * 1024,
        SECRET_KEY=_lade_oder_erzeuge_secret(data_dir),
        APPLICATION_ROOT=application_root,
        PREFERRED_URL_SCHEME="https" if secure_cookie else "http",
        SESSION_COOKIE_NAME="tomorrowworks_session",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=secure_cookie,
        SESSION_COOKIE_PATH=None if application_root == "/" else application_root,
        DASHBOARD_PORT=int(os.getenv("TW_DASHBOARD_PORT", "5070")),
        SQLITE_BUSY_TIMEOUT_MS=max(5_000, int(os.getenv("TW_SQLITE_BUSY_TIMEOUT_MS", "30000"))),
        GIT_MONITOR_ENABLED=os.getenv("TW_GIT_MONITOR", "1") != "0",
        GIT_MONITOR_INTERVAL=int(os.getenv("TW_GIT_MONITOR_INTERVAL", "30")),
        PUBLIC_BASE_URL=os.getenv("TW_PUBLIC_BASE_URL", "").strip().rstrip("/"),
        SMTP_HOST=os.getenv("TW_SMTP_HOST", "").strip(),
        SMTP_PORT=int(os.getenv("TW_SMTP_PORT", "587")),
        SMTP_USER=os.getenv("TW_SMTP_USER", "").strip(),
        SMTP_PASSWORD=os.getenv("TW_SMTP_PASSWORD", ""),
        SMTP_FROM=os.getenv("TW_SMTP_FROM", "").strip(),
        SMTP_TLS=os.getenv("TW_SMTP_TLS", "1") == "1",
        SMTP_SSL=os.getenv("TW_SMTP_SSL", "0") == "1",
        SMTP_TIMEOUT=int(os.getenv("TW_SMTP_TIMEOUT", "12")),
        TEAM_NOTIFY_EMAIL=os.getenv("TW_TEAM_NOTIFY_EMAIL", "").strip(),
        CONTRACT_LEGAL_APPROVED=os.getenv("TW_CONTRACT_LEGAL_APPROVED", "0") == "1",
        MIGRATION_TOKEN=os.getenv("TW_MIGRATION_TOKEN", "").strip(),
        TTS_API_KEY=(os.getenv("TW_TTS_API_KEY", "").strip() or os.getenv("OPENAI_API_KEY", "").strip()),
        TTS_MODEL=os.getenv("TW_TTS_MODEL", "gpt-4o-mini-tts").strip() or "gpt-4o-mini-tts",
        TTS_VOICE=os.getenv("TW_TTS_VOICE", "coral").strip() or "coral",
        TTS_INSTRUCTIONS=os.getenv(
            "TW_TTS_INSTRUCTIONS",
            (
                "Sprich auf Deutsch mit einer warmen, natürlichen und zuversichtlichen weiblichen Stimme. "
                "Klinge persönlich, hochwertig und verkaufsstark, aber nie aufdringlich. Nutze kurze Pausen, "
                "ein hörbares Lächeln und betone Zukunft, Transparenz und gemeinsame Weiterentwicklung."
            ),
        ).strip(),
    )
    if test_config:
        app.config.update(test_config)
    if app.config.get("TESTING") and not (test_config or {}).get("GIT_MONITOR_ENABLED"):
        app.config["GIT_MONITOR_ENABLED"] = False

    Path(app.config["DATABASE"]).parent.mkdir(parents=True, exist_ok=True)
    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)

    app.teardown_appcontext(close_db)

    with app.app_context():
        init_db()

    register_helpers(app)
    register_routes(app)
    if app.config["GIT_MONITOR_ENABLED"]:
        git_monitor_starten(app)
    return app


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        timeout_ms = int(current_app.config["SQLITE_BUSY_TIMEOUT_MS"])
        g.db = sqlite3.connect(
            current_app.config["DATABASE"],
            timeout=timeout_ms / 1000,
        )
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.execute(f"PRAGMA busy_timeout = {timeout_ms}")
        g.db.execute("PRAGMA journal_mode = WAL")
        g.db.execute("PRAGMA synchronous = NORMAL")
    return g.db


def close_db(_error=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _ticket_sicherstellen(db: sqlite3.Connection, kunde_id: int) -> sqlite3.Row:
    ticket = db.execute("SELECT * FROM kunden_tickets WHERE kunde_id = ?", (kunde_id,)).fetchone()
    if ticket is not None:
        return ticket
    timestamp = jetzt()
    cur = db.execute(
        """
        INSERT INTO kunden_tickets (kunde_id, token, status, phase, created_at, updated_at, last_activity_at)
        VALUES (?, ?, 'aktiv', 'kennenlernen', ?, ?, ?)
        """,
        (kunde_id, secrets.token_urlsafe(36), timestamp, timestamp, timestamp),
    )
    ticket_id = cur.lastrowid
    db.execute(
        """
        INSERT INTO portal_nachrichten
          (ticket_id, absender, art, text, gelesen_team, gelesen_kunde, created_at)
        VALUES (?, 'system', 'meilenstein', ?, 1, 0, ?)
        """,
        (
            ticket_id,
            "Der gemeinsame Projektraum wurde eröffnet. Hier bleiben Fragen, Dateien, Entscheidungen und Neuerungen dauerhaft an einem Ort.",
            timestamp,
        ),
    )
    return db.execute("SELECT * FROM kunden_tickets WHERE id = ?", (ticket_id,)).fetchone()


def init_db() -> None:
    db = get_db()
    db.executescript(SCHEMA)
    kunden_spalten = {
        "adresse": "TEXT NOT NULL DEFAULT ''",
        "keine_website": "INTEGER NOT NULL DEFAULT 0",
        "whatsapp_freigabe": "INTEGER NOT NULL DEFAULT 0",
    }
    vorhandene_kundenspalten = {row[1] for row in db.execute("PRAGMA table_info(kunden)").fetchall()}
    for name, definition in kunden_spalten.items():
        if name not in vorhandene_kundenspalten:
            db.execute(f"ALTER TABLE kunden ADD COLUMN {name} {definition}")
    projekt_spalten = {
        "preview_pfad": "TEXT NOT NULL DEFAULT '.'",
        "preview_port": "INTEGER",
        "preview_aktiv": "INTEGER NOT NULL DEFAULT 0",
        "agent_token": "TEXT NOT NULL DEFAULT ''",
        "git_status": "TEXT NOT NULL DEFAULT 'nicht_verbunden'",
        "git_branch": "TEXT NOT NULL DEFAULT ''",
        "git_commit": "TEXT NOT NULL DEFAULT ''",
        "git_kurz": "TEXT NOT NULL DEFAULT ''",
        "git_author": "TEXT NOT NULL DEFAULT ''",
        "git_author_email": "TEXT NOT NULL DEFAULT ''",
        "git_nachricht": "TEXT NOT NULL DEFAULT ''",
        "git_geaendert_am": "TEXT NOT NULL DEFAULT ''",
        "git_dirty": "INTEGER NOT NULL DEFAULT 0",
        "git_ahead": "INTEGER NOT NULL DEFAULT 0",
        "git_behind": "INTEGER NOT NULL DEFAULT 0",
        "git_last_checked": "TEXT NOT NULL DEFAULT ''",
        "git_fehler": "TEXT NOT NULL DEFAULT ''",
    }
    vorhandene = {row[1] for row in db.execute("PRAGMA table_info(projekte)").fetchall()}
    for name, definition in projekt_spalten.items():
        if name not in vorhandene:
            db.execute(f"ALTER TABLE projekte ADD COLUMN {name} {definition}")
    for row in db.execute("SELECT id FROM projekte WHERE agent_token = '' OR agent_token IS NULL").fetchall():
        db.execute("UPDATE projekte SET agent_token = ? WHERE id = ?", (secrets.token_urlsafe(32), row[0]))
    for row in db.execute("SELECT id FROM kunden").fetchall():
        _ticket_sicherstellen(db, row[0])
    db.execute("UPDATE projekte SET preview_aktiv = 0")
    db.commit()


def projekt_git_synchronisieren(
    db: sqlite3.Connection,
    projekt: sqlite3.Row,
    *,
    aktivitaet_schreiben: bool = True,
) -> dict[str, object]:
    info = git_projektstand(projekt["lokaler_pfad"] or "")
    alter_commit = projekt["git_commit"] or ""
    neuer_commit = str(info["git_commit"] or "")
    remote = str(info["git_remote"] or "")
    db.execute(
        """
        UPDATE projekte SET git_status = ?, git_branch = ?, git_commit = ?, git_kurz = ?,
            git_author = ?, git_author_email = ?, git_nachricht = ?, git_geaendert_am = ?,
            git_dirty = ?, git_ahead = ?, git_behind = ?, git_last_checked = ?, git_fehler = ?,
            repo_url = CASE WHEN repo_url = '' AND ? != '' THEN ? ELSE repo_url END
        WHERE id = ?
        """,
        (
            info["git_status"],
            info["git_branch"],
            info["git_commit"],
            info["git_kurz"],
            info["git_author"],
            info["git_author_email"],
            info["git_nachricht"],
            info["git_geaendert_am"],
            info["git_dirty"],
            info["git_ahead"],
            info["git_behind"],
            jetzt(),
            info["git_fehler"],
            remote,
            remote,
            projekt["id"],
        ),
    )
    if aktivitaet_schreiben and alter_commit and neuer_commit and alter_commit != neuer_commit:
        db.execute(
            """
            INSERT INTO aktivitaeten (projekt_id, teammitglied_id, aktion, text, created_at)
            VALUES (?, NULL, 'Neuer Git-Stand erkannt', ?, ?)
            """,
            (projekt["id"], f"{info['git_kurz']} · {info['git_nachricht']} · {info['git_author']}", jetzt()),
        )
        db.execute("UPDATE projekte SET updated_at = ? WHERE id = ?", (jetzt(), projekt["id"]))
    return info


def git_monitor_starten(app: Flask) -> None:
    if app.extensions.get("tw_git_monitor"):
        return
    stop_event = threading.Event()

    def monitor() -> None:
        while not stop_event.is_set():
            try:
                with app.app_context():
                    db = get_db()
                    projekte = db.execute(
                        "SELECT * FROM projekte WHERE lokaler_pfad != '' AND status != 'abgeschlossen'"
                    ).fetchall()
                    for projekt in projekte:
                        projekt_git_synchronisieren(db, projekt)
                    db.commit()
            except Exception:
                app.logger.exception("Git-Monitor konnte Projekte nicht aktualisieren.")
            stop_event.wait(max(int(app.config["GIT_MONITOR_INTERVAL"]), 10))

    thread = threading.Thread(target=monitor, name="tw-git-monitor", daemon=True)
    thread.start()
    app.extensions["tw_git_monitor"] = {"thread": thread, "stop": stop_event}


def interne_vorschau_url(projekt: sqlite3.Row) -> str:
    if not projekt["preview_port"] or not vorschau_aktiv(projekt["id"]):
        return ""
    host = request.host.split(":", 1)[0]
    return f"http://{host}:{projekt['preview_port']}/"


def register_helpers(app: Flask) -> None:
    @app.before_request
    def benutzer_laden():
        g.user = None
        user_id = session.get("user_id")
        if user_id:
            g.user = get_db().execute(
                "SELECT * FROM teammitglieder WHERE id = ? AND aktiv = 1", (user_id,)
            ).fetchone()

    @app.before_request
    def csrf_pruefen():
        csrf_freie_endpunkte = {"agent_update_api", "migration_import"}
        if request.method == "POST" and request.endpoint not in csrf_freie_endpunkte:
            session_token = session.get("_csrf_token", "")
            form_token = request.form.get("_csrf_token", "")
            if not session_token or not form_token or not secrets.compare_digest(session_token, form_token):
                abort(400, "Die Formularsitzung ist abgelaufen. Bitte Seite neu laden.")

    @app.context_processor
    def template_werte():
        portal_ungelesen = 0
        if g.user:
            portal_ungelesen = get_db().execute(
                "SELECT COUNT(*) FROM portal_nachrichten WHERE absender = 'kunde' AND gelesen_team = 0"
            ).fetchone()[0]
        return {
            "csrf_token": csrf_token,
            "projekt_status": PROJEKT_STATUS,
            "projekt_status_labels": PROJEKT_STATUS_LABELS,
            "projekt_typ_labels": PROJEKT_TYP_LABELS,
            "git_status_labels": GIT_STATUS_LABELS,
            "angebot_status_labels": ANGEBOT_STATUS_LABELS,
            "ticket_phase_labels": TICKET_PHASE_LABELS,
            "portal_ungelesen": portal_ungelesen,
            "vertrag_rechtlich_freigegeben": bool(app.config["CONTRACT_LEGAL_APPROVED"]),
            "team_adressen": netzwerk_adressen(int(app.config["DASHBOARD_PORT"])),
            "heute": date.today().isoformat(),
        }

    @app.after_request
    def portal_schutzheader(response):
        if request.endpoint and request.endpoint.startswith("kundenportal"):
            response.headers["Cache-Control"] = "no-store, private"
            response.headers["Referrer-Policy"] = "no-referrer"
            response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.template_filter("datum")
    def datum_filter(value):
        if not value:
            return "–"
        try:
            return datetime.fromisoformat(str(value)).strftime("%d.%m.%Y")
        except ValueError:
            return value

    @app.template_filter("zeitpunkt")
    def zeitpunkt_filter(value):
        if not value:
            return "–"
        try:
            return datetime.fromisoformat(str(value)).strftime("%d.%m.%Y · %H:%M")
        except ValueError:
            return value

    @app.template_filter("euro")
    def euro_filter(value):
        try:
            betrag = Decimal(int(value or 0)) / Decimal(100)
        except (TypeError, ValueError, InvalidOperation):
            betrag = Decimal(0)
        return f"{betrag:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €"

    @app.errorhandler(400)
    @app.errorhandler(404)
    @app.errorhandler(413)
    def fehlerseite(error):
        texte = {
            400: "Die Anfrage konnte nicht verarbeitet werden.",
            404: "Diese Seite wurde nicht gefunden.",
            413: "Die Datei ist zu groß. Maximal sind 20 MB erlaubt.",
        }
        return render_template("fehler.html", code=error.code, meldung=texte.get(error.code, str(error))), error.code


def csrf_token() -> str:
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_urlsafe(32)
    return session["_csrf_token"]


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("anmelden", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if g.user["rolle"] != "admin":
            abort(403)
        return view(*args, **kwargs)

    return wrapped


def _saubere_url(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Bitte eine vollständige Adresse mit http:// oder https:// eingeben.")
    return value


def _gueltige_email(value: str) -> bool:
    value = (value or "").strip().lower()
    if value.startswith("noch-einzurichten@") or value.endswith("@invalid.local"):
        return False
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value))


def _whatsapp_nummer(value: str) -> str:
    raw = (value or "").strip()
    if not raw or any(wort in raw.lower() for wort in ("erfassen", "unbekannt", "fehlt")):
        return ""
    ziffern = re.sub(r"\D", "", raw)
    if raw.startswith("+"):
        nummer = ziffern
    elif ziffern.startswith("00"):
        nummer = ziffern[2:]
    elif ziffern.startswith("0"):
        nummer = "49" + ziffern[1:]
    else:
        nummer = ziffern
    return nummer if 8 <= len(nummer) <= 15 else ""


def _whatsapp_link(telefon: str, text: str) -> str:
    nummer = _whatsapp_nummer(telefon)
    if not nummer:
        return ""
    return f"https://wa.me/{nummer}?text={quote(text)}"


def _fehlende_kundendaten(kunde: sqlite3.Row) -> list[str]:
    fehlt: list[str] = []
    if not (kunde["adresse"] or "").strip():
        fehlt.append("Geschäftsanschrift")
    if not _gueltige_email(kunde["email"]):
        fehlt.append("aktuelle E-Mail-Adresse")
    if not _whatsapp_nummer(kunde["telefon"]):
        fehlt.append("Handynummer")
    if not (kunde["website"] or "").strip() and not kunde["keine_website"]:
        fehlt.append("bestehende Website oder Bestätigung „noch keine Website“")
    return fehlt


def _int_form(name: str, default: int = 0, minimum: int = 0, maximum: int = 100) -> int:
    try:
        value = int(request.form.get(name, default))
    except (TypeError, ValueError):
        value = default
    return min(max(value, minimum), maximum)


def _zeilen(value: str, *, maximum: int, laenge: int) -> list[str]:
    return [zeile.strip()[:laenge] for zeile in (value or "").splitlines() if zeile.strip()][:maximum]


def _verkaufsvideo_standardtexte(projekt: sqlite3.Row) -> dict[str, str]:
    kundenname = projekt["kundenname"]
    return {
        "headline": f"{kundenname} digital erleben.",
        "subtitle": "Ein kompakter Rundgang durch den aktuellen Entwurf – mit den nächsten Chancen für mehr Sichtbarkeit und Anfragen.",
        "kapitel": "\n".join(
            (
                "Startseite & Positionierung",
                "Angebote & Leistungen",
                "Termine & Buchung",
                "Referenzen & Galerie",
                "Persönlichkeit & Vertrauen",
                "Kontakt & Anfrage",
                "Visitenkarte & QR-Code",
                "Marke & Wiedererkennung",
            )
        ),
        "potenziale": "\n".join(
            (
                "Online-Termin- oder Kursbuchung direkt aus der Website",
                "Google-Unternehmensprofil und lokale Auffindbarkeit sauber ausbauen",
                "Google Ads erst nach Freigabe gezielt auf passende Suchanfragen ausrichten",
                "WhatsApp- und Kontaktanfragen einfacher und messbar machen",
            )
        ),
        "servicepunkte": "\n".join(
            (
                "Betreuung im Abo: Inhalte, Termine und Technik aktuell halten",
                "Google-Auswertung und Ads erst nach gemeinsamer Freigabe weiterentwickeln",
                "Transparentes Kundenportal mit Aufgaben, nächsten Schritten und Ergebnissen",
            )
        ),
        "sprechertext": (
            f"Stellen Sie sich vor: Menschen entdecken {kundenname} und verstehen sofort, wofür das Angebot steht. "
            "Genau dafür haben wir diesen digitalen Auftritt entwickelt. Die Startseite verbindet Persönlichkeit "
            "mit einer klaren, hochwertigen Bildwelt. Leistungen, Termine und Preise werden übersichtlich präsentiert "
            "und führen ohne Umwege zur Anfrage oder Buchung. Referenzen, persönliche Einblicke und klare Kontaktwege "
            "schaffen Vertrauen. Marke, Printmaterial und QR-Code tragen den neuen Auftritt konsequent nach außen. "
            "Nach dem Start beginnt die Zusammenarbeit erst richtig: Im Betreuungspaket halten wir Inhalte, Termine "
            "und Technik aktuell. Wir analysieren, wie Menschen das Angebot finden, entwickeln die Google-Sichtbarkeit "
            "weiter und können – selbstverständlich erst nach Freigabe – gezielte Google-Ads-Kampagnen umsetzen. "
            "Im persönlichen Kundenportal bleibt transparent, was erledigt wurde, welche Schritte anstehen und welche "
            "Ergebnisse die Maßnahmen zeigen. So entsteht nicht nur eine schöne Website, sondern ein betreuter digitaler "
            "Auftritt, der mit dem Unternehmen wächst. Wenn dieser Weg passt, finalisieren wir ihn gemeinsam und bringen "
            "das Angebot sichtbar nach vorn."
        ),
        "cta": "Wie gefällt Ihnen der aktuelle Stand? Lassen Sie uns die nächsten Schritte gemeinsam festlegen.",
    }


def _initialen(name: str) -> str:
    teile = [teil for teil in name.split() if teil]
    if not teile:
        return "TW"
    return "".join(teil[0].upper() for teil in teile[:2])


def _projekt_oder_404(projekt_id: int):
    projekt = get_db().execute(
        """
        SELECT p.*, k.firma AS kundenname, k.ansprechpartner, k.email AS kunden_email,
               k.telefon AS kunden_telefon
        FROM projekte p
        JOIN kunden k ON k.id = p.kunde_id
        WHERE p.id = ?
        """,
        (projekt_id,),
    ).fetchone()
    if projekt is None:
        abort(404)
    return projekt


def _kunde_oder_404(kunde_id: int):
    kunde = get_db().execute("SELECT * FROM kunden WHERE id = ?", (kunde_id,)).fetchone()
    if kunde is None:
        abort(404)
    return kunde


def _ticket_oder_404(kunde_id: int):
    ticket = get_db().execute(
        """
        SELECT kt.*, k.firma, k.ansprechpartner, k.email, k.telefon, k.adresse,
               k.website, k.keine_website, k.whatsapp_freigabe
        FROM kunden_tickets kt JOIN kunden k ON k.id = kt.kunde_id
        WHERE kt.kunde_id = ?
        """,
        (kunde_id,),
    ).fetchone()
    if ticket is None:
        abort(404)
    return ticket


def _ticket_token_oder_404(token: str):
    ticket = get_db().execute(
        """
        SELECT kt.*, k.firma, k.ansprechpartner, k.email, k.telefon, k.adresse,
               k.website, k.keine_website, k.whatsapp_freigabe
        FROM kunden_tickets kt JOIN kunden k ON k.id = kt.kunde_id
        WHERE kt.token = ? AND kt.status = 'aktiv'
        """,
        (token,),
    ).fetchone()
    if ticket is None:
        abort(404)
    return ticket


def _angebot_sicherstellen(db: sqlite3.Connection, ticket_id: int, user_id: int | None = None):
    angebot = db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket_id,)).fetchone()
    if angebot is not None:
        return angebot
    timestamp = jetzt()
    db.execute(
        """
        INSERT INTO ticket_angebote
          (ticket_id, paket_name, einmalig_cent, monatlich_cent, einrichtung_cent,
           laufzeit_monate, kuendigungsfrist_monate, leistungsumfang, hinweise,
           erstellt_von, created_at, updated_at)
        VALUES (?, ?, 250000, 30000, 0, 12, 3, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id,
            "Website & digitales Wachstumssystem",
            "Individuelle Website inklusive Konzeption, Gestaltung, technischer Umsetzung und gemeinsamer Projektabstimmung. Optionale KI-Funktionen, Dashboard, Kalender- und Social-Media-Anbindungen werden im finalen Umfang ausdrücklich aufgeführt.",
            "Externe API- und Nutzungsgebühren werden nur berechnet, wenn sie im Angebot ausdrücklich genannt sind. Vertrags- und Datenschutztexte müssen vor rechtsverbindlicher Nutzung freigegeben sein.",
            user_id,
            timestamp,
            timestamp,
        ),
    )
    return db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket_id,)).fetchone()


def _portal_url(ticket: sqlite3.Row) -> str:
    basis = current_app.config["PUBLIC_BASE_URL"] or request.url_root.rstrip("/")
    return f"{basis}/portal/{ticket['token']}"


def _euro_zu_cent(value: str, default: int = 0) -> int:
    raw = (value or "").strip().replace("€", "").replace(" ", "")
    if not raw:
        return default
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    try:
        betrag = Decimal(raw).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError("Bitte gültige Euro-Beträge eingeben.") from exc
    if betrag < 0 or betrag > Decimal("1000000"):
        raise ValueError("Der Betrag liegt außerhalb des erlaubten Bereichs.")
    return int(betrag * 100)


def _portal_nachricht_anlegen(
    db: sqlite3.Connection,
    ticket_id: int,
    *,
    absender: str,
    art: str,
    text: str,
    projekt_id: int | None = None,
    teammitglied_id: int | None = None,
) -> int:
    timestamp = jetzt()
    cur = db.execute(
        """
        INSERT INTO portal_nachrichten
          (ticket_id, projekt_id, teammitglied_id, absender, art, text, gelesen_team, gelesen_kunde, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_id,
            projekt_id,
            teammitglied_id,
            absender,
            art,
            text.strip()[:5000],
            int(absender != "kunde"),
            int(absender == "kunde"),
            timestamp,
        ),
    )
    db.execute(
        "UPDATE kunden_tickets SET updated_at = ?, last_activity_at = ? WHERE id = ?",
        (timestamp, timestamp, ticket_id),
    )
    return cur.lastrowid


def _portal_dateien_speichern(
    db: sqlite3.Connection,
    ticket_id: int,
    nachricht_id: int,
    dateien,
) -> tuple[int, list[str]]:
    gespeichert = 0
    fehler: list[str] = []
    for datei in list(dateien)[:8]:
        if not datei or not datei.filename:
            continue
        original = secure_filename(datei.filename)
        endung = original.rsplit(".", 1)[-1].lower() if "." in original else ""
        if not original or endung not in PORTAL_DATEIEN:
            fehler.append(datei.filename)
            continue
        gespeichert_name = f"portal-{uuid4().hex}.{endung}"
        ziel = Path(current_app.config["UPLOAD_FOLDER"]) / gespeichert_name
        datei.save(ziel)
        db.execute(
            """
            INSERT INTO portal_dateien
              (ticket_id, nachricht_id, original_name, gespeichert_name, mimetype, groesse, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ticket_id, nachricht_id, original, gespeichert_name, datei.mimetype or "application/octet-stream", ziel.stat().st_size, jetzt()),
        )
        gespeichert += 1
    return gespeichert, fehler


def _portal_verlauf(db: sqlite3.Connection, ticket_id: int):
    nachrichten = db.execute(
        """
        SELECT n.*, t.name AS teamname, t.farbe AS teamfarbe, p.titel AS projekttitel
        FROM portal_nachrichten n
        LEFT JOIN teammitglieder t ON t.id = n.teammitglied_id
        LEFT JOIN projekte p ON p.id = n.projekt_id
        WHERE n.ticket_id = ? ORDER BY n.created_at DESC, n.id DESC
        """,
        (ticket_id,),
    ).fetchall()
    dateien = db.execute(
        "SELECT * FROM portal_dateien WHERE ticket_id = ? ORDER BY id",
        (ticket_id,),
    ).fetchall()
    nachrichten_dateien: dict[int, list[sqlite3.Row]] = {}
    for datei in dateien:
        nachrichten_dateien.setdefault(datei["nachricht_id"], []).append(datei)
    return nachrichten, nachrichten_dateien


def _team_empfaenger(db: sqlite3.Connection) -> str:
    konfiguriert = current_app.config["TEAM_NOTIFY_EMAIL"].replace(";", ",").strip(" ,")
    if konfiguriert:
        return konfiguriert
    return ", ".join(
        row["email"]
        for row in db.execute("SELECT email FROM teammitglieder WHERE aktiv = 1 AND rolle = 'admin' ORDER BY id").fetchall()
        if row["email"]
    )


def _portal_mail(
    db: sqlite3.Connection,
    ticket: sqlite3.Row,
    *,
    nachricht_id: int | None,
    richtung: str,
    empfaenger: str,
    betreff: str,
    text: str,
) -> str:
    timestamp = jetzt()
    cur = db.execute(
        """
        INSERT INTO portal_benachrichtigungen
          (ticket_id, nachricht_id, richtung, empfaenger, betreff, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'vorbereitet', ?)
        """,
        (ticket["id"], nachricht_id, richtung, empfaenger, betreff, timestamp),
    )
    db.commit()
    if not _gueltige_email(empfaenger):
        status, fehler = "ungueltige_adresse", "Es ist keine aktuelle, gültige E-Mail-Adresse hinterlegt."
    elif richtung == "an_kunde" and not current_app.config["PUBLIC_BASE_URL"]:
        status, fehler = "keine_domain", "Öffentliche Portal-Adresse ist noch nicht eingerichtet."
    else:
        status, fehler = mail_senden(current_app.config, empfaenger, betreff, text)
    db.execute(
        "UPDATE portal_benachrichtigungen SET status = ?, fehler = ?, sent_at = ? WHERE id = ?",
        (status, fehler, jetzt() if status == "gesendet" else None, cur.lastrowid),
    )
    db.commit()
    return status


def _aktivitaet(projekt_id: int, aktion: str, text: str = "", user_id: int | None = None) -> None:
    db = get_db()
    db.execute(
        "INSERT INTO aktivitaeten (projekt_id, teammitglied_id, aktion, text, created_at) VALUES (?, ?, ?, ?, ?)",
        (projekt_id, user_id or (g.user["id"] if g.user else None), aktion, text.strip(), jetzt()),
    )


def _zuweisungen_speichern(db: sqlite3.Connection, projekt_id: int) -> None:
    ids = []
    for value in request.form.getlist("teammitglieder"):
        try:
            ids.append(int(value))
        except ValueError:
            continue
    gueltige_ids = {
        row["id"] for row in db.execute("SELECT id FROM teammitglieder WHERE aktiv = 1").fetchall()
    }
    ids = [value for value in dict.fromkeys(ids) if value in gueltige_ids]
    haupt_id = request.form.get("hauptverantwortlich", type=int)
    if haupt_id not in ids:
        haupt_id = ids[0] if ids else None
    db.execute("DELETE FROM projekt_team WHERE projekt_id = ?", (projekt_id,))
    for team_id in ids:
        db.execute(
            "INSERT INTO projekt_team (projekt_id, teammitglied_id, ist_hauptverantwortlich) VALUES (?, ?, ?)",
            (projekt_id, team_id, int(team_id == haupt_id)),
        )


def register_routes(app: Flask) -> None:
    def verkaufsvideo_lock_anfordern() -> bool:
        if not VIDEO_GENERATION_LOCK.acquire(blocking=False):
            return False
        g.verkaufsvideo_lock_aktiv = True
        return True

    @app.teardown_request
    def verkaufsvideo_lock_freigeben(_error):
        if getattr(g, "verkaufsvideo_lock_aktiv", False):
            g.verkaufsvideo_lock_aktiv = False
            VIDEO_GENERATION_LOCK.release()

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "app": "tomorrowworks-dashboard"}

    @app.post("/migration/import")
    def migration_import():
        expected_token = current_app.config["MIGRATION_TOKEN"]
        if not expected_token:
            abort(404)
        authorization = request.headers.get("Authorization", "")
        supplied_token = authorization[7:].strip() if authorization.startswith("Bearer ") else ""
        if not supplied_token or not secrets.compare_digest(supplied_token, expected_token):
            return jsonify({"ok": False, "error": "Nicht autorisiert."}), 403
        uploaded = request.files.get("database")
        if uploaded is None or not uploaded.filename:
            return jsonify({"ok": False, "error": "Datenbankdatei fehlt."}), 400

        target = Path(current_app.config["DATABASE"])
        staging = target.with_name(f".{target.name}.{uuid4().hex}.import")
        backup_dir = target.parent / "backups"
        required_tables = {"kunden", "projekte", "teammitglieder", "kunden_tickets", "portal_nachrichten"}
        try:
            uploaded.save(staging)
            source = sqlite3.connect(staging)
            try:
                integrity = source.execute("PRAGMA integrity_check").fetchone()[0]
                tables = {
                    row[0]
                    for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                counts = {
                    table: source.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in required_tables
                    if table in tables
                }
            finally:
                source.close()
            if integrity != "ok" or not required_tables.issubset(tables) or counts.get("teammitglieder", 0) < 1:
                return jsonify({"ok": False, "error": "Datenbankprüfung fehlgeschlagen."}), 400

            close_db()
            backup_dir.mkdir(parents=True, exist_ok=True)
            if target.exists():
                backup_path = backup_dir / f"vor-import-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
                old_db = sqlite3.connect(target)
                backup_db = sqlite3.connect(backup_path)
                try:
                    old_db.backup(backup_db)
                finally:
                    backup_db.close()
                    old_db.close()
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{target}{suffix}")
                if sidecar.exists():
                    sidecar.unlink()
            os.replace(staging, target)
            init_db()
            get_db().commit()
        finally:
            if staging.exists():
                staging.unlink()

        return jsonify({"ok": True, "counts": counts})

    @app.route("/einrichtung", methods=["GET", "POST"])
    def einrichtung():
        db = get_db()
        if db.execute("SELECT COUNT(*) FROM teammitglieder").fetchone()[0] > 0:
            return redirect(url_for("anmelden"))
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            passwort = request.form.get("passwort", "")
            if len(name) < 2 or "@" not in email or len(passwort) < 8:
                flash("Bitte Name, gültige E-Mail und mindestens 8 Zeichen als Passwort eingeben.", "error")
            else:
                cur = db.execute(
                    """
                    INSERT INTO teammitglieder (name, email, rolle, password_hash, farbe, created_at)
                    VALUES (?, ?, 'admin', ?, ?, ?)
                    """,
                    (name, email, generate_password_hash(passwort), TEAM_FARBEN[0], jetzt()),
                )
                db.commit()
                session.clear()
                session["user_id"] = cur.lastrowid
                flash("Das Dashboard ist eingerichtet. Jetzt kannst du Kunden, Projekte und Team anlegen.", "success")
                return redirect(url_for("dashboard"))
        return render_template("setup.html")

    @app.route("/anmelden", methods=["GET", "POST"])
    def anmelden():
        db = get_db()
        if db.execute("SELECT COUNT(*) FROM teammitglieder").fetchone()[0] == 0:
            return redirect(url_for("einrichtung"))
        if g.user:
            return redirect(url_for("dashboard"))
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            passwort = request.form.get("passwort", "")
            user = db.execute(
                "SELECT * FROM teammitglieder WHERE lower(email) = ? AND aktiv = 1", (email,)
            ).fetchone()
            if user and check_password_hash(user["password_hash"], passwort):
                session.clear()
                session["user_id"] = user["id"]
                next_url = request.args.get("next", "")
                if not next_url.startswith("/") or next_url.startswith("//"):
                    next_url = url_for("dashboard")
                return redirect(next_url)
            flash("E-Mail oder Passwort stimmt nicht.", "error")
        return render_template("login.html")

    @app.post("/abmelden")
    @login_required
    def abmelden():
        session.clear()
        return redirect(url_for("anmelden"))

    @app.get("/")
    @login_required
    def dashboard():
        db = get_db()
        status_filter = request.args.get("status", "").strip()
        team_filter = request.args.get("team", type=int)
        suche = request.args.get("q", "").strip()
        where = ["1 = 1"]
        params: list[object] = []
        if status_filter in PROJEKT_STATUS_LABELS:
            where.append("p.status = ?")
            params.append(status_filter)
        if team_filter:
            where.append("EXISTS (SELECT 1 FROM projekt_team ptf WHERE ptf.projekt_id = p.id AND ptf.teammitglied_id = ?)")
            params.append(team_filter)
        if suche:
            where.append("(lower(p.titel) LIKE ? OR lower(k.firma) LIKE ? OR lower(p.aktuelle_aufgabe) LIKE ?)")
            pattern = f"%{suche.lower()}%"
            params.extend([pattern, pattern, pattern])

        projekte = db.execute(
            f"""
            SELECT p.*, k.firma AS kundenname,
                   GROUP_CONCAT(t.name, '|||') AS teamnamen,
                   GROUP_CONCAT(t.farbe, '|||') AS teamfarben
            FROM projekte p
            JOIN kunden k ON k.id = p.kunde_id
            LEFT JOIN projekt_team pt ON pt.projekt_id = p.id
            LEFT JOIN teammitglieder t ON t.id = pt.teammitglied_id AND t.aktiv = 1
            WHERE {' AND '.join(where)}
            GROUP BY p.id
            ORDER BY CASE p.prioritaet WHEN 'dringend' THEN 0 WHEN 'hoch' THEN 1 ELSE 2 END,
                     CASE WHEN p.zieldatum IS NULL OR p.zieldatum = '' THEN 1 ELSE 0 END,
                     p.zieldatum, p.updated_at DESC
            """,
            params,
        ).fetchall()

        stats = db.execute(
            """
            SELECT
              SUM(CASE WHEN status NOT IN ('abgeschlossen', 'veroeffentlicht', 'pausiert') THEN 1 ELSE 0 END) AS aktiv,
              SUM(CASE WHEN status = 'blockiert' THEN 1 ELSE 0 END) AS blockiert,
              SUM(CASE WHEN status IN ('kundenfreigabe', 'wartet_auf_kunde') THEN 1 ELSE 0 END) AS wartet,
              SUM(CASE WHEN zieldatum != '' AND zieldatum IS NOT NULL AND zieldatum < ?
                         AND status NOT IN ('abgeschlossen', 'veroeffentlicht') THEN 1 ELSE 0 END) AS ueberfaellig
            FROM projekte
            """,
            (date.today().isoformat(),),
        ).fetchone()

        team = db.execute(
            """
            SELECT t.*, p.titel AS fokus_titel, p.status AS fokus_status, k.firma AS fokus_kunde,
                   (SELECT MAX(a.created_at) FROM aktivitaeten a WHERE a.teammitglied_id = t.id) AS letzte_aktivitaet
            FROM teammitglieder t
            LEFT JOIN projekte p ON p.id = t.fokus_projekt_id
            LEFT JOIN kunden k ON k.id = p.kunde_id
            WHERE t.aktiv = 1
            ORDER BY t.name
            """
        ).fetchall()
        fokus_projekte = db.execute(
            """
            SELECT p.id, p.titel, k.firma FROM projekte p JOIN kunden k ON k.id = p.kunde_id
            WHERE p.status NOT IN ('abgeschlossen', 'veroeffentlicht') ORDER BY k.firma, p.titel
            """
        ).fetchall()
        kunden_updates = db.execute(
            """
            SELECT kt.kunde_id, k.firma, COUNT(n.id) AS ungelesen, MAX(n.created_at) AS letzte_nachricht
            FROM kunden_tickets kt
            JOIN kunden k ON k.id = kt.kunde_id
            JOIN portal_nachrichten n ON n.ticket_id = kt.id AND n.absender = 'kunde' AND n.gelesen_team = 0
            GROUP BY kt.id ORDER BY letzte_nachricht DESC LIMIT 5
            """
        ).fetchall()
        return render_template(
            "dashboard.html",
            projekte=projekte,
            stats=stats,
            team=team,
            fokus_projekte=fokus_projekte,
            status_filter=status_filter,
            team_filter=team_filter,
            suche=suche,
            kunden_updates=kunden_updates,
        )

    @app.post("/mein-fokus")
    @login_required
    def mein_fokus():
        db = get_db()
        projekt_id = request.form.get("projekt_id", type=int)
        notiz = request.form.get("fokus_notiz", "").strip()[:180]
        if projekt_id and not db.execute("SELECT 1 FROM projekte WHERE id = ?", (projekt_id,)).fetchone():
            abort(400)
        db.execute(
            "UPDATE teammitglieder SET fokus_projekt_id = ?, fokus_notiz = ?, fokus_aktualisiert_am = ? WHERE id = ?",
            (projekt_id, notiz, jetzt(), g.user["id"]),
        )
        db.commit()
        flash("Dein heutiger Fokus ist aktualisiert.", "success")
        return redirect(url_for("dashboard"))

    @app.get("/kunden")
    @login_required
    def kunden_liste():
        suche = request.args.get("q", "").strip().lower()
        params: list[object] = []
        where = ""
        if suche:
            where = "WHERE lower(k.firma) LIKE ? OR lower(k.ansprechpartner) LIKE ?"
            params = [f"%{suche}%", f"%{suche}%"]
        kunden = get_db().execute(
            f"""
            SELECT k.*,
                   COUNT(p.id) AS projektanzahl,
                   SUM(CASE WHEN p.status NOT IN ('abgeschlossen', 'veroeffentlicht') THEN 1 ELSE 0 END) AS aktive_projekte,
                   kt.id AS ticket_id, kt.last_activity_at,
                   (SELECT COUNT(*) FROM portal_nachrichten n
                    WHERE n.ticket_id = kt.id AND n.absender = 'kunde' AND n.gelesen_team = 0) AS ungelesen
            FROM kunden k LEFT JOIN projekte p ON p.kunde_id = k.id
            LEFT JOIN kunden_tickets kt ON kt.kunde_id = k.id
            {where}
            GROUP BY k.id ORDER BY k.updated_at DESC, k.firma
            """,
            params,
        ).fetchall()
        return render_template("kunden.html", kunden=kunden, suche=suche)

    @app.route("/kunden/neu", methods=["GET", "POST"])
    @login_required
    def kunde_neu():
        if request.method == "POST":
            firma = request.form.get("firma", "").strip()
            email = request.form.get("email", "").strip().lower()
            telefon = request.form.get("telefon", "").strip()
            adresse = request.form.get("adresse", "").strip()
            kundenstatus = request.form.get("status", "aktiv")
            einladungsmodus = request.form.get("einladungsmodus", "")
            if einladungsmodus not in {"spaeter", "jetzt"}:
                # Alte Formularaufrufe ohne das neue Feld behalten für aktive Kunden
                # den bisherigen Sofortversand. Interessenten werden sicher nur intern angelegt.
                einladungsmodus = "jetzt" if kundenstatus == "aktiv" else "spaeter"
            keine_website = request.form.get("keine_website") == "1"
            whatsapp_freigabe = request.form.get("whatsapp_freigabe") == "1"
            formular_gueltig = True
            try:
                website = _saubere_url(request.form.get("website", ""))
            except ValueError:
                website = ""
                formular_gueltig = False
                flash("Bitte bei ‚Bestehende Website‘ eine vollständige Webadresse wie https://beispiel.de eingeben – keine E-Mail-Adresse.", "error")
            if website:
                keine_website = False
            if not firma or not _gueltige_email(email) or not _whatsapp_nummer(telefon) or len(adresse) < 5:
                formular_gueltig = False
                flash("Bitte Firma, Geschäftsanschrift, aktuelle E-Mail-Adresse und Handynummer vollständig eingeben.", "error")
            if not website and not keine_website:
                formular_gueltig = False
                flash("Bitte die bestehende Website eintragen oder bestätigen, dass derzeit noch keine Website vorhanden ist.", "error")
            if formular_gueltig:
                timestamp = jetzt()
                db = get_db()
                vorhandener_kunde = db.execute(
                    "SELECT id FROM kunden WHERE lower(trim(firma)) = lower(trim(?)) LIMIT 1",
                    (firma,),
                ).fetchone()
                if vorhandener_kunde:
                    flash("Dieser Kunde ist bereits angelegt. Bitte den bestehenden Kundeneintrag bearbeiten.", "error")
                    return redirect(url_for("kunde_bearbeiten", kunde_id=vorhandener_kunde["id"]))
                cur = db.execute(
                    """
                    INSERT INTO kunden
                      (firma, ansprechpartner, email, telefon, adresse, website, keine_website,
                       whatsapp_freigabe, notizen, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        firma,
                        request.form.get("ansprechpartner", "").strip(),
                        email,
                        telefon,
                        adresse,
                        website,
                        int(keine_website),
                        int(whatsapp_freigabe),
                        request.form.get("notizen", "").strip(),
                        kundenstatus,
                        timestamp,
                        timestamp,
                    ),
                )
                ticket = _ticket_sicherstellen(db, cur.lastrowid)
                db.commit()
                if einladungsmodus == "spaeter":
                    flash(
                        "Kunde und dauerhaftes Ticket wurden nur intern angelegt. Es wurde keine E-Mail gesendet; die Einladung kann später im Kunden-Ticket verschickt werden.",
                        "success",
                    )
                    return redirect(url_for("kunde_detail", kunde_id=cur.lastrowid))
                portal_link = _portal_url(ticket)
                mail_status = _portal_mail(
                    db,
                    ticket,
                    nachricht_id=None,
                    richtung="an_kunde",
                    empfaenger=email,
                    betreff=f"Ihr gemeinsamer Projektraum · {firma}",
                    text=(
                        f"Guten Tag {request.form.get('ansprechpartner', '').strip() or firma},\n\n"
                        "Tomorrow Works hat Ihren dauerhaften Projektraum eröffnet. Dort sehen Sie Neuerungen, "
                        "beantworten Rückfragen und laden Logos, Bilder oder Dokumente hoch.\n\n"
                        f"Ihr persönlicher Link:\n{portal_link}\n\n"
                        "Bitte behandeln Sie diesen Link vertraulich.\n\nTomorrow Works"
                    ),
                )
                if mail_status == "gesendet":
                    flash("Kunde und dauerhaftes Ticket wurden angelegt. Die Einladung wurde per E-Mail gesendet.", "success")
                else:
                    flash("Kunde und dauerhaftes Ticket wurden angelegt. Die Einladung kann nach Einrichtung von Domain und Mailversand gesendet werden.", "info")
                return redirect(url_for("kunde_detail", kunde_id=cur.lastrowid))
        return render_template("kunde_form.html", kunde=None)

    @app.get("/kunden/<int:kunde_id>")
    @login_required
    def kunde_detail(kunde_id: int):
        kunde = _kunde_oder_404(kunde_id)
        db = get_db()
        ticket = _ticket_sicherstellen(db, kunde_id)
        db.commit()
        projekte = db.execute(
            """
            SELECT p.*, GROUP_CONCAT(t.name, '|||') AS teamnamen, GROUP_CONCAT(t.farbe, '|||') AS teamfarben
            FROM projekte p
            LEFT JOIN projekt_team pt ON pt.projekt_id = p.id
            LEFT JOIN teammitglieder t ON t.id = pt.teammitglied_id
            WHERE p.kunde_id = ? GROUP BY p.id
            ORDER BY p.updated_at DESC
            """,
            (kunde_id,),
        ).fetchall()
        ungelesen = db.execute(
            "SELECT COUNT(*) FROM portal_nachrichten WHERE ticket_id = ? AND absender = 'kunde' AND gelesen_team = 0",
            (ticket["id"],),
        ).fetchone()[0]
        portal_link = _portal_url(ticket)
        fehlende_kundendaten = _fehlende_kundendaten(kunde)
        whatsapp_text = (
            f"Hallo {kunde['ansprechpartner'] or kunde['firma']},\n\n"
            "hier ist Ihr persönlicher Tomorrow-Works-Projektraum:\n"
            f"{portal_link}\n\n"
            "Dort können Sie Nachrichten, Bilder und Unterlagen sicher mit uns austauschen."
        )
        whatsapp_link = ""
        if not fehlende_kundendaten and kunde["whatsapp_freigabe"] and current_app.config["PUBLIC_BASE_URL"]:
            whatsapp_link = _whatsapp_link(kunde["telefon"], whatsapp_text)
        return render_template(
            "kunde_detail.html",
            kunde=kunde,
            projekte=projekte,
            ticket=ticket,
            ticket_ungelesen=ungelesen,
            fehlende_kundendaten=fehlende_kundendaten,
            whatsapp_link=whatsapp_link,
            portal_oeffentlich=bool(current_app.config["PUBLIC_BASE_URL"]),
        )

    @app.route("/kunden/<int:kunde_id>/bearbeiten", methods=["GET", "POST"])
    @login_required
    def kunde_bearbeiten(kunde_id: int):
        kunde = _kunde_oder_404(kunde_id)
        if request.method == "POST":
            firma = request.form.get("firma", "").strip()
            email = request.form.get("email", "").strip().lower()
            telefon = request.form.get("telefon", "").strip()
            adresse = request.form.get("adresse", "").strip()
            keine_website = request.form.get("keine_website") == "1"
            whatsapp_freigabe = request.form.get("whatsapp_freigabe") == "1"
            formular_gueltig = True
            try:
                website = _saubere_url(request.form.get("website", ""))
            except ValueError:
                website = ""
                formular_gueltig = False
                flash("Bitte bei ‚Bestehende Website‘ eine vollständige Webadresse wie https://beispiel.de eingeben – keine E-Mail-Adresse.", "error")
            if website:
                keine_website = False
            if not firma or not _gueltige_email(email) or not _whatsapp_nummer(telefon) or len(adresse) < 5:
                formular_gueltig = False
                flash("Bitte Firma, Geschäftsanschrift, aktuelle E-Mail-Adresse und Handynummer vollständig eingeben.", "error")
            if not website and not keine_website:
                formular_gueltig = False
                flash("Bitte die bestehende Website eintragen oder bestätigen, dass derzeit noch keine Website vorhanden ist.", "error")
            if formular_gueltig:
                get_db().execute(
                    """
                    UPDATE kunden SET firma = ?, ansprechpartner = ?, email = ?, telefon = ?, adresse = ?,
                        website = ?, keine_website = ?, whatsapp_freigabe = ?, notizen = ?, status = ?,
                        updated_at = ? WHERE id = ?
                    """,
                    (
                        firma,
                        request.form.get("ansprechpartner", "").strip(),
                        email,
                        telefon,
                        adresse,
                        website,
                        int(keine_website),
                        int(whatsapp_freigabe),
                        request.form.get("notizen", "").strip(),
                        request.form.get("status", "aktiv"),
                        jetzt(),
                        kunde_id,
                    ),
                )
                get_db().commit()
                flash("Kundendaten wurden gespeichert.", "success")
                return redirect(url_for("kunde_detail", kunde_id=kunde_id))
        return render_template("kunde_form.html", kunde=kunde)

    @app.get("/tickets")
    @login_required
    def tickets_liste():
        tickets = get_db().execute(
            """
            SELECT kt.*, k.firma, k.ansprechpartner, k.email,
                   (SELECT COUNT(*) FROM portal_nachrichten n
                    WHERE n.ticket_id = kt.id AND n.absender = 'kunde' AND n.gelesen_team = 0) AS ungelesen,
                   (SELECT text FROM portal_nachrichten n
                    WHERE n.ticket_id = kt.id ORDER BY n.created_at DESC, n.id DESC LIMIT 1) AS letzte_nachricht,
                   a.status AS angebot_status
            FROM kunden_tickets kt
            JOIN kunden k ON k.id = kt.kunde_id
            LEFT JOIN ticket_angebote a ON a.ticket_id = kt.id
            ORDER BY ungelesen DESC, kt.last_activity_at DESC
            """
        ).fetchall()
        return render_template("tickets.html", tickets=tickets)

    @app.get("/kunden/<int:kunde_id>/ticket")
    @login_required
    def kunden_ticket(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        db.execute(
            "UPDATE portal_nachrichten SET gelesen_team = 1 WHERE ticket_id = ? AND absender = 'kunde'",
            (ticket["id"],),
        )
        db.commit()
        nachrichten, nachrichten_dateien = _portal_verlauf(db, ticket["id"])
        projekte = db.execute(
            "SELECT id, titel, status, fortschritt FROM projekte WHERE kunde_id = ? ORDER BY updated_at DESC",
            (kunde_id,),
        ).fetchall()
        angebot = db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket["id"],)).fetchone()
        benachrichtigungen = db.execute(
            "SELECT * FROM portal_benachrichtigungen WHERE ticket_id = ? ORDER BY id DESC LIMIT 8",
            (ticket["id"],),
        ).fetchall()
        portal_link = _portal_url(ticket)
        portal_oeffentlich = bool(current_app.config["PUBLIC_BASE_URL"])
        fehlende_kundendaten = _fehlende_kundendaten(ticket)
        whatsapp_text = (
            f"Hallo {ticket['ansprechpartner'] or ticket['firma']},\n\n"
            "hier ist Ihr persönlicher Tomorrow-Works-Projektraum:\n"
            f"{portal_link}\n\n"
            "Dort können Sie Nachrichten, Bilder und Unterlagen sicher mit uns austauschen."
        )
        whatsapp_link = ""
        if not fehlende_kundendaten and ticket["whatsapp_freigabe"] and portal_oeffentlich:
            whatsapp_link = _whatsapp_link(ticket["telefon"], whatsapp_text)
        return render_template(
            "kunden_ticket.html",
            ticket=ticket,
            portal_link=portal_link,
            portal_oeffentlich=portal_oeffentlich,
            fehlende_kundendaten=fehlende_kundendaten,
            email_bereit=_gueltige_email(ticket["email"]) and not fehlende_kundendaten and portal_oeffentlich,
            whatsapp_link=whatsapp_link,
            nachrichten=nachrichten,
            nachrichten_dateien=nachrichten_dateien,
            projekte=projekte,
            angebot=angebot,
            benachrichtigungen=benachrichtigungen,
        )

    @app.post("/kunden/<int:kunde_id>/ticket/nachricht")
    @login_required
    def kunden_ticket_nachricht(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        text = request.form.get("text", "").strip()
        dateien = [datei for datei in request.files.getlist("dateien") if datei and datei.filename]
        if not text and not dateien:
            flash("Bitte eine Nachricht oder mindestens eine Datei hinzufügen.", "error")
            return redirect(url_for("kunden_ticket", kunde_id=kunde_id) + "#schreiben")
        art = request.form.get("art", "nachricht")
        if art not in {"nachricht", "update", "anforderung", "meilenstein"}:
            art = "nachricht"
        projekt_id = request.form.get("projekt_id", type=int)
        if projekt_id and not db.execute(
            "SELECT 1 FROM projekte WHERE id = ? AND kunde_id = ?", (projekt_id, kunde_id)
        ).fetchone():
            abort(400)
        nachricht_id = _portal_nachricht_anlegen(
            db,
            ticket["id"],
            absender="team",
            art=art,
            text=text,
            projekt_id=projekt_id,
            teammitglied_id=g.user["id"],
        )
        gespeichert, fehler = _portal_dateien_speichern(db, ticket["id"], nachricht_id, dateien)
        if not text and gespeichert == 0:
            db.execute("DELETE FROM portal_nachrichten WHERE id = ?", (nachricht_id,))
            db.commit()
            flash("Keine der ausgewählten Dateien wird unterstützt.", "error")
            return redirect(url_for("kunden_ticket", kunde_id=kunde_id) + "#schreiben")
        db.commit()
        art_name = {"nachricht": "Nachricht", "update": "Neuerung", "anforderung": "Rückfrage", "meilenstein": "Meilenstein"}[art]
        mail_status = _portal_mail(
            db,
            ticket,
            nachricht_id=nachricht_id,
            richtung="an_kunde",
            empfaenger=ticket["email"],
            betreff=f"{art_name} in Ihrem Projektraum · {ticket['firma']}",
            text=(
                f"Guten Tag {ticket['ansprechpartner'] or ticket['firma']},\n\n"
                f"Tomorrow Works hat eine neue {art_name.lower()} in Ihrem Projektraum hinterlegt.\n\n"
                f"{text[:1200]}\n\nZum Projektraum:\n{_portal_url(ticket)}\n\nTomorrow Works"
            ),
        )
        if fehler:
            flash("Einige Dateitypen wurden nicht übernommen: " + ", ".join(fehler), "error")
        if mail_status == "gesendet":
            flash("Der Eintrag wurde veröffentlicht und der Kunde per E-Mail informiert.", "success")
        elif mail_status == "ungueltige_adresse":
            flash("Der Eintrag wurde veröffentlicht. Eine E-Mail wurde wegen unvollständiger Kundendaten nicht versendet.", "info")
        else:
            flash("Der Eintrag wurde veröffentlicht. Die E-Mail wartet noch auf Domain-/Mail-Einrichtung.", "info")
        return redirect(url_for("kunden_ticket", kunde_id=kunde_id) + "#verlauf")

    @app.post("/kunden/<int:kunde_id>/ticket/phase")
    @login_required
    def kunden_ticket_phase(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        phase = request.form.get("phase", "")
        if phase not in TICKET_PHASE_LABELS:
            abort(400)
        db.execute("UPDATE kunden_tickets SET phase = ?, updated_at = ? WHERE id = ?", (phase, jetzt(), ticket["id"]))
        db.commit()
        flash("Die Kundenphase wurde aktualisiert.", "success")
        return redirect(url_for("kunden_ticket", kunde_id=kunde_id))

    @app.post("/kunden/<int:kunde_id>/ticket/einladung")
    @login_required
    def kunden_ticket_einladung(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        fehlende_kundendaten = _fehlende_kundendaten(ticket)
        if fehlende_kundendaten:
            flash("Die Einladung wurde nicht versendet. Zuerst Kundendaten vervollständigen: " + ", ".join(fehlende_kundendaten) + ".", "error")
            return redirect(url_for("kunde_bearbeiten", kunde_id=kunde_id))
        if not _gueltige_email(ticket["email"]):
            flash("Die Einladung wurde nicht versendet. Bitte zuerst eine aktuelle E-Mail-Adresse eintragen.", "error")
            return redirect(url_for("kunde_bearbeiten", kunde_id=kunde_id))
        status = _portal_mail(
            db,
            ticket,
            nachricht_id=None,
            richtung="an_kunde",
            empfaenger=ticket["email"],
            betreff=f"Ihr dauerhafter Projektraum · {ticket['firma']}",
            text=(
                f"Guten Tag {ticket['ansprechpartner'] or ticket['firma']},\n\n"
                "über diesen persönlichen Link bleiben Sie dauerhaft mit Tomorrow Works verbunden. "
                "Sie sehen Neuerungen und können Nachrichten, Logos, Bilder oder Dokumente senden.\n\n"
                f"{_portal_url(ticket)}\n\nBitte behandeln Sie den Link vertraulich.\n\nTomorrow Works"
            ),
        )
        flash(
            "Die Einladung wurde per E-Mail gesendet." if status == "gesendet" else "Die Einladung ist vorbereitet; öffentliche Domain oder Mailserver fehlen noch.",
            "success" if status == "gesendet" else "info",
        )
        return redirect(url_for("kunden_ticket", kunde_id=kunde_id))

    @app.post("/kunden/<int:kunde_id>/ticket/link-erneuern")
    @admin_required
    def kunden_ticket_link_erneuern(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        db.execute(
            "UPDATE kunden_tickets SET token = ?, updated_at = ? WHERE id = ?",
            (secrets.token_urlsafe(36), jetzt(), ticket["id"]),
        )
        db.commit()
        flash("Der alte Kundenlink wurde ungültig. Bitte die neue Einladung senden.", "success")
        return redirect(url_for("kunden_ticket", kunde_id=kunde_id))

    @app.get("/kunden/<int:kunde_id>/ticket/dateien/<int:datei_id>")
    @login_required
    def kunden_ticket_datei(kunde_id: int, datei_id: int):
        ticket = _ticket_oder_404(kunde_id)
        datei = get_db().execute(
            "SELECT * FROM portal_dateien WHERE id = ? AND ticket_id = ?", (datei_id, ticket["id"])
        ).fetchone()
        if datei is None:
            abort(404)
        return send_from_directory(
            current_app.config["UPLOAD_FOLDER"],
            datei["gespeichert_name"],
            download_name=datei["original_name"],
            as_attachment=request.args.get("download") == "1",
        )

    @app.route("/kunden/<int:kunde_id>/ticket/angebot", methods=["GET", "POST"])
    @admin_required
    def kunden_ticket_angebot(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        angebot = _angebot_sicherstellen(db, ticket["id"], g.user["id"])
        db.commit()
        if request.method == "POST":
            if angebot["status"] in {"angenommen", "freigegeben"}:
                flash("Ein bereits bestätigtes Angebot kann nicht überschrieben werden.", "error")
            else:
                try:
                    einmalig = _euro_zu_cent(request.form.get("einmalig", ""), angebot["einmalig_cent"])
                    monatlich = _euro_zu_cent(request.form.get("monatlich", ""), angebot["monatlich_cent"])
                    einrichtung = _euro_zu_cent(request.form.get("einrichtung", ""), angebot["einrichtung_cent"])
                except ValueError as exc:
                    flash(str(exc), "error")
                else:
                    paket_name = request.form.get("paket_name", "").strip()
                    leistungsumfang = request.form.get("leistungsumfang", "").strip()
                    if not paket_name or not leistungsumfang:
                        flash("Paketname und Leistungsumfang dürfen nicht leer sein.", "error")
                    else:
                        db.execute(
                            """
                            UPDATE ticket_angebote SET paket_name = ?, einmalig_cent = ?, monatlich_cent = ?,
                                einrichtung_cent = ?, laufzeit_monate = ?, kuendigungsfrist_monate = ?,
                                leistungsumfang = ?, hinweise = ?, gueltig_bis = ?, status = 'entwurf',
                                updated_at = ? WHERE id = ?
                            """,
                            (
                                paket_name,
                                einmalig,
                                monatlich,
                                einrichtung,
                                _int_form("laufzeit_monate", 12, 0, 120),
                                _int_form("kuendigungsfrist_monate", 3, 0, 24),
                                leistungsumfang,
                                request.form.get("hinweise", "").strip(),
                                request.form.get("gueltig_bis") or None,
                                jetzt(),
                                angebot["id"],
                            ),
                        )
                        db.execute("UPDATE kunden_tickets SET phase = 'angebot', updated_at = ? WHERE id = ?", (jetzt(), ticket["id"]))
                        db.commit()
                        flash("Der Angebotsentwurf wurde gespeichert.", "success")
                        return redirect(url_for("kunden_ticket_angebot", kunde_id=kunde_id))
            angebot = db.execute("SELECT * FROM ticket_angebote WHERE id = ?", (angebot["id"],)).fetchone()
        return render_template("angebot_form.html", ticket=ticket, angebot=angebot)

    @app.post("/kunden/<int:kunde_id>/ticket/angebot/senden")
    @admin_required
    def kunden_ticket_angebot_senden(kunde_id: int):
        db = get_db()
        ticket = _ticket_oder_404(kunde_id)
        angebot = db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket["id"],)).fetchone()
        if angebot is None:
            flash("Bitte zuerst ein Angebot anlegen.", "error")
            return redirect(url_for("kunden_ticket_angebot", kunde_id=kunde_id))
        if angebot["status"] in {"angenommen", "freigegeben"}:
            flash("Das Angebot wurde bereits bestätigt.", "info")
            return redirect(url_for("kunden_ticket", kunde_id=kunde_id))
        timestamp = jetzt()
        db.execute("UPDATE ticket_angebote SET status = 'gesendet', sent_at = ?, updated_at = ? WHERE id = ?", (timestamp, timestamp, angebot["id"]))
        nachricht_id = _portal_nachricht_anlegen(
            db,
            ticket["id"],
            absender="system",
            art="angebot",
            text=f"Ein neues Angebot wurde bereitgestellt: {angebot['paket_name']}.",
            teammitglied_id=g.user["id"],
        )
        db.commit()
        status = _portal_mail(
            db,
            ticket,
            nachricht_id=nachricht_id,
            richtung="an_kunde",
            empfaenger=ticket["email"],
            betreff=f"Ihr Angebot von Tomorrow Works · {ticket['firma']}",
            text=(
                f"Guten Tag {ticket['ansprechpartner'] or ticket['firma']},\n\n"
                f"Ihr Angebot „{angebot['paket_name']}“ ist im Projektraum verfügbar.\n"
                f"Einmalig: {angebot['einmalig_cent'] / 100:.2f} EUR\n"
                f"Monatlich: {angebot['monatlich_cent'] / 100:.2f} EUR\n\n"
                f"Angebot ansehen:\n{_portal_url(ticket)}\n\nTomorrow Works"
            ),
        )
        flash(
            "Das Angebot wurde veröffentlicht und per E-Mail gesendet." if status == "gesendet" else "Das Angebot wurde veröffentlicht; die E-Mail wartet noch auf Domain-/Mail-Einrichtung.",
            "success" if status == "gesendet" else "info",
        )
        return redirect(url_for("kunden_ticket", kunde_id=kunde_id))

    @app.get("/portal/<token>")
    def kundenportal(token: str):
        db = get_db()
        ticket = _ticket_token_oder_404(token)
        db.execute(
            "UPDATE portal_nachrichten SET gelesen_kunde = 1 WHERE ticket_id = ? AND absender != 'kunde'",
            (ticket["id"],),
        )
        db.commit()
        nachrichten, nachrichten_dateien = _portal_verlauf(db, ticket["id"])
        projekte = db.execute(
            """
            SELECT id, titel, typ, status, fortschritt, aktuelle_aufgabe, vorschau_url, updated_at
            FROM projekte WHERE kunde_id = ? ORDER BY updated_at DESC
            """,
            (ticket["kunde_id"],),
        ).fetchall()
        angebot = db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket["id"],)).fetchone()
        return render_template(
            "kundenportal.html",
            ticket=ticket,
            nachrichten=nachrichten,
            nachrichten_dateien=nachrichten_dateien,
            projekte=projekte,
            angebot=angebot,
        )

    @app.post("/portal/<token>/nachricht")
    def kundenportal_nachricht(token: str):
        db = get_db()
        ticket = _ticket_token_oder_404(token)
        grenze = (datetime.now() - timedelta(minutes=10)).replace(microsecond=0).isoformat(sep=" ")
        anzahl = db.execute(
            "SELECT COUNT(*) FROM portal_nachrichten WHERE ticket_id = ? AND absender = 'kunde' AND created_at >= ?",
            (ticket["id"], grenze),
        ).fetchone()[0]
        if anzahl >= 12:
            flash("Bitte kurz warten, bevor Sie weitere Nachrichten senden.", "error")
            return redirect(url_for("kundenportal", token=token) + "#schreiben")
        text = request.form.get("text", "").strip()
        dateien = [datei for datei in request.files.getlist("dateien") if datei and datei.filename]
        if not text and not dateien:
            flash("Bitte schreiben Sie eine Nachricht oder wählen Sie mindestens eine Datei aus.", "error")
            return redirect(url_for("kundenportal", token=token) + "#schreiben")
        projekt_id = request.form.get("projekt_id", type=int)
        if projekt_id and not db.execute(
            "SELECT 1 FROM projekte WHERE id = ? AND kunde_id = ?", (projekt_id, ticket["kunde_id"])
        ).fetchone():
            abort(400)
        nachricht_id = _portal_nachricht_anlegen(
            db,
            ticket["id"],
            absender="kunde",
            art="nachricht",
            text=text,
            projekt_id=projekt_id,
        )
        gespeichert, fehler = _portal_dateien_speichern(db, ticket["id"], nachricht_id, dateien)
        if not text and gespeichert == 0:
            db.execute("DELETE FROM portal_nachrichten WHERE id = ?", (nachricht_id,))
            db.commit()
            flash("Keine der ausgewählten Dateien wird unterstützt.", "error")
            return redirect(url_for("kundenportal", token=token) + "#schreiben")
        db.commit()
        _portal_mail(
            db,
            ticket,
            nachricht_id=nachricht_id,
            richtung="an_team",
            empfaenger=_team_empfaenger(db),
            betreff=f"Neue Kunden-Nachricht · {ticket['firma']}",
            text=(
                f"{ticket['ansprechpartner'] or ticket['firma']} hat im Tomorrow-Works-Projektraum geschrieben.\n\n"
                f"{text[:1500]}\n\nIm internen Dashboard öffnen:\n"
                f"{request.url_root.rstrip('/')}{url_for('kunden_ticket', kunde_id=ticket['kunde_id'])}"
            ),
        )
        if fehler:
            flash("Einige Dateitypen konnten nicht übernommen werden: " + ", ".join(fehler), "error")
        flash("Vielen Dank. Ihre Nachricht ist jetzt für das Tomorrow-Works-Team sichtbar.", "success")
        return redirect(url_for("kundenportal", token=token) + "#verlauf")

    @app.get("/portal/<token>/dateien/<int:datei_id>")
    def kundenportal_datei(token: str, datei_id: int):
        ticket = _ticket_token_oder_404(token)
        datei = get_db().execute(
            "SELECT * FROM portal_dateien WHERE id = ? AND ticket_id = ?", (datei_id, ticket["id"])
        ).fetchone()
        if datei is None:
            abort(404)
        endung = datei["original_name"].rsplit(".", 1)[-1].lower() if "." in datei["original_name"] else ""
        inline_erlaubt = endung in {"pdf", "png", "jpg", "jpeg", "webp"}
        return send_from_directory(
            current_app.config["UPLOAD_FOLDER"],
            datei["gespeichert_name"],
            download_name=datei["original_name"],
            as_attachment=not inline_erlaubt or request.args.get("download") == "1",
        )

    @app.post("/portal/<token>/angebot/freigeben")
    def kundenportal_angebot_freigeben(token: str):
        db = get_db()
        ticket = _ticket_token_oder_404(token)
        angebot = db.execute("SELECT * FROM ticket_angebote WHERE ticket_id = ?", (ticket["id"],)).fetchone()
        if angebot is None or angebot["status"] != "gesendet":
            flash("Dieses Angebot kann derzeit nicht bestätigt werden.", "error")
            return redirect(url_for("kundenportal", token=token) + "#angebot")
        name = request.form.get("name", "").strip()
        bestaetigt = request.form.get("zahlungspflichtig") == "1" if current_app.config["CONTRACT_LEGAL_APPROVED"] else request.form.get("freigabe") == "1"
        if len(name) < 3 or not bestaetigt:
            flash("Bitte Namen und Bestätigung vollständig angeben.", "error")
            return redirect(url_for("kundenportal", token=token) + "#angebot")
        neuer_status = "angenommen" if current_app.config["CONTRACT_LEGAL_APPROVED"] else "freigegeben"
        timestamp = jetzt()
        db.execute(
            """
            UPDATE ticket_angebote SET status = ?, accepted_at = ?, accepted_name = ?, accepted_email = ?, updated_at = ?
            WHERE id = ?
            """,
            (neuer_status, timestamp, name, ticket["email"], timestamp, angebot["id"]),
        )
        db.execute("UPDATE kunden_tickets SET phase = 'umsetzung', updated_at = ?, last_activity_at = ? WHERE id = ?", (timestamp, timestamp, ticket["id"]))
        nachricht_id = _portal_nachricht_anlegen(
            db,
            ticket["id"],
            absender="kunde",
            art="vertrag",
            text=(
                f"{name} hat das Angebot „{angebot['paket_name']}“ "
                + ("zahlungspflichtig angenommen." if neuer_status == "angenommen" else "zur abschließenden Vertragsprüfung freigegeben.")
            ),
        )
        db.commit()
        _portal_mail(
            db,
            ticket,
            nachricht_id=nachricht_id,
            richtung="an_team",
            empfaenger=_team_empfaenger(db),
            betreff=f"Angebot bestätigt · {ticket['firma']}",
            text=f"{name} hat das Angebot „{angebot['paket_name']}“ bestätigt. Bitte im internen Kunden-Ticket prüfen.",
        )
        _portal_mail(
            db,
            ticket,
            nachricht_id=nachricht_id,
            richtung="an_kunde",
            empfaenger=ticket["email"],
            betreff=f"Bestätigung Ihres Angebots · {ticket['firma']}",
            text=(
                f"Guten Tag {name},\n\nIhre Bestätigung für „{angebot['paket_name']}“ wurde am {timestamp} gespeichert.\n\n"
                f"Ihr Projektraum bleibt dauerhaft erreichbar:\n{_portal_url(ticket)}\n\nTomorrow Works"
            ),
        )
        flash(
            "Vielen Dank. Das Angebot wurde verbindlich angenommen." if neuer_status == "angenommen" else "Vielen Dank. Ihre Freigabe wurde gespeichert; Tomorrow Works bereitet den geprüften Vertragsabschluss vor.",
            "success",
        )
        return redirect(url_for("kundenportal", token=token) + "#angebot")

    @app.route("/projekte/neu", methods=["GET", "POST"])
    @login_required
    def projekt_neu():
        db = get_db()
        kunden = db.execute("SELECT id, firma FROM kunden WHERE status != 'archiviert' ORDER BY firma").fetchall()
        team = db.execute("SELECT * FROM teammitglieder WHERE aktiv = 1 ORDER BY name").fetchall()
        if not kunden:
            flash("Lege zuerst einen Kunden an.", "info")
            return redirect(url_for("kunde_neu"))
        if request.method == "POST":
            try:
                vorschau_url = _saubere_url(request.form.get("vorschau_url", ""))
                repo_url = _saubere_url(request.form.get("repo_url", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                kunde_id = request.form.get("kunde_id", type=int)
                titel = request.form.get("titel", "").strip()
                if not titel or not db.execute("SELECT 1 FROM kunden WHERE id = ?", (kunde_id,)).fetchone():
                    flash("Bitte Kunde und Projektnamen vollständig angeben.", "error")
                else:
                    timestamp = jetzt()
                    cur = db.execute(
                        """
                        INSERT INTO projekte
                          (kunde_id, titel, typ, beschreibung, status, prioritaet, startdatum, zieldatum,
                           fortschritt, aktuelle_aufgabe, blockiert_grund, vorschau_url, repo_url, lokaler_pfad,
                           preview_pfad, agent_token, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            kunde_id,
                            titel,
                            request.form.get("typ", "website"),
                            request.form.get("beschreibung", "").strip(),
                            request.form.get("status", "neu"),
                            request.form.get("prioritaet", "normal"),
                            request.form.get("startdatum") or None,
                            request.form.get("zieldatum") or None,
                            _int_form("fortschritt"),
                            request.form.get("aktuelle_aufgabe", "").strip(),
                            request.form.get("blockiert_grund", "").strip(),
                            vorschau_url,
                            repo_url,
                            request.form.get("lokaler_pfad", "").strip(),
                            request.form.get("preview_pfad", ".").strip() or ".",
                            secrets.token_urlsafe(32),
                            timestamp,
                            timestamp,
                        ),
                    )
                    _zuweisungen_speichern(db, cur.lastrowid)
                    _aktivitaet(cur.lastrowid, "Projekt angelegt", request.form.get("aktuelle_aufgabe", ""))
                    db.execute("UPDATE kunden SET updated_at = ? WHERE id = ?", (timestamp, kunde_id))
                    projekt_row = db.execute("SELECT * FROM projekte WHERE id = ?", (cur.lastrowid,)).fetchone()
                    if projekt_row and projekt_row["lokaler_pfad"]:
                        projekt_git_synchronisieren(db, projekt_row, aktivitaet_schreiben=False)
                    db.commit()
                    flash("Projekt wurde angelegt und dem Team zugewiesen.", "success")
                    return redirect(url_for("projekt_detail", projekt_id=cur.lastrowid))
        kunde_vorauswahl = request.args.get("kunde", type=int) or request.form.get("kunde_id", type=int)
        return render_template(
            "projekt_form.html",
            projekt=None,
            kunden=kunden,
            team=team,
            zugewiesene_ids=set(),
            haupt_id=None,
            kunde_vorauswahl=kunde_vorauswahl,
            projekt_typen=PROJEKT_TYPEN,
            prioritaeten=PRIORITAETEN,
        )

    @app.get("/projekte/<int:projekt_id>")
    @login_required
    def projekt_detail(projekt_id: int):
        db = get_db()
        projekt = _projekt_oder_404(projekt_id)
        if projekt["lokaler_pfad"]:
            projekt_git_synchronisieren(db, projekt)
            db.commit()
            projekt = _projekt_oder_404(projekt_id)
        team = db.execute(
            """
            SELECT t.*, pt.ist_hauptverantwortlich FROM projekt_team pt
            JOIN teammitglieder t ON t.id = pt.teammitglied_id
            WHERE pt.projekt_id = ? ORDER BY pt.ist_hauptverantwortlich DESC, t.name
            """,
            (projekt_id,),
        ).fetchall()
        aktivitaeten = db.execute(
            """
            SELECT a.*, t.name AS teamname, t.farbe AS teamfarbe FROM aktivitaeten a
            LEFT JOIN teammitglieder t ON t.id = a.teammitglied_id
            WHERE a.projekt_id = ? ORDER BY a.created_at DESC, a.id DESC LIMIT 50
            """,
            (projekt_id,),
        ).fetchall()
        dateien = db.execute(
            """
            SELECT d.*, t.name AS teamname FROM projekt_dateien d
            LEFT JOIN teammitglieder t ON t.id = d.hochgeladen_von
            WHERE d.projekt_id = ? ORDER BY d.created_at DESC
            """,
            (projekt_id,),
        ).fetchall()
        video_dateien = [
            datei for datei in dateien
            if Path(datei["original_name"]).suffix.lower().lstrip(".") in VIDEO_DATEIEN
        ]
        interne_url = interne_vorschau_url(projekt)
        agent_script = str((BASE_DIR / "agent_sync.py").resolve())
        dashboard_url = request.url_root.rstrip("/")
        agent_befehl = (
            f'$env:TW_PROJECT_TOKEN="{projekt["agent_token"]}"; '
            f'python "{agent_script}" --dashboard "{dashboard_url}" --project-id {projekt_id} '
            '--status in_arbeit --progress 50 --task "Nächster Schritt" --note "Interne Übergabe" '
            '--customer-update "Konkrete Neuerung für den Kunden"'
        )
        agent_anweisung = (
            "Nach jeder relevanten Änderung: Tests ausführen, den Git-Stand sauber speichern und anschließend "
            "den folgenden Dashboard-Befehl mit dem tatsächlichen Status, Fortschritt, nächsten Schritt und "
            "einer kurzen Notiz ausführen. --customer-update nur verwenden, wenn der Text wirklich für den Kunden "
            "bestimmt ist; dadurch entsteht ein sichtbarer Ticket-Eintrag samt E-Mail. Das Projekt-Token niemals committen."
        )
        preview_team_urls = []
        if interne_url:
            preview_team_urls.append(interne_url)
            preview_team_urls.extend(
                f"{adresse.rsplit(':', 1)[0]}:{projekt['preview_port']}/" for adresse in netzwerk_adressen(app.config["DASHBOARD_PORT"])
            )
        return render_template(
            "projekt_detail.html",
            projekt=projekt,
            team=team,
            aktivitaeten=aktivitaeten,
            dateien=dateien,
            video_dateien=video_dateien,
            interne_vorschau_url=interne_url,
            preview_team_urls=list(dict.fromkeys(preview_team_urls)),
            agent_befehl=agent_befehl,
            agent_anweisung=agent_anweisung,
        )

    @app.get("/projekte/<int:projekt_id>/verkaufsvideo")
    @login_required
    def verkaufsvideo_form(projekt_id: int):
        projekt = _projekt_oder_404(projekt_id)
        dateien = get_db().execute(
            """
            SELECT * FROM projekt_dateien
            WHERE projekt_id = ? ORDER BY created_at DESC, id DESC
            """,
            (projekt_id,),
        ).fetchall()
        quell_dateien = [
            datei for datei in dateien
            if Path(datei["original_name"]).suffix.lower().lstrip(".") in VIDEO_QUELLDATEIEN
        ]
        # Für einen nachvollziehbaren Rundgang stehen Quellen in Upload-Reihenfolge.
        # Das zuletzt erzeugte Video bleibt dagegen oben in der Vorschau.
        quell_dateien.reverse()
        bestehende_videos = [
            datei for datei in dateien
            if Path(datei["original_name"]).suffix.lower().lstrip(".") in VIDEO_DATEIEN
        ]
        return render_template(
            "verkaufsvideo_form.html",
            projekt=projekt,
            quell_dateien=quell_dateien,
            bestehende_videos=bestehende_videos,
            werte=_verkaufsvideo_standardtexte(projekt),
            tts_configured=bool(app.config["TTS_API_KEY"]),
            tts_voice=app.config["TTS_VOICE"],
        )

    @app.post("/projekte/<int:projekt_id>/verkaufsvideo")
    @login_required
    def verkaufsvideo_generieren(projekt_id: int):
        projekt = _projekt_oder_404(projekt_id)
        ausgewaehlte_ids: list[int] = []
        for raw in request.form.getlist("datei_ids"):
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value > 0 and value not in ausgewaehlte_ids:
                ausgewaehlte_ids.append(value)
        if len(ausgewaehlte_ids) > 8:
            flash("Bitte höchstens acht Quelldateien auswählen. Der Film zeigt maximal zehn Ansichten.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        source_rows: list[sqlite3.Row] = []
        if ausgewaehlte_ids:
            platzhalter = ",".join("?" for _ in ausgewaehlte_ids)
            rows = get_db().execute(
                f"SELECT * FROM projekt_dateien WHERE projekt_id = ? AND id IN ({platzhalter})",
                (projekt_id, *ausgewaehlte_ids),
            ).fetchall()
            row_map = {row["id"]: row for row in rows}
            source_rows = [row_map[datei_id] for datei_id in ausgewaehlte_ids if datei_id in row_map]
            if len(source_rows) != len(ausgewaehlte_ids) or any(
                Path(row["original_name"]).suffix.lower().lstrip(".") not in VIDEO_QUELLDATEIEN
                for row in source_rows
            ):
                abort(400)

        standardwerte = _verkaufsvideo_standardtexte(projekt)
        headline = request.form.get("headline", "").strip()[:140]
        subtitle = request.form.get("subtitle", "").strip()[:320]
        cta = request.form.get("cta", "").strip()[:240]
        kapitel = _zeilen(request.form.get("kapitel", ""), maximum=12, laenge=90)
        potenziale = _zeilen(request.form.get("potenziale", ""), maximum=4, laenge=170)
        servicepunkte = _zeilen(
            request.form.get("servicepunkte", standardwerte["servicepunkte"]),
            maximum=3,
            laenge=190,
        )
        mit_sprecher = request.form.get("mit_sprecher") == "1"
        sprechertext = request.form.get("sprechertext", "").strip()[:2500] if mit_sprecher else ""
        dauer = _int_form("foliendauer", 4, 2, 5)
        if not headline or not subtitle or not cta or not potenziale or not servicepunkte:
            flash("Bitte Titel, Kurzbeschreibung, Betreuung, Potenziale und Abschluss vollständig ausfüllen.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        if mit_sprecher and not app.config["TTS_API_KEY"]:
            flash("Die sichere Sprecherstimme ist auf diesem Server noch nicht eingerichtet.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        if mit_sprecher and len(sprechertext) < 80:
            flash("Bitte für die Sprecherstimme einen vollständigen Text mit mindestens 80 Zeichen eingeben.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        upload_root = Path(app.config["UPLOAD_FOLDER"]).resolve()
        sources: list[SalesVideoSource] = []
        for row in source_rows:
            path = (upload_root / row["gespeichert_name"]).resolve()
            if path.parent != upload_root or not path.is_file():
                flash(f"Die Projektdatei ‚{row['original_name']}‘ ist nicht mehr verfügbar.", "error")
                return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
            sources.append(SalesVideoSource(path=path, display_name=row["original_name"]))

        if not verkaufsvideo_lock_anfordern():
            flash("Gerade wird bereits ein Verkaufsvideo erzeugt. Bitte in einem Moment erneut starten.", "info")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        gespeichert_name = f"{projekt_id}_verkaufsvideo_{uuid4().hex}.mp4"
        output_path = upload_root / gespeichert_name
        try:
            result = create_sales_video(
                sources,
                output_path,
                customer_name=projekt["kundenname"],
                project_title=projekt["titel"],
                headline=headline,
                subtitle=subtitle,
                chapters=kapitel,
                service_points=servicepunkte,
                potentials=potenziale,
                cta=cta,
                progress=projekt["fortschritt"],
                seconds_per_slide=dauer,
                narration_text=sprechertext,
                tts_api_key=app.config["TTS_API_KEY"],
                tts_model=app.config["TTS_MODEL"],
                tts_voice=app.config["TTS_VOICE"],
                tts_instructions=app.config["TTS_INSTRUCTIONS"],
            )
        except SalesVideoError as exc:
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Unvollständiges Verkaufsvideo konnte nicht gelöscht werden: %s", output_path)
            flash(str(exc), "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        except Exception:
            current_app.logger.exception("Unerwarteter Fehler beim Verkaufsvideo für Projekt %s", projekt_id)
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Unvollständiges Verkaufsvideo konnte nicht gelöscht werden: %s", output_path)
            flash("Das Verkaufsvideo konnte unerwartet nicht erzeugt werden. Bitte später erneut versuchen.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        try:
            video_size = output_path.stat().st_size
        except OSError:
            current_app.logger.exception("Erzeugtes Verkaufsvideo ist nicht lesbar: %s", output_path)
            flash("Die erzeugte MP4-Datei konnte nicht sicher gelesen werden.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        if video_size > 20 * 1024 * 1024:
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Zu großes Verkaufsvideo konnte nicht gelöscht werden: %s", output_path)
            flash("Das Video wäre größer als 20 MB. Bitte weniger Quelldateien auswählen.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        original_name = secure_filename(
            f"Verkaufsvideo_{projekt['kundenname']}_{date.today().isoformat()}.mp4"
        ) or f"Verkaufsvideo_Projekt-{projekt_id}_{date.today().isoformat()}.mp4"
        db = get_db()
        alte_videos = db.execute(
            """
            SELECT id, gespeichert_name FROM projekt_dateien
            WHERE projekt_id = ? AND mimetype = 'video/mp4' AND original_name LIKE 'Verkaufsvideo_%'
            """,
            (projekt_id,),
        ).fetchall()
        try:
            db.execute(
                "DELETE FROM projekt_dateien WHERE projekt_id = ? AND mimetype = 'video/mp4' AND original_name LIKE 'Verkaufsvideo_%'",
                (projekt_id,),
            )
            db.execute(
                """
                INSERT INTO projekt_dateien
                  (projekt_id, original_name, gespeichert_name, mimetype, hochgeladen_von, created_at)
                VALUES (?, ?, ?, 'video/mp4', ?, ?)
                """,
                (projekt_id, original_name, gespeichert_name, g.user["id"], jetzt()),
            )
            db.execute("UPDATE projekte SET updated_at = ? WHERE id = ?", (jetzt(), projekt_id))
            _aktivitaet(
                projekt_id,
                "Verkaufsvideo generiert · intern",
                (
                    f"{result.slide_count} Folien · {result.duration_seconds} Sekunden · "
                    f"{result.source_count} Projektansichten · "
                    f"{'KI-Sprecherstimme' if result.narrated else 'dezente Musik'} · kein Kundenversand"
                ),
            )
            db.commit()
        except sqlite3.Error:
            db.rollback()
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Nicht registriertes Verkaufsvideo konnte nicht gelöscht werden: %s", output_path)
            flash("Das Video wurde erzeugt, konnte aber nicht sicher im Projekt gespeichert werden.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))
        for altes_video in alte_videos:
            alter_pfad = (upload_root / altes_video["gespeichert_name"]).resolve()
            if alter_pfad.parent == upload_root:
                try:
                    alter_pfad.unlink(missing_ok=True)
                except OSError:
                    current_app.logger.warning("Altes Verkaufsvideo konnte nicht gelöscht werden: %s", alter_pfad)

        ton_hinweis = "mit KI-generierter Sprecherstimme" if result.narrated else "mit dezenter Musik"
        flash(
            f"Verkaufsvideo wurde {ton_hinweis} intern erzeugt und im Projekt gespeichert. "
            "Es wurde nichts an den Kunden gesendet.",
            "success",
        )
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#verkaufsvideo")

    @app.post("/projekte/<int:projekt_id>/verkaufsvideo/hochladen")
    @login_required
    def verkaufsvideo_hochladen(projekt_id: int):
        projekt = _projekt_oder_404(projekt_id)
        datei = request.files.get("video")
        if not datei or not datei.filename:
            flash("Bitte einen fertigen MP4-Film auswählen.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        original = secure_filename(datei.filename)
        if not original or Path(original).suffix.lower() != ".mp4":
            flash("Für den fertigen Sprecherfilm ist nur eine MP4-Datei erlaubt.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        if not verkaufsvideo_lock_anfordern():
            flash("Gerade wird bereits ein Verkaufsvideo verarbeitet. Bitte in einem Moment erneut versuchen.", "info")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        upload_root = Path(app.config["UPLOAD_FOLDER"]).resolve()
        gespeichert_name = f"{projekt_id}_verkaufsvideo_{uuid4().hex}.mp4"
        output_path = upload_root / gespeichert_name
        try:
            datei.save(output_path)
            video_size = output_path.stat().st_size
            with output_path.open("rb") as stream:
                header = stream.read(32)
        except OSError:
            current_app.logger.exception("Bereitgestellter Sprecherfilm konnte nicht gespeichert werden")
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                pass
            flash("Der fertige Sprecherfilm konnte nicht sicher gespeichert werden.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        if video_size < 12 or header[4:8] != b"ftyp":
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Ungültiger Sprecherfilm konnte nicht gelöscht werden: %s", output_path)
            flash("Die ausgewählte Datei ist keine gültige MP4-Datei.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        try:
            validate_sales_video_mp4(output_path)
        except SalesVideoError as exc:
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Nicht lesbarer Sprecherfilm konnte nicht gelöscht werden: %s", output_path)
            flash(str(exc), "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        original_name = secure_filename(
            f"Verkaufsvideo_{projekt['kundenname']}_{date.today().isoformat()}.mp4"
        ) or f"Verkaufsvideo_Projekt-{projekt_id}_{date.today().isoformat()}.mp4"
        db = get_db()
        alte_videos = db.execute(
            """
            SELECT id, gespeichert_name FROM projekt_dateien
            WHERE projekt_id = ? AND mimetype = 'video/mp4' AND original_name LIKE 'Verkaufsvideo_%'
            """,
            (projekt_id,),
        ).fetchall()
        try:
            db.execute(
                "DELETE FROM projekt_dateien WHERE projekt_id = ? AND mimetype = 'video/mp4' AND original_name LIKE 'Verkaufsvideo_%'",
                (projekt_id,),
            )
            db.execute(
                """
                INSERT INTO projekt_dateien
                  (projekt_id, original_name, gespeichert_name, mimetype, hochgeladen_von, created_at)
                VALUES (?, ?, ?, 'video/mp4', ?, ?)
                """,
                (projekt_id, original_name, gespeichert_name, g.user["id"], jetzt()),
            )
            db.execute("UPDATE projekte SET updated_at = ? WHERE id = ?", (jetzt(), projekt_id))
            _aktivitaet(
                projekt_id,
                "Verkaufsvideo hochgeladen · intern",
                f"Fertiger MP4-Film · {video_size / 1024 / 1024:.1f} MB · Bild- und Tonspur geprüft · kein Kundenversand",
            )
            db.commit()
        except sqlite3.Error:
            db.rollback()
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                current_app.logger.warning("Nicht registrierter Sprecherfilm konnte nicht gelöscht werden: %s", output_path)
            flash("Der Film wurde hochgeladen, konnte aber nicht sicher im Projekt registriert werden.", "error")
            return redirect(url_for("verkaufsvideo_form", projekt_id=projekt_id))

        for altes_video in alte_videos:
            alter_pfad = (upload_root / altes_video["gespeichert_name"]).resolve()
            if alter_pfad.parent == upload_root:
                try:
                    alter_pfad.unlink(missing_ok=True)
                except OSError:
                    current_app.logger.warning("Altes Verkaufsvideo konnte nicht gelöscht werden: %s", alter_pfad)

        flash(
            "Der fertige Sprecherfilm wurde intern übernommen und im Projekt gespeichert. "
            "Es wurde nichts an den Kunden gesendet.",
            "success",
        )
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#verkaufsvideo")

    @app.route("/projekte/<int:projekt_id>/bearbeiten", methods=["GET", "POST"])
    @login_required
    def projekt_bearbeiten(projekt_id: int):
        db = get_db()
        projekt = _projekt_oder_404(projekt_id)
        kunden = db.execute("SELECT id, firma FROM kunden WHERE status != 'archiviert' ORDER BY firma").fetchall()
        team = db.execute("SELECT * FROM teammitglieder WHERE aktiv = 1 ORDER BY name").fetchall()
        zuweisungen = db.execute(
            "SELECT teammitglied_id, ist_hauptverantwortlich FROM projekt_team WHERE projekt_id = ?", (projekt_id,)
        ).fetchall()
        zugewiesene_ids = {row["teammitglied_id"] for row in zuweisungen}
        haupt_id = next((row["teammitglied_id"] for row in zuweisungen if row["ist_hauptverantwortlich"]), None)
        if request.method == "POST":
            try:
                vorschau_url = _saubere_url(request.form.get("vorschau_url", ""))
                repo_url = _saubere_url(request.form.get("repo_url", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                titel = request.form.get("titel", "").strip()
                kunde_id = request.form.get("kunde_id", type=int)
                status = request.form.get("status", "neu")
                if not titel or status not in PROJEKT_STATUS_LABELS:
                    flash("Bitte Projektname und Status prüfen.", "error")
                else:
                    db.execute(
                        """
                        UPDATE projekte SET kunde_id = ?, titel = ?, typ = ?, beschreibung = ?, status = ?,
                            prioritaet = ?, startdatum = ?, zieldatum = ?, fortschritt = ?, aktuelle_aufgabe = ?,
                            blockiert_grund = ?, vorschau_url = ?, repo_url = ?, lokaler_pfad = ?, preview_pfad = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            kunde_id,
                            titel,
                            request.form.get("typ", "website"),
                            request.form.get("beschreibung", "").strip(),
                            status,
                            request.form.get("prioritaet", "normal"),
                            request.form.get("startdatum") or None,
                            request.form.get("zieldatum") or None,
                            _int_form("fortschritt"),
                            request.form.get("aktuelle_aufgabe", "").strip(),
                            request.form.get("blockiert_grund", "").strip(),
                            vorschau_url,
                            repo_url,
                            request.form.get("lokaler_pfad", "").strip(),
                            request.form.get("preview_pfad", ".").strip() or ".",
                            jetzt(),
                            projekt_id,
                        ),
                    )
                    _zuweisungen_speichern(db, projekt_id)
                    _aktivitaet(projekt_id, "Projekt aktualisiert", request.form.get("aktuelle_aufgabe", ""))
                    aktualisiert = db.execute("SELECT * FROM projekte WHERE id = ?", (projekt_id,)).fetchone()
                    if aktualisiert and aktualisiert["lokaler_pfad"]:
                        projekt_git_synchronisieren(db, aktualisiert)
                    db.commit()
                    flash("Projekt wurde aktualisiert.", "success")
                    return redirect(url_for("projekt_detail", projekt_id=projekt_id))
        return render_template(
            "projekt_form.html",
            projekt=projekt,
            kunden=kunden,
            team=team,
            zugewiesene_ids=zugewiesene_ids,
            haupt_id=haupt_id,
            kunde_vorauswahl=projekt["kunde_id"],
            projekt_typen=PROJEKT_TYPEN,
            prioritaeten=PRIORITAETEN,
        )

    @app.post("/projekte/<int:projekt_id>/status")
    @login_required
    def projekt_status_aendern(projekt_id: int):
        projekt = _projekt_oder_404(projekt_id)
        status = request.form.get("status", "")
        if status not in PROJEKT_STATUS_LABELS:
            abort(400)
        fortschritt = _int_form("fortschritt", projekt["fortschritt"])
        aufgabe = request.form.get("aktuelle_aufgabe", "").strip()
        grund = request.form.get("blockiert_grund", "").strip() if status == "blockiert" else ""
        db = get_db()
        db.execute(
            "UPDATE projekte SET status = ?, fortschritt = ?, aktuelle_aufgabe = ?, blockiert_grund = ?, updated_at = ? WHERE id = ?",
            (status, fortschritt, aufgabe, grund, jetzt(), projekt_id),
        )
        _aktivitaet(projekt_id, f"Status: {PROJEKT_STATUS_LABELS[status]}", aufgabe or grund)
        db.commit()
        flash("Projektstatus wurde aktualisiert.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id))

    @app.post("/projekte/<int:projekt_id>/git-synchronisieren")
    @login_required
    def projekt_git_sync(projekt_id: int):
        db = get_db()
        projekt = _projekt_oder_404(projekt_id)
        if not projekt["lokaler_pfad"]:
            flash("Bitte zuerst den lokalen Projektordner hinterlegen.", "error")
        else:
            info = projekt_git_synchronisieren(db, projekt)
            db.commit()
            if info["git_fehler"]:
                flash(str(info["git_fehler"]), "error")
            else:
                flash("Git-Stand wurde aktualisiert.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#verbindung")

    @app.post("/projekte/<int:projekt_id>/vorschau/starten")
    @login_required
    def projekt_vorschau_start(projekt_id: int):
        db = get_db()
        projekt = _projekt_oder_404(projekt_id)
        preview_pfad = request.form.get("preview_pfad", projekt["preview_pfad"] or ".").strip() or "."
        try:
            info = vorschau_starten(projekt_id, projekt["lokaler_pfad"], preview_pfad)
        except (ValueError, RuntimeError) as exc:
            flash(str(exc), "error")
        else:
            db.execute(
                "UPDATE projekte SET preview_pfad = ?, preview_port = ?, preview_aktiv = 1, updated_at = ? WHERE id = ?",
                (preview_pfad, info["port"], jetzt(), projekt_id),
            )
            _aktivitaet(projekt_id, "Interne Vorschau gestartet", f"{preview_pfad} · Port {info['port']}")
            db.commit()
            flash("Interne Vorschau läuft jetzt für das Team.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#verbindung")

    @app.post("/projekte/<int:projekt_id>/vorschau/stoppen")
    @login_required
    def projekt_vorschau_stop(projekt_id: int):
        _projekt_oder_404(projekt_id)
        vorschau_stoppen(projekt_id)
        db = get_db()
        db.execute("UPDATE projekte SET preview_aktiv = 0 WHERE id = ?", (projekt_id,))
        _aktivitaet(projekt_id, "Interne Vorschau gestoppt")
        db.commit()
        flash("Interne Vorschau wurde gestoppt.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#verbindung")

    @app.post("/projekte/<int:projekt_id>/agent-token-neu")
    @admin_required
    def projekt_agent_token_neu(projekt_id: int):
        _projekt_oder_404(projekt_id)
        db = get_db()
        db.execute("UPDATE projekte SET agent_token = ? WHERE id = ?", (secrets.token_urlsafe(32), projekt_id))
        _aktivitaet(projekt_id, "Agent-Verbindungsschlüssel erneuert")
        db.commit()
        flash("Der Verbindungsschlüssel wurde erneuert. Alte Befehle funktionieren nicht mehr.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#agent-verbindung")

    @app.get("/api/projekte/status")
    @login_required
    def projekt_status_api():
        projekte = get_db().execute(
            """
            SELECT id, git_status, git_branch, git_kurz, git_author, git_nachricht, git_dirty,
                   git_ahead, git_behind, git_last_checked, git_fehler, preview_port, preview_aktiv
            FROM projekte ORDER BY id
            """
        ).fetchall()
        return {
            "ok": True,
            "projekte": [
                {
                    **dict(projekt),
                    "git_label": GIT_STATUS_LABELS.get(projekt["git_status"], projekt["git_status"]),
                    "preview_aktiv": bool(projekt["preview_aktiv"] and vorschau_aktiv(projekt["id"])),
                }
                for projekt in projekte
            ],
        }

    @app.post("/api/agent/projekte/<int:projekt_id>/update")
    def agent_update_api(projekt_id: int):
        db = get_db()
        projekt = db.execute("SELECT * FROM projekte WHERE id = ?", (projekt_id,)).fetchone()
        if projekt is None:
            return {"ok": False, "error": "Projekt nicht gefunden."}, 404
        authorization = request.headers.get("Authorization", "")
        token = authorization[7:].strip() if authorization.startswith("Bearer ") else ""
        if not token or not secrets.compare_digest(token, projekt["agent_token"] or ""):
            return {"ok": False, "error": "Ungültiger Projekt-Schlüssel."}, 401
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return {"ok": False, "error": "JSON-Daten fehlen."}, 400

        updates: dict[str, object] = {}
        status = str(payload.get("status", "")).strip()
        if status:
            if status not in PROJEKT_STATUS_LABELS:
                return {"ok": False, "error": "Ungültiger Projektstatus."}, 400
            updates["status"] = status
        if "fortschritt" in payload:
            try:
                updates["fortschritt"] = min(max(int(payload["fortschritt"]), 0), 100)
            except (TypeError, ValueError):
                return {"ok": False, "error": "Fortschritt muss eine Zahl sein."}, 400
        for feld in ("aktuelle_aufgabe", "git_branch", "git_commit", "git_kurz", "git_author", "git_author_email", "git_nachricht", "git_geaendert_am"):
            if feld in payload:
                updates[feld] = str(payload.get(feld, "")).strip()[:500]
        if "git_dirty" in payload:
            try:
                updates["git_dirty"] = max(int(payload["git_dirty"]), 0)
            except (TypeError, ValueError):
                updates["git_dirty"] = 0
        if "vorschau_url" in payload:
            try:
                updates["vorschau_url"] = _saubere_url(str(payload.get("vorschau_url", "")))
            except ValueError as exc:
                return {"ok": False, "error": str(exc)}, 400
        if any(key.startswith("git_") for key in updates):
            updates["git_status"] = "lokale_aenderungen" if updates.get("git_dirty") else "aktuell"
            updates["git_last_checked"] = jetzt()
            updates["git_fehler"] = ""

        updates["updated_at"] = jetzt()
        felder = ", ".join(f"{name} = ?" for name in updates)
        db.execute(f"UPDATE projekte SET {felder} WHERE id = ?", (*updates.values(), projekt_id))

        author_email = str(payload.get("git_author_email", "")).strip().lower()
        mitglied = None
        if author_email:
            mitglied = db.execute(
                "SELECT id FROM teammitglieder WHERE lower(email) = ? AND aktiv = 1", (author_email,)
            ).fetchone()
        agent = str(payload.get("agent", "Codex/Claude")).strip()[:80] or "Codex/Claude"
        notiz = str(payload.get("notiz", "")).strip()[:2000]
        teile = [notiz]
        if updates.get("git_kurz") or updates.get("git_nachricht"):
            teile.append(f"{updates.get('git_kurz', '')} · {updates.get('git_nachricht', '')}".strip(" ·"))
        if updates.get("aktuelle_aufgabe"):
            teile.append(f"Nächster Schritt: {updates['aktuelle_aufgabe']}")
        db.execute(
            """
            INSERT INTO aktivitaeten (projekt_id, teammitglied_id, aktion, text, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (projekt_id, mitglied["id"] if mitglied else None, f"Agent-Update · {agent}", "\n".join(filter(None, teile)), jetzt()),
        )
        db.commit()

        kunden_update = str(payload.get("kunden_update", "")).strip()[:3000]
        if kunden_update:
            ticket = db.execute(
                """
                SELECT kt.*, k.firma, k.ansprechpartner, k.email, k.telefon
                FROM kunden_tickets kt JOIN kunden k ON k.id = kt.kunde_id
                WHERE kt.kunde_id = ?
                """,
                (projekt["kunde_id"],),
            ).fetchone()
            if ticket:
                portal_nachricht_id = _portal_nachricht_anlegen(
                    db,
                    ticket["id"],
                    absender="team",
                    art="update",
                    text=kunden_update,
                    projekt_id=projekt_id,
                    teammitglied_id=mitglied["id"] if mitglied else None,
                )
                db.commit()
                _portal_mail(
                    db,
                    ticket,
                    nachricht_id=portal_nachricht_id,
                    richtung="an_kunde",
                    empfaenger=ticket["email"],
                    betreff=f"Neue Verbesserung in Ihrem Projektraum · {ticket['firma']}",
                    text=(
                        f"Guten Tag {ticket['ansprechpartner'] or ticket['firma']},\n\n"
                        f"Tomorrow Works hat eine neue Verbesserung veröffentlicht:\n\n{kunden_update}\n\n"
                        f"Zum Projektraum:\n{_portal_url(ticket)}\n\nTomorrow Works"
                    ),
                )
        return {
            "ok": True,
            "message": f"Dashboard-Projekt {projekt_id} wurde aktualisiert.",
            "projekt_id": projekt_id,
        }

    @app.post("/projekte/<int:projekt_id>/notiz")
    @login_required
    def projekt_notiz(projekt_id: int):
        _projekt_oder_404(projekt_id)
        text = request.form.get("text", "").strip()
        if not text:
            flash("Bitte eine Notiz eingeben.", "error")
        else:
            _aktivitaet(projekt_id, "Notiz", text)
            get_db().execute("UPDATE projekte SET updated_at = ? WHERE id = ?", (jetzt(), projekt_id))
            get_db().commit()
            flash("Notiz wurde gespeichert.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#aktivitaet")

    @app.post("/projekte/<int:projekt_id>/datei")
    @login_required
    def projekt_datei_hochladen(projekt_id: int):
        _projekt_oder_404(projekt_id)
        datei = request.files.get("datei")
        if not datei or not datei.filename:
            flash("Bitte eine Datei auswählen.", "error")
            return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#dateien")
        original = secure_filename(datei.filename)
        endung = original.rsplit(".", 1)[-1].lower() if "." in original else ""
        if not original or endung not in ERLAUBTE_DATEIEN:
            flash("Erlaubt sind PDF, PNG, JPG und WebP.", "error")
            return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#dateien")
        gespeichert = f"{projekt_id}_{uuid4().hex}.{endung}"
        datei.save(Path(app.config["UPLOAD_FOLDER"]) / gespeichert)
        db = get_db()
        db.execute(
            """
            INSERT INTO projekt_dateien
              (projekt_id, original_name, gespeichert_name, mimetype, hochgeladen_von, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (projekt_id, original, gespeichert, datei.mimetype or "application/octet-stream", g.user["id"], jetzt()),
        )
        _aktivitaet(projekt_id, "Datei hochgeladen", original)
        db.commit()
        flash("Datei wurde zum Projekt hinzugefügt.", "success")
        return redirect(url_for("projekt_detail", projekt_id=projekt_id) + "#dateien")

    @app.get("/dateien/<int:datei_id>")
    @login_required
    def projekt_datei(datei_id: int):
        datei = get_db().execute("SELECT * FROM projekt_dateien WHERE id = ?", (datei_id,)).fetchone()
        if datei is None:
            abort(404)
        return send_from_directory(
            app.config["UPLOAD_FOLDER"],
            datei["gespeichert_name"],
            as_attachment=request.args.get("download") == "1",
            download_name=datei["original_name"],
        )

    @app.get("/team")
    @login_required
    def team_liste():
        team = get_db().execute(
            """
            SELECT t.*, p.titel AS fokus_titel, k.firma AS fokus_kunde,
                   COUNT(DISTINCT pt.projekt_id) AS projektanzahl
            FROM teammitglieder t
            LEFT JOIN projekte p ON p.id = t.fokus_projekt_id
            LEFT JOIN kunden k ON k.id = p.kunde_id
            LEFT JOIN projekt_team pt ON pt.teammitglied_id = t.id
            GROUP BY t.id ORDER BY t.aktiv DESC, t.name
            """
        ).fetchall()
        return render_template("team.html", team=team)

    @app.route("/team/neu", methods=["GET", "POST"])
    @admin_required
    def team_neu():
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            passwort = request.form.get("passwort", "")
            if len(name) < 2 or "@" not in email or len(passwort) < 8:
                flash("Bitte Name, gültige E-Mail und ein Passwort mit mindestens 8 Zeichen eingeben.", "error")
            else:
                db = get_db()
                try:
                    db.execute(
                        """
                        INSERT INTO teammitglieder (name, email, rolle, password_hash, farbe, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            name,
                            email,
                            request.form.get("rolle", "team"),
                            generate_password_hash(passwort),
                            request.form.get("farbe", TEAM_FARBEN[0]),
                            jetzt(),
                        ),
                    )
                    db.commit()
                except sqlite3.IntegrityError:
                    flash("Diese E-Mail-Adresse ist bereits vergeben.", "error")
                else:
                    flash(f"{name} wurde zum Team hinzugefügt.", "success")
                    return redirect(url_for("team_liste"))
        return render_template("team_form.html", mitglied=None, team_farben=TEAM_FARBEN)

    @app.route("/team/<int:team_id>/bearbeiten", methods=["GET", "POST"])
    @admin_required
    def team_bearbeiten(team_id: int):
        db = get_db()
        mitglied = db.execute("SELECT * FROM teammitglieder WHERE id = ?", (team_id,)).fetchone()
        if mitglied is None:
            abort(404)
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            rolle = request.form.get("rolle", "team")
            aktiv = int(request.form.get("aktiv") == "1")
            if team_id == g.user["id"] and (rolle != "admin" or not aktiv):
                flash("Den eigenen aktiven Admin-Zugang kannst du nicht abschalten.", "error")
            else:
                passwort = request.form.get("passwort", "")
                felder = [name, email, rolle, request.form.get("farbe", TEAM_FARBEN[0]), aktiv, team_id]
                sql = "UPDATE teammitglieder SET name = ?, email = ?, rolle = ?, farbe = ?, aktiv = ? WHERE id = ?"
                if passwort:
                    if len(passwort) < 8:
                        flash("Das neue Passwort muss mindestens 8 Zeichen lang sein.", "error")
                        return render_template("team_form.html", mitglied=mitglied, team_farben=TEAM_FARBEN)
                    sql = "UPDATE teammitglieder SET name = ?, email = ?, rolle = ?, farbe = ?, aktiv = ?, password_hash = ? WHERE id = ?"
                    felder = [name, email, rolle, request.form.get("farbe", TEAM_FARBEN[0]), aktiv, generate_password_hash(passwort), team_id]
                try:
                    db.execute(sql, felder)
                    db.commit()
                except sqlite3.IntegrityError:
                    flash("Diese E-Mail-Adresse ist bereits vergeben.", "error")
                else:
                    flash("Teammitglied wurde aktualisiert.", "success")
                    return redirect(url_for("team_liste"))
        return render_template("team_form.html", mitglied=mitglied, team_farben=TEAM_FARBEN)


if __name__ == "__main__":
    app = create_app()
    port = int(app.config["DASHBOARD_PORT"])
    host = os.getenv("TW_DASHBOARD_HOST", "0.0.0.0")
    app.run(host=host, port=port, debug=os.getenv("FLASK_DEBUG") == "1")
