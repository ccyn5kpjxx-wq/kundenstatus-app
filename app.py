"""
Gärtner Karosserie & Lack — Autohaus-Terminportal
=================================================
Starten: python app.py
Admin:   http://localhost:5000/admin
Partner: http://localhost:5000/partner/<slug>
"""

from collections import defaultdict
import base64
from datetime import date, datetime
from difflib import SequenceMatcher
from functools import wraps
import hmac
import json
import mimetypes
import os
import pathlib
import re
import secrets
import shutil
import sqlite3
import time
import uuid

try:
    import psycopg
except Exception:
    psycopg = None

try:
    import cv2
except Exception:
    cv2 = None

try:
    import fitz
except Exception:
    fitz = None

try:
    import numpy as np
except Exception:
    np = None

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    from rapidocr_onnxruntime import RapidOCR
except Exception:
    RapidOCR = None

try:
    import requests
except Exception:
    requests = None

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:
    hashes = None
    serialization = None
    padding = None

from pypdf import PdfReader
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.utils import secure_filename


BASE = pathlib.Path(__file__).parent
DATA_DIR = BASE / "data"
DB = DATA_DIR / "auftraege.db"
UPLOAD_DIR = DATA_DIR / "uploads"


def load_env_file(path):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


for env_file in (BASE / ".env.local", BASE / ".env"):
    load_env_file(env_file)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))
if USE_POSTGRES:
    UPLOAD_DIR = pathlib.Path(os.environ.get("UPLOAD_DIR", "/tmp/kundenstatus-uploads"))
DEFAULT_ADMIN_PASS = "gaertner2026"
DEFAULT_FLASK_SECRET_KEY = "gaertner-autohaus-2026"
ADMIN_PASS = os.environ.get("ADMIN_PASS") or DEFAULT_ADMIN_PASS
DATE_FMT = "%d.%m.%Y"
DATETIME_FMT = "%d.%m.%Y %H:%M"
MAX_UPLOAD_MB = 25
OPENAI_EXTRACTION_MODEL = os.environ.get("OPENAI_EXTRACTION_MODEL", "gpt-4o")
OPENAI_API_URL = os.environ.get(
    "OPENAI_API_URL", "https://api.openai.com/v1/chat/completions"
)
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DOC_AI_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
GOOGLE_DOC_AI_TIMEOUT = 45
OPENAI_VISION_MAX_PAGES = 4
OPENAI_VISION_MAX_IMAGE_SIDE = 1800
CSRF_FIELD_NAME = "csrf_token"

GOOGLE_ACCESS_TOKEN = {"token": "", "expires_at": 0}

STATUSLISTE = {
    1: dict(key="angelegt", label="Angelegt", icon="📝", farbe="secondary"),
    2: dict(key="eingeplant", label="Eingeplant", icon="📅", farbe="primary"),
    3: dict(key="in_arbeit", label="In Arbeit", icon="🔧", farbe="info"),
    4: dict(key="fertig", label="Fertig", icon="✅", farbe="success"),
}

TRANSPORT_ARTEN = {
    "standard": {
        "label": "Kunde bringt und holt",
        "annahme_label": "Anlieferung",
        "abholung_label": "Abholung",
        "partner_annahme_label": "Fahrzeug wird von Ihnen angeliefert",
        "partner_abholung_label": "Fahrzeug wird von Ihnen geholt",
        "angebot_annahme_label": "Gewünschter Bringtermin",
        "angebot_abholung_label": "Gewünschter Holtermin",
    },
    "hol_und_bring": {
        "label": "Hol- und Bringservice",
        "annahme_label": "Abholung durch uns",
        "abholung_label": "Rückbringung",
        "partner_annahme_label": "Fahrzeug wird von uns geholt",
        "partner_abholung_label": "Fahrzeug wird von uns gebracht",
        "angebot_annahme_label": "Gewünschter Abholtermin",
        "angebot_abholung_label": "Gewünschter Bringtermin",
    },
}

PREISLISTE_LACKIERUNG = {
    "hinweis": (
        "Reine Lackierarbeiten. Montage- und Demontagearbeiten sind nicht enthalten. "
        "Diese Richtwerte gelten ausschließlich für die Lackierleistung."
    ),
    "positionen": {
        ("stossstange", "gebrauchtteil"): {
            "leistung": "Stoßstange lackieren (Gebrauchtteil)",
            "von": 220,
            "bis": 300,
        },
        ("stossstange", "neuteil"): {
            "leistung": "Stoßstange lackieren (Neuteil)",
            "von": 260,
            "bis": 340,
        },
        ("kotfluegel", "gebrauchtteil"): {
            "leistung": "Kotflügel lackieren (Gebrauchtteil)",
            "von": 160,
            "bis": 220,
        },
        ("kotfluegel", "neuteil"): {
            "leistung": "Kotflügel lackieren (Neuteil)",
            "von": 190,
            "bis": 250,
        },
        ("motorhaube", "gebrauchtteil"): {
            "leistung": "Motorhaube lackieren (Gebrauchtteil)",
            "von": 260,
            "bis": 340,
        },
        ("motorhaube", "neuteil"): {
            "leistung": "Motorhaube lackieren (Neuteil)",
            "von": 320,
            "bis": 420,
        },
        ("beilackieren", "standard"): {
            "leistung": "Beilackieren angrenzender Teile",
            "von": 80,
            "bis": 120,
        },
        ("neuwagenaufbereitung", "standard"): {
            "leistung": "Neuwagenaufbereitung komplett",
            "von": 90,
            "bis": 140,
        },
        ("gebrauchtwagenaufbereitung", "standard"): {
            "leistung": "Gebrauchtwagenaufbereitung komplett",
            "von": 150,
            "bis": 240,
        },
    },
}

EVENT_FELDER = (
    ("annahme_datum", "Anlieferung", "secondary"),
    ("start_datum", "Start", "primary"),
    ("fertig_datum", "Fertig", "warning"),
    ("abholtermin", "Abholung", "success"),
)

DOCUMENT_REVIEW_FIELDS = (
    ("fahrzeug", "Fahrzeug"),
    ("kennzeichen", "Kennzeichen"),
    ("fin_nummer", "FIN"),
    ("auftragsnummer", "Auftrag / Vorgang"),
    ("rep_max_kosten", "Rep.-Max.-Kosten"),
    ("bauteile_override", "Bauteile"),
    ("analyse_text", "Kurzanalyse"),
    ("beschreibung", "Beschreibung"),
    ("annahme_datum", "Annahme"),
    ("fertig_datum", "Fertig bis"),
)

WOCHENTAGE = {
    0: "Montag",
    1: "Dienstag",
    2: "Mittwoch",
    3: "Donnerstag",
    4: "Freitag",
    5: "Samstag",
    6: "Sonntag",
}

ALLOWED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".pdf",
    ".txt",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".heic",
}

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic"}

TEILE_PATTERNS = {
    "Stoßstange vorne": [r"(stoß|stoss)(fänger|stange).*(vorn|vorne|front)", r"frontschürze"],
    "Stoßstange hinten": [r"(stoß|stoss)(fänger|stange).*(hinten|heck)", r"heckschürze", r"\bladekante\b"],
    "Tür vorne links": [r"(fahrertür|vordertür links|tür vorne links)"],
    "Tür vorne rechts": [r"(beifahrertür|vordertür rechts|tür vorne rechts)"],
    "Tür hinten links": [r"(hintertür links|tür hinten links)"],
    "Tür hinten rechts": [r"(hintertür rechts|tür hinten rechts)"],
    "Kotflügel links": [r"(kotflügel|kotfluegel).*(links)"],
    "Kotflügel rechts": [r"(kotflügel|kotfluegel).*(rechts)"],
    "Kotflügel vorne links": [r"(kotflügel|kotfluegel).*(vorn|vorne).*(links)"],
    "Kotflügel vorne rechts": [r"(kotflügel|kotfluegel).*(vorn|vorne).*(rechts)"],
    "Radhausverbreiterung hinten links": [r"radhausverbreiterung.*hinten.*links"],
    "Radhausverbreiterung hinten rechts": [r"radhausverbreiterung.*hinten.*rechts"],
    "Radhausverbreiterung": [r"radhausverbreiterung"],
    "Heckklappe": [r"heckklappe", r"kofferraumklappe"],
    "Motorhaube": [r"motorhaube"],
    "Seitenteil links": [r"seitenteil.*links"],
    "Seitenteil rechts": [r"seitenteil.*rechts"],
    "Schweller links": [r"schweller.*links"],
    "Schweller rechts": [r"schweller.*rechts"],
    "Spiegel": [r"spiegel"],
    "Scheinwerfer": [r"scheinwerfer", r"frontlicht"],
    "Felge": [r"\bfelge\b", r"leichtmetallfelgen", r"stahlfelgen"],
    "Windschutzscheibe": [r"windschutzscheibe", r"frontscheibe"],
}

ARBEIT_PATTERNS = (
    ("smart repair", [r"smart\s*rep", r"smart\s*repair"]),
    ("instandsetzen", [r"instand", r"ausbeulen", r"richten", r"beule", r"eingedrückt", r"verformt"]),
    ("lackieren", [r"lack", r"kratzer", r"schramm", r"lackieren", r"lackschaden"]),
    ("ersetzen", [r"ersetzen", r"tauschen", r"gerissen", r"gebrochen", r"kaputt"]),
)

DOCUMENT_PATTERNS = (
    ("Lackierauftrag", [r"lackierauftrag"]),
    ("DEKRA-Gutachten", [r"dekra", r"schadengutachten", r"gutachten"]),
    ("DAT-Kalkulation", [r"\bdat\b", r"reparaturkosten-kalkulation", r"dat europa-code"]),
    ("TUEV-Bericht", [r"tuv", r"tüv", r"protokollnummer", r"besichtigungsdatum"]),
    ("Gutachten", [r"gutachten", r"sachverstaendigen", r"sachverständigen", r"bewertung"]),
    ("Reparaturauftrag", [r"reparaturauftrag", r"arbeitsauftrag", r"auftrag"]),
    ("Kostenvoranschlag", [r"kostenvoranschlag", r"kva", r"kostenvoranschlag"]),
    ("Rechnung", [r"rechnung", r"rechnungsnummer"]),
)

OCR_TEILE_PATTERNS = (
    ("Stoßstange vorne", [r"stossfaenger\s*[vy]\b", r"stossfaenger\s*vorn", r"frontschuerze"]),
    ("Stoßstange hinten", [r"stossfaenger\s*h\b", r"heckschuerze"]),
    ("Frontblech", [r"frontblech"]),
    ("Motorhaube", [r"haube", r"motorhaube"]),
    ("Heckklappe", [r"heckklappe"]),
    ("Radhausverbreiterung hinten links", [r"radhausverbreiterung.*hinten.*links"]),
    ("Radhausverbreiterung hinten rechts", [r"radhausverbreiterung.*hinten.*rechts"]),
    ("Radhausverbreiterung", [r"radhausverbreiterung"]),
    ("Kotflügel links", [r"kotfluegel\s+links\b", r"kotfluegel\s+l\b"]),
    ("Kotflügel rechts", [r"kotfluegel\s+rechts\b", r"kotfluegel\s+r\b"]),
    ("Kotflügel vorne links", [r"kotfluegel\s*l\b"]),
    ("Kotflügel vorne rechts", [r"kotfluegel\s*r\b"]),
    ("Tür vorne links", [r"tuer\s*vorn\s*l\b"]),
    ("Tür vorne rechts", [r"tuer\s*vorn\s*r\b"]),
    ("Tür hinten links", [r"tuer\s*hinten\s*l\b"]),
    ("Tür hinten rechts", [r"tuer\s*hinten\s*r\b"]),
    ("Seitenteil links", [r"seitenteil\s*l\b"]),
    ("Seitenteil rechts", [r"seitenteil\s*r\b"]),
    ("Einstieg links", [r"einstieg\s*l\b"]),
    ("Einstieg rechts", [r"einstieg\s*r\b"]),
    ("Spoiler vorne", [r"spoiler\s*v\b"]),
    ("Spoiler hinten", [r"spoiler\s*h\b"]),
    ("Abschlussblech", [r"abschlussblech"]),
    ("Dach", [r"\bdach\b"]),
    ("Außenspiegel", [r"aussenspiegel"]),
    ("Parksensor", [r"parksensor"]),
    ("Felge", [r"\bfelge\b"]),
    ("Schweller", [r"schweller"]),
    ("Lackstift", [r"lackstift"]),
)

LINE_ITEM_PART_PATTERNS = (
    ("Stoßstange vorne", [r"stossfaenger\s+v\b", r"stossf\s+v\b"]),
    ("Stoßstange hinten", [r"stossfaenger\s+h\b", r"stossf\s+h\b"]),
    ("Stoßstangenträger vorne", [r"stossfaengertraeger\s+v"]),
    ("Radhausverbreiterung hinten links", [r"radhausverbreiterung.*hinten.*links"]),
    ("Radhausverbreiterung hinten rechts", [r"radhausverbreiterung.*hinten.*rechts"]),
    ("Radhausverbreiterung", [r"radhausverbreiterung"]),
    ("Kotflügel links", [r"kotfluegel\s+links\b", r"kotfl\s+links\b"]),
    ("Kotflügel rechts", [r"kotfluegel\s+rechts\b", r"kotfl\s+rechts\b"]),
    ("Kotflügel vorne links", [r"kotfluegel\s+v\s+l\b", r"kotfl\s+v\s+l\b"]),
    ("Kotflügel vorne rechts", [r"kotfluegel\s+v\s+r\b", r"kotfl\s+v\s+r\b"]),
    ("Motorhaube", [r"motorhaube"]),
    ("Tür vorne links", [r"tuer\s+v\s+l\b", r"tuer\s+vorn\s+l\b"]),
    ("Tür vorne rechts", [r"tuer\s+v\s+r\b", r"tuer\s+vorn\s+r\b"]),
    ("Tür hinten links", [r"tuer\s+h\s+l\b", r"tuer\s+hinten\s+l\b"]),
    ("Tür hinten rechts", [r"tuer\s+h\s+r\b", r"tuer\s+hinten\s+r\b"]),
    ("A-Säule links", [r"a-saeule\s+a?\s*l\b"]),
    ("Schlossträger", [r"schlosstraeger"]),
    ("Radhausblech links", [r"radhausblech\s+a?\s*l\b"]),
    ("Halter Kotflügel vorne links", [r"halter\s+v\s+kotfl\s+v\s+l\b"]),
    ("Halter Kotflügel hinten links", [r"halter\s+v\s+kotfl\s+h\s+l\b"]),
    ("Scheinwerfer", [r"scheinwerfer"]),
    ("Spiegel", [r"spiegel"]),
    ("Schwellerverkleidung", [r"schwellerverkleidung"]),
)

OCR_IGNORED_LINE_PATTERNS = (
    r"^lackierauftrag$",
    r"^auftraggeber$",
    r"^lieferant$",
    r"^karosserieteil$",
    r"^bemerkung$",
    r"^abnahme$",
    r"^amtl\.?\s*kennzeichen",
    r"^auftrags",
    r"^typ:?$",
    r"^fg[-.\s]*nr",
    r"^farb[-.\s]*nr",
    r"^fertig bis",
    r"^audi$",
    r"^vw$",
    r"^gaertner$",
    r"^kaesmann$",
    r"^kasmann$",
    r"^[#\d./\s-]+$",
)

OCR_TABLE_PART_LABEL_PATTERNS = (
    r"^stossfaenger\s*[hvy]?$",
    r"^frontblech$",
    r"^haube$",
    r"^heckklappe$",
    r"^kotfluegel(\s+[lr])?$",
    r"^tuer\s+vorn(\s+[lr])?$",
    r"^tuer\s+hinten(\s+[lr1?])?$",
    r"^seitenteil(\s+[lr])?$",
    r"^einstieg(\s+[lr])?$",
    r"^spoiler(\s+[hv])?$",
    r"^abschlussblech$",
    r"^dach$",
    r"^aussenspiegel$",
    r"^parksensor$",
    r"^felge$",
    r"^schweller$",
    r"^lackstift$",
)

RAPID_OCR_ENGINE = None
TESSERACT_CMD = shutil.which("tesseract")


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or DEFAULT_FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


def get_csrf_token():
    token = session.get(CSRF_FIELD_NAME)
    if not token:
        token = secrets.token_urlsafe(32)
        session[CSRF_FIELD_NAME] = token
    return token


def csrf_field():
    return (
        f'<input type="hidden" name="{CSRF_FIELD_NAME}" '
        f'value="{get_csrf_token()}">'
    )


@app.context_processor
def inject_csrf_helpers():
    return {"csrf_token": get_csrf_token, "csrf_field": csrf_field}


@app.before_request
def protect_csrf():
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    expected = session.get(CSRF_FIELD_NAME)
    provided = request.form.get(CSRF_FIELD_NAME) or request.headers.get("X-CSRF-Token")
    if not expected or not provided or not hmac.compare_digest(expected, provided):
        abort(400)
    return None


@app.after_request
def add_csrf_fields(response):
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type.lower() or response.direct_passthrough:
        return response
    html = response.get_data(as_text=True)
    field = csrf_field()

    def inject_field(match):
        return f"{match.group(1)}\n    {field}"

    html = re.sub(
        r'(<form\b(?=[^>]*\bmethod\s*=\s*["\']?post["\']?)[^>]*>)',
        inject_field,
        html,
        flags=re.IGNORECASE,
    )
    response.set_data(html)
    return response


def get_startup_warnings():
    warnings = []
    if ADMIN_PASS in {"", "change-me", DEFAULT_ADMIN_PASS}:
        warnings.append(
            "ADMIN_PASS ist nicht sicher gesetzt. Bitte in .env.local ein eigenes Passwort eintragen."
        )
    if app.secret_key in {"", "change-me", DEFAULT_FLASK_SECRET_KEY}:
        warnings.append(
            "FLASK_SECRET_KEY ist nicht sicher gesetzt. Bitte in .env.local einen langen Zufallswert eintragen."
        )
    return warnings


def get_admin_pass():
    return clean_text(os.environ.get("ADMIN_PASS")) or clean_text(ADMIN_PASS) or DEFAULT_ADMIN_PASS


def clean_text(value):
    return str(value or "").strip()


def parse_date(value):
    if not value:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    for fmt in (DATE_FMT, "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def format_date(value):
    parsed = parse_date(value)
    if parsed:
        return parsed.strftime(DATE_FMT)
    return clean_text(value)


def iso_date(value):
    parsed = parse_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def now_str():
    return datetime.now().strftime(DATETIME_FMT)


def resolve_config_path(value):
    raw = clean_text(value)
    if not raw:
        return None
    candidate = pathlib.Path(raw)
    if not candidate.is_absolute():
        candidate = BASE / candidate
    return candidate


def get_ai_config():
    service_account_path = resolve_config_path(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        or os.environ.get("GOOGLE_DOC_AI_SERVICE_ACCOUNT_FILE")
    )
    google_ready = bool(
        requests is not None
        and hashes is not None
        and serialization is not None
        and padding is not None
        and service_account_path
        and service_account_path.exists()
        and clean_text(os.environ.get("GOOGLE_DOC_AI_PROJECT_ID"))
        and clean_text(os.environ.get("GOOGLE_DOC_AI_LOCATION"))
        and clean_text(os.environ.get("GOOGLE_DOC_AI_PROCESSOR_ID"))
    )
    openai_ready = bool(
        requests is not None and clean_text(os.environ.get("OPENAI_API_KEY"))
    )
    ready = google_ready and openai_ready
    return {
        "ready": ready,
        "google_ready": google_ready,
        "openai_ready": openai_ready,
        "service_account_path": str(service_account_path) if service_account_path else "",
        "env_file": str(BASE / ".env.local"),
        "google_project_id": clean_text(os.environ.get("GOOGLE_DOC_AI_PROJECT_ID")),
        "google_location": clean_text(os.environ.get("GOOGLE_DOC_AI_LOCATION")),
        "google_processor_id": clean_text(os.environ.get("GOOGLE_DOC_AI_PROCESSOR_ID")),
        "openai_model": clean_text(os.environ.get("OPENAI_EXTRACTION_MODEL"))
        or OPENAI_EXTRACTION_MODEL,
    }


def get_ai_status():
    config = get_ai_config()
    if config["ready"]:
        message = "Google Document AI und OpenAI sind verbunden."
    elif config["google_ready"] and not config["openai_ready"]:
        message = "Google OCR ist bereit, OpenAI fehlt noch."
    elif config["openai_ready"] and not config["google_ready"]:
        message = "OpenAI ist bereit, Google Document AI fehlt noch."
    else:
        message = "API-Zugangsdaten fehlen noch. Bis dahin bleibt die lokale OCR aktiv."
    return {
        "ready": config["ready"],
        "google_ready": config["google_ready"],
        "openai_ready": config["openai_ready"],
        "message": message,
        "env_file": config["env_file"],
        "openai_model": config["openai_model"],
        "service_account_path": config["service_account_path"],
    }


def b64url_encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def create_google_service_account_jwt(service_account_info):
    if hashes is None or serialization is None or padding is None:
        raise RuntimeError("cryptography ist nicht verfuegbar")
    header = {"alg": "RS256", "typ": "JWT"}
    now_ts = int(time.time())
    claims = {
        "iss": service_account_info["client_email"],
        "scope": GOOGLE_DOC_AI_SCOPE,
        "aud": GOOGLE_TOKEN_URL,
        "iat": now_ts,
        "exp": now_ts + 3600,
    }
    signing_input = (
        b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        + "."
        + b64url_encode(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
    )
    private_key = serialization.load_pem_private_key(
        service_account_info["private_key"].encode("utf-8"),
        password=None,
    )
    signature = private_key.sign(
        signing_input.encode("ascii"),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    return signing_input + "." + b64url_encode(signature)


def get_google_access_token():
    config = get_ai_config()
    if not config["google_ready"]:
        raise RuntimeError("Google Document AI ist noch nicht fertig konfiguriert")
    if GOOGLE_ACCESS_TOKEN["token"] and GOOGLE_ACCESS_TOKEN["expires_at"] > time.time() + 60:
        return GOOGLE_ACCESS_TOKEN["token"]

    service_account_info = json.loads(
        pathlib.Path(config["service_account_path"]).read_text(encoding="utf-8")
    )
    assertion = create_google_service_account_jwt(service_account_info)
    response = requests.post(
        GOOGLE_TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        },
        timeout=GOOGLE_DOC_AI_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    GOOGLE_ACCESS_TOKEN["token"] = clean_text(payload.get("access_token"))
    GOOGLE_ACCESS_TOKEN["expires_at"] = time.time() + int(payload.get("expires_in", 3600))
    return GOOGLE_ACCESS_TOKEN["token"]


def extract_text_with_google_document_ai(path, filename=""):
    config = get_ai_config()
    if not config["google_ready"]:
        return {"text": "", "source": "", "error": "Google Document AI nicht konfiguriert"}
    mime_type = mimetypes.guess_type(filename or str(path))[0] or "application/octet-stream"
    endpoint = (
        f"https://{config['google_location']}-documentai.googleapis.com/v1/"
        f"projects/{config['google_project_id']}/locations/{config['google_location']}/"
        f"processors/{config['google_processor_id']}:process"
    )
    token = get_google_access_token()
    payload = {
        "skipHumanReview": True,
        "rawDocument": {
            "mimeType": mime_type,
            "content": base64.b64encode(pathlib.Path(path).read_bytes()).decode("ascii"),
        },
    }
    response = requests.post(
        endpoint,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=GOOGLE_DOC_AI_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    text = clean_text(data.get("document", {}).get("text"))
    return {"text": text, "source": "google_document_ai", "error": ""}


def build_openai_document_schema():
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "document_type": {"type": "string"},
            "vehicle_type": {"type": "string"},
            "fin_nummer": {"type": "string"},
            "auftragsnummer": {"type": "string"},
            "kennzeichen": {"type": "string"},
            "auftrags_datum": {"type": "string"},
            "fertig_bis": {"type": "string"},
            "rep_max_kosten": {"type": "string"},
            "farbnummer": {"type": "string"},
            "offene_bauteile": {"type": "array", "items": {"type": "string"}},
            "erledigte_bauteile": {"type": "array", "items": {"type": "string"}},
            "kurzanalyse": {"type": "string"},
            "lesefassung": {"type": "string"},
            "confidence": {"type": "number"},
            "needs_review": {"type": "boolean"},
            "review_reason": {"type": "string"},
        },
        "required": [
            "document_type",
            "vehicle_type",
            "fin_nummer",
            "auftragsnummer",
            "kennzeichen",
            "auftrags_datum",
            "fertig_bis",
            "rep_max_kosten",
            "farbnummer",
            "offene_bauteile",
            "erledigte_bauteile",
            "kurzanalyse",
            "lesefassung",
            "confidence",
            "needs_review",
            "review_reason",
        ],
    }


def extract_openai_response_json(data):
    if not isinstance(data, dict):
        return {}
    choices = data.get("choices") or []
    if not choices:
        return {}
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        try:
            return json.loads(content)
        except Exception:
            return {}
    if isinstance(content, list):
        fragments = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                fragments.append(item.get("text", ""))
        joined = clean_text(" ".join(fragments))
        if joined:
            try:
                return json.loads(joined)
            except Exception:
                return {}
    return {}


def encode_openai_image_data_url(image_bytes, mime_type):
    if not image_bytes:
        return ""
    mime_type = clean_text(mime_type) or "image/jpeg"
    return "data:{};base64,{}".format(
        mime_type,
        base64.b64encode(image_bytes).decode("ascii"),
    )


def prepare_openai_image_bytes(path):
    raw = pathlib.Path(path).read_bytes()
    if cv2 is None or np is None:
        return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"
    try:
        image = cv2.imdecode(np.frombuffer(raw, dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"
        height, width = image.shape[:2]
        longest = max(height, width)
        if longest > OPENAI_VISION_MAX_IMAGE_SIDE:
            scale = OPENAI_VISION_MAX_IMAGE_SIDE / longest
            image = cv2.resize(
                image,
                (max(1, int(width * scale)), max(1, int(height * scale))),
                interpolation=cv2.INTER_AREA,
            )
        ok, encoded = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), 88])
        if ok:
            return encoded.tobytes(), "image/jpeg"
    except Exception:
        pass
    return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"


def build_openai_visual_inputs(path, filename=""):
    suffix = pathlib.Path(filename or str(path)).suffix.lower()
    path = pathlib.Path(path)
    inputs = []

    if suffix in IMAGE_EXTENSIONS:
        image_bytes, mime_type = prepare_openai_image_bytes(path)
        data_url = encode_openai_image_data_url(image_bytes, mime_type)
        if data_url:
            inputs.append(
                {
                    "type": "image_url",
                    "image_url": {"url": data_url, "detail": "high"},
                }
            )
        return inputs

    if suffix == ".pdf" and fitz is not None:
        try:
            doc = fitz.open(str(path))
            max_pages = min(doc.page_count, OPENAI_VISION_MAX_PAGES)
            for page_index in range(max_pages):
                page = doc.load_page(page_index)
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                data_url = encode_openai_image_data_url(pix.tobytes("png"), "image/png")
                if data_url:
                    inputs.append(
                        {
                            "type": "image_url",
                            "image_url": {"url": data_url, "detail": "high"},
                        }
                    )
        except Exception:
            return inputs
    return inputs


def extract_structured_data_with_openai(filename, ocr_text, local_text="", visual_inputs=None):
    config = get_ai_config()
    if not config["openai_ready"]:
        return {"data": {}, "error": "OpenAI ist noch nicht konfiguriert"}

    schema = build_openai_document_schema()
    user_prompt = "\n".join(
        [
            "Analysiere den folgenden Werkstattbeleg fuer einen Karosserie- und Lackbetrieb.",
            "Gib NUR JSON gemaess Schema zurueck.",
            "Wichtig:",
            "- OCR kann fehlerhaft sein.",
            "- Wenn ein Bild/PDF sichtbar ist, nutze vorrangig das Originalbild und nicht nur OCR-Text.",
            "- Lies Fahrzeugtyp, Kennzeichen, FIN, Auftragsnummer, Termine und angekreuzte/markierte Bauteile direkt aus Formularfeldern und Tabellen.",
            "- Bei Lackierauftrag-Formularen sind Felder wie Typ, FG-Nr, Amtl. Kennzeichen, Auftrags-Nr, Fertig bis und angekreuzte Teile wichtig.",
            "- Bei handschriftlich erledigten Positionen diese in erledigte_bauteile aufnehmen und NICHT in offene_bauteile.",
            "- In offene_bauteile nur Positionen aufnehmen, die noch fuer unsere Werkstatt relevant sind.",
            "- Sichtbare Werte immer eintragen, auch wenn sie unsicher sind.",
            "- Wenn ein Wert unsicher ist, den erkannten Wert trotzdem eintragen, needs_review auf true setzen und review_reason mit 'Bitte überprüfen' beginnen.",
            "- Datumsformat immer TT.MM.JJJJ.",
            "- Wenn ein Fertig-bis-Datum vor dem Auftragsdatum liegt, den sichtbaren Wert trotzdem eintragen und zur Pruefung markieren.",
            "- Wenn Originalbilder angehaengt sind, pruefe die sichtbaren Tabellen, Haekchen, handschriftlichen Notizen und Datumsfelder direkt im Bild.",
            "- Bei Lackierauftraegen sind Auftrags-Nr., Typ, FG-Nr., Farb-Nr., Fertig-bis-Datum, angekreuzte Karosserieteile und Bemerkungen entscheidend.",
            "",
            f"Dateiname: {filename}",
            "",
            "[Google OCR / Haupttext]",
            clean_text(ocr_text) or "-",
            "",
            "[Lokale OCR / Vergleich]",
            clean_text(local_text) or "-",
        ]
    )
    user_content = [{"type": "text", "text": user_prompt}]
    for visual_input in visual_inputs or []:
        user_content.append(visual_input)

    payload = {
        "model": config["openai_model"],
        "messages": [
            {
                "role": "system",
                "content": (
                    "Du extrahierst strukturierte Felder aus deutschen Werkstattbelegen. "
                    "Arbeite vorsichtig und halluziniere nicht. Wenn ein Wert sichtbar, aber unsicher ist, "
                    "gib ihn trotzdem zur menschlichen Pruefung zurueck."
                ),
            },
            {"role": "user", "content": user_content},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "document_extraction",
                "strict": True,
                "schema": schema,
            },
        },
    }
    response = requests.post(
        OPENAI_API_URL,
        headers={
            "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=GOOGLE_DOC_AI_TIMEOUT,
    )
    response.raise_for_status()
    parsed = extract_openai_response_json(response.json())
    return {"data": parsed, "error": ""}


def normalize_openai_document_data(data):
    if not data:
        return {}
    offene_bauteile = [
        compact_whitespace(item)
        for item in data.get("offene_bauteile", [])
        if clean_text(item)
    ]
    analyse = clean_text(data.get("kurzanalyse"))
    if not analyse and offene_bauteile:
        analyse = ", ".join(offene_bauteile)[:220]
    beschreibung = clean_text(data.get("lesefassung"))
    if not beschreibung:
        description_parts = []
        if clean_text(data.get("document_type")):
            description_parts.append(clean_text(data["document_type"]))
        if clean_text(data.get("vehicle_type")):
            description_parts.append(f"Typ {clean_text(data['vehicle_type'])}")
        if analyse:
            description_parts.append(f"Arbeit {analyse}")
        if clean_text(data.get("rep_max_kosten")):
            description_parts.append(
                f"Rep.-Max.-Kosten {clean_text(data['rep_max_kosten'])}"
            )
        beschreibung = ". ".join(description_parts)
    fields = {
        "fahrzeug": clean_text(data.get("vehicle_type")),
        "fin_nummer": clean_text(data.get("fin_nummer")).upper(),
        "auftragsnummer": clean_text(data.get("auftragsnummer")),
        "kennzeichen": clean_text(data.get("kennzeichen")).upper(),
        "annahme_datum": format_date(clean_text(data.get("auftrags_datum"))),
        "fertig_datum": format_date(clean_text(data.get("fertig_bis"))),
        "rep_max_kosten": clean_text(data.get("rep_max_kosten")),
        "analyse_text": analyse[:220],
        "beschreibung": beschreibung[:900],
        "bauteile_override": "\n".join(offene_bauteile),
        "analyse_pruefen": 1 if data.get("needs_review") else 0,
        "analyse_hinweis": clean_text(data.get("review_reason")),
        "analyse_confidence": float(data.get("confidence") or 0),
    }
    return quality_check_document_fields(fields)


def add_analysis_note(fields, note):
    note = clean_text(note)
    if not note:
        return fields
    existing = clean_text(fields.get("analyse_hinweis"))
    if note not in existing:
        fields["analyse_hinweis"] = f"{existing} {note}".strip()
    fields["analyse_pruefen"] = 1
    return fields


def values_disagree(left, right, is_date=False):
    left_text = clean_text(left)
    right_text = clean_text(right)
    if not left_text or not right_text:
        return False
    if is_date:
        left_date = parse_date(left_text)
        right_date = parse_date(right_text)
        if left_date and right_date:
            return left_date != right_date
    return normalize_document_text(left_text) != normalize_document_text(right_text)


def has_extracted_document_values(fields):
    for key in (
        "fahrzeug",
        "fin_nummer",
        "auftragsnummer",
        "kennzeichen",
        "annahme_datum",
        "fertig_datum",
        "rep_max_kosten",
        "analyse_text",
        "beschreibung",
        "bauteile_override",
    ):
        if clean_text((fields or {}).get(key)):
            return True
    return False


def quality_check_document_fields(fields):
    fields = dict(fields or {})
    if has_extracted_document_values(fields):
        add_analysis_note(
            fields,
            "Bitte überprüfen: Die Daten wurden automatisch aus der Unterlage übernommen.",
        )

    confidence = float(fields.get("analyse_confidence") or 0)
    if confidence and confidence < 0.72:
        add_analysis_note(
            fields,
            "Die automatische Erkennung war nicht sicher genug. Bitte die Felder kurz gegen die Originaldatei prüfen.",
        )

    beschreibung_norm = normalize_document_text(fields.get("beschreibung"))
    is_lackierauftrag = "lackierauftrag" in beschreibung_norm
    has_work = clean_text(fields.get("analyse_text")) or clean_text(fields.get("bauteile_override"))

    if not clean_text(fields.get("fahrzeug")):
        add_analysis_note(fields, "Fahrzeugtyp konnte nicht sicher erkannt werden.")
    if not has_work:
        add_analysis_note(fields, "Reparaturpositionen/Bauteile konnten nicht sicher erkannt werden.")
    if is_lackierauftrag and not clean_text(fields.get("auftragsnummer")):
        add_analysis_note(fields, "Auftragsnummer aus dem Lackierauftrag fehlt oder ist unsicher.")
    if is_lackierauftrag and not clean_text(fields.get("fertig_datum")):
        add_analysis_note(fields, "Fertig-bis-Datum aus dem Lackierauftrag fehlt oder ist unsicher.")

    for key, label in (
        ("annahme_datum", "Auftrags-/Annahmedatum"),
        ("fertig_datum", "Fertig-bis-Datum"),
    ):
        value = clean_text(fields.get(key))
        if value and not parse_date(value):
            add_analysis_note(fields, f"{label} wurde nicht als gültiges Datum erkannt.")

    annahme = parse_date(fields.get("annahme_datum"))
    fertig = parse_date(fields.get("fertig_datum"))
    if annahme and fertig and fertig < annahme:
        add_analysis_note(
            fields,
            "Bitte überprüfen: Das Fertig-bis-Datum liegt vor dem Auftrags-/Annahmedatum.",
        )

    return fields


def build_document_analysis_bundle(path, filename=""):
    local_text = extract_document_text_local(path, filename)
    bundle = {
        "text": local_text,
        "source": "local_ocr" if local_text else "",
        "status": "fallback",
        "hint": "",
        "structured": {},
        "analysis_json": "",
    }

    google_result = {"text": "", "source": "", "error": ""}
    try:
        google_result = extract_text_with_google_document_ai(path, filename)
    except Exception as exc:
        google_result["error"] = str(exc)

    preferred_text = clean_text(google_result.get("text")) or clean_text(local_text)
    if google_result.get("text") and local_text and clean_text(google_result["text"]) != clean_text(local_text):
        preferred_text = (
            "[Google OCR]\n"
            + clean_text(google_result["text"])
            + "\n\n[Lokale OCR]\n"
            + clean_text(local_text)
        )
    if preferred_text:
        bundle["text"] = preferred_text
    if google_result.get("text"):
        bundle["source"] = "google_document_ai"
        bundle["status"] = "ocr_ready"

    ai_result = {"data": {}, "error": ""}
    try:
        visual_inputs = build_openai_visual_inputs(path, filename)
        ai_result = extract_structured_data_with_openai(
            filename,
            google_result.get("text") or local_text,
            local_text,
            visual_inputs,
        )
    except Exception as exc:
        ai_result["error"] = str(exc)

    structured = normalize_openai_document_data(ai_result.get("data"))
    if structured:
        bundle["structured"] = structured
        bundle["analysis_json"] = json.dumps(ai_result.get("data"), ensure_ascii=False)
        bundle["status"] = "ai_ready"
        if google_result.get("text") and visual_inputs:
            bundle["source"] = "google_document_ai+openai_vision"
        elif google_result.get("text"):
            bundle["source"] = "google_document_ai+openai"
        elif visual_inputs:
            bundle["source"] = "local_ocr+openai_vision"
        else:
            bundle["source"] = "local_ocr+openai"
        if structured.get("analyse_pruefen"):
            bundle["hint"] = structured.get("analyse_hinweis") or "Bitte kurz pruefen"
            bundle["status"] = "review"
    elif ai_result.get("error"):
        bundle["hint"] = ai_result["error"]
    elif google_result.get("error") and not local_text:
        bundle["hint"] = google_result["error"]

    config = get_ai_config()
    if not bundle["hint"] and not (config["openai_ready"] or config["google_ready"]):
        bundle["hint"] = "API-Konfiguration fehlt, lokale OCR bleibt aktiv"
    return bundle


def day_label(day_value):
    return f"{WOCHENTAGE[day_value.weekday()]}, {day_value.strftime(DATE_FMT)}"


def allowed_file(filename):
    return pathlib.Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def analyse_text(text):
    original = clean_text(text)
    if not original:
        return ""

    lowered = original.lower()
    normalized = normalize_document_text(original)
    teile = []
    for label, patterns in TEILE_PATTERNS.items():
        if matches_any_pattern(patterns, lowered, normalized):
            teile.append(label)

    arbeiten = []
    for label, patterns in ARBEIT_PATTERNS:
        if matches_any_pattern(patterns, lowered, normalized):
            arbeiten.append(label)

    if teile:
        suffix = "/".join(arbeiten[:2]) if arbeiten else "prüfen"
        return ", ".join(f"{teil} {suffix}" for teil in teile)

    lines = [
        re.sub(r"\s+", " ", part).strip(" ,.;:-")
        for part in re.split(r"[.\n]+", original)
        if clean_text(part)
    ]
    return "; ".join(lines[:2])[:220]


def get_part_patterns(label):
    patterns = list(TEILE_PATTERNS.get(label, []))
    for other_label, other_patterns in OCR_TEILE_PATTERNS:
        if other_label == label:
            patterns.extend(other_patterns)
    for other_label, other_patterns in LINE_ITEM_PART_PATTERNS:
        if other_label == label:
            patterns.extend(other_patterns)
    return patterns


def normalize_pattern_text(pattern):
    return (
        pattern.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


def matches_any_pattern(patterns, *texts):
    for pattern in patterns:
        variants = (pattern, normalize_pattern_text(pattern))
        for text in texts:
            if text and any(re.search(variant, text) for variant in variants):
                return True
    return False


def extract_affected_parts(text):
    original = clean_text(text)
    if not original:
        return []
    lowered = original.lower()
    normalized = normalize_document_text(original)
    found = []
    for label, patterns in TEILE_PATTERNS.items():
        if matches_any_pattern(patterns, lowered, normalized):
            found.append(label)
    for label, patterns in OCR_TEILE_PATTERNS:
        if label not in found and matches_any_pattern(patterns, lowered, normalized):
            found.append(label)
    for label, patterns in LINE_ITEM_PART_PATTERNS:
        if label not in found and matches_any_pattern(patterns, lowered, normalized):
            found.append(label)
    return remove_less_specific_part_labels(found)


def remove_less_specific_part_labels(parts):
    normalized_parts = [normalize_document_text(part) for part in parts or []]
    result = []
    for part, part_norm in zip(parts or [], normalized_parts):
        if not part_norm:
            continue
        if any(other != part_norm and other.startswith(f"{part_norm} ") for other in normalized_parts):
            continue
        result.append(part)
    return result


def build_customer_part_summaries(auftrag):
    manual_override = parse_manual_parts(auftrag.get("bauteile_override"))
    if manual_override:
        return [{"teil": teil, "arbeiten": []} for teil in manual_override]

    source = " ".join(
        part
        for part in [clean_text(auftrag.get("beschreibung")), clean_text(auftrag.get("analyse_text"))]
        if part
    ).strip()
    if not source:
        return []
    parts = extract_affected_parts(source)
    if not parts:
        return []

    fragments = [
        compact_whitespace(fragment)
        for fragment in re.split(r"[.\n;,]+", source)
        if clean_text(fragment)
    ]
    summaries = []
    for part in parts:
        actions = []
        patterns = get_part_patterns(part)
        for fragment in fragments:
            lowered_fragment = fragment.lower()
            normalized_fragment = normalize_document_text(fragment)
            if not matches_any_pattern(patterns, lowered_fragment, normalized_fragment):
                continue
            smart_repair_fragment = matches_any_pattern(
                [r"smart\s*repair", r"smart\s*rep", r"\bsr\b"],
                lowered_fragment,
                normalized_fragment,
            )
            for action_label, action_patterns in ARBEIT_PATTERNS:
                if (
                    action_label == "lackieren"
                    and smart_repair_fragment
                    and not re.search(r"lack|schleif|polier", normalized_fragment)
                ):
                    continue
                if action_label not in actions and matches_any_pattern(
                    action_patterns, lowered_fragment, normalized_fragment
                ):
                    actions.append(action_label)
        summaries.append(
            {
                "teil": part,
                "arbeiten": actions[:2],
            }
        )
    return summaries


def parse_price_amount(value):
    text = normalize_document_text(value)
    match = re.search(r"([0-9]{1,6})(?:[.,][0-9]{2})?", text)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def format_euro(value):
    if not value:
        return ""
    return f"{int(value)} € netto"


def format_price_range(von, bis):
    if von and bis and von != bis:
        return f"{int(von)} - {int(bis)} € netto"
    if von:
        return format_euro(von)
    return ""


def get_price_part_family(part):
    normalized = normalize_document_text(part)
    if any(term in normalized for term in ("stossfaenger", "stossstange")):
        return "stossstange"
    if "kotfluegel" in normalized:
        return "kotfluegel"
    if "motorhaube" in normalized or re.search(r"\bhaube\b", normalized):
        return "motorhaube"
    return ""


def get_price_part_condition(source):
    normalized = normalize_document_text(source)
    if re.search(r"\b(neuteil|neu teil|neues teil)\b", normalized):
        return "neuteil"
    return "gebrauchtteil"


def build_price_suggestion(auftrag):
    source = " ".join(
        part
        for part in (
            clean_text(auftrag.get("analyse_text")),
            clean_text(auftrag.get("beschreibung")),
            clean_text(auftrag.get("bauteile_override")),
        )
        if part
    )
    normalized_source = normalize_document_text(source)
    parts = auftrag.get("kunden_bauteile") or build_customer_part_summaries(auftrag)
    condition = get_price_part_condition(source)
    positionen = []
    hinweise = []
    seen = set()

    for item in parts:
        teil = clean_text(item.get("teil"))
        arbeiten = [normalize_document_text(arbeit) for arbeit in item.get("arbeiten", [])]
        family = get_price_part_family(teil)
        if not family:
            continue
        has_lackieren = "lackieren" in arbeiten or bool(
            re.search(rf"{re.escape(normalize_document_text(teil))}.*lackier", normalized_source)
        )
        has_smart_repair = any("smart repair" in arbeit for arbeit in arbeiten) or bool(
            re.search(rf"{re.escape(normalize_document_text(teil))}.*smart repair", normalized_source)
        )
        if has_lackieren:
            key = (family, condition)
            if key in PREISLISTE_LACKIERUNG["positionen"]:
                preis = PREISLISTE_LACKIERUNG["positionen"][key]
                entry_key = (key, teil)
                if entry_key not in seen:
                    seen.add(entry_key)
                    positionen.append(
                        {
                            "teil": teil,
                            "leistung": preis["leistung"],
                            "von": preis["von"],
                            "bis": preis["bis"],
                            "richtwert": format_price_range(preis["von"], preis["bis"]),
                        }
                    )
        elif has_smart_repair:
            hinweise.append(f"Für {teil} Smart Repair ist noch kein fester Richtwert hinterlegt.")

    for family in ("beilackieren", "neuwagenaufbereitung", "gebrauchtwagenaufbereitung"):
        if family not in normalized_source:
            continue
        key = (family, "standard")
        preis = PREISLISTE_LACKIERUNG["positionen"][key]
        if key not in seen:
            seen.add(key)
            positionen.append(
                {
                    "teil": "",
                    "leistung": preis["leistung"],
                    "von": preis["von"],
                    "bis": preis["bis"],
                    "richtwert": format_price_range(preis["von"], preis["bis"]),
                }
            )

    rep_max = parse_price_amount(auftrag.get("rep_max_kosten"))
    if rep_max:
        hinweise.append(f"Rep.-Max.-Kosten aus Unterlage: {format_euro(rep_max)}.")

    if "smart repair" in normalized_source and not any("Smart Repair" in hinweis for hinweis in hinweise):
        hinweise.append("Smart-Repair-Arbeiten bitte separat prüfen, wenn sie nicht in der Preisliste stehen.")

    total_von = sum(item["von"] for item in positionen)
    total_bis = sum(item["bis"] for item in positionen)
    empfehlung = 0
    if total_von or total_bis:
        empfehlung = int(round(((total_von + total_bis) / 2) / 10) * 10)
        if rep_max and empfehlung > rep_max:
            empfehlung = rep_max
            hinweise.append("Empfehlung wurde auf die Rep.-Max.-Kosten begrenzt.")

    return {
        "hat_vorschlag": bool(positionen),
        "positionen": positionen,
        "hinweise": list(dict.fromkeys(hinweise)),
        "richtwert": format_price_range(total_von, total_bis),
        "empfehlung": format_euro(empfehlung),
        "hinweis_preisliste": PREISLISTE_LACKIERUNG["hinweis"],
    }


def parse_manual_parts(value):
    parts = []
    seen = set()
    for fragment in re.split(r"[\n,;]+", clean_text(value)):
        teil = compact_whitespace(fragment).strip(" -")
        if not teil:
            continue
        key = teil.lower()
        if key in seen:
            continue
        seen.add(key)
        parts.append(teil)
    return parts


def compact_whitespace(text):
    return re.sub(r"\s+", " ", clean_text(text))


OFFER_TEXT_REPLACEMENTS = (
    (r"\bstostange\b", "Stoßstange"),
    (r"\bstosstange\b", "Stoßstange"),
    (r"\bstossstange\b", "Stoßstange"),
    (r"\bstossfanger\b", "Stoßfänger"),
    (r"\bstossfanger\b", "Stoßfänger"),
    (r"\bstosfanger\b", "Stoßfänger"),
    (r"\bfahrertuer\b", "Fahrertür"),
    (r"\bbeifahrertuer\b", "Beifahrertür"),
    (r"\bsmart\s*rep\b", "Smart Repair"),
    (r"\bsmartrepair\b", "Smart Repair"),
    (r"\blackierenen\b", "lackieren"),
    (r"\bverkratz\b", "verkratzt"),
    (r"\bbilder im anhang\b", "Bilder im Anhang"),
)


def beautify_offer_text(text):
    value = compact_whitespace(text)
    if not value:
        return ""
    value = re.sub(r"^kunde schreibt:\s*", "", value, flags=re.IGNORECASE)
    for pattern, replacement in OFFER_TEXT_REPLACEMENTS:
        value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
    value = re.sub(r"\s*([,;:])\s*", r"\1 ", value)
    value = re.sub(r"\s*\.\s*", ". ", value)
    value = compact_whitespace(value)
    parts = [part.strip(" -") for part in re.split(r"(?:\n+|(?<=[.!?])\s+)", value) if clean_text(part)]
    formatted = []
    for part in parts:
        sentence = part[0].upper() + part[1:] if part and part[0].islower() else part
        if sentence and sentence[-1] not in ".!?":
            sentence += "."
        formatted.append(sentence)
    return " ".join(formatted)[:700]


def is_generated_offer_text(text):
    value = compact_whitespace(text)
    if not value:
        return False
    normalized = normalize_document_text(value)
    return bool(
        normalized.startswith("automatisch aus datei erkannt")
        or normalized.startswith("dateihinweis")
        or normalized.startswith("schadensmeldung:")
        or normalized.startswith("lackierauftrag")
        or ("auftrags-nr" in normalized and ("fg.-nr" in normalized or "vin" in normalized))
    )


def build_offer_texts(customer_short, customer_long, doc_analysis, doc_description):
    short_text = "" if is_generated_offer_text(customer_short) else beautify_offer_text(customer_short)
    long_text = "" if is_generated_offer_text(customer_long) else beautify_offer_text(customer_long)
    doc_short = beautify_offer_text(doc_analysis)
    doc_long = beautify_offer_text(doc_description)

    final_short = short_text or doc_short or analyse_text(long_text)
    if short_text and doc_short and short_text.lower() != doc_short.lower():
        final_short = f"{short_text} / Datei: {doc_short}"[:220]

    description_parts = []
    if long_text:
        description_parts.append(f"Kundentext: {long_text}")
    elif short_text:
        description_parts.append(f"Schadensmeldung: {short_text}")
    if doc_short:
        description_parts.append(f"Automatisch aus Datei erkannt: {doc_short}")
    elif doc_long and doc_long.lower() not in " ".join(description_parts).lower():
        description_parts.append(f"Dateihinweis: {doc_long}")
    final_long = " ".join(dict.fromkeys(part for part in description_parts if part)).strip()[:900]
    return final_short[:220], final_long


def normalize_document_text(text):
    value = clean_text(text).lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "spatestens": "spaetestens",
        "stobtanger": "stossfaenger",
        "stobtfanger": "stossfaenger",
        "stolbfanger": "stossfaenger",
        "stolfanger": "stossfaenger",
        "stosfanger": "stossfaenger",
        "stobfangerv": "stossfaenger v",
        "stobfangery": "stossfaenger v",
        "kotfligel": "kotfluegel",
        "kotflugel": "kotfluegel",
        "tur ": "tuer ",
        "tor ": "tuer ",
        "au8enspiegel": "aussenspiegel",
        "aubenspiegel": "aussenspiegel",
    }
    for source, target in replacements.items():
        value = value.replace(source, target)
    value = re.sub(r"\bsmart\s*rep\b", "smart repair", value)
    value = re.sub(r"(?<![a-z])sr(?![a-z])", "smart repair", value)
    value = re.sub(r"sto\w{0,4}fanger", "stossfaenger", value)
    value = re.sub(r"\bstossfaenger([hvy])\b", r"stossfaenger \1", value)
    value = re.sub(r"[^a-z0-9#./:\-\n\s\[\]]+", " ", value)
    return compact_whitespace(value).replace(" \n ", "\n")


def get_rapid_ocr():
    global RAPID_OCR_ENGINE
    if RAPID_OCR_ENGINE is None and RapidOCR is not None:
        RAPID_OCR_ENGINE = RapidOCR()
    return RAPID_OCR_ENGINE


def load_image_for_ocr(path):
    if cv2 is None or np is None:
        return None
    try:
        buffer = np.fromfile(path, dtype=np.uint8)
        if buffer.size == 0:
            return None
        image = cv2.imdecode(buffer, cv2.IMREAD_COLOR)
        return preprocess_cv_image(image)
    except Exception:
        return None


def preprocess_cv_image(image):
    if cv2 is None or image is None:
        return image
    try:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        scaled = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
        _, threshold = cv2.threshold(
            scaled, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )
        return threshold
    except Exception:
        return image


def extract_lines_from_ocr_result(result):
    lines = []
    for item in result or []:
        if len(item) < 3:
            continue
        text = clean_text(item[1])
        try:
            score = float(item[2])
        except (TypeError, ValueError):
            score = 0.0
        if text and score >= 0.35:
            lines.append(text)
    return lines


def extract_ocr_lines(source):
    best_lines = []
    ocr_engine = get_rapid_ocr()
    if ocr_engine is not None:
        candidates = [source]
        if isinstance(source, str):
            prepared = load_image_for_ocr(source)
            if prepared is not None:
                candidates.append(prepared)
        else:
            prepared = preprocess_cv_image(source)
            if prepared is not None:
                candidates.append(prepared)
        for candidate in candidates:
            if candidate is None:
                continue
            try:
                result, _ = ocr_engine(candidate)
            except Exception:
                continue
            lines = extract_lines_from_ocr_result(result)
            if len(lines) > len(best_lines):
                best_lines = lines
    return best_lines


def extract_image_text(path):
    best_lines = extract_ocr_lines(str(path))

    if not best_lines and pytesseract is not None and TESSERACT_CMD:
        try:
            pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
            image = load_image_for_ocr(str(path))
            if image is not None:
                text = pytesseract.image_to_string(image, lang="deu+eng")
                best_lines = [
                    clean_text(line) for line in text.splitlines() if clean_text(line)
                ]
        except Exception:
            best_lines = []

    return "\n".join(best_lines)


def extract_pdf_text(path):
    text_chunks = []
    try:
        reader = PdfReader(str(path))
    except Exception:
        reader = None

    if reader is not None:
        for page in reader.pages[:20]:
            try:
                page_text = page.extract_text() or ""
            except Exception:
                continue
            if clean_text(page_text):
                text_chunks.append(page_text)

    direct_text = "\n".join(clean_text(chunk) for chunk in text_chunks if clean_text(chunk))
    needs_ocr = len(compact_whitespace(direct_text)) < 1200

    # Gescannte PDFs und bildlastige PDFs zusätzlich per OCR lesen.
    if needs_ocr and fitz is not None:
        try:
            doc = fitz.open(str(path))
            max_pages = min(doc.page_count, 4)
            for page_index in range(max_pages):
                try:
                    page = doc.load_page(page_index)
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                    if cv2 is None or np is None:
                        continue
                    image = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                        pix.height, pix.width, pix.n
                    )
                    if pix.n == 4:
                        image = cv2.cvtColor(image, cv2.COLOR_RGBA2BGR)
                    elif pix.n == 3:
                        image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
                    lines = extract_ocr_lines(image)
                    if lines:
                        text_chunks.append("\n".join(lines))
                except Exception:
                    continue
        except Exception:
            pass

    unique_chunks = []
    seen = set()
    for chunk in text_chunks:
        cleaned_chunk = clean_text(chunk)
        if cleaned_chunk and cleaned_chunk not in seen:
            unique_chunks.append(cleaned_chunk)
            seen.add(cleaned_chunk)
    return "\n".join(unique_chunks)


def extract_document_text_local(path, filename=""):
    suffix = pathlib.Path(filename or str(path)).suffix.lower()
    if suffix == ".pdf":
        return extract_pdf_text(path)
    if suffix in IMAGE_EXTENSIONS:
        return extract_image_text(path)
    return ""


def extract_document_text(path, filename=""):
    return build_document_analysis_bundle(path, filename).get("text", "")


def detect_ocr_part(normalized_line):
    for label, patterns in OCR_TEILE_PATTERNS:
        if matches_any_pattern(patterns, normalized_line):
            return label
    for label, patterns in TEILE_PATTERNS.items():
        if matches_any_pattern(patterns, normalized_line):
            return label
    return None


def detect_line_item_part(normalized_line):
    for label, patterns in LINE_ITEM_PART_PATTERNS:
        if any(re.search(pattern, normalized_line) for pattern in patterns):
            return label
    return detect_ocr_part(normalized_line)


def looks_like_part_label(normalized_line):
    return bool(
        normalized_line
        and (
            detect_line_item_part(normalized_line)
            or any(re.search(pattern, normalized_line) for pattern in OCR_TABLE_PART_LABEL_PATTERNS)
        )
    )


def is_ignored_ocr_line(normalized_line):
    return any(re.search(pattern, normalized_line) for pattern in OCR_IGNORED_LINE_PATTERNS)


def detect_action(text, doc_type=""):
    normalized = normalize_document_text(text)
    if re.search(r"ersetz|erneuer", normalized):
        return "ersetzen"
    if re.search(r"instand|delle|drueck|drck", normalized):
        return "instandsetzen"
    if re.search(r"lack|neuteillack|reparaturlack|oberfl|lackierung", normalized):
        return "lackieren"
    for label, patterns in ARBEIT_PATTERNS:
        if matches_any_pattern(patterns, normalized):
            return label
    if doc_type in {"DAT-Kalkulation", "DEKRA-Gutachten", "TUEV-Bericht"}:
        return "lackieren"
    return ""


def clean_work_remark(text):
    value = compact_whitespace(text)
    if not value:
        return ""
    value = re.sub(r"\bsto(?:ss|s|f)?f?[aä]nger\b", "Stoßstange", value, flags=re.IGNORECASE)
    value = re.sub(r"\bsto[fs]{1,2}stange\b", "Stoßstange", value, flags=re.IGNORECASE)
    value = re.sub(r"\bundpolieren\b", "und polieren", value, flags=re.IGNORECASE)
    value = re.sub(r"\bSO GUT ES GEHT\b", "so gut es geht", value, flags=re.IGNORECASE)
    value = re.sub(r"\bsmart\s*rep\b", "Smart Repair", value, flags=re.IGNORECASE)
    value = re.sub(r"(?<![A-Za-z])SR(?![A-Za-z])", "Smart Repair", value)
    return compact_whitespace(value)


def is_relevant_lackierauftrag_position(line_text, bemerkung_text=""):
    source = " ".join(
        part for part in [clean_text(line_text), clean_text(bemerkung_text)] if clean_text(part)
    )
    if not source:
        return False
    normalized = normalize_document_text(source)
    return bool(
        re.search(
            r"smart\s*repair|smart\s*rep|\bsr\b|lack|instand|delle|drueck|drck|kratzer|schleif|polier|"
            r"neuteillack|reparaturlack|oberfl|spachtel",
            normalized,
        )
    )


def merge_damage_lines(lines):
    merged = []
    index = 0
    while index < len(lines):
        line = clean_text(lines[index])
        normalized_line = normalize_document_text(line)
        if (
            ("beschaedigung" in normalized_line or "schaden" in normalized_line)
            and index + 1 < len(lines)
        ):
            next_line = clean_text(lines[index + 1])
            normalized_next = normalize_document_text(next_line)
            if next_line and not re.search(r"^seite\s+\d+", normalized_next):
                merged.append(f"{line} {next_line}")
                index += 2
                continue
        merged.append(line)
        index += 1
    return merged


def extract_damage_entries(lines, doc_type=""):
    entries = []
    seen = set()
    for line in merge_damage_lines(lines):
        normalized_line = normalize_document_text(line)
        if "beschaedigung" not in normalized_line and "schaden" not in normalized_line:
            continue
        teil = detect_ocr_part(normalized_line)
        if not teil:
            continue
        action = detect_action(normalized_line, doc_type)
        bemerkung = line
        key = (teil, action, bemerkung.lower())
        if key in seen:
            continue
        seen.add(key)
        entries.append({"teil": teil, "bemerkung": bemerkung, "aktion": action})
    return entries


def looks_like_work_position(normalized_line):
    return bool(
        re.search(
            r"beschadig|beschaedig|schaden|instand|lack(?!stift\b)|ersetz|erneuer|neuteillack|"
            r"reparaturlack|oberfl|aus-/einbauen|a\+e|aufgerissen|deformiert|abgerissen|"
            r"verkratzt|kratzer|schleif|polier|verschuerft|verschurft|gedrueckt|delle|drueck|drck|"
            r"beaufschlagt|smart\s*repair|smart\s*rep|\bsr\b",
            normalized_line,
        )
    )


def select_relevant_position_lines(text, doc_type=""):
    lines = [clean_text(line) for line in clean_text(text).splitlines() if clean_text(line)]
    if doc_type == "DAT-Kalkulation":
        relevant = []
        in_block = False
        for line in lines:
            normalized_line = normalize_document_text(line)
            if re.search(r"^arbeitslohn$|^lackierung$", normalized_line):
                in_block = True
                continue
            if in_block and re.search(
                r"reparaturkosten netto|reparaturkosten brutto|summe lackierung|summenblock",
                normalized_line,
            ):
                continue
            if in_block and (
                looks_like_work_position(normalized_line)
                or detect_line_item_part(normalized_line)
            ):
                relevant.append(line)
        return relevant or lines

    if doc_type in {"DEKRA-Gutachten", "TUEV-Bericht", "Gutachten"}:
        relevant = []
        in_damage = False
        in_calc = False
        for line in lines:
            normalized_line = normalize_document_text(line)
            if "schadensbeschreibung" in normalized_line or "hauptbeschaedigungsbereich" in normalized_line:
                in_damage = True
            if re.search(r"^instandsetzung$|arb\.pos\.nr", normalized_line):
                in_calc = True
            if re.search(r"^e\s*r\s*s\s*a\s*t\s*z", normalized_line):
                in_calc = False
            if in_damage and (
                looks_like_work_position(normalized_line)
                or detect_line_item_part(normalized_line)
            ):
                relevant.append(line)
            if in_calc and (
                looks_like_work_position(normalized_line)
                or detect_line_item_part(normalized_line)
            ):
                relevant.append(line)
        return relevant or lines

    return lines


def extract_cost_hints(text):
    hints = []
    for label, patterns in (
        ("Reparaturkosten netto", [r"reparaturkosten netto\s+([\d.'’]+,\d{2}\s*eur?)"]),
        (
            "Reparaturkosten brutto",
            [
                r"reparaturkosten brutto\s+([\d.'’]+,\d{2}\s*eur?)",
                r"reparaturkosten\s+[\d.'’]+,\d{2}\s*eur\s+([\d.'’]+,\d{2}\s*eur?)",
            ],
        ),
        ("Reparaturdauer", [r"reparaturdauer\s+([0-9]+\s*[a-zäöüß ]+)"]),
        ("Wiederbeschaffungswert", [r"wiederbeschaffungswert[^\n]*?([\d.'’]+,\d{2}\s*eur?)"]),
    ):
        value = first_match(text, patterns)
        if value:
            hints.append(f"{label} {value}")
    rep_max = extract_rep_max_kosten(text)
    if rep_max:
        hints.append(f"Rep.-Max.-Kosten {rep_max}")
    return hints


def extract_rep_max_kosten(text):
    cleaned = clean_text(text)
    normalized = normalize_document_text(text)
    patterns = (
        r"max\.?\s*rep\.?\s*kosten\s*([0-9]{1,5}(?:[.,][0-9]{2})?\s*euro(?:\s*[a-z]+)?)",
        r"rep\.?\s*max\.?\s*kosten\s*([0-9]{1,5}(?:[.,][0-9]{2})?\s*euro(?:\s*[a-z]+)?)",
    )
    for source in (cleaned, normalized):
        for pattern in patterns:
            match = re.search(pattern, source, re.IGNORECASE)
            if not match:
                continue
            value = compact_whitespace(match.group(1))
            value = re.sub(r"(?<=\d)(?=euro)", " ", value, flags=re.IGNORECASE)
            value = re.sub(r"(?<=euro)(?=[A-Za-z])", " ", value, flags=re.IGNORECASE)
            value = re.sub(r"(?i)euro", "Euro", value)
            value = re.sub(r"(?i)\bkomplett\b", "komplett", value)
            value = re.sub(r"(?i)\s+(auftrag|arbeit|datum|unterschrift).*$", "", value).strip()
            return value
    return ""


def extract_position_entries(text, doc_type=""):
    lines = select_relevant_position_lines(text, doc_type)
    positionen = extract_damage_entries(lines, doc_type)
    seen = set()
    for eintrag in positionen:
        seen.add((eintrag["teil"], eintrag["bemerkung"].lower(), eintrag["aktion"]))
    for index, line in enumerate(lines):
        normalized_line = normalize_document_text(line)
        if is_ignored_ocr_line(normalized_line):
            continue
        teil = detect_line_item_part(normalized_line)
        if not teil:
            continue

        bemerkung = ""
        marked = "[x]" in normalized_line or re.search(r"\bx\b", normalized_line)
        action = detect_action(bemerkung or line, doc_type)
        line_has_work = looks_like_work_position(normalized_line)
        if line_has_work:
            bemerkung = line
        else:
            for offset in (1, 2):
                if index + offset >= len(lines):
                    break
                candidate = clean_text(lines[index + offset])
                normalized_candidate = normalize_document_text(candidate)
                if not normalized_candidate or looks_like_part_label(normalized_candidate):
                    break
                if is_ignored_ocr_line(normalized_candidate):
                    continue
                if (
                    doc_type == "Lackierauftrag"
                    and not marked
                    and is_short_repair_note(normalized_candidate)
                    and index + offset + 1 < len(lines)
                    and looks_like_part_label(
                        normalize_document_text(lines[index + offset + 1])
                    )
                ):
                    # In Foto-OCR steht eine kurze Bemerkung wie "rechts SR" oft
                    # zwischen leerer Vorzeile und der tatsaechlich markierten Zeile.
                    break
                bemerkung = candidate
                break

        action = detect_action(bemerkung or line, doc_type)
        if (
            not looks_like_work_position(normalized_line)
            and not looks_like_work_position(normalize_document_text(bemerkung))
            and not marked
        ):
            continue
        if doc_type == "Lackierauftrag" and not marked and not is_relevant_lackierauftrag_position(line, bemerkung):
            continue

        key = (teil, bemerkung.lower(), action)
        if key in seen:
            continue
        seen.add(key)
        positionen.append(
            {
                "teil": teil,
                "bemerkung": clean_work_remark(bemerkung),
                "aktion": action,
            }
        )
    return positionen


def detect_contextual_part_label(lines, index, radius=3):
    current_line = normalize_document_text(lines[index]) if 0 <= index < len(lines) else ""
    short_repair_note = is_short_repair_note(current_line)
    positions = [index]
    for offset in range(1, radius + 1):
        if short_repair_note:
            positions.extend([index + offset, index - offset])
        else:
            positions.extend([index - offset, index + offset])
    for position in positions:
        if position < 0 or position >= len(lines):
            continue
        candidate = clean_text(lines[position])
        if not candidate:
            continue
        normalized_candidate = normalize_document_text(candidate)
        label = detect_line_item_part(normalized_candidate)
        if label:
            if label == "Radhausverbreiterung":
                directions = []
                for neighbor_offset in range(-2, 3):
                    if neighbor_offset == 0:
                        continue
                    neighbor_position = position + neighbor_offset
                    if neighbor_position < 0 or neighbor_position >= len(lines):
                        continue
                    normalized_neighbor = normalize_document_text(lines[neighbor_position])
                    for token in ("hinten", "vorne", "links", "rechts"):
                        if re.search(rf"\b{token}\b", normalized_neighbor) and token not in directions:
                            directions.append(token)
                for first in directions:
                    for second in directions:
                        if first == second:
                            continue
                        directed_label = detect_line_item_part(
                            f"{normalized_candidate} {first} {second}"
                        )
                        if directed_label and directed_label != label:
                            return directed_label
            return label

        directions = []
        for neighbor_offset in range(-2, 3):
            if neighbor_offset == 0:
                continue
            neighbor_position = position + neighbor_offset
            if neighbor_position < 0 or neighbor_position >= len(lines):
                continue
            normalized_neighbor = normalize_document_text(lines[neighbor_position])
            for token in ("vorne", "hinten", "links", "rechts"):
                if re.search(rf"\b{token}\b", normalized_neighbor) and token not in directions:
                    directions.append(token)
        for first in directions:
            for second in directions:
                if first == second:
                    continue
                label = detect_line_item_part(f"{normalized_candidate} {first} {second}")
                if label:
                    return label
        for direction in directions:
            label = detect_line_item_part(f"{normalized_candidate} {direction}")
            if label:
                return label
    return ""


def is_short_repair_note(normalized_line):
    line = clean_text(normalized_line)
    return bool(
        re.fullmatch(r"(rechts|links|li|re)?\s*(smart\s*repair|sr)\s*(rechts|links|li|re)?", line)
        or re.fullmatch(r"(rechts|links|li|re)\s+(smart\s*repair|sr)", line)
    )


def extract_lackierauftrag_work_entries(lines):
    positionen = []
    seen = set()
    for index, line in enumerate(lines):
        if not is_relevant_lackierauftrag_position(line):
            continue
        teil = detect_contextual_part_label(lines, index)
        if not teil:
            continue
        bemerkung = build_lackierauftrag_remark(lines, index, teil)
        action = detect_action(bemerkung, "Lackierauftrag")
        key = (teil, bemerkung.lower(), action)
        if key in seen:
            continue
        seen.add(key)
        positionen.append({"teil": teil, "bemerkung": bemerkung, "aktion": action})
    return merge_same_part_action(positionen)


def build_lackierauftrag_remark(lines, index, teil):
    line = clean_work_remark(lines[index])
    normalized_part = normalize_document_text(teil)
    hints = []
    start = max(0, index - 3)
    end = min(len(lines), index + 5)
    for position in range(start, end):
        candidate = clean_text(lines[position])
        normalized_candidate = normalize_document_text(candidate)
        if not candidate or is_ignored_ocr_line(normalized_candidate):
            continue
        if "ladekante" in normalized_candidate and "Ladekante" not in hints:
            hints.append("Ladekante")
        if "stossstange hinten" in normalized_part or "stossfaenger h" in normalized_candidate:
            if re.search(r"\bhinten\s+rechts\b|\brechts\b", normalized_candidate) and "hinten rechts" not in hints:
                hints.append("hinten rechts")
            if re.search(r"\bhinten\s+links\b|\blinks\b", normalized_candidate) and "hinten links" not in hints:
                hints.append("hinten links")
    if hints:
        return clean_work_remark(f"{' / '.join(hints)} {line}")
    return line


def merge_position_entries(*entry_groups):
    merged = []
    seen = set()
    for entries in entry_groups:
        for entry in entries or []:
            key = (
                clean_text(entry.get("teil")).lower(),
                clean_text(entry.get("bemerkung")).lower(),
                clean_text(entry.get("aktion")).lower(),
            )
            if not key[0] or key in seen:
                continue
            seen.add(key)
            merged.append(entry)
    return merged


def merge_same_part_action(entries):
    merged = []
    by_key = {}
    for entry in entries or []:
        teil = clean_text(entry.get("teil"))
        action = clean_text(entry.get("aktion"))
        bemerkung = clean_text(entry.get("bemerkung"))
        if not teil:
            continue
        key = (teil.lower(), action.lower())
        if key not in by_key:
            new_entry = {"teil": teil, "bemerkung": bemerkung, "aktion": action}
            by_key[key] = new_entry
            merged.append(new_entry)
            continue
        existing = by_key[key]
        if bemerkung and bemerkung.lower() not in clean_text(existing.get("bemerkung")).lower():
            existing["bemerkung"] = compact_whitespace(
                f"{clean_text(existing.get('bemerkung'))}; {bemerkung}".strip("; ")
            )
    return merged


def remove_less_specific_part_entries(entries):
    result = []
    normalized_parts = [normalize_document_text(entry.get("teil")) for entry in entries or []]
    for entry in entries or []:
        teil_norm = normalize_document_text(entry.get("teil"))
        if not teil_norm:
            continue
        is_less_specific = any(
            other != teil_norm and other.startswith(f"{teil_norm} ")
            for other in normalized_parts
        )
        if is_less_specific:
            continue
        result.append(entry)
    return result


def first_match(text, patterns, flags=re.IGNORECASE):
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return clean_text(match.group(1))
    return ""


def looks_like_field_label(value):
    normalized = normalize_document_text(value)
    return bool(
        re.search(
            r"^(typ|fg[-.\s]*nr|farb[-.\s]*nr|abnahme|auftrags[-\s]*nr|amtl\.?\s*kennzeichen|"
            r"kennzeichen|auftraggeber|lieferant|auftrags[-\s]*datum|fertig\s*bis|fertigbis|i\.?o\.?|n\.?i\.?o\.?)",
            normalized,
        )
    )


def looks_like_date(value):
    return bool(re.fullmatch(r"\d{1,2}\.\d{1,2}\.\d{2,4}", clean_text(value)))


def is_vehicle_candidate(candidate, normalized_candidate):
    candidate = clean_text(candidate)
    normalized_candidate = clean_text(normalized_candidate)
    return bool(
        re.fullmatch(r"[A-Za-z0-9ÄÖÜäöüß][A-Za-z0-9ÄÖÜäöüß ._/-]{1,30}", candidate)
        and re.search(r"[A-Za-zÄÖÜäöüß]", candidate)
        and not looks_like_field_label(candidate)
        and not looks_like_date(candidate)
        and not re.search(r"^audi$|^vw$|^volkswagen$|^gaertner$|^kasmann$|^kaesmann$", normalized_candidate)
        and not re.fullmatch(r"#?[A-HJ-NPR-Z0-9]{8,20}", candidate.upper())
        and "/" not in candidate
    )


def find_nearby_value(
    lines, index, validator, window_before=2, window_after=5, prefer_nearest=False
):
    start = max(0, index - window_before)
    end = min(len(lines), index + window_after + 1)
    positions = [position for position in range(start, end) if position != index]
    if prefer_nearest:
        positions.sort(key=lambda position: (abs(position - index), position))
    for position in positions:
        candidate = clean_text(lines[position])
        if not candidate:
            continue
        normalized_candidate = normalize_document_text(candidate)
        if validator(candidate, normalized_candidate):
            return candidate
        if is_ignored_ocr_line(normalized_candidate):
            continue
    return ""


def find_matching_autohaus_id(text):
    lines = [clean_text(line) for line in clean_text(text).splitlines() if clean_text(line)]
    candidates = lines or [clean_text(text)]
    normalized_candidates = [normalize_document_text(line) for line in candidates if clean_text(line)]
    normalized_candidates = [value for value in normalized_candidates if value]
    if not normalized_candidates:
        return None
    db = get_db()
    rows = db.execute("SELECT id, name FROM autohaeuser ORDER BY name ASC").fetchall()
    db.close()
    for row in rows:
        name_normalized = normalize_document_text(row["name"])
        if any(name_normalized in candidate for candidate in normalized_candidates):
            return row["id"]
        if any(SequenceMatcher(None, name_normalized, candidate).ratio() >= 0.75 for candidate in normalized_candidates):
            return row["id"]
    return None


def parse_document_fields(text, filename=""):
    cleaned = clean_text(text)
    if not cleaned:
        return {}

    normalized = normalize_document_text(cleaned)
    doc_type = classify_document(cleaned, filename)
    positionen = extract_position_entries(cleaned, doc_type)
    lines = [clean_text(line) for line in cleaned.splitlines() if clean_text(line)]
    if doc_type == "Lackierauftrag":
        positionen = merge_position_entries(
            positionen,
            extract_lackierauftrag_work_entries(lines),
        )
        positionen = merge_same_part_action(remove_less_specific_part_entries(positionen))

    fahrzeug = ""
    hersteller = ""
    haupttyp = ""
    untertyp = ""
    vin = ""
    kennzeichen = ""
    annahme_datum = ""
    fertig_datum = ""
    auftragsnummer = ""
    fahrgestellnummer = ""
    farbnummer = ""
    rep_max_kosten = extract_rep_max_kosten(cleaned)

    for index, line in enumerate(lines):
        normalized_line = normalize_document_text(line)
        typ_inline = first_match(
            line,
            [r"\bTyp[:.\s]+([A-Za-z][A-Za-z0-9 .\-/]{1,24})"],
            flags=re.IGNORECASE,
        )
        if typ_inline and not fahrzeug and not re.fullmatch(r"[A-HJ-NPR-Z0-9]{15,20}", typ_inline.upper()):
            fahrzeug = compact_whitespace(typ_inline)

        if re.search(r"^hersteller", normalized_line) and not hersteller:
            hersteller = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(re.fullmatch(r"[A-Za-zÄÖÜäöüß .-]{2,30}", candidate)),
                window_before=0,
                window_after=2,
                prefer_nearest=True,
            )
        elif re.search(r"^haupttyp", normalized_line) and not haupttyp:
            haupttyp = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(re.fullmatch(r"[A-Za-z0-9ÄÖÜäöüß .()/-]{2,40}", candidate)),
                window_before=0,
                window_after=2,
                prefer_nearest=True,
            )
        elif re.search(r"^untertyp", normalized_line) and not untertyp:
            untertyp = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(re.fullmatch(r"[A-Za-z0-9ÄÖÜäöüß .()/-]{2,40}", candidate)),
                window_before=0,
                window_after=2,
                prefer_nearest=True,
            )
        elif re.search(r"^vin", normalized_line) and not vin:
            vin = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(re.fullmatch(r"[A-HJ-NPR-Z0-9]{8,20}", candidate)),
                window_before=0,
                window_after=2,
                prefer_nearest=True,
            ).upper()
        if re.search(r"^typ", normalized_line) and not fahrzeug:
            fahrzeug = find_nearby_value(
                lines,
                index,
                lambda candidate, normalized_candidate: bool(
                    is_vehicle_candidate(candidate, normalized_candidate)
                ),
                window_before=0,
                window_after=7,
            )
        elif re.search(r"^kennzeichen$", normalized_line) and not kennzeichen:
            kennzeichen = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(
                    re.fullmatch(r"[A-ZÄÖÜ]{1,3}[-\s]?[A-Z]{1,2}\s?\d{1,4}", candidate)
                ),
                window_before=0,
                window_after=2,
                prefer_nearest=True,
            ).upper()
        elif re.search(r"^amtl\.?\s*kennzeichen", normalized_line) and not kennzeichen:
            kennzeichen = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(
                    re.fullmatch(r"[A-ZÄÖÜ]{1,3}[-\s]?[A-Z]{1,2}\s?\d{1,4}", candidate)
                ),
                window_before=0,
                window_after=4,
                ).upper()
        elif re.search(r"^auftrags[-\s]*datum", normalized_line) and not annahme_datum:
            annahme_datum = format_date(
                find_nearby_value(
                    lines,
                    index,
                    lambda candidate, _: bool(re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", candidate)),
                    window_before=4,
                    window_after=3,
                    prefer_nearest=True,
                )
            )
        elif re.search(r"^fertig\s*bis|^fertigbis", normalized_line) and not fertig_datum:
            fertig_datum = format_date(
                find_nearby_value(
                    lines,
                    index,
                    lambda candidate, _: bool(re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", candidate)),
                    window_before=4,
                    window_after=3,
                    prefer_nearest=True,
                )
            )
        elif re.search(r"^auftrags[-\s]*nr", normalized_line) and not auftragsnummer:
            inline_nr = first_match(line, [r"auftrags[-\s]*nr[:.\s]*([A-Z0-9/\-]{4,18})"], flags=re.IGNORECASE)
            if inline_nr and normalize_document_text(inline_nr) != "unbekannt":
                auftragsnummer = inline_nr
                continue
            auftragsnummer = find_nearby_value(
                lines,
                index,
                lambda candidate, normalized_candidate: bool(
                    re.fullmatch(r"[A-Z0-9/\-]{3,12}", candidate)
                    and not re.search(r"kennzeichen|audi|vw", normalized_candidate)
                    and normalized_candidate != "unbekannt"
                ),
                window_before=0,
                window_after=4,
            )
        elif re.search(r"^fg[-.\s]*nr", normalized_line) and not fahrgestellnummer:
            fahrgestellnummer = find_nearby_value(
                lines,
                index,
                lambda candidate, _: bool(re.fullmatch(r"#?[A-Z0-9\-]{4,20}", candidate)),
                window_before=0,
                window_after=5,
            ).lstrip("#")
        elif re.search(r"^farb[-.\s]*nr", normalized_line) and not farbnummer:
            farbnummer = find_nearby_value(
                lines,
                index,
                lambda candidate, normalized_candidate: bool(
                    re.fullmatch(r"[A-Z0-9]{3,8}", candidate)
                    and not re.search(r"abnahme|io|nio", normalized_candidate)
                ),
                window_before=0,
                window_after=5,
            ).upper()

    if not fahrzeug:
        fahrzeug = first_match(
            cleaned,
            [
                r"\bTyp[:.\s]+([A-Za-z][A-Za-z0-9 .\-/]{1,24})",
                r"\bFahrzeug[:.\s]+([A-Za-z0-9][A-Za-z0-9 .\-/]{1,30})",
                r"\bHaupttyp[:.\s]+([A-Za-z0-9ÄÖÜäöüß .()/-]{2,40})",
            ],
        )
        if fahrzeug and not is_vehicle_candidate(fahrzeug, normalize_document_text(fahrzeug)):
            fahrzeug = ""
    if not hersteller:
        hersteller = first_match(cleaned, [r"\bHersteller[:.\s]+([A-Za-zÄÖÜäöüß .-]{2,30})"])
    if not haupttyp:
        haupttyp = first_match(cleaned, [r"\bHaupttyp[:.\s]+([A-Za-z0-9ÄÖÜäöüß .()/-]{2,40})"])
    if not untertyp:
        untertyp = first_match(cleaned, [r"\bUntertyp[:.\s]+([A-Za-z0-9ÄÖÜäöüß .()/-]{2,40})"])
    if not vin:
        vin = first_match(cleaned, [r"\bVIN[:.\s]+([A-HJ-NPR-Z0-9]{8,20})"]).upper()
    if not annahme_datum:
        annahme_datum = format_date(
            first_match(cleaned, [r"Auftrags[-\s]*Datum[:.]?\s*(\d{2}\.\d{2}\.\d{4})"])
        )
    if not annahme_datum:
        annahme_datum = format_date(
            first_match(cleaned, [r"Besichtigung\s+(\d{2}\.\d{2}\.\d{4})"])
        )
    if not fertig_datum:
        fertig_datum = format_date(
            first_match(
                cleaned,
                [r"Fertig\s*bis(?:\s*spaetestens|\s*spätestens)?[:.]?\s*(\d{2}\.\d{2}\.\d{4})"],
            )
        )

    analyse = ""
    if positionen:
        teile = []
        for eintrag in positionen:
            if eintrag["aktion"]:
                teile.append(f"{eintrag['teil']} {eintrag['aktion']}")
            else:
                teile.append(eintrag["teil"])
        analyse = ", ".join(dict.fromkeys(teile))[:220]
    if not analyse:
        analyse = analyse_text(normalized)
    if not analyse and (doc_type in {"DAT-Kalkulation", "DEKRA-Gutachten", "TUEV-Bericht"}):
        teile = []
        for label, patterns in TEILE_PATTERNS.items():
            if matches_any_pattern(patterns, normalized):
                teile.append(label)
        if teile:
            action = detect_action(normalized, doc_type) or "pruefen"
            analyse = ", ".join(f"{teil} {action}" for teil in dict.fromkeys(teile))[:220]

    if not fahrzeug:
        fahrzeug = " ".join(part for part in [hersteller, haupttyp, untertyp] if part).strip()
    if re.search(r"profiltiefe|art ", fahrzeug.lower()):
        fahrzeug = ""

    details = [doc_type]
    if auftragsnummer:
        details.append(f"Auftrags-Nr. {auftragsnummer}")
    if fahrzeug:
        details.append(f"Typ {fahrzeug}")
    if vin:
        details.append(f"VIN {vin}")
    if fahrgestellnummer:
        details.append(f"Fg.-Nr. {fahrgestellnummer}")
    if farbnummer:
        details.append(f"Farb-Nr. {farbnummer}")
    if annahme_datum:
        details.append(f"Auftrags-Datum {annahme_datum}")
    if fertig_datum:
        details.append(f"Fertig bis spätestens {fertig_datum}")
    if positionen:
        arbeiten = []
        for eintrag in positionen:
            textteil = eintrag["teil"]
            if eintrag["bemerkung"]:
                textteil = f"{textteil}: {eintrag['bemerkung']}"
            arbeiten.append(textteil)
        details.append("Arbeiten " + "; ".join(arbeiten))
    elif analyse:
        details.append("Arbeit " + analyse)
    for hint in extract_cost_hints(cleaned):
        details.append(hint)

    return {
        "autohaus_id": find_matching_autohaus_id(cleaned),
        "fahrzeug": fahrzeug,
        "fin_nummer": vin or fahrgestellnummer,
        "auftragsnummer": auftragsnummer,
        "kennzeichen": kennzeichen,
        "annahme_datum": annahme_datum,
        "fertig_datum": fertig_datum,
        "rep_max_kosten": rep_max_kosten,
        "analyse_text": analyse,
        "beschreibung": ". ".join(part for part in details if part)[:500],
    }



def classify_document(text, filename=""):
    lowered = f"{clean_text(filename)} {clean_text(text)}".lower()
    normalized = normalize_document_text(f"{clean_text(filename)} {clean_text(text)}")
    for label, patterns in DOCUMENT_PATTERNS:
        if matches_any_pattern(patterns, lowered, normalized):
            return label
    suffix = pathlib.Path(filename).suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return "Bilddokument"
    if suffix == ".pdf":
        return "PDF-Dokument"
    return "Dokument"


def summarize_document_text(text, filename=""):
    cleaned = compact_whitespace(text)
    if not cleaned:
        return ""

    doc_type = classify_document(cleaned, filename)
    felder = parse_document_fields(text, filename)
    analyse = clean_text(felder.get("analyse_text")) or analyse_text(cleaned)

    hints = []
    if clean_text(felder.get("fahrzeug")):
        hints.append(f"Typ: {felder['fahrzeug']}")
    if clean_text(felder.get("kennzeichen")):
        hints.append(f"Kennzeichen: {felder['kennzeichen']}")
    if clean_text(felder.get("auftragsnummer")):
        hints.append(f"Auftrag: {felder['auftragsnummer']}")
    if analyse:
        hints.append(f"Arbeit: {analyse}")
    if clean_text(felder.get("annahme_datum")):
        hints.append(f"Annahme: {felder['annahme_datum']}")
    if clean_text(felder.get("fertig_datum")):
        hints.append(f"Fertig: {felder['fertig_datum']}")

    key_lines = []
    for pattern in (
        r"(schaden[^.]{0,180}\.)",
        r"(reparatur[^.]{0,180}\.)",
        r"(lack[^.]{0,180}\.)",
        r"(instand[^.]{0,180}\.)",
    ):
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            key_lines.append(compact_whitespace(match.group(1)))
    summary_core = " ".join(dict.fromkeys(key_lines))[:320]

    parts = [doc_type]
    if hints:
        parts.append(" | ".join(hints))
    if summary_core:
        parts.append(summary_core)
    return " - ".join(part for part in parts if part)[:500]


def slugify(text):
    value = clean_text(text).lower()
    value = (
        value.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or f"autohaus-{uuid.uuid4().hex[:6]}"


def get_db():
    if USE_POSTGRES:
        if psycopg is None:
            raise RuntimeError(
                "DATABASE_URL ist gesetzt, aber psycopg ist nicht installiert."
            )
        return PostgresConnection(psycopg.connect(DATABASE_URL))
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


class DbRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class PostgresCursor:
    def __init__(self, rows=None, lastrowid=None):
        self._rows = rows
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._rows is None:
            return None
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows or [])


class PostgresConnection:
    def __init__(self, conn):
        self.conn = conn
        self.lastrowid = None

    def execute(self, sql, params=()):
        sql = sql.strip()
        if sql.upper() == "SELECT LAST_INSERT_ROWID()":
            return PostgresCursor([DbRow({"last_insert_rowid": self.lastrowid})])

        converted_sql = convert_sqlite_sql_to_postgres(sql)
        params = tuple(params or ())
        lowered = converted_sql.lstrip().lower()
        inserts_with_id = lowered.startswith("insert into ") and " returning " not in lowered
        if inserts_with_id:
            converted_sql = f"{converted_sql} RETURNING id"

        with self.conn.cursor() as cur:
            cur.execute(converted_sql, params)
            if cur.description:
                names = [column.name for column in cur.description]
                rows = [DbRow(dict(zip(names, values))) for values in cur.fetchall()]
            else:
                rows = []

        if inserts_with_id:
            self.lastrowid = rows[0]["id"] if rows else None
            return PostgresCursor([], self.lastrowid)
        return PostgresCursor(rows)

    def executescript(self, script):
        for statement in split_sql_script(script):
            self.execute(statement)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def split_sql_script(script):
    return [part.strip() for part in script.split(";") if part.strip()]


def convert_sqlite_sql_to_postgres(sql):
    converted = sql.replace("?", "%s")
    converted = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "SERIAL PRIMARY KEY",
        converted,
        flags=re.IGNORECASE,
    )
    return converted


def get_table_columns(db, table_name):
    if USE_POSTGRES:
        rows = db.execute(
            """
            SELECT column_name AS name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        ).fetchall()
    else:
        rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def ensure_column(db, table_name, column_name, column_definition):
    columns = get_table_columns(db, table_name)
    if column_name not in columns:
        try:
            db.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
            )
        except Exception as exc:
            message = str(exc).lower()
            if "duplicate column name" not in message and "already exists" not in message:
                raise


def init_db():
    if USE_POSTGRES:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    else:
        DATA_DIR.mkdir(exist_ok=True)
        UPLOAD_DIR.mkdir(exist_ok=True)
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS autohaeuser (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            slug         TEXT UNIQUE NOT NULL,
            portal_key   TEXT UNIQUE DEFAULT '',
            kontakt_name TEXT DEFAULT '',
            email        TEXT DEFAULT '',
            telefon      TEXT DEFAULT '',
            strasse      TEXT DEFAULT '',
            plz          TEXT DEFAULT '',
            ort          TEXT DEFAULT '',
            zugangscode  TEXT NOT NULL,
            portal_titel TEXT DEFAULT '',
            willkommen_text TEXT DEFAULT '',
            notiz        TEXT DEFAULT '',
            erstellt_am  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS auftraege (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            token          TEXT DEFAULT '',
            kunde_email    TEXT DEFAULT '',
            autohaus_id    INTEGER,
            kunde_name     TEXT DEFAULT '',
            fahrzeug       TEXT NOT NULL,
            fin_nummer     TEXT DEFAULT '',
            auftragsnummer TEXT DEFAULT '',
            rep_max_kosten TEXT DEFAULT '',
            bauteile_override TEXT DEFAULT '',
            kennzeichen    TEXT DEFAULT '',
            beschreibung   TEXT DEFAULT '',
            analyse_text   TEXT DEFAULT '',
            analyse_pruefen INTEGER DEFAULT 0,
            analyse_hinweis TEXT DEFAULT '',
            analyse_confidence REAL DEFAULT 0,
            angebotsphase  INTEGER DEFAULT 0,
            angebot_abgesendet INTEGER DEFAULT 0,
            angebot_status TEXT DEFAULT 'entwurf',
            werkstatt_angebot_text TEXT DEFAULT '',
            werkstatt_angebot_preis TEXT DEFAULT '',
            werkstatt_angebot_am TEXT DEFAULT '',
            status         INTEGER DEFAULT 1,
            annahme_datum  TEXT DEFAULT '',
            start_datum    TEXT DEFAULT '',
            fertig_datum   TEXT DEFAULT '',
            abholtermin    TEXT DEFAULT '',
            transport_art  TEXT DEFAULT 'standard',
            archiviert     INTEGER DEFAULT 0,
            kontakt_telefon TEXT DEFAULT '',
            notiz_intern   TEXT DEFAULT '',
            quelle         TEXT DEFAULT 'intern',
            erstellt_am    TEXT NOT NULL,
            geaendert_am   TEXT NOT NULL,
            FOREIGN KEY (autohaus_id) REFERENCES autohaeuser(id)
        );

        CREATE TABLE IF NOT EXISTS status_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id  INTEGER NOT NULL,
            status      INTEGER NOT NULL,
            zeitstempel TEXT NOT NULL,
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );

        CREATE TABLE IF NOT EXISTS dateien (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id     INTEGER NOT NULL,
            original_name  TEXT NOT NULL,
            stored_name    TEXT NOT NULL,
            mime_type      TEXT,
            size           INTEGER DEFAULT 0,
            quelle         TEXT DEFAULT 'intern',
            dokument_typ   TEXT DEFAULT '',
            extrahierter_text TEXT DEFAULT '',
            extrakt_kurz   TEXT DEFAULT '',
            analyse_quelle TEXT DEFAULT '',
            analyse_json   TEXT DEFAULT '',
            analyse_hinweis TEXT DEFAULT '',
            hochgeladen_am TEXT NOT NULL,
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );

        CREATE TABLE IF NOT EXISTS verzoegerungen (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id             INTEGER NOT NULL,
            quelle                 TEXT NOT NULL,
            meldung                TEXT NOT NULL,
            vorgeschlagen_start    TEXT DEFAULT '',
            vorgeschlagen_fertig   TEXT DEFAULT '',
            vorgeschlagen_abholung TEXT DEFAULT '',
            uebernommen            INTEGER DEFAULT 0,
            erstellt_am            TEXT NOT NULL,
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );

        CREATE TABLE IF NOT EXISTS reklamationen (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id     INTEGER NOT NULL,
            quelle         TEXT NOT NULL,
            meldung        TEXT NOT NULL,
            bearbeitet     INTEGER DEFAULT 0,
            erstellt_am    TEXT NOT NULL,
            bearbeitet_am  TEXT DEFAULT '',
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );
        """
    )

    ensure_column(db, "auftraege", "autohaus_id", "INTEGER")
    ensure_column(db, "auftraege", "token", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "kunde_email", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "analyse_text", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "fin_nummer", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "auftragsnummer", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "rep_max_kosten", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "bauteile_override", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "analyse_pruefen", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "analyse_hinweis", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "analyse_confidence", "REAL DEFAULT 0")
    ensure_column(db, "auftraege", "angebotsphase", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "angebot_abgesendet", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "angebot_status", "TEXT DEFAULT 'entwurf'")
    ensure_column(db, "auftraege", "werkstatt_angebot_text", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "werkstatt_angebot_preis", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "werkstatt_angebot_am", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "annahme_datum", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "start_datum", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "abholtermin", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "transport_art", "TEXT DEFAULT 'standard'")
    ensure_column(db, "auftraege", "archiviert", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "kontakt_telefon", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "notiz_intern", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "quelle", "TEXT DEFAULT 'intern'")
    ensure_column(db, "dateien", "quelle", "TEXT DEFAULT 'intern'")
    ensure_column(db, "dateien", "kategorie", "TEXT DEFAULT 'standard'")
    ensure_column(db, "dateien", "reklamation_id", "INTEGER")
    ensure_column(db, "dateien", "dokument_typ", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "extrahierter_text", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "extrakt_kurz", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_quelle", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_json", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_hinweis", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "portal_key", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "portal_titel", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "willkommen_text", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "strasse", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "plz", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "ort", "TEXT DEFAULT ''")

    rows = db.execute("SELECT id, portal_key FROM autohaeuser").fetchall()
    for row in rows:
        if not clean_text(row["portal_key"]):
            db.execute(
                "UPDATE autohaeuser SET portal_key=? WHERE id=?",
                (uuid.uuid4().hex[:16], row["id"]),
            )

    db.commit()
    db.close()


def row_to_autohaus(row):
    if not row:
        return None
    autohaus = dict(row)
    autohaus["portal_label"] = clean_text(autohaus.get("portal_titel")) or autohaus["name"]
    adresse_teile = [
        clean_text(autohaus.get("strasse")),
        " ".join(
            teil
            for teil in (clean_text(autohaus.get("plz")), clean_text(autohaus.get("ort")))
            if teil
        ),
    ]
    autohaus["adresse_kompakt"] = ", ".join(teil for teil in adresse_teile if teil)
    autohaus["portal_welcome"] = clean_text(autohaus.get("willkommen_text")) or (
        f"Willkommen im Portal von {autohaus['name']}."
    )
    autohaus["portal_url"] = f"/portal/{clean_text(autohaus.get('portal_key'))}"
    return autohaus


def row_to_auftrag(row):
    if not row:
        return None
    auftrag = dict(row)
    transport_art = clean_text(auftrag.get("transport_art")) or "standard"
    if transport_art not in TRANSPORT_ARTEN:
        transport_art = "standard"
    transport_meta = TRANSPORT_ARTEN[transport_art]
    auftrag["transport_art"] = transport_art
    auftrag["transport_meta"] = transport_meta
    auftrag["archiviert"] = bool(auftrag.get("archiviert"))
    auftrag["angebotsphase"] = bool(auftrag.get("angebotsphase"))
    auftrag["angebot_abgesendet"] = bool(auftrag.get("angebot_abgesendet"))
    auftrag["angebot_status"] = clean_text(auftrag.get("angebot_status")) or (
        "angefragt" if auftrag["angebot_abgesendet"] else "entwurf"
    )
    auftrag["analyse_pruefen"] = bool(auftrag.get("analyse_pruefen"))
    try:
        auftrag["analyse_confidence"] = float(auftrag.get("analyse_confidence") or 0)
    except Exception:
        auftrag["analyse_confidence"] = 0
    auftrag["status_meta"] = STATUSLISTE.get(auftrag["status"], STATUSLISTE[1])
    for feld, _, _ in EVENT_FELDER:
        auftrag[f"{feld}_obj"] = parse_date(auftrag.get(feld))
        auftrag[feld] = format_date(auftrag.get(feld))
    auftrag["annahme_label"] = transport_meta["annahme_label"]
    auftrag["abholung_label"] = transport_meta["abholung_label"]
    auftrag["partner_annahme_label"] = transport_meta.get("partner_annahme_label", transport_meta["annahme_label"])
    auftrag["partner_abholung_label"] = transport_meta.get("partner_abholung_label", transport_meta["abholung_label"])
    auftrag["angebot_annahme_label"] = transport_meta.get("angebot_annahme_label", "Gewünschter Bringtermin")
    auftrag["angebot_abholung_label"] = transport_meta.get("angebot_abholung_label", "Gewünschter Holtermin")
    auftrag["kunden_bauteile"] = build_customer_part_summaries(auftrag)
    auftrag["preisvorschlag"] = build_price_suggestion(auftrag)
    return auftrag


def ensure_auftrag_analysis_from_documents(auftrag):
    if not auftrag:
        return auftrag
    updates = apply_document_data_to_auftrag(auftrag["id"])
    if updates:
        auftrag.update(updates)
    return auftrag


def list_autohaeuser():
    db = get_db()
    rows = db.execute("SELECT * FROM autohaeuser ORDER BY name ASC").fetchall()
    db.close()
    return [row_to_autohaus(row) for row in rows]


def get_autohaus(autohaus_id):
    db = get_db()
    row = db.execute("SELECT * FROM autohaeuser WHERE id=?", (autohaus_id,)).fetchone()
    db.close()
    return row_to_autohaus(row)


def get_autohaus_by_slug(slug):
    db = get_db()
    row = db.execute("SELECT * FROM autohaeuser WHERE slug=?", (slug,)).fetchone()
    db.close()
    return row_to_autohaus(row)


def get_autohaus_by_portal_key(portal_key):
    db = get_db()
    row = db.execute(
        "SELECT * FROM autohaeuser WHERE portal_key=?",
        (portal_key,),
    ).fetchone()
    db.close()
    return row_to_autohaus(row)


def get_unique_slug(name):
    base_slug = slugify(name)
    db = get_db()
    slug = base_slug
    counter = 2
    while db.execute("SELECT 1 FROM autohaeuser WHERE slug=?", (slug,)).fetchone():
        slug = f"{base_slug}-{counter}"
        counter += 1
    db.close()
    return slug


def get_auftrag(auftrag_id):
    db = get_db()
    row = db.execute(
        """
        SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug
        FROM auftraege a
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE a.id=?
        """,
        (auftrag_id,),
    ).fetchone()
    db.close()
    return ensure_auftrag_analysis_from_documents(row_to_auftrag(row))


def list_auftraege(autohaus_id=None, include_archived=False, include_angebote=False):
    db = get_db()
    archived_filter = "" if include_archived else "AND a.archiviert = 0"
    angebots_filter = "" if include_angebote else "AND a.angebotsphase = 0"
    if autohaus_id is None:
        rows = db.execute(
            """
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE 1=1
            """
            + archived_filter
            + "\n"
            + angebots_filter
            + """
            ORDER BY a.geaendert_am DESC, a.id DESC
            """
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE a.autohaus_id=?
            """
            + archived_filter
            + "\n"
            + angebots_filter
            + """
            ORDER BY a.geaendert_am DESC, a.id DESC
            """,
            (autohaus_id,),
        ).fetchall()
    db.close()

    auftraege = [ensure_auftrag_analysis_from_documents(row_to_auftrag(row)) for row in rows]
    auftraege.sort(
        key=lambda a: (
            a["status"] == 4,
            a["annahme_datum_obj"] or date.max,
            a["abholtermin_obj"] or date.max,
            clean_text(a.get("kennzeichen")).lower(),
        )
    )
    return auftraege


def list_angebotsanfragen(autohaus_id=None):
    db = get_db()
    if autohaus_id is None:
        rows = db.execute(
            """
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE a.angebotsphase = 1 AND a.angebot_abgesendet = 1
            ORDER BY a.geaendert_am DESC, a.id DESC
            """
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE a.angebotsphase = 1 AND a.angebot_abgesendet = 1 AND a.autohaus_id = ?
            ORDER BY a.geaendert_am DESC, a.id DESC
            """,
            (autohaus_id,),
        ).fetchall()
    db.close()
    anfragen = [ensure_auftrag_analysis_from_documents(row_to_auftrag(row)) for row in rows]
    for anfrage in anfragen:
        anfrage["dateien"] = list_dateien(anfrage["id"])
    return anfragen


def list_dateien(auftrag_id):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM dateien WHERE auftrag_id=? ORDER BY hochgeladen_am DESC, id DESC",
        (auftrag_id,),
    ).fetchall()
    db.close()
    return [hydrate_datei(dict(row)) for row in rows]


def split_dateien(dateien):
    standard = [d for d in dateien if clean_text(d.get("kategorie")) != "fertigbild"]
    fertigbilder = [d for d in dateien if clean_text(d.get("kategorie")) == "fertigbild"]
    return standard, fertigbilder


def dateien_mit_kategorie(dateien, kategorie):
    return [d for d in dateien if clean_text(d.get("kategorie")) == kategorie]


def get_datei(datei_id):
    db = get_db()
    row = db.execute("SELECT * FROM dateien WHERE id=?", (datei_id,)).fetchone()
    db.close()
    return hydrate_datei(dict(row)) if row else None


def hydrate_datei(datei):
    if not datei:
        return None

    suffix = pathlib.Path(datei["original_name"]).suffix.lower()
    datei["is_pdf"] = suffix == ".pdf"
    datei["is_image"] = suffix in IMAGE_EXTENSIONS
    datei["kategorie"] = clean_text(datei.get("kategorie")) or "standard"
    datei["has_extract"] = bool(
        clean_text(datei.get("extrakt_kurz")) or clean_text(datei.get("extrahierter_text"))
    )
    datei["text_preview"] = clean_text(datei.get("extrahierter_text"))[:2000]
    return datei


def should_replace_fahrzeug(existing_value):
    existing = clean_text(existing_value).lower()
    return not existing or existing in {"fahrzeug", "neues fahrzeug", "unbekannt"}


def load_saved_analysis_json(value):
    raw = clean_text(value)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return normalize_openai_document_data(data)


def looks_like_specific_work_text(value):
    normalized = normalize_document_text(value)
    if not normalized:
        return False
    return any(matches_any_pattern(patterns, normalized, normalized) for patterns in TEILE_PATTERNS.values())


def merge_document_fields(ai_fields, local_fields):
    fields = dict(ai_fields or {})
    review_notes = []
    for key, label, is_date in (
        ("fahrzeug", "Fahrzeugtyp", False),
        ("fin_nummer", "FIN", False),
        ("auftragsnummer", "Auftragsnummer", False),
        ("annahme_datum", "Auftrags-/Annahmedatum", True),
        ("fertig_datum", "Fertig-bis-Datum", True),
    ):
        ai_value = (ai_fields or {}).get(key)
        local_value = (local_fields or {}).get(key)
        if values_disagree(ai_value, local_value, is_date=is_date):
            review_notes.append(
                f"{label}: OCR und KI liefern unterschiedliche Werte. Bitte Originaldatei prüfen."
            )

    for key, value in (local_fields or {}).items():
        if value and not clean_text(fields.get(key)):
            fields[key] = value

    local_analysis = clean_text((local_fields or {}).get("analyse_text"))
    ai_analysis = clean_text(fields.get("analyse_text"))
    if local_analysis and looks_like_specific_work_text(local_analysis):
        ai_is_generic = (
            not looks_like_specific_work_text(ai_analysis)
            or "durchgefuehrt" in normalize_document_text(ai_analysis)
        )
        if ai_is_generic:
            fields["analyse_text"] = local_analysis

    fields = quality_check_document_fields(fields)
    for note in review_notes:
        add_analysis_note(fields, note)
    return fields


def normalized_review_value(key, value):
    value = clean_text(value)
    if not value:
        return ""
    if key in {"annahme_datum", "fertig_datum", "abholtermin", "start_datum"}:
        return format_date(value)
    if key in {"kennzeichen", "fin_nummer"}:
        return value.upper()
    return value


def values_match_for_review(key, left, right):
    left_value = normalized_review_value(key, left)
    right_value = normalized_review_value(key, right)
    if not left_value or not right_value:
        return False
    return left_value == right_value


def list_document_review_items(auftrag_id, auftrag=None):
    if not auftrag:
        auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        return []

    db = get_db()
    rows = db.execute(
        """
        SELECT id, original_name, dokument_typ, extrahierter_text, analyse_json,
               analyse_quelle, analyse_hinweis
        FROM dateien
        WHERE auftrag_id=?
          AND kategorie='standard'
          AND reklamation_id IS NULL
          AND (extrahierter_text != '' OR analyse_json != '')
        ORDER BY id DESC
        """,
        (auftrag_id,),
    ).fetchall()
    db.close()

    reviews = []
    for row in rows:
        datei = dict(row)
        ai_felder = load_saved_analysis_json(datei.get("analyse_json"))
        local_felder = parse_document_fields(
            datei.get("extrahierter_text"),
            datei.get("original_name"),
        )
        felder = merge_document_fields(ai_felder, local_felder)
        items = []
        for key, label in DOCUMENT_REVIEW_FIELDS:
            value = normalized_review_value(key, felder.get(key))
            if not value:
                continue
            current_value = normalized_review_value(key, auftrag.get(key))
            items.append(
                {
                    "key": key,
                    "label": label,
                    "value": value,
                    "current_value": current_value,
                    "active": values_match_for_review(key, value, current_value),
                }
            )
        if items:
            reviews.append(
                {
                    "datei_id": datei["id"],
                    "original_name": clean_text(datei.get("original_name")),
                    "dokument_typ": clean_text(datei.get("dokument_typ")),
                    "analyse_quelle": clean_text(datei.get("analyse_quelle")),
                    "analyse_hinweis": clean_text(datei.get("analyse_hinweis"))
                    or clean_text(felder.get("analyse_hinweis")),
                    "needs_review": bool(felder.get("analyse_pruefen")),
                    "confidence": felder.get("analyse_confidence") or 0,
                    "items": items,
                }
            )
    return reviews


def apply_document_data_to_auftrag(auftrag_id, prefer_documents=False):
    db = get_db()
    auftrag_row = db.execute("SELECT * FROM auftraege WHERE id=?", (auftrag_id,)).fetchone()
    if not auftrag_row:
        db.close()
        return {}

    auftrag = dict(auftrag_row)
    dateien = db.execute(
        """
        SELECT original_name, extrahierter_text, analyse_json, analyse_hinweis
        FROM dateien
        WHERE auftrag_id=? AND (extrahierter_text != '' OR analyse_json != '')
        ORDER BY id DESC
        """,
        (auftrag_id,),
    ).fetchall()

    erkannt = {}
    for datei in dateien:
        ai_felder = load_saved_analysis_json(datei["analyse_json"])
        local_felder = parse_document_fields(datei["extrahierter_text"], datei["original_name"])
        felder = merge_document_fields(ai_felder, local_felder)
        for key, value in felder.items():
            if value and key not in erkannt:
                erkannt[key] = value

    updates = {}
    if erkannt.get("autohaus_id") and not auftrag.get("autohaus_id"):
        updates["autohaus_id"] = erkannt["autohaus_id"]
    if erkannt.get("fahrzeug") and (
        prefer_documents or should_replace_fahrzeug(auftrag.get("fahrzeug"))
    ):
        updates["fahrzeug"] = erkannt["fahrzeug"]
    if erkannt.get("fin_nummer") and (
        prefer_documents or not clean_text(auftrag.get("fin_nummer"))
    ):
        updates["fin_nummer"] = erkannt["fin_nummer"]
    if erkannt.get("auftragsnummer") and (
        prefer_documents or not clean_text(auftrag.get("auftragsnummer"))
    ):
        updates["auftragsnummer"] = erkannt["auftragsnummer"]
    if erkannt.get("rep_max_kosten") and (
        prefer_documents or not clean_text(auftrag.get("rep_max_kosten"))
    ):
        updates["rep_max_kosten"] = erkannt["rep_max_kosten"]
    if erkannt.get("bauteile_override") and (
        prefer_documents or not clean_text(auftrag.get("bauteile_override"))
    ):
        updates["bauteile_override"] = erkannt["bauteile_override"]
    if erkannt.get("kennzeichen") and (
        prefer_documents or not clean_text(auftrag.get("kennzeichen"))
    ):
        updates["kennzeichen"] = erkannt["kennzeichen"]
    if erkannt.get("annahme_datum") and (
        prefer_documents or not clean_text(auftrag.get("annahme_datum"))
    ):
        updates["annahme_datum"] = erkannt["annahme_datum"]
    if erkannt.get("fertig_datum") and (
        prefer_documents or not clean_text(auftrag.get("fertig_datum"))
    ):
        updates["fertig_datum"] = erkannt["fertig_datum"]
    if erkannt.get("fertig_datum") and (
        prefer_documents or not clean_text(auftrag.get("abholtermin"))
    ):
        updates["abholtermin"] = erkannt["fertig_datum"]
    if erkannt.get("analyse_text") and (
        prefer_documents
        or not clean_text(auftrag.get("analyse_text"))
        or len(clean_text(auftrag.get("analyse_text"))) < 10
    ):
        updates["analyse_text"] = erkannt["analyse_text"][:220]
    if erkannt.get("beschreibung") and (
        prefer_documents or not clean_text(auftrag.get("beschreibung"))
    ):
        updates["beschreibung"] = erkannt["beschreibung"]
    if "analyse_pruefen" in erkannt:
        updates["analyse_pruefen"] = 1 if erkannt.get("analyse_pruefen") else 0
    if erkannt.get("analyse_hinweis"):
        updates["analyse_hinweis"] = erkannt["analyse_hinweis"]
    if erkannt.get("analyse_confidence") is not None:
        updates["analyse_confidence"] = erkannt.get("analyse_confidence") or 0

    if updates:
        updates["geaendert_am"] = now_str()
        assignments = ", ".join(f"{feld}=?" for feld in updates)
        db.execute(
            f"UPDATE auftraege SET {assignments} WHERE id=?",
            tuple(updates.values()) + (auftrag_id,),
        )
        db.commit()

    db.close()
    return updates


def get_status_log(auftrag_id):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM status_log WHERE auftrag_id=? ORDER BY zeitstempel ASC, id ASC",
        (auftrag_id,),
    ).fetchall()
    db.close()
    return rows


def list_verzoegerungen(auftrag_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT * FROM verzoegerungen
        WHERE auftrag_id=?
        ORDER BY uebernommen ASC, erstellt_am DESC, id DESC
        """,
        (auftrag_id,),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def get_verzoegerung(verzoegerung_id):
    db = get_db()
    row = db.execute(
        """
        SELECT v.*, a.autohaus_id
        FROM verzoegerungen v
        JOIN auftraege a ON a.id = v.auftrag_id
        WHERE v.id=?
        """,
        (verzoegerung_id,),
    ).fetchone()
    db.close()
    return dict(row) if row else None


def list_reklamationen(auftrag_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT * FROM reklamationen
        WHERE auftrag_id=?
        ORDER BY bearbeitet ASC, erstellt_am DESC, id DESC
        """,
        (auftrag_id,),
    ).fetchall()
    db.close()
    reklamationen = [dict(row) for row in rows]
    for reklamation in reklamationen:
        reklamation["dateien"] = list_dateien_by_reklamation(reklamation["id"])
    return reklamationen


def list_offene_reklamationen():
    db = get_db()
    rows = db.execute(
        """
        SELECT r.*, a.fahrzeug, a.kennzeichen, h.name AS autohaus_name
        FROM reklamationen r
        JOIN auftraege a ON a.id = r.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE r.bearbeitet = 0
        ORDER BY r.erstellt_am DESC, r.id DESC
        """
    ).fetchall()
    db.close()
    reklamationen = [dict(row) for row in rows]
    for reklamation in reklamationen:
        reklamation["dateien"] = list_dateien_by_reklamation(reklamation["id"])
    return reklamationen


def get_reklamation(reklamation_id):
    db = get_db()
    row = db.execute(
        """
        SELECT r.*, a.autohaus_id
        FROM reklamationen r
        JOIN auftraege a ON a.id = r.auftrag_id
        WHERE r.id=?
        """,
        (reklamation_id,),
    ).fetchone()
    db.close()
    return dict(row) if row else None


def list_dateien_by_reklamation(reklamation_id):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM dateien WHERE reklamation_id=? ORDER BY hochgeladen_am DESC, id DESC",
        (reklamation_id,),
    ).fetchall()
    db.close()
    return [hydrate_datei(dict(row)) for row in rows]


def save_uploads(auftrag_id, files, quelle, kategorie="standard", reklamation_id=None):
    saved = 0
    saved_analysis_document = False
    db = get_db()
    timestamp = now_str()
    for file in files:
        if not file or not file.filename:
            continue
        original_name = secure_filename(file.filename)
        if not original_name or not allowed_file(original_name):
            continue
        suffix = pathlib.Path(original_name).suffix.lower()
        stored_name = f"{uuid.uuid4().hex}{suffix}"
        target = UPLOAD_DIR / stored_name
        file.save(target)
        mime_type = file.mimetype or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
        dokument_typ = ""
        extrahierter_text = ""
        extrakt_kurz = ""
        analyse_quelle = ""
        analyse_json = ""
        analyse_hinweis = ""
        is_analysis_document = (
            clean_text(kategorie) == "standard" and reklamation_id is None
        )
        if is_analysis_document and (suffix == ".pdf" or suffix in IMAGE_EXTENSIONS):
            saved_analysis_document = True
            bundle = build_document_analysis_bundle(target, original_name)
            extrahierter_text = bundle.get("text", "")
            analyse_quelle = clean_text(bundle.get("source"))
            analyse_json = clean_text(bundle.get("analysis_json"))
            analyse_hinweis = clean_text(bundle.get("hint"))
            dokument_typ = (
                classify_document(extrahierter_text, original_name)
                if extrahierter_text
                else classify_document("", original_name)
            )
            extrakt_kurz = summarize_document_text(extrahierter_text, original_name)
        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, reklamation_id, original_name, stored_name, mime_type, size, quelle, kategorie, dokument_typ,
             extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                auftrag_id,
                reklamation_id,
                original_name,
                stored_name,
                mime_type,
                target.stat().st_size,
                quelle,
                kategorie,
                dokument_typ,
                extrahierter_text,
                extrakt_kurz,
                analyse_quelle,
                analyse_json,
                analyse_hinweis,
                timestamp,
            ),
        )     
        saved += 1
    db.commit()
    db.close()
    if saved_analysis_document:
        return saved, apply_document_data_to_auftrag(auftrag_id, prefer_documents=True)
    return saved, {}


def flash_upload_analysis_result(saved_result, success_message="Datei hochgeladen."):
    if isinstance(saved_result, tuple):
        saved, updates = saved_result
    else:
        saved, updates = saved_result, {}
    if not saved:
        return saved
    meaningful_updates = {
        key: value
        for key, value in (updates or {}).items()
        if key not in {"geaendert_am", "analyse_pruefen", "analyse_confidence"}
        and clean_text(value)
    }
    if meaningful_updates:
        flash(success_message, "success")
    else:
        flash(
            "Datei hochgeladen, aber keine sicheren Fahrzeugdaten erkannt. Bitte Felder manuell pruefen.",
            "warning",
        )
    return saved


def reanalyze_existing_documents(auftrag_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM dateien
        WHERE auftrag_id=?
          AND kategorie='standard'
          AND reklamation_id IS NULL
        ORDER BY id ASC
        """,
        (auftrag_id,),
    ).fetchall()
    count = 0
    for row in rows:
        datei = dict(row)
        original_name = clean_text(datei.get("original_name"))
        suffix = pathlib.Path(original_name).suffix.lower()
        if suffix != ".pdf" and suffix not in IMAGE_EXTENSIONS:
            continue
        path = UPLOAD_DIR / clean_text(datei.get("stored_name"))
        if not path.exists():
            continue
        bundle = build_document_analysis_bundle(path, original_name)
        extracted_text = clean_text(bundle.get("text"))
        doc_type = (
            classify_document(extracted_text, original_name)
            if extracted_text
            else classify_document("", original_name)
        )
        db.execute(
            """
            UPDATE dateien
            SET dokument_typ=?,
                extrahierter_text=?,
                extrakt_kurz=?,
                analyse_quelle=?,
                analyse_json=?,
                analyse_hinweis=?
            WHERE id=?
            """,
            (
                doc_type,
                extracted_text,
                summarize_document_text(extracted_text, original_name),
                clean_text(bundle.get("source")),
                clean_text(bundle.get("analysis_json")),
                clean_text(bundle.get("hint")),
                datei["id"],
            ),
        )
        count += 1
    db.commit()
    db.close()
    updates = apply_document_data_to_auftrag(auftrag_id, prefer_documents=True) if count else {}
    return count, updates


def get_allowed_uploads(files):
    uploads = []
    for file in files or []:
        if not file or not file.filename:
            continue
        original_name = secure_filename(file.filename)
        if not original_name or not allowed_file(original_name):
            continue
        uploads.append(file)
    return uploads


def add_reklamation(auftrag_id, quelle, meldung):
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO reklamationen (auftrag_id, quelle, meldung, bearbeitet, erstellt_am, bearbeitet_am)
        VALUES (?, ?, ?, 0, ?, '')
        """,
        (auftrag_id, quelle, clean_text(meldung), now_str()),
    )
    db.commit()
    reklamation_id = cursor.lastrowid
    db.close()
    return reklamation_id


def set_reklamation_status(reklamation_id, bearbeitet):
    db = get_db()
    db.execute(
        """
        UPDATE reklamationen
        SET bearbeitet=?, bearbeitet_am=?
        WHERE id=?
        """,
        (1 if bearbeitet else 0, now_str() if bearbeitet else "", reklamation_id),
    )
    db.commit()
    db.close()


def archive_auftrag(auftrag_id, archiviert=1):
    db = get_db()
    db.execute(
        "UPDATE auftraege SET archiviert=?, geaendert_am=? WHERE id=?",
        (archiviert, now_str(), auftrag_id),
    )
    db.commit()
    db.close()


def archive_auftraege(auftrag_ids, archiviert=1, autohaus_id=None):
    geaendert = 0
    for raw_id in auftrag_ids:
        try:
            auftrag_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        auftrag = get_auftrag(auftrag_id)
        if not auftrag:
            continue
        if autohaus_id is not None and auftrag.get("autohaus_id") != autohaus_id:
            continue
        archive_auftrag(auftrag_id, archiviert)
        geaendert += 1
    return geaendert


def delete_auftraege(auftrag_ids, autohaus_id=None):
    geloescht = 0
    for raw_id in auftrag_ids:
        try:
            auftrag_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        auftrag = get_auftrag(auftrag_id)
        if not auftrag:
            continue
        if autohaus_id is not None and auftrag.get("autohaus_id") != autohaus_id:
            continue
        delete_auftrag(auftrag_id)
        geloescht += 1
    return geloescht


def delete_auftrag(auftrag_id):
    db = get_db()
    dateien = db.execute(
        "SELECT stored_name FROM dateien WHERE auftrag_id=?",
        (auftrag_id,),
    ).fetchall()
    for datei in dateien:
        stored_name = clean_text(datei["stored_name"])
        if not stored_name:
            continue
        path = UPLOAD_DIR / pathlib.Path(stored_name).name
        try:
            if path.exists() and path.is_file() and path.parent == UPLOAD_DIR:
                path.unlink()
        except OSError:
            pass
    db.execute("DELETE FROM dateien WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM reklamationen WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM verzoegerungen WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM status_log WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM auftraege WHERE id=?", (auftrag_id,))
    db.commit()
    db.close()


def create_auftrag(
    quelle,
    autohaus_id=None,
    kunde_name="",
    fahrzeug="",
    fin_nummer="",
    auftragsnummer="",
    rep_max_kosten="",
    werkstatt_angebot_text="",
    werkstatt_angebot_preis="",
    kennzeichen="",
    beschreibung="",
    analyse="",
    annahme_datum="",
    start_datum="",
    fertig_datum="",
    abholtermin="",
    transport_art="standard",
    kontakt_telefon="",
    notiz_intern="",
    angebotsphase=0,
    angebot_abgesendet=0,
):
    jetzt = now_str()
    db = get_db()
    db.execute(
        """
        INSERT INTO auftraege
        (token, kunde_email, autohaus_id, kunde_name, fahrzeug, fin_nummer, auftragsnummer, rep_max_kosten, bauteile_override, kennzeichen,
         beschreibung, analyse_text, angebotsphase, angebot_abgesendet, angebot_status, werkstatt_angebot_text, werkstatt_angebot_preis, werkstatt_angebot_am, status, annahme_datum, start_datum, fertig_datum, abholtermin, transport_art,
         kontakt_telefon, notiz_intern, quelle, erstellt_am, geaendert_am)
        VALUES (?, '', ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, '', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex[:12],
            autohaus_id,
            kunde_name,
            fahrzeug or "Neues Fahrzeug",
            fin_nummer,
            auftragsnummer,
            rep_max_kosten,
            kennzeichen,
            beschreibung,
            analyse,
            1 if angebotsphase else 0,
            1 if angebot_abgesendet else 0,
            "angefragt" if angebot_abgesendet else ("entwurf" if angebotsphase else ""),
            werkstatt_angebot_text,
            werkstatt_angebot_preis,
            annahme_datum,
            start_datum,
            fertig_datum,
            abholtermin,
            transport_art,
            kontakt_telefon,
            notiz_intern,
            quelle,
            jetzt,
            jetzt,
        ),
    )
    auftrag_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.execute(
        "INSERT INTO status_log (auftrag_id, status, zeitstempel) VALUES (?, 1, ?)",
        (auftrag_id, jetzt),
    )
    db.commit()
    db.close()
    return auftrag_id


def add_verzoegerung(
    auftrag_id,
    quelle,
    meldung,
    start_datum="",
    fertig_datum="",
    abholtermin="",
    uebernommen=0,
):
    db = get_db()
    db.execute(
        """
        INSERT INTO verzoegerungen
        (auftrag_id, quelle, meldung, vorgeschlagen_start, vorgeschlagen_fertig,
         vorgeschlagen_abholung, uebernommen, erstellt_am)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            auftrag_id,
            quelle,
            clean_text(meldung),
            format_date(start_datum),
            format_date(fertig_datum),
            format_date(abholtermin),
            uebernommen,
            now_str(),
        ),
    )
    db.commit()
    db.close()


def apply_delay_to_order(auftrag_id, start_datum="", fertig_datum="", abholtermin=""):
    start_clean = format_date(start_datum)
    fertig_clean = format_date(fertig_datum)
    abholung_clean = format_date(abholtermin)

    updates = []
    values = []
    if start_clean:
        updates.append("start_datum=?")
        values.append(start_clean)
    if fertig_clean:
        updates.append("fertig_datum=?")
        values.append(fertig_clean)
    if abholung_clean:
        updates.append("abholtermin=?")
        values.append(abholung_clean)
    if not updates:
        return

    updates.append("geaendert_am=?")
    values.append(now_str())
    values.append(auftrag_id)

    db = get_db()
    db.execute(f"UPDATE auftraege SET {', '.join(updates)} WHERE id=?", tuple(values))
    db.commit()
    db.close()


def angebot_annehmen(auftrag_id):
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET angebotsphase=0,
            angebot_status='angenommen',
            geaendert_am=?
        WHERE id=?
        """,
        (now_str(), auftrag_id),
    )
    db.commit()
    db.close()


def refresh_offer_texts(auftrag_id, customer_short="", customer_long=""):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        return {}
    doc_analysis_values = []
    doc_description_values = []
    for datei in list_dateien(auftrag_id):
        parsed = load_saved_analysis_json(datei.get("analyse_json"))
        if not parsed:
            parsed = parse_document_fields(
                clean_text(datei.get("extrahierter_text")),
                clean_text(datei.get("original_name")),
            )
        if clean_text(parsed.get("analyse_text")):
            doc_analysis_values.append(clean_text(parsed["analyse_text"]))
        if clean_text(parsed.get("beschreibung")):
            doc_description_values.append(clean_text(parsed["beschreibung"]))
    doc_analysis = " / ".join(dict.fromkeys(doc_analysis_values))[:220]
    doc_description = " ".join(dict.fromkeys(doc_description_values))[:700]
    final_short, final_long = build_offer_texts(customer_short, customer_long, doc_analysis, doc_description)
    updates = {}
    if final_short:
        updates["analyse_text"] = final_short
    if final_long:
        updates["beschreibung"] = final_long
    if not updates:
        return {}
    updates["geaendert_am"] = now_str()
    db = get_db()
    assignments = ", ".join(f"{field}=?" for field in updates)
    db.execute(
        f"UPDATE auftraege SET {assignments} WHERE id=?",
        tuple(updates.values()) + (auftrag_id,),
    )
    db.commit()
    db.close()
    return updates


def submit_offer_request(auftrag_id):
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET angebot_abgesendet=1,
            angebot_status='angefragt',
            geaendert_am=?
        WHERE id=? AND angebotsphase=1 AND angebot_status!='angebot_abgegeben'
        """,
        (now_str(), auftrag_id),
    )
    db.commit()
    db.close()


def send_workshop_offer(auftrag_id, angebot_text, angebot_preis):
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET werkstatt_angebot_text=?,
            werkstatt_angebot_preis=?,
            werkstatt_angebot_am=?,
            angebot_status='angebot_abgegeben',
            geaendert_am=?
        WHERE id=? AND angebotsphase=1
        """,
        (
            clean_text(angebot_text),
            clean_text(angebot_preis),
            now_str(),
            now_str(),
            auftrag_id,
        ),
    )
    db.commit()
    db.close()


def dashboard_daten(auftraege):
    heute = date.today()
    offene_verzoegerungen = []
    offene_reklamationen = []

    db = get_db()
    rows = db.execute(
        """
        SELECT v.*, a.fahrzeug, a.kennzeichen, h.name AS autohaus_name
        FROM verzoegerungen v
        JOIN auftraege a ON a.id = v.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE v.uebernommen = 0
        ORDER BY v.erstellt_am DESC, v.id DESC
        """
    ).fetchall()
    offene_verzoegerungen = [dict(row) for row in rows]
    rows = db.execute(
        """
        SELECT r.*, a.fahrzeug, a.kennzeichen, h.name AS autohaus_name
        FROM reklamationen r
        JOIN auftraege a ON a.id = r.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE r.bearbeitet = 0
        ORDER BY r.erstellt_am DESC, r.id DESC
        """
    ).fetchall()
    db.close()
    offene_reklamationen = [dict(row) for row in rows]
    for reklamation in offene_reklamationen:
        reklamation["dateien"] = list_dateien_by_reklamation(reklamation["id"])

    heute_bringen = [a for a in auftraege if a["annahme_datum_obj"] == heute]
    heute_abholen = [a for a in auftraege if a["abholtermin_obj"] == heute]
    heute_starten = [a for a in auftraege if a["start_datum_obj"] == heute]
    heute_fertig = [a for a in auftraege if a["fertig_datum_obj"] == heute]
    ueberfaellig = [
        a for a in auftraege if a["fertig_datum_obj"] and a["fertig_datum_obj"] < heute and a["status"] < 4
    ]

    naechste_events = []
    for auftrag in auftraege:
        for feld, label, farbe in EVENT_FELDER:
            event_date = auftrag.get(f"{feld}_obj")
            if not event_date or event_date < heute:
                continue
            naechste_events.append(
                {
                    "datum": event_date,
                    "datum_text": auftrag[feld],
                    "label": label,
                    "farbe": farbe,
                    "auftrag": auftrag,
                }
            )
    naechste_events.sort(
        key=lambda item: (
            item["datum"],
            clean_text(item["auftrag"].get("autohaus_name")).lower(),
            clean_text(item["auftrag"].get("kennzeichen")).lower(),
        )
    )

    return {
        "heute_text": format_date(heute.strftime("%Y-%m-%d")),
        "heute_bringen": heute_bringen,
        "heute_abholen": heute_abholen,
        "heute_starten": heute_starten,
        "heute_fertig": heute_fertig,
        "ueberfaellig": ueberfaellig,
        "offene_verzoegerungen": offene_verzoegerungen,
        "offene_reklamationen": offene_reklamationen,
        "naechste_events": naechste_events[:12],
    }


def kalender_daten(auftraege):
    tage = defaultdict(dict)
    for auftrag in auftraege:
        for feld, label, farbe in EVENT_FELDER:
            event_date = auftrag.get(f"{feld}_obj")
            if event_date:
                eintraege = tage[event_date]
                eintrag = eintraege.get(auftrag["id"])
                if not eintrag:
                    eintrag = {
                        "auftrag": auftrag,
                        "termine": [],
                        "felder": set(),
                    }
                    eintraege[auftrag["id"]] = eintrag
                if feld not in eintrag["felder"]:
                    eintrag["termine"].append(
                        {"feld": feld, "label": label, "farbe": farbe}
                    )
                    eintrag["felder"].add(feld)

    kalender = []
    for tag in sorted(tage.keys()):
        events = sorted(
            tage[tag].values(),
            key=lambda item: (
                clean_text(item["auftrag"].get("autohaus_name")).lower(),
                clean_text(item["auftrag"].get("kennzeichen")).lower(),
            ),
        )
        kalender.append(
            {
                "datum_lang": day_label(tag),
                "datum_text": tag.strftime(DATE_FMT),
                "events": events,
            }
        )
    return kalender


def autohaus_dashboard_daten(auftraege):
    heute = date.today()
    return {
        "heute_text": format_date(heute.strftime("%Y-%m-%d")),
        "heute_bringen": [a for a in auftraege if a["annahme_datum_obj"] == heute],
        "heute_abholen": [a for a in auftraege if a["abholtermin_obj"] == heute],
        "in_arbeit": [
            a
            for a in auftraege
            if a["status"] == 3 or (a["start_datum_obj"] and a["start_datum_obj"] <= heute and a["status"] < 4)
        ],
        "heute_fertig": [a for a in auftraege if a["fertig_datum_obj"] == heute],
        "zurueckgegeben": [a for a in auftraege if a["status"] == 4 and a["abholtermin_obj"] and a["abholtermin_obj"] < heute],
    }


@app.template_filter("iso_date")
def iso_date_filter(value):
    return iso_date(value)


def admin_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)

    return wrapper


def partner_session_required(slug):
    autohaus = get_autohaus_by_slug(slug)
    if not autohaus:
        abort(404)
    if session.get("partner_autohaus_id") != autohaus["id"]:
        return None, redirect(url_for("partner_login_slug", slug=slug))
    return autohaus, None


def partner_session_required_by_key(portal_key):
    autohaus = get_autohaus_by_portal_key(portal_key)
    if not autohaus:
        abort(404)
    if session.get("partner_autohaus_id") != autohaus["id"]:
        return None, redirect(url_for("partner_login_key", portal_key=portal_key))
    return autohaus, None


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if clean_text(request.form.get("passwort")) == get_admin_pass():
            session["admin"] = True
            return redirect(url_for("dashboard"))
        flash("Falsches Passwort.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@app.route("/admin")
@admin_required
def dashboard():
    auftraege = list_auftraege()
    archivierte_auftraege = list_auftraege(include_archived=True)
    archivierte_auftraege = [a for a in archivierte_auftraege if a["archiviert"]]
    return render_template(
        "dashboard.html",
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        angebotsanfragen=list_angebotsanfragen(),
        autohaeuser=list_autohaeuser(),
        cockpit=dashboard_daten(auftraege),
        ki_status=get_ai_status(),
        startup_warnings=get_startup_warnings(),
        statusliste=STATUSLISTE,
    )


@app.route("/admin/autohaus/neu", methods=["POST"])
@admin_required
def autohaus_neu():
    name = clean_text(request.form.get("name"))
    if not name:
        flash("Bitte einen Autohaus-Namen eintragen.", "warning")
        return redirect(url_for("dashboard"))

    slug = get_unique_slug(name)
    zugangscode = clean_text(request.form.get("zugangscode")) or uuid.uuid4().hex[:8].upper()
    portal_key = uuid.uuid4().hex[:16]

    db = get_db()
    db.execute(
        """
        INSERT INTO autohaeuser
        (name, slug, portal_key, kontakt_name, email, telefon, strasse, plz, ort, zugangscode,
         portal_titel, willkommen_text, notiz, erstellt_am)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            slug,
            portal_key,
            clean_text(request.form.get("kontakt_name")),
            clean_text(request.form.get("email")),
            clean_text(request.form.get("telefon")),
            clean_text(request.form.get("strasse")),
            clean_text(request.form.get("plz")),
            clean_text(request.form.get("ort")),
            zugangscode,
            clean_text(request.form.get("portal_titel")),
            clean_text(request.form.get("willkommen_text")),
            clean_text(request.form.get("notiz")),
            now_str(),
        ),
    )
    db.commit()
    db.close()
    flash(f"Autohaus angelegt. Portal-Link: /portal/{portal_key}", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/autohaus/<int:autohaus_id>/update", methods=["POST"])
@admin_required
def autohaus_update(autohaus_id):
    autohaus = get_autohaus(autohaus_id)
    if not autohaus:
        abort(404)

    db = get_db()
    db.execute(
        """
        UPDATE autohaeuser
        SET name=?,
            kontakt_name=?,
            email=?,
            telefon=?,
            strasse=?,
            plz=?,
            ort=?,
            zugangscode=?,
            portal_titel=?,
            willkommen_text=?,
            notiz=?
        WHERE id=?
        """,
        (
            clean_text(request.form.get("name")) or autohaus["name"],
            clean_text(request.form.get("kontakt_name")),
            clean_text(request.form.get("email")),
            clean_text(request.form.get("telefon")),
            clean_text(request.form.get("strasse")),
            clean_text(request.form.get("plz")),
            clean_text(request.form.get("ort")),
            clean_text(request.form.get("zugangscode")) or autohaus["zugangscode"],
            clean_text(request.form.get("portal_titel")),
            clean_text(request.form.get("willkommen_text")),
            clean_text(request.form.get("notiz")),
            autohaus_id,
        ),
    )
    db.commit()
    db.close()
    flash("Autohaus aktualisiert.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/neu", methods=["GET", "POST"])
@admin_required
def neuer_auftrag():
    autohaeuser = list_autohaeuser()
    if request.method == "POST":
        form = request.form
        aktion = form.get("aktion", "speichern")
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        if aktion == "upload_analyze" and not any(file and file.filename for file in dateien):
            flash("Bitte zuerst eine Datei auswählen.", "warning")
            return render_template(
                "neu.html",
                autohaeuser=autohaeuser,
                transport_arten=TRANSPORT_ARTEN,
            )
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return render_template(
                "neu.html",
                autohaeuser=autohaeuser,
                transport_arten=TRANSPORT_ARTEN,
            )

        auftrag_id = create_auftrag(
            "intern",
            autohaus_id=int(form.get("autohaus_id")) if form.get("autohaus_id") else None,
            kunde_name=clean_text(form.get("kunde_name")),
            fahrzeug=clean_text(form.get("fahrzeug")),
            kennzeichen=clean_text(form.get("kennzeichen")).upper(),
            beschreibung=clean_text(form.get("beschreibung")),
            analyse=clean_text(form.get("analyse_text")) or analyse_text(form.get("beschreibung")),
            annahme_datum=format_date(form.get("annahme_datum")),
            start_datum=format_date(form.get("start_datum")),
            fertig_datum=format_date(form.get("fertig_datum")),
            abholtermin=format_date(form.get("abholtermin")),
            transport_art=clean_text(form.get("transport_art")) or "standard",
            kontakt_telefon=clean_text(form.get("kontakt_telefon")),
            notiz_intern=clean_text(form.get("notiz_intern")),
        )
        upload_result = save_uploads(auftrag_id, erlaubte_dateien, "intern", "standard")
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Auftrag automatisch befuellt.",
            )
        else:
            flash("Fahrzeug angelegt.", "success")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    return render_template(
        "neu.html",
        autohaeuser=autohaeuser,
        transport_arten=TRANSPORT_ARTEN,
    )


@app.route("/admin/auftrag/<int:auftrag_id>", methods=["GET", "POST"])
@admin_required
def auftrag_detail(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)

    autohaeuser = list_autohaeuser()
    if request.method == "POST":
        form = request.form
        aktion = form.get("aktion", "speichern")
        analyse = clean_text(form.get("analyse_text")) or analyse_text(form.get("beschreibung"))
        db = get_db()
        db.execute(
            """
            UPDATE auftraege
            SET autohaus_id=?,
                kunde_name=?,
                fahrzeug=?,
                fin_nummer=?,
                auftragsnummer=?,
                bauteile_override=?,
                kennzeichen=?,
                beschreibung=?,
                analyse_text=?,
                annahme_datum=?,
                start_datum=?,
                fertig_datum=?,
                abholtermin=?,
                transport_art=?,
                kontakt_telefon=?,
                notiz_intern=?,
                geaendert_am=?
            WHERE id=?
            """,
            (
                int(form.get("autohaus_id")) if form.get("autohaus_id") else None,
                clean_text(form.get("kunde_name")),
                clean_text(form.get("fahrzeug")),
                clean_text(form.get("fin_nummer")).upper(),
                clean_text(form.get("auftragsnummer")),
                clean_text(form.get("bauteile_override")),
                clean_text(form.get("kennzeichen")).upper(),
                clean_text(form.get("beschreibung")),
                analyse,
                format_date(form.get("annahme_datum")),
                format_date(form.get("start_datum")),
                format_date(form.get("fertig_datum")),
                format_date(form.get("abholtermin")),
                clean_text(form.get("transport_art")) or "standard",
                clean_text(form.get("kontakt_telefon")),
                clean_text(form.get("notiz_intern")),
                now_str(),
                auftrag_id,
            ),
        )
        db.commit()
        db.close()
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        if aktion == "reanalyze_existing":
            count, _ = reanalyze_existing_documents(auftrag_id)
            if count:
                flash(f"{count} vorhandene Unterlage(n) neu analysiert.", "success")
            else:
                flash("Keine auswertbaren vorhandenen Unterlagen gefunden.", "warning")
            return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
        if aktion == "upload_analyze" and not any(file and file.filename for file in dateien):
            flash("Bitte zuerst eine Datei auswählen.", "warning")
            return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
        upload_result = save_uploads(auftrag_id, erlaubte_dateien, "intern", "standard")
        save_uploads(
            auftrag_id,
            get_allowed_uploads(request.files.getlist("fertigbilder")),
            "intern",
            "fertigbild",
        )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Auftrag neu analysiert.",
            )
        else:
            flash("Auftrag aktualisiert.", "success")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    dateien = list_dateien(auftrag_id)
    standard_dateien = dateien_mit_kategorie(dateien, "standard")
    fertigbilder = dateien_mit_kategorie(dateien, "fertigbild")
    return render_template(
        "auftrag_detail.html",
        auftrag=auftrag,
        autohaeuser=autohaeuser,
        transport_arten=TRANSPORT_ARTEN,
        statusliste=STATUSLISTE,
        log=get_status_log(auftrag_id),
        dateien=standard_dateien,
        fertigbilder=fertigbilder,
        dokument_pruefung=list_document_review_items(auftrag_id, auftrag),
        reklamationen=list_reklamationen(auftrag_id),
        verzoegerungen=list_verzoegerungen(auftrag_id),
    )


@app.route("/admin/auftrag/<int:auftrag_id>/fertigbilder", methods=["POST"])
@admin_required
def admin_fertigbilder_upload(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)

    erlaubte_dateien = get_allowed_uploads(request.files.getlist("fertigbilder"))
    if not erlaubte_dateien:
        flash("Bitte JPG, PNG, WebP oder PDF als Fertigbild auswählen.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    gespeichert, _ = save_uploads(auftrag_id, erlaubte_dateien, "intern", "fertigbild")
    if gespeichert:
        flash(f"{gespeichert} Fertigbild(er) hochgeladen. Das Autohaus sieht sie im Portal.", "success")
    else:
        flash("Es wurde kein Fertigbild gespeichert.", "warning")
    return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/status/<int:auftrag_id>/<int:neuer_status>", methods=["POST"])
@admin_required
def status_update(auftrag_id, neuer_status):
    if neuer_status not in STATUSLISTE:
        abort(400)
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)

    heute = format_date(date.today().strftime("%Y-%m-%d"))
    start_datum = auftrag["start_datum"] or heute if neuer_status >= 2 else auftrag["start_datum"]
    fertig_datum = auftrag["fertig_datum"] or heute if neuer_status == 4 else auftrag["fertig_datum"]

    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET status=?, start_datum=?, fertig_datum=?, geaendert_am=?
        WHERE id=?
        """,
        (neuer_status, start_datum, fertig_datum, now_str(), auftrag_id),
    )
    db.execute(
        "INSERT INTO status_log (auftrag_id, status, zeitstempel) VALUES (?, ?, ?)",
        (auftrag_id, neuer_status, now_str()),
    )
    db.commit()
    db.close()
    flash("Status aktualisiert.", "success")
    ziel = clean_text(request.form.get("next"))
    if ziel.startswith("/"):
        return redirect(ziel)
    return redirect(request.referrer or url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/auftrag/<int:auftrag_id>/verzoegerung", methods=["POST"])
@admin_required
def admin_verzoegerung(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    meldung = clean_text(request.form.get("meldung"))
    if not meldung:
        flash("Bitte eine Verzögerungsmeldung eintragen.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    start_datum = request.form.get("start_datum", "")
    fertig_datum = request.form.get("fertig_datum", "")
    abholtermin = request.form.get("abholtermin", "")
    add_verzoegerung(
        auftrag_id,
        "werkstatt",
        meldung,
        start_datum=start_datum,
        fertig_datum=fertig_datum,
        abholtermin=abholtermin,
        uebernommen=1,
    )
    apply_delay_to_order(auftrag_id, start_datum, fertig_datum, abholtermin)
    flash("Verzögerung gespeichert.", "success")
    return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/verzoegerung/uebernehmen/<int:verzoegerung_id>", methods=["POST"])
@admin_required
def verzoegerung_uebernehmen(verzoegerung_id):
    verzoegerung = get_verzoegerung(verzoegerung_id)
    if not verzoegerung:
        abort(404)
    apply_delay_to_order(
        verzoegerung["auftrag_id"],
        verzoegerung.get("vorgeschlagen_start", ""),
        verzoegerung.get("vorgeschlagen_fertig", ""),
        verzoegerung.get("vorgeschlagen_abholung", ""),
    )
    db = get_db()
    db.execute("UPDATE verzoegerungen SET uebernommen=1 WHERE id=?", (verzoegerung_id,))
    db.commit()
    db.close()
    flash("Terminänderung übernommen.", "success")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/admin/angebot/<int:auftrag_id>/annehmen", methods=["POST"])
@admin_required
def angebot_annehmen_route(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or not auftrag.get("angebotsphase"):
        abort(404)
    angebot_annehmen(auftrag_id)
    flash("Angebot angenommen. Der Vorgang läuft jetzt als normaler Auftrag weiter.", "success")
    return redirect(request.referrer or url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/angebot/<int:auftrag_id>/senden", methods=["POST"])
@admin_required
def angebot_senden_route(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or not auftrag.get("angebotsphase"):
        abort(404)
    angebot_text = clean_text(request.form.get("werkstatt_angebot_text"))
    angebot_preis = clean_text(request.form.get("werkstatt_angebot_preis"))
    if not angebot_text and not angebot_preis:
        flash("Bitte Angebotstext oder Preis eintragen.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
    send_workshop_offer(auftrag_id, angebot_text, angebot_preis)
    flash("Angebot an das Autohaus gesendet.", "success")
    return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/reklamation/<int:reklamation_id>/status", methods=["POST"])
@admin_required
def reklamation_status(reklamation_id):
    reklamation = get_reklamation(reklamation_id)
    if not reklamation:
        abort(404)
    bearbeitet = request.form.get("bearbeitet") == "1"
    set_reklamation_status(reklamation_id, bearbeitet)
    flash(
        "Reklamation als bearbeitet markiert." if bearbeitet else "Reklamation wieder geöffnet.",
        "info",
    )
    return redirect(request.referrer or url_for("auftrag_detail", auftrag_id=reklamation["auftrag_id"]))


@app.route("/admin/datei/<int:datei_id>")
@admin_required
def admin_datei(datei_id):
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    path = UPLOAD_DIR / datei["stored_name"]
    if not path.exists():
        abort(404)
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=False,
    )


@app.route("/admin/datei/<int:datei_id>/download")
@admin_required
def admin_datei_download(datei_id):
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    path = UPLOAD_DIR / datei["stored_name"]
    if not path.exists():
        abort(404)
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=True,
    )


@app.route("/admin/loeschen/<int:auftrag_id>", methods=["POST"])
@admin_required
def loeschen(auftrag_id):
    delete_auftrag(auftrag_id)
    flash("Fahrzeug gelöscht.", "info")
    return redirect(url_for("dashboard"))


@app.route("/admin/archivieren/<int:auftrag_id>", methods=["POST"])
@admin_required
def archivieren(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    archive_auftrag(auftrag_id, 0 if auftrag["archiviert"] else 1)
    flash("Auftrag archiviert." if not auftrag["archiviert"] else "Auftrag wieder aktiviert.", "info")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/admin/auftraege/sammelaktion", methods=["POST"])
@admin_required
def admin_sammelaktion():
    aktion = clean_text(request.form.get("aktion"))
    auftrag_ids = request.form.getlist("auftrag_ids")
    if not auftrag_ids:
        flash("Bitte zuerst Fahrzeuge auswählen.", "warning")
        return redirect(request.referrer or url_for("dashboard"))
    if aktion == "archivieren":
        anzahl = archive_auftraege(auftrag_ids, 1)
        flash(f"{anzahl} Auftrag/Aufträge archiviert.", "info")
    elif aktion == "aktivieren":
        anzahl = archive_auftraege(auftrag_ids, 0)
        flash(f"{anzahl} Auftrag/Aufträge wieder aktiviert.", "info")
    elif aktion == "loeschen":
        anzahl = delete_auftraege(auftrag_ids)
        flash(f"{anzahl} Auftrag/Aufträge gelöscht.", "info")
    else:
        flash("Unbekannte Sammelaktion.", "warning")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/admin/kalender")
@admin_required
def kalender():
    return render_template("kalender.html", kalender_items=kalender_daten(list_auftraege()))


@app.route("/portal")
@app.route("/portal/")
def portal_redirect():
    return redirect(url_for("partner_login"))


@app.route("/partner")
def partner_login():
    return render_template("partner_index.html", autohaeuser=list_autohaeuser())


@app.route("/portal/<portal_key>", methods=["GET", "POST"])
def partner_login_key(portal_key):
    autohaus = get_autohaus_by_portal_key(portal_key)
    if not autohaus:
        abort(404)

    if request.method == "POST":
        if clean_text(request.form.get("zugangscode")) == autohaus["zugangscode"]:
            session["partner_autohaus_id"] = autohaus["id"]
            return redirect(url_for("partner_dashboard_key", portal_key=portal_key))
        flash("Falscher Zugangscode.", "danger")

    if session.get("partner_autohaus_id") == autohaus["id"]:
        return redirect(url_for("partner_dashboard_key", portal_key=portal_key))

    return render_template("partner_login.html", autohaus=autohaus)


@app.route("/partner/logout")
def partner_logout():
    session.pop("partner_autohaus_id", None)
    return redirect(url_for("partner_login"))


@app.route("/partner/<slug>", methods=["GET", "POST"])
def partner_login_slug(slug):
    autohaus = get_autohaus_by_slug(slug)
    if not autohaus:
        abort(404)
    return redirect(url_for("partner_login_key", portal_key=autohaus["portal_key"]))


@app.route("/partner/<slug>/dashboard")
def partner_dashboard(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftraege = list_auftraege(autohaus["id"])
    archivierte_auftraege = [
        a for a in list_auftraege(autohaus["id"], include_archived=True) if a["archiviert"]
    ]
    return render_template(
        "partner_dashboard.html",
        autohaus=autohaus,
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        angebotsanfragen=list_angebotsanfragen(autohaus["id"]),
        cockpit=autohaus_dashboard_daten(auftraege),
        statusliste=STATUSLISTE,
    )


@app.route("/portal/<portal_key>/dashboard")
def partner_dashboard_key(portal_key):
    autohaus, redirect_response = partner_session_required_by_key(portal_key)
    if redirect_response:
        return redirect_response
    return redirect(url_for("partner_dashboard", slug=autohaus["slug"]))


@app.route("/partner/<slug>/neu", methods=["GET", "POST"])
def partner_neuer_auftrag(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response

    if request.method == "POST":
        form = request.form
        aktion = form.get("aktion", "speichern")
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        if aktion == "upload_analyze" and not any(file and file.filename for file in dateien):
            flash("Bitte zuerst eine Datei auswählen.", "warning")
            return render_template(
                "partner_neu.html",
                autohaus=autohaus,
                transport_arten=TRANSPORT_ARTEN,
            )
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return render_template(
                "partner_neu.html",
                autohaus=autohaus,
                transport_arten=TRANSPORT_ARTEN,
            )
        beschreibung = clean_text(form.get("beschreibung"))
        analyse = clean_text(form.get("analyse_text")) or analyse_text(beschreibung)
        auftrag_id = create_auftrag(
            "autohaus",
            autohaus_id=autohaus["id"],
            kunde_name=clean_text(form.get("kunde_name")),
            fahrzeug=clean_text(form.get("fahrzeug")),
            kennzeichen=clean_text(form.get("kennzeichen")).upper(),
            beschreibung=beschreibung,
            analyse=analyse,
            annahme_datum=format_date(form.get("annahme_datum")),
            start_datum=format_date(form.get("start_datum")),
            fertig_datum=format_date(form.get("fertig_datum")),
            abholtermin=format_date(form.get("abholtermin")),
            transport_art=clean_text(form.get("transport_art")) or "standard",
            kontakt_telefon=clean_text(form.get("kontakt_telefon")),
        )
        upload_result = save_uploads(auftrag_id, erlaubte_dateien, "autohaus", "standard")
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Auftrag automatisch befuellt.",
            )
        else:
            flash("Fahrzeug angelegt.", "success")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    return render_template(
        "partner_neu.html",
        autohaus=autohaus,
        transport_arten=TRANSPORT_ARTEN,
    )


@app.route("/partner/<slug>/angebot/neu", methods=["GET", "POST"])
def partner_neues_angebot(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response

    if request.method == "POST":
        form = request.form
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        kunden_kurz = beautify_offer_text(form.get("analyse_text"))
        kunden_text = beautify_offer_text(form.get("beschreibung"))
        analyse = kunden_kurz or analyse_text(kunden_text)
        angebot_id = create_auftrag(
            "autohaus",
            autohaus_id=autohaus["id"],
            kunde_name=clean_text(form.get("kunde_name")),
            fahrzeug=clean_text(form.get("fahrzeug")),
            fin_nummer=clean_text(form.get("fin_nummer")).upper(),
            auftragsnummer=clean_text(form.get("auftragsnummer")),
            kennzeichen=clean_text(form.get("kennzeichen")).upper(),
            beschreibung=kunden_text,
            analyse=analyse,
            annahme_datum=format_date(form.get("annahme_datum")),
            abholtermin=format_date(form.get("abholtermin")),
            transport_art=clean_text(form.get("transport_art")) or "standard",
            kontakt_telefon=clean_text(form.get("kontakt_telefon")),
            angebotsphase=1,
            angebot_abgesendet=0,
        )
        upload_result = save_uploads(angebot_id, erlaubte_dateien, "autohaus", "standard")
        refresh_offer_texts(angebot_id, kunden_kurz, kunden_text)
        flash_upload_analysis_result(
            upload_result,
            "Angebotsanfrage analysiert. Bitte prüfen und danach absenden.",
        )
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=angebot_id))

    return render_template(
        "partner_angebot.html",
        autohaus=autohaus,
        angebot=None,
        dateien=[],
        transport_arten=TRANSPORT_ARTEN,
    )


@app.route("/partner/<slug>/angebot/<int:auftrag_id>", methods=["GET", "POST"])
def partner_angebot_detail(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response

    angebot = get_auftrag(auftrag_id)
    if not angebot or angebot.get("autohaus_id") != autohaus["id"]:
        abort(404)
    if not angebot.get("angebotsphase"):
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    if request.method == "POST":
        form = request.form
        aktion = clean_text(form.get("aktion")) or "analyze"
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        kunden_kurz = beautify_offer_text(form.get("analyse_text"))
        kunden_text = beautify_offer_text(form.get("beschreibung"))
        analyse = kunden_kurz or analyse_text(kunden_text)
        bleibt_abgesendet = bool(angebot.get("angebot_abgesendet")) or aktion == "submit_offer"
        db = get_db()
        db.execute(
            """
            UPDATE auftraege
            SET kunde_name=?,
                fahrzeug=?,
                fin_nummer=?,
                auftragsnummer=?,
                bauteile_override=?,
                kennzeichen=?,
                beschreibung=?,
                analyse_text=?,
                annahme_datum=?,
                abholtermin=?,
                transport_art=?,
                kontakt_telefon=?,
                angebot_abgesendet=?,
                geaendert_am=?
            WHERE id=? AND autohaus_id=? AND angebotsphase=1
            """,
            (
                clean_text(form.get("kunde_name")),
                clean_text(form.get("fahrzeug")) or angebot["fahrzeug"],
                clean_text(form.get("fin_nummer")).upper(),
                clean_text(form.get("auftragsnummer")),
                clean_text(form.get("bauteile_override")) or angebot.get("bauteile_override", ""),
                clean_text(form.get("kennzeichen")).upper(),
                kunden_text,
                analyse,
                format_date(form.get("annahme_datum")),
                format_date(form.get("abholtermin")),
                clean_text(form.get("transport_art")) or "standard",
                clean_text(form.get("kontakt_telefon")),
                1 if bleibt_abgesendet else 0,
                now_str(),
                auftrag_id,
                autohaus["id"],
            ),
        )
        db.commit()
        db.close()
        upload_result = save_uploads(auftrag_id, erlaubte_dateien, "autohaus", "standard")
        refresh_offer_texts(auftrag_id, kunden_kurz, kunden_text)
        if aktion == "submit_offer":
            submit_offer_request(auftrag_id)
            flash("Angebotsanfrage abgesendet. Die Werkstatt kann sie jetzt prüfen.", "success")
        else:
            flash_upload_analysis_result(
                upload_result,
                "Angebotsanfrage analysiert. Bitte prüfen und danach absenden.",
            )
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=auftrag_id))

    sichtbare_dateien = [d for d in list_dateien(auftrag_id) if d.get("quelle") in {"autohaus", "intern"}]
    return render_template(
        "partner_angebot.html",
        autohaus=autohaus,
        angebot=angebot,
        dateien=sichtbare_dateien,
        dokument_pruefung=list_document_review_items(auftrag_id, angebot),
        transport_arten=TRANSPORT_ARTEN,
    )


@app.route("/partner/<slug>/angebot/<int:auftrag_id>/annehmen", methods=["POST"])
def partner_angebot_annehmen(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    angebot = get_auftrag(auftrag_id)
    if not angebot or angebot.get("autohaus_id") != autohaus["id"] or not angebot.get("angebotsphase"):
        abort(404)
    if angebot.get("angebot_status") != "angebot_abgegeben":
        flash("Das Angebot der Werkstatt liegt noch nicht vor.", "warning")
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=auftrag_id))
    angebot_annehmen(auftrag_id)
    flash("Angebot angenommen. Das Fahrzeug wurde in Ihre Aufträge übernommen.", "success")
    return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>", methods=["GET", "POST"])
def partner_auftrag(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response

    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)
    if auftrag.get("angebotsphase"):
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=auftrag_id))

    if request.method == "POST":
        form = request.form
        aktion = form.get("aktion", "speichern")
        analyse = clean_text(form.get("analyse_text")) or analyse_text(form.get("beschreibung"))
        start_datum = format_date(form.get("start_datum")) if "start_datum" in form else auftrag["start_datum"]
        fertig_datum = format_date(form.get("fertig_datum")) if "fertig_datum" in form else auftrag["fertig_datum"]
        db = get_db()
        db.execute(
            """
            UPDATE auftraege
            SET kunde_name=?,
                fahrzeug=?,
                fin_nummer=?,
                auftragsnummer=?,
                bauteile_override=?,
                kennzeichen=?,
                beschreibung=?,
                analyse_text=?,
                annahme_datum=?,
                start_datum=?,
                fertig_datum=?,
                abholtermin=?,
                transport_art=?,
                kontakt_telefon=?,
                geaendert_am=?
            WHERE id=? AND autohaus_id=?
            """,
            (
                clean_text(form.get("kunde_name")),
                clean_text(form.get("fahrzeug")),
                clean_text(form.get("fin_nummer")).upper(),
                clean_text(form.get("auftragsnummer")),
                clean_text(form.get("bauteile_override")),
                clean_text(form.get("kennzeichen")).upper(),
                clean_text(form.get("beschreibung")),
                analyse,
                format_date(form.get("annahme_datum")),
                start_datum,
                fertig_datum,
                format_date(form.get("abholtermin")),
                clean_text(form.get("transport_art")) or "standard",
                clean_text(form.get("kontakt_telefon")),
                now_str(),
                auftrag_id,
                autohaus["id"],
            ),
        )
        db.commit()
        db.close()
        dateien = request.files.getlist("dateien")
        erlaubte_dateien = get_allowed_uploads(dateien)
        if aktion == "reanalyze_existing":
            count, _ = reanalyze_existing_documents(auftrag_id)
            if count:
                flash(f"{count} vorhandene Unterlage(n) neu analysiert.", "success")
            else:
                flash("Keine auswertbaren vorhandenen Unterlagen gefunden.", "warning")
            return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))
        if aktion == "upload_analyze" and not any(file and file.filename for file in dateien):
            flash("Bitte zuerst eine Datei auswählen.", "warning")
            return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))
        upload_result = save_uploads(auftrag_id, erlaubte_dateien, "autohaus", "standard")
        save_uploads(
            auftrag_id,
            get_allowed_uploads(request.files.getlist("fertigbilder")),
            "autohaus",
            "fertigbild",
        )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Auftrag neu analysiert.",
            )
        else:
            flash("Termine aktualisiert.", "success")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    sichtbare_dateien = [d for d in list_dateien(auftrag_id) if d.get("quelle") in {"autohaus", "intern"}]
    standard_dateien = dateien_mit_kategorie(sichtbare_dateien, "standard")
    fertigbilder = dateien_mit_kategorie(sichtbare_dateien, "fertigbild")
    return render_template(
        "partner_auftrag.html",
        autohaus=autohaus,
        auftrag=auftrag,
        dateien=standard_dateien,
        fertigbilder=fertigbilder,
        dokument_pruefung=list_document_review_items(auftrag_id, auftrag),
        reklamationen=list_reklamationen(auftrag_id),
        verzoegerungen=list_verzoegerungen(auftrag_id),
        transport_arten=TRANSPORT_ARTEN,
        statusliste=STATUSLISTE,
    )


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/archivieren", methods=["POST"])
def partner_archivieren(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    archive_auftrag(auftrag_id, 0 if auftrag["archiviert"] else 1)
    flash(
        "Auftrag archiviert." if not auftrag["archiviert"] else "Auftrag wieder aktiviert.",
        "info",
    )
    return redirect(url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/auftraege/sammelaktion", methods=["POST"])
def partner_sammelaktion(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    aktion = clean_text(request.form.get("aktion"))
    auftrag_ids = request.form.getlist("auftrag_ids")
    if not auftrag_ids:
        flash("Bitte zuerst Fahrzeuge auswählen.", "warning")
        return redirect(url_for("partner_dashboard", slug=slug))
    if aktion == "archivieren":
        anzahl = archive_auftraege(auftrag_ids, 1, autohaus_id=autohaus["id"])
        flash(f"{anzahl} Fahrzeug/Fahrzeuge archiviert.", "info")
    elif aktion == "aktivieren":
        anzahl = archive_auftraege(auftrag_ids, 0, autohaus_id=autohaus["id"])
        flash(f"{anzahl} Fahrzeug/Fahrzeuge wieder aktiviert.", "info")
    elif aktion == "loeschen":
        anzahl = delete_auftraege(auftrag_ids, autohaus_id=autohaus["id"])
        flash(f"{anzahl} Fahrzeug/Fahrzeuge gelöscht.", "info")
    else:
        flash("Unbekannte Sammelaktion.", "warning")
    return redirect(url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/loeschen", methods=["POST"])
def partner_loeschen(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    delete_auftrag(auftrag_id)
    flash("Fahrzeug gelöscht.", "info")
    return redirect(url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/verzoegerung", methods=["POST"])
def partner_verzoegerung(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    meldung = clean_text(request.form.get("meldung"))
    if not meldung:
        flash("Bitte eine Verzögerung beschreiben.", "warning")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    add_verzoegerung(
        auftrag_id,
        "autohaus",
        meldung,
        start_datum=request.form.get("start_datum", ""),
        fertig_datum=request.form.get("fertig_datum", ""),
        abholtermin=request.form.get("abholtermin", ""),
        uebernommen=0,
    )
    flash("Verzögerung an die Werkstatt gemeldet.", "success")
    return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/reklamation", methods=["POST"])
def partner_reklamation(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    meldung = clean_text(request.form.get("meldung"))
    dateien = get_allowed_uploads(request.files.getlist("reklamationsbilder"))
    if not meldung:
        flash("Bitte die Reklamation kurz beschreiben.", "warning")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    reklamation_id = add_reklamation(auftrag_id, "autohaus", meldung)
    save_uploads(
        auftrag_id,
        dateien,
        "autohaus",
        "reklamation",
        reklamation_id=reklamation_id,
    )
    flash("Reklamation als Alarm an die Werkstatt gemeldet.", "danger")
    return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))


@app.route("/partner/<slug>/datei/<int:datei_id>")
def partner_datei(slug, datei_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    auftrag = get_auftrag(datei["auftrag_id"])
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)
    path = UPLOAD_DIR / datei["stored_name"]
    if not path.exists():
        abort(404)
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=False,
    )


@app.route("/partner/<slug>/datei/<int:datei_id>/download")
def partner_datei_download(slug, datei_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    auftrag = get_auftrag(datei["auftrag_id"])
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)
    path = UPLOAD_DIR / datei["stored_name"]
    if not path.exists():
        abort(404)
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=True,
    )

init_db()


if __name__ == "__main__":
    print("=" * 58)
    print("  Gärtner Autohaus-Terminportal gestartet")
    print("  Admin:   http://localhost:5000/admin")
    print("  Partner: http://localhost:5000/partner")
    for warning in get_startup_warnings():
        print(f"  WARNUNG: {warning}")
    print("=" * 58)
    app.run(debug=False, host="0.0.0.0", port=5000)
