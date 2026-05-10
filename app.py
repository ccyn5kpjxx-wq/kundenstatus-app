"""
Gärtner Karosserie & Lack — Autohaus-Terminportal
=================================================
Starten: python app.py
Admin:   http://localhost:5000/admin
Partner: http://localhost:5000/partner/<slug>
"""

from collections import defaultdict
import base64
import calendar
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from functools import wraps
from html import escape
import hmac
import importlib
from io import BytesIO
import json
import mimetypes
import os
import pathlib
import re
import secrets
import shutil
import sqlite3
import tempfile
import threading
import time
from urllib.parse import quote
import uuid
import zipfile
import xml.etree.ElementTree as ET

try:
    import psycopg
except Exception:
    psycopg = None

cv2 = None
fitz = None
np = None
pytesseract = None
RapidOCR = None
requests = None
hashes = None
serialization = None
padding = None
PdfReader = None
OPTIONAL_IMPORT_ERRORS = {}

from flask import (
    Flask,
    abort,
    flash,
    g,
    has_request_context,
    jsonify,
    redirect,
    render_template,
    render_template_string,
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
DELETED_UPLOAD_DIR = DATA_DIR / "deleted_uploads"


def load_optional_module(module_name, global_name=None):
    target_name = global_name or module_name
    cached = globals().get(target_name)
    if cached is not None:
        return cached
    if target_name in OPTIONAL_IMPORT_ERRORS:
        return None
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        OPTIONAL_IMPORT_ERRORS[target_name] = str(exc)
        return None
    globals()[target_name] = module
    return module


def get_cv2():
    return load_optional_module("cv2", "cv2")


def get_fitz():
    return load_optional_module("fitz", "fitz")


def get_numpy():
    return load_optional_module("numpy", "np")


def get_pytesseract():
    return load_optional_module("pytesseract", "pytesseract")


def get_requests():
    return load_optional_module("requests", "requests")


def get_pdf_reader():
    global PdfReader
    if PdfReader is not None:
        return PdfReader
    if "PdfReader" in OPTIONAL_IMPORT_ERRORS:
        return None
    try:
        from pypdf import PdfReader as reader
    except Exception as exc:
        OPTIONAL_IMPORT_ERRORS["PdfReader"] = str(exc)
        return None
    PdfReader = reader
    return PdfReader


def get_rapidocr_class():
    global RapidOCR
    if RapidOCR is not None:
        return RapidOCR
    if "RapidOCR" in OPTIONAL_IMPORT_ERRORS:
        return None
    try:
        from rapidocr_onnxruntime import RapidOCR as rapid_ocr_class
    except Exception as exc:
        OPTIONAL_IMPORT_ERRORS["RapidOCR"] = str(exc)
        return None
    RapidOCR = rapid_ocr_class
    return RapidOCR


def load_crypto_modules():
    global hashes, serialization, padding
    if hashes is not None and serialization is not None and padding is not None:
        return True
    if "cryptography" in OPTIONAL_IMPORT_ERRORS:
        return False
    try:
        from cryptography.hazmat.primitives import hashes as crypto_hashes
        from cryptography.hazmat.primitives import serialization as crypto_serialization
        from cryptography.hazmat.primitives.asymmetric import padding as crypto_padding
    except Exception as exc:
        OPTIONAL_IMPORT_ERRORS["cryptography"] = str(exc)
        return False
    hashes = crypto_hashes
    serialization = crypto_serialization
    padding = crypto_padding
    return True


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
if os.environ.get("RENDER"):
    DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "/var/data"))
    DB = pathlib.Path(os.environ.get("SQLITE_DB_PATH", str(DATA_DIR / "auftraege.db")))
if USE_POSTGRES:
    UPLOAD_DIR = pathlib.Path(os.environ.get("UPLOAD_DIR", str(DATA_DIR / "uploads")))
elif os.environ.get("RENDER"):
    UPLOAD_DIR = pathlib.Path(os.environ.get("UPLOAD_DIR", str(DATA_DIR / "uploads")))


def env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


RUNNING_ON_RENDER = bool(os.environ.get("RENDER") or USE_POSTGRES)
REQUIRE_POSTGRES_ON_RENDER = env_flag("REQUIRE_POSTGRES_ON_RENDER", False)
if RUNNING_ON_RENDER and REQUIRE_POSTGRES_ON_RENDER and not USE_POSTGRES:
    raise RuntimeError(
        "Render ist auf Postgres festgelegt, aber DATABASE_URL fehlt. "
        "Bitte in Render eine PostgreSQL-Datenbank verbinden und DATABASE_URL setzen."
    )
SQLITE_BUSY_TIMEOUT_SECONDS = max(
    5,
    env_int("SQLITE_BUSY_TIMEOUT_SECONDS", 60 if RUNNING_ON_RENDER else 15),
)
DEFAULT_ADMIN_PASS = "gaertner2026"
DEFAULT_FLASK_SECRET_KEY = "gaertner-autohaus-2026"
ADMIN_PASS = os.environ.get("ADMIN_PASS") or DEFAULT_ADMIN_PASS
DEFAULT_PUBLIC_BASE_URL = ""
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL") or DEFAULT_PUBLIC_BASE_URL).strip().rstrip("/")
LEXWARE_KUNDEN_URL = (os.environ.get("LEXWARE_KUNDEN_URL") or "").strip()
LEXWARE_RECHNUNGEN_URL = (os.environ.get("LEXWARE_RECHNUNGEN_URL") or "").strip()
LEXWARE_API_KEY = (os.environ.get("LEXWARE_API_KEY") or "").strip()
LEXWARE_API_BASE_URL = (os.environ.get("LEXWARE_API_BASE_URL") or "https://api.lexware.io").strip().rstrip("/")
LEXWARE_APP_BASE_URL = (os.environ.get("LEXWARE_APP_BASE_URL") or "https://app.lexware.de").strip().rstrip("/")
LEXWARE_TAX_RATE = float(os.environ.get("LEXWARE_TAX_RATE") or 19)
DATE_FMT = "%d.%m.%Y"
DATETIME_FMT = "%d.%m.%Y %H:%M"
MAX_UPLOAD_MB = 25
BACKUP_DIR = pathlib.Path(
    os.environ.get(
        "BACKUP_DIR",
        str(DATA_DIR / "backups"),
    )
)
DELETED_UPLOAD_DIR = pathlib.Path(
    os.environ.get(
        "DELETED_UPLOAD_DIR",
        str(DATA_DIR / "deleted_uploads"),
    )
)
AUTO_BACKUP_ENABLED = env_flag("AUTO_BACKUP_ENABLED", True)
AUTO_BACKUP_INTERVAL_SECONDS = max(60, env_int("AUTO_BACKUP_INTERVAL_SECONDS", 3600))
AUTO_BACKUP_KEEP = max(1, env_int("AUTO_BACKUP_KEEP", 168))
AUTO_BACKUP_ON_STARTUP = env_flag("AUTO_BACKUP_ON_STARTUP", False)
AUTO_CHANGE_BACKUP_ENABLED = env_flag("AUTO_CHANGE_BACKUP_ENABLED", True)
AUTO_CHANGE_BACKUP_DELAY_SECONDS = max(1, env_int("AUTO_CHANGE_BACKUP_DELAY_SECONDS", 3))
OPENAI_EXTRACTION_MODEL = os.environ.get("OPENAI_EXTRACTION_MODEL", "gpt-4o")
OPENAI_API_URL = os.environ.get(
    "OPENAI_API_URL", "https://api.openai.com/v1/chat/completions"
)
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DOC_AI_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
GOOGLE_DOC_AI_TIMEOUT = env_int(
    "DOCUMENT_ANALYSIS_TIMEOUT_SECONDS",
    20 if RUNNING_ON_RENDER else 45,
)
OPENAI_API_TIMEOUT = env_int(
    "OPENAI_API_TIMEOUT_SECONDS",
    max(GOOGLE_DOC_AI_TIMEOUT, 90),
)
ENABLE_LOCAL_OCR = env_flag("ENABLE_LOCAL_OCR", not RUNNING_ON_RENDER)
OPENAI_VISION_MAX_PAGES = 4
OPENAI_VISION_MAX_IMAGE_SIDE = 1800
OPENAI_TRANSIENT_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
OPENAI_TEST_IMAGE_DATA_URL = (
    "data:image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAAfElEQVR4nNXOQREAIADDsFL/"
    "nocIHlyjIGcbZRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncR"
    "IncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncRIncf4OvLpyqgN9"
    "ZSiDcwAAAABJRU5ErkJggg=="
)
CSRF_FIELD_NAME = "csrf_token"
_sqlite_wal_configured = False
_sqlite_wal_lock = threading.Lock()

GOOGLE_ACCESS_TOKEN = {"token": "", "expires_at": 0}


def get_public_base_url():
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    if has_request_context():
        forwarded_proto = clean_text(request.headers.get("X-Forwarded-Proto")).split(",", 1)[0]
        forwarded_host = clean_text(request.headers.get("X-Forwarded-Host")).split(",", 1)[0]
        scheme = forwarded_proto or request.scheme
        host = forwarded_host or request.host
        if host:
            return f"{scheme}://{host}".rstrip("/")
        return request.url_root.rstrip("/")
    return ""

DEFAULT_AUTOHAEUSER = [
    {
        "name": "Auto Pfaff GmbH",
        "slug": "autohaus-pfaff",
        "portal_key": "b900b7d3d54f4afa",
        "kontakt_name": "Auto Pfaff GmbH",
        "email": "info@auto-pfaff.de",
        "telefon": "06261 9310-0",
        "strasse": "Neuwiesenweg 19",
        "plz": "74834",
        "ort": "Elztal-Dallau",
        "zugangscode": "PFAFF2026",
        "portal_titel": "Portal Auto Pfaff",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von Auto Pfaff.",
        "notiz": "Aus Kundenordner angelegt und mit öffentlichem Impressum ergänzt am 26.04.2026.",
    },
    {
        "name": "Autohaus Ralph Müller OHG",
        "slug": "autohaus-mueller",
        "portal_key": "9847b961ecdf4387",
        "kontakt_name": "Autohaus Ralph Müller",
        "email": "",
        "telefon": "",
        "strasse": "Ortsstraße 7",
        "plz": "74847",
        "ort": "Obrigheim-Asbach",
        "zugangscode": "MUELLER2026",
        "portal_titel": "Portal Autohaus Müller",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von Autohaus Müller.",
        "notiz": "Aus Rechnungen im Kundenordner übernommen am 26.04.2026.",
    },
    {
        "name": "HSE Autowelt GmbH",
        "slug": "hse-autowelt",
        "portal_key": "ecd6b48321124e96",
        "kontakt_name": "HSE Autowelt",
        "email": "info@hse-autowelt.de",
        "telefon": "+49 1515 0928930",
        "strasse": "Langenelzer Str. 45",
        "plz": "69427",
        "ort": "Mudau",
        "zugangscode": "HSE2026",
        "portal_titel": "Portal HSE Autowelt",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von HSE Autowelt.",
        "notiz": "Aus Kundenordner/Partnermappe ergänzt am 26.04.2026. Ansprechpartner bitte bei Gelegenheit final prüfen.",
    },
    {
        "name": "Johnatan Dold und Marcel Deisling GbR",
        "slug": "johnatan-dold-und-marcel-deisling-gbr",
        "portal_key": "f255abee6f8d4353",
        "kontakt_name": "Johnatan Dold / Marcel Deisling",
        "email": "",
        "telefon": "",
        "strasse": "Hohlweg 24",
        "plz": "74821",
        "ort": "Mosbach",
        "zugangscode": "DOLD2026",
        "portal_titel": "Portal Dold & Deisling",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von Dold & Deisling.",
        "notiz": "Adresse aus vorhandenen Rechnungen im Kundenordner übernommen am 26.04.2026.",
    },
    {
        "name": "Käsmann",
        "slug": "kaesmann",
        "portal_key": "4080695acfd54eea",
        "kontakt_name": "Käsmann",
        "email": "info@kaesmann.de",
        "telefon": "06261 9730-0",
        "strasse": "Mosbacher Straße 67",
        "plz": "74821",
        "ort": "Mosbach",
        "zugangscode": "KAES2026",
        "portal_titel": "Portal Käsmann",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von Käsmann.",
        "notiz": "Stammdaten ergänzt am 26.04.2026. Quelle: Käsmann-Impressum; Audi-Standort zusätzlich Industriestraße 1a, 74821 Mosbach.",
    },
    {
        "name": "MHC Mobility GmbH",
        "slug": "mhc-mobility",
        "portal_key": "620bc857773b4b60",
        "kontakt_name": "MHC Mobility",
        "email": "info@mhcmobility.de",
        "telefon": "04286 7703-0",
        "strasse": "An der Autobahn 12-16",
        "plz": "27404",
        "ort": "Gyhum/Bockel",
        "zugangscode": "MHC2026",
        "portal_titel": "Portal MHC Mobility",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von MHC Mobility.",
        "notiz": "Aus Kundenordner und MHC-Impressum ergänzt am 26.04.2026.",
    },
    {
        "name": "Kraftfahrzeugtechnik Lock",
        "slug": "kraftfahrzeugtechnik-lock",
        "portal_key": "72c1b85572a2468b",
        "kontakt_name": "",
        "email": "",
        "telefon": "",
        "strasse": "",
        "plz": "",
        "ort": "",
        "zugangscode": "LOCK2026",
        "portal_titel": "Portal Kraftfahrzeugtechnik Lock",
        "willkommen_text": "",
        "notiz": "",
    },
    {
        "name": "Autohaus Günther GmbH",
        "slug": "autohaus-guenther-gmbh",
        "portal_key": "ac718b1335474918",
        "kontakt_name": "Johannes Baumann",
        "email": "johannes.baumann@guenther-dasautohaus.de",
        "telefon": "06282/9230-25",
        "strasse": "",
        "plz": "",
        "ort": "",
        "zugangscode": "GUENTHER2026",
        "portal_titel": "Portal Autohaus Günther",
        "willkommen_text": "Willkommen im persönlichen Terminbereich von Autohaus Günther.",
        "notiz": "Gebrauchtwagenkoordinator. Fax: 06282/6345. Webseite: www.wolfert-gruppe.de",
    },
]

DEFAULT_AUFTRAEGE = [
    {
        "autohaus_slug": "autohaus-pfaff",
        "kunde_name": "Sabine Jörg / Denis Schwalbe",
        "fahrzeug": "Hyundai Kona SX2",
        "fin_nummer": "KMHHB813XTU379862",
        "auftragsnummer": "Schwalbe / 24.04.2026",
        "kennzeichen": "MOS-J 1551",
        "beschreibung": (
            "Quelle: Email Denis Schwalbe vom 22.04.2026.\n"
            "Endkunde: Sabine Jörg. Farbcode SAW.\n"
            "1x Stoßstange hinten / Neuteil (SAW), 1x Radlaufleiste hinten rechts / Neuteil (SAW), "
            "4x Zierleiste: 1x Neuteil und 3x Farbangleich wenn notwendig.\n"
            "Fertigstellung gewünscht bis 24.04.2026, 12:00 Uhr."
        ),
        "analyse_text": "Stoßstange hinten, Radlaufleiste hinten rechts und Zierleisten lackieren",
        "bauteile_override": "Stoßstange hinten\nRadlaufleiste hinten rechts\nZierleisten",
        "status": 4,
        "annahme_datum": "21.04.2026",
        "start_datum": "21.04.2026",
        "fertig_datum": "24.04.2026",
        "abholtermin": "24.04.2026",
        "notiz_intern": "Aus Pfaff-Mail/Kalender übernommen. Deadline 24.04.2026 bis 12 Uhr.",
    },
    {
        "autohaus_slug": "autohaus-pfaff",
        "kunde_name": "Frau Nies / Frank Frühwirth",
        "fahrzeug": "Hyundai i10",
        "fin_nummer": "",
        "auftragsnummer": "61876",
        "kennzeichen": "MOS-K 842",
        "beschreibung": (
            "Quelle: Email Frank Frühwirth vom 23.04.2026.\n"
            "Unfallschaden Frau Nies, Gutachten-Nr. 61876.\n"
            "Bitte einplanen vom 11.05.2026 bis 13.05.2026. Lackumfang aus Gutachten im Anhang."
        ),
        "analyse_text": "GP-Schaden, Lackumfang laut Gutachten",
        "bauteile_override": "GP-Schaden laut Gutachten\nLackumfang aus Gutachten",
        "status": 1,
        "annahme_datum": "11.05.2026",
        "start_datum": "11.05.2026",
        "fertig_datum": "13.05.2026",
        "abholtermin": "13.05.2026",
        "notiz_intern": "Aus Pfaff-Mail/Kalender übernommen. Termin 11.05.-13.05.2026.",
    },
    {
        "autohaus_slug": "autohaus-pfaff",
        "kunde_name": "Auto Pfaff / Frank Frühwirth",
        "fahrzeug": "Hyundai Kona SX2 EV",
        "fin_nummer": "TMAH881BXSJ060170",
        "auftragsnummer": "KV 2026-0415-01",
        "kennzeichen": "MOS-HU 574",
        "beschreibung": (
            "Quelle: Kalender Disposition_Termine_KW16.ics und KV_Hyundai_Kona_MOS-HU574.pdf.\n"
            "Delle in der Motorhaube mit Lackbeschädigung, Komplettinstandsetzung mittels Smart Repair.\n"
            "Spachtelarbeiten, Spot-Lackierung Motorhaube Farbton Meta Blue PM2 sowie De-/Montage.\n"
            "Termin bestätigt: 18.05.2026 bis 21.05.2026."
        ),
        "analyse_text": "Delle Motorhaube, Smart Repair komplett",
        "bauteile_override": "Motorhaube\nSmart Repair\nSpot-Lackierung",
        "status": 1,
        "annahme_datum": "18.05.2026",
        "start_datum": "18.05.2026",
        "fertig_datum": "21.05.2026",
        "abholtermin": "21.05.2026",
        "notiz_intern": "Aus Kalender übernommen. KV brutto 416,50 EUR, netto 350,00 EUR.",
    },
]

STATUSLISTE = {
    1: dict(key="angelegt", label="Angelegt", icon="📝", farbe="secondary"),
    2: dict(key="eingeplant", label="Eingeplant", icon="📅", farbe="primary"),
    3: dict(key="in_arbeit", label="In Arbeit", icon="🔧", farbe="info"),
    4: dict(key="fertig", label="Fertig", icon="✅", farbe="success"),
    5: dict(key="zurueckgegeben", label="Zurückgegeben", icon="↩️", farbe="dark"),
}

BONUSMODELL_STUFEN = (
    {"schwelle": 3000.0, "satz": 0.02, "label": "2 %", "schwelle_label": "3.000 EUR"},
    {"schwelle": 5000.0, "satz": 0.03, "label": "3 %", "schwelle_label": "5.000 EUR"},
    {"schwelle": 8000.0, "satz": 0.04, "label": "4 %", "schwelle_label": "8.000 EUR"},
)

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
    ".docx",
    ".xlsx",
    ".heic",
}

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic"}
TEXT_ANALYSIS_EXTENSIONS = {".txt", ".docx", ".xlsx"}
ANALYSIS_EXTENSIONS = {".pdf"} | IMAGE_EXTENSIONS | TEXT_ANALYSIS_EXTENSIONS

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

PREVIOUS_DAMAGE_SECTION_START_PATTERNS = (
    r"^v\s*orsch(?:a|ae)den?$",
    r"^unreparierte?\s+v\s*orsch(?:a|ae)den?",
    r"^reparierte?\s+v\s*orsch(?:a|ae)den?",
    r"^bild\s+\d+\s*:?\s*v\s*orsch(?:a|ae)d",
)

PREVIOUS_DAMAGE_SECTION_END_PATTERNS = (
    r"^dekra[-\s]*nr",
    r"^seite\s+\d+",
    r"^instandsetzung$",
    r"^hinweise\s+zum\s+reparaturweg",
    r"^reparaturauftrag$",
    r"^reparaturkosten$",
    r"^schadenkalkulation",
)

NON_WORK_POSITION_LINE_PATTERNS = (
    r"^arb[.\s]*pos[.\s]*nr",
    r"^instandsetzung$",
    r"^schadenkalkulation\s+nr",
    r"^reifen\b.*\bfelge\b.*\bnotrad\b",
    r"^gesamtsumme\s+lackierung",
    r"^summe\s+lackierung",
)

RAPID_OCR_ENGINE = None
TESSERACT_CMD = shutil.which("tesseract")


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or DEFAULT_FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


@app.teardown_appcontext
def close_request_db(exception=None):
    db = g.pop("db_connection", None)
    if db is not None:
        db.force_close()


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


@app.after_request
def backup_after_successful_change(response):
    if not app.config.get("TESTING") and response.status_code < 400 and should_backup_after_request():
        schedule_change_backup(request.endpoint)
    return response


@app.errorhandler(500)
def internal_server_error(error):
    original = getattr(error, "original_exception", None) or error
    message = clean_text(str(original)) or original.__class__.__name__
    print(f"INTERNAL SERVER ERROR: {original.__class__.__name__}: {message}")
    return render_template_string(
        """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fehler</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <main class="container py-5">
    <div class="alert alert-danger">
      <h1 class="h4">Diese Seite konnte gerade nicht geladen werden.</h1>
      <p class="mb-2">Pfad: <code>{{ path }}</code></p>
      <p class="mb-0">Technischer Fehler: <code>{{ message }}</code></p>
    </div>
    <a class="btn btn-outline-dark" href="{{ fallback_url }}">Zurück</a>
  </main>
</body>
</html>
        """,
        path=request.path,
        message=message[:500],
        fallback_url=request.referrer or url_for("partner_login"),
    ), 500


def get_startup_warnings():
    warnings = []
    if get_admin_pass() in {"", "change-me", DEFAULT_ADMIN_PASS}:
        warnings.append(
            "ADMIN_PASS ist nicht sicher gesetzt. Bitte in .env.local ein eigenes Passwort eintragen."
        )
    if app.secret_key in {"", "change-me", DEFAULT_FLASK_SECRET_KEY}:
        warnings.append(
            "FLASK_SECRET_KEY ist nicht sicher gesetzt. Bitte in .env.local einen langen Zufallswert eintragen."
        )
    if RUNNING_ON_RENDER and not USE_POSTGRES:
        warnings.append(
            "Live läuft noch mit SQLite. Für stabilen Betrieb bitte Render PostgreSQL verbinden und DATABASE_URL setzen."
        )
    return warnings


def get_database_status():
    engine = "postgres" if USE_POSTGRES else "sqlite"
    if USE_POSTGRES:
        label = "Postgres aktiv"
        message = "Render nutzt die stabile PostgreSQL-Datenbank."
        detail = "DATABASE_URL ist gesetzt."
    else:
        label = "SQLite aktiv"
        if RUNNING_ON_RENDER:
            message = "Live nutzt noch SQLite. Das kann bei gleichzeitigen Zugriffen zu Sperren führen."
            detail = f"Datenbankdatei: {DB}"
        else:
            message = "Lokale Entwicklung nutzt SQLite."
            detail = f"Datenbankdatei: {DB}"
    return {
        "engine": engine,
        "label": label,
        "message": message,
        "detail": detail,
        "stable": USE_POSTGRES or not RUNNING_ON_RENDER,
        "running_on_render": RUNNING_ON_RENDER,
        "database_url_present": bool(DATABASE_URL),
        "require_postgres": REQUIRE_POSTGRES_ON_RENDER,
    }


def clean_text(value):
    return str(value or "").strip()


def clean_secret_value(value):
    text = clean_text(value)
    for _ in range(3):
        previous = text
        if "=" in text:
            key, possible_value = text.split("=", 1)
            if key.strip().upper() == "ADMIN_PASS":
                text = possible_value.strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            text = text[1:-1].strip()
        if text == previous:
            break
    return text


def get_admin_pass():
    return clean_secret_value(os.environ.get("ADMIN_PASS")) or clean_secret_value(ADMIN_PASS) or DEFAULT_ADMIN_PASS


def get_app_setting(key, default=""):
    key = clean_text(key)
    if not key:
        return default
    db = None
    try:
        db = get_db()
        row = db.execute(
            "SELECT value FROM app_settings WHERE key=?",
            (key,),
        ).fetchone()
        if row:
            return clean_text(row["value"])
    except Exception:
        return default
    finally:
        if db is not None:
            db.close()
    return default


def set_app_setting(key, value):
    key = clean_text(key)
    if not key:
        return
    db = get_db()
    now = now_str()
    try:
        existing = db.execute(
            "SELECT key FROM app_settings WHERE key=?",
            (key,),
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE app_settings SET value=?, updated_at=? WHERE key=?",
                (clean_text(value), now, key),
            )
        else:
            db.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
                (key, clean_text(value), now),
            )
        db.commit()
    finally:
        db.close()


def get_openai_api_key():
    return clean_secret_value(
        get_app_setting("OPENAI_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
    )


def admin_password_matches(value):
    submitted = clean_secret_value(value)
    candidates = {
        password
        for password in (get_admin_pass(), DEFAULT_ADMIN_PASS)
        if clean_text(password)
    }
    return bool(
        submitted
        and any(hmac.compare_digest(submitted, password) for password in candidates)
    )


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
    openai_api_key = get_openai_api_key()
    service_account_path = resolve_config_path(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        or os.environ.get("GOOGLE_DOC_AI_SERVICE_ACCOUNT_FILE")
    )
    google_ready = bool(
        service_account_path
        and service_account_path.exists()
        and clean_text(os.environ.get("GOOGLE_DOC_AI_PROJECT_ID"))
        and clean_text(os.environ.get("GOOGLE_DOC_AI_LOCATION"))
        and clean_text(os.environ.get("GOOGLE_DOC_AI_PROCESSOR_ID"))
    )
    openai_ready = bool(openai_api_key)
    ready = google_ready or openai_ready
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
    elif ENABLE_LOCAL_OCR:
        message = "API-Zugangsdaten fehlen noch. Bis dahin liest die lokale OCR Dateien aus."
    else:
        message = "OpenAI fehlt auf Render. Bitte OPENAI_API_KEY in Render eintragen, damit Kunden-Uploads online ausgelesen werden."
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
    if not load_crypto_modules():
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
    requests_module = get_requests()
    if requests_module is None:
        raise RuntimeError("requests ist nicht verfuegbar")
    response = requests_module.post(
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
    requests_module = get_requests()
    if requests_module is None:
        raise RuntimeError("requests ist nicht verfuegbar")
    response = requests_module.post(
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


def friendly_analysis_error(message, provider="OpenAI"):
    normalized = normalize_document_text(message)
    provider = clean_text(provider) or "KI"
    if "timeout" in normalized or "timed out" in normalized or "read timed out" in normalized:
        return f"{provider} hat zu lange gebraucht. Die lokale Auslesung bleibt aktiv."
    if "connection" in normalized or "network" in normalized or "dns" in normalized:
        return f"{provider} konnte vom Server nicht erreicht werden. Die lokale Auslesung bleibt aktiv."
    if "401" in normalized or "unauthorized" in normalized:
        return f"{provider}-Zugang ist nicht gültig oder abgelaufen. Die lokale Auslesung bleibt aktiv."
    if "403" in normalized or "forbidden" in normalized:
        return f"{provider}-Zugriff ist nicht freigegeben. Die lokale Auslesung bleibt aktiv."
    if "429" in normalized or "rate limit" in normalized or "quota" in normalized:
        return f"{provider} ist gerade limitiert. Die lokale Auslesung bleibt aktiv."
    if normalized:
        return f"{provider}-Auslesung war gerade nicht erreichbar. Die lokale Auslesung bleibt aktiv."
    return ""


def extract_openai_error_message(response):
    if response is None:
        return ""
    detail = ""
    try:
        payload = response.json()
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            detail = clean_text(error.get("message") or error.get("code") or error.get("type"))
        elif error:
            detail = clean_text(error)
    except Exception:
        detail = clean_text(getattr(response, "text", ""))
    status = f"{response.status_code} {getattr(response, 'reason', '')}".strip()
    return f"{status}: {detail[:500]}" if detail else status


def post_openai_chat_completion(requests_module, payload):
    api_key = get_openai_api_key()
    if not api_key:
        return None, "OpenAI ist noch nicht konfiguriert"
    last_error = ""
    for attempt in range(3):
        try:
            response = requests_module.post(
                OPENAI_API_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=OPENAI_API_TIMEOUT,
            )
        except Exception as exc:
            last_error = str(exc)
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None, last_error
        if response.status_code in OPENAI_TRANSIENT_STATUS_CODES and attempt < 2:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_seconds = min(max(float(retry_after or 0), 1), 8)
            except ValueError:
                wait_seconds = 1.5 * (attempt + 1)
            time.sleep(wait_seconds)
            continue
        return response, ""
    return None, last_error


def test_openai_document_analysis_connection():
    config = get_ai_config()
    if not config["openai_ready"]:
        return False, "OpenAI ist noch nicht konfiguriert."
    requests_module = get_requests()
    if requests_module is None:
        return False, "requests ist nicht verfuegbar."

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["status"],
        "properties": {
            "status": {
                "type": "string",
                "description": "ok wenn der Test erfolgreich war",
            }
        },
    }
    payload = {
        "model": config["openai_model"],
        "messages": [
            {
                "role": "system",
                "content": "Du pruefst nur die Verbindung fuer eine Dokumentanalyse.",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Antworte als JSON mit status=ok, wenn du dieses Testbild lesen kannst.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": OPENAI_TEST_IMAGE_DATA_URL, "detail": "low"},
                    },
                ],
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "openai_connection_test",
                "strict": True,
                "schema": schema,
            },
        },
        "max_tokens": 50,
    }

    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error:
        readable = friendly_analysis_error(request_error, "OpenAI")
        return False, readable or "OpenAI-Test fehlgeschlagen."
    if response.status_code >= 400:
        raw_error = extract_openai_error_message(response)
        readable = friendly_analysis_error(raw_error, "OpenAI")
        detail = clean_text(raw_error)[:220]
        if detail and detail not in readable:
            return False, f"{readable} Detail: {detail}"
        return False, readable or "OpenAI-Test fehlgeschlagen."
    parsed = extract_openai_response_json(response.json())
    if normalize_document_text((parsed or {}).get("status")) != "ok":
        return False, "OpenAI-Testantwort war erreichbar, aber nicht im erwarteten Format."
    return True, "OpenAI-Test erfolgreich. Der Dokumentanalyse-Aufruf funktioniert live."


def should_retry_openai_without_schema(error_message):
    normalized = normalize_document_text(error_message)
    return (
        "response_format" in normalized
        or "json_schema" in normalized
        or "schema" in normalized
        or "structured output" in normalized
    )


def build_openai_json_mode_fallback_payload(payload):
    fallback = dict(payload)
    fallback["response_format"] = {"type": "json_object"}
    return fallback


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
    if not ENABLE_LOCAL_OCR:
        return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"
    cv2_module = get_cv2()
    np_module = get_numpy()
    if cv2_module is None or np_module is None:
        return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"
    try:
        image = cv2_module.imdecode(np_module.frombuffer(raw, dtype=np_module.uint8), cv2_module.IMREAD_COLOR)
        if image is None:
            return raw, mimetypes.guess_type(str(path))[0] or "image/jpeg"
        height, width = image.shape[:2]
        longest = max(height, width)
        if longest > OPENAI_VISION_MAX_IMAGE_SIDE:
            scale = OPENAI_VISION_MAX_IMAGE_SIDE / longest
            image = cv2_module.resize(
                image,
                (max(1, int(width * scale)), max(1, int(height * scale))),
                interpolation=cv2_module.INTER_AREA,
            )
        ok, encoded = cv2_module.imencode(".jpg", image, [int(cv2_module.IMWRITE_JPEG_QUALITY), 88])
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

    fitz_module = get_fitz() if suffix == ".pdf" else None
    if suffix == ".pdf" and fitz_module is not None:
        try:
            doc = fitz_module.open(str(path))
            max_pages = min(doc.page_count, OPENAI_VISION_MAX_PAGES)
            for page_index in range(max_pages):
                page = doc.load_page(page_index)
                pix = page.get_pixmap(matrix=fitz_module.Matrix(2, 2), alpha=False)
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
            "- Vorschaden, Vorschäden, Altschaden oder unreparierte Vorschäden sind keine aktuelle Reparatur: niemals in offene_bauteile, kurzanalyse oder lesefassung aufnehmen.",
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
    requests_module = get_requests()
    if requests_module is None:
        return {"data": {}, "error": "requests ist nicht verfuegbar"}
    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error:
        return {
            "data": {},
            "error": friendly_analysis_error(request_error, "OpenAI"),
        }
    if response.status_code >= 400:
        error_message = extract_openai_error_message(response)
        if response.status_code == 400 and should_retry_openai_without_schema(error_message):
            fallback_payload = build_openai_json_mode_fallback_payload(payload)
            response, request_error = post_openai_chat_completion(
                requests_module,
                fallback_payload,
            )
            if request_error:
                return {
                    "data": {},
                    "error": friendly_analysis_error(request_error, "OpenAI"),
                }
            if response.status_code < 400:
                parsed = extract_openai_response_json(response.json())
                if parsed:
                    return {"data": parsed, "error": ""}
                return {
                    "data": {},
                    "error": "OpenAI-Antwort enthielt kein lesbares JSON.",
                }
            error_message = extract_openai_error_message(response)
        return {
            "data": {},
            "error": friendly_analysis_error(
                error_message,
                "OpenAI",
            ),
        }
    response.raise_for_status()
    parsed = extract_openai_response_json(response.json())
    if not parsed:
        return {
            "data": {},
            "error": "OpenAI-Antwort enthielt kein lesbares JSON.",
        }
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
    elif ai_result.get("error") and not clean_text(bundle.get("text")):
        bundle["hint"] = friendly_analysis_error(ai_result["error"], "OpenAI")
    elif google_result.get("error") and not local_text:
        bundle["hint"] = friendly_analysis_error(google_result["error"], "Google OCR")

    config = get_ai_config()
    if not bundle["hint"] and not (config["openai_ready"] or config["google_ready"]):
        if ENABLE_LOCAL_OCR:
            bundle["hint"] = "API-Konfiguration fehlt, lokale OCR bleibt aktiv"
        else:
            bundle["hint"] = "OpenAI ist auf Render noch nicht eingerichtet. Bitte OPENAI_API_KEY in Render setzen, damit Kunden-Uploads online analysiert werden."
    if not clean_text(bundle.get("text")) and not bundle.get("structured"):
        bundle["status"] = "error"
        if not bundle["hint"]:
            bundle["hint"] = "Aus der Datei konnten keine Daten gelesen werden."
    return bundle


def build_document_analysis_bundle_safe(path, filename=""):
    try:
        return build_document_analysis_bundle(path, filename)
    except Exception as exc:
        return {
            "text": "",
            "source": "analysis_error",
            "status": "error",
            "hint": f"Analyse konnte nicht abgeschlossen werden: {clean_text(str(exc))[:300]}",
            "structured": {},
            "analysis_json": "",
        }


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


def extract_previous_damage_parts(text):
    parts = []
    in_previous_damage = False
    for line in [clean_text(line) for line in clean_text(text).splitlines() if clean_text(line)]:
        normalized_line = normalize_document_text(line)
        if starts_previous_damage_section(normalized_line):
            in_previous_damage = True
            continue
        if not in_previous_damage:
            continue
        if ends_previous_damage_section(normalized_line):
            in_previous_damage = False
            continue
        part = detect_line_item_part(normalized_line)
        if part:
            parts.append(part)
    return remove_less_specific_part_labels(list(dict.fromkeys(parts)))


def part_mentioned_in_text(text, part):
    normalized_text = normalize_document_text(text)
    normalized_part = normalize_document_text(part)
    if not normalized_text or not normalized_part:
        return False
    return normalized_part in normalized_text or matches_any_pattern(
        get_part_patterns(part),
        normalized_text,
        normalized_text,
    )


def remove_previous_damage_fragments(value, previous_parts, keep_text=""):
    fragments = [
        compact_whitespace(fragment)
        for fragment in re.split(r"[\n;,]+", clean_text(value))
        if clean_text(fragment)
    ]
    if not fragments:
        return clean_text(value)

    kept = []
    for fragment in fragments:
        is_previous_only = any(
            part_mentioned_in_text(fragment, part)
            and not part_mentioned_in_text(keep_text, part)
            for part in previous_parts
        )
        if not is_previous_only:
            kept.append(fragment)
    return ", ".join(dict.fromkeys(kept))[:220]


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
    if not ENABLE_LOCAL_OCR:
        return None
    rapid_ocr_class = get_rapidocr_class()
    if RAPID_OCR_ENGINE is None and rapid_ocr_class is not None:
        RAPID_OCR_ENGINE = rapid_ocr_class()
    return RAPID_OCR_ENGINE


def load_image_for_ocr(path):
    cv2_module = get_cv2()
    np_module = get_numpy()
    if cv2_module is None or np_module is None:
        return None
    try:
        buffer = np_module.fromfile(path, dtype=np_module.uint8)
        if buffer.size == 0:
            return None
        image = cv2_module.imdecode(buffer, cv2_module.IMREAD_COLOR)
        return preprocess_cv_image(image)
    except Exception:
        return None


def preprocess_cv_image(image):
    cv2_module = get_cv2()
    if cv2_module is None or image is None:
        return image
    try:
        gray = cv2_module.cvtColor(image, cv2_module.COLOR_BGR2GRAY)
        scaled = cv2_module.resize(gray, None, fx=2, fy=2, interpolation=cv2_module.INTER_CUBIC)
        _, threshold = cv2_module.threshold(
            scaled, 0, 255, cv2_module.THRESH_BINARY + cv2_module.THRESH_OTSU
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
    if not ENABLE_LOCAL_OCR:
        return ""
    best_lines = extract_ocr_lines(str(path))

    pytesseract_module = get_pytesseract()
    if not best_lines and pytesseract_module is not None and TESSERACT_CMD:
        try:
            pytesseract_module.pytesseract.tesseract_cmd = TESSERACT_CMD
            image = load_image_for_ocr(str(path))
            if image is not None:
                text = pytesseract_module.image_to_string(image, lang="deu+eng")
                best_lines = [
                    clean_text(line) for line in text.splitlines() if clean_text(line)
                ]
        except Exception:
            best_lines = []

    return "\n".join(best_lines)


def extract_pdf_text(path):
    text_chunks = []
    pdf_reader_class = get_pdf_reader()
    try:
        reader = pdf_reader_class(str(path)) if pdf_reader_class is not None else None
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
    fitz_module = get_fitz() if needs_ocr and ENABLE_LOCAL_OCR else None
    cv2_module = get_cv2() if needs_ocr and ENABLE_LOCAL_OCR else None
    np_module = get_numpy() if needs_ocr and ENABLE_LOCAL_OCR else None
    if needs_ocr and fitz_module is not None and cv2_module is not None and np_module is not None:
        try:
            doc = fitz_module.open(str(path))
            max_pages = min(doc.page_count, 4)
            for page_index in range(max_pages):
                try:
                    page = doc.load_page(page_index)
                    pix = page.get_pixmap(matrix=fitz_module.Matrix(2, 2), alpha=False)
                    image = np_module.frombuffer(pix.samples, dtype=np_module.uint8).reshape(
                        pix.height, pix.width, pix.n
                    )
                    if pix.n == 4:
                        image = cv2_module.cvtColor(image, cv2_module.COLOR_RGBA2BGR)
                    elif pix.n == 3:
                        image = cv2_module.cvtColor(image, cv2_module.COLOR_RGB2BGR)
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


def extract_plain_text_file(path):
    raw = pathlib.Path(path).read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def xml_text_nodes(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    return [
        clean_text(element.text)
        for element in root.iter()
        if clean_text(element.text)
    ]


def extract_docx_text(path):
    chunks = []
    try:
        with zipfile.ZipFile(path) as archive:
            names = [
                name
                for name in archive.namelist()
                if name.startswith("word/") and name.endswith(".xml")
            ]
            for name in names:
                if name.startswith(("word/header", "word/footer")):
                    continue
                chunks.extend(xml_text_nodes(archive.read(name)))
    except Exception:
        return ""
    return "\n".join(dict.fromkeys(chunks))


def extract_xlsx_text(path):
    chunks = []
    try:
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                if name == "xl/sharedStrings.xml" or (
                    name.startswith("xl/worksheets/") and name.endswith(".xml")
                ):
                    chunks.extend(xml_text_nodes(archive.read(name)))
    except Exception:
        return ""
    return "\n".join(dict.fromkeys(chunks))


def extract_document_text_local(path, filename=""):
    suffix = pathlib.Path(filename or str(path)).suffix.lower()
    if suffix == ".pdf":
        return extract_pdf_text(path)
    if suffix in IMAGE_EXTENSIONS:
        return extract_image_text(path)
    if suffix == ".txt":
        return extract_plain_text_file(path)
    if suffix == ".docx":
        return extract_docx_text(path)
    if suffix == ".xlsx":
        return extract_xlsx_text(path)
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
        if is_previous_damage_marker(normalized_line):
            continue
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


def is_previous_damage_marker(normalized_line):
    return bool(
        re.search(
            r"\bv\s*orsch(?:a|ae)d[a-z]*|unreparierte?\s+v\s*orsch|alt\s*schad",
            normalized_line,
        )
    )


def starts_previous_damage_section(normalized_line):
    return any(
        re.search(pattern, normalized_line)
        for pattern in PREVIOUS_DAMAGE_SECTION_START_PATTERNS
    )


def ends_previous_damage_section(normalized_line):
    return any(
        re.search(pattern, normalized_line)
        for pattern in PREVIOUS_DAMAGE_SECTION_END_PATTERNS
    )


def is_non_work_position_line(normalized_line):
    return any(
        re.search(pattern, normalized_line)
        for pattern in NON_WORK_POSITION_LINE_PATTERNS
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
        in_previous_damage = False
        for line in lines:
            normalized_line = normalize_document_text(line)
            if starts_previous_damage_section(normalized_line):
                in_previous_damage = True
                in_damage = False
                continue
            if in_previous_damage:
                if ends_previous_damage_section(normalized_line):
                    in_previous_damage = False
                    if re.search(r"^instandsetzung$|^arb[.\s]*pos[.\s]*nr", normalized_line):
                        in_calc = True
                continue
            if is_previous_damage_marker(normalized_line):
                continue
            if "schadensbeschreibung" in normalized_line or "hauptbeschaedigungsbereich" in normalized_line:
                in_damage = True
                continue
            if re.search(r"^instandsetzung$|^arb[.\s]*pos[.\s]*nr", normalized_line):
                in_calc = True
                continue
            if re.search(r"^e\s*r\s*s\s*a\s*t\s*z", normalized_line):
                in_calc = False
            if is_non_work_position_line(normalized_line):
                continue
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
        if (
            is_ignored_ocr_line(normalized_line)
            or is_previous_damage_marker(normalized_line)
            or is_non_work_position_line(normalized_line)
        ):
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
    previous_damage_parts = extract_previous_damage_parts(cleaned)
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
        inline_date = first_match(line, [r"(\d{2}\.\d{2}\.\d{4})"])
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
                inline_date
                or
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
                inline_date
                or
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
                [r"Fertig\s*bis(?:\s*spaetestens|\s*spätestens|\s*spatestens)?[:.]?\s*(\d{2}\.\d{2}\.\d{4})"],
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
        "_previous_damage_parts": "\n".join(previous_damage_parts),
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
        if has_request_context():
            db = getattr(g, "db_connection", None)
            if db is None:
                db = PostgresConnection(psycopg.connect(DATABASE_URL), close_on_close=False)
                g.db_connection = db
            return db
        return PostgresConnection(psycopg.connect(DATABASE_URL))
    conn = sqlite3.connect(DB, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn)
    return conn


def configure_sqlite_connection(conn):
    global _sqlite_wal_configured
    busy_timeout_ms = SQLITE_BUSY_TIMEOUT_SECONDS * 1000
    try:
        conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        return
    if _sqlite_wal_configured:
        return
    with _sqlite_wal_lock:
        if _sqlite_wal_configured:
            return
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=1000")
            _sqlite_wal_configured = True
        except sqlite3.Error as exc:
            print(f"WARNUNG: SQLite WAL konnte nicht aktiviert werden: {exc}")


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
    def __init__(self, conn, close_on_close=True):
        self.conn = conn
        self.lastrowid = None
        self.close_on_close = close_on_close

    def execute(self, sql, params=()):
        sql = sql.strip()
        if sql.upper() == "SELECT LAST_INSERT_ROWID()":
            return PostgresCursor([DbRow({"last_insert_rowid": self.lastrowid})])

        converted_sql = convert_sqlite_sql_to_postgres(sql)
        params = tuple(params or ())
        lowered = converted_sql.lstrip().lower()
        insert_table = get_insert_table_name(converted_sql)
        inserts_with_id = bool(insert_table and insert_table not in {"app_settings"} and " returning " not in lowered)
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

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if self.close_on_close:
            self.conn.close()

    def force_close(self):
        self.conn.close()


def split_sql_script(script):
    return [part.strip() for part in script.split(";") if part.strip()]


def get_insert_table_name(sql):
    match = re.match(r"\s*insert\s+into\s+\"?([a-zA-Z_][\w]*)\"?", sql, re.IGNORECASE)
    return match.group(1).lower() if match else ""


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


def get_table_column_types(db, table_name):
    if USE_POSTGRES:
        rows = db.execute(
            """
            SELECT column_name AS name, data_type AS type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        ).fetchall()
    else:
        rows = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {
        row["name"]: clean_text(row["type"]).lower()
        for row in rows
    }


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


def ensure_index(db, index_name, table_name, columns):
    column_sql = ", ".join(columns)
    db.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_sql})")


BACKUP_TABLES = (
    "autohaeuser",
    "auftraege",
    "dateien",
    "status_log",
    "chat_nachrichten",
    "benachrichtigungen",
    "verzoegerungen",
    "reklamationen",
)
_backup_lock = threading.Lock()
_backup_thread_started = False
_change_backup_lock = threading.Lock()
_change_backup_pending = False
_change_backup_running = False
_upload_blob_backfill_started = False

DATEI_LIST_COLUMNS = """
    id, auftrag_id, reklamation_id, original_name, stored_name, mime_type, size,
    quelle, kategorie, dokument_typ, extrahierter_text, extrakt_kurz,
    analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am
"""


def list_table_rows_for_backup(db, table_name):
    try:
        columns = get_table_columns(db, table_name)
    except Exception:
        return []
    if not columns:
        return []
    rows = [dict(row) for row in db.execute(f"SELECT * FROM {table_name}").fetchall()]
    if table_name == "dateien":
        for row in rows:
            row.pop("content_blob", None)
    return rows


def write_uploads_to_backup(archive):
    if not UPLOAD_DIR.exists():
        return 0
    count = 0
    for path in sorted(UPLOAD_DIR.iterdir()):
        if not path.is_file():
            continue
        archive.write(path, f"uploads/{path.name}")
        count += 1
    return count


def write_datei_blobs_to_backup(archive, db):
    try:
        rows = db.execute(
            """
            SELECT id, stored_name, content_blob
            FROM dateien
            WHERE content_blob IS NOT NULL AND length(content_blob) > 0
            """
        ).fetchall()
    except Exception as exc:
        print(f"WARNUNG: Datei-Blobs konnten nicht ins Backup geschrieben werden: {exc}")
        return 0

    existing_names = set(archive.namelist())
    count = 0
    for row in rows:
        stored_name = pathlib.Path(clean_text(row["stored_name"])).name
        content = blob_to_bytes(row["content_blob"])
        if not stored_name or not content:
            continue
        archive_name = f"uploads/{stored_name}"
        if archive_name in existing_names:
            continue
        archive.writestr(archive_name, content)
        existing_names.add(archive_name)
        count += 1
    return count


def create_backup_package(reason="auto"):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = BACKUP_DIR / f"kundenstatus-backup-{timestamp}.zip"

    with _backup_lock:
        db = get_db()
        try:
            export = {
                "created_at": now_str(),
                "reason": reason,
                "database": "postgres" if USE_POSTGRES else "sqlite",
                "tables": {},
            }
            for table_name in BACKUP_TABLES:
                export["tables"][table_name] = list_table_rows_for_backup(db, table_name)

            with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "backup.json",
                    json.dumps(export, ensure_ascii=False, indent=2, default=str),
                )
                if not USE_POSTGRES and DB.exists():
                    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                        tmp_path = pathlib.Path(tmp.name)
                    try:
                        sqlite_source = sqlite3.connect(DB, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
                        sqlite_target = sqlite3.connect(tmp_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
                        try:
                            configure_sqlite_connection(sqlite_source)
                            sqlite_source.backup(sqlite_target)
                        finally:
                            sqlite_target.close()
                            sqlite_source.close()
                        archive.write(tmp_path, "auftraege.db")
                    finally:
                        try:
                            tmp_path.unlink()
                        except OSError:
                            pass
                upload_count = write_uploads_to_backup(archive)
                blob_upload_count = write_datei_blobs_to_backup(archive, db)
                archive.writestr(
                    "manifest.json",
                    json.dumps(
                        {
                            "created_at": now_str(),
                            "reason": reason,
                            "backup_file": backup_path.name,
                            "upload_count": upload_count,
                            "blob_upload_count": blob_upload_count,
                            "keep": AUTO_BACKUP_KEEP,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                )
        finally:
            if not has_request_context():
                db.close()

    prune_old_backups()
    return backup_path


def create_safety_backup(reason):
    if not AUTO_BACKUP_ENABLED:
        return None
    try:
        return create_backup_package(reason)
    except Exception as exc:
        print(f"WARNUNG: Sicherheitsbackup fehlgeschlagen ({reason}): {exc}")
        return None


def change_backup_worker():
    global _change_backup_pending, _change_backup_running
    while True:
        time.sleep(AUTO_CHANGE_BACKUP_DELAY_SECONDS)
        with _change_backup_lock:
            if not _change_backup_pending:
                _change_backup_running = False
                return
            _change_backup_pending = False
        try:
            create_backup_package("change")
        except Exception as exc:
            print(f"WARNUNG: Änderungsbackup fehlgeschlagen: {exc}")


def schedule_change_backup(reason="change"):
    global _change_backup_pending, _change_backup_running
    if not AUTO_CHANGE_BACKUP_ENABLED or not AUTO_BACKUP_ENABLED:
        return
    with _change_backup_lock:
        _change_backup_pending = True
        if _change_backup_running:
            return
        _change_backup_running = True
    thread = threading.Thread(target=change_backup_worker, daemon=True)
    thread.start()


DATA_CHANGE_ENDPOINT_EXCLUDES = {
    "admin_backup_sofort",
    "admin_backup_download",
    "login",
    "partner_login",
    "partner_login_key",
    "partner_login_slug",
    "partner_logout",
}


def should_backup_after_request():
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False
    endpoint = clean_text(request.endpoint)
    if not endpoint or endpoint in DATA_CHANGE_ENDPOINT_EXCLUDES:
        return False
    return True


def move_upload_to_deleted_area(path, reason="deleted"):
    if not path.exists() or not path.is_file() or path.parent != UPLOAD_DIR:
        return False
    DELETED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_reason = re.sub(r"[^a-zA-Z0-9_-]+", "-", clean_text(reason))[:40] or "deleted"
    target_name = f"{timestamp}-{safe_reason}-{path.name}"
    target = DELETED_UPLOAD_DIR / target_name
    counter = 1
    while target.exists():
        target = DELETED_UPLOAD_DIR / f"{timestamp}-{safe_reason}-{counter}-{path.name}"
        counter += 1
    shutil.move(str(path), str(target))
    return True


def prune_old_backups():
    if not BACKUP_DIR.exists():
        return
    backups = sorted(
        BACKUP_DIR.glob("kundenstatus-backup-*.zip"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for old_backup in backups[AUTO_BACKUP_KEEP:]:
        try:
            old_backup.unlink()
        except OSError:
            pass


def hourly_backup_worker():
    if not AUTO_BACKUP_ON_STARTUP:
        time.sleep(AUTO_BACKUP_INTERVAL_SECONDS)
    while True:
        try:
            create_backup_package("auto")
        except Exception as exc:
            print(f"WARNUNG: Automatisches Backup fehlgeschlagen: {exc}")
        time.sleep(AUTO_BACKUP_INTERVAL_SECONDS)


def start_hourly_backups():
    global _backup_thread_started
    if _backup_thread_started or not AUTO_BACKUP_ENABLED:
        return
    _backup_thread_started = True
    thread = threading.Thread(target=hourly_backup_worker, daemon=True)
    thread.start()


def init_db():
    global DATA_DIR, DB, UPLOAD_DIR, BACKUP_DIR, DELETED_UPLOAD_DIR
    if USE_POSTGRES:
        try:
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            DELETED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        except PermissionError as exc:
            print(f"WARNUNG: Render-Disk nicht beschreibbar ({exc}). Nutze lokalen Fallback.")
            UPLOAD_DIR = BASE / "data" / "uploads"
            BACKUP_DIR = BASE / "data" / "backups"
            DELETED_UPLOAD_DIR = BASE / "data" / "deleted_uploads"
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            DELETED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    else:
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            DELETED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        except PermissionError as exc:
            print(f"WARNUNG: Render-Disk nicht beschreibbar ({exc}). Nutze lokalen Fallback.")
            DATA_DIR = BASE / "data"
            DB = DATA_DIR / "auftraege.db"
            UPLOAD_DIR = DATA_DIR / "uploads"
            BACKUP_DIR = DATA_DIR / "backups"
            DELETED_UPLOAD_DIR = DATA_DIR / "deleted_uploads"
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            DELETED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key        TEXT PRIMARY KEY,
            value      TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

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
            werkstatt_angebot_notiz TEXT DEFAULT '',
            werkstatt_angebot_am TEXT DEFAULT '',
            status         INTEGER DEFAULT 1,
            annahme_datum  TEXT DEFAULT '',
            start_datum    TEXT DEFAULT '',
            fertig_datum   TEXT DEFAULT '',
            abholtermin    TEXT DEFAULT '',
            transport_art  TEXT DEFAULT 'standard',
            archiviert     INTEGER DEFAULT 0,
            lexware_kunde_angelegt INTEGER DEFAULT 0,
            lexware_contact_id TEXT DEFAULT '',
            lexware_invoice_id TEXT DEFAULT '',
            lexware_invoice_url TEXT DEFAULT '',
            rechnung_status TEXT DEFAULT 'offen',
            rechnung_nummer TEXT DEFAULT '',
            rechnung_geschrieben_am TEXT DEFAULT '',
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
            content_blob   BYTEA,
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

        CREATE TABLE IF NOT EXISTS benachrichtigungen (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id  INTEGER NOT NULL,
            quelle      TEXT NOT NULL,
            titel       TEXT NOT NULL,
            nachricht   TEXT NOT NULL,
            gelesen     INTEGER DEFAULT 0,
            erstellt_am TEXT NOT NULL,
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );

        CREATE TABLE IF NOT EXISTS chat_nachrichten (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id        INTEGER NOT NULL,
            absender          TEXT NOT NULL,
            nachricht         TEXT NOT NULL,
            gelesen_admin     INTEGER DEFAULT 0,
            gelesen_autohaus  INTEGER DEFAULT 0,
            erstellt_am       TEXT NOT NULL,
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
    ensure_column(db, "auftraege", "analyse_autohaus_geprueft", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "analyse_werkstatt_geprueft", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "analyse_geprueft_am", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "angebotsphase", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "angebot_abgesendet", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "angebot_status", "TEXT DEFAULT 'entwurf'")
    ensure_column(db, "auftraege", "werkstatt_angebot_text", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "werkstatt_angebot_preis", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "werkstatt_angebot_notiz", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "werkstatt_angebot_am", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "annahme_datum", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "start_datum", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "abholtermin", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "transport_art", "TEXT DEFAULT 'standard'")
    ensure_column(db, "auftraege", "archiviert", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "lexware_kunde_angelegt", "INTEGER DEFAULT 0")
    ensure_column(db, "auftraege", "lexware_contact_id", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "lexware_invoice_id", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "lexware_invoice_url", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "rechnung_status", "TEXT DEFAULT 'offen'")
    ensure_column(db, "auftraege", "rechnung_nummer", "TEXT DEFAULT ''")
    ensure_column(db, "auftraege", "rechnung_geschrieben_am", "TEXT DEFAULT ''")
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
    ensure_column(db, "dateien", "content_blob", "BYTEA")
    ensure_column(db, "autohaeuser", "portal_key", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "portal_titel", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "willkommen_text", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "strasse", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "plz", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "ort", "TEXT DEFAULT ''")

    ensure_index(db, "idx_auftraege_dashboard", "auftraege", ("archiviert", "angebotsphase", "autohaus_id"))
    ensure_index(db, "idx_auftraege_angebote", "auftraege", ("angebotsphase", "angebot_abgesendet", "autohaus_id"))
    ensure_index(db, "idx_dateien_auftrag", "dateien", ("auftrag_id", "kategorie", "reklamation_id"))
    ensure_index(db, "idx_status_log_lookup", "status_log", ("status", "zeitstempel", "auftrag_id"))
    ensure_index(db, "idx_verzoegerungen_offen", "verzoegerungen", ("uebernommen", "erstellt_am"))
    ensure_index(db, "idx_reklamationen_offen", "reklamationen", ("bearbeitet", "erstellt_am"))

    seed_default_autohaeuser(db)
    seed_default_auftraege(db)

    rows = db.execute("SELECT id, portal_key FROM autohaeuser").fetchall()
    for row in rows:
        if not clean_text(row["portal_key"]):
            db.execute(
                "UPDATE autohaeuser SET portal_key=? WHERE id=?",
                (uuid.uuid4().hex[:16], row["id"]),
            )

    db.commit()
    db.close()


def seed_default_autohaeuser(db):
    now = now_str()
    for autohaus in DEFAULT_AUTOHAEUSER:
        existing = db.execute(
            "SELECT id FROM autohaeuser WHERE slug=? OR portal_key=?",
            (autohaus["slug"], autohaus["portal_key"]),
        ).fetchone()
        values = (
            autohaus["name"],
            autohaus["slug"],
            autohaus["portal_key"],
            autohaus["kontakt_name"],
            autohaus["email"],
            autohaus["telefon"],
            autohaus["strasse"],
            autohaus["plz"],
            autohaus["ort"],
            autohaus["zugangscode"],
            autohaus["portal_titel"],
            autohaus["willkommen_text"],
            clean_text(autohaus.get("notiz")),
        )
        if existing:
            db.execute(
                """
                UPDATE autohaeuser
                SET name=?, slug=?, portal_key=?, kontakt_name=?, email=?, telefon=?,
                    strasse=?, plz=?, ort=?, zugangscode=?, portal_titel=?,
                    willkommen_text=?, notiz=?
                WHERE id=?
                """,
                values + (existing["id"],),
            )
            continue
        db.execute(
            """
            INSERT INTO autohaeuser
            (name, slug, portal_key, kontakt_name, email, telefon, strasse, plz, ort,
             zugangscode, portal_titel, willkommen_text, notiz, erstellt_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values + (now,),
        )


def seed_default_auftraege(db):
    now = now_str()
    for auftrag in DEFAULT_AUFTRAEGE:
        existing = db.execute(
            """
            SELECT id FROM auftraege
            WHERE kennzeichen=? OR (auftragsnummer != '' AND auftragsnummer=?)
            """,
            (auftrag["kennzeichen"], auftrag["auftragsnummer"]),
        ).fetchone()
        if existing:
            continue
        autohaus = db.execute(
            "SELECT id FROM autohaeuser WHERE slug=?",
            (auftrag["autohaus_slug"],),
        ).fetchone()
        if not autohaus:
            continue
        db.execute(
            """
            INSERT INTO auftraege
            (token, kunde_email, autohaus_id, kunde_name, fahrzeug, fin_nummer,
             auftragsnummer, rep_max_kosten, bauteile_override, kennzeichen,
             beschreibung, analyse_text, angebotsphase, angebot_abgesendet,
             angebot_status, werkstatt_angebot_text, werkstatt_angebot_preis,
             werkstatt_angebot_am, status, annahme_datum, start_datum,
             fertig_datum, abholtermin, transport_art, archiviert, kontakt_telefon,
             notiz_intern, quelle, erstellt_am, geaendert_am)
            VALUES (?, '', ?, ?, ?, ?, ?, '', ?, ?, ?, ?, 0, 0, '', '', '', '',
                    ?, ?, ?, ?, ?, 'standard', 0, '', ?, 'intern', ?, ?)
            """,
            (
                uuid.uuid4().hex[:12],
                autohaus["id"],
                auftrag["kunde_name"],
                auftrag["fahrzeug"],
                auftrag["fin_nummer"],
                auftrag["auftragsnummer"],
                auftrag["bauteile_override"],
                auftrag["kennzeichen"],
                auftrag["beschreibung"],
                auftrag["analyse_text"],
                auftrag["status"],
                auftrag["annahme_datum"],
                auftrag["start_datum"],
                auftrag["fertig_datum"],
                auftrag["abholtermin"],
                auftrag["notiz_intern"],
                now,
                now,
            ),
        )
        auftrag_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO status_log (auftrag_id, status, zeitstempel) VALUES (?, ?, ?)",
            (auftrag_id, auftrag["status"], now),
        )


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


def lackierauftrag_filename(autohaus):
    slug = clean_text(autohaus.get("slug")) or slugify(autohaus.get("name")) or "autohaus"
    return f"Lackierauftrag_{slug}.pdf"


def make_lackierauftrag_pdf(autohaus):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    def text(value):
        return escape(clean_text(value))

    def style(name, size=9, bold=False, color=None, align=TA_LEFT, space=0):
        return ParagraphStyle(
            name,
            fontName="Helvetica-Bold" if bold else "Helvetica",
            fontSize=size,
            textColor=color or colors.HexColor("#1A1A1A"),
            alignment=align,
            spaceAfter=space,
            leading=size * 1.3,
        )

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )

    orange = colors.HexColor("#E8651A")
    light_gray = colors.HexColor("#F0F0F0")
    mid_gray = colors.HexColor("#CCCCCC")
    black = colors.HexColor("#1A1A1A")

    hdr = style("hdr", 22, bold=True)
    sub = style("sub", 8, color=colors.HexColor("#555555"), align=TA_RIGHT)
    label = style("lbl", 8)
    bold8 = style("b8", 8, bold=True)
    bold9 = style("b9", 9, bold=True)
    small = style("sm", 7, color=colors.HexColor("#555555"))
    footer_style = style("ft", 8, align=TA_CENTER)

    telefon_email = " / ".join(
        part
        for part in (clean_text(autohaus.get("telefon")), clean_text(autohaus.get("email")))
        if part
    )
    firma = text(autohaus.get("name"))
    ansprechpartner = text(autohaus.get("kontakt_name"))
    kontakt = text(telefon_email)

    story = []
    story.append(
        Table(
            [
                [
                    Paragraph("Lackierauftrag", hdr),
                    Paragraph(
                        "Gärtner Karosserie &amp; Lack GmbH<br/>"
                        "Binauer Höhe 4 · 74821 Mosbach-Lohrbach<br/>"
                        "Tel. +49 1522 770 66 94 · info@auto-lackierzentrum.de",
                        sub,
                    ),
                ]
            ],
            colWidths=[9 * cm, 9 * cm],
            style=TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, orange),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ]
            ),
        )
    )
    story.append(Spacer(1, 8))

    left_rows = [
        [Paragraph("Auftraggeber", bold9), ""],
        [Paragraph("Firma:", label), Paragraph(firma, bold8)],
        [Paragraph("Ansprechpartner:", label), Paragraph(ansprechpartner, bold8)],
        [Paragraph("Tel. / E-Mail:", label), Paragraph(kontakt, bold8)],
        [Paragraph("Auftrags-Datum:", label), ""],
        [Paragraph("Fertig bis spätestens:", label), ""],
    ]
    right_rows = [
        [Paragraph("Fahrzeug", bold9), ""],
        [Paragraph("Typ:", label), ""],
        [Paragraph("Amtl. Kennzeichen:", label), ""],
        [Paragraph("Fg.-Nr.:", label), ""],
        [Paragraph("Farb-Nr.:", label), ""],
        [Paragraph("km-Stand:", label), ""],
    ]

    table_style = TableStyle(
        [
            ("GRID", (0, 1), (-1, -1), 0.5, mid_gray),
            ("BACKGROUND", (0, 0), (-1, 0), light_gray),
            ("SPAN", (0, 0), (1, 0)),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
        ]
    )
    row_h = 0.72 * cm
    left_t = Table(left_rows, colWidths=[3.5 * cm, 5.2 * cm], rowHeights=[0.6 * cm] + [row_h] * 5, style=table_style)
    right_t = Table(right_rows, colWidths=[4.2 * cm, 4.5 * cm], rowHeights=[0.6 * cm] + [row_h] * 5, style=table_style)
    story.append(
        Table(
            [[left_t, right_t]],
            colWidths=[8.9 * cm, 9.1 * cm],
            style=TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (1, 0), (1, 0), 8)]),
        )
    )
    story.append(Spacer(1, 10))

    story.append(
        Table(
            [
                [Paragraph("Abrechnung", bold9), "", "", ""],
                [
                    Paragraph("[ ] Selbstzahler", label),
                    Paragraph("[ ] Kaskoversicherung", label),
                    Paragraph("[ ] Haftpflicht gegnerisch", label),
                    Paragraph("[ ] Sammelrechnung", label),
                ],
                [Paragraph("Versicherung / Schaden-Nr.:", label), "", Paragraph("Vers.-Nehmer:", label), ""],
            ],
            colWidths=[4.5 * cm, 4.5 * cm, 4.5 * cm, 4.5 * cm],
            rowHeights=[0.55 * cm, 0.6 * cm, 0.7 * cm],
            style=TableStyle(
                [
                    ("SPAN", (0, 0), (3, 0)),
                    ("BACKGROUND", (0, 0), (3, 0), light_gray),
                    ("FONTNAME", (0, 0), (3, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 2), (1, 2), 0.5, mid_gray),
                    ("GRID", (2, 2), (3, 2), 0.5, mid_gray),
                    ("SPAN", (0, 2), (1, 2)),
                    ("SPAN", (2, 2), (3, 2)),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("LINEBELOW", (0, 0), (3, 0), 0.5, mid_gray),
                ]
            ),
        )
    )
    story.append(Spacer(1, 10))

    data = [[Paragraph("Karosserieteil", bold9), Paragraph("Seite", bold9), Paragraph("Bemerkung / Arbeitsumfang", bold9), Paragraph("I.O. / n.I.O.", bold9)]]
    data.extend([["", "", "", ""] for _ in range(14)])
    story.append(
        Table(
            data,
            colWidths=[4.5 * cm, 2 * cm, 9 * cm, 2.5 * cm],
            rowHeights=[0.55 * cm] + [0.7 * cm] * 14,
            style=TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, mid_gray),
                    ("BACKGROUND", (0, 0), (-1, 0), light_gray),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                ]
            ),
        )
    )
    story.append(Spacer(1, 14))

    story.append(
        Table(
            [
                [Paragraph("Auftrag ordnungsgemäß ausgeführt / Qualitätskontrolle durchgeführt", footer_style)],
                [
                    Table(
                        [[Paragraph("Datum:", label), "", Paragraph("Unterschrift Auftragnehmer:", label), ""]],
                        colWidths=[2 * cm, 5.5 * cm, 5 * cm, 5.5 * cm],
                        rowHeights=[0.8 * cm],
                        style=TableStyle(
                            [
                                ("LINEBELOW", (1, 0), (1, 0), 0.5, mid_gray),
                                ("LINEBELOW", (3, 0), (3, 0), 0.5, mid_gray),
                                ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
                                ("FONTSIZE", (0, 0), (-1, -1), 8),
                                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                            ]
                        ),
                    )
                ],
            ],
            colWidths=[18 * cm],
            style=TableStyle(
                [
                    ("LINEABOVE", (0, 0), (0, 0), 1.5, orange),
                    ("TOPPADDING", (0, 0), (0, 0), 6),
                    ("ALIGN", (0, 0), (0, 0), "CENTER"),
                ]
            ),
        )
    )

    doc.build(story)
    buffer.seek(0)
    return buffer


def send_lackierauftrag_pdf(autohaus):
    return send_file(
        make_lackierauftrag_pdf(autohaus),
        download_name=lackierauftrag_filename(autohaus),
        mimetype="application/pdf",
        as_attachment=True,
    )


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
    auftrag["lexware_kunde_angelegt"] = bool(auftrag.get("lexware_kunde_angelegt"))
    auftrag["lexware_contact_id"] = clean_text(auftrag.get("lexware_contact_id"))
    auftrag["lexware_invoice_id"] = clean_text(auftrag.get("lexware_invoice_id"))
    auftrag["lexware_invoice_url"] = clean_text(auftrag.get("lexware_invoice_url"))
    auftrag["rechnung_status"] = clean_text(auftrag.get("rechnung_status")) or "offen"
    auftrag["rechnung_nummer"] = clean_text(auftrag.get("rechnung_nummer"))
    auftrag["rechnung_geschrieben_am"] = clean_text(auftrag.get("rechnung_geschrieben_am"))
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


def build_lexware_rechnung_context(auftrag):
    autohaus = get_autohaus(auftrag["autohaus_id"]) if auftrag.get("autohaus_id") else None
    kunde_name = clean_text(autohaus.get("name") if autohaus else "") or clean_text(auftrag.get("kunde_name"))
    kunde = {
        "name": kunde_name or "Kunde noch eintragen",
        "kontakt_name": clean_text(autohaus.get("kontakt_name") if autohaus else ""),
        "email": clean_text(autohaus.get("email") if autohaus else auftrag.get("kunde_email")),
        "telefon": clean_text(autohaus.get("telefon") if autohaus else auftrag.get("kontakt_telefon")),
        "strasse": clean_text(autohaus.get("strasse") if autohaus else ""),
        "plz": clean_text(autohaus.get("plz") if autohaus else ""),
        "ort": clean_text(autohaus.get("ort") if autohaus else ""),
        "quelle": "Autohaus" if autohaus else "Endkunde / Referenz",
    }
    positionen = []
    angebot_text = clean_text(auftrag.get("werkstatt_angebot_text"))
    angebot_preis = clean_text(auftrag.get("werkstatt_angebot_preis"))
    if angebot_text or angebot_preis:
        positionen.append(
            {
                "bezeichnung": angebot_text or "Karosserie- und Lackierarbeiten",
                "preis": angebot_preis,
            }
        )
    else:
        positionen.append(
            {
                "bezeichnung": clean_text(auftrag.get("analyse_text"))
                or clean_text(auftrag.get("beschreibung"))
                or "Karosserie- und Lackierarbeiten",
                "preis": clean_text(auftrag.get("rep_max_kosten")),
            }
        )
    belegtext = "\n".join(
        part
        for part in [
            f"Auftrag #{auftrag['id']}",
            f"Autohaus: {clean_text(auftrag.get('autohaus_name'))}" if auftrag.get("autohaus_name") else "",
            f"Endkunde/Referenz: {clean_text(auftrag.get('kunde_name'))}" if auftrag.get("kunde_name") else "",
            f"Fahrzeug: {clean_text(auftrag.get('fahrzeug'))}",
            f"Kennzeichen: {clean_text(auftrag.get('kennzeichen'))}" if auftrag.get("kennzeichen") else "",
            f"FIN: {clean_text(auftrag.get('fin_nummer'))}" if auftrag.get("fin_nummer") else "",
            f"Auftragsnummer: {clean_text(auftrag.get('auftragsnummer'))}" if auftrag.get("auftragsnummer") else "",
            f"Zurückgegeben am: {clean_text(auftrag.get('abholtermin'))}" if auftrag.get("abholtermin") else "",
        ]
        if part
    )
    return {
        "kunde": kunde,
        "positionen": positionen,
        "belegtext": belegtext,
        "netto_betrag": parse_money_amount(clean_text(auftrag.get("werkstatt_angebot_preis"))),
        "api_ready": bool(LEXWARE_API_KEY),
        "tax_rate": LEXWARE_TAX_RATE,
        "lexware_kunden_url": LEXWARE_KUNDEN_URL,
        "lexware_rechnungen_url": LEXWARE_RECHNUNGEN_URL,
    }


def parse_money_amount(value):
    text = clean_text(value)
    if not text:
        return None
    matches = re.findall(r"\d+(?:[.\s]\d{3})*(?:[,.]\d+)?", text)
    if not matches:
        return None
    raw = matches[-1].replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", raw):
        raw = raw.replace(".", "")
    try:
        return round(float(raw), 2)
    except ValueError:
        return None


def format_bonus_money(value):
    try:
        amount = round(float(value or 0), 2)
    except (TypeError, ValueError):
        amount = 0.0
    integer, decimals = f"{amount:.2f}".split(".")
    groups = []
    while integer:
        groups.append(integer[-3:])
        integer = integer[:-3]
    return f"{'.'.join(reversed(groups))},{decimals} EUR"


def auftrag_bonus_preis(auftrag):
    for key, label in (
        ("werkstatt_angebot_preis", "Werkstatt-Angebot"),
        ("rep_max_kosten", "Reparaturwert"),
    ):
        amount = parse_money_amount((auftrag or {}).get(key))
        if amount:
            return amount, label
    internal_note = clean_text((auftrag or {}).get("notiz_intern"))
    if "netto" in normalize_document_text(internal_note):
        amount = parse_money_amount(internal_note)
        if amount:
            return amount, "Fahrzeugdaten"
    return 0.0, ""


def build_bonusmodell(auftraege, reference_date=None):
    today = reference_date or date.today()
    month_start = date(today.year, today.month, 1)
    month_end = date(today.year + int(today.month == 12), 1 if today.month == 12 else today.month + 1, 1) - timedelta(days=1)
    bonus_items = []
    monatsumsatz = 0.0
    offene_preise = 0

    for auftrag in auftraege or []:
        if auftrag.get("angebotsphase"):
            continue
        if int(auftrag.get("status") or 1) < 5:
            continue
        rueckgabe = parse_date(auftrag.get("abholtermin")) or parse_date(auftrag.get("geaendert_am"))
        if not rueckgabe or rueckgabe < month_start or rueckgabe > month_end:
            continue
        betrag, quelle = auftrag_bonus_preis(auftrag)
        preis_fehlt = not bool(betrag)
        if preis_fehlt:
            offene_preise += 1
        else:
            monatsumsatz += betrag
        bonus_items.append(
            {
                "auftrag": auftrag,
                "datum_text": format_date(rueckgabe),
                "betrag": betrag,
                "betrag_label": format_bonus_money(betrag) if betrag else "",
                "quelle": quelle,
                "preis_fehlt": preis_fehlt,
            }
        )

    aktive_stufe = {"schwelle": 0.0, "satz": 0.0, "label": "0 %", "schwelle_label": "unter 3.000 EUR"}
    for stufe in BONUSMODELL_STUFEN:
        if monatsumsatz >= stufe["schwelle"]:
            aktive_stufe = stufe

    naechste_stufe = next((stufe for stufe in BONUSMODELL_STUFEN if monatsumsatz < stufe["schwelle"]), None)
    bonus_netto = round(monatsumsatz * aktive_stufe["satz"], 2)
    stufen = []
    for stufe in BONUSMODELL_STUFEN:
        item = dict(stufe)
        item["aktiv"] = stufe["label"] == aktive_stufe["label"]
        item["erreicht"] = monatsumsatz >= stufe["schwelle"]
        stufen.append(item)

    return {
        "monat_label": f"{month_start.strftime('%m.%Y')}",
        "umsatz_netto": round(monatsumsatz, 2),
        "umsatz_netto_label": format_bonus_money(monatsumsatz),
        "bonus_satz_label": aktive_stufe["label"],
        "bonus_netto": bonus_netto,
        "bonus_netto_label": format_bonus_money(bonus_netto),
        "verrechnung_label": "offen" if bonus_netto > 0 else "unter Schwelle",
        "stufen": stufen,
        "naechste_stufe": naechste_stufe,
        "bis_naechste_stufe_label": (
            format_bonus_money(max(0.0, naechste_stufe["schwelle"] - monatsumsatz))
            if naechste_stufe
            else ""
        ),
        "auftraege": sorted(bonus_items, key=lambda item: item["datum_text"], reverse=True),
        "offene_preise_count": offene_preise,
        "gezaehlte_auftraege_count": sum(1 for item in bonus_items if not item["preis_fehlt"]),
    }


def get_partner_assistant_auftrag(auftrag_id, autohaus_id):
    try:
        auftrag_id = int(auftrag_id or 0)
    except (TypeError, ValueError):
        return None
    if auftrag_id <= 0:
        return None
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or int(auftrag.get("autohaus_id") or 0) != int(autohaus_id or 0):
        return None
    return auftrag


def is_vehicle_planning_question(question):
    text = normalize_document_text(question)
    vehicle_terms = ("fahrzeug", "fahrzeuge", "auto", "autos", "wagen", "auftrag", "auftraege")
    planning_terms = ("eingeplant", "geplant", "planung", "termin", "termine", "heute", "morgen", "woche", "start", "fertig", "abholung")
    return any(term in text for term in vehicle_terms) and any(term in text for term in planning_terms)


def assistant_vehicle_line(auftrag):
    fahrzeug = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
    kennzeichen = clean_text(auftrag.get("kennzeichen"))
    status = clean_text((auftrag.get("status_meta") or {}).get("label")) or "Status offen"
    event_parts = []
    for feld, label, _ in EVENT_FELDER:
        if auftrag.get(feld):
            event_parts.append(f"{label} {auftrag[feld]}")
    details = f" ({kennzeichen})" if kennzeichen else ""
    termin = ", ".join(event_parts[:4]) if event_parts else "kein Termin gesetzt"
    return f"{fahrzeug}{details}: {termin}, Status {status}"


def assistant_planned_vehicle_answer(question, autohaus_id):
    auftraege = [a for a in list_auftraege(autohaus_id) if not a.get("archiviert") and not a.get("angebotsphase")]
    dated = []
    for auftrag in auftraege:
        dates = [auftrag.get(f"{feld}_obj") for feld, _, _ in EVENT_FELDER if auftrag.get(f"{feld}_obj")]
        if dates:
            dated.append((min(dates), auftrag))
    dated.sort(key=lambda item: item[0])
    if not dated:
        return "Aktuell sind keine aktiven Fahrzeugtermine hinterlegt."
    lines = [f"- {assistant_vehicle_line(auftrag)}" for _, auftrag in dated[:6]]
    rest = len(dated) - len(lines)
    if rest > 0:
        lines.append(f"- Weitere {rest} Fahrzeuge stehen im Portal.")
    return "Aktuelle Fahrzeugplanung:\n" + "\n".join(lines)


def partner_assistant_context_text(autohaus, auftrag=None):
    parts = [
        f"Autohaus: {clean_text((autohaus or {}).get('name'))}",
        "Portal-Funktionen: Fahrzeuge anlegen, Unterlagen hochladen, Angebote anfragen, Status und Termine ansehen.",
    ]
    if auftrag:
        parts.append(
            "Aktueller Auftrag: "
            + "; ".join(
                part
                for part in [
                    clean_text(auftrag.get("fahrzeug")),
                    clean_text(auftrag.get("kennzeichen")),
                    clean_text(auftrag.get("analyse_text") or auftrag.get("beschreibung")),
                    assistant_vehicle_line(auftrag),
                ]
                if part
            )
        )
    planned = assistant_planned_vehicle_answer("fahrzeuge termine", autohaus["id"])
    parts.append(planned)
    return "\n".join(parts)


def fallback_partner_assistant_answer(question, autohaus=None):
    text = normalize_document_text(question)
    if autohaus and is_vehicle_planning_question(question):
        return assistant_planned_vehicle_answer(question, autohaus["id"])
    if "bonus" in text or "stufe" in text:
        return "Das Bonusmodell sehen Sie im Kundenportal. Dort stehen Monatsumsatz, aktuelle Stufe, Bonus netto und der Betrag bis zur nächsten Stufe."
    if "angebot" in text:
        return "Für ein Angebot können Sie im Portal 'Angebot anfordern' nutzen. Die Werkstatt prüft die Anfrage und stellt das Angebot anschließend zur Annahme bereit."
    if "upload" in text or "datei" in text or "bild" in text:
        return "Sie können Bilder, PDFs und Unterlagen direkt im Auftrag oder beim neuen Fahrzeug hochladen. Danach prüft die Werkstatt die Daten."
    if "termin" in text or "kalender" in text:
        return "Die Termine sehen Sie im Monatsblick und in den Fahrzeugdetails. Feiertage und Betriebsurlaub sind im Kalender markiert."
    return "Ich helfe direkt hier im Autohaus-Portal: Fragen Sie nach Fahrzeugen, Terminen, Status, Uploads, Angeboten oder dem Bonusmodell."


def ask_partner_assistant(question, autohaus, auftrag=None):
    question = clean_text(question)[:900]
    if not question:
        return "Bitte geben Sie eine Frage ein.", "portal"
    if is_vehicle_planning_question(question):
        return assistant_planned_vehicle_answer(question, autohaus["id"]), "portal"

    config = get_ai_config()
    requests_module = get_requests()
    if not config["openai_ready"] or requests_module is None:
        return fallback_partner_assistant_answer(question, autohaus), "fallback"

    payload = {
        "model": config["openai_model"],
        "messages": [
            {
                "role": "system",
                "content": (
                    "Du bist der digitale KI-Helfer im Autohaus-Portal von Gärtner Karosserie & Lack. "
                    "Antworte kurz, freundlich und praktisch. Hilf bei Fahrzeugstatus, Terminen, Uploads, "
                    "Angeboten und Bonusmodell. Erfinde keine Preise, Termine oder Zusagen."
                ),
            },
            {"role": "system", "content": partner_assistant_context_text(autohaus, auftrag)},
            {"role": "user", "content": question},
        ],
        "temperature": 0.2,
        "max_tokens": 320,
    }
    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error or response is None or response.status_code >= 400:
        return fallback_partner_assistant_answer(question, autohaus), "fallback"
    try:
        answer = clean_text(response.json()["choices"][0]["message"]["content"])
    except Exception:
        answer = ""
    return (answer or fallback_partner_assistant_answer(question, autohaus), "openai" if answer else "fallback")


def lexware_request(method, path, payload=None, query=""):
    if not LEXWARE_API_KEY:
        raise RuntimeError("LEXWARE_API_KEY fehlt. Bitte in .env.local eintragen.")
    requests_module = get_requests()
    if requests_module is None:
        raise RuntimeError("Python-Paket requests ist nicht verfügbar.")
    url = f"{LEXWARE_API_BASE_URL}{path}{query}"
    response = None
    for attempt in range(3):
        response = requests_module.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {LEXWARE_API_KEY}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )
        if response.status_code != 429 or attempt == 2:
            break
        retry_after = response.headers.get("Retry-After")
        try:
            wait_seconds = min(max(int(retry_after or 0), 1), 8)
        except ValueError:
            wait_seconds = 2 + attempt * 2
        time.sleep(wait_seconds)
    if response.status_code >= 400:
        details = ""
        try:
            details = response.json()
        except Exception:
            details = response.text[:500]
        if response.status_code == 429:
            raise RuntimeError(
                "Lexware hat gerade zu viele Anfragen bekommen. "
                "Bitte 1 bis 2 Minuten warten und dann genau einmal erneut klicken."
            )
        raise RuntimeError(f"Lexware API Fehler {response.status_code}: {details}")
    if response.status_code == 204 or not response.content:
        return {}
    return response.json()


def find_lexware_contact(kunde):
    name = clean_text(kunde.get("name"))
    if len(name) < 3:
        return None
    data = lexware_request(
        "GET",
        "/v1/contacts",
        query=f"?name={quote(name)}&customer=true",
    )
    for contact in data.get("content", []):
        company_name = clean_text((contact.get("company") or {}).get("name"))
        person = contact.get("person") or {}
        person_name = clean_text(f"{person.get('firstName', '')} {person.get('lastName', '')}")
        if company_name.lower() == name.lower() or person_name.lower() == name.lower():
            return contact
    content = data.get("content") or []
    return content[0] if content else None


def create_lexware_contact(kunde):
    payload = {
        "version": 0,
        "roles": {"customer": {}},
        "company": {"name": clean_text(kunde.get("name"))},
        "addresses": {"billing": [build_lexware_contact_address(kunde)]},
    }
    email = clean_text(kunde.get("email"))
    telefon = clean_text(kunde.get("telefon"))
    if email:
        payload["emailAddresses"] = {"business": [email]}
    if telefon:
        payload["phoneNumbers"] = {"business": [telefon]}
    return lexware_request("POST", "/v1/contacts", payload)


def build_lexware_contact_address(kunde):
    address = {"countryCode": "DE"}
    if clean_text(kunde.get("strasse")):
        address["street"] = clean_text(kunde.get("strasse"))
    if clean_text(kunde.get("plz")):
        address["zip"] = clean_text(kunde.get("plz"))
    if clean_text(kunde.get("ort")):
        address["city"] = clean_text(kunde.get("ort"))
    return address


def build_lexware_invoice_address(kunde):
    address = build_lexware_contact_address(kunde)
    address["name"] = clean_text(kunde.get("name")) or "Kunde"
    return address


def get_lexware_contact(contact_id):
    if not clean_text(contact_id):
        return None
    return lexware_request("GET", f"/v1/contacts/{contact_id}")


def lexware_contact_has_single_billing_address(contact):
    addresses = (contact or {}).get("addresses") or {}
    billing = addresses.get("billing") or []
    return len(billing) == 1


def ensure_lexware_contact(kunde):
    contact = find_lexware_contact(kunde)
    if contact:
        contact_id = contact["id"]
        full_contact = get_lexware_contact(contact_id)
        return contact_id, False, lexware_contact_has_single_billing_address(full_contact)
    created = create_lexware_contact(kunde)
    contact_id = created["id"]
    full_contact = get_lexware_contact(contact_id)
    return contact_id, True, lexware_contact_has_single_billing_address(full_contact)


def lexware_datetime(value=None):
    parsed = parse_date(value) or date.today()
    return f"{parsed.strftime('%Y-%m-%d')}T00:00:00.000+01:00"


def create_lexware_invoice_draft(auftrag, rechnung, net_amount):
    contact_id, contact_created, can_reference_contact = ensure_lexware_contact(rechnung["kunde"])
    position = rechnung["positionen"][0] if rechnung["positionen"] else {}
    description = rechnung["belegtext"]
    invoice_address = (
        {"contactId": contact_id}
        if can_reference_contact
        else build_lexware_invoice_address(rechnung["kunde"])
    )
    payload = {
        "archived": False,
        "voucherDate": lexware_datetime(),
        "address": invoice_address,
        "lineItems": [
            {
                "type": "custom",
                "name": clean_text(position.get("bezeichnung")) or "Karosserie- und Lackierarbeiten",
                "description": description,
                "quantity": 1,
                "unitName": "Stück",
                "unitPrice": {
                    "currency": "EUR",
                    "netAmount": net_amount,
                    "taxRatePercentage": LEXWARE_TAX_RATE,
                },
                "discountPercentage": 0,
            }
        ],
        "totalPrice": {"currency": "EUR"},
        "taxConditions": {"taxType": "net"},
        "shippingConditions": {
            "shippingDate": lexware_datetime(auftrag.get("abholtermin")),
            "shippingType": "delivery",
        },
        "title": "Rechnung",
        "introduction": "Die ausgeführten Karosserie- und Lackierarbeiten stellen wir Ihnen hiermit in Rechnung.",
        "remark": "Vielen Dank für Ihren Auftrag.",
    }
    created = lexware_request("POST", "/v1/invoices", payload)
    invoice_id = created["id"]
    invoice_url = f"{LEXWARE_APP_BASE_URL}/permalink/invoices/edit/{invoice_id}"
    return {
        "contact_id": contact_id,
        "contact_created": contact_created,
        "invoice_id": invoice_id,
        "invoice_url": invoice_url,
    }


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

    auftraege = [row_to_auftrag(row) for row in rows]
    auftraege.sort(
        key=lambda a: (
            a["status"] >= 4,
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
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug,
                   (
                       SELECT COUNT(*)
                       FROM dateien d
                       WHERE d.auftrag_id = a.id
                   ) AS dateien_count
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE a.angebotsphase = 1 AND a.angebot_abgesendet = 1
            ORDER BY a.geaendert_am DESC, a.id DESC
            """
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT a.*, h.name AS autohaus_name, h.slug AS autohaus_slug,
                   (
                       SELECT COUNT(*)
                       FROM dateien d
                       WHERE d.auftrag_id = a.id
                   ) AS dateien_count
            FROM auftraege a
            LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
            WHERE a.angebotsphase = 1 AND a.angebot_abgesendet = 1 AND a.autohaus_id = ?
            ORDER BY a.geaendert_am DESC, a.id DESC
            """,
            (autohaus_id,),
        ).fetchall()
    db.close()
    anfragen = [row_to_auftrag(row) for row in rows]
    dateien_by_auftrag = list_dateien_for_auftraege([anfrage["id"] for anfrage in anfragen])
    for anfrage in anfragen:
        anfrage["dateien_count"] = int(anfrage.get("dateien_count") or 0)
        anfrage["dateien"] = dateien_by_auftrag.get(int(anfrage["id"]), [])
    return anfragen


def get_status_ids_logged_today(auftraege, status):
    ids = [int(a["id"]) for a in auftraege if a.get("status") == status]
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    heute_prefix = date.today().strftime(DATE_FMT) + "%"
    db = get_db()
    rows = db.execute(
        f"""
        SELECT DISTINCT auftrag_id
        FROM status_log
        WHERE status=?
          AND zeitstempel LIKE ?
          AND auftrag_id IN ({placeholders})
        """,
        (status, heute_prefix, *ids),
    ).fetchall()
    db.close()
    return {int(row["auftrag_id"]) for row in rows}


def is_today_finished_auftrag(auftrag, today_finished_ids, today):
    return (
        auftrag["status"] == 4
        and (
            auftrag["fertig_datum_obj"] == today
            or int(auftrag["id"]) in today_finished_ids
        )
    )


def list_dateien(auftrag_id):
    db = get_db()
    rows = db.execute(
        f"SELECT {DATEI_LIST_COLUMNS} FROM dateien WHERE auftrag_id=? ORDER BY hochgeladen_am DESC, id DESC",
        (auftrag_id,),
    ).fetchall()
    db.close()
    return [hydrate_datei(dict(row)) for row in rows]


def list_dateien_for_auftraege(auftrag_ids):
    ids = [int(auftrag_id) for auftrag_id in auftrag_ids if auftrag_id]
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    db = get_db()
    rows = db.execute(
        f"""
        SELECT {DATEI_LIST_COLUMNS}
        FROM dateien
        WHERE auftrag_id IN ({placeholders})
        ORDER BY hochgeladen_am DESC, id DESC
        """,
        ids,
    ).fetchall()
    db.close()
    grouped = {auftrag_id: [] for auftrag_id in ids}
    for row in rows:
        datei = hydrate_datei(dict(row))
        grouped.setdefault(int(datei["auftrag_id"]), []).append(datei)
    return grouped


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


def blob_to_bytes(value):
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return b""


def update_datei_content_blob(datei_id, content):
    if not datei_id or not content:
        return
    db = get_db()
    try:
        db.execute(
            "UPDATE dateien SET content_blob=?, size=? WHERE id=?",
            (content, len(content), datei_id),
        )
        db.commit()
    finally:
        db.close()


def remember_existing_datei_file(datei, path):
    if blob_to_bytes(datei.get("content_blob")):
        return
    try:
        content = path.read_bytes()
    except OSError:
        return
    update_datei_content_blob(datei.get("id"), content)


def newest_backup_paths():
    if not BACKUP_DIR.exists():
        return []

    def sort_key(path):
        try:
            return path.stat().st_mtime
        except OSError:
            return 0

    return sorted(BACKUP_DIR.glob("kundenstatus-backup-*.zip"), key=sort_key, reverse=True)


def restore_datei_content_from_backups(stored_name):
    member_name = f"uploads/{stored_name}"
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    for backup_path in newest_backup_paths():
        try:
            with zipfile.ZipFile(backup_path) as archive:
                if member_name not in archive.namelist():
                    continue
                with archive.open(member_name) as source:
                    content = source.read(max_bytes + 1)
        except Exception as exc:
            print(f"WARNUNG: Upload-Restore aus {backup_path.name} fehlgeschlagen: {exc}")
            continue
        if content and len(content) <= max_bytes:
            return content
    return b""


def write_restored_datei_file(stored_name, content):
    path = UPLOAD_DIR / stored_name
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path
    except OSError:
        fallback = pathlib.Path(tempfile.gettempdir()) / stored_name
        fallback.write_bytes(content)
        return fallback


def resolve_datei_path(datei):
    stored_name = pathlib.Path(clean_text(datei.get("stored_name"))).name
    if not stored_name:
        return None
    path = UPLOAD_DIR / stored_name
    if path.exists():
        remember_existing_datei_file(datei, path)
        return path

    content = blob_to_bytes(datei.get("content_blob"))
    if not content:
        content = restore_datei_content_from_backups(stored_name)
        if not content:
            return None
        update_datei_content_blob(datei.get("id"), content)
    return write_restored_datei_file(stored_name, content)


def backfill_upload_content_blobs(limit=None):
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    limit = max(1, int(limit or env_int("UPLOAD_BLOB_BACKFILL_LIMIT", 5000)))
    db = get_db()
    saved = 0
    missing = 0
    try:
        rows = db.execute(
            """
            SELECT id, stored_name
            FROM dateien
            WHERE content_blob IS NULL OR length(content_blob)=0
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for row in rows:
            stored_name = pathlib.Path(clean_text(row["stored_name"])).name
            if not stored_name:
                missing += 1
                continue
            path = UPLOAD_DIR / stored_name
            content = b""
            try:
                if path.exists() and path.is_file() and path.stat().st_size <= max_bytes:
                    content = path.read_bytes()
            except OSError:
                content = b""
            if not content:
                content = restore_datei_content_from_backups(stored_name)
            if content:
                db.execute(
                    "UPDATE dateien SET content_blob=?, size=? WHERE id=?",
                    (content, len(content), row["id"]),
                )
                saved += 1
            else:
                missing += 1
        db.commit()
    finally:
        db.close()
    if saved or missing:
        print(f"Upload-Nachsicherung: {saved} Datei(en) in der Datenbank gesichert, {missing} fehlen weiterhin.")
    return {"saved": saved, "missing": missing}


def upload_blob_backfill_worker():
    try:
        backfill_upload_content_blobs()
    except Exception as exc:
        print(f"WARNUNG: Upload-Nachsicherung fehlgeschlagen: {exc}")


def start_upload_blob_backfill():
    global _upload_blob_backfill_started
    if _upload_blob_backfill_started or not env_flag("UPLOAD_BLOB_BACKFILL_ON_STARTUP", True):
        return
    _upload_blob_backfill_started = True
    thread = threading.Thread(target=upload_blob_backfill_worker, daemon=True)
    thread.start()


def missing_datei_response(datei, back_url=None, replace_url=None):
    back_url = clean_text(back_url)
    if not back_url:
        back_url = url_for("auftrag_detail", auftrag_id=datei["auftrag_id"])
    return (
        render_template_string(
            """
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Datei nicht verfügbar</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <main class="container py-5">
    <div class="alert alert-warning">
      <h1 class="h4">Datei ist nicht mehr im Speicher vorhanden.</h1>
      <p class="mb-2">
        Der Datenbankeintrag existiert noch, aber die Originaldatei
        <strong>{{ datei['original_name'] }}</strong> liegt nicht mehr im Upload-Speicher.
      </p>
      <p class="mb-0">Bitte die Unterlage in diesem Auftrag einmal neu hochladen.</p>
    </div>
    {% if replace_url %}
    <form method="POST" action="{{ replace_url }}" enctype="multipart/form-data" class="card card-body mb-3">
      {{ csrf_field()|safe }}
      <label class="form-label fw-semibold">Fehlende Datei ersetzen</label>
      <input type="file" name="datei" class="form-control mb-3" required>
      <button type="submit" class="btn btn-dark align-self-start">Datei ersetzen</button>
    </form>
    {% endif %}
    <a class="btn btn-outline-dark" href="{{ back_url }}">Zurück zum Auftrag</a>
  </main>
</body>
</html>
            """,
            datei=datei,
            back_url=back_url,
            replace_url=replace_url,
        ),
        404,
    )


def replace_datei_content(datei, file_storage):
    if not datei:
        raise ValueError("Datei-Eintrag nicht gefunden.")
    if not file_storage or not file_storage.filename:
        raise ValueError("Bitte eine Datei auswählen.")

    original_name = secure_filename(file_storage.filename)
    if not original_name or not allowed_file(original_name):
        raise ValueError("Dieser Dateityp ist nicht erlaubt.")

    suffix = pathlib.Path(original_name).suffix.lower()
    stored_name = f"{uuid.uuid4().hex}{suffix}"
    target = UPLOAD_DIR / stored_name
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(target)
    content = target.read_bytes()
    mime_type = file_storage.mimetype or mimetypes.guess_type(original_name)[0] or "application/octet-stream"

    dokument_typ = clean_text(datei.get("dokument_typ"))
    extrahierter_text = clean_text(datei.get("extrahierter_text"))
    extrakt_kurz = clean_text(datei.get("extrakt_kurz"))
    analyse_quelle = clean_text(datei.get("analyse_quelle"))
    analyse_json = clean_text(datei.get("analyse_json"))
    analyse_hinweis = clean_text(datei.get("analyse_hinweis"))
    is_analysis_document = (
        clean_text(datei.get("kategorie")) == "standard"
        and datei.get("reklamation_id") is None
        and suffix in ANALYSIS_EXTENSIONS
    )
    if is_analysis_document:
        bundle = build_document_analysis_bundle_safe(target, original_name)
        extrahierter_text = clean_text(bundle.get("text"))
        analyse_quelle = clean_text(bundle.get("source"))
        analyse_json = clean_text(bundle.get("analysis_json"))
        analyse_hinweis = clean_text(bundle.get("hint"))
        dokument_typ = (
            classify_document(extrahierter_text, original_name)
            if extrahierter_text
            else classify_document("", original_name)
        )
        extrakt_kurz = summarize_document_text(extrahierter_text, original_name)

    old_stored_name = pathlib.Path(clean_text(datei.get("stored_name"))).name
    if old_stored_name and old_stored_name != stored_name:
        move_upload_to_deleted_area(UPLOAD_DIR / old_stored_name, f"replace-datei-{datei['id']}")

    db = get_db()
    try:
        db.execute(
            """
            UPDATE dateien
            SET original_name=?,
                stored_name=?,
                mime_type=?,
                size=?,
                dokument_typ=?,
                extrahierter_text=?,
                extrakt_kurz=?,
                analyse_quelle=?,
                analyse_json=?,
                analyse_hinweis=?,
                content_blob=?,
                hochgeladen_am=?
            WHERE id=?
            """,
            (
                original_name,
                stored_name,
                mime_type,
                len(content),
                dokument_typ,
                extrahierter_text,
                extrakt_kurz,
                analyse_quelle,
                analyse_json,
                analyse_hinweis,
                content,
                now_str(),
                datei["id"],
            ),
        )
        db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now_str(), datei["auftrag_id"]))
        db.commit()
    finally:
        db.close()

    if is_analysis_document:
        apply_document_data_to_auftrag(datei["auftrag_id"], prefer_documents=False)
    return stored_name


def datei_ersetzen_form_response(datei, action_url=None, back_url=None):
    action_url = clean_text(action_url) or url_for("admin_datei_ersetzen", datei_id=datei["id"])
    back_url = clean_text(back_url) or url_for("auftrag_detail", auftrag_id=datei["auftrag_id"])
    return render_template_string(
        """
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Datei ersetzen</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <main class="container py-5">
    <div class="card shadow-sm">
      <div class="card-body p-4">
        <h1 class="h4 mb-2">Datei ersetzen</h1>
        <p class="text-muted mb-4">
          Datei-ID {{ datei['id'] }} · aktuell hinterlegt:
          <strong>{{ datei['original_name'] }}</strong>
        </p>
        <form method="POST" action="{{ action_url }}" enctype="multipart/form-data">
          {{ csrf_field()|safe }}
          <label class="form-label fw-semibold">Originaldatei neu hochladen</label>
          <input type="file" name="datei" class="form-control form-control-lg mb-3" required>
          <div class="d-flex gap-2 flex-wrap">
            <button type="submit" class="btn btn-dark btn-lg">Datei ersetzen</button>
            <a class="btn btn-outline-dark btn-lg" href="{{ back_url }}">Zurück zum Auftrag</a>
          </div>
        </form>
      </div>
    </div>
  </main>
</body>
</html>
        """,
        datei=datei,
        action_url=action_url,
        back_url=back_url,
    )


def delete_partner_datei(autohaus_id, datei_id):
    datei = get_datei(datei_id)
    if not datei:
        return False, None
    auftrag = get_auftrag(datei["auftrag_id"])
    if (
        not auftrag
        or int(auftrag.get("autohaus_id") or 0) != int(autohaus_id or 0)
        or clean_text(datei.get("quelle")) != "autohaus"
    ):
        return False, None

    stored_name = pathlib.Path(clean_text(datei.get("stored_name"))).name
    path = UPLOAD_DIR / stored_name
    try:
        move_upload_to_deleted_area(path, f"partner-datei-{datei_id}")
    except OSError:
        pass

    db = get_db()
    db.execute("DELETE FROM dateien WHERE id=? AND auftrag_id=? AND quelle='autohaus'", (datei_id, auftrag["id"]))
    db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now_str(), auftrag["id"]))
    db.commit()
    db.close()

    if clean_text(datei.get("kategorie")) == "standard":
        reset_document_review_checks(
            auftrag["id"],
            "Eine hochgeladene Unterlage wurde entfernt. Bitte die erkannten Daten kurz prüfen.",
        )
    return True, auftrag


def hydrate_datei(datei):
    if not datei:
        return None

    suffix = pathlib.Path(datei["original_name"]).suffix.lower()
    datei["is_pdf"] = suffix == ".pdf"
    datei["is_image"] = suffix in IMAGE_EXTENSIONS
    datei["is_browser_image"] = suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}
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
    previous_damage_parts = parse_manual_parts((local_fields or {}).get("_previous_damage_parts"))
    local_analysis = clean_text((local_fields or {}).get("analyse_text"))
    if previous_damage_parts:
        cleaned_analysis = remove_previous_damage_fragments(
            fields.get("analyse_text"),
            previous_damage_parts,
            local_analysis,
        )
        cleaned_bauteile = remove_previous_damage_fragments(
            fields.get("bauteile_override"),
            previous_damage_parts,
            local_analysis,
        )
        if clean_text(fields.get("analyse_text")) and cleaned_analysis != clean_text(fields.get("analyse_text")):
            fields["analyse_text"] = cleaned_analysis
            review_notes.append("Vorschäden aus dem Gutachten wurden nicht als aktuelle Arbeit übernommen.")
        if clean_text(fields.get("bauteile_override")) and cleaned_bauteile != clean_text(fields.get("bauteile_override")):
            fields["bauteile_override"] = cleaned_bauteile
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


def ensure_document_review_fallback(fields, extracted_text="", original_name=""):
    fields = dict(fields or {})
    if not clean_text(fields.get("analyse_text")):
        summary = summarize_document_text(extracted_text, original_name)
        if summary:
            fields["analyse_text"] = summary[:220]
    if not clean_text(fields.get("beschreibung")):
        doc_type = classify_document(extracted_text, original_name)
        parts = [part for part in (doc_type, summarize_document_text(extracted_text, original_name)) if clean_text(part)]
        if parts:
            fields["beschreibung"] = ". ".join(parts)[:500]
    if clean_text(fields.get("analyse_text")) or clean_text(fields.get("beschreibung")):
        add_analysis_note(
            fields,
            "Bitte überprüfen: Die Unterlage wurde automatisch eingetragen, aber die Erkennung ist unsicher.",
        )
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


def reset_document_review_checks(auftrag_id, reason=""):
    hint = clean_text(reason) or "Neue Unterlage hochgeladen. Bitte erkannte Werte gegen die Originaldatei prüfen."
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET analyse_pruefen=1,
            analyse_hinweis=?,
            analyse_autohaus_geprueft=0,
            analyse_werkstatt_geprueft=0,
            analyse_geprueft_am='',
            geaendert_am=?
        WHERE id=?
        """,
        (hint, now_str(), auftrag_id),
    )
    db.commit()
    db.close()


def confirm_document_review(auftrag_id, role):
    if role not in {"autohaus", "werkstatt"}:
        return False
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        return False
    autohaus_checked = 1 if role == "autohaus" else int(auftrag.get("analyse_autohaus_geprueft") or 0)
    werkstatt_checked = 1 if role == "werkstatt" else int(auftrag.get("analyse_werkstatt_geprueft") or 0)
    both_checked = bool(autohaus_checked and werkstatt_checked)
    hint = (
        "Dokumentdaten wurden von Autohaus und Werkstatt geprüft."
        if both_checked
        else "Dokumentdaten sind sichtbar. Bitte zweite Prüfung noch abschließen."
    )
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET analyse_autohaus_geprueft=?,
            analyse_werkstatt_geprueft=?,
            analyse_pruefen=?,
            analyse_hinweis=?,
            analyse_geprueft_am=?,
            geaendert_am=?
        WHERE id=?
        """,
        (
            autohaus_checked,
            werkstatt_checked,
            0 if both_checked else 1,
            hint,
            now_str() if both_checked else "",
            now_str(),
            auftrag_id,
        ),
    )
    db.commit()
    db.close()
    return both_checked


def apply_document_data_to_auftrag(auftrag_id, prefer_documents=False):
    db = get_db()
    auftrag_row = db.execute("SELECT * FROM auftraege WHERE id=?", (auftrag_id,)).fetchone()
    if not auftrag_row:
        db.close()
        return {}

    auftrag = dict(auftrag_row)
    analysis_double_checked = bool(
        int(auftrag.get("analyse_autohaus_geprueft") or 0)
        and int(auftrag.get("analyse_werkstatt_geprueft") or 0)
    )
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
        felder = ensure_document_review_fallback(
            felder,
            datei["extrahierter_text"],
            datei["original_name"],
        )
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
    if "analyse_pruefen" in erkannt and not analysis_double_checked:
        updates["analyse_pruefen"] = 1 if erkannt.get("analyse_pruefen") else 0
    if erkannt.get("analyse_hinweis") and not analysis_double_checked:
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


def add_benachrichtigung(auftrag_id, titel, nachricht, quelle="werkstatt"):
    titel = clean_text(titel)
    nachricht = clean_text(nachricht)
    if not titel or not nachricht:
        return
    db = get_db()
    db.execute(
        """
        INSERT INTO benachrichtigungen
        (auftrag_id, quelle, titel, nachricht, gelesen, erstellt_am)
        VALUES (?, ?, ?, ?, 0, ?)
        """,
        (auftrag_id, clean_text(quelle) or "werkstatt", titel, nachricht, now_str()),
    )
    db.commit()
    db.close()


def list_benachrichtigungen(auftrag_id, limit=20):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM benachrichtigungen
        WHERE auftrag_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (auftrag_id, limit),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def list_autohaus_benachrichtigungen(autohaus_id, limit=10):
    db = get_db()
    rows = db.execute(
        """
        SELECT b.*, a.fahrzeug, a.kennzeichen, a.auftragsnummer
        FROM benachrichtigungen b
        JOIN auftraege a ON a.id = b.auftrag_id
        WHERE a.autohaus_id=? AND a.archiviert=0 AND COALESCE(b.gelesen, 0)=0
        ORDER BY b.id DESC
        LIMIT ?
        """,
        (autohaus_id, limit),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def mark_autohaus_benachrichtigung_gelesen(autohaus_id, benachrichtigung_id):
    db = get_db()
    db.execute(
        """
        UPDATE benachrichtigungen
        SET gelesen=1
        WHERE id=?
          AND auftrag_id IN (
            SELECT id FROM auftraege WHERE autohaus_id=?
          )
        """,
        (benachrichtigung_id, autohaus_id),
    )
    db.commit()
    db.close()


def add_chat_nachricht(auftrag_id, absender, nachricht):
    nachricht = clean_text(nachricht)
    absender = clean_text(absender) or "werkstatt"
    if absender not in {"werkstatt", "autohaus"}:
        absender = "werkstatt"
    if not nachricht:
        return None
    gelesen_admin = 1 if absender == "werkstatt" else 0
    gelesen_autohaus = 1 if absender == "autohaus" else 0
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO chat_nachrichten
        (auftrag_id, absender, nachricht, gelesen_admin, gelesen_autohaus, erstellt_am)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (auftrag_id, absender, nachricht, gelesen_admin, gelesen_autohaus, now_str()),
    )
    db.execute(
        "UPDATE auftraege SET geaendert_am=? WHERE id=?",
        (now_str(), auftrag_id),
    )
    db.commit()
    chat_id = cursor.lastrowid
    db.close()
    return chat_id


def list_chat_nachrichten(auftrag_id):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM chat_nachrichten
        WHERE auftrag_id=?
        ORDER BY id ASC
        """,
        (auftrag_id,),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def mark_chat_gelesen(auftrag_id, empfaenger):
    empfaenger = clean_text(empfaenger)
    if empfaenger not in {"admin", "autohaus"}:
        return
    if empfaenger == "admin":
        sql = """
            UPDATE chat_nachrichten
            SET gelesen_admin=1
            WHERE auftrag_id=? AND absender='autohaus' AND gelesen_admin=0
        """
    else:
        sql = """
            UPDATE chat_nachrichten
            SET gelesen_autohaus=1
            WHERE auftrag_id=? AND absender='werkstatt' AND gelesen_autohaus=0
        """
    db = get_db()
    db.execute(sql, (auftrag_id,))
    db.commit()
    db.close()


def list_offene_chat_nachrichten(limit=12):
    db = get_db()
    rows = db.execute(
        """
        SELECT c.*, a.fahrzeug, a.kennzeichen, a.auftragsnummer, h.name AS autohaus_name
        FROM chat_nachrichten c
        JOIN auftraege a ON a.id = c.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE c.absender='autohaus' AND c.gelesen_admin=0 AND a.archiviert=0
        ORDER BY c.id DESC
        LIMIT ?
        """,
        (limit,),
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
        f"SELECT {DATEI_LIST_COLUMNS} FROM dateien WHERE reklamation_id=? ORDER BY hochgeladen_am DESC, id DESC",
        (reklamation_id,),
    ).fetchall()
    db.close()
    return [hydrate_datei(dict(row)) for row in rows]


def save_uploads(auftrag_id, files, quelle, kategorie="standard", reklamation_id=None):
    saved = 0
    saved_analysis_document = False
    analysis_errors = []
    db = get_db()
    timestamp = now_str()
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        db.close()
        return 0, {"_analysis_error": f"Upload-Speicher ist nicht erreichbar: {clean_text(str(exc))[:300]}"}
    for file in files:
        if not file or not file.filename:
            continue
        original_name = secure_filename(file.filename)
        if not original_name or not allowed_file(original_name):
            continue
        suffix = pathlib.Path(original_name).suffix.lower()
        stored_name = f"{uuid.uuid4().hex}{suffix}"
        target = UPLOAD_DIR / stored_name
        try:
            file.save(target)
        except Exception as exc:
            analysis_errors.append(f"{original_name} konnte nicht gespeichert werden: {clean_text(str(exc))[:300]}")
            continue
        try:
            content_blob = target.read_bytes()
        except OSError:
            content_blob = b""
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
        if is_analysis_document and suffix in ANALYSIS_EXTENSIONS:
            saved_analysis_document = True
            bundle = build_document_analysis_bundle_safe(target, original_name)
            extrahierter_text = bundle.get("text", "")
            analyse_quelle = clean_text(bundle.get("source"))
            analyse_json = clean_text(bundle.get("analysis_json"))
            analyse_hinweis = clean_text(bundle.get("hint"))
            if bundle.get("status") == "error" and analyse_hinweis:
                analysis_errors.append(analyse_hinweis)
            elif analyse_hinweis and not (clean_text(extrahierter_text) or clean_text(analyse_json)):
                analysis_errors.append(analyse_hinweis)
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
             extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json, analyse_hinweis, content_blob, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                content_blob,
                timestamp,
            ),
        )     
        saved += 1
    db.commit()
    db.close()
    if not saved and analysis_errors:
        return 0, {"_analysis_error": analysis_errors[0]}
    if saved_analysis_document:
        try:
            updates = apply_document_data_to_auftrag(auftrag_id, prefer_documents=False) or {}
            reset_document_review_checks(
                auftrag_id,
                clean_text(updates.get("analyse_hinweis"))
                or "Neue Unterlage hochgeladen. Bitte erkannte Werte gegen die Originaldatei prüfen.",
            )
        except Exception as exc:
            updates = {"_analysis_error": f"Analyse konnte nicht übernommen werden: {clean_text(str(exc))[:300]}"}
        if analysis_errors and not clean_text(updates.get("_analysis_error")):
            updates["_analysis_error"] = analysis_errors[0]
        return saved, updates
    return saved, {}


def flash_upload_analysis_result(saved_result, success_message="Datei hochgeladen."):
    if isinstance(saved_result, tuple):
        saved, updates = saved_result
    else:
        saved, updates = saved_result, {}
    analysis_error = clean_text((updates or {}).get("_analysis_error"))
    if not saved:
        if analysis_error:
            flash(f"Datei konnte nicht ausgewertet werden. {analysis_error}", "warning")
        return saved
    if analysis_error:
        flash(f"Datei gespeichert, aber die Analyse ist abgebrochen. {analysis_error}", "warning")
        return saved
    meaningful_updates = {
        key: value
        for key, value in (updates or {}).items()
        if key not in {"geaendert_am", "analyse_pruefen", "analyse_confidence"}
        and clean_text(value)
    }
    if meaningful_updates:
        flash(
            "Datei hochgeladen und Analyse sichtbar gemacht. Bitte erkannte Werte prüfen.",
            "warning",
        )
    else:
        flash(
            "Datei hochgeladen und zur Prüfung eingetragen. Bitte die erkannten Daten kontrollieren.",
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
        if suffix not in ANALYSIS_EXTENSIONS:
            continue
        path = UPLOAD_DIR / clean_text(datei.get("stored_name"))
        if not path.exists():
            continue
        bundle = build_document_analysis_bundle_safe(path, original_name)
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
    updates = apply_document_data_to_auftrag(auftrag_id, prefer_documents=False) if count else {}
    if count:
        reset_document_review_checks(
            auftrag_id,
            clean_text(updates.get("analyse_hinweis"))
            or "Unterlagen neu analysiert. Bitte erkannte Werte gegen die Originaldatei prüfen.",
        )
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


def get_allowed_finish_uploads(files):
    uploads = []
    allowed = IMAGE_EXTENSIONS | {".pdf"}
    for file in files or []:
        if not file or not file.filename:
            continue
        original_name = secure_filename(file.filename)
        if not original_name:
            continue
        if pathlib.Path(original_name).suffix.lower() not in allowed:
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
    geplante_ids = []
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
        geplante_ids.append(auftrag_id)
    if geplante_ids:
        create_safety_backup(f"before-bulk-delete-{len(geplante_ids)}")
    for auftrag_id in geplante_ids:
        delete_auftrag(auftrag_id, safety_backup=False)
        geloescht += 1
    return geloescht


def delete_auftrag(auftrag_id, safety_backup=True):
    if safety_backup:
        create_safety_backup(f"before-delete-auftrag-{auftrag_id}")
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
            move_upload_to_deleted_area(path, f"auftrag-{auftrag_id}")
        except OSError:
            pass
    db.execute("DELETE FROM dateien WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM reklamationen WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM verzoegerungen WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM status_log WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM benachrichtigungen WHERE auftrag_id=?", (auftrag_id,))
    db.execute("DELETE FROM chat_nachrichten WHERE auftrag_id=?", (auftrag_id,))
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


def send_workshop_offer(auftrag_id, angebot_text, angebot_preis, angebot_notiz=""):
    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET werkstatt_angebot_text=?,
            werkstatt_angebot_preis=?,
            werkstatt_angebot_notiz=?,
            werkstatt_angebot_am=?,
            angebot_status='angebot_abgegeben',
            geaendert_am=?
        WHERE id=? AND angebotsphase=1
        """,
        (
            clean_text(angebot_text),
            clean_text(angebot_preis),
            clean_text(angebot_notiz),
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
    offene_chat_nachrichten = list_offene_chat_nachrichten()

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
    heute_fertig_ids = get_status_ids_logged_today(auftraege, 4)
    heute_fertig = [
        a for a in auftraege if is_today_finished_auftrag(a, heute_fertig_ids, heute)
    ]
    zurueckgegeben = [a for a in auftraege if a["status"] == 5]
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
        "zurueckgegeben": zurueckgegeben,
        "ueberfaellig": ueberfaellig,
        "offene_verzoegerungen": offene_verzoegerungen,
        "offene_reklamationen": offene_reklamationen,
        "offene_chat_nachrichten": offene_chat_nachrichten,
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


def easter_date(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def bw_feiertage(year):
    ostern = easter_date(year)
    return {
        date(year, 1, 1): "Neujahr",
        date(year, 1, 6): "Heilige Drei Könige",
        ostern - timedelta(days=2): "Karfreitag",
        ostern + timedelta(days=1): "Ostermontag",
        date(year, 5, 1): "Tag der Arbeit",
        ostern + timedelta(days=39): "Christi Himmelfahrt",
        ostern + timedelta(days=50): "Pfingstmontag",
        ostern + timedelta(days=60): "Fronleichnam",
        date(year, 10, 3): "Tag der Deutschen Einheit",
        date(year, 11, 1): "Allerheiligen",
        date(year, 12, 25): "1. Weihnachtsfeiertag",
        date(year, 12, 26): "2. Weihnachtsfeiertag",
    }


BETRIEBSURLAUB_ZEITRAEUME = (
    (date(2026, 8, 19), date(2026, 9, 4), "Betriebsurlaub"),
)


def parse_mini_calendar_month(value):
    cleaned = clean_text(value)
    if cleaned:
        try:
            parsed = datetime.strptime(cleaned, "%Y-%m").date()
            return date(parsed.year, parsed.month, 1)
        except ValueError:
            pass
    today = date.today()
    return date(today.year, today.month, 1)


def shift_month(month_start, offset):
    month_index = month_start.month - 1 + offset
    year = month_start.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def build_mini_monatskalender(
    auftraege,
    month_value="",
    endpoint="",
    route_values=None,
    only_arrival_events=False,
    include_timeline=False,
):
    month_start = parse_mini_calendar_month(month_value)
    month_end = shift_month(month_start, 1) - timedelta(days=1)
    today = date.today()
    cal = calendar.Calendar(firstweekday=0)
    month_names = {
        1: "Januar",
        2: "Februar",
        3: "März",
        4: "April",
        5: "Mai",
        6: "Juni",
        7: "Juli",
        8: "August",
        9: "September",
        10: "Oktober",
        11: "November",
        12: "Dezember",
    }

    event_dates = defaultdict(list)
    for auftrag in auftraege or []:
        event_fields = EVENT_FELDER
        if only_arrival_events:
            event_fields = (("annahme_datum", "Anlieferung", "secondary"),)
        for feld, label, _ in event_fields:
            event_date = auftrag.get(f"{feld}_obj")
            if event_date and month_start <= event_date <= month_end:
                party_name = (
                    clean_text(auftrag.get("autohaus_name"))
                    or clean_text(auftrag.get("kunde_name"))
                    or "Kunde noch eintragen"
                )
                fahrzeug_name = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
                kennzeichen = clean_text(auftrag.get("kennzeichen"))
                vehicle_label = f"{fahrzeug_name} · {kennzeichen}" if kennzeichen else fahrzeug_name
                title = f"{label}: {party_name} | {vehicle_label}"
                if only_arrival_events:
                    rueckgabe = (
                        auftrag.get("abholtermin_obj")
                        or parse_date(auftrag.get("abholtermin"))
                        or parse_date(auftrag.get("fertig_datum"))
                    )
                    if rueckgabe:
                        title = f"{title} | Rückgabe: {rueckgabe.strftime(DATE_FMT)}"
                event_dates[event_date].append(
                    {
                        "label": label,
                        "party_name": party_name,
                        "vehicle_label": vehicle_label,
                        "tooltip": title,
                    }
                )

    holidays = bw_feiertage(month_start.year)
    betriebsurlaub_dates = defaultdict(list)
    for start, end, title in BETRIEBSURLAUB_ZEITRAEUME:
        current = max(start, month_start)
        last_day = min(end, month_end)
        while current <= last_day:
            betriebsurlaub_dates[current].append(title)
            current += timedelta(days=1)

    weeks = []
    for week in cal.monthdatescalendar(month_start.year, month_start.month):
        row = []
        for current in week:
            holiday_title = holidays.get(current)
            labels = []
            if holiday_title:
                labels.append(holiday_title)
            labels.extend(betriebsurlaub_dates.get(current, []))
            day_events = sorted(
                event_dates.get(current, []),
                key=lambda event: (
                    clean_text(event.get("party_name")).lower(),
                    clean_text(event.get("vehicle_label")).lower(),
                    clean_text(event.get("label")).lower(),
                ),
            )
            labels.extend([event["tooltip"] for event in day_events[:3]])
            row.append(
                {
                    "tag": current.day,
                    "datum_text": current.strftime(DATE_FMT),
                    "in_month": current.month == month_start.month,
                    "is_today": current == today,
                    "is_weekend": current.weekday() >= 5,
                    "is_holiday": bool(holiday_title),
                    "has_betriebsurlaub": bool(betriebsurlaub_dates.get(current)),
                    "has_events": bool(day_events),
                    "event_count": len(day_events),
                    "events": day_events[:2],
                    "more_event_count": max(0, len(day_events) - 2),
                    "tooltip": " | ".join(labels),
                }
            )
        weeks.append(row)

    timeline_days = []
    current = month_start
    while current <= month_end:
        timeline_days.append(
            {
                "tag": current.day,
                "weekday": ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"][current.weekday()],
                "is_weekend": current.weekday() >= 5,
                "is_today": current == today,
            }
        )
        current += timedelta(days=1)

    timeline_rows = []
    if include_timeline:
        color_classes = (
            "timeline-color-blue",
            "timeline-color-green",
            "timeline-color-warm",
            "timeline-color-gold",
            "timeline-color-red",
            "timeline-color-steel",
        )
        for auftrag in auftraege or []:
            all_dates = [
                auftrag.get(f"{feld}_obj")
                for feld, _, _ in EVENT_FELDER
                if auftrag.get(f"{feld}_obj")
            ]
            if not all_dates:
                continue
            start_date = auftrag.get("annahme_datum_obj") or auftrag.get("start_datum_obj") or min(all_dates)
            end_date = auftrag.get("abholtermin_obj") or auftrag.get("fertig_datum_obj") or max(all_dates)
            if end_date < start_date:
                end_date = start_date
            if end_date < month_start or start_date > month_end:
                continue
            visible_start = max(start_date, month_start)
            visible_end = min(end_date, month_end)
            party_name = (
                clean_text(auftrag.get("autohaus_name"))
                or clean_text(auftrag.get("kunde_name"))
                or "Kunde noch eintragen"
            )
            fahrzeug_name = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
            kennzeichen = clean_text(auftrag.get("kennzeichen"))
            vehicle_label = f"{fahrzeug_name} · {kennzeichen}" if kennzeichen else fahrzeug_name
            detail_url = ""
            if has_request_context() and auftrag.get("id"):
                detail_url = url_for("auftrag_detail", auftrag_id=auftrag["id"])
            timeline_rows.append(
                {
                    "party_name": party_name,
                    "vehicle_label": vehicle_label,
                    "start_text": start_date.strftime(DATE_FMT),
                    "end_text": end_date.strftime(DATE_FMT),
                    "short_range": f"{start_date.strftime('%d.%m.')} - {end_date.strftime('%d.%m.')}",
                    "start_col": (visible_start - month_start).days + 1,
                    "end_col": (visible_end - month_start).days + 2,
                    "detail_url": detail_url,
                    "color_class": color_classes[len(timeline_rows) % len(color_classes)],
                }
            )
        timeline_rows.sort(
            key=lambda row: (
                row["start_col"],
                clean_text(row["party_name"]).lower(),
                clean_text(row["vehicle_label"]).lower(),
            )
        )

    route_values = dict(route_values or {})
    prev_url = next_url = ""
    if endpoint and has_request_context():
        prev_url = url_for(endpoint, **route_values, monat=shift_month(month_start, -1).strftime("%Y-%m"))
        next_url = url_for(endpoint, **route_values, monat=shift_month(month_start, 1).strftime("%Y-%m"))

    return {
        "title": f"{month_names[month_start.month]} {month_start.year}",
        "prev_url": prev_url,
        "next_url": next_url,
        "today_text": today.strftime(DATE_FMT),
        "weekdays": ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"],
        "weeks": weeks,
        "event_label": "Anlieferung" if only_arrival_events else "Fahrzeugtermin",
        "show_timeline": include_timeline,
        "timeline_days": timeline_days,
        "timeline_days_count": len(timeline_days),
        "timeline_rows": timeline_rows,
    }


def autohaus_dashboard_daten(auftraege):
    heute = date.today()
    heute_fertig_ids = get_status_ids_logged_today(auftraege, 4)
    return {
        "heute_text": format_date(heute.strftime("%Y-%m-%d")),
        "heute_bringen": [a for a in auftraege if a["annahme_datum_obj"] == heute],
        "heute_abholen": [a for a in auftraege if a["abholtermin_obj"] == heute],
        "in_arbeit": [
            a
            for a in auftraege
            if a["status"] == 3 or (a["start_datum_obj"] and a["start_datum_obj"] <= heute and a["status"] < 4)
        ],
        "heute_fertig": [
            a for a in auftraege if is_today_finished_auftrag(a, heute_fertig_ids, heute)
        ],
        "zurueckgegeben": [a for a in auftraege if a["status"] == 5],
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


def render_partner_new_form(autohaus):
    try:
        return render_template(
            "partner_neu.html",
            autohaus=autohaus,
            transport_arten=TRANSPORT_ARTEN,
        )
    except Exception as exc:
        print(f"FEHLER partner_neu.html: {exc}")
        return render_template_string(
            """
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ autohaus['portal_label'] }} - Neues Fahrzeug</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { background:#f4f1ea; }
    .shell { max-width:900px; margin:0 auto; padding:24px 14px; }
    .panel { background:#fffdf9; border-radius:18px; padding:24px; box-shadow:0 12px 34px rgba(64,47,25,.12); }
    .form-control,.form-select,.btn { min-height:48px; font-size:16px; }
  </style>
</head>
<body>
  <div class="shell">
    {% with messages = get_flashed_messages(with_categories=true) %}
      {% for cat, msg in messages %}
      <div class="alert alert-{{ cat }} mb-3">{{ msg }}</div>
      {% endfor %}
    {% endwith %}
    <div class="panel">
      <div class="d-flex justify-content-between align-items-center gap-3 flex-wrap mb-4">
        <div>
          <div class="text-muted">{{ autohaus['portal_welcome'] }}</div>
          <h1 class="h2 mb-0">Neues Fahrzeug für {{ autohaus['portal_label'] }}</h1>
        </div>
        <a href="{{ url_for('partner_dashboard', slug=autohaus['slug']) }}" class="btn btn-outline-dark">Zurück</a>
      </div>
      <form method="POST" enctype="multipart/form-data" class="row g-3">
        <div class="col-md-6">
          <label class="form-label">Endkunde / Referenz</label>
          <input type="text" name="kunde_name" class="form-control">
        </div>
        <div class="col-md-6">
          <label class="form-label">Telefon</label>
          <input type="text" name="kontakt_telefon" class="form-control">
        </div>
        <div class="col-md-8">
          <label class="form-label">Fahrzeug</label>
          <input type="text" name="fahrzeug" class="form-control" placeholder="Wird beim Upload, wenn möglich, automatisch erkannt">
        </div>
        <div class="col-md-4">
          <label class="form-label">Kennzeichen</label>
          <input type="text" name="kennzeichen" class="form-control" style="text-transform:uppercase;">
        </div>
        <div class="col-md-6">
          <label class="form-label">Kurzbeschreibung / Analyse</label>
          <input type="text" name="analyse_text" class="form-control">
        </div>
        <div class="col-md-6">
          <label class="form-label">Datei hochladen</label>
          <input type="file" name="dateien" class="form-control" multiple accept=".jpg,.jpeg,.png,.webp,.gif,.heic,.pdf,.txt,.docx,.xlsx,image/*,application/pdf">
        </div>
        <div class="col-12">
          <label class="form-label">Beschreibung</label>
          <textarea name="beschreibung" class="form-control" rows="4"></textarea>
        </div>
        <div class="col-md-4">
          <label class="form-label">Transport</label>
          <select name="transport_art" class="form-select">
            {% for key, meta in transport_arten.items() %}
            <option value="{{ key }}">{{ meta['label'] }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="col-md-4">
          <label class="form-label">Anlieferung</label>
          <input type="date" name="annahme_datum" class="form-control">
        </div>
        <div class="col-md-4">
          <label class="form-label">Abholung</label>
          <input type="date" name="abholtermin" class="form-control">
        </div>
        <div class="col-12 d-grid gap-2 d-md-flex">
          <button type="submit" name="aktion" value="upload_analyze" class="btn btn-outline-dark">Hochladen & analysieren</button>
          <button type="submit" name="aktion" value="speichern" class="btn btn-dark">Fahrzeug speichern</button>
        </div>
      </form>
    </div>
  </div>
</body>
</html>
            """,
            autohaus=autohaus,
            transport_arten=TRANSPORT_ARTEN,
        )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if admin_password_matches(request.form.get("passwort")):
            session["admin"] = True
            return redirect(url_for("dashboard"))
        flash("Falsches Passwort.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/favicon.ico")
def favicon():
    logo_path = BASE / "static" / "logo.png"
    if not logo_path.exists():
        abort(404)
    return send_file(logo_path, mimetype="image/png")


@app.route("/")
@app.route("/admin")
@admin_required
def dashboard():
    alle_auftraege = list_auftraege(include_archived=True)
    auftraege = [a for a in alle_auftraege if not a["archiviert"]]
    archivierte_auftraege = [a for a in alle_auftraege if a["archiviert"]]
    mini_calendar = build_mini_monatskalender(
        auftraege,
        request.args.get("monat", ""),
        endpoint="dashboard",
        include_timeline=True,
    )
    mini_calendar.update(
        {
            "section_class": "page-card p-4 p-lg-5 mb-4 mini-calendar mini-calendar-large",
            "heading": f"Werkstatt-Kalender {mini_calendar['title']}",
            "subtitle": f"Heute: {mini_calendar['today_text']} · Alle Fahrzeugtermine im Monatsblick",
            "aria_label": "Werkstatt-Kalender",
        }
    )
    return render_template(
        "dashboard.html",
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        angebotsanfragen=list_angebotsanfragen(),
        autohaeuser=list_autohaeuser(),
        cockpit=dashboard_daten(auftraege),
        ki_status=get_ai_status(),
        database_status=get_database_status(),
        startup_warnings=get_startup_warnings(),
        mini_calendar=mini_calendar,
        statusliste=STATUSLISTE,
        public_base_url=get_public_base_url(),
    )


@app.route("/admin/zugaenge")
@admin_required
def admin_zugaenge():
    return render_template(
        "zugaenge.html",
        autohaeuser=list_autohaeuser(),
        public_base_url=get_public_base_url(),
    )


def extract_import_package_files(archive, names, tmp_path):
    if "auftraege.db" not in names:
        raise ValueError("Datenpaket ungültig: auftraege.db fehlt.")

    imported_db = tmp_path / "auftraege.db"
    with archive.open("auftraege.db") as source, imported_db.open("wb") as target:
        shutil.copyfileobj(source, target)

    probe = sqlite3.connect(imported_db)
    try:
        probe.execute("SELECT COUNT(*) FROM auftraege").fetchone()
        probe.execute("SELECT COUNT(*) FROM autohaeuser").fetchone()
    finally:
        probe.close()

    imported_uploads = tmp_path / "uploads"
    imported_uploads.mkdir(exist_ok=True)
    for name in names:
        if not name.startswith("uploads/") or name.endswith("/"):
            continue
        stored_name = pathlib.Path(name).name
        if not stored_name:
            continue
        with archive.open(name) as source, (imported_uploads / stored_name).open("wb") as target:
            shutil.copyfileobj(source, target)

    return imported_db, imported_uploads


def replace_uploads_from_import(imported_uploads):
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for existing in UPLOAD_DIR.iterdir():
        if existing.is_file():
            move_upload_to_deleted_area(existing, "before-data-import")

    for imported_upload in imported_uploads.iterdir():
        if imported_upload.is_file():
            shutil.copy2(imported_upload, UPLOAD_DIR / imported_upload.name)


def reset_postgres_id_sequences(db):
    for table_name in BACKUP_TABLES:
        try:
            columns = get_table_columns(db, table_name)
        except Exception:
            continue
        if "id" not in columns:
            continue
        db.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table_name}', 'id'),
                COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                (SELECT COUNT(*) FROM {table_name}) > 0
            )
            """
        )


def normalize_import_value(value, column_type):
    column_type = clean_text(column_type).lower()
    if value == "":
        if any(
            marker in column_type
            for marker in (
                "int",
                "real",
                "double",
                "numeric",
                "decimal",
                "bool",
            )
        ):
            return None
    if value is None:
        return None
    if "bool" in column_type and isinstance(value, str):
        normalized = normalize_document_text(value)
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    if "int" in column_type and isinstance(value, str):
        stripped = value.strip()
        if re.fullmatch(r"-?\d+", stripped):
            return int(stripped)
    if any(marker in column_type for marker in ("real", "double", "numeric", "decimal")) and isinstance(value, str):
        stripped = value.strip().replace(",", ".")
        try:
            return float(stripped)
        except ValueError:
            return value
    return value


def import_sqlite_rows_into_current_database(imported_db):
    source = sqlite3.connect(imported_db)
    source.row_factory = sqlite3.Row
    target = get_db()
    try:
        source_tables = {
            row["name"]
            for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table_name in reversed(BACKUP_TABLES):
            target.execute(f"DELETE FROM {table_name}")

        for table_name in BACKUP_TABLES:
            if table_name not in source_tables:
                continue
            target_columns = get_table_columns(target, table_name)
            target_types = get_table_column_types(target, table_name)
            if not target_columns:
                continue
            rows = source.execute(f"SELECT * FROM {table_name}").fetchall()
            for row in rows:
                data = {
                    key: normalize_import_value(row[key], target_types.get(key, ""))
                    for key in row.keys()
                    if key in target_columns
                }
                if not data:
                    continue
                columns = list(data.keys())
                placeholders = ", ".join("?" for _ in columns)
                column_sql = ", ".join(columns)
                target.execute(
                    f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})",
                    tuple(data[column] for column in columns),
                )

        if USE_POSTGRES:
            reset_postgres_id_sequences(target)
        target.commit()
    except Exception:
        if hasattr(target, "rollback"):
            target.rollback()
        raise
    finally:
        source.close()
        target.close()


@app.route("/admin/daten-import", methods=["POST"])
@admin_required
def admin_daten_import():
    paket = request.files.get("datenpaket")
    if not paket or not paket.filename:
        flash("Bitte ein Datenpaket auswählen.", "warning")
        return redirect(url_for("dashboard"))

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = pathlib.Path(tmp_dir)
            archive_path = tmp_path / "datenpaket.zip"
            paket.save(archive_path)

            with zipfile.ZipFile(archive_path) as archive:
                names = set(archive.namelist())
                imported_db, imported_uploads = extract_import_package_files(
                    archive,
                    names,
                    tmp_path,
                )

                create_safety_backup("before-data-import")
                if USE_POSTGRES:
                    import_sqlite_rows_into_current_database(imported_db)
                else:
                    DATA_DIR.mkdir(exist_ok=True)
                    backup_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
                    if DB.exists():
                        shutil.copy2(DB, DATA_DIR / f"auftraege.backup-{backup_suffix}.db")
                    shutil.copy2(imported_db, DB)
                replace_uploads_from_import(imported_uploads)

        flash("Daten wurden importiert. Fahrzeuge und Dateien sind jetzt auf diesem Server verfügbar.", "success")
    except ValueError as exc:
        flash(clean_text(str(exc))[:300], "danger")
    except Exception as exc:
        flash(f"Datenimport fehlgeschlagen: {clean_text(str(exc))[:300]}", "danger")
    return redirect(url_for("dashboard"))


@app.route("/admin/ki/openai-key", methods=["POST"])
@admin_required
def admin_openai_key_speichern():
    api_key = clean_secret_value(request.form.get("openai_api_key"))
    if not api_key:
        flash("Bitte einen OpenAI API-Key eintragen.", "warning")
        return redirect(url_for("dashboard"))
    if not api_key.startswith("sk-"):
        flash("Der OpenAI API-Key sieht ungültig aus. Bitte den Key prüfen.", "warning")
        return redirect(url_for("dashboard"))
    set_app_setting("OPENAI_API_KEY", api_key)
    flash("OpenAI-Key wurde gespeichert. Neue Uploads werden jetzt mit OpenAI analysiert.", "success")
    return redirect(url_for("dashboard"))


@app.route("/admin/ki/openai-test", methods=["POST"])
@admin_required
def admin_openai_test():
    ok, message = test_openai_document_analysis_connection()
    flash(message, "success" if ok else "danger")
    return redirect(url_for("dashboard"))


@app.route("/admin/backup/sofort", methods=["POST"])
@admin_required
def admin_backup_sofort():
    try:
        backup_path = create_backup_package("manual")
        flash(f"Backup erstellt: {backup_path.name}", "success")
    except Exception as exc:
        flash(f"Backup fehlgeschlagen: {clean_text(str(exc))[:300]}", "danger")
    return redirect(url_for("dashboard"))


@app.route("/admin/backup/download", methods=["POST"])
@admin_required
def admin_backup_download():
    try:
        backup_path = create_backup_package("manual-download")
    except Exception as exc:
        flash(f"Backup fehlgeschlagen: {clean_text(str(exc))[:300]}", "danger")
        return redirect(url_for("dashboard"))
    return send_file(
        backup_path,
        download_name=backup_path.name,
        mimetype="application/zip",
        as_attachment=True,
    )


@app.route("/admin/autohaus/neu", methods=["POST"])
@admin_required
def autohaus_neu():
    name = clean_text(request.form.get("name"))
    if not name:
        flash("Bitte einen Autohaus-Namen eintragen.", "warning")
        return redirect(url_for("admin_zugaenge"))

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
    return redirect(url_for("admin_zugaenge"))


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
    return redirect(url_for("admin_zugaenge"))


@app.route("/admin/autohaus/<int:autohaus_id>/lackierauftrag-vorlage.pdf")
@admin_required
def admin_lackierauftrag_vorlage(autohaus_id):
    autohaus = get_autohaus(autohaus_id)
    if not autohaus:
        abort(404)
    return send_lackierauftrag_pdf(autohaus)


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
                "Datei hochgeladen und Analyse sichtbar gemacht.",
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
                add_benachrichtigung(
                    auftrag_id,
                    "Unterlagen neu geprüft",
                    "Die Werkstatt hat vorhandene Unterlagen erneut analysiert und den Auftrag geprüft.",
                )
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
        fertigbilder_result = save_uploads(
            auftrag_id,
            get_allowed_finish_uploads(request.files.getlist("fertigbilder")),
            "intern",
            "fertigbild",
        )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Analyse sichtbar gemacht.",
            )
        else:
            flash("Auftrag aktualisiert.", "success")
        if aktion == "upload_analyze":
            add_benachrichtigung(
                auftrag_id,
                "Neue Unterlage ausgewertet",
                "Die Werkstatt hat eine Unterlage hochgeladen. Erkannte Werte sind zur Prüfung sichtbar.",
            )
        else:
            add_benachrichtigung(
                auftrag_id,
                "Auftrag aktualisiert",
                "Die Werkstatt hat Daten oder Termine an diesem Auftrag geändert.",
            )
        if isinstance(fertigbilder_result, tuple) and fertigbilder_result[0]:
            add_benachrichtigung(
                auftrag_id,
                "Neue Fertigbilder",
                f"Die Werkstatt hat {fertigbilder_result[0]} Fertigbild(er) hochgeladen.",
            )
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    dateien = list_dateien(auftrag_id)
    standard_dateien = dateien_mit_kategorie(dateien, "standard")
    fertigbilder = dateien_mit_kategorie(dateien, "fertigbild")
    chat_nachrichten = list_chat_nachrichten(auftrag_id)
    mark_chat_gelesen(auftrag_id, "admin")
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
        benachrichtigungen=list_benachrichtigungen(auftrag_id),
        chat_nachrichten=chat_nachrichten,
    )


@app.route("/admin/auftrag/<int:auftrag_id>/chat", methods=["POST"])
@admin_required
def admin_chat_nachricht(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    nachricht = clean_text(request.form.get("nachricht"))
    if not nachricht:
        flash("Bitte eine Nachricht eingeben.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
    add_chat_nachricht(auftrag_id, "werkstatt", nachricht)
    add_benachrichtigung(
        auftrag_id,
        "Neue Chat-Nachricht",
        "Die Werkstatt hat im Auftrag geantwortet.",
    )
    flash("Nachricht an das Autohaus gesendet.", "success")
    return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/auftrag/<int:auftrag_id>/dokumente/geprueft", methods=["POST"])
@admin_required
def admin_dokumente_geprueft(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    both_checked = confirm_document_review(auftrag_id, "werkstatt")
    if both_checked:
        flash("Dokumentdaten sind jetzt doppelt geprüft.", "success")
    else:
        flash("Werkstatt-Prüfung gespeichert. Die Autohaus-Prüfung fehlt noch.", "warning")
    return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/auftrag/<int:auftrag_id>/fertigbilder", methods=["POST"])
@admin_required
def admin_fertigbilder_upload(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)

    erlaubte_dateien = get_allowed_finish_uploads(request.files.getlist("fertigbilder"))
    if not erlaubte_dateien:
        flash("Bitte ein Bild oder PDF als Fertigbild auswählen.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))

    gespeichert, _ = save_uploads(auftrag_id, erlaubte_dateien, "intern", "fertigbild")
    if gespeichert:
        db = get_db()
        db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now_str(), auftrag_id))
        db.commit()
        db.close()
        add_benachrichtigung(
            auftrag_id,
            "Neue Fertigbilder",
            f"Die Werkstatt hat {gespeichert} Fertigbild(er) hochgeladen.",
        )
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
    fertig_datum = auftrag["fertig_datum"] or heute if neuer_status >= 4 else auftrag["fertig_datum"]
    abholtermin = auftrag["abholtermin"] or heute if neuer_status >= 5 else auftrag["abholtermin"]

    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET status=?, start_datum=?, fertig_datum=?, abholtermin=?, geaendert_am=?
        WHERE id=?
        """,
        (neuer_status, start_datum, fertig_datum, abholtermin, now_str(), auftrag_id),
    )
    db.execute(
        "INSERT INTO status_log (auftrag_id, status, zeitstempel) VALUES (?, ?, ?)",
        (auftrag_id, neuer_status, now_str()),
    )
    db.commit()
    db.close()
    add_benachrichtigung(
        auftrag_id,
        "Status geändert",
        f"Die Werkstatt hat den Status auf „{STATUSLISTE[neuer_status]['label']}“ gesetzt.",
    )
    flash("Status aktualisiert.", "success")
    ziel = clean_text(request.form.get("next"))
    if ziel.startswith("/"):
        return redirect(ziel)
    return redirect(request.referrer or url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/auftrag/<int:auftrag_id>/rechnung", methods=["GET", "POST"])
@admin_required
def rechnung_schreiben(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)

    if request.method == "POST":
        lexware_kunde_angelegt = 1 if request.form.get("lexware_kunde_angelegt") else 0
        rechnung_geschrieben = 1 if request.form.get("rechnung_geschrieben") else 0
        rechnung_nummer = clean_text(request.form.get("rechnung_nummer"))
        rechnung_status = "geschrieben" if rechnung_geschrieben else "offen"
        if rechnung_geschrieben:
            geschrieben_am = clean_text(auftrag.get("rechnung_geschrieben_am")) or now_str()
        else:
            geschrieben_am = ""

        db = get_db()
        db.execute(
            """
            UPDATE auftraege
            SET lexware_kunde_angelegt=?,
                rechnung_status=?,
                rechnung_nummer=?,
                rechnung_geschrieben_am=?,
                geaendert_am=?
            WHERE id=?
            """,
            (
                lexware_kunde_angelegt,
                rechnung_status,
                rechnung_nummer,
                geschrieben_am,
                now_str(),
                auftrag_id,
            ),
        )
        db.commit()
        db.close()
        flash("Rechnungsstatus gespeichert.", "success")
        return redirect(url_for("rechnung_schreiben", auftrag_id=auftrag_id))

    if auftrag["status"] < 5:
        flash("Rechnung am besten erst nach Zurückgabe schreiben.", "warning")

    return render_template(
        "rechnung.html",
        auftrag=auftrag,
        rechnung=build_lexware_rechnung_context(auftrag),
    )


@app.route("/admin/auftrag/<int:auftrag_id>/rechnung/lexware", methods=["POST"])
@admin_required
def lexware_rechnung_erstellen(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    if auftrag["status"] < 5:
        flash("Bitte den Auftrag zuerst auf Zurückgegeben setzen.", "warning")
        return redirect(url_for("rechnung_schreiben", auftrag_id=auftrag_id))
    if auftrag.get("lexware_invoice_id"):
        flash("Für diesen Auftrag gibt es bereits einen Lexware-Rechnungsentwurf.", "info")
        return redirect(auftrag.get("lexware_invoice_url") or url_for("rechnung_schreiben", auftrag_id=auftrag_id))

    rechnung = build_lexware_rechnung_context(auftrag)
    net_amount = parse_money_amount(request.form.get("netto_betrag"))
    if not net_amount or net_amount <= 0:
        flash("Bitte einen Netto-Rechnungsbetrag eintragen.", "warning")
        return redirect(url_for("rechnung_schreiben", auftrag_id=auftrag_id))

    try:
        result = create_lexware_invoice_draft(auftrag, rechnung, net_amount)
    except Exception as exc:
        flash(str(exc), "danger")
        return redirect(url_for("rechnung_schreiben", auftrag_id=auftrag_id))

    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET lexware_kunde_angelegt=1,
            lexware_contact_id=?,
            lexware_invoice_id=?,
            lexware_invoice_url=?,
            rechnung_status='lexware_entwurf',
            geaendert_am=?
        WHERE id=?
        """,
        (
            result["contact_id"],
            result["invoice_id"],
            result["invoice_url"],
            now_str(),
            auftrag_id,
        ),
    )
    db.commit()
    db.close()

    if result["contact_created"]:
        flash("Kunde in Lexware angelegt und Rechnungsentwurf erstellt.", "success")
    else:
        flash("Bestehenden Lexware-Kunden gefunden und Rechnungsentwurf erstellt.", "success")
    return redirect(result["invoice_url"])


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
    add_benachrichtigung(
        auftrag_id,
        "Terminänderung",
        meldung,
    )
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
    add_benachrichtigung(
        verzoegerung["auftrag_id"],
        "Terminänderung übernommen",
        "Die Werkstatt hat die gemeldete Terminänderung übernommen.",
    )
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
    angebot_notiz = clean_text(request.form.get("werkstatt_angebot_notiz"))
    angebot_war_vorhanden = auftrag.get("angebot_status") == "angebot_abgegeben"
    if not angebot_text and not angebot_preis and not angebot_notiz:
        flash("Bitte Preis, Angebotstext oder Notiz eintragen.", "warning")
        return redirect(url_for("auftrag_detail", auftrag_id=auftrag_id))
    send_workshop_offer(auftrag_id, angebot_text, angebot_preis, angebot_notiz)
    titel = "Werkstatt-Angebot wurde aktualisiert" if angebot_war_vorhanden else "Werkstatt-Angebot liegt vor"
    nachricht = (
        "Die Werkstatt hat das Angebot aktualisiert. Sie können es im Portal erneut prüfen und annehmen."
        if angebot_war_vorhanden
        else "Die Werkstatt hat ein Angebot abgegeben. Sie können es im Portal prüfen und annehmen."
    )
    add_benachrichtigung(
        auftrag_id,
        titel,
        nachricht,
    )
    flash(
        "Angebot aktualisiert und an das Autohaus gesendet."
        if angebot_war_vorhanden
        else "Angebot an das Autohaus gesendet.",
        "success",
    )
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
    path = resolve_datei_path(datei)
    if not path:
        return missing_datei_response(
            datei,
            replace_url=url_for("admin_datei_ersetzen", datei_id=datei_id),
        )
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
    path = resolve_datei_path(datei)
    if not path:
        return missing_datei_response(
            datei,
            replace_url=url_for("admin_datei_ersetzen", datei_id=datei_id),
        )
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=True,
    )


@app.route("/admin/datei/<int:datei_id>/ersetzen", methods=["GET", "POST"])
@admin_required
def admin_datei_ersetzen(datei_id):
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    if request.method == "GET":
        return datei_ersetzen_form_response(datei)
    try:
        replace_datei_content(datei, request.files.get("datei"))
    except ValueError as exc:
        flash(str(exc), "warning")
        return redirect(url_for("admin_datei", datei_id=datei_id))
    except Exception as exc:
        flash(f"Datei konnte nicht ersetzt werden: {clean_text(str(exc))[:300]}", "danger")
        return redirect(url_for("admin_datei", datei_id=datei_id))
    flash("Datei ersetzt und wieder gespeichert.", "success")
    return redirect(url_for("admin_datei", datei_id=datei_id))


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


@app.route("/partner", methods=["GET", "POST"])
def partner_login():
    autohaeuser = list_autohaeuser()
    if request.method == "POST":
        portal_key = clean_text(request.form.get("portal_key"))
        zugangscode = clean_text(request.form.get("zugangscode"))
        autohaus = get_autohaus_by_portal_key(portal_key)
        if autohaus and zugangscode == autohaus["zugangscode"]:
            session["partner_autohaus_id"] = autohaus["id"]
            return redirect(url_for("partner_dashboard_key", portal_key=autohaus["portal_key"]))
        flash("Autohaus oder Passwort/Zugangscode stimmt nicht.", "danger")
    return render_template("partner_index.html", autohaeuser=autohaeuser)


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
    alle_auftraege = list_auftraege(autohaus["id"], include_archived=True)
    auftraege = [a for a in alle_auftraege if not a["archiviert"]]
    archivierte_auftraege = [a for a in alle_auftraege if a["archiviert"]]
    return render_template(
        "partner_dashboard.html",
        autohaus=autohaus,
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        angebotsanfragen=list_angebotsanfragen(autohaus["id"]),
        benachrichtigungen=list_autohaus_benachrichtigungen(autohaus["id"]),
        cockpit=autohaus_dashboard_daten(auftraege),
        mini_calendar=build_mini_monatskalender(
            auftraege,
            request.args.get("monat"),
            endpoint="partner_dashboard",
            route_values={"slug": autohaus["slug"]},
            only_arrival_events=True,
        ),
        statusliste=STATUSLISTE,
    )


@app.route("/partner/<slug>/bonusmodell")
def partner_bonusmodell(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    alle_auftraege = list_auftraege(autohaus["id"], include_archived=True)
    return render_template(
        "partner_bonusmodell.html",
        autohaus=autohaus,
        bonusmodell=build_bonusmodell(alle_auftraege),
    )


@app.route("/partner/<slug>/hinweis/<int:hinweis_id>/entfernen", methods=["POST"])
def partner_hinweis_entfernen(slug, hinweis_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    mark_autohaus_benachrichtigung_gelesen(autohaus["id"], hinweis_id)
    flash("Hinweis entfernt.", "info")
    return redirect(url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/ki/chat", methods=["POST"])
def partner_ki_chat(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return jsonify({"error": "Nicht eingeloggt."}), 401
    payload = request.get_json(silent=True) or {}
    question = clean_text(payload.get("message"))[:900]
    if not question:
        return jsonify({"error": "Bitte eine Frage eingeben."}), 400
    auftrag = get_partner_assistant_auftrag(payload.get("auftrag_id"), autohaus["id"])
    answer, source = ask_partner_assistant(question, autohaus, auftrag)
    return jsonify({"answer": answer, "source": source})


@app.route("/partner/<slug>/ki/chat/loeschen", methods=["POST"])
def partner_ki_chat_loeschen(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return jsonify({"error": "Nicht eingeloggt."}), 401
    return jsonify({"ok": True})


@app.route("/partner/<slug>/lackierauftrag-vorlage.pdf")
def partner_lackierauftrag_vorlage(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    return send_lackierauftrag_pdf(autohaus)


@app.route("/portal/<portal_key>/dashboard")
def partner_dashboard_key(portal_key):
    autohaus, redirect_response = partner_session_required_by_key(portal_key)
    if redirect_response:
        return redirect_response
    return redirect(url_for("partner_dashboard", slug=autohaus["slug"]))


@app.route("/portal/<portal_key>/lackierauftrag-vorlage.pdf")
def partner_lackierauftrag_vorlage_key(portal_key):
    autohaus, redirect_response = partner_session_required_by_key(portal_key)
    if redirect_response:
        return redirect_response
    return redirect(url_for("partner_lackierauftrag_vorlage", slug=autohaus["slug"]))


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
            return render_partner_new_form(autohaus)
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return render_partner_new_form(autohaus)
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
        try:
            upload_result = save_uploads(auftrag_id, erlaubte_dateien, "autohaus", "standard")
        except Exception as exc:
            upload_result = (
                0,
                {"_analysis_error": f"Upload/Analyse konnte nicht abgeschlossen werden: {clean_text(str(exc))[:300]}"},
            )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Analyse sichtbar gemacht.",
            )
        else:
            flash("Fahrzeug angelegt.", "success")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    return render_partner_new_form(autohaus)


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
        try:
            upload_result = save_uploads(auftrag_id, erlaubte_dateien, "autohaus", "standard")
        except Exception as exc:
            upload_result = (
                0,
                {"_analysis_error": f"Upload/Analyse konnte nicht abgeschlossen werden: {clean_text(str(exc))[:300]}"},
            )
        save_uploads(
            auftrag_id,
            get_allowed_finish_uploads(request.files.getlist("fertigbilder")),
            "autohaus",
            "fertigbild",
        )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Datei hochgeladen und Analyse sichtbar gemacht.",
            )
        else:
            flash("Termine aktualisiert.", "success")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    sichtbare_dateien = [d for d in list_dateien(auftrag_id) if d.get("quelle") in {"autohaus", "intern"}]
    standard_dateien = dateien_mit_kategorie(sichtbare_dateien, "standard")
    fertigbilder = dateien_mit_kategorie(sichtbare_dateien, "fertigbild")
    chat_nachrichten = list_chat_nachrichten(auftrag_id)
    mark_chat_gelesen(auftrag_id, "autohaus")
    return render_template(
        "partner_auftrag.html",
        autohaus=autohaus,
        auftrag=auftrag,
        dateien=standard_dateien,
        fertigbilder=fertigbilder,
        dokument_pruefung=list_document_review_items(auftrag_id, auftrag),
        benachrichtigungen=list_benachrichtigungen(auftrag_id),
        reklamationen=list_reklamationen(auftrag_id),
        verzoegerungen=list_verzoegerungen(auftrag_id),
        transport_arten=TRANSPORT_ARTEN,
        statusliste=STATUSLISTE,
        chat_nachrichten=chat_nachrichten,
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


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/dokumente/geprueft", methods=["POST"])
def partner_dokumente_geprueft(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)
    both_checked = confirm_document_review(auftrag_id, "autohaus")
    if both_checked:
        flash("Dokumentdaten sind jetzt doppelt geprüft.", "success")
    else:
        flash("Ihre Prüfung wurde gespeichert. Die Werkstatt prüft die Werte ebenfalls.", "warning")
    target = "partner_angebot_detail" if auftrag.get("angebotsphase") else "partner_auftrag"
    return redirect(url_for(target, slug=slug, auftrag_id=auftrag_id))


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


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/chat", methods=["POST"])
def partner_chat_nachricht(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    nachricht = clean_text(request.form.get("nachricht"))
    if not nachricht:
        flash("Bitte eine Nachricht eingeben.", "warning")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))

    add_chat_nachricht(auftrag_id, "autohaus", nachricht)
    flash("Nachricht an die Werkstatt gesendet.", "success")
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
    path = resolve_datei_path(datei)
    if not path:
        replace_url = ""
        if clean_text(datei.get("quelle")) == "autohaus":
            replace_url = url_for("partner_datei_ersetzen", slug=slug, datei_id=datei_id)
        return missing_datei_response(
            datei,
            url_for("partner_auftrag", slug=slug, auftrag_id=datei["auftrag_id"]),
            replace_url=replace_url,
        )
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
    path = resolve_datei_path(datei)
    if not path:
        replace_url = ""
        if clean_text(datei.get("quelle")) == "autohaus":
            replace_url = url_for("partner_datei_ersetzen", slug=slug, datei_id=datei_id)
        return missing_datei_response(
            datei,
            url_for("partner_auftrag", slug=slug, auftrag_id=datei["auftrag_id"]),
            replace_url=replace_url,
        )
    return send_file(
        path,
        download_name=datei["original_name"],
        mimetype=datei["mime_type"],
        as_attachment=True,
    )


@app.route("/partner/<slug>/datei/<int:datei_id>/ersetzen", methods=["GET", "POST"])
def partner_datei_ersetzen(slug, datei_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    auftrag = get_auftrag(datei["auftrag_id"])
    if (
        not auftrag
        or auftrag.get("autohaus_id") != autohaus["id"]
        or clean_text(datei.get("quelle")) != "autohaus"
    ):
        abort(404)
    back_url = url_for("partner_auftrag", slug=slug, auftrag_id=datei["auftrag_id"])
    if request.method == "GET":
        return datei_ersetzen_form_response(
            datei,
            action_url=url_for("partner_datei_ersetzen", slug=slug, datei_id=datei_id),
            back_url=back_url,
        )
    try:
        replace_datei_content(datei, request.files.get("datei"))
    except ValueError as exc:
        flash(str(exc), "warning")
        return redirect(url_for("partner_datei_ersetzen", slug=slug, datei_id=datei_id))
    except Exception as exc:
        flash(f"Datei konnte nicht ersetzt werden: {clean_text(str(exc))[:300]}", "danger")
        return redirect(url_for("partner_datei_ersetzen", slug=slug, datei_id=datei_id))
    flash("Datei ersetzt und wieder gespeichert.", "success")
    return redirect(url_for("partner_datei", slug=slug, datei_id=datei_id))


@app.route("/partner/<slug>/datei/<int:datei_id>/loeschen", methods=["POST"])
def partner_datei_loeschen(slug, datei_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    ok, auftrag = delete_partner_datei(autohaus["id"], datei_id)
    if ok:
        flash("Datei entfernt. Falls dadurch erkannte Daten falsch waren, bitte oben korrigieren und speichern.", "info")
    else:
        flash("Diese Datei kann hier nicht gelöscht werden.", "warning")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/partner/"):
        return redirect(next_url)
    if auftrag:
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag["id"]))
    return redirect(url_for("partner_dashboard", slug=slug))


init_db()
start_upload_blob_backfill()
start_hourly_backups()


if __name__ == "__main__":
    print("=" * 58)
    print("  Gärtner Autohaus-Terminportal gestartet")
    print("  Admin:   http://localhost:5000/admin")
    print("  Partner: http://localhost:5000/partner")
    for warning in get_startup_warnings():
        print(f"  WARNUNG: {warning}")
    print("=" * 58)
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
