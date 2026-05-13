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
import csv
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from functools import wraps
from html import escape, unescape
import hmac
import hashlib
import imaplib
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
if USE_POSTGRES:
    UPLOAD_DIR = pathlib.Path(os.environ.get("UPLOAD_DIR", "/tmp/kundenstatus-uploads"))
elif os.environ.get("RENDER"):
    DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "/var/data"))
    DB = pathlib.Path(os.environ.get("SQLITE_DB_PATH", str(DATA_DIR / "auftraege.db")))
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
APP_VERSION = "login-unlimited-attempts-v6"
ADMIN_PASS = os.environ.get("ADMIN_PASS") or DEFAULT_ADMIN_PASS
DEFAULT_PUBLIC_BASE_URL = ""
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL") or DEFAULT_PUBLIC_BASE_URL).strip().rstrip("/")
LEXWARE_KUNDEN_URL = (os.environ.get("LEXWARE_KUNDEN_URL") or "").strip()
LEXWARE_RECHNUNGEN_URL = (os.environ.get("LEXWARE_RECHNUNGEN_URL") or "").strip()
LEXWARE_API_KEY = (os.environ.get("LEXWARE_API_KEY") or "").strip()
WERKSTATT_EMAIL_API_TOKEN = (os.environ.get("WERKSTATT_EMAIL_API_TOKEN") or "").strip()
WHATSAPP_ENABLED = env_flag("WHATSAPP_ENABLED", False)
WHATSAPP_ACCESS_TOKEN = (os.environ.get("WHATSAPP_ACCESS_TOKEN") or "").strip()
WHATSAPP_PHONE_NUMBER_ID = (os.environ.get("WHATSAPP_PHONE_NUMBER_ID") or "").strip()
WHATSAPP_VERIFY_TOKEN = (os.environ.get("WHATSAPP_VERIFY_TOKEN") or "").strip()
WHATSAPP_APP_SECRET = (os.environ.get("WHATSAPP_APP_SECRET") or "").strip()
WHATSAPP_WORKSHOP_NUMBERS = (
    os.environ.get("WHATSAPP_WORKSHOP_NUMBERS")
    or os.environ.get("WHATSAPP_WORKSHOP_NUMBER")
    or ""
).strip()
WHATSAPP_GRAPH_VERSION = (os.environ.get("WHATSAPP_GRAPH_VERSION") or "v25.0").strip()
WHATSAPP_REPLY_WINDOW_HOURS = max(1, env_int("WHATSAPP_REPLY_WINDOW_HOURS", 48))
WHATSAPP_NOTIFICATION_TEMPLATE = (os.environ.get("WHATSAPP_NOTIFICATION_TEMPLATE") or "").strip()
WHATSAPP_TEMPLATE_LANGUAGE = (os.environ.get("WHATSAPP_TEMPLATE_LANGUAGE") or "de").strip()
MAIL_IMAP_HOST = (os.environ.get("MAIL_IMAP_HOST") or "").strip()
MAIL_IMAP_PORT = max(1, env_int("MAIL_IMAP_PORT", 993))
MAIL_IMAP_USER = (os.environ.get("MAIL_IMAP_USER") or "").strip()
MAIL_IMAP_PASS = (os.environ.get("MAIL_IMAP_PASS") or "").strip()
MAIL_IMAP_FOLDER = (os.environ.get("MAIL_IMAP_FOLDER") or "INBOX").strip() or "INBOX"
MAIL_IMAP_SSL = env_flag("MAIL_IMAP_SSL", True)
MAIL_IMAP_MARK_SEEN = env_flag("MAIL_IMAP_MARK_SEEN", False)
MAIL_IMAP_ARCHIVE_FOLDER = (os.environ.get("MAIL_IMAP_ARCHIVE_FOLDER") or "").strip()
MAIL_IMAP_SEARCH = (os.environ.get("MAIL_IMAP_SEARCH") or "UNSEEN").strip().upper() or "UNSEEN"
MAIL_IMAP_LIMIT = max(1, min(env_int("MAIL_IMAP_LIMIT", 30), 200))
MAIL_IMAP_TIMEOUT_SECONDS = max(5, env_int("MAIL_IMAP_TIMEOUT_SECONDS", 20))
LEXWARE_API_BASE_URL = (os.environ.get("LEXWARE_API_BASE_URL") or "https://api.lexware.io").strip().rstrip("/")
LEXWARE_APP_BASE_URL = (os.environ.get("LEXWARE_APP_BASE_URL") or "https://app.lexware.de").strip().rstrip("/")
LEXWARE_TAX_RATE = float(os.environ.get("LEXWARE_TAX_RATE") or 19)
LEXWARE_AUTO_SYNC_MINUTES = max(5, env_int("LEXWARE_AUTO_SYNC_MINUTES", 30))
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
OPENAI_CHAT_MODEL = os.environ.get("OPENAI_CHAT_MODEL") or OPENAI_EXTRACTION_MODEL
OPENAI_API_URL = os.environ.get(
    "OPENAI_API_URL", "https://api.openai.com/v1/chat/completions"
)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
MOSBACH_WEATHER_LOCATION = {
    "name": "Mosbach",
    "latitude": 49.3536,
    "longitude": 9.1517,
}
WEATHER_API_TIMEOUT_SECONDS = max(3, env_int("WEATHER_API_TIMEOUT_SECONDS", 8))
WEATHER_CACHE_SECONDS = max(60, env_int("WEATHER_CACHE_SECONDS", 600))
TOPCOLOR_EMAIL = (os.environ.get("TOPCOLOR_EMAIL") or "").strip()
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
INSECURE_SECRET_VALUES = {"", "change-me", DEFAULT_FLASK_SECRET_KEY}
INSECURE_ADMIN_PASSWORDS = {"", "change-me", DEFAULT_ADMIN_PASS}
ALLOW_INSECURE_LOCAL_LOGIN = env_flag("ALLOW_INSECURE_LOCAL_LOGIN", True)
LOGIN_RATE_LIMIT_ENABLED = env_flag("LOGIN_RATE_LIMIT_ENABLED", False)
LOGIN_RATE_LIMIT_MAX = max(3, env_int("LOGIN_RATE_LIMIT_MAX", 8))
LOGIN_RATE_LIMIT_WINDOW_SECONDS = max(60, env_int("LOGIN_RATE_LIMIT_WINDOW_SECONDS", 15 * 60))
LOGIN_RATE_LIMIT_LOCK_SECONDS = max(60, env_int("LOGIN_RATE_LIMIT_LOCK_SECONDS", 10 * 60))
SESSION_IDLE_TIMEOUT_MINUTES = max(5, env_int("SESSION_IDLE_TIMEOUT_MINUTES", 8 * 60))
REMEMBER_LOGIN_DAYS = max(1, env_int("REMEMBER_LOGIN_DAYS", 30))
ADMIN_REMEMBER_COOKIE = "gaertner_admin_remember"
PARTNER_REMEMBER_COOKIE = "gaertner_partner_remember"
LOGIN_ATTEMPTS = {}
LOGIN_ATTEMPTS_LOCK = threading.Lock()
_sqlite_wal_configured = False
_sqlite_wal_lock = threading.Lock()

GOOGLE_ACCESS_TOKEN = {"token": "", "expires_at": 0}
WEATHER_CACHE = {"payload": None, "expires_at": 0}
WEATHER_CACHE_LOCK = threading.Lock()


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
    {"schwelle": 3000.0, "satz": 0.02, "label": "2 %", "schwelle_label": "3.000 €"},
    {"schwelle": 5000.0, "satz": 0.03, "label": "3 %", "schwelle_label": "5.000 €"},
    {"schwelle": 8000.0, "satz": 0.04, "label": "4 %", "schwelle_label": "8.000 €"},
)
BONUSMODELL_AKTIVE_PARTNER = {"autohaus-pfaff"}
RAHMENVERTRAG_TEXT = (
    "Den Rahmenvertrag vereinbaren wir gemeinsam im Gespräch. Dabei legen wir einen fairen "
    "Partnerpreis fest; bei höherem Monatsumsatz kann die Bonusstufe größer werden. So sieht "
    "das Autohaus direkt, wie viel zur nächsten Stufe fehlt und wann der Bonus verrechnet wird."
)
LACKIERAUFTRAG_POSITION_COUNT = 14
LACKIERAUFTRAG_ABRECHNUNG = (
    ("selbstzahler", "Selbstzahler"),
    ("kasko", "Kaskoversicherung"),
    ("haftpflicht", "Haftpflicht gegnerisch"),
    ("sammelrechnung", "Sammelrechnung"),
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

DEFAULT_WERKSTATT_NEWS = [
    {
        "news_key": "betriebsurlaub-2026",
        "titel": "Betriebsurlaub",
        "nachricht": "Betriebsurlaub vom 19.08.2026 bis 04.09.2026.",
        "start_datum": "19.08.2026",
        "end_datum": "04.09.2026",
        "kategorie": "betrieb",
        "pinned": 1,
    },
    {
        "news_key": "ion7-stickstoff-technik",
        "titel": "Lackieren mit erwärmtem Stickstoff",
        "nachricht": (
            "Unsere neue ION-7 nutzt erwärmten Stickstoff für stabilere Bedingungen beim Lackauftrag. "
            "Das unterstützt ein gleichmäßigeres Lackbild, weniger Overspray und planbarere Abläufe."
        ),
        "start_datum": "",
        "end_datum": "",
        "kategorie": "betrieb",
        "pinned": 1,
    },
]

EVENT_FELDER = (
    ("annahme_datum", "Anlieferung", "secondary"),
    ("start_datum", "Start", "primary"),
    ("fertig_datum", "Fertig", "warning"),
    ("abholtermin", "Abholung", "success"),
)

KALENDER_KATEGORIEN = {
    "termin": {"label": "Termin", "farbe": "primary"},
    "urlaub": {"label": "Urlaub", "farbe": "success"},
    "geburtstag": {"label": "Geburtstag", "farbe": "warning"},
    "privat": {"label": "Privat", "farbe": "secondary"},
    "betrieb": {"label": "Betrieb", "farbe": "dark"},
    "feiertag": {"label": "Feiertag", "farbe": "danger"},
    "hinweis": {"label": "Hinweis", "farbe": "info"},
}

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

MONATSNAMEN = {
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
EINKAUF_UPLOAD_EXTENSIONS = IMAGE_EXTENSIONS | {".pdf", ".txt"}
KONTOAUSZUG_UPLOAD_EXTENSIONS = {".csv", ".txt", ".pdf", ".tsv"}

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
    ("instandsetzen", [r"instand", r"ausbeulen", r"richten", r"beule", r"delle", r"dellen", r"eingedrückt", r"eingedellt", r"verformt", r"verbeult"]),
    ("lackieren", [r"lack", r"kratzer", r"schramm", r"verkratzt", r"lackieren", r"lackschaden"]),
    ("ersetzen", [r"ersetzen", r"tauschen", r"gerissen", r"gebrochen", r"kaputt", r"abgebrochen"]),
    ("demontiert/prüfen", [r"demont", r"zerlegt", r"abgebaut", r"bauteil\s+fehlt", r"teil\s+fehlt"]),
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


def generated_flask_secret_path():
    configured_path = (os.environ.get("FLASK_SECRET_KEY_FILE") or "").strip().strip('"').strip("'")
    if configured_path:
        return pathlib.Path(configured_path)
    if os.environ.get("RENDER"):
        return pathlib.Path(os.environ.get("DATA_DIR", "/var/data")) / "flask_secret.key"
    return DATA_DIR / "flask_secret.key"


def read_or_create_generated_flask_secret():
    secret_path = generated_flask_secret_path()
    try:
        if secret_path.exists():
            saved_secret = secret_path.read_text(encoding="utf-8").strip()
            if saved_secret and saved_secret not in INSECURE_SECRET_VALUES:
                return saved_secret
        secret_path.parent.mkdir(parents=True, exist_ok=True)
        generated_secret = secrets.token_urlsafe(64)
        secret_path.write_text(generated_secret, encoding="utf-8")
        return generated_secret
    except Exception as exc:
        print(f"WARNUNG: Flask-Secret konnte nicht dauerhaft gespeichert werden: {exc}")
        return ""


def configured_flask_secret_key():
    configured = (os.environ.get("FLASK_SECRET_KEY") or "").strip().strip('"').strip("'")
    if configured and configured not in INSECURE_SECRET_VALUES:
        return configured, False, False
    generated = read_or_create_generated_flask_secret()
    if generated:
        return generated, False, True
    return secrets.token_urlsafe(64), True, False


app = Flask(__name__)
(
    _configured_secret_key,
    USING_EPHEMERAL_SECRET_KEY,
    USING_GENERATED_FLASK_SECRET_KEY,
) = configured_flask_secret_key()
app.secret_key = _configured_secret_key
app.config.update(
    MAX_CONTENT_LENGTH=MAX_UPLOAD_MB * 1024 * 1024,
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=SESSION_IDLE_TIMEOUT_MINUTES),
    SESSION_REFRESH_EACH_REQUEST=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=env_flag(
        "SESSION_COOKIE_SECURE",
        RUNNING_ON_RENDER,
    ),
)


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
    return {
        "csrf_token": get_csrf_token,
        "csrf_field": csrf_field,
        "admin_postfach_count": admin_postfach_count,
        "admin_einkauf_count": admin_einkauf_count,
        "admin_rechnungen_count": admin_rechnungen_count,
        "admin_email_count": admin_email_count,
        "admin_mitarbeiter_urlaub_count": admin_mitarbeiter_urlaub_count,
        "analysis_loading_news": analysis_loading_news,
    }


def session_is_authenticated():
    return bool(session.get("admin") or session.get("partner_autohaus_id"))


def mark_authenticated_session_active():
    if session_is_authenticated():
        session.permanent = True
        session.modified = True


def remember_login_cookie_name(scope):
    return PARTNER_REMEMBER_COOKIE if scope == "partner" else ADMIN_REMEMBER_COOKIE


def remember_login_token_hash(token):
    token = clean_secret_value(token)
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def remember_login_max_age_seconds():
    return REMEMBER_LOGIN_DAYS * 24 * 60 * 60


def remember_login_expires_at():
    return db_datetime_str(datetime.now() + timedelta(days=REMEMBER_LOGIN_DAYS))


def prune_expired_remember_logins(db):
    db.execute("DELETE FROM login_tokens WHERE expires_at < ?", (db_datetime_str(),))


def create_remember_login_token(scope, autohaus_id=0):
    scope = "partner" if scope == "partner" else "admin"
    token = secrets.token_urlsafe(48)
    db = get_db()
    try:
        prune_expired_remember_logins(db)
        db.execute(
            """
            INSERT INTO login_tokens
              (token_hash, scope, autohaus_id, erstellt_am, zuletzt_genutzt_am, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                remember_login_token_hash(token),
                scope,
                int(autohaus_id or 0),
                db_datetime_str(),
                db_datetime_str(),
                remember_login_expires_at(),
            ),
        )
        db.commit()
    finally:
        db.close()
    return token


def get_remember_login(scope, token):
    token_hash = remember_login_token_hash(token)
    if not token_hash:
        return None
    scope = "partner" if scope == "partner" else "admin"
    db = get_db()
    try:
        row = db.execute(
            """
            SELECT *
            FROM login_tokens
            WHERE token_hash=? AND scope=?
            LIMIT 1
            """,
            (token_hash, scope),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        expires_at = parse_postfach_datetime(item.get("expires_at"))
        if expires_at <= datetime.now():
            db.execute("DELETE FROM login_tokens WHERE token_hash=?", (token_hash,))
            db.commit()
            return None
        db.execute(
            "UPDATE login_tokens SET zuletzt_genutzt_am=? WHERE token_hash=?",
            (db_datetime_str(), token_hash),
        )
        db.commit()
        return item
    finally:
        db.close()


def delete_remember_login_token(token):
    token_hash = remember_login_token_hash(token)
    if not token_hash:
        return
    db = get_db()
    try:
        db.execute("DELETE FROM login_tokens WHERE token_hash=?", (token_hash,))
        db.commit()
    finally:
        db.close()


def set_remember_login_cookie(response, scope, token):
    response.set_cookie(
        remember_login_cookie_name(scope),
        token,
        max_age=remember_login_max_age_seconds(),
        httponly=True,
        secure=app.config.get("SESSION_COOKIE_SECURE", False),
        samesite="Lax",
        path="/",
    )


def clear_remember_login_cookie(response, scope):
    delete_remember_login_token(request.cookies.get(remember_login_cookie_name(scope)))
    response.delete_cookie(
        remember_login_cookie_name(scope),
        path="/",
        secure=app.config.get("SESSION_COOKIE_SECURE", False),
        samesite="Lax",
    )


def remember_authenticated_login(response, scope, autohaus_id=0):
    token = create_remember_login_token(scope, autohaus_id=autohaus_id)
    set_remember_login_cookie(response, scope, token)
    return response


def restore_remember_login_scope(scope):
    token = request.cookies.get(remember_login_cookie_name(scope))
    item = get_remember_login(scope, token)
    if not item:
        return False
    if scope == "partner":
        autohaus_id = int(item.get("autohaus_id") or 0)
        if not get_autohaus(autohaus_id):
            delete_remember_login_token(token)
            return False
        session.permanent = True
        session["partner_autohaus_id"] = autohaus_id
        return True
    session.permanent = True
    session["admin"] = True
    return True


def restore_remember_login_session():
    if session_is_authenticated():
        return
    preferred_scopes = ("partner", "admin") if request.path.startswith("/partner") else ("admin", "partner")
    for scope in preferred_scopes:
        if restore_remember_login_scope(scope):
            session.modified = True
            return


@app.before_request
def refresh_authenticated_session():
    restore_remember_login_session()
    mark_authenticated_session_active()
    return None


@app.before_request
def protect_csrf():
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    if request.path.startswith("/webhooks/whatsapp"):
        return None
    if request.path.startswith("/api/werkstatt/") and werkstatt_api_token_valid():
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
def add_security_headers(response):
    response.headers.setdefault("X-Kundenstatus-Version", APP_VERSION)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=(), payment=()",
    )
    if request.scheme == "https" or PUBLIC_BASE_URL.startswith("https://"):
        response.headers.setdefault(
            "Strict-Transport-Security",
            "max-age=31536000; includeSubDomains",
        )
    response.headers.setdefault(
        "Content-Security-Policy",
        "; ".join(
            [
                "default-src 'self'",
                "base-uri 'self'",
                "form-action 'self'",
                "frame-ancestors 'self'",
                "object-src 'none'",
                "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
                "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com",
                "font-src 'self' https://fonts.gstatic.com data:",
                "img-src 'self' data: blob: https:",
                "connect-src 'self'",
            ]
        ),
    )
    response.headers.setdefault("Cache-Control", "no-store" if session.get("admin") else "private, no-cache")
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
      <p class="mb-0">Der Fehler wurde intern protokolliert. Bitte kurz neu laden oder später erneut versuchen.</p>
    </div>
    <a class="btn btn-outline-dark" href="{{ fallback_url }}">Zurück</a>
  </main>
</body>
        </html>
        """,
        fallback_url=request.referrer or url_for("partner_login"),
    ), 500


def get_startup_warnings():
    warnings = []
    if get_admin_pass() in {"", "change-me", DEFAULT_ADMIN_PASS}:
        warnings.append(
            "ADMIN_PASS ist nicht sicher gesetzt. Bitte in .env.local ein eigenes Passwort eintragen."
        )
    if USING_EPHEMERAL_SECRET_KEY:
        warnings.append(
            "FLASK_SECRET_KEY ist nicht sicher gesetzt. Die App nutzt vorübergehend einen Zufalls-Secret; bitte in .env.local dauerhaft setzen."
        )
    elif USING_GENERATED_FLASK_SECRET_KEY:
        warnings.append(
            "FLASK_SECRET_KEY fehlt. Die App nutzt einen lokal gespeicherten Secret; für Render bitte trotzdem dauerhaft als Umgebungsvariable setzen."
        )
    if RUNNING_ON_RENDER and not USE_POSTGRES:
        warnings.append(
            "Live läuft noch mit SQLite. Für stabilen Betrieb bitte Render PostgreSQL verbinden und DATABASE_URL setzen."
        )
    if WHATSAPP_ENABLED and not whatsapp_bridge_enabled():
        warnings.append(
            "WhatsApp ist aktiviert, aber Token, Phone-Number-ID oder Werkstattnummer fehlen."
        )
    if WHATSAPP_ENABLED and not WHATSAPP_APP_SECRET:
        warnings.append(
            "WHATSAPP_APP_SECRET fehlt. Eingehende WhatsApp-Webhooks werden ohne Signaturprüfung angenommen."
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


def is_local_request():
    if not has_request_context():
        return False
    remote = clean_text(request.remote_addr)
    host = clean_text(request.host).split(":", 1)[0].lower()
    return remote in {"127.0.0.1", "::1", "localhost"} or host in {"localhost", "127.0.0.1", "::1"}


def admin_password_matches(value):
    submitted = clean_secret_value(value)
    configured = get_admin_pass()
    candidates = set()
    if configured and configured not in INSECURE_ADMIN_PASSWORDS:
        candidates.add(configured)
    elif ALLOW_INSECURE_LOCAL_LOGIN and is_local_request():
        candidates.add(DEFAULT_ADMIN_PASS)
    return bool(
        submitted
        and any(hmac.compare_digest(submitted, password) for password in candidates)
    )


def partner_access_code_matches(submitted, autohaus):
    submitted = clean_secret_value(submitted)
    expected = clean_secret_value((autohaus or {}).get("zugangscode"))
    return bool(submitted and expected and hmac.compare_digest(submitted, expected))


def login_attempt_key(scope, identifier=""):
    remote = clean_text(request.remote_addr) if has_request_context() else "unknown"
    identifier = normalize_document_text(identifier)[:120]
    return f"{scope}:{remote}:{identifier}"


def login_rate_limit_status(scope, identifier=""):
    if not LOGIN_RATE_LIMIT_ENABLED:
        return False, 0
    key = login_attempt_key(scope, identifier)
    now_ts = time.time()
    with LOGIN_ATTEMPTS_LOCK:
        state = LOGIN_ATTEMPTS.get(key)
        if not state:
            return False, 0
        if now_ts >= float(state.get("locked_until") or 0):
            if now_ts - float(state.get("first_failed_at") or now_ts) > LOGIN_RATE_LIMIT_WINDOW_SECONDS:
                LOGIN_ATTEMPTS.pop(key, None)
            return False, 0
        return True, int(float(state.get("locked_until") or now_ts) - now_ts) + 1


def record_failed_login(scope, identifier=""):
    if not LOGIN_RATE_LIMIT_ENABLED:
        return 0
    key = login_attempt_key(scope, identifier)
    now_ts = time.time()
    with LOGIN_ATTEMPTS_LOCK:
        state = LOGIN_ATTEMPTS.get(key) or {"count": 0, "first_failed_at": now_ts, "locked_until": 0}
        if now_ts - float(state.get("first_failed_at") or now_ts) > LOGIN_RATE_LIMIT_WINDOW_SECONDS:
            state = {"count": 0, "first_failed_at": now_ts, "locked_until": 0}
        state["count"] = int(state.get("count") or 0) + 1
        if state["count"] >= LOGIN_RATE_LIMIT_MAX:
            state["locked_until"] = now_ts + LOGIN_RATE_LIMIT_LOCK_SECONDS
        LOGIN_ATTEMPTS[key] = state
        return int(state.get("locked_until") or 0)


def clear_login_attempts(scope, identifier=""):
    key = login_attempt_key(scope, identifier)
    with LOGIN_ATTEMPTS_LOCK:
        LOGIN_ATTEMPTS.pop(key, None)


def login_wait_label(seconds):
    seconds = max(1, int(seconds or 1))
    minutes = max(1, (seconds + 59) // 60)
    return f"{minutes} Minute" if minutes == 1 else f"{minutes} Minuten"


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


def db_datetime_str(value=None):
    return (value or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")


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
        "openai_chat_model": clean_text(os.environ.get("OPENAI_CHAT_MODEL"))
        or OPENAI_CHAT_MODEL,
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
        "openai_chat_model": config["openai_chat_model"],
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


def save_ki_assistent_message(autohaus_id, auftrag_id, absender, nachricht):
    text = clean_text(nachricht)
    if not text:
        return
    db = get_db()
    try:
        db.execute(
            """
            INSERT INTO ki_assistent_nachrichten
            (autohaus_id, auftrag_id, absender, nachricht, erstellt_am)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(autohaus_id or 0),
                int(auftrag_id or 0),
                clean_text(absender) or "ki",
                text[:1600],
                now_str(),
            ),
        )
        db.commit()
    finally:
        db.close()


def list_ki_assistent_history(autohaus_id, auftrag_id, limit=8):
    db = get_db()
    rows = db.execute(
        """
        SELECT absender, nachricht
        FROM ki_assistent_nachrichten
        WHERE autohaus_id=? AND auftrag_id=?
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(autohaus_id or 0), int(auftrag_id or 0), int(limit or 8)),
    ).fetchall()
    db.close()
    history = []
    for row in reversed(rows):
        role = "assistant" if row["absender"] == "ki" else "user"
        content = clean_text(row["nachricht"])[:1000]
        if role == "assistant" and is_unhelpful_login_answer(content):
            continue
        history.append({"role": role, "content": content})
    return history


def is_login_or_tone_correction(question):
    text = normalize_document_text(question)
    login_terms = (
        "eingeloggt",
        "eingellog",
        "angemeldet",
        "bin doch drin",
        "ich bin drin",
        "login",
        "anmeld",
    )
    complaint_terms = (
        "frech",
        "unfreundlich",
        "falsch",
        "quatsch",
        "stimmt nicht",
        "du sagst",
        "du sags",
        "sagst immer",
        "support",
    )
    return any(term in text for term in login_terms) or any(term in text for term in complaint_terms)


def is_unhelpful_login_answer(answer):
    text = normalize_document_text(answer)
    bad_phrases = (
        "wenn sie eingeloggt sind",
        "wenn du eingeloggt bist",
        "bitte melden sie sich",
        "melden sie sich im autohaus portal",
        "nicht eingeloggt",
        "technischen support",
        "technischer support",
    )
    return any(phrase in text for phrase in bad_phrases)


def friendly_login_acknowledgement(context="partner"):
    if context == "admin":
        return (
            "Sie sind im internen Portal. Entschuldigung, die vorige Antwort war nicht hilfreich. "
            "Ich helfe direkt hier weiter: Sie können nach eingeplanten Fahrzeugen, offenen E-Mails, "
            "Terminen, Angeboten, Uploads oder einem bestimmten Auftrag fragen."
        )
    return (
        "Sie sind im Autohaus-Portal. Entschuldigung, die vorige Antwort war nicht passend. "
        "Ich helfe direkt hier weiter: Sie können nach Ihren Fahrzeugen, Terminen, Status, "
        "Uploads oder Angebotsanfragen fragen."
    )


def is_vehicle_planning_question(question):
    text = normalize_document_text(question)
    vehicle_terms = (
        "fahrzeug",
        "fahrzeuge",
        "auto",
        "autos",
        "wagen",
        "auftrag",
        "auftraege",
    )
    planning_terms = (
        "eingeplant",
        "geplant",
        "planung",
        "termin",
        "termine",
        "anstehend",
        "heute",
        "morgen",
        "woche",
        "start",
        "fertig",
        "abholung",
    )
    return any(term in text for term in vehicle_terms) and any(term in text for term in planning_terms)


def assistant_planning_event(auftrag):
    events = list((auftrag.get("planung") or {}).get("events") or [])
    if not events:
        return None
    relevant = next((event for event in events if event.get("is_relevant")), None)
    if relevant:
        return relevant
    return sorted(events, key=lambda event: (event.get("datum") or date.max, event.get("priority") or 99))[0]


def assistant_vehicle_line(auftrag, include_autohaus=False):
    fahrzeug = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
    kennzeichen = clean_text(auftrag.get("kennzeichen"))
    autohaus_name = clean_text(auftrag.get("autohaus_name"))
    status = clean_text((auftrag.get("status_meta") or {}).get("label")) or "Status offen"
    event = assistant_planning_event(auftrag)
    termin = "kein Termin gesetzt"
    if event:
        termin = f"{clean_text(event.get('label'))} {clean_text(event.get('datum_text'))}".strip()
        if event.get("is_past") and int(auftrag.get("status") or 1) < 5:
            termin = f"{termin} (überfällig)"
    details = []
    if kennzeichen:
        details.append(kennzeichen)
    if include_autohaus and autohaus_name:
        details.append(autohaus_name)
    detail_text = f" ({', '.join(details)})" if details else ""
    return f"{fahrzeug}{detail_text}: {termin}, Status {status}"


ASSISTANT_SEARCH_STOP_WORDS = {
    "aber",
    "alle",
    "also",
    "auch",
    "auf",
    "auftrag",
    "auftraege",
    "bei",
    "bin",
    "bitte",
    "das",
    "dem",
    "den",
    "der",
    "des",
    "die",
    "dies",
    "dieser",
    "dieses",
    "ein",
    "eine",
    "einen",
    "einer",
    "es",
    "fuer",
    "gibt",
    "haben",
    "ich",
    "ist",
    "jetzt",
    "ki",
    "mal",
    "mir",
    "mit",
    "nach",
    "noch",
    "oder",
    "portal",
    "sehen",
    "sie",
    "sind",
    "status",
    "und",
    "vom",
    "von",
    "warum",
    "was",
    "welche",
    "welcher",
    "welches",
    "wenn",
    "wie",
    "wir",
    "wo",
    "zu",
    "zum",
    "zur",
}


def assistant_search_tokens(question):
    normalized = normalize_document_text(question)
    tokens = []
    for token in re.findall(r"[a-z0-9]+", normalized):
        if len(token) < 3 and not token.isdigit():
            continue
        if token in ASSISTANT_SEARCH_STOP_WORDS:
            continue
        tokens.append(token)
    return list(dict.fromkeys(tokens))[:8]


def assistant_auftrag_search_values(auftrag, include_autohaus=False):
    values = [
        auftrag.get("fahrzeug"),
        auftrag.get("kennzeichen"),
        auftrag.get("auftragsnummer"),
        auftrag.get("fin_nummer"),
        auftrag.get("kunde_name"),
        auftrag.get("analyse_text"),
        auftrag.get("beschreibung"),
        clean_text((auftrag.get("status_meta") or {}).get("label")),
        auftrag.get("angebot_status"),
    ]
    if include_autohaus:
        values.append(auftrag.get("autohaus_name"))
    return [clean_text(value) for value in values if clean_text(value)]


def assistant_relevant_auftraege(question, autohaus_id=None, include_autohaus=False, limit=5):
    tokens = assistant_search_tokens(question)
    query_key = normalize_auftrag_search_key(question)
    if not tokens and len(query_key) < 3:
        return []
    try:
        auftraege = list_auftraege(
            autohaus_id=autohaus_id,
            include_archived=True,
            include_angebote=True,
        )
    except Exception:
        return []

    scored = []
    for index, auftrag in enumerate(auftraege):
        values = assistant_auftrag_search_values(auftrag, include_autohaus=include_autohaus)
        normalized_blob = normalize_document_text(" ".join(values))
        key_blob = " ".join(normalize_auftrag_search_key(value) for value in values)
        score = 0
        if len(query_key) >= 3 and query_key in key_blob:
            score += 14
        for token in tokens:
            token_key = normalize_auftrag_search_key(token)
            if token in normalized_blob:
                score += 2
            if token_key and token_key in key_blob:
                score += 3
        kennzeichen_key = normalize_auftrag_search_key(auftrag.get("kennzeichen"))
        if query_key and kennzeichen_key and query_key == kennzeichen_key:
            score += 20
        if score:
            scored.append((-score, index, auftrag))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in scored[:limit]]


def assistant_auftrag_context_line(auftrag, include_autohaus=False):
    typ = "Angebotsanfrage" if auftrag.get("angebotsphase") else "Auftrag"
    fahrzeug = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
    kopf = f"{typ} #{auftrag.get('id')}: {fahrzeug}"
    kennzeichen = clean_text(auftrag.get("kennzeichen"))
    if kennzeichen:
        kopf += f", Kennzeichen {kennzeichen}"
    if include_autohaus and clean_text(auftrag.get("autohaus_name")):
        kopf += f", Autohaus {clean_text(auftrag.get('autohaus_name'))}"

    details = []
    kunde = clean_text(auftrag.get("kunde_name"))
    if kunde:
        details.append(f"Kunde {kunde}")
    auftragsnummer = clean_text(auftrag.get("auftragsnummer"))
    if auftragsnummer:
        details.append(f"Auftragsnummer {auftragsnummer}")
    status = clean_text((auftrag.get("status_meta") or {}).get("label"))
    if status:
        details.append(f"Status {status}")
    if auftrag.get("angebotsphase"):
        angebot_status = clean_text(auftrag.get("angebot_status")) or "offen"
        details.append(f"Angebotsstatus {angebot_status}")
    termine = []
    for feld, label in (
        ("annahme_datum", "Anlieferung"),
        ("start_datum", "Start"),
        ("fertig_datum", "Fertig"),
        ("abholtermin", "Abholung"),
    ):
        wert = clean_text(auftrag.get(feld))
        if wert:
            termine.append(f"{label} {wert}")
    if termine:
        details.append("Termine " + ", ".join(termine))
    arbeit = clean_text(auftrag.get("analyse_text")) or clean_text(auftrag.get("beschreibung"))
    if arbeit:
        details.append(f"Arbeiten {arbeit[:180]}")
    preis = clean_text(auftrag.get("werkstatt_angebot_preis"))
    if preis and preis not in {"0", "0.0", "0,00"}:
        preis_label = preis if ("€" in preis or "eur" in preis.lower()) else f"{preis} EUR"
        details.append(f"Werkstattangebot netto {preis_label}")
    return (kopf + (" | " + " | ".join(details) if details else ""))[:700]


def assistant_auftrag_context_block(title, auftraege, include_autohaus=False, limit=5):
    lines = [
        assistant_auftrag_context_line(auftrag, include_autohaus=include_autohaus)
        for auftrag in (auftraege or [])[:limit]
    ]
    lines = [line for line in lines if clean_text(line)]
    if not lines:
        return ""
    return title + ":\n" + "\n".join(f"- {line}" for line in lines)


def is_offer_overview_question(question):
    text = normalize_document_text(question)
    offer_terms = ("angebot", "angebote", "anfrage", "anfragen", "preis", "kosten")
    overview_terms = ("offen", "welche", "gibt", "status", "stand", "uebersicht", "wartet", "freigabe")
    return any(term in text for term in offer_terms) and any(term in text for term in overview_terms)


def assistant_offer_lines(autohaus_id=None, include_autohaus=False, limit=6):
    try:
        anfragen = list_angebotsanfragen(autohaus_id=autohaus_id)
    except Exception:
        return []
    return [
        assistant_auftrag_context_line(anfrage, include_autohaus=include_autohaus)
        for anfrage in anfragen[:limit]
    ]


def assistant_offer_answer(autohaus_id=None, include_autohaus=False):
    try:
        anfragen = list_angebotsanfragen(autohaus_id=autohaus_id)
    except Exception:
        anfragen = []
    if not anfragen:
        return "Aktuell sehe ich keine offenen Angebotsanfragen."
    max_lines = 6
    lines = [
        f"- {assistant_auftrag_context_line(anfrage, include_autohaus=include_autohaus)}"
        for anfrage in anfragen[:max_lines]
    ]
    extra_count = max(0, len(anfragen) - max_lines)
    extra_text = f"\nWeitere {extra_count} Angebotsanfrage(n) stehen im Portal." if extra_count else ""
    return "Diese offenen Angebotsanfragen sehe ich:\n" + "\n".join(lines) + extra_text


def is_current_order_question(question):
    text = normalize_document_text(question)
    current_terms = ("dieser", "diesem", "diese", "aktueller", "aktuellen", "hier", "auftrag", "angebot")
    info_terms = ("status", "stand", "naechste", "weiter", "termin", "fertig", "abholung", "preis")
    return any(term in text for term in current_terms) and any(term in text for term in info_terms)


def is_order_lookup_question(question):
    text = normalize_document_text(question)
    if any(term in text for term in ("hochladen", "upload", "datei", "bild", "bilder", "pdf", "dokument")):
        return any(term in text for term in ("status", "stand", "termin", "fertig", "auftrag"))
    lookup_terms = (
        "abholung",
        "annahme",
        "auto",
        "fahrzeug",
        "fertig",
        "fin",
        "kennzeichen",
        "kunde",
        "naechste",
        "stand",
        "start",
        "status",
        "termin",
        "vorgang",
        "wagen",
        "wo",
    )
    tokens = assistant_search_tokens(question)
    return any(term in text for term in lookup_terms) or any(any(ch.isdigit() for ch in token) for token in tokens)


def assistant_order_lookup_answer(question, autohaus_id=None, include_autohaus=False):
    if not is_order_lookup_question(question):
        return ""
    matches = assistant_relevant_auftraege(
        question,
        autohaus_id=autohaus_id,
        include_autohaus=include_autohaus,
        limit=5,
    )
    if not matches:
        return ""
    if len(matches) == 1:
        return "Dazu sehe ich diesen Vorgang:\n- " + assistant_auftrag_context_line(
            matches[0],
            include_autohaus=include_autohaus,
        )
    return "Ich habe diese passenden Vorgänge gefunden:\n" + "\n".join(
        f"- {assistant_auftrag_context_line(auftrag, include_autohaus=include_autohaus)}"
        for auftrag in matches
    )


def assistant_planned_vehicles(autohaus_id=None):
    try:
        auftraege = list_auftraege(autohaus_id=autohaus_id)
    except Exception:
        return []
    planned = []
    for auftrag in auftraege:
        if int(auftrag.get("status") or 1) >= 5:
            continue
        event = assistant_planning_event(auftrag)
        if not event:
            continue
        planned.append((event, auftrag))
    return sorted(
        planned,
        key=lambda item: (
            item[0].get("datum") or date.max,
            item[0].get("priority") or 99,
            clean_text(item[1].get("fahrzeug")).lower(),
            int(item[1].get("id") or 0),
        ),
    )


def assistant_planned_vehicle_lines(autohaus_id=None, include_autohaus=False, limit=6):
    lines = []
    for _, auftrag in assistant_planned_vehicles(autohaus_id=autohaus_id)[:limit]:
        lines.append(assistant_vehicle_line(auftrag, include_autohaus=include_autohaus))
    return lines


def assistant_planned_vehicle_answer(question, autohaus_id=None, include_autohaus=False):
    text = normalize_document_text(question)
    planned = assistant_planned_vehicles(autohaus_id=autohaus_id)
    today = date.today()
    scope = "aktuell"
    if "heute" in text:
        planned = [item for item in planned if item[0].get("datum") == today]
        scope = "heute"
    elif "morgen" in text:
        planned = [item for item in planned if item[0].get("datum") == today + timedelta(days=1)]
        scope = "morgen"
    elif "woche" in text:
        planned = [
            item
            for item in planned
            if item[0].get("datum") and today <= item[0].get("datum") <= today + timedelta(days=7)
        ]
        scope = "in den nächsten 7 Tagen"

    if not planned:
        return f"Es sind {scope} keine aktiven Fahrzeuge mit Termin hinterlegt."

    max_lines = 6
    lines = [
        f"- {assistant_vehicle_line(auftrag, include_autohaus=include_autohaus)}"
        for _, auftrag in planned[:max_lines]
    ]
    extra_count = max(0, len(planned) - max_lines)
    extra_text = f"\nWeitere {extra_count} Fahrzeuge stehen im Lackierportal." if extra_count else ""
    return f"Diese Fahrzeuge sind {scope} eingeplant:\n" + "\n".join(lines) + extra_text


def partner_assistant_context_text(autohaus, auftrag=None, question=""):
    parts = [
        "Portal: Gärtner Karosserie & Lack Autohaus-Portal",
        f"Heutiges Datum: {date.today().strftime(DATE_FMT)}",
        f"Autohaus: {clean_text(autohaus.get('name'))}",
    ]
    if auftrag:
        parts.append(
            assistant_auftrag_context_block(
                "Aktueller sichtbarer Vorgang",
                [auftrag],
                include_autohaus=False,
                limit=1,
            )
        )
    planned_lines = assistant_planned_vehicle_lines(autohaus.get("id"), limit=8)
    if planned_lines:
        parts.append("Aktuell eingeplante Fahrzeuge dieses Autohauses:\n" + "\n".join(planned_lines))
    offer_lines = assistant_offer_lines(autohaus_id=autohaus.get("id"), limit=6)
    if offer_lines:
        parts.append("Offene Angebotsanfragen dieses Autohauses:\n" + "\n".join(offer_lines))
    relevant_auftraege = assistant_relevant_auftraege(
        question,
        autohaus_id=autohaus.get("id"),
        limit=5,
    )
    if auftrag:
        current_id = int(auftrag.get("id") or 0)
        relevant_auftraege = [
            item for item in relevant_auftraege if int(item.get("id") or 0) != current_id
        ]
    relevant_block = assistant_auftrag_context_block(
        "Weitere zur Frage passende Vorgänge",
        relevant_auftraege,
        include_autohaus=False,
        limit=5,
    )
    if relevant_block:
        parts.append(relevant_block)
    return "\n".join(part for part in parts if clean_text(part))


def fallback_partner_assistant_answer(question, autohaus=None, auftrag=None):
    text = normalize_document_text(question)
    if is_login_or_tone_correction(question):
        return friendly_login_acknowledgement("partner")
    if auftrag and is_current_order_question(question):
        return "Zum aktuellen Vorgang sehe ich:\n- " + assistant_auftrag_context_line(auftrag)
    if is_vehicle_planning_question(question) and autohaus:
        return assistant_planned_vehicle_answer(question, autohaus_id=autohaus["id"])
    if is_offer_overview_question(question) and autohaus:
        return assistant_offer_answer(autohaus_id=autohaus["id"])
    if autohaus:
        lookup_answer = assistant_order_lookup_answer(question, autohaus_id=autohaus["id"])
        if lookup_answer:
            return lookup_answer
    if any(word in text for word in ("datei", "bild", "pdf", "hochladen", "dokument", "unterlage")):
        return (
            "Sie können Bilder, PDFs oder Unterlagen direkt im Auftrag hochladen. "
            "Die Werkstatt sieht die Datei danach im internen Auftrag; erkannte Werte bitte kurz prüfen."
        )
    if any(word in text for word in ("angebot", "preis", "kosten", "kostet")):
        return (
            "Für ein Angebot öffnen Sie im Portal 'Angebot anfordern', füllen die Fahrzeugdaten aus "
            "und laden Bilder oder Dokumente dazu hoch. Den verbindlichen Preis gibt danach die Werkstatt frei."
        )
    if any(word in text for word in ("termin", "abholung", "bringen", "holen", "fertig")):
        return (
            "Termine sehen Sie direkt im Auftrag. Wenn sich etwas ändert, können Sie im Auftrag eine Verzögerung "
            "oder Rückfrage an die Werkstatt senden."
        )
    if any(word in text for word in ("status", "fertig", "arbeit", "prozess")):
        return (
            "Der Status im Auftrag zeigt, ob das Fahrzeug angelegt, eingeplant, in Arbeit, fertig oder zurückgegeben ist. "
            "Bei Unklarheiten schreiben Sie am besten direkt im Auftragschat."
        )
    return (
        "Ich helfe Ihnen beim Portal: Fahrzeug anlegen, Angebotsanfrage senden, Bilder oder PDFs hochladen, "
        "Status prüfen und der Werkstatt eine Rückfrage schreiben. Worum geht es genau?"
    )


def ask_partner_assistant(question, autohaus, auftrag=None):
    question = clean_text(question)[:900]
    if not question:
        return "Bitte geben Sie kurz ein, wobei ich helfen soll.", "fallback"
    if is_login_or_tone_correction(question):
        return friendly_login_acknowledgement("partner"), "portal"
    if auftrag and is_current_order_question(question):
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "portal"
    if is_vehicle_planning_question(question):
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "portal"
    if is_offer_overview_question(question):
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "portal"
    config = get_ai_config()
    requests_module = get_requests()
    if not config["openai_ready"] or requests_module is None:
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "fallback"

    auftrag_id = int((auftrag or {}).get("id") or 0)
    messages = [
        {
            "role": "system",
            "content": (
                "Du bist der digitale KI-Helfer von Gärtner Karosserie & Lack im Autohaus-Portal. "
                "Antworte auf Deutsch, warm, freundlich, konkret und in der Sie-Form. Der Nutzer ist bereits "
                "im Portal, sonst könnte er den Chat nicht sehen. Sage niemals 'wenn Sie eingeloggt sind', "
                "niemals 'melden Sie sich an' und verweise nicht auf technischen Support. Entschuldige dich kurz, "
                "wenn eine vorherige Antwort unpassend war, und hilf dann direkt weiter. Hilf bei Fahrzeug anlegen, "
                "Angebotsanfrage, Bilder/PDFs hochladen, Lackierauftrag, Status, Termine und Chat mit der Werkstatt. "
                "Gib keine verbindlichen Preise, Zusagen oder Rechtsberatung. Verweise bei Preis, Terminfreigabe "
                "oder Sonderfällen an die Werkstatt. Maximal fünf Sätze."
                " Nutze fuer Live-Daten nur den Kontext, den das Portal mitgibt. Erfinde keine Fahrzeuge,"
                " Preise oder Termine, wenn sie dort nicht stehen."
            ),
        },
        {
            "role": "system",
            "content": partner_assistant_context_text(autohaus, auftrag, question),
        },
    ]
    messages.extend(list_ki_assistent_history(autohaus["id"], auftrag_id, limit=8))
    messages.append({"role": "user", "content": question})
    payload = {
        "model": config["openai_chat_model"],
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 320,
    }
    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error:
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "fallback"
    if response.status_code >= 400:
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "fallback"
    try:
        data = response.json()
        answer = clean_text(data["choices"][0]["message"]["content"])[:1400]
    except Exception:
        answer = ""
    if not answer:
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "fallback"
    if is_unhelpful_login_answer(answer):
        return fallback_partner_assistant_answer(question, autohaus, auftrag), "portal"
    return answer, "openai"


def admin_assistant_context_text(question=""):
    parts = [
        f"Heutiges Datum: {date.today().strftime(DATE_FMT)}",
        (
            "Kalender: Standardansicht zeigt kommende Termine; Vergangenheit ist separat aufrufbar; "
            "Feiertage bleiben in der Wochenansicht sichtbar."
        ),
    ]
    planned_lines = assistant_planned_vehicle_lines(include_autohaus=True, limit=10)
    parts.append(
        "Aktuell eingeplante Fahrzeuge:\n"
        + ("\n".join(planned_lines) if planned_lines else "Keine aktiven Termine hinterlegt.")
    )
    offer_lines = assistant_offer_lines(include_autohaus=True, limit=8)
    if offer_lines:
        parts.append("Offene Angebotsanfragen:\n" + "\n".join(offer_lines))
    relevant_block = assistant_auftrag_context_block(
        "Zur Frage passende Vorgänge",
        assistant_relevant_auftraege(question, include_autohaus=True, limit=6),
        include_autohaus=True,
        limit=6,
    )
    if relevant_block:
        parts.append(relevant_block)
    return "\n".join(part for part in parts if clean_text(part))


def fallback_admin_assistant_answer(question):
    text = normalize_document_text(question)
    if is_login_or_tone_correction(question):
        return friendly_login_acknowledgement("admin")
    if is_vehicle_planning_question(question):
        return assistant_planned_vehicle_answer(question, include_autohaus=True)
    if is_offer_overview_question(question):
        return assistant_offer_answer(include_autohaus=True)
    lookup_answer = assistant_order_lookup_answer(question, include_autohaus=True)
    if lookup_answer:
        return lookup_answer
    if any(word in text for word in ("kalender", "termin", "geburtstag", "feiertag", "schnelleintrag")):
        return (
            "Kalendereinträge können Sie oben im Betriebs-Cockpit oder im internen Kalender anlegen. "
            "Der Kalender zeigt standardmäßig kommende Termine; die Vergangenheit erreichen Sie über den Umschalter. "
            "Feiertage bleiben in der Wochenansicht sichtbar, werden unten aber nicht als eigene Tageskarten gelistet."
        )
    if any(word in text for word in ("angebot", "preis", "freigabe", "lackierpreis")):
        return (
            "Offene Angebotsanfragen finden Sie im Cockpit und im Admin-Dashboard. "
            "Die Preisvorschläge sind nur Entscheidungshilfe; den verbindlichen Werkstattpreis tragen Sie im Angebot ein."
        )
    if any(word in text for word in ("datei", "bild", "pdf", "ocr", "beleg", "dokument")):
        return (
            "Normale Beleg-Unterlagen können Fahrzeugdaten aktualisieren, Fertigbilder und Reklamationsbilder nicht. "
            "Bei unsicherer Auslese sollten erkannte Felder erst geprüft werden, bevor sie übernommen werden."
        )
    if any(word in text for word in ("partner", "autohaus", "kunde", "portal", "zugang")):
        return (
            "Partner nutzen ihr Autohausportal für Fahrzeuge, Angebotsanfragen, Uploads, Verzögerungen und Reklamationen. "
            "Im Adminbereich sehen Sie dieselben Vorgänge zentral mit Status, Dateien, Postfach und Alarmen."
        )
    return (
        "Ich helfe im Betriebs-Cockpit bei Kalender, Aufträgen, Angeboten, Uploads, Postfach, Einkauf und Partnerportal. "
        "Sagen Sie kurz, wobei Sie gerade hängen."
    )


def ask_admin_assistant(question):
    question = clean_text(question)[:900]
    if not question:
        return "Bitte geben Sie kurz ein, wobei ich helfen soll.", "fallback"
    if is_login_or_tone_correction(question):
        return friendly_login_acknowledgement("admin"), "portal"
    if is_vehicle_planning_question(question):
        return fallback_admin_assistant_answer(question), "portal"
    if is_offer_overview_question(question):
        return fallback_admin_assistant_answer(question), "portal"
    config = get_ai_config()
    requests_module = get_requests()
    if not config["openai_ready"] or requests_module is None:
        return fallback_admin_assistant_answer(question), "fallback"

    messages = [
        {
            "role": "system",
            "content": (
                "Du bist der interne KI-Helfer im Betriebs-Cockpit von Gärtner Karosserie & Lack. "
                "Antworte auf Deutsch, warm, freundlich und praktisch. Hilf bei Kalender, Aufträgen, "
                "Angeboten, Dokumentanalyse, Upload-Kategorien, Autohaus-Portal, Postfach, Reklamationen "
                "und Werkstattorganisation. Der Nutzer ist bereits im internen Adminbereich; sonst könnte er "
                "den Chat nicht sehen. Sage niemals 'wenn Sie eingeloggt sind', niemals 'melden Sie sich an' "
                "und verweise nicht auf technischen Support. Entschuldige dich kurz, wenn eine vorherige Antwort "
                "unpassend war, und hilf dann direkt weiter. Keine verbindlichen Preise oder Rechtsberatung. Maximal fünf Sätze."
                " Nutze fuer Live-Daten nur den Kontext, den das Portal mitgibt. Erfinde keine Fahrzeuge,"
                " Preise oder Termine, wenn sie dort nicht stehen."
            ),
        },
        {
            "role": "system",
            "content": admin_assistant_context_text(question),
        },
    ]
    messages.extend(list_ki_assistent_history(0, 0, limit=8))
    messages.append({"role": "user", "content": question})
    payload = {
        "model": config["openai_chat_model"],
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 320,
    }
    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error or response.status_code >= 400:
        return fallback_admin_assistant_answer(question), "fallback"
    try:
        data = response.json()
        answer = clean_text(data["choices"][0]["message"]["content"])[:1400]
    except Exception:
        answer = ""
    if not answer:
        return fallback_admin_assistant_answer(question), "fallback"
    if is_unhelpful_login_answer(answer):
        return fallback_admin_assistant_answer(question), "portal"
    return answer, "openai"


def clear_ki_assistent_history(autohaus_id, auftrag_id=0):
    db = get_db()
    try:
        db.execute(
            """
            DELETE FROM ki_assistent_nachrichten
            WHERE autohaus_id=? AND auftrag_id=?
            """,
            (int(autohaus_id or 0), int(auftrag_id or 0)),
        )
        db.commit()
    finally:
        db.close()


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
    has_visual_input = bool(visual_inputs)
    user_prompt = "\n".join(
        [
            "Analysiere den folgenden Werkstattbeleg oder das folgende Fahrzeugfoto fuer einen Karosserie- und Lackbetrieb.",
            "Gib NUR JSON gemaess Schema zurueck.",
            "Wichtig:",
            "- OCR kann fehlerhaft sein.",
            "- Wenn ein Bild/PDF sichtbar ist, nutze vorrangig das Originalbild und nicht nur OCR-Text.",
            "- Wenn es ein Schadensfoto ist, beschreibe nur sichtbar erkennbare Karosserieteile und Schaeden.",
            "- Denke bei Fahrzeugfotos auch an zerlegte oder demontierte Fahrzeuge: fehlende Stoßstange, abgebauter Scheinwerfer, offener Radlauf, demontierter Kotflügel, freiliegende Halter oder lose Verkleidung.",
            "- Verwende fuer Schadensfotos kurze deutsche Schlagwoerter: z.B. 'Kotflügel links Kratzer', 'Tür hinten rechts Delle', 'Stoßstange vorne gerissen', 'Scheinwerfer links fehlt', 'Seitenteil rechts demontiert'.",
            "- Wenn links/rechts oder vorne/hinten aus dem Foto nicht sicher erkennbar ist, schreibe den Wert trotzdem vorsichtig und markiere needs_review=true.",
            "- Bei Fotos nicht erfinden: keine FIN, Termine, Preise oder Kennzeichen ausdenken, wenn sie nicht sichtbar sind.",
            "- Bei Fotos offene_bauteile mit kurzen Eintraegen fuellen, je ein sichtbares Teil/Schaden pro Eintrag.",
            "- Bei Fotos kurzanalyse maximal 6 einfache Schlagwoerter/Fragmente, keine langen Saetze.",
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
                    "Du extrahierst strukturierte Felder aus deutschen Werkstattbelegen und Schadensfotos. "
                    "Du kennst typische Karosserieteile auch in zerlegtem Zustand: Stoßstange, Kotflügel, "
                    "Radlauf, Seitenteil, Tür, Schweller, Motorhaube, Heckklappe, Scheinwerfer, Spiegel, "
                    "Halter, Verkleidung und Anbauteile. Arbeite vorsichtig und halluziniere nicht. "
                    "Wenn ein Wert sichtbar, aber unsicher ist, gib ihn trotzdem zur menschlichen Pruefung zurueck."
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
        "max_tokens": 900 if has_visual_input else 700,
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


def structured_analysis_text(fields, filename=""):
    fields = dict(fields or {})
    lines = ["Automatische Bild-/Dokumentanalyse"]
    filename = clean_text(filename)
    if filename:
        lines.append(f"Datei: {filename}")
    if clean_text(fields.get("fahrzeug")):
        lines.append(f"Fahrzeug: {fields['fahrzeug']}")
    if clean_text(fields.get("kennzeichen")):
        lines.append(f"Kennzeichen: {fields['kennzeichen']}")
    if clean_text(fields.get("auftragsnummer")):
        lines.append(f"Auftrag: {fields['auftragsnummer']}")
    if clean_text(fields.get("analyse_text")):
        lines.append(f"Erkannt: {fields['analyse_text']}")
    if clean_text(fields.get("bauteile_override")):
        parts = [
            clean_text(part)
            for part in clean_text(fields.get("bauteile_override")).splitlines()
            if clean_text(part)
        ]
        if parts:
            lines.append("Bauteile/Schaden: " + "; ".join(parts[:8]))
    if clean_text(fields.get("beschreibung")):
        lines.append(f"Beschreibung: {fields['beschreibung']}")
    if clean_text(fields.get("analyse_hinweis")):
        lines.append(f"Prüfung: {fields['analyse_hinweis']}")
    return "\n".join(lines)


def structured_analysis_summary(fields, filename=""):
    fields = dict(fields or {})
    suffix = pathlib.Path(filename or "").suffix.lower()
    prefix = "Bildanalyse" if suffix in IMAGE_EXTENSIONS else "Dokumentanalyse"
    hints = []
    if clean_text(fields.get("analyse_text")):
        hints.append(clean_text(fields["analyse_text"]))
    parts = [
        clean_text(part)
        for part in clean_text(fields.get("bauteile_override")).splitlines()
        if clean_text(part)
    ]
    if parts:
        hints.append("; ".join(parts[:4]))
    if clean_text(fields.get("fahrzeug")):
        hints.append(f"Typ: {fields['fahrzeug']}")
    if clean_text(fields.get("kennzeichen")):
        hints.append(f"Kennzeichen: {fields['kennzeichen']}")
    if not hints and clean_text(fields.get("beschreibung")):
        hints.append(clean_text(fields["beschreibung"])[:220])
    return f"{prefix}: " + " | ".join(dict.fromkeys(hints))[:460] if hints else ""


def append_upload_note_to_analysis(text, note):
    note = clean_text(note)[:500]
    if not note:
        return clean_text(text)
    base = clean_text(text)
    note_text = f"Hinweis vom Autohaus: {note}"
    if not base:
        return note_text
    if note_text in base:
        return base
    return f"{base}\n\n{note_text}"


def append_upload_note_to_summary(summary, note):
    note = clean_text(note)[:220]
    if not note:
        return clean_text(summary)
    note_text = f"Hinweis Autohaus: {note}"
    summary = clean_text(summary)
    if not summary:
        return note_text
    if note_text in summary:
        return summary
    return f"{summary} | {note_text}"[:500]


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
        if not clean_text(bundle.get("text")):
            bundle["text"] = structured_analysis_text(structured, filename)
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
    "whatsapp_nachrichten",
    "benachrichtigungen",
    "rahmenvertrag_anfragen",
    "lackierauftrag_entwuerfe",
    "postfach_ausblendungen",
    "ki_assistent_nachrichten",
    "einkaufsliste",
    "einkauf_belege",
    "einkauf_artikel",
    "werkstatt_news",
    "werkstatt_emails",
    "lexware_rechnungen",
    "kontoauszug_importe",
    "kontoauszug_buchungen",
    "verzoegerungen",
    "reklamationen",
    "kalender_notizen",
    "mitarbeiter",
    "mitarbeiter_urlaub",
)
_backup_lock = threading.Lock()
_backup_thread_started = False
_change_backup_lock = threading.Lock()
_change_backup_pending = False
_change_backup_running = False


def list_table_rows_for_backup(db, table_name):
    try:
        columns = get_table_columns(db, table_name)
    except Exception:
        return []
    if not columns:
        return []
    return [dict(row) for row in db.execute(f"SELECT * FROM {table_name}").fetchall()]


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
                archive.writestr(
                    "manifest.json",
                    json.dumps(
                        {
                            "created_at": now_str(),
                            "reason": reason,
                            "backup_file": backup_path.name,
                            "upload_count": upload_count,
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
    if app.config.get("TESTING"):
        return
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

        CREATE TABLE IF NOT EXISTS login_tokens (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            token_hash          TEXT UNIQUE NOT NULL,
            scope               TEXT NOT NULL,
            autohaus_id         INTEGER DEFAULT 0,
            erstellt_am         TEXT NOT NULL,
            zuletzt_genutzt_am  TEXT DEFAULT '',
            expires_at          TEXT NOT NULL
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
            bonus_netto_betrag REAL DEFAULT 0,
            bonus_preis_aktualisiert_am TEXT DEFAULT '',
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
            notiz          TEXT DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS rahmenvertrag_anfragen (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            autohaus_id INTEGER NOT NULL,
            status      TEXT DEFAULT 'offen',
            nachricht   TEXT DEFAULT '',
            erstellt_am TEXT NOT NULL,
            erledigt_am TEXT DEFAULT '',
            FOREIGN KEY (autohaus_id) REFERENCES autohaeuser(id)
        );

        CREATE TABLE IF NOT EXISTS lackierauftrag_entwuerfe (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            autohaus_id  INTEGER NOT NULL,
            daten_json   TEXT DEFAULT '{}',
            erstellt_am  TEXT NOT NULL,
            geaendert_am TEXT NOT NULL,
            FOREIGN KEY (autohaus_id) REFERENCES autohaeuser(id)
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

        CREATE TABLE IF NOT EXISTS whatsapp_nachrichten (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            auftrag_id          INTEGER NOT NULL,
            chat_id             INTEGER DEFAULT 0,
            richtung            TEXT NOT NULL,
            telefon             TEXT DEFAULT '',
            provider_message_id TEXT DEFAULT '',
            provider_context_id TEXT DEFAULT '',
            nachricht           TEXT DEFAULT '',
            status              TEXT DEFAULT '',
            fehler              TEXT DEFAULT '',
            payload_json        TEXT DEFAULT '',
            erstellt_am         TEXT NOT NULL,
            FOREIGN KEY (auftrag_id) REFERENCES auftraege(id)
        );

        CREATE TABLE IF NOT EXISTS kalender_notizen (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            datum        TEXT NOT NULL,
            titel        TEXT NOT NULL,
            notiz        TEXT DEFAULT '',
            kategorie    TEXT DEFAULT 'termin',
            wiederholung TEXT DEFAULT 'einmalig',
            erstellt_am  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS erinnerungen (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            text        TEXT NOT NULL,
            status      TEXT DEFAULT 'offen',
            erstellt_am TEXT NOT NULL,
            erledigt_am TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS mitarbeiter (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            rolle        TEXT DEFAULT '',
            telefon      TEXT DEFAULT '',
            email        TEXT DEFAULT '',
            adresse      TEXT DEFAULT '',
            geburtsdatum TEXT DEFAULT '',
            geburtsort   TEXT DEFAULT '',
            staatsangehoerigkeit TEXT DEFAULT '',
            eintritt_datum TEXT DEFAULT '',
            austritt_datum TEXT DEFAULT '',
            beschaeftigung TEXT DEFAULT '',
            qualifikation TEXT DEFAULT '',
            arbeitszeit  TEXT DEFAULT '',
            urlaubsanspruch TEXT DEFAULT '',
            ordner_pfad  TEXT DEFAULT '',
            dokumente_notiz TEXT DEFAULT '',
            notiz        TEXT DEFAULT '',
            aktiv        INTEGER DEFAULT 1,
            erstellt_am  TEXT NOT NULL,
            geaendert_am TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mitarbeiter_urlaub (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            mitarbeiter_id INTEGER NOT NULL,
            start_datum    TEXT NOT NULL,
            end_datum      TEXT NOT NULL,
            notiz          TEXT DEFAULT '',
            erstellt_am    TEXT NOT NULL,
            geaendert_am   TEXT NOT NULL,
            FOREIGN KEY (mitarbeiter_id) REFERENCES mitarbeiter(id)
        );

        CREATE TABLE IF NOT EXISTS postfach_ausblendungen (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            empfaenger   TEXT NOT NULL,
            autohaus_id  INTEGER DEFAULT 0,
            item_key     TEXT NOT NULL,
            erstellt_am  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ki_assistent_nachrichten (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            autohaus_id INTEGER NOT NULL,
            auftrag_id  INTEGER DEFAULT 0,
            absender    TEXT NOT NULL,
            nachricht   TEXT NOT NULL,
            erstellt_am TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS einkaufsliste (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            lieferant     TEXT DEFAULT 'Topcolor',
            kategorie     TEXT DEFAULT 'Material',
            artikelnummer TEXT DEFAULT '',
            produkt_name  TEXT DEFAULT '',
            produkt_beschreibung TEXT DEFAULT '',
            produktbild_url TEXT DEFAULT '',
            titel         TEXT DEFAULT '',
            menge         TEXT DEFAULT '',
            ve            TEXT DEFAULT 'Stueck',
            stueckzahl    INTEGER DEFAULT 1,
            gebinde       TEXT DEFAULT '',
            auto_color_preis TEXT DEFAULT '',
            vergleich_preis TEXT DEFAULT '',
            vergleich_lieferant TEXT DEFAULT '',
            lieferzeit    TEXT DEFAULT '',
            preisquelle   TEXT DEFAULT '',
            angebotsstatus TEXT DEFAULT 'offen',
            angebotsnotiz TEXT DEFAULT '',
            notiz         TEXT DEFAULT '',
            qr_text       TEXT DEFAULT '',
            quelle_email_id INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'offen',
            original_name TEXT DEFAULT '',
            stored_name   TEXT DEFAULT '',
            mime_type     TEXT DEFAULT '',
            size          INTEGER DEFAULT 0,
            erstellt_am   TEXT NOT NULL,
            bestellt_am   TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS einkauf_belege (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            beleg_typ        TEXT DEFAULT 'rechnung',
            lieferant        TEXT DEFAULT '',
            original_name    TEXT DEFAULT '',
            stored_name      TEXT DEFAULT '',
            mime_type        TEXT DEFAULT '',
            size             INTEGER DEFAULT 0,
            extrahierter_text TEXT DEFAULT '',
            positionen_count INTEGER DEFAULT 0,
            status           TEXT DEFAULT 'importiert',
            erstellt_am      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS einkauf_artikel (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            lieferant           TEXT DEFAULT '',
            kategorie           TEXT DEFAULT 'Material',
            artikelnummer       TEXT DEFAULT '',
            produkt_name        TEXT DEFAULT '',
            produkt_beschreibung TEXT DEFAULT '',
            produktbild_url     TEXT DEFAULT '',
            ve                  TEXT DEFAULT 'Stueck',
            gebinde             TEXT DEFAULT '',
            letzter_preis       TEXT DEFAULT '',
            letzter_preis_datum TEXT DEFAULT '',
            preisquelle         TEXT DEFAULT '',
            quelle_beleg_id     INTEGER DEFAULT 0,
            quelle_item_id      INTEGER DEFAULT 0,
            nutzungen_count     INTEGER DEFAULT 0,
            status              TEXT DEFAULT 'aktiv',
            erstellt_am         TEXT NOT NULL,
            geaendert_am        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS werkstatt_news (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            news_key    TEXT DEFAULT '',
            titel       TEXT NOT NULL,
            nachricht   TEXT DEFAULT '',
            start_datum TEXT DEFAULT '',
            end_datum   TEXT DEFAULT '',
            kategorie   TEXT DEFAULT 'betrieb',
            sichtbar    INTEGER DEFAULT 1,
            pinned      INTEGER DEFAULT 1,
            erstellt_am TEXT NOT NULL,
            geaendert_am TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS werkstatt_emails (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            quelle         TEXT DEFAULT 'manuell',
            kategorie      TEXT DEFAULT 'allgemein',
            ziel_modul     TEXT DEFAULT '',
            absender_name  TEXT DEFAULT '',
            absender_email TEXT DEFAULT '',
            empfaenger     TEXT DEFAULT '',
            betreff        TEXT DEFAULT '',
            nachricht      TEXT DEFAULT '',
            empfangen_am   TEXT DEFAULT '',
            message_id     TEXT DEFAULT '',
            source_uid     TEXT DEFAULT '',
            raw_hash       TEXT DEFAULT '',
            attachments_count INTEGER DEFAULT 0,
            status         TEXT DEFAULT 'neu',
            autohaus_id    INTEGER DEFAULT 0,
            auftrag_id     INTEGER DEFAULT 0,
            original_payload TEXT DEFAULT '',
            erstellt_am    TEXT NOT NULL,
            geaendert_am   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS lexware_rechnungen (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            voucher_id         TEXT UNIQUE NOT NULL,
            voucher_type       TEXT DEFAULT '',
            richtung           TEXT DEFAULT '',
            status             TEXT DEFAULT 'offen',
            payment_status     TEXT DEFAULT '',
            voucher_status     TEXT DEFAULT '',
            voucher_number     TEXT DEFAULT '',
            contact_name       TEXT DEFAULT '',
            contact_id         TEXT DEFAULT '',
            total_amount       REAL DEFAULT 0,
            open_amount        REAL DEFAULT 0,
            currency           TEXT DEFAULT 'EUR',
            voucher_date       TEXT DEFAULT '',
            due_date           TEXT DEFAULT '',
            paid_date          TEXT DEFAULT '',
            lexware_url        TEXT DEFAULT '',
            source_email_id    INTEGER DEFAULT 0,
            raw_json           TEXT DEFAULT '',
            zuletzt_synced_am  TEXT DEFAULT '',
            erstellt_am        TEXT NOT NULL,
            geaendert_am       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS kontoauszug_importe (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            original_name    TEXT DEFAULT '',
            stored_name      TEXT DEFAULT '',
            mime_type        TEXT DEFAULT '',
            size             INTEGER DEFAULT 0,
            file_hash        TEXT DEFAULT '',
            extrahierter_text TEXT DEFAULT '',
            buchungen_count  INTEGER DEFAULT 0,
            matched_count    INTEGER DEFAULT 0,
            pruefen_count    INTEGER DEFAULT 0,
            erstellt_am      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS kontoauszug_buchungen (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id        INTEGER DEFAULT 0,
            buchung_datum    TEXT DEFAULT '',
            name             TEXT DEFAULT '',
            verwendungszweck TEXT DEFAULT '',
            betrag           REAL DEFAULT 0,
            waehrung         TEXT DEFAULT 'EUR',
            richtung         TEXT DEFAULT '',
            status           TEXT DEFAULT 'offen',
            rechnung_id      INTEGER DEFAULT 0,
            match_score      INTEGER DEFAULT 0,
            hinweis          TEXT DEFAULT '',
            buchung_key      TEXT DEFAULT '',
            erstellt_am      TEXT NOT NULL
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
    ensure_column(db, "auftraege", "bonus_netto_betrag", "REAL DEFAULT 0")
    ensure_column(db, "auftraege", "bonus_preis_aktualisiert_am", "TEXT DEFAULT ''")
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
    ensure_column(db, "dateien", "notiz", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "extrahierter_text", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "extrakt_kurz", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_quelle", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_json", "TEXT DEFAULT ''")
    ensure_column(db, "dateien", "analyse_hinweis", "TEXT DEFAULT ''")
    ensure_column(db, "rahmenvertrag_anfragen", "autohaus_id", "INTEGER DEFAULT 0")
    ensure_column(db, "rahmenvertrag_anfragen", "status", "TEXT DEFAULT 'offen'")
    ensure_column(db, "rahmenvertrag_anfragen", "nachricht", "TEXT DEFAULT ''")
    ensure_column(db, "rahmenvertrag_anfragen", "erstellt_am", "TEXT DEFAULT ''")
    ensure_column(db, "rahmenvertrag_anfragen", "erledigt_am", "TEXT DEFAULT ''")
    ensure_column(db, "lackierauftrag_entwuerfe", "autohaus_id", "INTEGER DEFAULT 0")
    ensure_column(db, "lackierauftrag_entwuerfe", "daten_json", "TEXT DEFAULT '{}'")
    ensure_column(db, "lackierauftrag_entwuerfe", "erstellt_am", "TEXT DEFAULT ''")
    ensure_column(db, "lackierauftrag_entwuerfe", "geaendert_am", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "portal_key", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "portal_titel", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "willkommen_text", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "strasse", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "plz", "TEXT DEFAULT ''")
    ensure_column(db, "autohaeuser", "ort", "TEXT DEFAULT ''")
    ensure_column(db, "login_tokens", "autohaus_id", "INTEGER DEFAULT 0")
    ensure_column(db, "login_tokens", "zuletzt_genutzt_am", "TEXT DEFAULT ''")
    ensure_index(db, "idx_login_tokens_scope", "login_tokens", ("scope", "autohaus_id", "expires_at"))

    ensure_index(db, "idx_auftraege_dashboard", "auftraege", ("archiviert", "angebotsphase", "autohaus_id"))
    ensure_index(db, "idx_auftraege_angebote", "auftraege", ("angebotsphase", "angebot_abgesendet", "autohaus_id"))
    ensure_index(db, "idx_dateien_auftrag", "dateien", ("auftrag_id", "kategorie", "reklamation_id"))
    ensure_index(db, "idx_rahmenvertrag_anfragen_autohaus", "rahmenvertrag_anfragen", ("autohaus_id", "status"))
    ensure_index(db, "idx_lackierauftrag_entwuerfe_autohaus", "lackierauftrag_entwuerfe", ("autohaus_id",))
    ensure_index(db, "idx_status_log_lookup", "status_log", ("status", "zeitstempel", "auftrag_id"))
    ensure_index(db, "idx_verzoegerungen_offen", "verzoegerungen", ("uebernommen", "erstellt_am"))
    ensure_index(db, "idx_reklamationen_offen", "reklamationen", ("bearbeitet", "erstellt_am"))
    ensure_index(db, "idx_kalender_notizen_datum", "kalender_notizen", ("datum", "wiederholung"))
    ensure_index(db, "idx_erinnerungen_status", "erinnerungen", ("status", "erstellt_am"))
    ensure_column(db, "mitarbeiter", "rolle", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "telefon", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "email", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "adresse", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "geburtsdatum", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "geburtsort", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "staatsangehoerigkeit", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "eintritt_datum", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "austritt_datum", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "beschaeftigung", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "qualifikation", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "arbeitszeit", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "urlaubsanspruch", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "ordner_pfad", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "dokumente_notiz", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "notiz", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter", "aktiv", "INTEGER DEFAULT 1")
    ensure_column(db, "mitarbeiter", "geaendert_am", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter_urlaub", "end_datum", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter_urlaub", "notiz", "TEXT DEFAULT ''")
    ensure_column(db, "mitarbeiter_urlaub", "geaendert_am", "TEXT DEFAULT ''")
    ensure_index(db, "idx_mitarbeiter_aktiv", "mitarbeiter", ("aktiv", "name"))
    ensure_index(
        db,
        "idx_mitarbeiter_urlaub_zeitraum",
        "mitarbeiter_urlaub",
        ("mitarbeiter_id", "start_datum", "end_datum"),
    )
    ensure_index(
        db,
        "idx_postfach_ausblendungen_lookup",
        "postfach_ausblendungen",
        ("empfaenger", "autohaus_id", "item_key"),
    )
    ensure_index(
        db,
        "idx_ki_assistent_lookup",
        "ki_assistent_nachrichten",
        ("autohaus_id", "auftrag_id", "id"),
    )
    ensure_column(db, "whatsapp_nachrichten", "chat_id", "INTEGER DEFAULT 0")
    ensure_column(db, "whatsapp_nachrichten", "telefon", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "provider_message_id", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "provider_context_id", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "nachricht", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "status", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "fehler", "TEXT DEFAULT ''")
    ensure_column(db, "whatsapp_nachrichten", "payload_json", "TEXT DEFAULT ''")
    ensure_index(
        db,
        "idx_whatsapp_provider_message",
        "whatsapp_nachrichten",
        ("provider_message_id",),
    )
    ensure_index(
        db,
        "idx_whatsapp_reply_lookup",
        "whatsapp_nachrichten",
        ("telefon", "erstellt_am", "id"),
    )
    ensure_index(
        db,
        "idx_einkaufsliste_status",
        "einkaufsliste",
        ("status", "lieferant", "erstellt_am"),
    )
    ensure_column(db, "einkaufsliste", "qr_text", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "kategorie", "TEXT DEFAULT 'Material'")
    ensure_column(db, "einkaufsliste", "artikelnummer", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "produkt_name", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "produkt_beschreibung", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "produktbild_url", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "ve", "TEXT DEFAULT 'Stueck'")
    ensure_column(db, "einkaufsliste", "stueckzahl", "INTEGER DEFAULT 1")
    ensure_column(db, "einkaufsliste", "gebinde", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "auto_color_preis", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "vergleich_preis", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "vergleich_lieferant", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "lieferzeit", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "preisquelle", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "angebotsstatus", "TEXT DEFAULT 'offen'")
    ensure_column(db, "einkaufsliste", "angebotsnotiz", "TEXT DEFAULT ''")
    ensure_column(db, "einkaufsliste", "quelle_email_id", "INTEGER DEFAULT 0")
    ensure_column(db, "einkaufsliste", "quelle_beleg_id", "INTEGER DEFAULT 0")
    ensure_index(
        db,
        "idx_einkaufsliste_email",
        "einkaufsliste",
        ("quelle_email_id", "status", "angebotsstatus"),
    )
    ensure_index(
        db,
        "idx_einkaufsliste_beleg",
        "einkaufsliste",
        ("quelle_beleg_id", "status", "id"),
    )
    ensure_column(db, "einkauf_belege", "beleg_typ", "TEXT DEFAULT 'rechnung'")
    ensure_column(db, "einkauf_belege", "lieferant", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_belege", "original_name", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_belege", "stored_name", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_belege", "mime_type", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_belege", "size", "INTEGER DEFAULT 0")
    ensure_column(db, "einkauf_belege", "extrahierter_text", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_belege", "positionen_count", "INTEGER DEFAULT 0")
    ensure_column(db, "einkauf_belege", "status", "TEXT DEFAULT 'importiert'")
    ensure_column(db, "einkauf_belege", "erstellt_am", "TEXT DEFAULT ''")
    ensure_index(
        db,
        "idx_einkauf_belege_lieferant",
        "einkauf_belege",
        ("lieferant", "id"),
    )
    ensure_column(db, "einkauf_artikel", "lieferant", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "kategorie", "TEXT DEFAULT 'Material'")
    ensure_column(db, "einkauf_artikel", "artikelnummer", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "produkt_name", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "produkt_beschreibung", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "produktbild_url", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "ve", "TEXT DEFAULT 'Stueck'")
    ensure_column(db, "einkauf_artikel", "gebinde", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "letzter_preis", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "letzter_preis_datum", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "preisquelle", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "quelle_beleg_id", "INTEGER DEFAULT 0")
    ensure_column(db, "einkauf_artikel", "quelle_item_id", "INTEGER DEFAULT 0")
    ensure_column(db, "einkauf_artikel", "nutzungen_count", "INTEGER DEFAULT 0")
    ensure_column(db, "einkauf_artikel", "status", "TEXT DEFAULT 'aktiv'")
    ensure_column(db, "einkauf_artikel", "erstellt_am", "TEXT DEFAULT ''")
    ensure_column(db, "einkauf_artikel", "geaendert_am", "TEXT DEFAULT ''")
    ensure_index(
        db,
        "idx_einkauf_artikel_lookup",
        "einkauf_artikel",
        ("lieferant", "artikelnummer", "produkt_name"),
    )
    ensure_index(
        db,
        "idx_werkstatt_news_sichtbar",
        "werkstatt_news",
        ("sichtbar", "pinned", "start_datum"),
    )
    ensure_index(db, "idx_werkstatt_news_key", "werkstatt_news", ("news_key",))
    ensure_index(
        db,
        "idx_werkstatt_emails_status",
        "werkstatt_emails",
        ("status", "empfangen_am", "id"),
    )
    ensure_column(db, "werkstatt_emails", "kategorie", "TEXT DEFAULT 'allgemein'")
    ensure_column(db, "werkstatt_emails", "ziel_modul", "TEXT DEFAULT ''")
    ensure_column(db, "werkstatt_emails", "message_id", "TEXT DEFAULT ''")
    ensure_column(db, "werkstatt_emails", "source_uid", "TEXT DEFAULT ''")
    ensure_column(db, "werkstatt_emails", "raw_hash", "TEXT DEFAULT ''")
    ensure_column(db, "werkstatt_emails", "attachments_count", "INTEGER DEFAULT 0")
    ensure_index(db, "idx_werkstatt_emails_message_id", "werkstatt_emails", ("message_id",))
    ensure_index(db, "idx_werkstatt_emails_source_uid", "werkstatt_emails", ("source_uid",))
    ensure_index(db, "idx_werkstatt_emails_raw_hash", "werkstatt_emails", ("raw_hash",))
    ensure_column(db, "lexware_rechnungen", "voucher_id", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "voucher_type", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "richtung", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "status", "TEXT DEFAULT 'offen'")
    ensure_column(db, "lexware_rechnungen", "payment_status", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "voucher_status", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "voucher_number", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "contact_name", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "contact_id", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "total_amount", "REAL DEFAULT 0")
    ensure_column(db, "lexware_rechnungen", "open_amount", "REAL DEFAULT 0")
    ensure_column(db, "lexware_rechnungen", "currency", "TEXT DEFAULT 'EUR'")
    ensure_column(db, "lexware_rechnungen", "voucher_date", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "due_date", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "paid_date", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "lexware_url", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "source_email_id", "INTEGER DEFAULT 0")
    ensure_column(db, "lexware_rechnungen", "raw_json", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "zuletzt_synced_am", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "erstellt_am", "TEXT DEFAULT ''")
    ensure_column(db, "lexware_rechnungen", "geaendert_am", "TEXT DEFAULT ''")
    ensure_index(db, "idx_lexware_rechnungen_voucher", "lexware_rechnungen", ("voucher_id",))
    ensure_index(
        db,
        "idx_lexware_rechnungen_status",
        "lexware_rechnungen",
        ("status", "richtung", "due_date"),
    )
    ensure_column(db, "kontoauszug_importe", "original_name", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_importe", "stored_name", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_importe", "mime_type", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_importe", "size", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_importe", "file_hash", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_importe", "extrahierter_text", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_importe", "buchungen_count", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_importe", "matched_count", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_importe", "pruefen_count", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_importe", "erstellt_am", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "import_id", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_buchungen", "buchung_datum", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "name", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "verwendungszweck", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "betrag", "REAL DEFAULT 0")
    ensure_column(db, "kontoauszug_buchungen", "waehrung", "TEXT DEFAULT 'EUR'")
    ensure_column(db, "kontoauszug_buchungen", "richtung", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "status", "TEXT DEFAULT 'offen'")
    ensure_column(db, "kontoauszug_buchungen", "rechnung_id", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_buchungen", "match_score", "INTEGER DEFAULT 0")
    ensure_column(db, "kontoauszug_buchungen", "hinweis", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "buchung_key", "TEXT DEFAULT ''")
    ensure_column(db, "kontoauszug_buchungen", "erstellt_am", "TEXT DEFAULT ''")
    ensure_index(db, "idx_kontoauszug_importe_hash", "kontoauszug_importe", ("file_hash",))
    ensure_index(db, "idx_kontoauszug_buchungen_key", "kontoauszug_buchungen", ("buchung_key",))
    ensure_index(
        db,
        "idx_kontoauszug_buchungen_status",
        "kontoauszug_buchungen",
        ("status", "buchung_datum", "id"),
    )

    old_betriebsurlaub_key = "betriebsurlaub-2026-" + "thai" + "land"
    db.execute(
        """
        UPDATE werkstatt_news
        SET news_key='betriebsurlaub-2026'
        WHERE news_key=?
        """,
        (old_betriebsurlaub_key,),
    )

    seed_default_autohaeuser(db)
    seed_default_auftraege(db)
    seed_default_werkstatt_news(db)
    merge_duplicate_einkauf_artikel(db)

    rows = db.execute("SELECT id, portal_key FROM autohaeuser").fetchall()
    for row in rows:
        if not clean_text(row["portal_key"]):
            db.execute(
                "UPDATE autohaeuser SET portal_key=? WHERE id=?",
                (uuid.uuid4().hex[:16], row["id"]),
            )

    db.execute(
        """
        UPDATE einkaufsliste
        SET produkt_beschreibung=qr_text
        WHERE COALESCE(produkt_beschreibung, '')=''
          AND COALESCE(qr_text, '')<>''
        """
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


def seed_default_werkstatt_news(db):
    now = now_str()
    for item in DEFAULT_WERKSTATT_NEWS:
        news_key = clean_text(item.get("news_key"))
        existing = db.execute(
            "SELECT id FROM werkstatt_news WHERE news_key=?",
            (news_key,),
        ).fetchone()
        values = (
            news_key,
            clean_text(item.get("titel")),
            clean_text(item.get("nachricht")),
            format_date(item.get("start_datum")),
            format_date(item.get("end_datum")),
            clean_text(item.get("kategorie")) or "betrieb",
            1,
            int(item.get("pinned") or 0),
            now,
        )
        if existing:
            db.execute(
                """
                UPDATE werkstatt_news
                SET news_key=?, titel=?, nachricht=?, start_datum=?, end_datum=?,
                    kategorie=?, sichtbar=?, pinned=?, geaendert_am=?
                WHERE id=?
                """,
                values + (existing["id"],),
            )
            continue
        db.execute(
            """
            INSERT INTO werkstatt_news
              (news_key, titel, nachricht, start_datum, end_datum, kategorie,
               sichtbar, pinned, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values + (now,),
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


def lackierauftrag_default_data(autohaus):
    telefon_email = " / ".join(
        part
        for part in (clean_text(autohaus.get("telefon")), clean_text(autohaus.get("email")))
        if part
    )
    data = {
        "firma": clean_text(autohaus.get("name")),
        "ansprechpartner": clean_text(autohaus.get("kontakt_name")),
        "kontakt": telefon_email,
        "auftrags_datum": date.today().strftime(DATE_FMT),
        "fertig_bis": "",
        "typ": "",
        "kennzeichen": "",
        "fg_nr": "",
        "farb_nr": "",
        "km_stand": "",
        "abrechnung": "",
        "versicherung_schaden_nr": "",
        "versicherungsnehmer": "",
        "kontrolle_datum": "",
        "unterschrift_auftragnehmer": "",
    }
    for index in range(1, LACKIERAUFTRAG_POSITION_COUNT + 1):
        data[f"position_{index}_teil"] = ""
        data[f"position_{index}_seite"] = ""
        data[f"position_{index}_bemerkung"] = ""
        data[f"position_{index}_status"] = ""
    return data


def normalize_lackierauftrag_data(raw_data, autohaus=None):
    data = lackierauftrag_default_data(autohaus or {})
    for key in list(data.keys()):
        data[key] = clean_text((raw_data or {}).get(key, data[key]))[:600]
    abrechnung = clean_text((raw_data or {}).get("abrechnung"))
    allowed_abrechnung = {key for key, _ in LACKIERAUFTRAG_ABRECHNUNG}
    data["abrechnung"] = abrechnung if abrechnung in allowed_abrechnung else ""
    for date_key in ("auftrags_datum", "fertig_bis", "kontrolle_datum"):
        data[date_key] = format_date(data.get(date_key)) or clean_text(data.get(date_key))
    return data


def parse_lackierauftrag_form(form, autohaus):
    return normalize_lackierauftrag_data(form, autohaus)


def get_lackierauftrag_entwurf(autohaus):
    defaults = lackierauftrag_default_data(autohaus)
    db = get_db()
    row = db.execute(
        """
        SELECT *
        FROM lackierauftrag_entwuerfe
        WHERE autohaus_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (autohaus["id"],),
    ).fetchone()
    db.close()
    if not row:
        return {
            "id": 0,
            "daten": defaults,
            "erstellt_am": "",
            "geaendert_am": "",
        }
    try:
        saved = json.loads(row["daten_json"] or "{}")
    except Exception:
        saved = {}
    merged = dict(defaults)
    merged.update(saved if isinstance(saved, dict) else {})
    return {
        "id": row["id"],
        "daten": normalize_lackierauftrag_data(merged, autohaus),
        "erstellt_am": clean_text(row["erstellt_am"]),
        "geaendert_am": clean_text(row["geaendert_am"]),
    }


def save_lackierauftrag_entwurf(autohaus, data):
    normalized = normalize_lackierauftrag_data(data, autohaus)
    payload = json.dumps(normalized, ensure_ascii=False)
    now = now_str()
    db = get_db()
    existing = db.execute(
        "SELECT id FROM lackierauftrag_entwuerfe WHERE autohaus_id=? ORDER BY id DESC LIMIT 1",
        (autohaus["id"],),
    ).fetchone()
    if existing:
        db.execute(
            """
            UPDATE lackierauftrag_entwuerfe
            SET daten_json=?, geaendert_am=?
            WHERE id=?
            """,
            (payload, now, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO lackierauftrag_entwuerfe
            (autohaus_id, daten_json, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?)
            """,
            (autohaus["id"], payload, now, now),
        )
    db.commit()
    db.close()
    return normalized


def lackierauftrag_filename(autohaus):
    slug = clean_text(autohaus.get("slug")) or slugify(autohaus.get("name")) or "autohaus"
    return f"Lackierauftrag_{slug}.pdf"


def make_lackierauftrag_pdf(autohaus, data=None):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    def text(value):
        return escape(clean_text(value))

    form_data = normalize_lackierauftrag_data(data or {}, autohaus)

    def value(key):
        return Paragraph(text(form_data.get(key)), bold8)

    def checkbox(key, label_text):
        mark = "x" if form_data.get("abrechnung") == key else " "
        return Paragraph(f"[{mark}] {escape(label_text)}", label)

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
        [Paragraph("Firma:", label), value("firma")],
        [Paragraph("Ansprechpartner:", label), value("ansprechpartner")],
        [Paragraph("Tel. / E-Mail:", label), value("kontakt")],
        [Paragraph("Auftrags-Datum:", label), value("auftrags_datum")],
        [Paragraph("Fertig bis spätestens:", label), value("fertig_bis")],
    ]
    right_rows = [
        [Paragraph("Fahrzeug", bold9), ""],
        [Paragraph("Typ:", label), value("typ")],
        [Paragraph("Amtl. Kennzeichen:", label), value("kennzeichen")],
        [Paragraph("Fg.-Nr.:", label), value("fg_nr")],
        [Paragraph("Farb-Nr.:", label), value("farb_nr")],
        [Paragraph("km-Stand:", label), value("km_stand")],
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
                    checkbox("selbstzahler", "Selbstzahler"),
                    checkbox("kasko", "Kaskoversicherung"),
                    checkbox("haftpflicht", "Haftpflicht gegnerisch"),
                    checkbox("sammelrechnung", "Sammelrechnung"),
                ],
                [
                    Paragraph("Versicherung / Schaden-Nr.:", label),
                    value("versicherung_schaden_nr"),
                    Paragraph("Vers.-Nehmer:", label),
                    value("versicherungsnehmer"),
                ],
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

    positions = [[Paragraph("Karosserieteil", bold9), Paragraph("Seite", bold9), Paragraph("Bemerkung / Arbeitsumfang", bold9), Paragraph("I.O. / n.I.O.", bold9)]]
    for index in range(1, LACKIERAUFTRAG_POSITION_COUNT + 1):
        positions.append(
            [
                Paragraph(text(form_data.get(f"position_{index}_teil")), label),
                Paragraph(text(form_data.get(f"position_{index}_seite")), label),
                Paragraph(text(form_data.get(f"position_{index}_bemerkung")), label),
                Paragraph(text(form_data.get(f"position_{index}_status")), label),
            ]
        )
    story.append(
        Table(
            positions,
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
                        [[Paragraph("Datum:", label), value("kontrolle_datum"), Paragraph("Unterschrift Auftragnehmer:", label), value("unterschrift_auftragnehmer")]],
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


def send_lackierauftrag_pdf(autohaus, data=None):
    return send_file(
        make_lackierauftrag_pdf(autohaus, data),
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
    bonus_amount = positive_money_amount(auftrag.get("bonus_netto_betrag"))
    auftrag["bonus_netto_betrag"] = bonus_amount or 0.0
    auftrag["bonus_netto_betrag_label"] = format_bonus_money(bonus_amount) if bonus_amount else ""
    auftrag["bonus_preis_aktualisiert_am"] = clean_text(auftrag.get("bonus_preis_aktualisiert_am"))
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
    auftrag["planung"] = build_auftrag_planung(auftrag)
    auftrag["kunden_bauteile"] = build_customer_part_summaries(auftrag)
    auftrag["preisvorschlag"] = build_price_suggestion(auftrag)
    bonus_basis, bonus_quelle, bonus_quelle_key = auftrag_bonus_preis(auftrag)
    auftrag["bonus_basis_betrag"] = bonus_basis or 0.0
    auftrag["bonus_basis_label"] = format_bonus_money(bonus_basis) if bonus_basis else ""
    auftrag["bonus_basis_quelle"] = bonus_quelle
    auftrag["bonus_basis_quelle_key"] = bonus_quelle_key
    auftrag["bonus_preis_fehlt"] = int(auftrag.get("status") or 1) >= 5 and not bonus_basis
    bonus_reference_date = auftrag.get("abholtermin_obj") or date.today()
    auftrag["bonus_verrechnung_label"] = bonus_verrechnung_label(bonus_reference_date, bonus_basis)
    auftrag["bonus_verrechnung_detail"] = bonus_verrechnung_detail(bonus_reference_date, bonus_basis)
    return auftrag


def auftrag_planung_label(auftrag, feld):
    if feld == "annahme_datum":
        return "Holen" if auftrag.get("transport_art") == "hol_und_bring" else "Kommt"
    if feld == "abholtermin":
        return "Rückbringen" if auftrag.get("transport_art") == "hol_und_bring" else "Muss weg"
    if feld == "start_datum":
        return "Start"
    if feld == "fertig_datum":
        return "Fertig"
    return feld


def build_auftrag_planung(auftrag, reference_date=None):
    heute = reference_date or date.today()
    felder = (
        ("annahme_datum", "secondary", 1),
        ("start_datum", "primary", 2),
        ("fertig_datum", "warning", 3),
        ("abholtermin", "success", 4),
    )
    events = []
    for feld, farbe, priority in felder:
        event_date = auftrag.get(f"{feld}_obj")
        if not event_date:
            continue
        label = auftrag_planung_label(auftrag, feld)
        events.append(
            {
                "feld": feld,
                "label": label,
                "datum": event_date,
                "datum_text": auftrag.get(feld),
                "farbe": farbe,
                "priority": priority,
                "is_today": event_date == heute,
                "is_past": event_date < heute,
                "is_relevant": False,
            }
        )

    date_warning = any(
        event["datum"].year < heute.year - 1 or event["datum"].year > heute.year + 2
        for event in events
    )

    status = int(auftrag.get("status") or 1)
    if status >= 5:
        relevante_felder = ("abholtermin", "fertig_datum", "start_datum", "annahme_datum")
    elif status >= 4:
        relevante_felder = ("abholtermin", "fertig_datum")
    elif status >= 3:
        relevante_felder = ("fertig_datum", "abholtermin")
    elif status >= 2:
        relevante_felder = ("start_datum", "fertig_datum", "abholtermin")
    else:
        relevante_felder = ("annahme_datum", "start_datum", "fertig_datum", "abholtermin")

    candidates = [event for event in events if event["feld"] in relevante_felder]
    future_or_today = [event for event in candidates if event["datum"] >= heute]
    if future_or_today:
        relevant = sorted(future_or_today, key=lambda event: (event["datum"], event["priority"]))[0]
    elif candidates:
        relevant = sorted(candidates, key=lambda event: (event["datum"], event["priority"]), reverse=True)[0]
    else:
        relevant = sorted(events, key=lambda event: (event["datum"], event["priority"]))[0] if events else None

    if relevant:
        for event in events:
            event["is_relevant"] = event["feld"] == relevant["feld"]
        delta = (relevant["datum"] - heute).days
        if status >= 5:
            sort_group = 3
            badge_farbe = "dark"
            hinweis = "Zurückgegeben"
        elif date_warning:
            sort_group = 4
            badge_farbe = "danger"
            hinweis = "Termin prüfen: Datum wirkt ungewöhnlich"
        elif delta < 0:
            sort_group = 0
            badge_farbe = "danger"
            hinweis = f"{relevant['label']} überfällig seit {relevant['datum_text']}"
        elif delta == 0:
            sort_group = 1
            badge_farbe = "success"
            hinweis = f"{relevant['label']} heute"
        else:
            sort_group = 2
            badge_farbe = "primary"
            hinweis = f"{relevant['label']} morgen" if delta == 1 else f"{relevant['label']} in {delta} Tagen"
        sort_days = abs(delta)
        sort_date_ord = relevant["datum"].toordinal()
        badge = "Zurückgegeben" if status >= 5 else ("Termin prüfen" if date_warning else relevant["label"])
        date_text = relevant["datum_text"]
    else:
        sort_group = 5
        badge_farbe = "secondary"
        hinweis = "Kein Termin gesetzt"
        sort_days = 9999
        sort_date_ord = date.max.toordinal()
        badge = "Ohne Termin"
        date_text = ""

    group_keys = {
        0: "overdue",
        1: "today",
        2: "upcoming",
        3: "done",
        4: "date-warning",
        5: "empty",
    }
    is_week_relevant = bool(
        relevant
        and not date_warning
        and status < 5
        and relevant["datum"] >= heute
        and (relevant["datum"] - heute).days <= 7
    )
    return {
        "events": events,
        "badge": badge,
        "badge_farbe": badge_farbe,
        "date_text": date_text,
        "hinweis": hinweis,
        "sort_group": sort_group,
        "sort_days": sort_days,
        "sort_date_ord": sort_date_ord,
        "group_key": group_keys.get(sort_group, "empty"),
        "has_date_warning": date_warning,
        "is_week_relevant": is_week_relevant,
    }


def auftrag_planung_sort_key(auftrag):
    planung = auftrag.get("planung") or {}
    return (
        int(planung.get("sort_group", 4)),
        int(planung.get("sort_days", 9999)),
        int(planung.get("sort_date_ord", date.max.toordinal())),
        int(auftrag.get("status") or 1),
        clean_text(auftrag.get("autohaus_name")).lower(),
        clean_text(auftrag.get("kennzeichen")).lower(),
        int(auftrag.get("id") or 0),
    )


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


def build_lexware_rechnung_context(auftrag, invoice_net_amount=None):
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
    bonus_betrag, _, _ = auftrag_bonus_preis(auftrag)
    bonus_preis_label = format_bonus_money(bonus_betrag) if bonus_betrag else ""
    if angebot_text or angebot_preis or bonus_preis_label:
        positionen.append(
            {
                "bezeichnung": angebot_text or "Karosserie- und Lackierarbeiten",
                "preis": angebot_preis or bonus_preis_label,
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
    bonus_invoice = build_invoice_bonus_transparency(auftrag, invoice_net_amount)
    return {
        "kunde": kunde,
        "positionen": positionen,
        "belegtext": belegtext,
        "lexware_beschreibung": build_invoice_lexware_description(belegtext, bonus_invoice.get("text")),
        "bonus_text": clean_text(bonus_invoice.get("text")),
        "bonus_summary": clean_text(bonus_invoice.get("summary")),
        "bonus_remark": clean_text(bonus_invoice.get("remark")),
        "bonusmodell": bonus_invoice.get("bonusmodell") or {},
        "netto_betrag": bonus_betrag,
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


def positive_money_amount(value):
    if isinstance(value, (int, float)):
        amount = round(float(value), 2)
    else:
        amount = parse_money_amount(value)
    if amount is None or amount <= 0:
        return None
    return amount


def format_bonus_money(value):
    amount = positive_money_amount(value)
    if amount is None:
        amount = 0.0
    formatted = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{formatted} €"


def is_bonusmodell_aktiv(autohaus):
    slug = clean_text((autohaus or {}).get("slug"))
    return slug in BONUSMODELL_AKTIVE_PARTNER


def rahmenvertrag_context(autohaus):
    aktiv = is_bonusmodell_aktiv(autohaus)
    return {
        "aktiv": aktiv,
        "status_label": "Rahmenvertrag aktiv" if aktiv else "Rahmenvertrag offen",
        "button_label": "Bonusmodell" if aktiv else "Rahmenvertrag vereinbaren",
        "erklaerung": RAHMENVERTRAG_TEXT,
    }


def bonus_verrechnung_date(reference_date=None):
    _, monat_ende = month_bounds(reference_date or date.today())
    return monat_ende


def parse_bonusmonat(value):
    text = clean_text(value)
    match = re.fullmatch(r"(\d{4})-(\d{1,2})", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), 1)
    except ValueError:
        return None


def bonusmonat_label(reference_date):
    reference = reference_date or date.today()
    return f"{MONATSNAMEN.get(reference.month, reference.month)} {reference.year}"


def bonus_verrechnung_label(reference_date=None, bonus_amount=0):
    if not positive_money_amount(bonus_amount):
        return "unter Schwelle"
    return f"ab {bonus_verrechnung_date(reference_date).strftime(DATE_FMT)}"


def bonus_verrechnung_detail(reference_date=None, bonus_amount=0):
    if not positive_money_amount(bonus_amount):
        return "Sobald eine Bonusstufe erreicht ist, wird die Verrechnung hier mit Datum angezeigt."
    return (
        f"Der Bonus wird {bonus_verrechnung_label(reference_date, bonus_amount)} "
        "auf der nächsten Rechnung verrechnet."
    )


def auftrag_bonus_preis(auftrag):
    bonus_betrag = positive_money_amount((auftrag or {}).get("bonus_netto_betrag"))
    if bonus_betrag:
        return bonus_betrag, "Rechnungs-/Bonuspreis", "final"
    angebot_betrag = positive_money_amount((auftrag or {}).get("werkstatt_angebot_preis"))
    if angebot_betrag:
        return angebot_betrag, "Werkstatt-Angebot", "angebot"
    return None, "", ""


def month_bounds(reference_date=None):
    reference = reference_date or date.today()
    start = date(reference.year, reference.month, 1)
    if reference.month == 12:
        ende = date(reference.year + 1, 1, 1)
    else:
        ende = date(reference.year, reference.month + 1, 1)
    return start, ende


def parse_bonus_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    cleaned = clean_text(value)
    if not cleaned:
        return None
    return parse_date(cleaned) or parse_date(cleaned[:10])


def auftrag_bonus_rechnungsdatum(auftrag):
    for feld in ("rechnung_geschrieben_am", "bonus_preis_aktualisiert_am"):
        parsed = parse_bonus_date((auftrag or {}).get(feld))
        if parsed:
            return parsed
    return None


def auftrag_bonus_reference_date(auftrag):
    return (
        auftrag_bonus_rechnungsdatum(auftrag)
        or (auftrag or {}).get("abholtermin_obj")
        or parse_bonus_date((auftrag or {}).get("abholtermin"))
    )


def build_bonus_month_options(auftraege, selected_reference=None):
    months = {}
    selected_start, _ = month_bounds(selected_reference or date.today())

    def add_month(reference_date):
        if not reference_date:
            return
        month_start, _ = month_bounds(reference_date)
        key = month_start.strftime("%Y-%m")
        months[key] = {
            "key": key,
            "label": bonusmonat_label(month_start),
            "active": key == selected_start.strftime("%Y-%m"),
        }

    add_month(date.today())
    add_month(selected_reference)
    for auftrag in auftraege or []:
        add_month(auftrag_bonus_reference_date(auftrag))

    return [
        months[key]
        for key in sorted(months.keys(), reverse=True)
    ]


def prepare_archivierte_bonus_auftraege(auftraege):
    archivierte = []
    for auftrag in auftraege or []:
        if not (auftrag or {}).get("archiviert"):
            continue
        reference = (
            auftrag_bonus_reference_date(auftrag)
            or auftrag.get("fertig_datum_obj")
            or auftrag.get("start_datum_obj")
            or auftrag.get("annahme_datum_obj")
        )
        if reference:
            auftrag["bonus_monat_key"] = reference.strftime("%Y-%m")
            auftrag["bonus_monat_label"] = bonusmonat_label(reference)
            auftrag["letzter_termin_obj"] = reference
            auftrag["letzter_termin_label"] = reference.strftime(DATE_FMT)
        else:
            auftrag["bonus_monat_key"] = ""
            auftrag["bonus_monat_label"] = ""
            auftrag["letzter_termin_obj"] = date.min
            auftrag["letzter_termin_label"] = ""
        archivierte.append(auftrag)
    archivierte.sort(
        key=lambda item: (
            item.get("letzter_termin_obj") or date.min,
            int(item.get("id") or 0),
        ),
        reverse=True,
    )
    return archivierte


def build_bonusmodell(auftraege, reference_date=None):
    reference = reference_date or date.today()
    monat_start, monat_ende = month_bounds(reference)
    monatsumsatz = 0.0
    bonus_auftraege = []
    offene_preise = []

    for auftrag in auftraege or []:
        try:
            status = int((auftrag or {}).get("status") or 0)
        except Exception:
            status = 0
        rechnungsbetrag = positive_money_amount((auftrag or {}).get("bonus_netto_betrag"))
        if status < 5 and not rechnungsbetrag:
            continue
        bonus_datum = auftrag_bonus_reference_date(auftrag)
        if not bonus_datum or bonus_datum < monat_start or bonus_datum >= monat_ende:
            continue

        betrag, quelle, quelle_key = auftrag_bonus_preis(auftrag)
        datum_label = "Rechnung vom" if rechnungsbetrag else "Zurückgegeben am"
        item = {
            "auftrag": auftrag,
            "datum": bonus_datum,
            "datum_text": bonus_datum.strftime(DATE_FMT),
            "datum_label": datum_label,
            "betrag": betrag,
            "betrag_label": format_bonus_money(betrag) if betrag else "",
            "quelle": quelle,
            "quelle_key": quelle_key,
            "preis_fehlt": betrag is None,
            "rechnung_zaehlt": bool(rechnungsbetrag),
        }
        if betrag is None:
            offene_preise.append(item)
        else:
            monatsumsatz += betrag
        bonus_auftraege.append(item)

    bonus_auftraege.sort(
        key=lambda item: (
            item["datum"],
            int((item.get("auftrag") or {}).get("id") or 0),
        ),
        reverse=True,
    )

    aktive_stufe = {"schwelle": 0.0, "satz": 0.0, "label": "0 %", "schwelle_label": "0 €"}
    for stufe in BONUSMODELL_STUFEN:
        if monatsumsatz >= stufe["schwelle"]:
            aktive_stufe = stufe
    naechste_stufe = next((stufe for stufe in BONUSMODELL_STUFEN if monatsumsatz < stufe["schwelle"]), None)
    bonus_netto = round(monatsumsatz * aktive_stufe["satz"], 2)
    stufen = []
    for stufe in BONUSMODELL_STUFEN:
        item = dict(stufe)
        item["erreicht"] = monatsumsatz >= stufe["schwelle"]
        item["aktiv"] = stufe["schwelle"] == aktive_stufe.get("schwelle") and monatsumsatz >= stufe["schwelle"]
        item["satz_prozent"] = int(round(stufe["satz"] * 100))
        stufen.append(item)

    return {
        "monat_key": monat_start.strftime("%Y-%m"),
        "monat_label": bonusmonat_label(reference),
        "umsatz_netto": round(monatsumsatz, 2),
        "umsatz_netto_label": format_bonus_money(monatsumsatz),
        "bonus_satz": aktive_stufe["satz"],
        "bonus_satz_label": aktive_stufe["label"],
        "bonus_netto": bonus_netto,
        "bonus_netto_label": format_bonus_money(bonus_netto),
        "verrechnung_label": bonus_verrechnung_label(reference, bonus_netto),
        "verrechnung_detail": bonus_verrechnung_detail(reference, bonus_netto),
        "stufen": stufen,
        "aktive_stufe": aktive_stufe,
        "naechste_stufe": naechste_stufe,
        "bis_naechste_stufe_label": (
            format_bonus_money(max(0.0, naechste_stufe["schwelle"] - monatsumsatz))
            if naechste_stufe
            else ""
        ),
        "auftraege": bonus_auftraege,
        "offene_preise": offene_preise,
        "offene_preise_count": len(offene_preise),
        "gezaehlte_auftraege_count": sum(1 for item in bonus_auftraege if not item["preis_fehlt"]),
        "gezaehlte_rechnungen_count": sum(1 for item in bonus_auftraege if item.get("rechnung_zaehlt")),
    }


def build_invoice_bonusmodell(auftrag, invoice_net_amount=None):
    autohaus_id = int((auftrag or {}).get("autohaus_id") or 0)
    if autohaus_id <= 0:
        return {}
    reference = auftrag_bonus_reference_date(auftrag) or date.today()
    effective_amount = positive_money_amount(invoice_net_amount) or positive_money_amount(
        (auftrag or {}).get("bonus_netto_betrag")
    )
    current_id = int((auftrag or {}).get("id") or 0)
    auftraege = list_auftraege(autohaus_id, include_archived=True)
    found = False
    for item in auftraege:
        if int(item.get("id") or 0) != current_id:
            continue
        found = True
        if effective_amount:
            item["bonus_netto_betrag"] = effective_amount
            item["bonus_netto_betrag_label"] = format_bonus_money(effective_amount)
        break
    if current_id and not found:
        item = dict(auftrag)
        if effective_amount:
            item["bonus_netto_betrag"] = effective_amount
        auftraege.append(row_to_auftrag(item) if "status_meta" not in item else item)
    return build_bonusmodell(auftraege, reference)


def build_invoice_bonus_transparency(auftrag, invoice_net_amount=None):
    if not int((auftrag or {}).get("autohaus_id") or 0):
        return {}
    bonusmodell = build_invoice_bonusmodell(auftrag, invoice_net_amount)
    if not bonusmodell:
        return {}
    current_amount = positive_money_amount(invoice_net_amount) or positive_money_amount(
        (auftrag or {}).get("bonus_netto_betrag")
    )
    current_label = format_bonus_money(current_amount) if current_amount else "noch nicht gesetzt"
    next_tier = bonusmodell.get("naechste_stufe")
    lines = [
        f"Partnerbonus transparent - Monatsübersicht {bonusmodell['monat_label']} (netto)",
        f"Aktueller Monatsumsatz: {bonusmodell['umsatz_netto_label']}",
        f"Diese Rechnung zählt mit: {current_label}",
        f"Aktuelle Bonusstufe: {bonusmodell['bonus_satz_label']}",
        f"Aktueller Bonus netto: {bonusmodell['bonus_netto_label']}",
        f"Verrechnung: {bonusmodell['verrechnung_label']} auf der Folgerechnung",
    ]
    if next_tier:
        lines.append(
            f"Nächste Stufe: noch {bonusmodell['bis_naechste_stufe_label']} bis {next_tier['label']} Bonus"
        )
    else:
        lines.append("Nächste Stufe: höchste Bonusstufe erreicht")
    lines.append(
        "Berechnung: Gezählt werden gespeicherte Werkstatt-Rechnungen dieses Monats; zurückgegebene Aufträge ohne Rechnung bleiben als Preisprüfung sichtbar."
    )
    counted_items = [item for item in bonusmodell.get("auftraege", []) if not item.get("preis_fehlt")]
    if counted_items:
        lines.append("Berücksichtigte Aufträge:")
        for item in counted_items[:8]:
            a = item.get("auftrag") or {}
            title_parts = [
                f"Auftrag #{a.get('id')}",
                clean_text(a.get("fahrzeug")) or "Fahrzeug",
                clean_text(a.get("kennzeichen")),
            ]
            title = " · ".join(part for part in title_parts if clean_text(part))
            lines.append(f"- {item['datum_label']} {item['datum_text']} · {title} · {item['betrag_label']}")
        if len(counted_items) > 8:
            lines.append(f"- weitere {len(counted_items) - 8} Auftrag/Aufträge im Monatslauf")
    if bonusmodell.get("offene_preise_count"):
        lines.append(f"Hinweis: {bonusmodell['offene_preise_count']} zurückgegebene Auftrag/Aufträge haben noch keinen Netto-Preis.")
    summary = (
        f"Monatsumsatz {bonusmodell['umsatz_netto_label']}, "
        f"Stufe {bonusmodell['bonus_satz_label']}, "
        f"Bonus {bonusmodell['bonus_netto_label']}, "
        f"Verrechnung {bonusmodell['verrechnung_label']}."
    )
    return {
        "text": "\n".join(lines),
        "summary": summary,
        "remark": f"Partnerbonus: {summary} Details stehen in der Rechnungsposition.",
        "bonusmodell": bonusmodell,
    }


def build_invoice_lexware_description(belegtext, bonus_text=""):
    parts = [clean_text(belegtext)]
    if clean_text(bonus_text):
        parts.append(clean_text(bonus_text))
    return "\n\n".join(part for part in parts if part)[:3500]


def extract_money_amounts_from_line(line):
    text = clean_text(line)
    if not text:
        return []
    matches = re.findall(
        r"(?<![A-Za-z0-9])([0-9]{1,3}(?:[.\s'’][0-9]{3})*(?:[,.][0-9]{2})|[0-9]{4,}(?:[,.][0-9]{2})|[0-9]{1,6},[0-9]{2})(?:\s*(?:€|eur|euro))?",
        text,
        flags=re.IGNORECASE,
    )
    amounts = []
    for raw in matches:
        amount = positive_money_amount(raw)
        if amount is not None:
            amounts.append(amount)
    return amounts


def extract_invoice_total_amount(text):
    cleaned = clean_text(text)
    if not cleaned:
        return None, ""
    lines = [compact_whitespace(line) for line in cleaned.splitlines() if clean_text(line)]
    priority_groups = (
        (
            "Netto-Gesamtbetrag",
            (
                "gesamt netto",
                "summe netto",
                "netto gesamt",
                "nettobetrag",
                "netto-betrag",
                "betrag netto",
                "rechnungsbetrag netto",
                "rechnung netto",
            ),
        ),
        (
            "Rechnungs-Gesamtbetrag",
            (
                "gesamtbetrag",
                "rechnungsbetrag",
                "rechnungssumme",
                "endbetrag",
                "zahlbetrag",
                "zu zahlen",
                "gesamt summe",
                "gesamtsumme",
                "gesamt",
            ),
        ),
        ("Brutto-Gesamtbetrag", ("bruttobetrag", "brutto gesamt", "gesamt brutto")),
    )
    for label, markers in priority_groups:
        for line in reversed(lines):
            normalized = normalize_document_text(line)
            if not any(marker in normalized for marker in markers):
                continue
            amounts = extract_money_amounts_from_line(line)
            if amounts:
                return amounts[-1], label

    all_amounts = []
    for line in lines:
        normalized = normalize_document_text(line)
        if any(skip in normalized for skip in ("mwst", "ust", "steuer", "rabatt", "skonto")):
            continue
        all_amounts.extend(extract_money_amounts_from_line(line))
    if all_amounts:
        return max(all_amounts), "größter erkannter Rechnungsbetrag"
    return None, ""


def normalize_invoice_reference(value):
    return re.sub(r"[^a-z0-9]", "", normalize_document_text(value))


def extract_invoice_number(text):
    source = clean_text(text)
    if not source:
        return ""
    patterns = (
        r"\brechnungs(?:nummer|nr\.?)\s*[:#.\-]?\s*([A-Z0-9][A-Z0-9/_\-]{2,30})",
        r"\brechnung\s*(?:nr\.?|nummer)\s*[:#.\-]?\s*([A-Z0-9][A-Z0-9/_\-]{2,30})",
        r"\bbeleg(?:nummer|nr\.?)\s*[:#.\-]?\s*([A-Z0-9][A-Z0-9/_\-]{2,30})",
        r"\binvoice\s*(?:no\.?|number|nr\.?)\s*[:#.\-]?\s*([A-Z0-9][A-Z0-9/_\-]{2,30})",
        r"\b(RG[-_/ ]?[A-Z0-9][A-Z0-9/_\-]{2,30})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, source, flags=re.IGNORECASE)
        if not match:
            continue
        number = clean_text(match.group(1)).strip(" .:-_/")
        if number and not re.search(r"[,.]\d{2}$", number):
            return number[:40]
    return ""


def extract_license_plate_tokens(text):
    source = clean_text(text).upper()
    tokens = set()
    for match in re.finditer(r"\b([A-ZÄÖÜ]{1,3})[-\s]+([A-Z]{1,2})\s*[-\s]*(\d{1,4})\b", source):
        token = re.sub(r"[^A-Z0-9ÄÖÜ]", "", "".join(match.groups()))
        if 4 <= len(token) <= 9:
            tokens.add(normalize_invoice_reference(token))
    return tokens


def extract_vin_tokens(text):
    source = clean_text(text).upper()
    tokens = set()
    for match in re.finditer(r"\b[A-HJ-NPR-Z0-9]{17}\b", source):
        token = match.group(0)
        if re.search(r"[A-Z]", token) and re.search(r"\d", token):
            tokens.add(normalize_invoice_reference(token))
    return tokens


def text_tokens_match_reference(blob, reference, minimum_matches=1):
    words = [
        word
        for word in re.findall(r"[a-z0-9]{3,}", normalize_document_text(reference))
        if word not in {"und", "der", "die", "das", "von", "mit", "autohaus", "gmbh"}
    ]
    if not words:
        return False
    matches = sum(1 for word in dict.fromkeys(words) if word in blob)
    return matches >= min(len(set(words)), max(1, minimum_matches))


def validate_werkstatt_rechnung_upload(auftrag, invoice_text, original_name, amount=None, invoice_number=""):
    combined = "\n".join(part for part in [original_name, invoice_text] if clean_text(part))
    normalized = normalize_document_text(combined)
    filename_normalized = normalize_document_text(original_name)
    compact = normalize_invoice_reference(combined)
    amount = positive_money_amount(amount)
    invoice_number = clean_text(invoice_number)
    invoice_word = bool(re.search(r"\brechnung\b|rechnungs", normalized))
    filename_invoice = bool(re.search(r"\brechnung\b|rechnungs|invoice|\brg[-\s_/]?\d", filename_normalized))
    invoice_marker = any(
        marker in normalized
        for marker in (
            "zahlbetrag",
            "gesamtbetrag",
            "rechnungssumme",
            "nettobetrag",
            "bruttobetrag",
            "umsatzsteuer",
            "mwst",
            "ust",
            "invoice",
        )
    )
    looks_like_invoice = bool(invoice_number or (invoice_word and (amount or invoice_marker)) or filename_invoice)

    result = {
        "valid": False,
        "needs_review": False,
        "looks_like_invoice": looks_like_invoice,
        "evidence": [],
        "warnings": [],
        "blockers": [],
    }
    if not looks_like_invoice:
        result["blockers"].append("Die Datei sieht nicht eindeutig wie eine Rechnung aus.")
        return result

    expected_plate = normalize_invoice_reference(auftrag.get("kennzeichen"))
    found_plates = extract_license_plate_tokens(combined)
    if expected_plate and found_plates and expected_plate not in found_plates:
        result["blockers"].append("Das erkannte Kennzeichen passt nicht zu diesem Auftrag.")
        return result

    expected_vin = normalize_invoice_reference(auftrag.get("fin_nummer"))
    found_vins = extract_vin_tokens(combined)
    if expected_vin and found_vins and expected_vin not in found_vins:
        result["blockers"].append("Die erkannte FIN passt nicht zu diesem Auftrag.")
        return result

    if expected_plate and expected_plate in compact:
        result["evidence"].append("Kennzeichen passt")
    if expected_vin and expected_vin in compact:
        result["evidence"].append("FIN passt")

    auftragsnummer = normalize_invoice_reference(auftrag.get("auftragsnummer"))
    if auftragsnummer and len(auftragsnummer) >= 4 and auftragsnummer in compact:
        result["evidence"].append("Auftragsnummer passt")

    auftrag_id = clean_text(auftrag.get("id"))
    if auftrag_id and any(
        token in compact
        for token in (
            f"auftrag{auftrag_id}",
            f"auftragsnummer{auftrag_id}",
            f"auftragid{auftrag_id}",
        )
    ):
        result["evidence"].append("Portal-Auftrag passt")

    if text_tokens_match_reference(compact, auftrag.get("fahrzeug"), minimum_matches=2):
        result["evidence"].append("Fahrzeug passt")
    if text_tokens_match_reference(compact, auftrag.get("autohaus_name"), minimum_matches=1):
        result["evidence"].append("Autohaus passt")
    if text_tokens_match_reference(compact, auftrag.get("kunde_name"), minimum_matches=1):
        result["evidence"].append("Kundenreferenz passt")

    if not result["evidence"]:
        result["needs_review"] = True
        result["warnings"].append(
            "Rechnung erkannt, aber keine eindeutige Auftragszuordnung gefunden. Bitte Original gegen den Auftrag prüfen."
        )
    else:
        result["valid"] = True

    if invoice_number:
        result["evidence"].append(f"Rechnungsnummer {invoice_number}")
    if amount:
        result["evidence"].append(f"Betrag {format_bonus_money(amount)} erkannt")
    return result


def save_werkstatt_rechnung_upload(auftrag_id, files):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        return {"saved": 0, "verified": 0, "amount": None, "invoice_number": "", "error": "Auftrag nicht gefunden."}

    saved = 0
    verified = 0
    needs_review = 0
    best_amount = None
    best_invoice_number = ""
    errors = []
    warnings = []
    db = get_db()
    timestamp = now_str()
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        db.close()
        return {"saved": 0, "verified": 0, "amount": None, "invoice_number": "", "error": f"Upload-Speicher ist nicht erreichbar: {clean_text(str(exc))[:300]}"}

    for file in files or []:
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
            errors.append(f"{original_name} konnte nicht gespeichert werden: {clean_text(str(exc))[:300]}")
            continue

        mime_type = file.mimetype or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
        bundle = build_document_analysis_bundle_safe(target, original_name) if suffix in ANALYSIS_EXTENSIONS else {}
        analysis_text = clean_text(bundle.get("text"))
        structured_text = structured_analysis_text(bundle.get("structured"), original_name) if bundle.get("structured") else ""
        invoice_text = "\n".join(part for part in [analysis_text, structured_text, original_name] if clean_text(part))
        amount, amount_source = extract_invoice_total_amount(invoice_text)
        invoice_number = extract_invoice_number(invoice_text)
        validation = validate_werkstatt_rechnung_upload(
            auftrag,
            invoice_text,
            original_name,
            amount=amount,
            invoice_number=invoice_number,
        )
        analyse_hinweis = clean_text(bundle.get("hint"))
        if bundle.get("status") == "error" and analyse_hinweis:
            validation["warnings"].append(analyse_hinweis)

        if validation["blockers"]:
            try:
                target.unlink(missing_ok=True)
            except Exception:
                pass
            errors.append(f"{original_name}: {' '.join(validation['blockers'])}")
            continue

        evidence = "; ".join(validation["evidence"])
        if validation["valid"]:
            status_text = "Prüfung bestanden"
            verified += 1
            if amount:
                best_amount = amount
            if invoice_number:
                best_invoice_number = invoice_number
        else:
            status_text = "Bitte prüfen"
            needs_review += 1
        if validation["warnings"]:
            warnings.extend(f"{original_name}: {warning}" for warning in validation["warnings"])

        amount_text = format_bonus_money(amount) if amount else ""
        extrakt_parts = [
            f"Werkstatt-Rechnung: {status_text}.",
            f"Betrag {amount_text} ({amount_source})." if amount_text else "",
            f"Rechnungsnummer {invoice_number}." if invoice_number else "",
            evidence if evidence else "",
        ]
        extrakt_kurz = " ".join(part for part in extrakt_parts if clean_text(part))[:500]
        if not extrakt_kurz:
            extrakt_kurz = "Werkstatt-Rechnung gespeichert. Bitte Original prüfen."

        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, reklamation_id, original_name, stored_name, mime_type, size, quelle, kategorie, dokument_typ, notiz,
             extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am)
            VALUES (?, NULL, ?, ?, ?, ?, 'intern', 'rechnung', 'Rechnung', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                auftrag_id,
                original_name,
                stored_name,
                mime_type,
                target.stat().st_size,
                status_text,
                analysis_text,
                extrakt_kurz,
                clean_text(bundle.get("source")),
                clean_text(bundle.get("analysis_json")),
                " ".join(validation["warnings"])[:500] or analyse_hinweis,
                timestamp,
            ),
        )
        saved += 1

    if verified:
        db.execute(
            """
            UPDATE auftraege
            SET rechnung_status='geschrieben',
                rechnung_nummer=CASE WHEN ? != '' THEN ? ELSE rechnung_nummer END,
                rechnung_geschrieben_am=CASE WHEN COALESCE(rechnung_geschrieben_am, '')='' THEN ? ELSE rechnung_geschrieben_am END,
                bonus_netto_betrag=CASE WHEN ? > 0 THEN ? ELSE bonus_netto_betrag END,
                bonus_preis_aktualisiert_am=CASE WHEN ? > 0 THEN ? ELSE bonus_preis_aktualisiert_am END,
                geaendert_am=?
            WHERE id=?
            """,
            (
                best_invoice_number,
                best_invoice_number,
                timestamp,
                best_amount or 0,
                best_amount or 0,
                best_amount or 0,
                timestamp,
                timestamp,
                auftrag_id,
            ),
        )
    elif saved:
        db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (timestamp, auftrag_id))

    db.commit()
    db.close()
    return {
        "saved": saved,
        "verified": verified,
        "needs_review": needs_review,
        "amount": best_amount,
        "invoice_number": best_invoice_number,
        "error": errors[0] if errors else "",
        "warning": warnings[0] if warnings else "",
    }


def save_bonusrechnung_upload(auftrag_id, files, quelle="intern", notiz=""):
    saved = 0
    best_amount = None
    best_source = ""
    best_filename = ""
    best_invoice_number = ""
    errors = []
    quelle = clean_text(quelle) or "intern"
    if quelle not in {"intern", "autohaus"}:
        quelle = "intern"
    notiz = clean_text(notiz)[:500]
    db = get_db()
    timestamp = now_str()
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        db.close()
        return {"saved": 0, "amount": None, "error": f"Upload-Speicher ist nicht erreichbar: {clean_text(str(exc))[:300]}"}

    for file in files or []:
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
            errors.append(f"{original_name} konnte nicht gespeichert werden: {clean_text(str(exc))[:300]}")
            continue

        mime_type = file.mimetype or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
        bundle = build_document_analysis_bundle_safe(target, original_name) if suffix in ANALYSIS_EXTENSIONS else {}
        analysis_text = clean_text(bundle.get("text"))
        structured_text = structured_analysis_text(bundle.get("structured"), original_name) if bundle.get("structured") else ""
        invoice_text = "\n".join(part for part in [analysis_text, structured_text, original_name] if clean_text(part))
        amount, amount_source = extract_invoice_total_amount(invoice_text)
        invoice_number = extract_invoice_number(invoice_text)
        extrakt_kurz = ""
        if amount:
            extrakt_kurz = f"Bonus-Rechnung: {format_bonus_money(amount)} erkannt ({amount_source})."
            if invoice_number:
                extrakt_kurz = f"{extrakt_kurz} Rechnungsnummer {invoice_number}."
            best_amount = amount
            best_source = amount_source
            best_filename = original_name
            if invoice_number:
                best_invoice_number = invoice_number
        else:
            extrakt_kurz = summarize_document_text(analysis_text, original_name) or "Rechnung gespeichert, Gesamtbetrag bitte prüfen."
            if invoice_number:
                extrakt_kurz = f"{extrakt_kurz} Rechnungsnummer {invoice_number}."
        analyse_hinweis = clean_text(bundle.get("hint"))
        if bundle.get("status") == "error" and analyse_hinweis:
            errors.append(analyse_hinweis)

        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, reklamation_id, original_name, stored_name, mime_type, size, quelle, kategorie, dokument_typ, notiz,
             extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am)
            VALUES (?, NULL, ?, ?, ?, ?, ?, 'bonusrechnung', 'Rechnung', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                auftrag_id,
                original_name,
                stored_name,
                mime_type,
                target.stat().st_size,
                quelle,
                notiz,
                analysis_text,
                extrakt_kurz,
                clean_text(bundle.get("source")),
                clean_text(bundle.get("analysis_json")),
                analyse_hinweis,
                timestamp,
            ),
        )
        saved += 1

    if best_amount:
        db.execute(
            """
            UPDATE auftraege
            SET bonus_netto_betrag=?,
                bonus_preis_aktualisiert_am=?,
                rechnung_nummer=CASE WHEN ? != '' THEN ? ELSE rechnung_nummer END,
                rechnung_status=CASE WHEN COALESCE(rechnung_status, '') IN ('', 'offen') THEN 'geschrieben' ELSE rechnung_status END,
                rechnung_geschrieben_am=CASE WHEN COALESCE(rechnung_geschrieben_am, '')='' THEN ? ELSE rechnung_geschrieben_am END,
                geaendert_am=?
            WHERE id=?
            """,
            (
                best_amount,
                timestamp,
                best_invoice_number,
                best_invoice_number,
                timestamp,
                timestamp,
                auftrag_id,
            ),
        )
    db.commit()
    db.close()
    return {
        "saved": saved,
        "amount": best_amount,
        "source": best_source,
        "filename": best_filename,
        "invoice_number": best_invoice_number,
        "error": errors[0] if errors else "",
    }


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


LEXWARE_RECHNUNG_TYPES = {
    "invoice": "Einnahme",
    "salesinvoice": "Einnahme",
    "downpaymentinvoice": "Einnahme",
    "purchaseinvoice": "Ausgabe",
}

LEXWARE_STATUS_LABELS = {
    "draft": "Entwurf",
    "open": "offen",
    "overdue": "ueberfaellig",
    "paid": "bezahlt",
    "paidoff": "bezahlt",
    "balanced": "bezahlt",
    "transferred": "in Ueberweisung",
    "sepadebit": "Lastschrift",
    "voided": "storniert",
    "unchecked": "zu pruefen",
}


def lexware_iso_to_date(value):
    cleaned = clean_text(value)
    if not cleaned:
        return None
    return parse_date(cleaned[:10])


def lexware_date_label(value):
    parsed = lexware_iso_to_date(value)
    return parsed.strftime(DATE_FMT) if parsed else clean_text(value)


def lexware_amount_label(value):
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        return ""
    return f"{amount:,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", ".")


def normalize_lexware_rechnung_status(voucher_status="", payment_status="", due_date="", open_amount=None):
    voucher_status = clean_text(voucher_status).lower()
    payment_status = clean_text(payment_status).lower()
    try:
        amount_open = float(open_amount or 0)
    except (TypeError, ValueError):
        amount_open = 0
    if voucher_status in {"paid", "paidoff", "voided"} or payment_status == "balanced" or amount_open <= 0:
        return "bezahlt"
    if voucher_status == "unchecked":
        return "pruefen"
    due = lexware_iso_to_date(due_date)
    if voucher_status == "overdue" or (due and due < date.today()):
        return "ueberfaellig"
    if voucher_status in {"transferred", "sepadebit"}:
        return "in_zahlung"
    return "offen"


def lexware_rechnung_status_label(status):
    return {
        "bezahlt": "bezahlt",
        "offen": "offen",
        "ueberfaellig": "ueberfaellig",
        "in_zahlung": "in Zahlung",
        "pruefen": "zu pruefen",
    }.get(clean_text(status), clean_text(status) or "offen")


def lexware_rechnung_typ(voucher_type):
    return LEXWARE_RECHNUNG_TYPES.get(clean_text(voucher_type).lower(), "Sonstiges")


def lexware_rechnung_permalink(voucher_id, voucher_type):
    voucher_id = clean_text(voucher_id)
    voucher_type = clean_text(voucher_type).lower()
    if not voucher_id:
        return ""
    path = "vouchers"
    if voucher_type in {"invoice", "salesinvoice", "downpaymentinvoice"}:
        path = "invoices"
    elif voucher_type == "purchaseinvoice":
        path = "vouchers"
    elif voucher_type == "creditnote":
        path = "credit-notes"
    return f"{LEXWARE_APP_BASE_URL}/permalink/{path}/view/{voucher_id}"


def fetch_lexware_voucherlist(voucher_types, voucher_status, size=100):
    query = (
        f"?voucherType={quote(voucher_types)}"
        f"&voucherStatus={quote(voucher_status)}"
        f"&archived=false&size={int(size)}&page=0&sort=updatedDate,DESC"
    )
    return lexware_request("GET", "/v1/voucherlist", query=query).get("content", [])


def fetch_lexware_payment(voucher_id):
    try:
        return lexware_request("GET", f"/v1/payments/{quote(clean_text(voucher_id))}")
    except RuntimeError as exc:
        return {"error": str(exc)}


def upsert_lexware_rechnung(voucher, payment=None):
    payment = payment or {}
    voucher_id = clean_text(voucher.get("id"))
    if not voucher_id:
        return False
    open_amount = payment.get("openAmount", voucher.get("openAmount"))
    status = normalize_lexware_rechnung_status(
        voucher.get("voucherStatus"),
        payment.get("paymentStatus"),
        voucher.get("dueDate"),
        open_amount,
    )
    now = now_str()
    db = get_db()
    try:
        db.execute(
            """
            INSERT INTO lexware_rechnungen
              (voucher_id, voucher_type, richtung, status, payment_status, voucher_status,
               voucher_number, contact_name, contact_id, total_amount, open_amount, currency,
               voucher_date, due_date, paid_date, lexware_url, raw_json, zuletzt_synced_am, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(voucher_id) DO UPDATE SET
              voucher_type=excluded.voucher_type,
              richtung=excluded.richtung,
              status=excluded.status,
              payment_status=excluded.payment_status,
              voucher_status=excluded.voucher_status,
              voucher_number=excluded.voucher_number,
              contact_name=excluded.contact_name,
              contact_id=excluded.contact_id,
              total_amount=excluded.total_amount,
              open_amount=excluded.open_amount,
              currency=excluded.currency,
              voucher_date=excluded.voucher_date,
              due_date=excluded.due_date,
              paid_date=excluded.paid_date,
              lexware_url=excluded.lexware_url,
              raw_json=excluded.raw_json,
              zuletzt_synced_am=excluded.zuletzt_synced_am,
              geaendert_am=excluded.geaendert_am
            """,
            (
                voucher_id,
                clean_text(voucher.get("voucherType")),
                lexware_rechnung_typ(voucher.get("voucherType")),
                status,
                clean_text(payment.get("paymentStatus")),
                clean_text(payment.get("voucherStatus") or voucher.get("voucherStatus")),
                clean_text(voucher.get("voucherNumber")),
                clean_text(voucher.get("contactName")),
                clean_text(voucher.get("contactId")),
                float(voucher.get("totalAmount") or 0),
                float(open_amount or 0),
                clean_text(voucher.get("currency") or payment.get("currency") or "EUR"),
                lexware_date_label(voucher.get("voucherDate")),
                lexware_date_label(voucher.get("dueDate")),
                lexware_date_label(payment.get("paidDate")),
                lexware_rechnung_permalink(voucher_id, voucher.get("voucherType")),
                json.dumps({"voucher": voucher, "payment": payment}, ensure_ascii=False)[:20000],
                now,
                now,
                now,
            ),
        )
        db.commit()
    finally:
        db.close()
    return True


def sync_lexware_rechnungen():
    if not LEXWARE_API_KEY:
        return {"ok": False, "message": "LEXWARE_API_KEY fehlt.", "synced": 0}
    synced = 0
    errors = []
    for voucher_status in ("open", "overdue", "transferred", "sepadebit", "paid"):
        try:
            vouchers = fetch_lexware_voucherlist("invoice,salesinvoice,purchaseinvoice,downpaymentinvoice", voucher_status)
        except RuntimeError as exc:
            errors.append(str(exc))
            continue
        for voucher in vouchers:
            payment = fetch_lexware_payment(voucher.get("id"))
            upsert_lexware_rechnung(voucher, payment)
            synced += 1
            time.sleep(0.55)
    set_app_setting("LEXWARE_LAST_SYNC", now_str())
    set_app_setting("LEXWARE_LAST_SYNC_ERROR", "\n".join(errors)[:1000])
    schedule_change_backup("lexware-rechnungen")
    return {
        "ok": not errors,
        "message": f"{synced} Lexware-Rechnung(en) synchronisiert." if synced else "Keine Rechnungen gefunden.",
        "synced": synced,
        "errors": errors,
    }


def maybe_sync_lexware_rechnungen():
    if app.config.get("TESTING"):
        return None
    if not LEXWARE_API_KEY:
        return None
    last_sync = get_app_setting("LEXWARE_LAST_SYNC", "")
    try:
        last_dt = datetime.strptime(last_sync, DATETIME_FMT)
    except ValueError:
        last_dt = None
    if last_dt and datetime.now() - last_dt < timedelta(minutes=LEXWARE_AUTO_SYNC_MINUTES):
        return None
    return sync_lexware_rechnungen()


def hydrate_lexware_rechnung(row):
    item = dict(row)
    item["status_label"] = lexware_rechnung_status_label(item.get("status"))
    item["status_class"] = {
        "bezahlt": "success",
        "offen": "warning",
        "ueberfaellig": "danger",
        "in_zahlung": "info",
        "pruefen": "secondary",
    }.get(clean_text(item.get("status")), "secondary")
    item["total_amount_label"] = lexware_amount_label(item.get("total_amount"))
    item["open_amount_label"] = lexware_amount_label(item.get("open_amount"))
    item["richtung_label"] = clean_text(item.get("richtung")) or lexware_rechnung_typ(item.get("voucher_type"))
    return item


def list_lexware_rechnungen(status="kritisch", limit=200):
    status = clean_text(status) or "kritisch"
    limit = max(1, min(int(limit or 200), 500))
    db = get_db()
    try:
        if status == "alle":
            rows = db.execute(
                "SELECT * FROM lexware_rechnungen ORDER BY CASE WHEN status='ueberfaellig' THEN 0 WHEN status='offen' THEN 1 ELSE 2 END, due_date ASC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        elif status == "kritisch":
            rows = db.execute(
                "SELECT * FROM lexware_rechnungen WHERE status IN ('offen','ueberfaellig','in_zahlung','pruefen') ORDER BY CASE WHEN status='ueberfaellig' THEN 0 WHEN status='offen' THEN 1 ELSE 2 END, due_date ASC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM lexware_rechnungen WHERE status=? ORDER BY due_date ASC, id DESC LIMIT ?",
                (status, limit),
            ).fetchall()
    finally:
        db.close()
    return [hydrate_lexware_rechnung(row) for row in rows]


def lexware_rechnungen_summary():
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT status, richtung, COUNT(*) AS count, COALESCE(SUM(open_amount), 0) AS amount
            FROM lexware_rechnungen
            GROUP BY status, richtung
            """
        ).fetchall()
    finally:
        db.close()
    summary = {
        "offen_count": 0,
        "ueberfaellig_count": 0,
        "offen_amount": 0.0,
        "ueberfaellig_amount": 0.0,
        "ausgaben_offen_count": 0,
        "ausgaben_offen_amount": 0.0,
        "ausgaben_offen_total_count": 0,
        "ausgaben_offen_total_amount": 0.0,
        "einnahmen_offen_count": 0,
        "einnahmen_offen_amount": 0.0,
        "einnahmen_offen_total_count": 0,
        "einnahmen_offen_total_amount": 0.0,
    }
    for row in rows:
        status = clean_text(row["status"])
        richtung = clean_text(row["richtung"])
        count = int(row["count"] or 0)
        amount = float(row["amount"] or 0)
        if status in {"offen", "in_zahlung", "pruefen"}:
            summary["offen_count"] += count
            summary["offen_amount"] += amount
            if richtung == "Ausgabe":
                summary["ausgaben_offen_count"] += count
                summary["ausgaben_offen_amount"] += amount
            elif richtung == "Einnahme":
                summary["einnahmen_offen_count"] += count
                summary["einnahmen_offen_amount"] += amount
        if status == "ueberfaellig":
            summary["ueberfaellig_count"] += count
            summary["ueberfaellig_amount"] += amount
            if richtung == "Ausgabe":
                summary["ausgaben_offen_total_count"] += count
                summary["ausgaben_offen_total_amount"] += amount
            elif richtung == "Einnahme":
                summary["einnahmen_offen_total_count"] += count
                summary["einnahmen_offen_total_amount"] += amount
    summary["ausgaben_offen_total_count"] += summary["ausgaben_offen_count"]
    summary["ausgaben_offen_total_amount"] += summary["ausgaben_offen_amount"]
    summary["einnahmen_offen_total_count"] += summary["einnahmen_offen_count"]
    summary["einnahmen_offen_total_amount"] += summary["einnahmen_offen_amount"]
    summary["offen_amount_label"] = lexware_amount_label(summary["offen_amount"])
    summary["ueberfaellig_amount_label"] = lexware_amount_label(summary["ueberfaellig_amount"])
    summary["ausgaben_offen_amount_label"] = lexware_amount_label(summary["ausgaben_offen_amount"])
    summary["einnahmen_offen_amount_label"] = lexware_amount_label(summary["einnahmen_offen_amount"])
    summary["ausgaben_offen_total_amount_label"] = lexware_amount_label(summary["ausgaben_offen_total_amount"])
    summary["einnahmen_offen_total_amount_label"] = lexware_amount_label(summary["einnahmen_offen_total_amount"])
    summary["last_sync"] = get_app_setting("LEXWARE_LAST_SYNC", "")
    summary["last_error"] = get_app_setting("LEXWARE_LAST_SYNC_ERROR", "")
    return summary


def kontoauszug_file_allowed(filename):
    return pathlib.Path(clean_text(filename)).suffix.lower() in KONTOAUSZUG_UPLOAD_EXTENSIONS


def decode_bank_statement_bytes(raw):
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="ignore")


def sha256_hex(data):
    return hashlib.sha256(data or b"").hexdigest()


BANK_DEBIT_TERMS = (
    "soll",
    "s",
    "belastung",
    "lastschrift",
    "ausgang",
    "abgang",
    "abbuchung",
    "kartenzahlung",
    "debit",
    "dr",
)
BANK_CREDIT_TERMS = (
    "haben",
    "h",
    "gutschrift",
    "eingang",
    "zugang",
    "zahlungseingang",
    "credit",
    "cr",
)


def normalize_bank_amount_text(value):
    return clean_text(value).replace("\u2212", "-").replace("\u2013", "-").replace("\u2014", "-")


def bank_sign_from_text(value):
    text = normalize_bank_amount_text(value)
    if not text:
        return 0
    normalized = normalize_document_text(text)
    if re.search(r"(^\s*-)|(-\s*$)|\((.*?)\)", text):
        return -1
    if re.search(r"(^\s*\+)|(\+\s*$)", text):
        return 1
    has_debit = any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in BANK_DEBIT_TERMS)
    has_credit = any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in BANK_CREDIT_TERMS)
    if has_debit and not has_credit:
        return -1
    if has_credit and not has_debit:
        return 1
    return 0


def parse_bank_amount(value):
    text = normalize_bank_amount_text(value)
    if not text:
        return None
    sign = bank_sign_from_text(text)
    text = re.sub(
        r"(EUR|€|Haben|Soll|Belastung|Lastschrift|Gutschrift|Eingang|Ausgang|Abgang|Zugang|Debit|Credit|Dr|Cr)",
        "",
        text,
        flags=re.I,
    )
    text = text.replace("\u00a0", " ").replace("'", "").strip()
    text = re.sub(r"[^0-9,.\-+]", "", text)
    if not text:
        return None
    text = text.strip("+-")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    elif "." in text:
        parts = text.split(".")
        if len(parts[-1]) == 2:
            text = "".join(parts[:-1]).replace(".", "") + "." + parts[-1]
        elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", text):
            text = text.replace(".", "")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", text):
        text = text.replace(".", "")
    try:
        amount = round(float(text), 2)
    except ValueError:
        return None
    if sign < 0:
        amount = -abs(amount)
    elif sign > 0:
        amount = abs(amount)
    return amount


def find_bank_amount_candidates(text):
    value = normalize_bank_amount_text(text)
    context_sign = bank_sign_from_text(value)
    pattern = (
        r"[-+]?\s*\(?\d{1,3}(?:[.']\d{3})*(?:,\d{2})\)?(?:\s*[-+])?(?:\s*(?:EUR|€|S|H|Soll|Haben|Abgang|Zugang))?"
        r"|[-+]?\s*\(?\d+\.\d{2}\)?(?:\s*[-+])?(?:\s*(?:EUR|€|S|H|Soll|Haben|Abgang|Zugang))?"
    )
    candidates = []
    for match in re.finditer(pattern, value, re.I):
        token = match.group(0)
        amount = parse_bank_amount(token)
        if amount is not None and bank_sign_from_text(token) == 0 and context_sign:
            amount = abs(amount) * context_sign
        if amount is not None:
            candidates.append({"text": token, "amount": amount, "start": match.start(), "end": match.end()})
    return candidates


def find_bank_amount_in_text(text):
    value = clean_text(text)
    if line_is_bank_summary(value):
        return None
    candidates = find_bank_amount_candidates(value)
    if not candidates:
        return None
    saldo_match = re.search(r"\b(Saldo|Kontostand|Alter Bestand|Neuer Bestand)\b", value, re.I)
    if saldo_match:
        non_saldo = [candidate for candidate in candidates if candidate["start"] < saldo_match.start()]
        if non_saldo:
            return non_saldo[-1]["amount"]
        return None
    for candidate in reversed(candidates):
        return candidate["amount"]
    return None


def parse_bank_date(value):
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\b", text)
    candidate = match.group(1) if match else text
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return parse_date(candidate)


def parse_bank_date_label(value):
    text = clean_text(value)
    if not text:
        return ""
    parsed = parse_bank_date(text)
    if parsed:
        return parsed.strftime(DATE_FMT)
    match = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\b", text)
    return clean_text(match.group(1) if match else text)


def infer_bank_statement_year(text="", original_name=""):
    filename = clean_text(original_name)
    filename_date = re.search(r"(20\d{2})[._-]\d{1,2}[._-]\d{1,2}", filename)
    if filename_date:
        return filename_date.group(1)
    for source in (filename, text[:1000]):
        matches = re.findall(r"(?<!\d)(20\d{2})(?!\d)", clean_text(source))
        if matches:
            return matches[0]
    return str(date.today().year)


def bank_short_date_label(value, year):
    text = clean_text(value).rstrip(".")
    match = re.match(r"^(\d{1,2})\.(\d{1,2})$", text)
    if not match:
        return parse_bank_date_label(value)
    return f"{int(match.group(1)):02d}.{int(match.group(2)):02d}.{year}"


def parse_bank_statement_amount_with_marker(amount_text, marker):
    amount = parse_bank_amount(amount_text)
    if amount is None:
        return None
    marker = clean_text(marker).upper()
    if marker == "S":
        return -abs(amount)
    if marker == "H":
        return abs(amount)
    return amount


def line_starts_structured_bank_booking(line):
    return bool(
        re.match(
            r"^\s*\d{1,2}\.\d{1,2}\.\s+\d{1,2}\.\d{1,2}\.\s+.+?\s+\d{1,3}(?:\.\d{3})*,\d{2}\s*[SH]\s*$",
            clean_text(line),
            re.I,
        )
    )


def line_ends_bank_statement_section(line):
    normalized = normalize_document_text(line)
    return bool(
        re.fullmatch(r"[─━_\-\s]+", clean_text(line))
        or
        line_is_bank_summary(line)
        or normalized.startswith("ubertrag ")
        or normalized.startswith("uebertrag ")
        or normalized.startswith("anlage ")
        or normalized.startswith("buchungstag")
        or normalized.startswith("wert:")
        or normalized.startswith("rechnung nr")
        or normalized.startswith("ust")
        or normalized.startswith("sehr geehrte")
        or normalized.startswith("sie haben ")
        or normalized.startswith("mit freundlichen")
        or normalized in {"wert vorgang", "vorgangwert"}
    )


def parse_structured_kontoauszug_text(text, original_name=""):
    year = infer_bank_statement_year(text, original_name)
    rows = []
    current = None
    booking_re = re.compile(
        r"^\s*(?P<book>\d{1,2}\.\d{1,2}\.)\s+"
        r"(?P<valuta>\d{1,2}\.\d{1,2}\.)\s+"
        r"(?P<title>.+?)\s+"
        r"(?P<amount>\d{1,3}(?:\.\d{3})*,\d{2})\s*(?P<marker>[SH])\s*$",
        re.I,
    )

    def flush():
        nonlocal current
        if not current:
            return
        details = [compact_whitespace(line) for line in current["details"] if compact_whitespace(line)]
        name = details[0] if details else current["title"]
        purpose = " ".join([current["title"]] + details)
        rows.append(
            {
                "buchung_datum": bank_short_date_label(current["book"], year),
                "name": name[:180],
                "verwendungszweck": purpose[:1000],
                "betrag": current["amount"],
            }
        )
        current = None

    for raw_line in text.splitlines():
        line = compact_whitespace(raw_line)
        if not line:
            continue
        match = booking_re.match(line)
        if match:
            flush()
            amount = parse_bank_statement_amount_with_marker(match.group("amount"), match.group("marker"))
            if amount is None:
                current = None
                continue
            current = {
                "book": match.group("book"),
                "title": compact_whitespace(match.group("title")),
                "amount": amount,
                "details": [],
            }
            title_norm = normalize_document_text(current["title"])
            if title_norm.startswith("abschluss lt anlage"):
                current["details"].append(current["title"])
            continue
        if not current:
            continue
        if line_starts_structured_bank_booking(line):
            flush()
            continue
        if line_ends_bank_statement_section(line):
            flush()
            continue
        current["details"].append(line)
    flush()
    return rows


def normalize_bank_header(value):
    text = normalize_document_text(value)
    return re.sub(r"[^a-z0-9]+", "", text)


def csv_pick(row, wanted_headers):
    _, value = csv_pick_with_key(row, wanted_headers)
    return value


def csv_pick_with_key(row, wanted_headers):
    normalized = {normalize_bank_header(key): value for key, value in row.items()}
    for header in wanted_headers:
        key = normalize_bank_header(header)
        if key in normalized and clean_text(normalized[key]):
            return key, clean_text(normalized[key])
    for raw_key, raw_value in row.items():
        normalized_key = normalize_bank_header(raw_key)
        if not clean_text(raw_value):
            continue
        for header in wanted_headers:
            wanted = normalize_bank_header(header)
            if wanted and wanted in normalized_key:
                return normalized_key, clean_text(raw_value)
    return "", ""


def csv_join_values(row, wanted_headers):
    parts = []
    seen = set()
    for header in wanted_headers:
        _, value = csv_pick_with_key(row, (header,))
        key = value.lower()
        if value and key not in seen:
            parts.append(value)
            seen.add(key)
    return " ".join(parts)


def guess_bank_csv_delimiter(lines):
    sample_lines = [line for line in lines[:20] if clean_text(line)]
    counts = {
        ";": sum(line.count(";") for line in sample_lines),
        "\t": sum(line.count("\t") for line in sample_lines),
        ",": sum(line.count(",") for line in sample_lines),
    }
    if counts[";"] >= 2:
        return ";"
    if counts["\t"] >= 2:
        return "\t"
    return "," if counts[","] >= 2 else ";"


BANK_CSV_HEADER_MARKERS = {
    "buchungstag",
    "buchungsdatum",
    "valuta",
    "valutadatum",
    "wertstellung",
    "datum",
    "date",
    "auftraggeberempfaenger",
    "auftraggeber",
    "empfaenger",
    "beguenstigter",
    "zahlungspflichtiger",
    "name",
    "verwendungszweck",
    "buchungstext",
    "beschreibung",
    "zahlungsreferenz",
    "kundenreferenz",
    "referenz",
    "betrag",
    "betrageur",
    "betraginer",
    "umsatz",
    "umsatzeur",
    "abgang",
    "zugang",
    "soll",
    "haben",
    "sollhaben",
    "sh",
    "waehrung",
    "wahrung",
    "currency",
}


def bank_csv_header_score(row):
    score = 0
    for cell in row:
        normalized = normalize_bank_header(cell)
        if not normalized:
            continue
        if normalized in BANK_CSV_HEADER_MARKERS:
            score += 2
        elif any(marker and marker in normalized for marker in BANK_CSV_HEADER_MARKERS):
            score += 1
    return score


def csv_reader_rows(text):
    lines = text.splitlines()
    delimiter = guess_bank_csv_delimiter(lines)
    reader = csv.reader(lines, delimiter=delimiter)
    return [[clean_text(cell) for cell in row] for row in reader if any(clean_text(cell) for cell in row)]


def csv_amount_from_row(row):
    amount_key, amount_text = csv_pick_with_key(
        row,
        (
            "Betrag",
            "Umsatz",
            "Amount",
            "Wert",
            "Abgang",
            "Zugang",
            "Betrag EUR",
            "Betrag in EUR",
            "Umsatz EUR",
            "Umsatz in EUR",
            "Transaction Amount",
        ),
    )
    amount = parse_bank_amount(amount_text)
    soll_key, soll_text = csv_pick_with_key(
        row,
        ("Soll", "Belastung", "Ausgang", "Abgang", "Abbuchung", "Debit", "Lastschrift"),
    )
    haben_key, haben_text = csv_pick_with_key(row, ("Haben", "Gutschrift", "Eingang", "Zugang", "Credit"))
    if amount is None and soll_text:
        amount = parse_bank_amount(soll_text)
        if amount is not None:
            amount = -abs(amount)
    if amount is None and haben_text:
        amount = parse_bank_amount(haben_text)
        if amount is not None:
            amount = abs(amount)
    marker = csv_join_values(
        row,
        ("Soll/Haben", "S/H", "Haben/Soll", "Debit/Credit", "Umsatzart", "Kennzeichen", "Buchungsart", "Art", "Typ"),
    )
    marker_sign = bank_sign_from_text(marker)
    debit_keys = {"soll", "belastung", "ausgang", "abgang", "abbuchung", "debit", "lastschrift"}
    credit_keys = {"haben", "gutschrift", "eingang", "zugang", "credit"}
    if amount is not None:
        if amount_key in debit_keys or soll_key in debit_keys or marker_sign < 0:
            amount = -abs(amount)
        elif amount_key in credit_keys or haben_key in credit_keys or marker_sign > 0:
            amount = abs(amount)
    return amount


def parse_kontoauszug_csv(text):
    rows = []
    try:
        raw_rows = csv_reader_rows(text)
    except csv.Error:
        rows = []
        raw_rows = []
    if not raw_rows:
        return rows
    header_index = 0
    best_score = -1
    for index, raw_row in enumerate(raw_rows[:30]):
        score = bank_csv_header_score(raw_row)
        if score > best_score:
            best_score = score
            header_index = index
    if best_score < 2:
        return rows
    headers = [clean_text(header) or f"Spalte {index + 1}" for index, header in enumerate(raw_rows[header_index])]
    for raw_row in raw_rows[header_index + 1 :]:
        if len(raw_row) < 2:
            continue
        padded = list(raw_row) + [""] * max(0, len(headers) - len(raw_row))
        if len(padded) > len(headers):
            padded = padded[: len(headers) - 1] + [" ".join(padded[len(headers) - 1 :])]
        row = dict(zip(headers, padded))
        amount = csv_amount_from_row(row)
        if amount is None:
            continue
        purpose = csv_join_values(
            row,
            (
                "Verwendungszweck",
                "Zahlungsreferenz",
                "Kundenreferenz",
                "Mandatsreferenz",
                "Referenz",
                "Buchungstext",
                "Beschreibung",
                "Text",
                "Purpose",
            ),
        )
        name = csv_pick(
            row,
            (
                "Auftraggeber/Empfänger",
                "Auftraggeber",
                "Empfänger",
                "Name",
                "Begünstigter",
                "Zahlungspflichtiger",
                "Debitor/Kreditor",
            ),
        )
        booking_date = parse_bank_date_label(
            csv_pick(row, ("Buchungstag", "Buchungsdatum", "Valuta", "Valutadatum", "Wertstellung", "Datum", "Date"))
        )
        joined_row = " ".join(clean_text(value) for value in row.values() if clean_text(value))
        rows.append(
            {
                "buchung_datum": booking_date,
                "name": name,
                "verwendungszweck": purpose or joined_row[:500],
                "betrag": amount,
            }
        )
    return rows


def line_is_bank_summary(line):
    return bool(
        re.search(
            r"\b("
            r"alter\s+saldo|neuer\s+saldo|anfangssaldo|endsaldo|zwischensaldo|saldo\s+alt|saldo\s+neu|"
            r"kontostand|kontosaldo|alter\s+bestand|neuer\s+bestand|buchungssaldo|abschlusssaldo|"
            r"summe\s+einnahmen|summe\s+ausgaben|gesamtumsatz|kontonummer|iban|bic|auszug\s+nr"
            r")\b",
            normalize_document_text(line),
            re.I,
        )
    )


def bank_line_has_date(line):
    return bool(re.search(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b|\b\d{4}-\d{1,2}-\d{1,2}\b", line))


def finalize_bank_text_block(block):
    lines = [line for line in block if clean_text(line)]
    if not lines:
        return None
    joined = " ".join(lines)
    if any(line_is_bank_summary(line) for line in lines):
        return None
    amount = None
    for line in reversed(lines):
        if line_is_bank_summary(line):
            continue
        candidate = find_bank_amount_in_text(line)
        if candidate is not None:
            amount = candidate
            break
    if amount is None:
        return None
    block_sign = bank_sign_from_text(joined)
    if block_sign:
        amount = abs(amount) * block_sign
    booking_date = ""
    for line in lines:
        if bank_line_has_date(line):
            booking_date = parse_bank_date_label(line)
            break
    if not booking_date:
        return None
    detail_lines = []
    for line in lines:
        cleaned = line
        cleaned = re.sub(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b|\b\d{4}-\d{1,2}-\d{1,2}\b", " ", cleaned)
        for candidate in find_bank_amount_candidates(cleaned):
            cleaned = cleaned.replace(candidate["text"], " ")
        cleaned = compact_whitespace(cleaned)
        if cleaned and not line_is_bank_summary(cleaned):
            detail_lines.append(cleaned)
    name = detail_lines[0] if detail_lines else ""
    purpose = " ".join(detail_lines) or joined
    return {
        "buchung_datum": booking_date,
        "name": name[:180],
        "verwendungszweck": purpose[:1000],
        "betrag": amount,
    }


def parse_kontoauszug_text(text, original_name=""):
    structured_rows = parse_structured_kontoauszug_text(text, original_name)
    if len(structured_rows) >= 3:
        return structured_rows
    rows = []
    current = []
    for raw_line in text.splitlines():
        line = compact_whitespace(raw_line)
        if not line:
            continue
        if line_is_bank_summary(line):
            continue
        has_date = bank_line_has_date(line)
        if has_date and current:
            finalized = finalize_bank_text_block(current)
            if finalized:
                rows.append(finalized)
            current = [line]
        else:
            current.append(line)
        if has_date and find_bank_amount_in_text(line) is not None and len(current) == 1:
            # Single-line export: keep the block open until the next date, so trailing
            # purpose lines can still be attached.
            pass
    if current:
        finalized = finalize_bank_text_block(current)
        if finalized:
            rows.append(finalized)
    if not rows:
        for raw_line in text.splitlines():
            line = compact_whitespace(raw_line)
            if not line or line_is_bank_summary(line):
                continue
            has_date = bank_line_has_date(line)
            amount = find_bank_amount_in_text(line)
            if has_date and amount is not None:
                rows.append(
                    {
                        "buchung_datum": parse_bank_date_label(line),
                        "name": "",
                        "verwendungszweck": line,
                        "betrag": amount,
                    }
                )
    return rows


def parse_kontoauszug_buchungen(text, original_name=""):
    suffix = pathlib.Path(clean_text(original_name)).suffix.lower()
    rows = []
    if suffix in {".csv", ".tsv"} or ";" in text[:1000] or "\t" in text[:1000]:
        rows = parse_kontoauszug_csv(text)
    if not rows:
        rows = parse_kontoauszug_text(text, original_name)
    cleaned = []
    for row in rows:
        amount = row.get("betrag")
        if amount is None:
            continue
        purpose = compact_whitespace(clean_text(row.get("verwendungszweck")))
        name = compact_whitespace(clean_text(row.get("name")))
        if not purpose and not name:
            continue
        cleaned.append(
            {
                "buchung_datum": clean_text(row.get("buchung_datum")),
                "name": name[:180],
                "verwendungszweck": purpose[:1000],
                "betrag": round(float(amount), 2),
                "waehrung": "EUR",
                "richtung": "Eingang" if float(amount) >= 0 else "Ausgang",
            }
        )
    return cleaned


def effective_bank_booking_amount(row):
    try:
        amount = float((row or {}).get("betrag") or 0)
    except (TypeError, ValueError):
        amount = 0.0
    text_sign = bank_sign_from_text(
        " ".join(
            clean_text((row or {}).get(key))
            for key in ("name", "verwendungszweck")
            if clean_text((row or {}).get(key))
        )
    )
    direction = clean_text((row or {}).get("richtung"))
    direction_sign = 0
    if direction == "Ausgang":
        direction_sign = -1
    elif direction == "Eingang":
        direction_sign = 1
    if text_sign:
        amount = abs(amount) * text_sign
    elif direction_sign:
        amount = abs(amount) * direction_sign
    return round(amount, 2)


def bank_booking_direction(amount):
    try:
        return "Eingang" if float(amount or 0) >= 0 else "Ausgang"
    except (TypeError, ValueError):
        return ""


def save_kontoauszug_upload(file_storage):
    filename = clean_text(getattr(file_storage, "filename", ""))
    if not filename:
        raise ValueError("Bitte einen Kontoauszug auswählen.")
    if not kontoauszug_file_allowed(filename):
        raise ValueError(f"{filename} ist kein erlaubter Kontoauszug. Bitte CSV, TXT oder PDF hochladen.")
    suffix = pathlib.Path(filename).suffix.lower()
    original_name = secure_filename(filename) or f"kontoauszug{suffix}"
    stored_name = f"kontoauszug-{uuid.uuid4().hex}{suffix}"
    target = UPLOAD_DIR / stored_name
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(target)
    raw = target.read_bytes()
    if suffix == ".pdf":
        text = clean_text(extract_document_text_local(target, original_name))
    else:
        text = decode_bank_statement_bytes(raw)
    return {
        "original_name": original_name,
        "stored_name": stored_name,
        "mime_type": clean_text(getattr(file_storage, "mimetype", "")) or clean_text(mimetypes.guess_type(original_name)[0]),
        "size": target.stat().st_size if target.exists() else 0,
        "file_hash": sha256_hex(raw),
        "text": clean_text(text),
    }


def amount_matches_invoice(amount, invoice):
    amount = abs(float(amount or 0))
    candidates = [float(invoice.get("open_amount") or 0), float(invoice.get("total_amount") or 0)]
    return any(abs(amount - abs(candidate)) <= 0.05 for candidate in candidates if abs(candidate) > 0)


def bank_direction_matches_invoice(amount, invoice):
    direction = clean_text(invoice.get("richtung"))
    if amount >= 0 and direction == "Einnahme":
        return True
    if amount < 0 and direction == "Ausgabe":
        return True
    return False


def compact_match_key(value):
    return re.sub(r"[^a-z0-9]+", "", normalize_document_text(value))


def kontoauszug_buchung_key(row):
    date_key = clean_text(row.get("buchung_datum"))
    amount_key = f"{float(row.get('betrag') or 0):.2f}"
    text_key = compact_match_key(" ".join([row.get("name", ""), row.get("verwendungszweck", "")]))[:160]
    return sha256_hex(f"{date_key}|{amount_key}|{text_key}".encode("utf-8"))


def kontoauszug_buchung_already_imported(row, exclude_import_id=0):
    key = kontoauszug_buchung_key(row)
    if not key:
        return False
    db = get_db()
    try:
        existing = db.execute(
            """
            SELECT id
            FROM kontoauszug_buchungen
            WHERE buchung_key=? AND import_id<>?
            LIMIT 1
            """,
            (key, int(exclude_import_id or 0)),
        ).fetchone()
    finally:
        db.close()
    return bool(existing)


def validate_kontoauszug_buchung(row):
    amount = abs(float(row.get("betrag") or 0))
    text_blob = " ".join([clean_text(row.get("name")), clean_text(row.get("verwendungszweck"))])
    normalized = normalize_document_text(text_blob)
    reasons = []
    if not clean_text(row.get("buchung_datum")):
        reasons.append("Datum fehlt")
    if amount <= 0:
        reasons.append("Betrag fehlt")
    if amount >= 50000:
        reasons.append("Betrag ungewoehnlich hoch")
    if any(term in normalized for term in ("saldo", "kontostand", "alter bestand", "neuer bestand", "iban", "bic")):
        reasons.append("Saldo/Kontodaten erkannt")
    if len(compact_match_key(text_blob)) < 8:
        reasons.append("Text zu kurz")
    if reasons:
        return False, "; ".join(reasons)
    return True, ""


def invoice_number_found_in_bank_text(voucher_number, bank_text):
    voucher = normalize_document_text(voucher_number)
    if not voucher:
        return False
    if voucher in bank_text:
        return True
    compact_voucher = compact_match_key(voucher)
    compact_text = compact_match_key(bank_text)
    return bool(compact_voucher and compact_voucher in compact_text)


def match_kontoauszug_buchung_to_rechnung(buchung):
    amount = float(buchung.get("betrag") or 0)
    text_blob = normalize_document_text(
        " ".join([buchung.get("name", ""), buchung.get("verwendungszweck", "")])
    )
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM lexware_rechnungen
            WHERE status IN ('offen', 'ueberfaellig', 'in_zahlung', 'pruefen')
            ORDER BY CASE WHEN status='ueberfaellig' THEN 0 ELSE 1 END, due_date ASC, id DESC
            LIMIT 500
            """
        ).fetchall()
    finally:
        db.close()
    candidates = []
    for row in rows:
        invoice = dict(row)
        score = 0
        reasons = []
        amount_hit = amount_matches_invoice(amount, invoice)
        direction_hit = bank_direction_matches_invoice(amount, invoice)
        invoice_hit = invoice_number_found_in_bank_text(invoice.get("voucher_number"), text_blob)
        if amount_hit:
            score += 45
            reasons.append("Betrag passt")
        if invoice_hit:
            score += 60
            reasons.append("Rechnungsnummer gefunden")
        contact_words = [
            word
            for word in normalize_document_text(invoice.get("contact_name")).split()
            if len(word) >= 4
        ][:4]
        if contact_words:
            hits = sum(1 for word in contact_words if word in text_blob)
            if hits:
                score += min(25, hits * 10)
                reasons.append("Kontakt passt")
        if direction_hit:
            score += 10
            reasons.append("Richtung passt")
        elif clean_text(invoice.get("richtung")) in {"Einnahme", "Ausgabe"}:
            score -= 15
            reasons.append("Richtung widerspricht")
        if score > 0:
            candidates.append(
                {
                    "invoice": invoice,
                    "score": score,
                    "reasons": reasons,
                    "amount_hit": amount_hit,
                    "direction_hit": direction_hit,
                    "invoice_hit": invoice_hit,
                }
            )
    if not candidates:
        return None, 0, "Kein sicherer Treffer"
    candidates.sort(key=lambda item: item["score"], reverse=True)
    best = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None
    hint = ", ".join(best["reasons"]) if best["reasons"] else "Treffer unsicher"
    if best["score"] < 70:
        return None, best["score"], f"Zu unsicher: {hint}"
    if not best["amount_hit"]:
        return None, best["score"], f"Betrag weicht ab - bitte manuell prüfen: {hint}"
    if not best["direction_hit"]:
        return None, best["score"], f"Richtung widerspricht - bitte manuell prüfen: {hint}"
    if not best["invoice_hit"]:
        return None, best["score"], f"Keine eindeutige Rechnungsnummer im Kontoauszug: {hint}"
    if second and second["score"] >= best["score"] - 12 and not best["invoice_hit"]:
        return None, best["score"], "Mehrere ähnliche Treffer - bitte manuell prüfen"
    return best["invoice"], best["score"], hint


def mark_rechnung_from_kontoauszug_paid(rechnung_id, buchung):
    db = get_db()
    try:
        db.execute(
            """
            UPDATE lexware_rechnungen
            SET status='bezahlt',
                payment_status='bankauszug',
                open_amount=0,
                paid_date=?,
                zuletzt_synced_am=?,
                geaendert_am=?
            WHERE id=?
            """,
            (
                clean_text(buchung.get("buchung_datum")) or date.today().strftime(DATE_FMT),
                now_str(),
                now_str(),
                int(rechnung_id),
            ),
        )
        db.commit()
    finally:
        db.close()


def import_kontoauszug(file_storage):
    upload = save_kontoauszug_upload(file_storage)
    db = get_db()
    try:
        duplicate_import = db.execute(
            """
            SELECT id, original_name, erstellt_am
            FROM kontoauszug_importe
            WHERE file_hash<>'' AND file_hash=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (upload["file_hash"],),
        ).fetchone()
    finally:
        db.close()
    if duplicate_import:
        return {
            "ok": True,
            "duplicate": True,
            "import_id": int(duplicate_import["id"]),
            "buchungen": 0,
            "matched": 0,
            "pruefen": 0,
            "message": (
                "Kontoauszug wurde bereits importiert "
                f"({duplicate_import['original_name']} am {duplicate_import['erstellt_am']})."
            ),
        }
    rows = parse_kontoauszug_buchungen(upload["text"], upload["original_name"])
    now = now_str()
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO kontoauszug_importe
              (original_name, stored_name, mime_type, size, file_hash, extrahierter_text,
               buchungen_count, matched_count, pruefen_count, erstellt_am)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?)
            """,
            (
                upload["original_name"],
                upload["stored_name"],
                upload["mime_type"],
                int(upload["size"] or 0),
                upload["file_hash"],
                upload["text"][:20000],
                now,
            ),
        )
        import_id = int(cursor.lastrowid)
        db.commit()
    finally:
        db.close()

    matched = 0
    pruefen = 0
    duplicate_rows = 0
    for row in rows:
        effective_amount = effective_bank_booking_amount(row)
        row["betrag"] = effective_amount
        row["richtung"] = bank_booking_direction(effective_amount)
        booking_key = kontoauszug_buchung_key(row)
        row_ok, row_hint = validate_kontoauszug_buchung(row)
        if kontoauszug_buchung_already_imported(row, exclude_import_id=import_id):
            rechnung, score, hint = None, 0, "Doppelte Buchung aus bereits importiertem Kontoauszug"
            status = "duplikat"
            rechnung_id = 0
            duplicate_rows += 1
        elif not row_ok:
            rechnung, score, hint = None, 0, f"Bitte pruefen: {row_hint}"
            status = "pruefen"
            rechnung_id = 0
            pruefen += 1
        else:
            rechnung, score, hint = match_kontoauszug_buchung_to_rechnung(row)
            status = "zugeordnet" if rechnung else "offen"
            rechnung_id = int(rechnung["id"]) if rechnung else 0
        if rechnung and status == "zugeordnet":
            mark_rechnung_from_kontoauszug_paid(rechnung_id, row)
            matched += 1
        db = get_db()
        try:
            db.execute(
                """
                INSERT INTO kontoauszug_buchungen
                  (import_id, buchung_datum, name, verwendungszweck, betrag, waehrung, richtung,
                   status, rechnung_id, match_score, hinweis, buchung_key, erstellt_am)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    import_id,
                    row["buchung_datum"],
                    row["name"],
                    row["verwendungszweck"],
                    row["betrag"],
                    row["waehrung"],
                    row["richtung"],
                    status,
                    rechnung_id,
                    int(score or 0),
                    hint,
                    booking_key,
                    now_str(),
                ),
            )
            db.commit()
        finally:
            db.close()

    db = get_db()
    try:
        db.execute(
            "UPDATE kontoauszug_importe SET buchungen_count=?, matched_count=?, pruefen_count=? WHERE id=?",
            (len(rows), matched, pruefen, import_id),
        )
        db.commit()
    finally:
        db.close()
    schedule_change_backup("kontoauszug")
    return {
        "ok": True,
        "import_id": import_id,
        "buchungen": len(rows),
        "matched": matched,
        "pruefen": pruefen,
        "duplicates": duplicate_rows,
        "message": (
            f"{len(rows)} Buchung(en) erkannt, {matched} Zahlung(en) zugeordnet, "
            f"{pruefen} zur Kontrolle, {duplicate_rows} doppelt."
        ),
    }


def hydrate_kontoauszug_buchung(row):
    item = dict(row)
    item["effektiver_betrag"] = effective_bank_booking_amount(item)
    item["effektive_richtung"] = bank_booking_direction(item["effektiver_betrag"])
    item["betrag_label"] = lexware_amount_label(item["effektiver_betrag"])
    item["betrag_korrigiert"] = abs(float(item.get("betrag") or 0) - item["effektiver_betrag"]) > 0.001
    item["status_label"] = {
        "zugeordnet": "erledigt",
        "offen": "offen",
        "pruefen": "prüfen",
        "duplikat": "doppelt",
        "ignoriert": "ignoriert",
    }.get(clean_text(item.get("status")), clean_text(item.get("status")) or "offen")
    item["status_class"] = {
        "zugeordnet": "success",
        "offen": "warning",
        "pruefen": "danger",
        "duplikat": "secondary",
        "ignoriert": "secondary",
    }.get(clean_text(item.get("status")), "secondary")
    return item


def kontoauszug_import_statement_sort_key(row):
    item = dict(row or {})
    original_name = clean_text(item.get("original_name"))
    candidates = [
        r"vom[_\s-]+(20\d{2})[._-](\d{1,2})[._-](\d{1,2})",
        r"(20\d{2})[._-](\d{1,2})[._-](\d{1,2})",
    ]
    for pattern in candidates:
        match = re.search(pattern, original_name, re.I)
        if match:
            year, month, day = (int(part) for part in match.groups())
            return (year * 10000 + month * 100 + day, int(item.get("id") or 0))
    return (0, int(item.get("id") or 0))


def list_kontoauszug_buchungen(limit=80):
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT b.*, r.voucher_number, r.contact_name AS rechnung_contact_name, r.lexware_url
            FROM kontoauszug_buchungen b
            LEFT JOIN lexware_rechnungen r ON r.id=b.rechnung_id
            ORDER BY b.id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit or 80), 300)),),
        ).fetchall()
    finally:
        db.close()
    return [hydrate_kontoauszug_buchung(row) for row in rows]


def list_kontoauszug_importe(limit=10):
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM kontoauszug_importe
            ORDER BY id DESC
            LIMIT 200
            """
        ).fetchall()
    finally:
        db.close()
    sorted_rows = sorted((dict(row) for row in rows), key=kontoauszug_import_statement_sort_key, reverse=True)
    return sorted_rows[: max(1, min(int(limit or 10), 50))]


def kontoauszug_auswertung(import_id=None):
    db = get_db()
    try:
        latest_import = None
        if import_id:
            latest_import = db.execute(
                "SELECT * FROM kontoauszug_importe WHERE id=?",
                (int(import_id),),
            ).fetchone()
        if latest_import is None:
            import_rows = db.execute(
                """
                SELECT *
                FROM kontoauszug_importe
                """
            ).fetchall()
            if import_rows:
                latest_import = sorted(import_rows, key=kontoauszug_import_statement_sort_key, reverse=True)[0]
        params = ()
        where_sql = ""
        if latest_import:
            where_sql = "WHERE import_id=?"
            params = (int(latest_import["id"]),)
        rows = db.execute(
            f"""
            SELECT betrag, status, name, verwendungszweck
            FROM kontoauszug_buchungen
            {where_sql}
            """,
            params,
        ).fetchall()
    finally:
        db.close()
    data = {
        "buchungen_count": len(rows or []),
        "einnahmen": 0.0,
        "ausgaben": 0.0,
        "saldo": 0.0,
        "einnahmen_count": 0,
        "ausgaben_count": 0,
        "zugeordnet_count": 0,
        "offen_count": 0,
        "pruefen_count": 0,
        "duplikat_count": 0,
        "offene_einnahmen": 0.0,
        "offene_ausgaben": 0.0,
        "pruefen_amount": 0.0,
    }
    for source_row in rows or []:
        row = dict(source_row)
        status = clean_text(row.get("status"))
        amount = effective_bank_booking_amount(row)
        if status == "zugeordnet":
            data["zugeordnet_count"] += 1
        elif status == "offen":
            data["offen_count"] += 1
        elif status == "pruefen":
            data["pruefen_count"] += 1
            data["pruefen_amount"] += abs(amount)
        elif status == "duplikat":
            data["duplikat_count"] += 1
        if status not in {"zugeordnet", "offen"}:
            continue
        data["saldo"] += amount
        if amount >= 0:
            data["einnahmen"] += amount
            data["einnahmen_count"] += 1
            if status == "offen":
                data["offene_einnahmen"] += amount
        else:
            data["ausgaben"] += abs(amount)
            data["ausgaben_count"] += 1
            if status == "offen":
                data["offene_ausgaben"] += abs(amount)
    einnahmen = float(data.get("einnahmen") or 0)
    ausgaben = float(data.get("ausgaben") or 0)
    saldo = float(data.get("saldo") or 0)
    offene_einnahmen = float(data.get("offene_einnahmen") or 0)
    offene_ausgaben = float(data.get("offene_ausgaben") or 0)
    pruefen_amount = float(data.get("pruefen_amount") or 0)
    latest = dict(latest_import) if latest_import else {}
    result = {
        "hat_import": bool(latest_import),
        "import_id": int(latest.get("id") or 0),
        "datei": clean_text(latest.get("original_name")),
        "erstellt_am": clean_text(latest.get("erstellt_am")),
        "buchungen_count": int(data.get("buchungen_count") or 0),
        "einnahmen_count": int(data.get("einnahmen_count") or 0),
        "ausgaben_count": int(data.get("ausgaben_count") or 0),
        "zugeordnet_count": int(data.get("zugeordnet_count") or 0),
        "offen_count": int(data.get("offen_count") or 0),
        "pruefen_count": int(data.get("pruefen_count") or 0),
        "duplikat_count": int(data.get("duplikat_count") or 0),
        "einnahmen_amount": einnahmen,
        "ausgaben_amount": ausgaben,
        "umsatz_amount": saldo,
        "offene_einnahmen_amount": offene_einnahmen,
        "offene_ausgaben_amount": offene_ausgaben,
        "pruefen_amount": pruefen_amount,
    }
    result["einnahmen_label"] = lexware_amount_label(einnahmen)
    result["ausgaben_label"] = lexware_amount_label(ausgaben)
    result["umsatz_label"] = lexware_amount_label(saldo)
    result["offene_einnahmen_label"] = lexware_amount_label(offene_einnahmen)
    result["offene_ausgaben_label"] = lexware_amount_label(offene_ausgaben)
    result["pruefen_label"] = lexware_amount_label(pruefen_amount)
    result["umsatz_class"] = "text-success" if saldo >= 0 else "text-danger"
    return result


def admin_rechnungen_count():
    try:
        summary = lexware_rechnungen_summary()
        return int(summary["offen_count"] + summary["ueberfaellig_count"])
    except Exception:
        return 0


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
    description = clean_text(rechnung.get("lexware_beschreibung")) or build_invoice_lexware_description(
        rechnung.get("belegtext"),
        rechnung.get("bonus_text"),
    )
    remark = clean_text(rechnung.get("bonus_remark")) or "Vielen Dank für Ihren Auftrag."
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
        "remark": remark,
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
    mark_auftraege_reklamationsstatus(auftraege)
    auftraege.sort(key=auftrag_planung_sort_key)
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
        "SELECT * FROM dateien WHERE auftrag_id=? ORDER BY hochgeladen_am DESC, id DESC",
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
        SELECT *
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

    if notiz:
        try:
            apply_document_data_to_auftrag(auftrag["id"], prefer_documents=False)
        except Exception:
            pass

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
    datei["notiz"] = clean_text(datei.get("notiz"))
    datei["has_extract"] = bool(
        clean_text(datei.get("extrakt_kurz")) or clean_text(datei.get("extrahierter_text"))
        or clean_text(datei.get("notiz"))
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
        SELECT id, original_name, dokument_typ, notiz, extrahierter_text, analyse_json,
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
        review_text = append_upload_note_to_analysis(
            datei.get("extrahierter_text"),
            datei.get("notiz"),
        )
        local_felder = parse_document_fields(
            review_text,
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
        SELECT original_name, notiz, extrahierter_text, analyse_json, analyse_hinweis
        FROM dateien
        WHERE auftrag_id=? AND (extrahierter_text != '' OR analyse_json != '' OR notiz != '')
        ORDER BY id DESC
        """,
        (auftrag_id,),
    ).fetchall()

    erkannt = {}
    for datei in dateien:
        ai_felder = load_saved_analysis_json(datei["analyse_json"])
        review_text = append_upload_note_to_analysis(datei["extrahierter_text"], datei["notiz"])
        local_felder = parse_document_fields(review_text, datei["original_name"])
        felder = merge_document_fields(ai_felder, local_felder)
        felder = ensure_document_review_fallback(
            felder,
            review_text,
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


def add_rahmenvertrag_anfrage(autohaus_id, nachricht=""):
    autohaus_id = int(autohaus_id or 0)
    if autohaus_id <= 0:
        return None, False
    db = get_db()
    existing = db.execute(
        """
        SELECT id
        FROM rahmenvertrag_anfragen
        WHERE autohaus_id=? AND status='offen'
        ORDER BY id DESC
        LIMIT 1
        """,
        (autohaus_id,),
    ).fetchone()
    if existing:
        db.close()
        return int(existing["id"]), False
    cursor = db.execute(
        """
        INSERT INTO rahmenvertrag_anfragen
        (autohaus_id, status, nachricht, erstellt_am)
        VALUES (?, 'offen', ?, ?)
        """,
        (
            autohaus_id,
            clean_text(nachricht)
            or "Das Autohaus möchte einen Rahmenvertrag und mögliche Bonusstufen besprechen.",
            now_str(),
        ),
    )
    db.commit()
    request_id = cursor.lastrowid
    db.close()
    return request_id, True


def list_benachrichtigungen(auftrag_id, limit=20, nur_ungelesen=False):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM benachrichtigungen
        WHERE auftrag_id=?
          AND (?=0 OR COALESCE(gelesen, 0)=0)
        ORDER BY id DESC
        LIMIT ?
        """,
        (auftrag_id, 1 if nur_ungelesen else 0, limit),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def list_autohaus_benachrichtigungen(autohaus_id, limit=10):
    db = get_db()
    rows = db.execute(
        """
        SELECT b.*, a.fahrzeug, a.kennzeichen, a.auftragsnummer, a.angebotsphase
        FROM benachrichtigungen b
        JOIN auftraege a ON a.id = b.auftrag_id
        WHERE a.autohaus_id=? AND a.archiviert=0 AND COALESCE(b.gelesen, 0)=0
        ORDER BY b.id DESC
        LIMIT ?
        """,
        (autohaus_id, limit),
    ).fetchall()
    db.close()
    hidden = postfach_hidden_keys("autohaus", autohaus_id)
    return [
        dict(row)
        for row in rows
        if f"autohaus-hinweis-{row['id']}" not in hidden
    ]


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


def normalize_whatsapp_number(value):
    text = clean_text(value)
    if text.lower().startswith("whatsapp:"):
        text = text.split(":", 1)[1]
    text = re.sub(r"[^\d+]", "", text)
    if text.startswith("00"):
        text = f"+{text[2:]}"
    if text and not text.startswith("+"):
        text = f"+{text}"
    return text


def whatsapp_number_key(value):
    return re.sub(r"\D", "", normalize_whatsapp_number(value))


def whatsapp_workshop_number_keys():
    raw_numbers = re.split(r"[,;\s]+", WHATSAPP_WORKSHOP_NUMBERS or "")
    return {
        whatsapp_number_key(number)
        for number in raw_numbers
        if whatsapp_number_key(number)
    }


def whatsapp_workshop_numbers():
    raw_numbers = re.split(r"[,;\s]+", WHATSAPP_WORKSHOP_NUMBERS or "")
    numbers = []
    seen = set()
    for number in raw_numbers:
        normalized = normalize_whatsapp_number(number)
        key = whatsapp_number_key(normalized)
        if key and key not in seen:
            numbers.append(normalized)
            seen.add(key)
    return numbers


def whatsapp_graph_version():
    version = clean_text(WHATSAPP_GRAPH_VERSION) or "v25.0"
    if not version.startswith("v"):
        version = f"v{version}"
    return version


def whatsapp_bridge_enabled():
    return bool(
        WHATSAPP_ENABLED
        and WHATSAPP_ACCESS_TOKEN
        and WHATSAPP_PHONE_NUMBER_ID
        and whatsapp_workshop_number_keys()
    )


def whatsapp_message_template_enabled():
    return bool(WHATSAPP_NOTIFICATION_TEMPLATE)


def whatsapp_payload_phone(value):
    return whatsapp_number_key(value)


def whatsapp_order_code(auftrag_id):
    try:
        return f"#A{int(auftrag_id)}"
    except (TypeError, ValueError):
        return "#A0"


def whatsapp_auftrag_label(auftrag):
    if not auftrag:
        return "Unbekannter Auftrag"
    details = []
    kennzeichen = clean_text(auftrag.get("kennzeichen"))
    fahrzeug = clean_text(auftrag.get("fahrzeug"))
    auftragsnummer = clean_text(auftrag.get("auftragsnummer"))
    autohaus_name = clean_text(auftrag.get("autohaus_name"))
    if kennzeichen:
        details.append(kennzeichen)
    if fahrzeug:
        details.append(fahrzeug)
    if auftragsnummer:
        details.append(f"Auftrag {auftragsnummer}")
    if autohaus_name:
        details.append(autohaus_name)
    label = " / ".join(details)
    return label or f"Auftrag {auftrag.get('id')}"


def build_whatsapp_chat_notification(auftrag, nachricht, absender_label="Autohaus"):
    auftrag_id = int((auftrag or {}).get("id") or 0)
    lines = [
        "Neue Portal-Nachricht",
        f"Kennung: {whatsapp_order_code(auftrag_id)}",
        f"Von: {clean_text(absender_label) or 'Autohaus'}",
        f"Fahrzeug: {whatsapp_auftrag_label(auftrag)}",
        "",
        clean_text(nachricht)[:1800],
        "",
        "Bitte direkt auf diese WhatsApp antworten. Die Antwort wird im Portal-Chat gespeichert.",
    ]
    return "\n".join(lines).strip()


def build_whatsapp_template_payload(to_phone, auftrag, nachricht, absender_label="Autohaus"):
    auftrag_id = int((auftrag or {}).get("id") or 0)
    parameters = [
        {"type": "text", "text": whatsapp_order_code(auftrag_id)},
        {"type": "text", "text": whatsapp_auftrag_label(auftrag)[:250]},
        {"type": "text", "text": (clean_text(absender_label) or "Autohaus")[:80]},
        {"type": "text", "text": clean_text(nachricht)[:900]},
    ]
    return {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": whatsapp_payload_phone(to_phone),
        "type": "template",
        "template": {
            "name": WHATSAPP_NOTIFICATION_TEMPLATE,
            "language": {"code": WHATSAPP_TEMPLATE_LANGUAGE or "de"},
            "components": [{"type": "body", "parameters": parameters}],
        },
    }


def build_whatsapp_text_payload(to_phone, body):
    return {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": whatsapp_payload_phone(to_phone),
        "type": "text",
        "text": {"preview_url": False, "body": clean_text(body)[:4000]},
    }


def post_whatsapp_payload(payload):
    requests_module = get_requests()
    if not requests_module:
        return False, "", "Python-Modul requests ist nicht verfügbar."
    url = (
        f"https://graph.facebook.com/{whatsapp_graph_version()}/"
        f"{WHATSAPP_PHONE_NUMBER_ID}/messages"
    )
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        response = requests_module.post(url, headers=headers, json=payload, timeout=12)
    except Exception as exc:
        return False, "", f"WhatsApp-Versand fehlgeschlagen: {exc}"

    try:
        data = response.json()
    except Exception:
        data = {}
    message_id = ""
    messages = data.get("messages") if isinstance(data, dict) else None
    if messages:
        message_id = clean_text(messages[0].get("id"))
    if 200 <= response.status_code < 300 and message_id:
        return True, message_id, ""
    error = ""
    if isinstance(data, dict):
        raw_error = data.get("error") or {}
        error = clean_text(raw_error.get("message") or raw_error.get("error_user_msg"))
    return False, message_id, error or f"WhatsApp API Status {response.status_code}"


def record_whatsapp_message(
    auftrag_id,
    chat_id=0,
    richtung="outbound",
    telefon="",
    provider_message_id="",
    provider_context_id="",
    nachricht="",
    status="",
    fehler="",
    payload=None,
):
    try:
        auftrag_id = int(auftrag_id or 0)
    except (TypeError, ValueError):
        auftrag_id = 0
    if auftrag_id <= 0:
        return None
    payload_json = ""
    if payload is not None:
        try:
            payload_json = json.dumps(payload, ensure_ascii=False)[:12000]
        except (TypeError, ValueError):
            payload_json = ""
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO whatsapp_nachrichten
          (auftrag_id, chat_id, richtung, telefon, provider_message_id,
           provider_context_id, nachricht, status, fehler, payload_json, erstellt_am)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            auftrag_id,
            int(chat_id or 0),
            clean_text(richtung) or "outbound",
            normalize_whatsapp_number(telefon),
            clean_text(provider_message_id)[:255],
            clean_text(provider_context_id)[:255],
            clean_text(nachricht)[:4000],
            clean_text(status)[:80],
            clean_text(fehler)[:500],
            payload_json,
            now_str(),
        ),
    )
    db.commit()
    row_id = cursor.lastrowid
    db.close()
    return row_id


def whatsapp_message_exists(provider_message_id):
    provider_message_id = clean_text(provider_message_id)
    if not provider_message_id:
        return False
    db = get_db()
    row = db.execute(
        "SELECT id FROM whatsapp_nachrichten WHERE provider_message_id=? LIMIT 1",
        (provider_message_id,),
    ).fetchone()
    db.close()
    return bool(row)


def find_whatsapp_context(provider_context_id):
    provider_context_id = clean_text(provider_context_id)
    if not provider_context_id:
        return None
    db = get_db()
    row = db.execute(
        """
        SELECT *
        FROM whatsapp_nachrichten
        WHERE provider_message_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (provider_context_id,),
    ).fetchone()
    db.close()
    return dict(row) if row else None


def find_recent_whatsapp_thread_for_phone(phone):
    phone_key = whatsapp_number_key(phone)
    if not phone_key:
        return None
    cutoff = datetime.now() - timedelta(hours=WHATSAPP_REPLY_WINDOW_HOURS)
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM whatsapp_nachrichten
        WHERE telefon LIKE ?
        ORDER BY id DESC
        LIMIT 20
        """,
        (f"%{phone_key}",),
    ).fetchall()
    db.close()
    for row in rows:
        item = dict(row)
        created = parse_postfach_datetime(item.get("erstellt_am"))
        if created >= cutoff:
            return item
    return None


def extract_whatsapp_order_id(text):
    match = re.search(r"(?:#A|auftrag\s*#?)\s*(\d+)", clean_text(text), flags=re.IGNORECASE)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return 0


def notify_workshop_whatsapp_for_chat(auftrag_id, chat_id, nachricht, absender_label="Autohaus"):
    if not whatsapp_bridge_enabled():
        return False
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        return False
    body = build_whatsapp_chat_notification(auftrag, nachricht, absender_label=absender_label)
    sent_any = False
    for target_number in whatsapp_workshop_numbers():
        if whatsapp_message_template_enabled():
            payload = build_whatsapp_template_payload(target_number, auftrag, nachricht, absender_label=absender_label)
        else:
            payload = build_whatsapp_text_payload(target_number, body)
        ok, provider_id, error = post_whatsapp_payload(payload)
        sent_any = sent_any or ok
        record_whatsapp_message(
            auftrag_id,
            chat_id=chat_id,
            richtung="outbound",
            telefon=target_number,
            provider_message_id=provider_id,
            nachricht=body,
            status="gesendet" if ok else "fehler",
            fehler=error,
            payload=payload if not ok else None,
        )
        if not ok:
            print(f"WARNUNG: WhatsApp-Benachrichtigung konnte nicht gesendet werden: {error}")
    return sent_any


def verify_whatsapp_signature():
    if not WHATSAPP_APP_SECRET:
        return True
    signature = clean_text(request.headers.get("X-Hub-Signature-256"))
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        WHATSAPP_APP_SECRET.encode("utf-8"),
        request.get_data(),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def whatsapp_inbound_text(message):
    message_type = clean_text(message.get("type"))
    if message_type == "text":
        return clean_text((message.get("text") or {}).get("body"))
    if message_type:
        return f"[WhatsApp-{message_type}]"
    return ""


def resolve_whatsapp_reply_auftrag_id(message, text, from_phone):
    context_id = clean_text((message.get("context") or {}).get("id"))
    context_row = find_whatsapp_context(context_id)
    if context_row:
        return int(context_row.get("auftrag_id") or 0), context_id

    explicit_id = extract_whatsapp_order_id(text)
    if explicit_id:
        return explicit_id, context_id

    recent_row = find_recent_whatsapp_thread_for_phone(from_phone)
    if recent_row:
        return int(recent_row.get("auftrag_id") or 0), context_id
    return 0, context_id


def handle_whatsapp_inbound_message(message):
    provider_id = clean_text(message.get("id"))
    if provider_id and whatsapp_message_exists(provider_id):
        return False

    from_phone = normalize_whatsapp_number(message.get("from"))
    if whatsapp_number_key(from_phone) not in whatsapp_workshop_number_keys():
        return False

    text = whatsapp_inbound_text(message)
    if not text:
        return False

    auftrag_id, context_id = resolve_whatsapp_reply_auftrag_id(message, text, from_phone)
    auftrag = get_auftrag(auftrag_id) if auftrag_id else None
    if not auftrag:
        print("WARNUNG: WhatsApp-Antwort konnte keinem Auftrag zugeordnet werden.")
        return False

    chat_id = add_chat_nachricht(auftrag_id, "werkstatt", text)
    add_benachrichtigung(
        auftrag_id,
        "Neue Chat-Nachricht",
        "Die Werkstatt hat per WhatsApp geantwortet.",
    )
    record_whatsapp_message(
        auftrag_id,
        chat_id=chat_id,
        richtung="inbound",
        telefon=from_phone,
        provider_message_id=provider_id,
        provider_context_id=context_id,
        nachricht=text,
        status="empfangen",
        payload=message,
    )
    return True


def process_whatsapp_webhook(payload):
    processed = 0
    entries = payload.get("entry") if isinstance(payload, dict) else []
    for entry in entries or []:
        for change in entry.get("changes") or []:
            value = change.get("value") or {}
            for message in value.get("messages") or []:
                if handle_whatsapp_inbound_message(message):
                    processed += 1
    return processed


def parse_postfach_datetime(value):
    cleaned = clean_text(value)
    if not cleaned:
        return datetime.min
    for fmt in (DATETIME_FMT, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return datetime.min


def postfach_excerpt(value, length=180):
    cleaned = clean_text(value)
    if len(cleaned) <= length:
        return cleaned
    return cleaned[: max(0, length - 1)].rstrip() + "…"


def postfach_hidden_keys(empfaenger, autohaus_id=0):
    empfaenger = clean_text(empfaenger) or "admin"
    autohaus_id = int(autohaus_id or 0)
    db = get_db()
    rows = db.execute(
        """
        SELECT item_key
        FROM postfach_ausblendungen
        WHERE empfaenger=? AND autohaus_id=?
        """,
        (empfaenger, autohaus_id),
    ).fetchall()
    db.close()
    return {clean_text(row["item_key"]) for row in rows}


def hide_postfach_item(empfaenger, item_key, autohaus_id=0):
    empfaenger = clean_text(empfaenger) or "admin"
    item_key = clean_text(item_key)
    if not item_key:
        return
    autohaus_id = int(autohaus_id or 0)
    db = get_db()
    db.execute(
        """
        INSERT INTO postfach_ausblendungen (empfaenger, autohaus_id, item_key, erstellt_am)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM postfach_ausblendungen
            WHERE empfaenger=? AND autohaus_id=? AND item_key=?
        )
        """,
        (
            empfaenger,
            autohaus_id,
            item_key,
            now_str(),
            empfaenger,
            autohaus_id,
            item_key,
        ),
    )
    db.commit()
    db.close()


def mark_autohaus_chat_gelesen_by_id(autohaus_id, chat_id):
    try:
        chat_id = int(chat_id or 0)
    except (TypeError, ValueError):
        return
    if chat_id <= 0:
        return
    db = get_db()
    db.execute(
        """
        UPDATE chat_nachrichten
        SET gelesen_autohaus=1
        WHERE id=?
          AND absender='werkstatt'
          AND auftrag_id IN (
            SELECT id FROM auftraege WHERE autohaus_id=?
          )
        """,
        (chat_id, int(autohaus_id or 0)),
    )
    db.commit()
    db.close()


def mark_autohaus_postfach_item_erledigt(autohaus_id, item_key):
    item_key = clean_text(item_key)
    hide_postfach_item("autohaus", item_key, autohaus_id)
    if item_key.startswith("autohaus-hinweis-"):
        raw_id = item_key.removeprefix("autohaus-hinweis-")
        if raw_id.isdigit():
            mark_autohaus_benachrichtigung_gelesen(autohaus_id, int(raw_id))
    elif item_key.startswith("autohaus-chat-"):
        raw_id = item_key.removeprefix("autohaus-chat-")
        if raw_id.isdigit():
            mark_autohaus_chat_gelesen_by_id(autohaus_id, int(raw_id))


def sort_postfach_items(items, limit=80):
    items.sort(
        key=lambda item: (
            parse_postfach_datetime(item.get("erstellt_am")),
            clean_text(item.get("item_key")),
        ),
        reverse=True,
    )
    return items[:limit]


def normalize_auftrag_search_key(value):
    return re.sub(r"[^0-9a-zA-ZäöüÄÖÜß]", "", clean_text(value)).lower()


def list_admin_auftrag_suche(query, limit=8):
    query = clean_text(query)[:80]
    if len(query) < 2:
        return []
    query_text = query.lower()
    query_key = normalize_auftrag_search_key(query)
    treffer = []
    for index, auftrag in enumerate(list_auftraege(include_archived=True, include_angebote=True)):
        suchfelder = [
            auftrag.get("fahrzeug"),
            auftrag.get("kennzeichen"),
            auftrag.get("auftragsnummer"),
            auftrag.get("fin_nummer"),
            auftrag.get("kunde_name"),
            auftrag.get("autohaus_name"),
        ]
        suchtext = " ".join(clean_text(value).lower() for value in suchfelder if clean_text(value))
        suchkey = " ".join(normalize_auftrag_search_key(value) for value in suchfelder if clean_text(value))
        kennzeichen_key = normalize_auftrag_search_key(auftrag.get("kennzeichen"))
        if not (query_text in suchtext or (query_key and query_key in suchkey)):
            continue

        if query_key and kennzeichen_key == query_key:
            score = 0
        elif query_key and kennzeichen_key.startswith(query_key):
            score = 1
        elif query_key and query_key in kennzeichen_key:
            score = 2
        elif query_text in clean_text(auftrag.get("fahrzeug")).lower():
            score = 3
        else:
            score = 4

        status = auftrag.get("status_meta") or {}
        meta = []
        if clean_text(auftrag.get("autohaus_name")):
            meta.append(clean_text(auftrag.get("autohaus_name")))
        if clean_text(auftrag.get("auftragsnummer")):
            meta.append(f"Auftrag {clean_text(auftrag.get('auftragsnummer'))}")
        if auftrag.get("archiviert"):
            meta.append("Archiv")
        treffer.append(
            (
                score,
                index,
                {
                    "id": auftrag["id"],
                    "fahrzeug": clean_text(auftrag.get("fahrzeug")) or "Fahrzeug",
                    "kennzeichen": clean_text(auftrag.get("kennzeichen")),
                    "autohaus": clean_text(auftrag.get("autohaus_name")),
                    "status": clean_text(status.get("label")) or "Offen",
                    "status_farbe": clean_text(status.get("farbe")) or "secondary",
                    "meta": " · ".join(meta),
                    "url": url_for("auftrag_detail", auftrag_id=auftrag["id"]),
                },
            )
        )
    treffer.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in treffer[:limit]]


def list_partner_auftrag_suche(autohaus, query, limit=8):
    query = clean_text(query)[:80]
    if len(query) < 2:
        return []
    query_text = query.lower()
    query_key = normalize_auftrag_search_key(query)
    treffer = []
    for index, auftrag in enumerate(
        list_auftraege(autohaus["id"], include_archived=True, include_angebote=True)
    ):
        suchfelder = [
            auftrag.get("fahrzeug"),
            auftrag.get("kennzeichen"),
            auftrag.get("auftragsnummer"),
            auftrag.get("fin_nummer"),
            auftrag.get("kunde_name"),
        ]
        suchtext = " ".join(clean_text(value).lower() for value in suchfelder if clean_text(value))
        suchkey = " ".join(normalize_auftrag_search_key(value) for value in suchfelder if clean_text(value))
        kennzeichen_key = normalize_auftrag_search_key(auftrag.get("kennzeichen"))
        if not (query_text in suchtext or (query_key and query_key in suchkey)):
            continue

        if query_key and kennzeichen_key == query_key:
            score = 0
        elif query_key and kennzeichen_key.startswith(query_key):
            score = 1
        elif query_key and query_key in kennzeichen_key:
            score = 2
        elif query_text in clean_text(auftrag.get("fahrzeug")).lower():
            score = 3
        else:
            score = 4

        status = auftrag.get("status_meta") or {}
        url_endpoint = "partner_angebot_detail" if auftrag.get("angebotsphase") else "partner_auftrag"
        meta = []
        if clean_text(auftrag.get("kunde_name")):
            meta.append(clean_text(auftrag.get("kunde_name")))
        if clean_text(auftrag.get("auftragsnummer")):
            meta.append(f"Auftrag {clean_text(auftrag.get('auftragsnummer'))}")
        if auftrag.get("archiviert"):
            meta.append("Archiv")
        if auftrag.get("angebotsphase"):
            meta.append("Angebotsanfrage")
        treffer.append(
            (
                score,
                index,
                {
                    "id": auftrag["id"],
                    "fahrzeug": clean_text(auftrag.get("fahrzeug")) or "Fahrzeug",
                    "kennzeichen": clean_text(auftrag.get("kennzeichen")),
                    "status": clean_text(status.get("label")) or "Offen",
                    "status_farbe": clean_text(status.get("farbe")) or "secondary",
                    "meta": " · ".join(meta),
                    "url": url_for(
                        url_endpoint,
                        slug=autohaus["slug"],
                        auftrag_id=auftrag["id"],
                    ),
                },
            )
        )
    treffer.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in treffer[:limit]]


def list_admin_postfach_items(limit=80):
    hidden = postfach_hidden_keys("admin", 0)
    items = []
    db = get_db()

    def admin_auftrag_detail_url(auftrag_id):
        if has_request_context():
            return url_for("auftrag_detail", auftrag_id=auftrag_id)
        return f"/admin/auftrag/{auftrag_id}"

    rows = db.execute(
        """
        SELECT c.id, c.auftrag_id, c.nachricht, c.erstellt_am,
               a.fahrzeug, a.kennzeichen, a.auftragsnummer, h.name AS autohaus_name
        FROM chat_nachrichten c
        JOIN auftraege a ON a.id = c.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE c.absender='autohaus' AND c.gelesen_admin=0 AND a.archiviert=0
        ORDER BY c.id DESC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        key = f"admin-chat-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Chat",
                "titel": "Neue Nachricht vom Autohaus",
                "nachricht": postfach_excerpt(row["nachricht"]),
                "erstellt_am": row["erstellt_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["auftrag_id"]),
            }
        )

    rows = db.execute(
        """
        SELECT a.id, a.fahrzeug, a.kennzeichen, a.auftragsnummer, a.analyse_text,
               a.beschreibung, a.geaendert_am, h.name AS autohaus_name
        FROM auftraege a
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE a.angebotsphase=1
          AND a.angebot_abgesendet=1
          AND a.angebot_status='angefragt'
          AND a.archiviert=0
        ORDER BY a.geaendert_am DESC, a.id DESC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        key = f"admin-angebot-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Anfrage",
                "titel": "Offene Angebotsanfrage",
                "nachricht": postfach_excerpt(row["analyse_text"] or row["beschreibung"] or "Das Autohaus wartet auf ein Werkstatt-Angebot."),
                "erstellt_am": row["geaendert_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["id"]),
            }
        )

    rows = db.execute(
        """
        SELECT a.id, a.fahrzeug, a.kennzeichen, a.analyse_text, a.beschreibung,
               a.erstellt_am, h.name AS autohaus_name
        FROM auftraege a
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE a.quelle='autohaus'
          AND a.angebotsphase=0
          AND a.archiviert=0
          AND a.status=1
        ORDER BY a.erstellt_am DESC, a.id DESC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        key = f"admin-auftrag-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Fahrzeug",
                "titel": "Neues Fahrzeug vom Autohaus",
                "nachricht": postfach_excerpt(row["analyse_text"] or row["beschreibung"] or "Neuer Auftrag wartet auf Prüfung."),
                "erstellt_am": row["erstellt_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["id"]),
            }
        )

    rows = db.execute(
        """
        SELECT v.id, v.auftrag_id, v.meldung, v.erstellt_am,
               a.fahrzeug, a.kennzeichen, h.name AS autohaus_name
        FROM verzoegerungen v
        JOIN auftraege a ON a.id = v.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE v.uebernommen=0 AND a.archiviert=0
        ORDER BY v.erstellt_am DESC, v.id DESC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        key = f"admin-verzoegerung-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Termin",
                "titel": "Terminänderung gemeldet",
                "nachricht": postfach_excerpt(row["meldung"]),
                "erstellt_am": row["erstellt_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["auftrag_id"]),
            }
        )

    rows = db.execute(
        """
        SELECT r.id, r.auftrag_id, r.meldung, r.erstellt_am,
               a.fahrzeug, a.kennzeichen, h.name AS autohaus_name
        FROM reklamationen r
        JOIN auftraege a ON a.id = r.auftrag_id
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE r.bearbeitet=0 AND a.archiviert=0
        ORDER BY r.erstellt_am DESC, r.id DESC
        LIMIT 80
        """
    ).fetchall()
    for row in rows:
        key = f"admin-reklamation-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Alarm",
                "titel": "Reklamation offen",
                "nachricht": postfach_excerpt(row["meldung"]),
                "erstellt_am": row["erstellt_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["auftrag_id"]),
            }
        )

    rows = db.execute(
        """
        SELECT a.id, a.fahrzeug, a.kennzeichen, a.analyse_hinweis, a.geaendert_am,
               h.name AS autohaus_name
        FROM auftraege a
        LEFT JOIN autohaeuser h ON h.id = a.autohaus_id
        WHERE a.analyse_pruefen=1
          AND a.analyse_werkstatt_geprueft=0
          AND a.archiviert=0
        ORDER BY a.geaendert_am DESC, a.id DESC
        LIMIT 80
        """
    ).fetchall()
    db.close()
    for row in rows:
        key = f"admin-dokument-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Prüfung",
                "titel": "Dokumentprüfung offen",
                "nachricht": postfach_excerpt(row["analyse_hinweis"] or "Erkannte Werte müssen von der Werkstatt geprüft werden."),
                "erstellt_am": row["geaendert_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": admin_auftrag_detail_url(row["id"]),
            }
        )

    db = get_db()
    rows = db.execute(
        """
        SELECT r.id, r.nachricht, r.erstellt_am, h.name AS autohaus_name
        FROM rahmenvertrag_anfragen r
        JOIN autohaeuser h ON h.id = r.autohaus_id
        WHERE r.status='offen'
        ORDER BY r.id DESC
        LIMIT 80
        """
    ).fetchall()
    db.close()
    for row in rows:
        key = f"admin-rahmenvertrag-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Partner",
                "titel": "Rahmenvertrag angefragt",
                "nachricht": postfach_excerpt(row["nachricht"] or RAHMENVERTRAG_TEXT),
                "erstellt_am": row["erstellt_am"],
                "autohaus_name": row["autohaus_name"],
                "fahrzeug": "",
                "kennzeichen": "",
                "ziel_url": url_for("betriebs_cockpit") if has_request_context() else "/admin/cockpit",
            }
        )

    return sort_postfach_items(items, limit)


def admin_postfach_count():
    try:
        return len(list_admin_postfach_items(limit=200))
    except Exception:
        return 0


def einkauf_file_allowed(filename):
    return pathlib.Path(filename or "").suffix.lower() in EINKAUF_UPLOAD_EXTENSIONS


def get_topcolor_email():
    try:
        configured = get_app_setting("TOPCOLOR_EMAIL", "")
    except Exception:
        configured = ""
    return clean_text(configured or TOPCOLOR_EMAIL)


def set_topcolor_email(value):
    set_app_setting("TOPCOLOR_EMAIL", clean_text(value))


EINKAUF_KATEGORIEN = (
    "Material",
    "Farbe / Lack",
    "Klarlack / Haerter",
    "Schleifmittel",
    "Klebeband / Abdeckung",
    "Politur / Finish",
    "Karosserieteile",
    "Verbrauchsmaterial",
    "Werkzeug",
    "Sonstiges",
)

EINKAUF_KATEGORIE_LABELS = {
    "Farbe / Lack": "Farbe",
    "Klarlack / Haerter": "Klarlack / Härter",
    "Schleifmittel": "Schleifpapier",
    "Klebeband / Abdeckung": "Abdecken / Kleben",
}

EINKAUF_KATEGORIE_ALIASES = {
    "farbe": "Farbe / Lack",
    "lack": "Farbe / Lack",
    "farbe lack": "Farbe / Lack",
    "basislack": "Farbe / Lack",
    "klarlack": "Klarlack / Haerter",
    "haerter": "Klarlack / Haerter",
    "harter": "Klarlack / Haerter",
    "klarlack haerter": "Klarlack / Haerter",
    "klarlack harter": "Klarlack / Haerter",
    "schleifpapier": "Schleifmittel",
    "schleifscheibe": "Schleifmittel",
    "schleifmittel": "Schleifmittel",
    "abdecken kleben": "Klebeband / Abdeckung",
    "klebeband": "Klebeband / Abdeckung",
    "abdeckung": "Klebeband / Abdeckung",
    "politur": "Politur / Finish",
    "finish": "Politur / Finish",
    "karosserieteile": "Karosserieteile",
    "verbrauchsmaterial": "Verbrauchsmaterial",
    "werkzeug": "Werkzeug",
    "sonstiges": "Sonstiges",
}

EINKAUF_ARTIKEL_STOP_WORDS = {
    "auf",
    "beleg",
    "betrag",
    "bitte",
    "brutto",
    "datum",
    "einzelpreis",
    "eur",
    "euro",
    "gesamt",
    "gesamtbetrag",
    "gesamtpreis",
    "iban",
    "kg",
    "kundennummer",
    "lt",
    "ltr",
    "mwst",
    "netto",
    "preis",
    "rabatt",
    "rechnung",
    "rechnungsnummer",
    "stk",
    "stueck",
    "summe",
    "ueber",
    "uebertrag",
    "ust",
    "von",
    "zahlung",
    "zahlen",
    "zahlbar",
    "zwischensumme",
}

EINKAUF_ARTIKEL_SKIP_TERMS = (
    "anzupassen",
    "bankverbindung",
    "bitte rechnung",
    "gesamtbetrag",
    "iban",
    "mehrwertsteuer",
    "rechnung ueber",
    "uebertrag",
    "zahlung",
    "zahlen",
    "zahlbar",
)

EINKAUF_VE_OPTIONEN = (
    "Stueck",
    "Dose",
    "Liter",
    "Set",
    "Rolle",
    "Karton",
    "Packung",
    "Meter",
)

EINKAUF_ANGEBOTSSTATUS = {
    "offen": "zu pruefen",
    "angefragt": "Angebot angefragt",
    "angebot_erhalten": "Angebot erhalten",
    "freigegeben": "freigegeben",
    "bestellt": "bestellt",
}

EINKAUF_RECHNUNG_PRUEF_STATUS = "rechnung_pruefen"

EINKAUF_VERGLEICH_ANBIETER = {
    "google-shopping": "google_shopping_url",
    "google": "google_suche_url",
    "bilder": "google_images_url",
    "idealo": "idealo_suche_url",
    "ebay": "ebay_suche_url",
}


def parse_positive_int(value, default=1):
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, 9999))


def normalize_einkauf_kategorie(value):
    raw = clean_text(value)
    if not raw:
        return "Material"
    for kategorie in EINKAUF_KATEGORIEN:
        if raw.lower() == kategorie.lower():
            return kategorie
    key = re.sub(r"[^a-z0-9]+", " ", normalize_document_text(raw)).strip()
    if key in EINKAUF_KATEGORIE_ALIASES:
        return EINKAUF_KATEGORIE_ALIASES[key]
    for alias, kategorie in EINKAUF_KATEGORIE_ALIASES.items():
        if alias in key:
            return kategorie
    return raw if raw in EINKAUF_KATEGORIEN else "Material"


def plausible_einkauf_product_text(product_name, article_number="", source_text=""):
    product = clean_text(product_name)
    if not product:
        return False
    normalized_product = normalize_document_text(product)
    normalized_source = normalize_document_text(source_text)
    combined = f"{normalized_product} {normalized_source}".strip()
    if any(term in combined for term in EINKAUF_ARTIKEL_SKIP_TERMS):
        return False
    if re.fullmatch(r"[\d\s,./%€$+-]+", product):
        return False

    words = [
        word
        for word in re.findall(r"[a-z][a-z0-9+-]{2,}", normalized_product)
        if word not in EINKAUF_ARTIKEL_STOP_WORDS and not re.fullmatch(r"\d+", word)
    ]
    has_article_number = bool(normalize_article_number(article_number))
    if not words and not has_article_number:
        return False
    if not words and has_article_number and normalized_product.startswith("artikel "):
        return True

    letters = len(re.findall(r"[A-Za-zÄÖÜäöüß]", product))
    noisy_chars = len(re.findall(r"[\d,./%€$+-]", product))
    if not has_article_number and noisy_chars > max(8, letters * 2):
        return False
    return True


def normalize_einkauf_status(value):
    status = clean_text(value).lower()
    return status if status in EINKAUF_ANGEBOTSSTATUS else "offen"


def parse_price_value(value):
    text = clean_text(value)
    if not text:
        return None
    normalized = text.replace(".", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", normalized)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def format_price_value(value):
    parsed = parse_price_value(value)
    if parsed is None:
        return clean_text(value)
    return f"{parsed:.2f} EUR".replace(".", ",")


def einkauf_form_payload(form):
    return {
        "lieferant": clean_text(form.get("lieferant")) or "Auto-Color / Topcolor",
        "kategorie": normalize_einkauf_kategorie(form.get("kategorie")),
        "artikelnummer": clean_text(form.get("artikelnummer")),
        "produkt_name": clean_text(form.get("produkt_name")),
        "produkt_beschreibung": clean_text(form.get("produkt_beschreibung")),
        "produktbild_url": clean_text(form.get("produktbild_url")),
        "titel": clean_text(form.get("titel")),
        "menge": clean_text(form.get("menge")),
        "ve": clean_text(form.get("ve")) or "Stueck",
        "stueckzahl": parse_positive_int(form.get("stueckzahl"), 1),
        "gebinde": clean_text(form.get("gebinde")),
        "auto_color_preis": clean_text(form.get("auto_color_preis")),
        "vergleich_preis": clean_text(form.get("vergleich_preis")),
        "vergleich_lieferant": clean_text(form.get("vergleich_lieferant")),
        "lieferzeit": clean_text(form.get("lieferzeit")),
        "preisquelle": clean_text(form.get("preisquelle")),
        "angebotsstatus": normalize_einkauf_status(form.get("angebotsstatus")),
        "angebotsnotiz": clean_text(form.get("angebotsnotiz")),
        "notiz": clean_text(form.get("notiz")),
    }


def extract_qr_text_from_image(path):
    suffix = pathlib.Path(path).suffix.lower()
    if suffix not in IMAGE_EXTENSIONS:
        return ""
    cv2_module = get_cv2()
    if cv2_module is None or not hasattr(cv2_module, "QRCodeDetector"):
        return ""
    try:
        image = cv2_module.imread(str(path))
        if image is None:
            return ""
        detector = cv2_module.QRCodeDetector()
        decoded = []
        if hasattr(detector, "detectAndDecodeMulti"):
            try:
                result = detector.detectAndDecodeMulti(image)
                ok = bool(result[0]) if result else False
                values = result[1] if len(result) > 1 else []
                if ok:
                    decoded.extend(clean_text(value) for value in values if clean_text(value))
            except Exception:
                decoded = []
        if not decoded:
            value, *_ = detector.detectAndDecode(image)
            if clean_text(value):
                decoded.append(clean_text(value))
        unique_values = list(dict.fromkeys(decoded))
        return "\n".join(unique_values)[:2000]
    except Exception:
        return ""


def is_placeholder_product_name(value):
    text = clean_text(value)
    if not text:
        return True
    if len(text) >= 24 and re.fullmatch(r"[a-f0-9-]+(?:_\d+)?", text.lower()):
        return True
    return bool(re.fullmatch(r"einkauf[-_][a-f0-9-]+", text.lower()))


def extract_line_matching(lines, pattern):
    for line in lines:
        if re.search(pattern, line, re.I):
            return clean_text(line)
    return ""


def detect_einkauf_brand(lines, text):
    lowered = normalize_document_text(text)
    if "mipa" in lowered:
        return "Mipa"
    if "3m" in lowered or any(line.strip().lower().startswith("3m") for line in lines):
        return "3M"
    if "q-refinish" in lowered or ("refinish" in lowered and "30-100" in lowered):
        return "Q-Refinish"
    for line in lines[:3]:
        candidate = clean_text(line)
        if candidate and len(candidate) <= 30:
            return candidate
    return ""


def detect_einkauf_category(text):
    lowered = normalize_document_text(text)
    if any(token in lowered for token in ("abrasive", "schleif", "sanding", "velour", "p320", "p400", "p500", "p800", "15-hole", "15 hole")):
        return "Schleifmittel"
    if any(token in lowered for token in ("perfect-it", "perfect it", "politur", "polish", "compound", "fast cut")):
        return "Politur / Finish"
    if any(token in lowered for token in ("spachtel", "filler", "masilla", "multi star", "polyester")):
        return "Verbrauchsmaterial"
    if any(token in lowered for token in ("klarlack", "haerter", "härter")):
        return "Klarlack / Haerter"
    return "Material"


def extract_einkauf_article_number(lines, text, brand=""):
    patterns = (
        r"\bP/?N[:\s]*([A-Z0-9][A-Z0-9./_-]{2,})",
        r"\b(?:Art\.?|Artikel(?:nr|nummer)?|Bestell(?:nr|nummer)?|SKU)[:#\s-]*([A-Z0-9][A-Z0-9./_-]{2,})",
        r"\b(\d{2,4}-\d{3}-\d{4})\b",
        r"\b(51815[A-Z]?)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return clean_text(match.group(1)).upper()
    if brand.lower() == "mipa" and re.search(r"\bP\s*99\b", text, re.I):
        return "P99"
    return ""


def extract_einkauf_grit(text):
    match = re.search(r"P\s?(\d{2,4})\b", text, re.I)
    if match:
        return f"P{match.group(1)}"
    return ""


def extract_einkauf_dimension(text):
    match = re.search(r"\b(75|125|150|180)\s*mm\b", text, re.I)
    if match:
        return f"{match.group(1)} mm"
    return ""


def extract_einkauf_packaging(text):
    text = clean_text(text)
    match = re.search(r"\b(?:Box|Packung|Pack|Inhalt)[:\s]*(\d{1,4})\s*(?:pcs\.?|stk\.?|stueck|stück)\b", text, re.I)
    if match:
        return f"{match.group(1)} Stueck"
    match = re.search(r"\b(\d{1,4})\s*(?:pcs\.?|stk\.?|stueck|stück)\b", text, re.I)
    if match and int(match.group(1)) > 1:
        return f"{match.group(1)} Stueck"
    match = re.search(r"\b(\d+(?:[.,]\d+)?)\s*(kg|l|ml)\b", text, re.I)
    if match:
        return f"{match.group(1).replace('.', ',')} {match.group(2).lower()}"
    return ""


def build_einkauf_product_name_from_text(lines, text, brand, article_number):
    lowered = normalize_document_text(text)
    grit = extract_einkauf_grit(text)
    dimension = extract_einkauf_dimension(text)
    if brand == "Q-Refinish" or "30-100" in lowered:
        parts = ["Q-Refinish", "30-100", "Abrasive Disc Premium Gold"]
        if grit:
            parts.append(grit)
        if dimension:
            parts.append(dimension)
        if "15" in lowered and ("hole" in lowered or "loch" in lowered or "huller" in lowered):
            parts.append("15-Loch")
        return " ".join(dict.fromkeys(parts))
    if brand == "Mipa" and ("p99" in lowered or "multi star" in lowered):
        return "Mipa P 99 Multi Star PE-Universalspachtel"
    if brand == "3M" and ("perfect" in lowered or article_number.startswith("51815")):
        return "3M Perfect-It Fast Cut Plus Extreme 51815"
    if brand == "3M" and grit:
        return f"3M Schleifscheibe {grit}"

    ignored = {
        "made in eu",
        "batch",
        "batch.no.",
        "use by",
        "hdpe",
        "bombola",
        "chiusura",
    }
    candidates = []
    for line in lines:
        clean = clean_text(line)
        normalized = normalize_document_text(clean)
        if not clean or normalized in ignored:
            continue
        if re.fullmatch(r"[\d\s./:-]+", clean):
            continue
        if clean.upper() == article_number:
            continue
        candidates.append(clean)
    if brand and candidates and candidates[0].lower() == brand.lower():
        candidates = candidates[1:]
    result = " ".join(candidates[:3]).strip()
    if brand and brand.lower() not in result.lower():
        result = f"{brand} {result}".strip()
    return result[:160] or article_number or "Material / Produkt"


def analyse_einkauf_product_text(text, filename=""):
    text = clean_text(text)
    lines = [clean_text(line) for line in text.splitlines() if clean_text(line)]
    if not lines and clean_text(filename):
        lines = [pathlib.Path(filename).stem]
        text = lines[0]
    brand = detect_einkauf_brand(lines, text)
    article_number = extract_einkauf_article_number(lines, text, brand)
    product_name = build_einkauf_product_name_from_text(lines, text, brand, article_number)
    category = detect_einkauf_category(text)
    if article_number.startswith("51815"):
        category = "Politur / Finish"
    elif brand == "3M" and extract_einkauf_grit(text):
        category = "Schleifmittel"
    packaging = extract_einkauf_packaging(text)
    ve = "Stueck"
    gebinde = packaging
    if category == "Schleifmittel" and packaging:
        ve = "Packung"
    elif category in {"Politur / Finish", "Verbrauchsmaterial", "Klarlack / Haerter"}:
        ve = "Dose"
    note_parts = []
    if brand:
        note_parts.append(f"Hersteller erkannt: {brand}")
    if text:
        note_parts.append("Etikett lokal gelesen.")
    return {
        "lieferant": "Auto-Color / Topcolor",
        "kategorie": category,
        "artikelnummer": article_number,
        "produkt_name": product_name,
        "titel": product_name,
        "produkt_beschreibung": text[:3000],
        "ve": ve,
        "stueckzahl": 1,
        "gebinde": gebinde,
        "notiz": " ".join(note_parts),
        "qr_text": text[:2000],
    }


def build_einkauf_item_search_query(item):
    parts = []
    for key in ("artikelnummer", "produkt_name", "gebinde"):
        value = clean_text(item.get(key))
        if value and value not in parts:
            parts.append(value)
    category = clean_text(item.get("kategorie"))
    if category == "Schleifmittel":
        parts.append("Schleifscheibe Autolack")
    elif category == "Politur / Finish":
        parts.append("Politur Autolack")
    elif category == "Verbrauchsmaterial":
        parts.append("Autolack Verbrauchsmaterial")
    query = " ".join(parts).strip()
    return compact_whitespace(query) or clean_text(item.get("titel")) or "Autolack Material"


def is_high_value_einkauf_item(item, auto_price=None):
    category = clean_text(item.get("kategorie"))
    name = normalize_document_text(item.get("produkt_name") or item.get("titel"))
    if category in {"Farbe / Lack", "Klarlack / Haerter"}:
        return True
    if any(token in name for token in ("envirobase", "ppg", "deltron", "basislack", "klarlack", "haerter")):
        return True
    return auto_price is not None and auto_price >= 150


def build_einkauf_preisradar(item, auto_price=None, compare_price=None):
    auto_price = parse_price_value(item.get("auto_color_preis")) if auto_price is None else auto_price
    compare_price = parse_price_value(item.get("vergleich_preis")) if compare_price is None else compare_price
    has_auto = auto_price is not None
    has_compare = compare_price is not None
    high_value = is_high_value_einkauf_item(item, auto_price)
    if has_auto and has_compare:
        cheaper = min(auto_price, compare_price)
        expensive = max(auto_price, compare_price)
        span = expensive - cheaper
        percent = (span / cheaper * 100) if cheaper > 0 else 0
        span_value_label = f"{span:.2f} EUR".replace(".", ",")
        if auto_price > compare_price and span >= 5 and percent >= 5:
            return {
                "status": "danger",
                "badge": "Bitte prüfen",
                "title": "Vergleich ist günstiger",
                "text": (
                    f"Auto-Color liegt {span_value_label} bzw. {percent:.0f}% über dem Vergleich. "
                    "Vor Bestellung Kondition prüfen oder beim Lieferanten nachverhandeln."
                ),
                "span_label": f"{cheaper:.2f} bis {expensive:.2f} EUR".replace(".", ","),
                "needs_review": True,
            }
        if high_value:
            return {
                "status": "warning",
                "badge": "Kondition",
                "title": "Teurer Lack-/Materialartikel",
                "text": "Preis ist erfasst. Bei Farbe, Lack und Hochpreisartikeln lohnt sich eine feste Kondition mit Topcolor.",
                "span_label": f"{cheaper:.2f} bis {expensive:.2f} EUR".replace(".", ","),
                "needs_review": True,
            }
        return {
            "status": "success",
            "badge": "Verglichen",
            "title": "Vergleichspreis vorhanden",
            "text": "Preis und Vergleich sind hinterlegt. Die Position kann fachlich geprüft werden.",
            "span_label": f"{cheaper:.2f} bis {expensive:.2f} EUR".replace(".", ","),
            "needs_review": False,
        }
    if high_value and has_auto:
        return {
            "status": "warning",
            "badge": "Kondition",
            "title": "Vergleich noch offen",
            "text": "Teurer Lack-/Materialartikel ohne Vergleichspreis. Bitte Webpreise prüfen und Konditionen anfragen.",
            "span_label": "",
            "needs_review": True,
        }
    if has_auto and not has_compare:
        return {
            "status": "info",
            "badge": "Web prüfen",
            "title": "Vergleichspreis fehlt",
            "text": "Preis ist aus der Rechnung erkannt. Für echtes Preisradar einen Web-/Lieferantenvergleich eintragen.",
            "span_label": "",
            "needs_review": False,
        }
    return {
        "status": "muted",
        "badge": "offen",
        "title": "Preis noch offen",
        "text": "Noch kein belastbarer Preisvergleich gespeichert.",
        "span_label": "",
        "needs_review": False,
    }


def hydrate_einkauf_internet_links(item):
    query = build_einkauf_item_search_query(item)
    encoded = quote(query)
    item["internet_query"] = query
    item["google_suche_url"] = f"https://www.google.com/search?q={encoded}"
    item["google_shopping_url"] = f"https://www.google.com/search?tbm=shop&q={encoded}"
    item["google_images_url"] = f"https://www.google.com/search?tbm=isch&q={encoded}"
    item["idealo_suche_url"] = f"https://www.idealo.de/preisvergleich/MainSearchProductCategory.html?q={encoded}"
    item["ebay_suche_url"] = f"https://www.ebay.de/sch/i.html?_nkw={encoded}"
    return item


def hydrate_einkauf_item(row):
    item = dict(row)
    original_name = clean_text(item.get("original_name"))
    stored_name = clean_text(item.get("stored_name"))
    suffix = pathlib.Path(original_name or stored_name).suffix.lower()
    item["is_image"] = suffix in IMAGE_EXTENSIONS
    item["is_browser_image"] = suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    item["is_pdf"] = suffix == ".pdf"
    item["datei_url"] = (
        url_for("admin_einkauf_datei", item_id=item["id"])
        if stored_name and has_request_context()
        else ""
    )
    try:
        item["quelle_beleg_id"] = int(item.get("quelle_beleg_id") or 0)
    except (TypeError, ValueError):
        item["quelle_beleg_id"] = 0
    item["beleg_datei_url"] = (
        url_for("admin_einkauf_beleg_datei", beleg_id=item["quelle_beleg_id"])
        if item["quelle_beleg_id"] and has_request_context()
        else ""
    )
    item["detail_url"] = url_for("admin_einkauf_detail", item_id=item["id"]) if has_request_context() else ""
    if not clean_text(item.get("produkt_name")):
        item["produkt_name"] = clean_text(item.get("titel"))
    item["produkt_beschreibung"] = clean_text(item.get("produkt_beschreibung")) or clean_text(item.get("qr_text"))
    item["produktbild_url"] = clean_text(item.get("produktbild_url"))
    if not clean_text(item.get("kategorie")):
        item["kategorie"] = "Material"
    item["kategorie"] = normalize_einkauf_kategorie(item.get("kategorie"))
    if item["kategorie"] == "Material":
        inferred_category = detect_einkauf_category(
            " ".join(
                clean_text(part)
                for part in (
                    item.get("produkt_name"),
                    item.get("titel"),
                    item.get("produkt_beschreibung"),
                    item.get("gebinde"),
                )
                if clean_text(part)
            )
        )
        if inferred_category != "Material":
            item["kategorie"] = inferred_category
    item["kategorie_label"] = einkauf_kategorie_label(item["kategorie"])
    if not clean_text(item.get("ve")):
        item["ve"] = "Stueck"
    item["stueckzahl"] = parse_positive_int(item.get("stueckzahl"), 1)
    item["angebotsstatus"] = normalize_einkauf_status(item.get("angebotsstatus"))
    item["angebotsstatus_label"] = EINKAUF_ANGEBOTSSTATUS.get(item["angebotsstatus"], item["angebotsstatus"])
    try:
        item["quelle_email_id"] = int(item.get("quelle_email_id") or 0)
    except (TypeError, ValueError):
        item["quelle_email_id"] = 0
    auto_price = parse_price_value(item.get("auto_color_preis"))
    compare_price = parse_price_value(item.get("vergleich_preis"))
    item["auto_color_preis_label"] = format_price_value(item.get("auto_color_preis"))
    item["vergleich_preis_label"] = format_price_value(item.get("vergleich_preis"))
    item["angebot_preis_label"] = item["auto_color_preis_label"] or item["vergleich_preis_label"]
    item["preis_delta_label"] = ""
    item["preis_delta_class"] = "text-muted"
    if auto_price is not None and compare_price is not None:
        delta = compare_price - auto_price
        if abs(delta) >= 0.01:
            item["preis_delta_label"] = f"{abs(delta):.2f} EUR {'teurer' if delta > 0 else 'guenstiger'}".replace(".", ",")
            item["preis_delta_class"] = "text-success" if delta > 0 else "text-danger"
        else:
            item["preis_delta_label"] = "preisgleich"
    item["preisradar"] = build_einkauf_preisradar(item, auto_price, compare_price)
    item["preiswarnung"] = item["preisradar"]["needs_review"]
    item["preiswarnung_class"] = f"price-alert-{item['preisradar']['status']}"
    item = hydrate_einkauf_internet_links(item)
    source_file_image = item["datei_url"] if item["is_browser_image"] else ""
    item["produktbild_preview_url"] = source_file_image or item["produktbild_url"]
    item["produktbild_missing"] = not bool(item["produktbild_preview_url"])
    return item


def get_einkauf_item(item_id):
    db = get_db()
    row = db.execute("SELECT * FROM einkaufsliste WHERE id=?", (int(item_id),)).fetchone()
    db.close()
    return hydrate_einkauf_item(row) if row else None


def list_einkauf_items(status="offen", limit=200):
    status = clean_text(status) or "offen"
    limit = max(1, min(int(limit or 200), 500))
    db = get_db()
    if status == "alle":
        rows = db.execute(
            """
            SELECT * FROM einkaufsliste
            ORDER BY CASE WHEN status='offen' THEN 0 ELSE 1 END, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT * FROM einkaufsliste
            WHERE status=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (status, limit),
        ).fetchall()
    db.close()
    return [hydrate_einkauf_item(row) for row in rows]


def einkauf_item_is_orderable(item):
    product = item.get("produkt_name") or item.get("titel")
    source = item.get("produkt_beschreibung") or item.get("qr_text") or item.get("notiz")
    if plausible_einkauf_product_text(product, item.get("artikelnummer"), source):
        return True
    if clean_text(item.get("stored_name")) and not any(
        term in normalize_document_text(source or product)
        for term in EINKAUF_ARTIKEL_SKIP_TERMS
    ):
        return True
    return False


def split_einkauf_items_by_orderability(items):
    orderable = []
    unclear = []
    for item in items or []:
        if einkauf_item_is_orderable(item):
            orderable.append(item)
        else:
            unclear.append(item)
    return orderable, unclear


def admin_einkauf_count():
    try:
        offene_items, _ = split_einkauf_items_by_orderability(list_einkauf_items("offen", limit=500))
        return len(offene_items)
    except Exception:
        return 0


def save_einkauf_upload(file_storage):
    filename = clean_text(getattr(file_storage, "filename", ""))
    if not filename:
        return {
            "original_name": "",
            "stored_name": "",
            "mime_type": "",
            "size": 0,
            "qr_text": "",
            "analysis_text": "",
        }
    if not einkauf_file_allowed(filename):
        raise ValueError(f"{filename} ist kein erlaubtes Einkaufsbild oder PDF.")
    suffix = pathlib.Path(filename).suffix.lower()
    original_name = secure_filename(filename) or f"einkauf{suffix}"
    stored_name = f"einkauf-{uuid.uuid4().hex}{suffix}"
    target = UPLOAD_DIR / stored_name
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(target)
    mime_type = clean_text(getattr(file_storage, "mimetype", "")) or clean_text(
        mimetypes.guess_type(original_name)[0]
    )
    qr_text = extract_qr_text_from_image(target)
    analysis_text = clean_text(extract_document_text_local(target, original_name))
    combined_text = "\n".join(
        dict.fromkeys(
            clean_text(chunk)
            for chunk in (qr_text, analysis_text)
            if clean_text(chunk)
        )
    )
    return {
        "original_name": original_name,
        "stored_name": stored_name,
        "mime_type": mime_type,
        "size": target.stat().st_size if target.exists() else 0,
        "qr_text": combined_text[:3000],
        "analysis_text": analysis_text[:3000],
    }


def merge_einkauf_upload_analysis(payload, upload):
    text = clean_text(upload.get("qr_text") or upload.get("analysis_text"))
    if not text:
        return payload, upload
    parsed = analyse_einkauf_product_text(text, upload.get("original_name"))
    merged = dict(payload)
    if parsed.get("produkt_name") and (
        not clean_text(merged.get("produkt_name"))
        or is_placeholder_product_name(merged.get("produkt_name"))
    ):
        merged["produkt_name"] = parsed["produkt_name"]
    if parsed.get("titel") and (
        not clean_text(merged.get("titel"))
        or is_placeholder_product_name(merged.get("titel"))
    ):
        merged["titel"] = parsed["titel"]
    for key in ("artikelnummer", "gebinde"):
        if parsed.get(key) and not clean_text(merged.get(key)):
            merged[key] = parsed[key]
    if parsed.get("kategorie") and clean_text(merged.get("kategorie")) in {"", "Material"}:
        merged["kategorie"] = parsed["kategorie"]
    if parsed.get("ve") and clean_text(merged.get("ve")) in {"", "Stueck"}:
        merged["ve"] = parsed["ve"]
    if parsed.get("produkt_beschreibung") and not clean_text(merged.get("produkt_beschreibung")):
        merged["produkt_beschreibung"] = parsed["produkt_beschreibung"]
    if parsed.get("notiz") and not clean_text(merged.get("notiz")):
        merged["notiz"] = parsed["notiz"]
    upload = dict(upload)
    upload["qr_text"] = parsed.get("qr_text") or clean_text(upload.get("qr_text"))
    return merged, upload


def create_einkauf_item(
    titel="",
    menge="",
    notiz="",
    file_storage=None,
    lieferant="Topcolor",
    **fields,
):
    upload = save_einkauf_upload(file_storage) if file_storage else {
        "original_name": "",
        "stored_name": "",
        "mime_type": "",
        "size": 0,
        "qr_text": "",
        "analysis_text": "",
    }
    payload = {
        "lieferant": clean_text(fields.get("lieferant") or lieferant) or "Topcolor",
        "kategorie": normalize_einkauf_kategorie(fields.get("kategorie")),
        "artikelnummer": clean_text(fields.get("artikelnummer")),
        "produkt_name": clean_text(fields.get("produkt_name")),
        "produkt_beschreibung": clean_text(fields.get("produkt_beschreibung")),
        "produktbild_url": clean_text(fields.get("produktbild_url")),
        "titel": clean_text(fields.get("titel") or titel),
        "menge": clean_text(fields.get("menge") or menge),
        "ve": clean_text(fields.get("ve")) or "Stueck",
        "stueckzahl": parse_positive_int(fields.get("stueckzahl"), 1),
        "gebinde": clean_text(fields.get("gebinde")),
        "auto_color_preis": clean_text(fields.get("auto_color_preis")),
        "vergleich_preis": clean_text(fields.get("vergleich_preis")),
        "vergleich_lieferant": clean_text(fields.get("vergleich_lieferant")),
        "lieferzeit": clean_text(fields.get("lieferzeit")),
        "preisquelle": clean_text(fields.get("preisquelle")),
        "angebotsstatus": normalize_einkauf_status(fields.get("angebotsstatus")),
        "angebotsnotiz": clean_text(fields.get("angebotsnotiz")),
        "notiz": clean_text(fields.get("notiz") or notiz),
        "quelle_email_id": int(fields.get("quelle_email_id") or 0),
        "quelle_beleg_id": int(fields.get("quelle_beleg_id") or 0),
    }
    payload, upload = merge_einkauf_upload_analysis(payload, upload)
    titel = payload["titel"]
    qr_text = clean_text(upload.get("qr_text"))
    if not titel and upload.get("original_name"):
        titel = pathlib.Path(upload["original_name"]).stem
    if not titel and qr_text:
        titel = qr_text.splitlines()[0][:90]
    titel = titel or payload["produkt_name"] or "Material / QR-Code"
    produkt_name = payload["produkt_name"] or titel
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO einkaufsliste
          (lieferant, kategorie, artikelnummer, produkt_name, produkt_beschreibung, produktbild_url, titel, menge, ve,
           stueckzahl, gebinde, auto_color_preis, vergleich_preis, vergleich_lieferant,
           lieferzeit, preisquelle, angebotsstatus, angebotsnotiz, notiz, qr_text,
           quelle_email_id, quelle_beleg_id, status, original_name, stored_name, mime_type, size, erstellt_am, bestellt_am)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'offen', ?, ?, ?, ?, ?, '')
        """,
        (
            payload["lieferant"],
            payload["kategorie"],
            payload["artikelnummer"],
            produkt_name,
            payload["produkt_beschreibung"],
            payload["produktbild_url"],
            titel,
            payload["menge"],
            payload["ve"],
            payload["stueckzahl"],
            payload["gebinde"],
            payload["auto_color_preis"],
            payload["vergleich_preis"],
            payload["vergleich_lieferant"],
            payload["lieferzeit"],
            payload["preisquelle"],
            payload["angebotsstatus"],
            payload["angebotsnotiz"],
            payload["notiz"],
            qr_text,
            payload["quelle_email_id"],
            payload["quelle_beleg_id"],
            clean_text(upload.get("original_name")),
            clean_text(upload.get("stored_name")),
            clean_text(upload.get("mime_type")),
            int(upload.get("size") or 0),
            now_str(),
        ),
    )
    db.commit()
    item_id = cursor.lastrowid
    db.close()
    sync_einkauf_item_to_artikel(item_id, increment_count=1)
    schedule_change_backup("einkauf")
    return item_id


def update_einkauf_item(item_id, sync_artikel=True, **fields):
    existing = get_einkauf_item(item_id)
    if not existing:
        return False
    payload = {
        "lieferant": clean_text(fields.get("lieferant")) or "Auto-Color / Topcolor",
        "kategorie": normalize_einkauf_kategorie(fields.get("kategorie")),
        "artikelnummer": clean_text(fields.get("artikelnummer")),
        "produkt_name": clean_text(fields.get("produkt_name")),
        "produkt_beschreibung": clean_text(fields.get("produkt_beschreibung")) or clean_text(existing.get("produkt_beschreibung")),
        "produktbild_url": clean_text(fields.get("produktbild_url")) if "produktbild_url" in fields else clean_text(existing.get("produktbild_url")),
        "titel": clean_text(fields.get("titel")),
        "menge": clean_text(fields.get("menge")),
        "ve": clean_text(fields.get("ve")) or "Stueck",
        "stueckzahl": parse_positive_int(fields.get("stueckzahl"), 1),
        "gebinde": clean_text(fields.get("gebinde")),
        "auto_color_preis": clean_text(fields.get("auto_color_preis")),
        "vergleich_preis": clean_text(fields.get("vergleich_preis")),
        "vergleich_lieferant": clean_text(fields.get("vergleich_lieferant")),
        "lieferzeit": clean_text(fields.get("lieferzeit")),
        "preisquelle": clean_text(fields.get("preisquelle")),
        "angebotsstatus": normalize_einkauf_status(fields.get("angebotsstatus")),
        "angebotsnotiz": clean_text(fields.get("angebotsnotiz")),
        "notiz": clean_text(fields.get("notiz")),
    }
    payload["titel"] = payload["titel"] or payload["produkt_name"] or "Material / QR-Code"
    payload["produkt_name"] = payload["produkt_name"] or payload["titel"]
    db = get_db()
    db.execute(
        """
        UPDATE einkaufsliste
        SET lieferant=?, kategorie=?, artikelnummer=?, produkt_name=?, produkt_beschreibung=?, produktbild_url=?, titel=?,
            menge=?, ve=?, stueckzahl=?, gebinde=?, auto_color_preis=?,
            vergleich_preis=?, vergleich_lieferant=?, lieferzeit=?, preisquelle=?,
            angebotsstatus=?, angebotsnotiz=?, notiz=?
        WHERE id=?
        """,
        (
            payload["lieferant"],
            payload["kategorie"],
            payload["artikelnummer"],
            payload["produkt_name"],
            payload["produkt_beschreibung"],
            payload["produktbild_url"],
            payload["titel"],
            payload["menge"],
            payload["ve"],
            payload["stueckzahl"],
            payload["gebinde"],
            payload["auto_color_preis"],
            payload["vergleich_preis"],
            payload["vergleich_lieferant"],
            payload["lieferzeit"],
            payload["preisquelle"],
            payload["angebotsstatus"],
            payload["angebotsnotiz"],
            payload["notiz"],
            int(item_id),
        ),
    )
    db.commit()
    db.close()
    if sync_artikel and clean_text(existing.get("status")) != EINKAUF_RECHNUNG_PRUEF_STATUS:
        sync_einkauf_item_to_artikel(item_id, increment_count=0)
    schedule_change_backup("einkauf")
    return True


def mark_einkauf_item_bestellt(item_id):
    db = get_db()
    db.execute(
        "UPDATE einkaufsliste SET status='bestellt', angebotsstatus='bestellt', bestellt_am=? WHERE id=?",
        (now_str(), int(item_id)),
    )
    db.commit()
    db.close()
    schedule_change_backup("einkauf")


def mark_all_einkauf_items_bestellt():
    db = get_db()
    db.execute(
        "UPDATE einkaufsliste SET status='bestellt', angebotsstatus='bestellt', bestellt_am=? WHERE status='offen'",
        (now_str(),),
    )
    db.commit()
    db.close()
    schedule_change_backup("einkauf")


def delete_einkauf_item(item_id):
    item = get_einkauf_item(item_id)
    if not item:
        return False
    stored_name = clean_text(item.get("stored_name"))
    if stored_name:
        path = UPLOAD_DIR / pathlib.Path(stored_name).name
        move_upload_to_deleted_area(path, reason="einkauf")
    db = get_db()
    db.execute("DELETE FROM einkaufsliste WHERE id=?", (int(item_id),))
    db.commit()
    db.close()
    schedule_change_backup("einkauf")
    return True


def analyse_einkauf_item(item_id, force=False):
    item = get_einkauf_item(item_id)
    if not item:
        return {"ok": False, "message": "Einkaufsposition nicht gefunden.", "item": None}
    stored_name = clean_text(item.get("stored_name"))
    original_name = clean_text(item.get("original_name"))
    text = clean_text(item.get("qr_text"))
    if stored_name:
        path = UPLOAD_DIR / pathlib.Path(stored_name).name
        if path.exists() and path.is_file():
            text = clean_text(extract_document_text_local(path, original_name or stored_name)) or text
    if not text:
        return {"ok": False, "message": "Aus dem Produktbild konnte kein Etikett gelesen werden.", "item": item}

    parsed = analyse_einkauf_product_text(text, original_name or stored_name)
    existing_name = clean_text(item.get("produkt_name") or item.get("titel"))
    should_replace_name = force or is_placeholder_product_name(existing_name)
    merged = {
        "lieferant": clean_text(item.get("lieferant")) or parsed["lieferant"],
        "kategorie": normalize_einkauf_kategorie(
            parsed["kategorie"] if force or clean_text(item.get("kategorie")) in {"", "Material"} else item["kategorie"]
        ),
        "artikelnummer": parsed["artikelnummer"] if force or not clean_text(item.get("artikelnummer")) else item["artikelnummer"],
        "produkt_name": parsed["produkt_name"] if should_replace_name else item["produkt_name"],
        "produkt_beschreibung": parsed["produkt_beschreibung"] if force or not clean_text(item.get("produkt_beschreibung")) else item["produkt_beschreibung"],
        "titel": parsed["titel"] if should_replace_name else item["titel"],
        "menge": item.get("menge"),
        "ve": parsed["ve"] if force or clean_text(item.get("ve")) in {"", "Stueck"} else item["ve"],
        "stueckzahl": item.get("stueckzahl") or 1,
        "gebinde": parsed["gebinde"] if force or not clean_text(item.get("gebinde")) else item["gebinde"],
        "auto_color_preis": item.get("auto_color_preis"),
        "vergleich_preis": item.get("vergleich_preis"),
        "vergleich_lieferant": item.get("vergleich_lieferant"),
        "lieferzeit": item.get("lieferzeit"),
        "preisquelle": item.get("preisquelle"),
        "produktbild_url": item.get("produktbild_url"),
        "angebotsstatus": item.get("angebotsstatus"),
        "angebotsnotiz": item.get("angebotsnotiz"),
        "notiz": clean_text(item.get("notiz")) or parsed["notiz"],
    }
    update_einkauf_item(item_id, **merged)
    db = get_db()
    try:
        db.execute(
            "UPDATE einkaufsliste SET qr_text=?, produkt_beschreibung=? WHERE id=?",
            (parsed["qr_text"], merged["produkt_beschreibung"], int(item_id)),
        )
        db.commit()
    finally:
        db.close()
    schedule_change_backup("einkauf-analyse")
    updated = get_einkauf_item(item_id)
    return {"ok": True, "message": "Produktanalyse gespeichert.", "item": updated}


def analyse_offene_einkauf_items(force=False):
    results = []
    for item in list_einkauf_items("offen", limit=500):
        if not clean_text(item.get("stored_name")):
            continue
        results.append(analyse_einkauf_item(item["id"], force=force))
    ok_count = sum(1 for result in results if result.get("ok"))
    return ok_count, len(results)


def normalize_article_lookup(value):
    return compact_whitespace(clean_text(value)).lower()


def normalize_article_number(value):
    return re.sub(r"[^a-z0-9]", "", clean_text(value).lower())


def normalize_supplier_lookup(value):
    normalized = normalize_article_lookup(value)
    normalized = normalized.replace("–", "-").replace("—", "-")
    compact = re.sub(r"[^a-z0-9]", "", normalized)
    if compact in {"autocolortopcolor", "topcolor", "topcolour", "autocolor"}:
        return "auto-color / topcolor"
    return normalized


def normalize_product_lookup(value):
    normalized = normalize_document_text(value)
    normalized = re.sub(r"\b(artikel|art|nr|nummer|produkt|material)\b", " ", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return compact_whitespace(normalized)


def product_names_match(left, right):
    left_key = normalize_product_lookup(left)
    right_key = normalize_product_lookup(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    if len(left_key) < 8 or len(right_key) < 8:
        return False
    return SequenceMatcher(None, left_key, right_key).ratio() >= 0.92


def hydrate_einkauf_beleg(row):
    item = dict(row)
    original_name = clean_text(item.get("original_name"))
    stored_name = clean_text(item.get("stored_name"))
    suffix = pathlib.Path(original_name or stored_name).suffix.lower()
    item["is_image"] = suffix in IMAGE_EXTENSIONS
    item["is_browser_image"] = suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    item["is_pdf"] = suffix == ".pdf"
    item["datei_url"] = (
        url_for("admin_einkauf_beleg_datei", beleg_id=item["id"])
        if stored_name and has_request_context()
        else ""
    )
    item["positionen_count"] = int(item.get("positionen_count") or 0)
    item["lieferant"] = clean_text(item.get("lieferant")) or "Lieferant offen"
    item["status"] = clean_text(item.get("status")) or "importiert"
    return item


def list_einkauf_belege(limit=20):
    limit = max(1, min(int(limit or 20), 200))
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM einkauf_belege
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        db.close()
    return [hydrate_einkauf_beleg(row) for row in rows]


def get_einkauf_beleg(beleg_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM einkauf_belege WHERE id=?", (int(beleg_id),)).fetchone()
    finally:
        db.close()
    return hydrate_einkauf_beleg(row) if row else None


def einkauf_artikel_source_image_url(quelle_item_id):
    if not has_request_context():
        return ""
    try:
        source_id = int(quelle_item_id or 0)
    except (TypeError, ValueError):
        source_id = 0
    if not source_id:
        return ""
    db = get_db()
    try:
        row = db.execute(
            """
            SELECT original_name, stored_name
            FROM einkaufsliste
            WHERE id=?
            """,
            (source_id,),
        ).fetchone()
    finally:
        db.close()
    if not row:
        return ""
    stored_name = clean_text(row["stored_name"])
    if not stored_name:
        return ""
    original_name = clean_text(row["original_name"])
    suffix = pathlib.Path(original_name or stored_name).suffix.lower()
    if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return ""
    path = UPLOAD_DIR / pathlib.Path(stored_name).name
    if not path.exists() or not path.is_file():
        return ""
    return url_for("admin_einkauf_datei", item_id=source_id)


def hydrate_einkauf_artikel(row):
    item = dict(row)
    item["lieferant"] = clean_text(item.get("lieferant")) or "Lieferant offen"
    item["produkt_name"] = clean_text(item.get("produkt_name")) or "Material / Artikel"
    item["produktbild_url"] = clean_text(item.get("produktbild_url"))
    item["status"] = clean_text(item.get("status")) or "aktiv"
    item["kategorie"] = normalize_einkauf_kategorie(item.get("kategorie"))
    if item["kategorie"] == "Material":
        inferred_category = detect_einkauf_category(
            " ".join(
                clean_text(part)
                for part in (
                    item.get("produkt_name"),
                    item.get("produkt_beschreibung"),
                    item.get("gebinde"),
                )
                if clean_text(part)
            )
        )
        if inferred_category != "Material":
            item["kategorie"] = inferred_category
    item["kategorie_label"] = einkauf_kategorie_label(item["kategorie"])
    item["ve"] = clean_text(item.get("ve")) or "Stueck"
    item["nutzungen_count"] = int(item.get("nutzungen_count") or 0)
    item["letzter_preis_label"] = format_price_value(item.get("letzter_preis"))
    item = hydrate_einkauf_internet_links(item)
    item["detail_query"] = item["internet_query"]
    source_image_url = (
        einkauf_artikel_source_image_url(item.get("quelle_item_id"))
        if not item["produktbild_url"]
        else ""
    )
    item["produktbild_preview_url"] = item["produktbild_url"] or source_image_url
    item["produktbild_missing"] = not bool(item["produktbild_preview_url"])
    return item


def einkauf_artikel_is_plausible(item):
    return plausible_einkauf_product_text(
        item.get("produkt_name"),
        item.get("artikelnummer"),
        item.get("produkt_beschreibung") or item.get("preisquelle"),
    )


def list_einkauf_artikel(limit=200):
    limit = max(1, min(int(limit or 200), 500))
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM einkauf_artikel
            WHERE COALESCE(status, 'aktiv')!='verworfen'
            ORDER BY geaendert_am DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        db.close()
    artikel = [hydrate_einkauf_artikel(row) for row in rows]
    return [item for item in artikel if einkauf_artikel_is_plausible(item)]


def list_einkauf_artikel_altlasten(limit=200):
    limit = max(1, min(int(limit or 200), 500))
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM einkauf_artikel
            WHERE COALESCE(status, 'aktiv')!='verworfen'
            ORDER BY geaendert_am DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        db.close()
    artikel = [hydrate_einkauf_artikel(row) for row in rows]
    return [item for item in artikel if not einkauf_artikel_is_plausible(item)]


def count_verworfene_einkauf_artikel():
    db = get_db()
    try:
        row = db.execute(
            "SELECT COUNT(*) AS count FROM einkauf_artikel WHERE status='verworfen'"
        ).fetchone()
        return int(row["count"] or 0) if row else 0
    finally:
        db.close()


def get_einkauf_artikel(artikel_id):
    try:
        artikel_id = int(artikel_id or 0)
    except (TypeError, ValueError):
        artikel_id = 0
    if not artikel_id:
        return None
    db = get_db()
    try:
        row = db.execute("SELECT * FROM einkauf_artikel WHERE id=?", (artikel_id,)).fetchone()
    finally:
        db.close()
    if not row:
        return None
    artikel = hydrate_einkauf_artikel(row)
    return None if artikel.get("status") == "verworfen" else artikel


def einkauf_kategorie_label(kategorie):
    value = normalize_einkauf_kategorie(kategorie)
    return EINKAUF_KATEGORIE_LABELS.get(value, value)


def group_einkauf_artikel_by_kategorie(artikel_items):
    grouped = {}
    for artikel in artikel_items or []:
        kategorie = clean_text(artikel.get("kategorie")) or "Material"
        key = slugify(kategorie)
        if key not in grouped:
            grouped[key] = {
                "key": key,
                "kategorie": kategorie,
                "label": einkauf_kategorie_label(kategorie),
                "items": [],
            }
        grouped[key]["items"].append(artikel)

    order = {slugify(kategorie): index for index, kategorie in enumerate(EINKAUF_KATEGORIEN)}
    groups = list(grouped.values())
    groups.sort(
        key=lambda group: (
            order.get(group["key"], len(order) + 1),
            normalize_document_text(group["label"]),
        )
    )
    for group in groups:
        group["count"] = len(group["items"])
    return groups


def admin_einkauf_artikel_count():
    try:
        return len(list_einkauf_artikel(limit=500))
    except Exception:
        return 0


def mark_einkauf_artikel_verworfen(artikel_id):
    try:
        artikel_id = int(artikel_id or 0)
    except (TypeError, ValueError):
        return False
    if not artikel_id:
        return False
    db = get_db()
    try:
        cursor = db.execute(
            "UPDATE einkauf_artikel SET status='verworfen', geaendert_am=? WHERE id=?",
            (now_str(), artikel_id),
        )
        db.commit()
        changed = cursor.rowcount > 0
    finally:
        db.close()
    if changed:
        schedule_change_backup("einkauf-artikel-verworfen")
    return changed


def cleanup_einkauf_artikel_altlasten(limit=500):
    altlasten = list_einkauf_artikel_altlasten(limit=limit)
    if not altlasten:
        return 0
    ids = [int(item["id"]) for item in altlasten if item.get("id")]
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    db = get_db()
    try:
        db.execute(
            f"UPDATE einkauf_artikel SET status='verworfen', geaendert_am=? WHERE id IN ({placeholders})",
            [now_str(), *ids],
        )
        db.commit()
    finally:
        db.close()
    schedule_change_backup("einkauf-artikel-altlasten")
    return len(ids)


def find_existing_einkauf_artikel(db, lieferant, artikelnummer, produkt_name):
    lieferant_key = normalize_supplier_lookup(lieferant)
    artikelnummer_key = normalize_article_number(artikelnummer)
    produkt_key = normalize_product_lookup(produkt_name)
    if artikelnummer_key:
        row = db.execute(
            """
            SELECT *
            FROM einkauf_artikel
            WHERE replace(replace(replace(lower(artikelnummer), '-', ''), ' ', ''), '/', '')=?
              AND COALESCE(status, 'aktiv')!='verworfen'
            ORDER BY
              CASE WHEN lower(lieferant)=? THEN 0 ELSE 1 END,
              geaendert_am DESC,
              id DESC
            LIMIT 1
            """,
            (artikelnummer_key, lieferant_key),
        ).fetchone()
        if row:
            return row
    if produkt_key:
        rows = db.execute(
            """
            SELECT *
            FROM einkauf_artikel
            WHERE COALESCE(status, 'aktiv')!='verworfen'
            ORDER BY geaendert_am DESC, id DESC
            LIMIT 80
            """
        ).fetchall()
        for row in rows:
            if normalize_supplier_lookup(row["lieferant"]) == lieferant_key and product_names_match(row["produkt_name"], produkt_name):
                return row
        rows = db.execute(
            """
            SELECT *
            FROM einkauf_artikel
            WHERE COALESCE(status, 'aktiv')!='verworfen'
            ORDER BY geaendert_am DESC, id DESC
            LIMIT 200
            """
        ).fetchall()
        for row in rows:
            if product_names_match(row["produkt_name"], produkt_name):
                return row
    return None


def einkauf_item_has_artikelstamm_match(db, item):
    try:
        item_id = int(item.get("id") or 0)
    except (TypeError, ValueError):
        item_id = 0
    if item_id:
        row = db.execute(
            "SELECT id FROM einkauf_artikel WHERE quelle_item_id=? LIMIT 1",
            (item_id,),
        ).fetchone()
        if row:
            return True
    produkt_name = item.get("produkt_name") or item.get("titel")
    return bool(
        find_existing_einkauf_artikel(
            db,
            item.get("lieferant"),
            item.get("artikelnummer"),
            produkt_name,
        )
    )


def filter_rechnung_pruef_items_without_artikel(raw_items):
    items = [
        item
        for item in list(raw_items or [])
        if plausible_einkauf_product_text(
            item.get("produkt_name") or item.get("titel"),
            item.get("artikelnummer"),
            item.get("produkt_beschreibung") or item.get("qr_text"),
        )
    ]
    if not items:
        return []
    db = get_db()
    try:
        return [item for item in items if not einkauf_item_has_artikelstamm_match(db, item)]
    finally:
        db.close()


def merge_duplicate_einkauf_artikel(db):
    rows = [
        dict(row)
        for row in db.execute(
            "SELECT * FROM einkauf_artikel WHERE COALESCE(status, 'aktiv')!='verworfen' ORDER BY id ASC"
        ).fetchall()
    ]
    keep_by_number = {}
    keep_by_name = {}
    for row in rows:
        number_key = normalize_article_number(row.get("artikelnummer"))
        supplier_key = normalize_supplier_lookup(row.get("lieferant"))
        name_key = normalize_product_lookup(row.get("produkt_name"))
        duplicate_of = None
        if number_key and number_key in keep_by_number:
            duplicate_of = keep_by_number[number_key]
        elif supplier_key and name_key and (supplier_key, name_key) in keep_by_name:
            duplicate_of = keep_by_name[(supplier_key, name_key)]

        if not duplicate_of:
            if number_key:
                keep_by_number[number_key] = row
            if supplier_key and name_key:
                keep_by_name[(supplier_key, name_key)] = row
            continue

        target = duplicate_of
        target_id = int(target["id"])
        source_id = int(row["id"])
        payload = {
            "lieferant": clean_text(target.get("lieferant")) or clean_text(row.get("lieferant")),
            "kategorie": clean_text(target.get("kategorie")) or clean_text(row.get("kategorie")) or "Material",
            "artikelnummer": clean_text(target.get("artikelnummer")) or clean_text(row.get("artikelnummer")),
            "produkt_name": clean_text(target.get("produkt_name")) or clean_text(row.get("produkt_name")),
            "produkt_beschreibung": clean_text(target.get("produkt_beschreibung")) or clean_text(row.get("produkt_beschreibung")),
            "produktbild_url": clean_text(target.get("produktbild_url")) or clean_text(row.get("produktbild_url")),
            "ve": clean_text(target.get("ve")) or clean_text(row.get("ve")) or "Stueck",
            "gebinde": clean_text(target.get("gebinde")) or clean_text(row.get("gebinde")),
            "letzter_preis": clean_text(row.get("letzter_preis")) or clean_text(target.get("letzter_preis")),
            "letzter_preis_datum": clean_text(row.get("letzter_preis_datum")) or clean_text(target.get("letzter_preis_datum")),
            "preisquelle": clean_text(row.get("preisquelle")) or clean_text(target.get("preisquelle")),
            "quelle_beleg_id": int(row.get("quelle_beleg_id") or target.get("quelle_beleg_id") or 0),
            "quelle_item_id": int(row.get("quelle_item_id") or target.get("quelle_item_id") or 0),
            "nutzungen_count": int(target.get("nutzungen_count") or 0) + int(row.get("nutzungen_count") or 0),
        }
        db.execute(
            """
            UPDATE einkauf_artikel
            SET lieferant=?, kategorie=?, artikelnummer=?, produkt_name=?,
                produkt_beschreibung=?, produktbild_url=?, ve=?, gebinde=?,
                letzter_preis=?, letzter_preis_datum=?, preisquelle=?,
                quelle_beleg_id=?, quelle_item_id=?, nutzungen_count=?, status='aktiv', geaendert_am=?
            WHERE id=?
            """,
            (
                payload["lieferant"],
                payload["kategorie"],
                payload["artikelnummer"],
                payload["produkt_name"],
                payload["produkt_beschreibung"],
                payload["produktbild_url"],
                payload["ve"],
                payload["gebinde"],
                payload["letzter_preis"],
                payload["letzter_preis_datum"],
                payload["preisquelle"],
                payload["quelle_beleg_id"],
                payload["quelle_item_id"],
                payload["nutzungen_count"],
                now_str(),
                target_id,
            ),
        )
        db.execute(
            "UPDATE einkaufsliste SET quelle_item_id=? WHERE quelle_item_id=?",
            (target_id, source_id),
        )
        db.execute("DELETE FROM einkauf_artikel WHERE id=?", (source_id,))
        target.update(payload)


def upsert_einkauf_artikel(
    lieferant="",
    kategorie="Material",
    artikelnummer="",
    produkt_name="",
    produkt_beschreibung="",
    produktbild_url="",
    ve="Stueck",
    gebinde="",
    letzter_preis="",
    preisquelle="",
    quelle_beleg_id=0,
    quelle_item_id=0,
    increment_count=0,
):
    lieferant = clean_text(lieferant) or "Lieferant offen"
    kategorie = normalize_einkauf_kategorie(kategorie)
    produkt_name = clean_text(produkt_name)
    artikelnummer = clean_text(artikelnummer)
    if not produkt_name and artikelnummer:
        produkt_name = f"Artikel {artikelnummer}"
    if not produkt_name:
        return 0
    if not plausible_einkauf_product_text(produkt_name, artikelnummer, produkt_beschreibung or preisquelle):
        return 0
    now = now_str()
    price = clean_text(letzter_preis)
    db = get_db()
    try:
        existing = find_existing_einkauf_artikel(db, lieferant, artikelnummer, produkt_name)
        if existing:
            current = dict(existing)
            payload = {
                "lieferant": lieferant or current.get("lieferant"),
                "kategorie": normalize_einkauf_kategorie(kategorie or current.get("kategorie")),
                "artikelnummer": artikelnummer or clean_text(current.get("artikelnummer")),
                "produkt_name": produkt_name or clean_text(current.get("produkt_name")),
                "produkt_beschreibung": clean_text(produkt_beschreibung) or clean_text(current.get("produkt_beschreibung")),
                "produktbild_url": clean_text(produktbild_url) or clean_text(current.get("produktbild_url")),
                "ve": clean_text(ve) or clean_text(current.get("ve")) or "Stueck",
                "gebinde": clean_text(gebinde) or clean_text(current.get("gebinde")),
                "letzter_preis": price or clean_text(current.get("letzter_preis")),
                "letzter_preis_datum": date.today().strftime(DATE_FMT) if price else clean_text(current.get("letzter_preis_datum")),
                "preisquelle": clean_text(preisquelle) or clean_text(current.get("preisquelle")),
                "quelle_beleg_id": int(quelle_beleg_id or current.get("quelle_beleg_id") or 0),
                "quelle_item_id": int(quelle_item_id or current.get("quelle_item_id") or 0),
                "nutzungen_count": int(current.get("nutzungen_count") or 0) + int(increment_count or 0),
            }
            db.execute(
                """
                UPDATE einkauf_artikel
                SET lieferant=?, kategorie=?, artikelnummer=?, produkt_name=?, produkt_beschreibung=?, produktbild_url=?,
                    ve=?, gebinde=?, letzter_preis=?, letzter_preis_datum=?, preisquelle=?,
                    quelle_beleg_id=?, quelle_item_id=?, nutzungen_count=?, geaendert_am=?
                WHERE id=?
                """,
                (
                    payload["lieferant"],
                    payload["kategorie"],
                    payload["artikelnummer"],
                    payload["produkt_name"],
                    payload["produkt_beschreibung"],
                    payload["produktbild_url"],
                    payload["ve"],
                    payload["gebinde"],
                    payload["letzter_preis"],
                    payload["letzter_preis_datum"],
                    payload["preisquelle"],
                    payload["quelle_beleg_id"],
                    payload["quelle_item_id"],
                    payload["nutzungen_count"],
                    now,
                    existing["id"],
                ),
            )
            db.commit()
            return int(existing["id"])
        cursor = db.execute(
            """
            INSERT INTO einkauf_artikel
              (lieferant, kategorie, artikelnummer, produkt_name, produkt_beschreibung, produktbild_url,
               ve, gebinde, letzter_preis, letzter_preis_datum, preisquelle,
               quelle_beleg_id, quelle_item_id, nutzungen_count, status, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'aktiv', ?, ?)
            """,
            (
                lieferant,
                normalize_einkauf_kategorie(kategorie),
                artikelnummer,
                produkt_name,
                clean_text(produkt_beschreibung),
                clean_text(produktbild_url),
                clean_text(ve) or "Stueck",
                clean_text(gebinde),
                price,
                date.today().strftime(DATE_FMT) if price else "",
                clean_text(preisquelle),
                int(quelle_beleg_id or 0),
                int(quelle_item_id or 0),
                max(1, int(increment_count or 0)),
                now,
                now,
            ),
        )
        db.commit()
        return int(cursor.lastrowid)
    finally:
        db.close()


def sync_einkauf_item_to_artikel(item_id, increment_count=0):
    item = get_einkauf_item(item_id)
    if not item:
        return 0
    price = clean_text(item.get("auto_color_preis") or item.get("vergleich_preis"))
    return upsert_einkauf_artikel(
        lieferant=item.get("lieferant"),
        kategorie=item.get("kategorie"),
        artikelnummer=item.get("artikelnummer"),
        produkt_name=item.get("produkt_name") or item.get("titel"),
        produkt_beschreibung=item.get("produkt_beschreibung") or item.get("qr_text"),
        produktbild_url=item.get("produktbild_url"),
        ve=item.get("ve"),
        gebinde=item.get("gebinde"),
        letzter_preis=price,
        preisquelle=item.get("preisquelle") or item.get("original_name"),
        quelle_beleg_id=item.get("quelle_beleg_id") or 0,
        quelle_item_id=item.get("id") or 0,
        increment_count=increment_count,
    )


def save_einkauf_beleg_upload(file_storage, lieferant="", beleg_typ="rechnung"):
    filename = clean_text(getattr(file_storage, "filename", ""))
    if not filename:
        raise ValueError("Bitte eine Rechnung oder einen Beleg auswählen.")
    if not einkauf_file_allowed(filename):
        raise ValueError(f"{filename} ist kein erlaubter Einkaufsbeleg.")
    suffix = pathlib.Path(filename).suffix.lower()
    original_name = secure_filename(filename) or f"einkauf-beleg{suffix}"
    stored_name = f"einkauf-beleg-{uuid.uuid4().hex}{suffix}"
    target = UPLOAD_DIR / stored_name
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_storage.save(target)
    mime_type = clean_text(getattr(file_storage, "mimetype", "")) or clean_text(
        mimetypes.guess_type(original_name)[0]
    )
    qr_text = extract_qr_text_from_image(target)
    text = clean_text(extract_document_text_local(target, original_name))
    combined_text = "\n".join(
        dict.fromkeys(clean_text(chunk) for chunk in (qr_text, text) if clean_text(chunk))
    )
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO einkauf_belege
              (beleg_typ, lieferant, original_name, stored_name, mime_type, size,
               extrahierter_text, positionen_count, status, erstellt_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'importiert', ?)
            """,
            (
                clean_text(beleg_typ) or "rechnung",
                clean_text(lieferant) or "Auto-Color / Topcolor",
                original_name,
                stored_name,
                mime_type,
                target.stat().st_size if target.exists() else 0,
                combined_text[:10000],
                now_str(),
            ),
        )
        db.commit()
        beleg_id = cursor.lastrowid
    finally:
        db.close()
    return get_einkauf_beleg(beleg_id)


def clean_beleg_product_text(line, price_matches, article_number=""):
    text = clean_text(line)
    for match in reversed(price_matches):
        text = (text[: match.start()] + text[match.end() :]).strip()
    if article_number:
        text = re.sub(
            r"(?:artikel(?:nr|nummer)?|bestell(?:nr|nummer)?|art\.?|nr\.?)[:#\s-]*"
            + re.escape(article_number),
            "",
            text,
            flags=re.I,
        )
        text = re.sub(r"^" + re.escape(article_number) + r"\b", "", text, flags=re.I).strip()
    text = re.sub(
        r"\b\d+\s*(?:x|stk\.?|stck\.?|stück|stueck|dose|dosen|l|ltr|liter|rolle|rollen|karton|packung|set|pack)\b",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\b(?:netto|brutto|eur|€|preis|einzelpreis|gesamtpreis|rabatt)\b[:\s-]*", "", text, flags=re.I)
    text = re.sub(r"\s{2,}", " ", text).strip(" -|:\t")
    return text[:180]


def extract_article_number_from_beleg_line(line):
    article_re = re.compile(
        r"(?:artikel(?:nr|nummer)?|bestell(?:nr|nummer)?|art\.?|nr\.?)[:#\s-]*(?P<article>[A-Za-z0-9][A-Za-z0-9./_-]{2,})",
        re.I,
    )
    match = article_re.search(line)
    if match:
        return clean_text(match.group("article"))
    first_token = re.match(r"^\s*(?P<article>[A-Za-z0-9][A-Za-z0-9./_-]{2,})\s+", line)
    if first_token:
        article = clean_text(first_token.group("article"))
        if any(char.isdigit() for char in article) and not re.fullmatch(r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}", article):
            return article
    return ""


def extract_einkauf_beleg_positions(text, filename=""):
    raw_text = clean_text(text)
    if not raw_text:
        return []
    lines = [
        compact_whitespace(line.replace("\t", " "))
        for line in re.split(r"[\r\n;]+", raw_text)
        if compact_whitespace(line)
    ]
    positions = []
    seen = set()
    price_re = re.compile(r"\b\d{1,6}(?:[.,]\d{2})\b")
    amount_re = re.compile(
        r"\b(?P<count>\d{1,4})\s*(?P<unit>x|stk\.?|stck\.?|stück|stueck|dose|dosen|l|ltr|liter|rolle|rollen|karton|packung|set|pack)\b",
        re.I,
    )
    skip_terms = (
        "bankverbindung",
        "bitte rechnung",
        "summe",
        "zwischensumme",
        "gesamtbetrag",
        "uebertrag",
        "übertrag",
        "vortrag",
        "mehrwertsteuer",
        "mwst",
        "ust",
        "steuer",
        "versand",
        "porto",
        "zahlung",
        "zahlen",
        "zahlbar",
        "iban",
        "rechnungsnummer",
        "kundennummer",
        "anzupassen",
    )
    for line in lines:
        lowered = line.lower()
        price_matches = list(price_re.finditer(line))
        if not price_matches:
            continue
        if any(term in lowered for term in skip_terms):
            continue
        if not (re.search(r"(eur|€)", line, re.I) or len(price_matches) >= 2 or amount_re.search(line)):
            continue
        article_number = extract_article_number_from_beleg_line(line)
        amount_match = amount_re.search(line)
        count = parse_positive_int(amount_match.group("count"), 1) if amount_match else 1
        unit = normalize_offer_unit(amount_match.group("unit")) if amount_match else "Stueck"
        product = clean_beleg_product_text(line, price_matches, article_number)
        if not product:
            product = f"Artikel {article_number}" if article_number else pathlib.Path(filename).stem
        if not plausible_einkauf_product_text(product, article_number, line):
            continue
        if len(product) < 4 or not re.search(r"[A-Za-zÄÖÜäöü]", product):
            continue
        if re.fullmatch(r"[\d\s,./%-]+", product):
            continue
        price = clean_text(price_matches[-1].group(0))
        key = (article_number.lower(), normalize_article_lookup(product), price)
        if key in seen:
            continue
        seen.add(key)
        parsed_product = analyse_einkauf_product_text(f"{article_number}\n{product}", filename)
        positions.append(
            {
                "produkt_name": product[:180],
                "artikelnummer": article_number[:80],
                "stueckzahl": count,
                "ve": unit,
                "preis": price,
                "kategorie": parsed_product.get("kategorie") or "Material",
                "produkt_beschreibung": line[:500],
                "quelle": line[:240],
            }
        )
    return positions[:150]


def normalize_einkauf_invoice_ai_position(position, filename=""):
    if not isinstance(position, dict):
        return {}
    product = clean_text(position.get("produkt_name") or position.get("name"))[:180]
    article_number = clean_text(position.get("artikelnummer") or position.get("nr"))[:80]
    if not product and article_number:
        product = f"Artikel {article_number}"
    if not product:
        return {}
    price = clean_text(position.get("preis") or position.get("einzelpreis") or position.get("gesamtpreis"))
    if price:
        price = price.replace("EUR", "").replace("€", "").strip()
    description = clean_text(position.get("produkt_beschreibung") or position.get("beschreibung") or product)
    if not plausible_einkauf_product_text(product, article_number, description):
        return {}
    unit = clean_text(position.get("ve") or position.get("einheit")) or "Stueck"
    return {
        "produkt_name": product,
        "artikelnummer": article_number,
        "stueckzahl": parse_positive_int(position.get("stueckzahl") or position.get("menge"), 1),
        "ve": normalize_offer_unit(unit),
        "preis": price[:60],
        "kategorie": normalize_einkauf_kategorie(position.get("kategorie") or detect_einkauf_category(description)),
        "produkt_beschreibung": description[:500],
        "quelle": clean_text(position.get("quelle") or filename)[:240],
    }


def extract_einkauf_beleg_positions_openai(path, filename="", text=""):
    config = get_ai_config()
    requests_module = get_requests()
    if not config["openai_ready"] or requests_module is None:
        return []
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["positionen"],
        "properties": {
            "positionen": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "produkt_name",
                        "artikelnummer",
                        "stueckzahl",
                        "ve",
                        "preis",
                        "kategorie",
                        "produkt_beschreibung",
                        "quelle",
                    ],
                    "properties": {
                        "produkt_name": {"type": "string"},
                        "artikelnummer": {"type": "string"},
                        "stueckzahl": {"type": "integer"},
                        "ve": {"type": "string"},
                        "preis": {"type": "string"},
                        "kategorie": {"type": "string"},
                        "produkt_beschreibung": {"type": "string"},
                        "quelle": {"type": "string"},
                    },
                },
            }
        },
    }
    prompt = "\n".join(
        [
            "Analysiere diese deutsche Lieferantenrechnung fuer den Einkauf einer Karosserie- und Lackwerkstatt.",
            "Extrahiere nur echte Produkt-/Materialpositionen.",
            "Ignoriere Summen, Uebertrag, MwSt, Versand, Zahlbedingungen, IBAN, Rabatte ohne Produktbezug und Kopf-/Fusszeilen.",
            "Nutze sichtbare PDF-/Bildseiten vorrangig vor fehlerhaftem OCR-Text.",
            "Wenn eine Position nur Kopfzeile, Summe, Zahlungshinweis, Uebertrag oder OCR-Zahlensalat ist, ignoriere sie.",
            "Preis ist der sichtbare Einzelpreis oder beste Positionspreis ohne Waehrung.",
            "Kategorien: Material, Farbe / Lack, Klarlack / Haerter, Schleifmittel, Klebeband / Abdeckung, Politur / Finish, Karosserieteile, Verbrauchsmaterial, Werkzeug, Sonstiges.",
            "",
            f"Dateiname: {filename}",
            "",
            "[OCR-Text]",
            clean_text(text)[:12000] or "-",
        ]
    )
    user_content = [{"type": "text", "text": prompt}]
    user_content.extend(build_openai_visual_inputs(path, filename))
    payload = {
        "model": config["openai_model"],
        "messages": [
            {
                "role": "system",
                "content": "Du liest Rechnungspositionen vorsichtig aus und gibst ausschliesslich JSON zurueck.",
            },
            {"role": "user", "content": user_content},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "einkauf_rechnung_positionen",
                "strict": True,
                "schema": schema,
            },
        },
    }
    response, request_error = post_openai_chat_completion(requests_module, payload)
    if request_error or response is None:
        return []
    if response.status_code >= 400:
        error_message = extract_openai_error_message(response)
        if response.status_code == 400 and should_retry_openai_without_schema(error_message):
            response, request_error = post_openai_chat_completion(
                requests_module,
                build_openai_json_mode_fallback_payload(payload),
            )
            if request_error or response is None or response.status_code >= 400:
                return []
        else:
            return []
    parsed = extract_openai_response_json(response.json())
    positions = parsed.get("positionen") if isinstance(parsed, dict) else []
    normalized = []
    seen = set()
    for position in positions or []:
        item = normalize_einkauf_invoice_ai_position(position, filename)
        if not item:
            continue
        key = (
            item["artikelnummer"].lower(),
            normalize_article_lookup(item["produkt_name"]),
            clean_text(item["preis"]),
        )
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)
    return normalized[:150]


def einkauf_pruefposition_exists(db, lieferant, artikelnummer, produkt_name):
    artikelnummer_key = normalize_article_number(artikelnummer)
    produkt_key = normalize_product_lookup(produkt_name)
    lieferant_key = normalize_supplier_lookup(lieferant)
    if artikelnummer_key:
        row = db.execute(
            """
            SELECT id
            FROM einkaufsliste
            WHERE status IN ('rechnung_pruefen', 'offen')
              AND replace(replace(replace(lower(artikelnummer), '-', ''), ' ', ''), '/', '')=?
            LIMIT 1
            """,
            (artikelnummer_key,),
        ).fetchone()
        if row:
            return True
    if not produkt_key:
        return False
    rows = db.execute(
        """
        SELECT lieferant, produkt_name
        FROM einkaufsliste
        WHERE status IN ('rechnung_pruefen', 'offen')
        ORDER BY id DESC
        LIMIT 250
        """
    ).fetchall()
    for row in rows:
        same_supplier = normalize_supplier_lookup(row["lieferant"]) == lieferant_key
        if product_names_match(row["produkt_name"], produkt_name) and same_supplier:
            return True
    return False


def create_einkauf_rechnung_pruefposition(beleg, position, lieferant=""):
    beleg_id = int(beleg.get("id") or 0)
    source = f"Rechnung #{beleg_id}: {beleg.get('original_name')}"
    produkt_name = clean_text(position.get("produkt_name")) or "Material / Rechnung"
    artikelnummer = clean_text(position.get("artikelnummer"))
    preis = clean_text(position.get("preis"))
    produkt_beschreibung = clean_text(position.get("produkt_beschreibung") or position.get("quelle"))
    lieferant_name = clean_text(lieferant) or clean_text(beleg.get("lieferant")) or "Auto-Color / Topcolor"
    if not plausible_einkauf_product_text(produkt_name, artikelnummer, produkt_beschreibung):
        return 0
    db = get_db()
    try:
        if find_existing_einkauf_artikel(db, lieferant_name, artikelnummer, produkt_name):
            return 0
        if einkauf_pruefposition_exists(db, lieferant_name, artikelnummer, produkt_name):
            return 0
        cursor = db.execute(
            """
            INSERT INTO einkaufsliste
              (lieferant, kategorie, artikelnummer, produkt_name, produkt_beschreibung, titel, menge, ve,
               stueckzahl, gebinde, auto_color_preis, vergleich_preis, vergleich_lieferant,
               lieferzeit, preisquelle, angebotsstatus, angebotsnotiz, notiz, qr_text,
               quelle_email_id, quelle_beleg_id, status, original_name, stored_name, mime_type, size, erstellt_am, bestellt_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, '', '', '', ?, 'offen', '', ?, ?, 0, ?, ?, ?, '', '', 0, ?, '')
            """,
            (
                lieferant_name,
                normalize_einkauf_kategorie(position.get("kategorie")),
                artikelnummer,
                produkt_name,
                produkt_beschreibung,
                produkt_name,
                f"{parse_positive_int(position.get('stueckzahl'), 1)} {clean_text(position.get('ve')) or 'Stueck'}",
                clean_text(position.get("ve")) or "Stueck",
                parse_positive_int(position.get("stueckzahl"), 1),
                preis,
                source,
                clean_text(position.get("quelle")),
                produkt_beschreibung,
                beleg_id,
                EINKAUF_RECHNUNG_PRUEF_STATUS,
                clean_text(beleg.get("original_name")),
                now_str(),
            ),
        )
        db.commit()
        return int(cursor.lastrowid)
    finally:
        db.close()


def update_einkauf_beleg_status(beleg_id, positionen_count=0, status="importiert"):
    db = get_db()
    try:
        db.execute(
            "UPDATE einkauf_belege SET positionen_count=?, status=? WHERE id=?",
            (int(positionen_count or 0), clean_text(status) or "importiert", int(beleg_id)),
        )
        db.commit()
    finally:
        db.close()


def import_einkauf_rechnung(file_storage, lieferant="Auto-Color / Topcolor"):
    beleg = save_einkauf_beleg_upload(file_storage, lieferant=lieferant, beleg_typ="rechnung")
    if not beleg:
        return {"ok": False, "beleg": None, "created": 0, "positions": [], "message": "Beleg konnte nicht gespeichert werden."}
    text = clean_text(beleg.get("extrahierter_text"))
    beleg_path = UPLOAD_DIR / pathlib.Path(clean_text(beleg.get("stored_name"))).name
    positions = []
    if beleg_path.exists() and beleg_path.is_file():
        positions = extract_einkauf_beleg_positions_openai(
            beleg_path,
            beleg.get("original_name"),
            text,
        )
    if not positions:
        positions = extract_einkauf_beleg_positions(text, beleg.get("original_name"))
    created_ids = []
    for position in positions:
        item_id = create_einkauf_rechnung_pruefposition(beleg, position, lieferant=lieferant)
        if item_id:
            created_ids.append(item_id)
    skipped = max(0, len(positions) - len(created_ids))
    status = "pruefen" if created_ids else "manuell_pruefen"
    update_einkauf_beleg_status(beleg["id"], len(created_ids), status)
    schedule_change_backup("einkauf-rechnung")
    if created_ids:
        message = f"{len(created_ids)} Produktposition(en) aus {beleg['original_name']} zum Anlegen vorbereitet."
        if skipped:
            message += f" {skipped} bereits bekannte oder ungeeignete Zeile(n) wurden übersprungen."
    elif positions:
        message = "Rechnung gespeichert. Die erkannten Positionen waren bereits angelegt oder nicht bestellfähig."
    else:
        message = "Rechnung gespeichert, aber es wurden noch keine sicheren Produktpositionen erkannt."
    return {
        "ok": bool(created_ids),
        "beleg": get_einkauf_beleg(beleg["id"]),
        "created": len(created_ids),
        "skipped": skipped,
        "positions": positions,
        "message": message,
    }


def group_einkauf_items_by_lieferant(items):
    grouped = defaultdict(list)
    for item in items:
        grouped[clean_text(item.get("lieferant")) or "Lieferant offen"].append(item)
    return dict(sorted(grouped.items(), key=lambda entry: entry[0].lower()))


def build_topcolor_order_draft(items, topcolor_email=""):
    subject = f"Einkauf / Angebotsanfrage Gaertner - {date.today().strftime(DATE_FMT)}"
    lines = [
        "Hallo,",
        "",
        "bitte fuer folgende Positionen ein Angebot bzw. eine schnelle Lieferung pruefen:",
        "Wichtig: Alternativen bitte nur anbieten, wenn sie technisch gleichwertig sind "
        "(gleiche Marke/Artikelnummer oder vergleichbare Spezifikation, Qualitaet, Gebinde und Freigabe fuer Fahrzeuglackierung).",
        "",
    ]
    if items:
        for lieferant, lieferant_items in group_einkauf_items_by_lieferant(items).items():
            lines.append(f"Lieferant/Ziel: {lieferant}")
            for index, item in enumerate(lieferant_items, 1):
                line_parts = [f"{index}. {clean_text(item.get('produkt_name') or item.get('titel')) or 'Material'}"]
                if clean_text(item.get("artikelnummer")):
                    line_parts.append(f"Artikel: {clean_text(item.get('artikelnummer'))}")
                line_parts.append(f"Menge: {item.get('stueckzahl') or 1} {clean_text(item.get('ve')) or 'Stueck'}")
                if clean_text(item.get("gebinde")):
                    line_parts.append(f"Gebinde: {clean_text(item.get('gebinde'))}")
                if clean_text(item.get("kategorie")):
                    line_parts.append(f"Kategorie: {clean_text(item.get('kategorie'))}")
                if clean_text(item.get("qr_text")):
                    line_parts.append(f"QR: {clean_text(item.get('qr_text')).replace(chr(10), ' / ')}")
                if clean_text(item.get("original_name")):
                    line_parts.append(f"Datei: {clean_text(item.get('original_name'))}")
                lines.append(" | ".join(line_parts))
                detail_lines = []
                if clean_text(item.get("auto_color_preis")):
                    detail_lines.append(f"Auto-Color bisher: {format_price_value(item.get('auto_color_preis'))}")
                if clean_text(item.get("vergleich_lieferant")) or clean_text(item.get("vergleich_preis")):
                    detail_lines.append(
                        "Alternative: "
                        + clean_text(item.get("vergleich_lieferant") or "offen")
                        + (
                            f" / {format_price_value(item.get('vergleich_preis'))}"
                            if clean_text(item.get("vergleich_preis"))
                            else ""
                        )
                    )
                if clean_text(item.get("lieferzeit")):
                    detail_lines.append(f"Lieferzeit: {clean_text(item.get('lieferzeit'))}")
                if clean_text(item.get("notiz")):
                    detail_lines.append(f"Notiz: {clean_text(item.get('notiz'))}")
                for detail in detail_lines:
                    lines.append(f"   {detail}")
            lines.append("")
    else:
        lines.append("- Aktuell sind keine offenen Einkaufspositionen gespeichert.")
    lines.extend(["", "Vielen Dank.", "", "Mit freundlichen Gruessen", "Gärtner Karosserie & Lack"])
    body = "\n".join(lines)
    email = clean_text(topcolor_email)
    mailto_url = (
        f"mailto:{quote(email, safe='@.+-_')}?subject={quote(subject)}&body={quote(body)}"
        if email
        else ""
    )
    return {
        "subject": subject,
        "body": body,
        "mailto_url": mailto_url,
    }


def build_supplier_api_request_draft(topcolor_email=""):
    subject = "API / feste Einkaufskonditionen fuer Gaertner-Portal"
    lines = [
        "Hallo,",
        "",
        "wir bauen unser Werkstatt- und Einkaufsportal weiter aus und moechten Topcolor/Auto-Color als festen Lieferanten sauber anbinden.",
        "Bitte teilen Sie uns mit, ob Sie eine API, OCI-/IDS-Anbindung, BMEcat, CSV/Excel-Export oder einen regelmaessigen Artikel-/Preisfeed anbieten.",
        "",
        "Fuer uns wichtig:",
        "- Artikelstamm mit Artikelnummer, EAN/Hersteller-Nr., Produktname, Kategorie, Gebinde und VE",
        "- kundenspezifische Netto-EK-Preise inklusive Staffelpreisen, Rabatten und Preisgueltigkeit",
        "- Verfuegbarkeit/Lagerbestand und Lieferzeit",
        "- Produktbilder, Sicherheitsdatenblaetter und technische Datenblaetter",
        "- Alternativartikel nur bei gleicher oder gleichwertiger Qualitaet",
        "- direkte Bestellausloesung oder Uebergabe eines Warenkorbs",
        "- Rueckmeldung mit Auftragsnummer, Liefertermin und Tracking",
        "",
        "Wir vergleichen aktuell insbesondere Verbrauchsmaterial, Schleifmittel, Klebebaender, Polituren, Lack/Klarlack/Haerter und Karosserieteile mit Auto-Color/Topcolor.",
        "Gerade bei teuren Lack- und PPG-/Envirobase-Positionen moechten wir feste Konditionen sauber im Portal hinterlegen, damit nicht jede Bestellung einzeln nachverhandelt werden muss.",
        "",
        "Wenn spaeter weitere Lackierbetriebe das Portal nutzen, koennte Topcolor als integrierter Lieferant direkt in deren Einkauf sichtbar sein. Das waere fuer beide Seiten eine Win-win-Situation: weniger Rueckfragen, sauberere Bestellungen und bessere Bindung ueber feste Konditionen.",
        "",
        "Bitte senden Sie uns technische Unterlagen, Preise und den passenden Ansprechpartner fuer API/Preisliste/Konditionen.",
        "",
        "Vielen Dank.",
        "",
        "Mit freundlichen Gruessen",
        "Gaertner Karosserie & Lack",
    ]
    body = "\n".join(lines)
    email = clean_text(topcolor_email)
    mailto_url = (
        f"mailto:{quote(email, safe='@.+-_')}?subject={quote(subject)}&body={quote(body)}"
        if email
        else ""
    )
    return {
        "subject": subject,
        "body": body,
        "mailto_url": mailto_url,
    }


def list_einkauf_items_for_email(email_id):
    try:
        email_id = int(email_id or 0)
    except (TypeError, ValueError):
        email_id = 0
    if not email_id:
        return []
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM einkaufsliste
            WHERE quelle_email_id=?
            ORDER BY id DESC
            """,
            (email_id,),
        ).fetchall()
    finally:
        db.close()
    return [hydrate_einkauf_item(row) for row in rows]


def guess_lieferant_from_email(email_item):
    subject = clean_text(email_item.get("betreff"))
    body = clean_text(email_item.get("nachricht"))
    name = clean_text(email_item.get("absender_name"))
    address = clean_text(email_item.get("absender_email"))
    search = f"{name} {address} {subject} {body}".lower()
    if any(token in search for token in ("topcolor", "top color", "auto-color", "autocolor")):
        return "Auto-Color / Topcolor"
    if name:
        return name[:120]
    if address and "@" in address:
        domain = address.split("@", 1)[1].split(".", 1)[0]
        return domain.replace("-", " ").title()[:120]
    return "Lieferant offen"


def is_lieferantenangebot_email(email_item, force=False):
    subject = clean_text(email_item.get("betreff"))
    body = clean_text(email_item.get("nachricht"))
    sender = email_sender_display(email_item)
    text = f"{sender}\n{subject}\n{body}".lower()
    has_price = bool(re.search(r"\d{1,6}(?:[.,]\d{2})\s*(?:eur|€)", text, re.I))
    if force:
        return has_price
    offer_terms = ("angebot", "preis", "netto", "eur", "€", "lieferzeit")
    purchase_terms = ("einkauf", "bestellung", "material", "artikel", "lieferant", "position")
    supplier_terms = (
        "topcolor",
        "top color",
        "auto-color",
        "autocolor",
        "mipa",
        "3m",
        "sata",
        "wurth",
        "würth",
        "lack",
        "schleif",
    )
    return (
        has_price
        and any(term in text for term in offer_terms)
        and (any(term in text for term in supplier_terms) or any(term in text for term in purchase_terms))
    )


def normalize_offer_unit(value):
    value = clean_text(value).lower()
    mapping = {
        "x": "Stueck",
        "stk": "Stueck",
        "stck": "Stueck",
        "stück": "Stueck",
        "stueck": "Stueck",
        "dose": "Dose",
        "dosen": "Dose",
        "l": "Liter",
        "ltr": "Liter",
        "liter": "Liter",
        "rolle": "Rolle",
        "rollen": "Rolle",
        "karton": "Karton",
        "packung": "Packung",
        "set": "Set",
    }
    return mapping.get(value, "Stueck")


def clean_offer_product_text(line, price_match, article_number=""):
    text = clean_text(line)
    if price_match:
        text = (text[: price_match.start()] + text[price_match.end() :]).strip()
    if article_number:
        text = re.sub(
            r"(?:artikel(?:nr|nummer)?|bestell(?:nr|nummer)?|art\.?|nr\.?)\b[:#\s-]*"
            + re.escape(article_number),
            "",
            text,
            flags=re.I,
        )
    text = re.sub(
        r"\b\d+\s*(?:x|stk\.?|stck\.?|stück|stueck|dose|dosen|l|ltr|liter|rolle|rollen|karton|packung|set)\b",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\b(?:netto|brutto|eur|preis|angebotspreis|einzelpreis|gesamtpreis)\b[:\s-]*", "", text, flags=re.I)
    text = re.sub(r"\s{2,}", " ", text).strip(" -|:\t")
    return text[:160]


def extract_lieferantenangebot_positions(email_item):
    subject = clean_text(email_item.get("betreff"))
    body = clean_text(email_item.get("nachricht"))
    raw_text = f"{subject}\n{body}"
    lines = [clean_text(line) for line in re.split(r"[\r\n;]+", raw_text) if clean_text(line)]
    positions = []
    price_re = re.compile(r"(?P<price>\d{1,6}(?:[.,]\d{2}))\s*(?:eur|€)", re.I)
    article_re = re.compile(
        r"(?:artikel(?:nr|nummer)?|bestell(?:nr|nummer)?|art\.?|nr\.?)\b[:#\s-]*(?P<article>[A-Za-z0-9][A-Za-z0-9./_-]{2,})",
        re.I,
    )
    amount_re = re.compile(
        r"\b(?P<count>\d{1,4})\s*(?P<unit>x|stk\.?|stck\.?|stück|stueck|dose|dosen|l|ltr|liter|rolle|rollen|karton|packung|set)\b",
        re.I,
    )
    skip_terms = ("summe", "gesamt", "zwischensumme", "mwst", "ust", "versand", "porto", "rabatt")
    for line in lines:
        lowered = line.lower()
        price_match = price_re.search(line)
        if not price_match:
            continue
        if any(term in lowered for term in skip_terms) and not article_re.search(line):
            continue
        article_match = article_re.search(line)
        amount_match = amount_re.search(line)
        article_number = clean_text(article_match.group("article")) if article_match else ""
        count = parse_positive_int(amount_match.group("count"), 1) if amount_match else 1
        unit = normalize_offer_unit(amount_match.group("unit")) if amount_match else "Stueck"
        product = clean_offer_product_text(line, price_match, article_number)
        if not product and article_number:
            product = f"Artikel {article_number}"
        if not product:
            product = clean_text(subject) or "Lieferantenposition"
        if not plausible_einkauf_product_text(product, article_number, line):
            continue
        positions.append(
            {
                "produkt_name": product,
                "artikelnummer": article_number,
                "stueckzahl": count,
                "ve": unit,
                "preis": clean_text(price_match.group("price")),
                "quelle": line[:220],
            }
        )
    return positions[:80]


def analyse_lieferantenangebot_email(email_id, force=False):
    email_item = get_werkstatt_email(email_id)
    if not email_item:
        return {"ok": False, "reason": "email_missing", "created": 0, "items": []}
    existing_items = list_einkauf_items_for_email(email_id)
    if existing_items:
        return {"ok": True, "reason": "existing", "created": 0, "items": existing_items}
    if not is_lieferantenangebot_email(email_item, force=force):
        return {"ok": False, "reason": "not_offer", "created": 0, "items": []}
    lieferant = guess_lieferant_from_email(email_item)
    positions = extract_lieferantenangebot_positions(email_item)
    if not positions:
        return {"ok": False, "reason": "no_positions", "created": 0, "items": []}
    created_ids = []
    supplier_lower = lieferant.lower()
    source = f"E-Mail #{email_item['id']}: {email_item['betreff']}"
    for position in positions:
        price_fields = {}
        if any(token in supplier_lower for token in ("topcolor", "top color", "auto-color", "autocolor")):
            price_fields["auto_color_preis"] = position["preis"]
        else:
            price_fields["vergleich_preis"] = position["preis"]
            price_fields["vergleich_lieferant"] = lieferant
        created_ids.append(
            create_einkauf_item(
                lieferant=lieferant,
                kategorie="Material",
                produkt_name=position["produkt_name"],
                titel=position["produkt_name"],
                artikelnummer=position["artikelnummer"],
                stueckzahl=position["stueckzahl"],
                ve=position["ve"],
                preisquelle=source,
                angebotsstatus="angebot_erhalten",
                angebotsnotiz=f"Aus Lieferanten-E-Mail erkannt: {position['quelle']}",
                quelle_email_id=email_item["id"],
                **price_fields,
            )
        )
    return {
        "ok": True,
        "reason": "created",
        "created": len(created_ids),
        "items": [get_einkauf_item(item_id) for item_id in created_ids],
    }


def einkauf_offer_anchor(key):
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", clean_text(key)).strip("-") or "offen"
    return f"lieferantenangebot-{slug}"


def build_einkauf_offer_groups(items=None, limit=20):
    source_items = items if items is not None else list_einkauf_items("offen", limit=500)
    offer_items = [
        item
        for item in source_items
        if item.get("quelle_email_id")
        or item.get("angebotsstatus") in {"angebot_erhalten", "freigegeben", "angefragt"}
    ]
    email_ids = sorted({int(item.get("quelle_email_id") or 0) for item in offer_items if item.get("quelle_email_id")})
    emails = {email_id: get_werkstatt_email(email_id) for email_id in email_ids}
    groups = {}
    for item in offer_items:
        email_id = int(item.get("quelle_email_id") or 0)
        key = f"email-{email_id}" if email_id else f"lieferant-{clean_text(item.get('lieferant')) or 'offen'}"
        email_item = emails.get(email_id) if email_id else None
        if key not in groups:
            title = clean_text((email_item or {}).get("betreff")) if email_item else ""
            supplier = clean_text(item.get("lieferant")) or "Lieferant offen"
            anchor = einkauf_offer_anchor(key)
            groups[key] = {
                "key": key,
                "anchor": anchor,
                "ziel_url": (url_for("admin_einkauf") + f"#{anchor}") if has_request_context() else "",
                "titel": title or f"Angebot von {supplier}",
                "lieferant": supplier,
                "empfangen_am": clean_text((email_item or {}).get("empfangen_am")) or clean_text(item.get("erstellt_am")),
                "absender": email_sender_display(email_item) if email_item else supplier,
                "items": [],
                "price_total": 0.0,
                "price_count": 0,
            }
        groups[key]["items"].append(item)
        price = parse_price_value(item.get("auto_color_preis") or item.get("vergleich_preis"))
        if price is not None:
            groups[key]["price_total"] += price * parse_positive_int(item.get("stueckzahl"), 1)
            groups[key]["price_count"] += 1
    result = []
    for group in groups.values():
        group["count"] = len(group["items"])
        group["preview"] = ", ".join(
            clean_text(item.get("produkt_name") or item.get("titel")) for item in group["items"][:3]
        )
        if group["price_count"]:
            group["preis_summe_label"] = f"{group['price_total']:.2f} EUR".replace(".", ",")
        else:
            group["preis_summe_label"] = ""
        result.append(group)
    return result[: max(1, int(limit or 20))]


def get_werkstatt_email_api_token():
    try:
        configured = get_app_setting("WERKSTATT_EMAIL_API_TOKEN", "")
    except Exception:
        configured = ""
    return clean_secret_value(configured or WERKSTATT_EMAIL_API_TOKEN)


def werkstatt_api_token_valid():
    token = get_werkstatt_email_api_token()
    if not token:
        return False
    provided = clean_secret_value(request.headers.get("X-Werkstatt-Token"))
    auth_header = clean_text(request.headers.get("Authorization"))
    if auth_header.lower().startswith("bearer "):
        provided = clean_secret_value(auth_header[7:])
    return bool(provided and hmac.compare_digest(provided, token))


def email_sender_display(email_item):
    name = clean_text(email_item.get("absender_name"))
    address = clean_text(email_item.get("absender_email"))
    if name and address:
        return f"{name} <{address}>"
    return name or address or "Unbekannter Absender"


def hydrate_werkstatt_email(row):
    item = dict(row)
    item["absender_display"] = email_sender_display(item)
    item["betreff"] = clean_text(item.get("betreff")) or "(ohne Betreff)"
    item["nachricht"] = clean_text(item.get("nachricht"))
    item["excerpt"] = postfach_excerpt(item["nachricht"], 170)
    item["empfangen_am"] = clean_text(item.get("empfangen_am")) or clean_text(item.get("erstellt_am"))
    item["is_neu"] = clean_text(item.get("status")) == "neu"
    item["kategorie"] = clean_text(item.get("kategorie")) or "allgemein"
    item["ziel_modul"] = clean_text(item.get("ziel_modul"))
    item["message_id"] = clean_text(item.get("message_id"))
    item["source_uid"] = clean_text(item.get("source_uid"))
    item["raw_hash"] = clean_text(item.get("raw_hash"))
    try:
        item["attachments_count"] = int(item.get("attachments_count") or 0)
    except (TypeError, ValueError):
        item["attachments_count"] = 0
    return item


def classify_werkstatt_email(betreff="", nachricht="", absender_email=""):
    text_blob = normalize_document_text(" ".join([betreff or "", nachricht or "", absender_email or ""]))
    invoice_tokens = (
        "rechnung",
        "invoice",
        "zahlbar",
        "faellig",
        "fällig",
        "mahnung",
        "beleg",
        "gutschrift",
        "zahlungserinnerung",
    )
    purchase_tokens = (
        "lieferant",
        "topcolor",
        "top color",
        "auto-color",
        "material",
        "bestellung",
        "angebot",
        "lieferschein",
    )
    if any(token in text_blob for token in invoice_tokens):
        if any(token in text_blob for token in purchase_tokens):
            return "rechnung_ausgabe", "rechnungen"
        return "rechnung", "rechnungen"
    if any(token in text_blob for token in purchase_tokens):
        return "einkauf", "einkauf"
    return "allgemein", ""


def list_werkstatt_emails(status="aktiv", limit=120):
    status = clean_text(status) or "aktiv"
    limit = max(1, min(int(limit or 120), 500))
    db = get_db()
    try:
        if status == "alle":
            rows = db.execute(
                """
                SELECT *
                FROM werkstatt_emails
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        elif status == "aktiv":
            rows = db.execute(
                """
                SELECT *
                FROM werkstatt_emails
                WHERE status!='archiviert'
                ORDER BY CASE WHEN status='neu' THEN 0 ELSE 1 END, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT *
                FROM werkstatt_emails
                WHERE status=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
    finally:
        db.close()
    return [hydrate_werkstatt_email(row) for row in rows]


def get_werkstatt_email(email_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM werkstatt_emails WHERE id=?", (int(email_id),)).fetchone()
    finally:
        db.close()
    return hydrate_werkstatt_email(row) if row else None


def admin_email_count():
    try:
        db = get_db()
        row = db.execute(
            "SELECT COUNT(*) AS count FROM werkstatt_emails WHERE status='neu'"
        ).fetchone()
        return int(row["count"] or 0) if row else 0
    except Exception:
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass


def normalize_email_date(value):
    cleaned = clean_text(value)
    if not cleaned:
        return now_str()
    parsed = parse_date(cleaned)
    if parsed:
        return parsed.strftime(DATE_FMT)
    for fmt in (DATETIME_FMT, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(cleaned[:19], fmt).strftime(DATETIME_FMT)
        except ValueError:
            continue
    return cleaned[:80]


def get_werkstatt_imap_config():
    search = re.sub(r"\s+", " ", MAIL_IMAP_SEARCH).strip().upper()
    if not search or any(char in search for char in "\r\n\"\\"):
        search = "UNSEEN"
    password = clean_secret_value(MAIL_IMAP_PASS)
    return {
        "configured": bool(MAIL_IMAP_HOST and MAIL_IMAP_USER and password),
        "host": MAIL_IMAP_HOST,
        "port": MAIL_IMAP_PORT,
        "user": MAIL_IMAP_USER,
        "password": password,
        "folder": MAIL_IMAP_FOLDER,
        "ssl": bool(MAIL_IMAP_SSL),
        "mark_seen": bool(MAIL_IMAP_MARK_SEEN),
        "archive_folder": MAIL_IMAP_ARCHIVE_FOLDER,
        "search": search,
        "limit": MAIL_IMAP_LIMIT,
        "timeout": MAIL_IMAP_TIMEOUT_SECONDS,
    }


def werkstatt_imap_status():
    config = get_werkstatt_imap_config()
    return {
        "configured": config["configured"],
        "host": config["host"],
        "port": config["port"],
        "user": config["user"],
        "folder": config["folder"],
        "ssl": config["ssl"],
        "mark_seen": config["mark_seen"],
        "archive_folder": config["archive_folder"],
        "search": config["search"],
        "limit": config["limit"],
    }


def decode_email_header_value(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(str(value))))
    except Exception:
        return clean_text(value)


def email_address_parts(value):
    decoded = decode_email_header_value(value)
    for name, address in getaddresses([decoded]):
        if name or address:
            return decode_email_header_value(name), clean_text(address)
    return "", clean_text(decoded)


def email_address_list(value):
    decoded = decode_email_header_value(value)
    addresses = []
    for name, address in getaddresses([decoded]):
        address = clean_text(address)
        if not address:
            continue
        name = decode_email_header_value(name)
        addresses.append(f"{name} <{address}>" if name else address)
    return ", ".join(addresses)


def email_message_date_label(message):
    date_header = clean_text(message.get("Date"))
    if not date_header:
        return now_str()
    try:
        parsed = parsedate_to_datetime(date_header)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone()
        return parsed.strftime(DATETIME_FMT)
    except Exception:
        return normalize_email_date(date_header)


def normalize_email_body_text(value):
    text = unescape(str(value or ""))
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    previous_blank = False
    for raw_line in text.splitlines():
        line = compact_whitespace(raw_line)
        blank = not line
        if blank and previous_blank:
            continue
        lines.append(line)
        previous_blank = blank
    return "\n".join(lines).strip()


def html_email_to_text(value):
    html = str(value or "")
    html = re.sub(r"(?is)<(script|style).*?</\1>", " ", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</(p|div|tr|li|h[1-6])>", "\n", html)
    html = re.sub(r"<[^>]+>", " ", html)
    return normalize_email_body_text(html)


def email_part_text(part):
    try:
        content = part.get_content()
        if isinstance(content, str):
            return content
    except Exception:
        pass
    payload = part.get_payload(decode=True)
    if not payload:
        return ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def email_message_body_text(message):
    plain_parts = []
    html_parts = []
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        if part.is_multipart():
            continue
        disposition = clean_text(part.get_content_disposition()).lower()
        if disposition == "attachment":
            continue
        content_type = clean_text(part.get_content_type()).lower()
        if content_type == "text/plain":
            plain_parts.append(email_part_text(part))
        elif content_type == "text/html":
            html_parts.append(email_part_text(part))
    if plain_parts:
        return normalize_email_body_text("\n\n".join(plain_parts))
    if html_parts:
        return html_email_to_text("\n\n".join(html_parts))
    return ""


def email_attachment_count(message):
    count = 0
    for part in message.walk() if message.is_multipart() else [message]:
        if part.is_multipart():
            continue
        disposition = clean_text(part.get_content_disposition()).lower()
        if disposition == "attachment" or clean_text(part.get_filename()):
            count += 1
    return count


def parse_werkstatt_email_raw_message(raw_bytes, source_uid=""):
    if isinstance(raw_bytes, str):
        raw_bytes = raw_bytes.encode("utf-8", errors="replace")
    raw_bytes = bytes(raw_bytes or b"")
    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
    absender_name, absender_email = email_address_parts(message.get("From"))
    message_id = clean_text(message.get("Message-ID") or message.get("Message-Id"))
    empfaenger = email_address_list(message.get("To") or message.get("Delivered-To"))
    betreff = decode_email_header_value(message.get("Subject"))
    nachricht = email_message_body_text(message)
    raw_hash = hashlib.sha256(raw_bytes).hexdigest() if raw_bytes else ""
    attachments_count = email_attachment_count(message)
    return {
        "absender_name": absender_name,
        "absender_email": absender_email,
        "empfaenger": empfaenger,
        "betreff": betreff,
        "nachricht": nachricht,
        "empfangen_am": email_message_date_label(message),
        "quelle": "imap",
        "message_id": message_id[:255],
        "source_uid": clean_text(source_uid)[:255],
        "raw_hash": raw_hash,
        "attachments_count": attachments_count,
        "original_payload": {
            "message_id": message_id,
            "source_uid": clean_text(source_uid),
            "raw_hash": raw_hash,
            "attachments_count": attachments_count,
            "from": {"name": absender_name, "email": absender_email},
            "to": empfaenger,
            "subject": betreff,
        },
    }


def find_existing_werkstatt_email(message_id="", raw_hash="", source_uid=""):
    checks = []
    params = []
    for column, value in (
        ("message_id", message_id),
        ("raw_hash", raw_hash),
        ("source_uid", source_uid),
    ):
        value = clean_text(value)
        if not value:
            continue
        checks.append(f"{column}=?")
        params.append(value[:255])
    if not checks:
        return 0
    db = get_db()
    try:
        row = db.execute(
            f"""
            SELECT id
            FROM werkstatt_emails
            WHERE {' OR '.join(checks)}
            ORDER BY id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    finally:
        db.close()
    return int(row["id"]) if row else 0


def import_werkstatt_email_raw_message(raw_bytes, source_uid=""):
    payload = parse_werkstatt_email_raw_message(raw_bytes, source_uid=source_uid)
    existing_id = find_existing_werkstatt_email(
        message_id=payload.get("message_id"),
        raw_hash=payload.get("raw_hash"),
        source_uid=payload.get("source_uid"),
    )
    if existing_id:
        return {
            "ok": True,
            "created": False,
            "id": existing_id,
            "reason": "duplicate",
            "betreff": payload.get("betreff") or "",
        }
    email_id = create_werkstatt_email(**payload)
    return {
        "ok": True,
        "created": True,
        "id": email_id,
        "reason": "created",
        "betreff": payload.get("betreff") or "",
    }


def connect_werkstatt_imap(config):
    if config["ssl"]:
        return imaplib.IMAP4_SSL(
            config["host"],
            config["port"],
            timeout=config["timeout"],
        )
    return imaplib.IMAP4(
        config["host"],
        config["port"],
        timeout=config["timeout"],
    )


def sync_werkstatt_imap(limit=None):
    config = get_werkstatt_imap_config()
    if not config["configured"]:
        raise ValueError(
            "IMAP ist noch nicht konfiguriert. Bitte MAIL_IMAP_HOST, MAIL_IMAP_USER und MAIL_IMAP_PASS in .env.local setzen."
        )
    limit = max(1, min(int(limit or config["limit"] or 30), 200))
    summary = {
        "ok": True,
        "checked": 0,
        "fetched": 0,
        "created": 0,
        "skipped": 0,
        "errors": [],
        "created_ids": [],
    }
    client = None
    delete_after_copy = False
    try:
        client = connect_werkstatt_imap(config)
        client.login(config["user"], config["password"])
        readonly = not (config["mark_seen"] or config["archive_folder"])
        status, _ = client.select(config["folder"], readonly=readonly)
        if status != "OK":
            raise RuntimeError(f"IMAP-Ordner '{config['folder']}' konnte nicht geöffnet werden.")
        status, data = client.uid("search", None, config["search"])
        if status != "OK":
            raise RuntimeError(f"IMAP-Suche '{config['search']}' ist fehlgeschlagen.")
        uid_blob = data[0] if data else b""
        uids = [uid for uid in uid_blob.split() if uid]
        selected_uids = uids[-limit:]
        summary["checked"] = len(selected_uids)
        for uid in reversed(selected_uids):
            uid_text = uid.decode("ascii", errors="ignore")
            source_uid = f"{config['folder']}:{uid_text}"
            if find_existing_werkstatt_email(source_uid=source_uid):
                summary["skipped"] += 1
                if config["mark_seen"]:
                    client.uid("STORE", uid, "+FLAGS", r"(\Seen)")
                continue
            status, fetched = client.uid("fetch", uid, "(BODY.PEEK[])")
            if status != "OK":
                summary["errors"].append(f"UID {uid_text}: Abruf fehlgeschlagen")
                continue
            raw_bytes = b""
            for item in fetched or []:
                if isinstance(item, tuple) and item[1]:
                    raw_bytes = item[1]
                    break
            if not raw_bytes:
                summary["errors"].append(f"UID {uid_text}: keine Nachrichtendaten")
                continue
            summary["fetched"] += 1
            result = import_werkstatt_email_raw_message(raw_bytes, source_uid=source_uid)
            if result.get("created"):
                summary["created"] += 1
                summary["created_ids"].append(result.get("id"))
            else:
                summary["skipped"] += 1
            if config["archive_folder"]:
                copy_status, _ = client.uid("COPY", uid, config["archive_folder"])
                if copy_status == "OK":
                    client.uid("STORE", uid, "+FLAGS", r"(\Deleted)")
                    delete_after_copy = True
                else:
                    summary["errors"].append(f"UID {uid_text}: Archivierung fehlgeschlagen")
            elif config["mark_seen"]:
                client.uid("STORE", uid, "+FLAGS", r"(\Seen)")
        if delete_after_copy:
            client.expunge()
        return summary
    finally:
        if client is not None:
            try:
                client.logout()
            except Exception:
                pass


def create_werkstatt_email(
    absender_name="",
    absender_email="",
    empfaenger="",
    betreff="",
    nachricht="",
    empfangen_am="",
    quelle="manuell",
    autohaus_id=0,
    auftrag_id=0,
    original_payload=None,
    message_id="",
    source_uid="",
    raw_hash="",
    attachments_count=0,
):
    betreff = clean_text(betreff)
    nachricht = clean_text(nachricht)
    absender_email = clean_text(absender_email)
    absender_name = clean_text(absender_name)
    message_id = clean_text(message_id)[:255]
    source_uid = clean_text(source_uid)[:255]
    raw_hash = clean_text(raw_hash)[:255]
    try:
        attachments_count = max(0, int(attachments_count or 0))
    except (TypeError, ValueError):
        attachments_count = 0
    if not any((betreff, nachricht, absender_email, absender_name)):
        raise ValueError("Bitte mindestens Absender, Betreff oder Nachricht eintragen.")
    existing_id = find_existing_werkstatt_email(
        message_id=message_id,
        raw_hash=raw_hash,
        source_uid=source_uid,
    )
    if existing_id:
        return existing_id
    kategorie, ziel_modul = classify_werkstatt_email(betreff, nachricht, absender_email)
    now = now_str()
    email_id = 0
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO werkstatt_emails
              (quelle, kategorie, ziel_modul, absender_name, absender_email, empfaenger, betreff, nachricht,
               empfangen_am, message_id, source_uid, raw_hash, attachments_count,
               status, autohaus_id, auftrag_id, original_payload,
               erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'neu', ?, ?, ?, ?, ?)
            """,
            (
                clean_text(quelle) or "manuell",
                kategorie,
                ziel_modul,
                absender_name[:160],
                absender_email[:220],
                clean_text(empfaenger)[:220],
                betreff[:220],
                nachricht[:5000],
                normalize_email_date(empfangen_am),
                message_id,
                source_uid,
                raw_hash,
                attachments_count,
                int(autohaus_id or 0),
                int(auftrag_id or 0),
                json.dumps(original_payload or {}, ensure_ascii=False)[:5000],
                now,
                now,
            ),
        )
        db.commit()
        email_id = cursor.lastrowid
    finally:
        db.close()
    try:
        analyse_lieferantenangebot_email(email_id)
    except Exception:
        pass
    try:
        create_rechnung_marker_from_email(email_id)
    except Exception:
        pass
    schedule_change_backup("werkstatt-email")
    return email_id


def create_rechnung_marker_from_email(email_id):
    email_item = get_werkstatt_email(email_id)
    if not email_item or clean_text(email_item.get("ziel_modul")) != "rechnungen":
        return 0
    direction = "Ausgabe" if clean_text(email_item.get("kategorie")) == "rechnung_ausgabe" else "Einnahme"
    text_blob = " ".join([email_item.get("betreff", ""), email_item.get("nachricht", "")])
    amount = parse_money_amount(text_blob) or 0
    invoice_number = ""
    match = re.search(r"(?:rechnung(?:snummer)?|re\.?\s*nr\.?|invoice)\s*[:#-]?\s*([A-Z0-9][A-Z0-9./_-]{2,})", text_blob, re.I)
    if match:
        invoice_number = clean_text(match.group(1))
    voucher_id = f"email-{int(email_id)}"
    now = now_str()
    db = get_db()
    try:
        existing = db.execute(
            "SELECT id FROM lexware_rechnungen WHERE source_email_id=? OR voucher_id=?",
            (int(email_id), voucher_id),
        ).fetchone()
        if existing:
            return int(existing["id"])
        cursor = db.execute(
            """
            INSERT INTO lexware_rechnungen
              (voucher_id, voucher_type, richtung, status, payment_status, voucher_status,
               voucher_number, contact_name, total_amount, open_amount, currency,
               voucher_date, due_date, source_email_id, raw_json, zuletzt_synced_am, erstellt_am, geaendert_am)
            VALUES (?, 'email', ?, 'pruefen', '', 'unchecked', ?, ?, ?, ?, 'EUR', ?, '', ?, ?, ?, ?, ?)
            """,
            (
                voucher_id,
                direction,
                invoice_number,
                email_sender_display(email_item),
                amount,
                amount,
                clean_text(email_item.get("empfangen_am")),
                int(email_id),
                json.dumps({"email_id": email_id, "betreff": email_item.get("betreff")}, ensure_ascii=False),
                now,
                now,
                now,
            ),
        )
        db.commit()
        return int(cursor.lastrowid)
    finally:
        db.close()


def update_werkstatt_email_status(email_id, status):
    status = clean_text(status)
    if status not in {"neu", "gelesen", "erledigt", "archiviert"}:
        status = "gelesen"
    db = get_db()
    try:
        db.execute(
            "UPDATE werkstatt_emails SET status=?, geaendert_am=? WHERE id=?",
            (status, now_str(), int(email_id)),
        )
        db.commit()
        schedule_change_backup("werkstatt-email-status")
    finally:
        db.close()


def email_payload_from_request():
    payload = request.get_json(silent=True) or {}
    sender = payload.get("sender") or payload.get("from") or {}
    if isinstance(sender, str):
        absender_name = ""
        absender_email = sender
    else:
        absender_name = sender.get("name") or payload.get("from_name") or payload.get("absender_name")
        absender_email = sender.get("email") or payload.get("from_email") or payload.get("absender_email")
    return {
        "absender_name": absender_name,
        "absender_email": absender_email,
        "empfaenger": payload.get("to") or payload.get("empfaenger"),
        "betreff": payload.get("subject") or payload.get("betreff"),
        "nachricht": payload.get("text") or payload.get("body") or payload.get("nachricht"),
        "empfangen_am": payload.get("received_at") or payload.get("empfangen_am"),
        "quelle": payload.get("source") or payload.get("quelle") or "api",
        "autohaus_id": payload.get("autohaus_id") or 0,
        "auftrag_id": payload.get("auftrag_id") or 0,
        "original_payload": payload,
    }


def list_partner_postfach_items(autohaus_id, slug, limit=80):
    autohaus_id = int(autohaus_id or 0)
    hidden = postfach_hidden_keys("autohaus", autohaus_id)
    items = []
    db = get_db()

    rows = db.execute(
        """
        SELECT b.*, a.fahrzeug, a.kennzeichen, a.auftragsnummer, a.angebotsphase
        FROM benachrichtigungen b
        JOIN auftraege a ON a.id = b.auftrag_id
        WHERE a.autohaus_id=? AND a.archiviert=0 AND COALESCE(b.gelesen, 0)=0
        ORDER BY b.id DESC
        LIMIT 80
        """,
        (autohaus_id,),
    ).fetchall()
    for row in rows:
        key = f"autohaus-hinweis-{row['id']}"
        if key in hidden:
            continue
        endpoint = "partner_angebot_detail" if row["angebotsphase"] else "partner_auftrag"
        items.append(
            {
                "item_key": key,
                "typ": "Werkstatt",
                "titel": row["titel"],
                "nachricht": postfach_excerpt(row["nachricht"]),
                "erstellt_am": row["erstellt_am"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": url_for(endpoint, slug=slug, auftrag_id=row["auftrag_id"]),
            }
        )

    rows = db.execute(
        """
        SELECT c.id, c.auftrag_id, c.nachricht, c.erstellt_am,
               a.fahrzeug, a.kennzeichen
        FROM chat_nachrichten c
        JOIN auftraege a ON a.id = c.auftrag_id
        WHERE a.autohaus_id=?
          AND c.absender='werkstatt'
          AND c.gelesen_autohaus=0
          AND a.archiviert=0
        ORDER BY c.id DESC
        LIMIT 80
        """,
        (autohaus_id,),
    ).fetchall()
    for row in rows:
        key = f"autohaus-chat-{row['id']}"
        if key in hidden:
            continue
        items.append(
            {
                "item_key": key,
                "typ": "Chat",
                "titel": "Neue Nachricht der Werkstatt",
                "nachricht": postfach_excerpt(row["nachricht"]),
                "erstellt_am": row["erstellt_am"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": url_for("partner_auftrag", slug=slug, auftrag_id=row["auftrag_id"]),
            }
        )

    rows = db.execute(
        """
        SELECT a.id, a.fahrzeug, a.kennzeichen, a.analyse_hinweis, a.geaendert_am, a.angebotsphase
        FROM auftraege a
        WHERE a.autohaus_id=?
          AND a.analyse_pruefen=1
          AND a.analyse_autohaus_geprueft=0
          AND a.archiviert=0
        ORDER BY a.geaendert_am DESC, a.id DESC
        LIMIT 80
        """,
        (autohaus_id,),
    ).fetchall()
    db.close()
    for row in rows:
        key = f"autohaus-dokument-{row['id']}"
        if key in hidden:
            continue
        endpoint = "partner_angebot_detail" if row["angebotsphase"] else "partner_auftrag"
        items.append(
            {
                "item_key": key,
                "typ": "Prüfung",
                "titel": "Dokumentprüfung offen",
                "nachricht": postfach_excerpt(row["analyse_hinweis"] or "Erkannte Werte müssen geprüft werden."),
                "erstellt_am": row["geaendert_am"],
                "fahrzeug": row["fahrzeug"],
                "kennzeichen": row["kennzeichen"],
                "ziel_url": url_for(endpoint, slug=slug, auftrag_id=row["id"]),
            }
        )

    return sort_postfach_items(items, limit)


def partner_postfach_count(autohaus_id, slug):
    try:
        return len(list_partner_postfach_items(autohaus_id, slug, limit=200))
    except Exception:
        return 0


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


def get_offene_reklamation_counts(auftrag_ids=None):
    ids = [int(auftrag_id) for auftrag_id in (auftrag_ids or []) if auftrag_id]
    db = get_db()
    try:
        if ids:
            placeholders = ",".join("?" for _ in ids)
            rows = db.execute(
                f"""
                SELECT auftrag_id, COUNT(*) AS count
                FROM reklamationen
                WHERE bearbeitet=0
                  AND auftrag_id IN ({placeholders})
                GROUP BY auftrag_id
                """,
                ids,
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT auftrag_id, COUNT(*) AS count
                FROM reklamationen
                WHERE bearbeitet=0
                GROUP BY auftrag_id
                """
            ).fetchall()
    finally:
        db.close()
    return {int(row["auftrag_id"]): int(row["count"] or 0) for row in rows}


def mark_auftraege_reklamationsstatus(auftraege):
    counts = get_offene_reklamation_counts([auftrag.get("id") for auftrag in auftraege])
    for auftrag in auftraege:
        count = counts.get(int(auftrag.get("id") or 0), 0)
        auftrag["offene_reklamationen_count"] = count
        auftrag["hat_offene_reklamation"] = count > 0
        auftrag["archivierbar_zurueckgegeben"] = auftrag.get("status") == 5 and count == 0
    return auftraege


def auftrag_has_offene_reklamation(auftrag_id):
    return bool(get_offene_reklamation_counts([auftrag_id]).get(int(auftrag_id or 0), 0))


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


def save_uploads(auftrag_id, files, quelle, kategorie="standard", reklamation_id=None, upload_note=""):
    saved = 0
    saved_analysis_document = False
    analysis_errors = []
    upload_note = clean_text(upload_note)[:500]
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
        mime_type = file.mimetype or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
        dokument_typ = ""
        extrahierter_text = ""
        extrakt_kurz = ""
        analyse_quelle = ""
        analyse_json = ""
        analyse_hinweis = ""
        datei_notiz = upload_note if clean_text(kategorie) == "standard" else ""
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
            structured_summary = structured_analysis_summary(bundle.get("structured"), original_name)
            extrakt_kurz = structured_summary or summarize_document_text(extrahierter_text, original_name)
        if datei_notiz:
            extrahierter_text = append_upload_note_to_analysis(extrahierter_text, datei_notiz)
            extrakt_kurz = append_upload_note_to_summary(extrakt_kurz, datei_notiz)
        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, reklamation_id, original_name, stored_name, mime_type, size, quelle, kategorie, dokument_typ, notiz,
             extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am)
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
                datei_notiz,
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
    message = clean_text(success_message) or "Datei hochgeladen."
    if meaningful_updates:
        flash(
            f"{message} Bitte erkannte Werte prüfen.",
            "warning",
        )
    else:
        flash(
            f"{message} Bitte die erkannten Daten kontrollieren.",
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
        note = clean_text(datei.get("notiz"))
        doc_type = (
            classify_document(extracted_text, original_name)
            if extracted_text
            else classify_document("", original_name)
        )
        summary = structured_analysis_summary(bundle.get("structured"), original_name) or summarize_document_text(extracted_text, original_name)
        if note:
            extracted_text = append_upload_note_to_analysis(extracted_text, note)
            summary = append_upload_note_to_summary(summary, note)
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
                summary,
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


def save_partner_standard_uploads(auftrag_id, files, upload_note="", success_message="Unterlage gespeichert und analysiert."):
    files = list(files or [])
    if not any(file and file.filename for file in files):
        flash("Bitte zuerst ein Bild oder Dokument auswählen.", "warning")
        return 0
    erlaubte_dateien = get_allowed_uploads(files)
    if not erlaubte_dateien:
        flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, TXT, DOCX oder XLSX verwenden.", "warning")
        return 0
    try:
        upload_result = save_uploads(
            auftrag_id,
            erlaubte_dateien,
            "autohaus",
            "standard",
            upload_note=upload_note,
        )
    except Exception as exc:
        upload_result = (
            0,
            {"_analysis_error": f"Upload/Analyse konnte nicht abgeschlossen werden: {clean_text(str(exc))[:300]}"},
        )
    return flash_upload_analysis_result(upload_result, success_message)


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
        if archiviert and auftrag_has_offene_reklamation(auftrag_id):
            continue
        archive_auftrag(auftrag_id, archiviert)
        geaendert += 1
    return geaendert


def archive_zurueckgegebene_ohne_offene_reklamation():
    db = get_db()
    try:
        cursor = db.execute(
            """
            UPDATE auftraege
            SET archiviert=1,
                geaendert_am=?
            WHERE archiviert=0
              AND status=5
              AND NOT EXISTS (
                SELECT 1
                FROM reklamationen r
                WHERE r.auftrag_id = auftraege.id
                  AND r.bearbeitet = 0
              )
            """,
            (now_str(),),
        )
        db.commit()
        count = int(cursor.rowcount or 0)
    finally:
        db.close()
    if count:
        schedule_change_backup("archive-zurueckgegeben")
    return count


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
    db.execute("DELETE FROM whatsapp_nachrichten WHERE auftrag_id=?", (auftrag_id,))
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
        date(year, 12, 25): "1. Weihnachtstag",
        date(year, 12, 26): "2. Weihnachtstag",
    }


def urlaubs_hinweise(year):
    hinweise = []
    feiertage = bw_feiertage(year)
    for feiertag, titel in sorted(feiertage.items()):
        if feiertag.weekday() == 3:
            freitag = feiertag + timedelta(days=1)
            if freitag.year == year:
                hinweise.append(
                    {
                        "datum": freitag,
                        "titel": f"Brückentag nach {titel}",
                        "notiz": "1 Urlaubstag ergibt mit dem Wochenende 4 freie Tage.",
                    }
                )
        elif feiertag.weekday() == 1:
            montag = feiertag - timedelta(days=1)
            if montag.year == year:
                hinweise.append(
                    {
                        "datum": montag,
                        "titel": f"Brückentag vor {titel}",
                        "notiz": "1 Urlaubstag ergibt mit dem Wochenende 4 freie Tage.",
                    }
                )

    weihnachten = date(year, 12, 25)
    if weihnachten.weekday() == 4:
        hinweise.append(
            {
                "datum": date(year, 12, 28),
                "titel": "Urlaub zwischen Weihnachten und Neujahr prüfen",
                "notiz": "28.12. bis 31.12. frei nehmen und bis Neujahr lang abschalten.",
            }
        )
    return hinweise


def infer_kalender_kategorie(text):
    normalized = clean_text(text).lower()
    if any(word in normalized for word in ("urlaub", "frei", "abwesend")):
        return "urlaub"
    if "geburt" in normalized:
        return "geburtstag"
    if any(word in normalized for word in ("heirat", "hochzeit", "privat")):
        return "privat"
    if any(word in normalized for word in ("geschlossen", "betrieb", "inventur")):
        return "betrieb"
    return "termin"


def parse_kalender_schnelleintrag(text):
    original = clean_text(text).replace(",", ".")
    if not original:
        raise ValueError("Bitte einen Eintrag schreiben.")

    match = re.search(r"\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\b", original)
    if not match:
        raise ValueError("Bitte ein Datum wie 28.05 oder 28.05.2026 angeben.")

    day = int(match.group(1))
    month = int(match.group(2))
    year_text = match.group(3)
    today = date.today()
    if year_text:
        year = int(year_text)
        if year < 100:
            year += 2000
    else:
        year = today.year

    try:
        parsed_date = date(year, month, day)
    except ValueError as exc:
        raise ValueError("Das Datum konnte nicht erkannt werden.") from exc

    rest = f"{original[:match.start()]} {original[match.end():]}".strip(" -:;")
    repeat_patterns = (
        r"\bjährlich\b",
        r"\bjaehrlich\b",
        r"\bjedes\s+jahr\b",
        r"\bjedes\s+jahr\s+wieder\b",
        r"\bgeburt\w*\b",
        r"\bhochzeitstag\b",
    )
    repeat = "jaehrlich" if any(re.search(pattern, rest.lower()) for pattern in repeat_patterns) else "einmalig"

    if not year_text and repeat == "einmalig" and parsed_date < today:
        parsed_date = date(today.year + 1, month, day)

    title = re.sub(
        r"\b(jährlich|jaehrlich|jedes\s+jahr(?:\s+wieder)?)\b",
        "",
        rest,
        flags=re.IGNORECASE,
    )
    title = compact_whitespace(title).strip(" -:;") or "Termin"

    return {
        "datum": parsed_date.strftime(DATE_FMT),
        "titel": title[:140],
        "notiz": original[:500],
        "kategorie": infer_kalender_kategorie(title),
        "wiederholung": repeat,
    }


def list_erinnerungen(status="offen", limit=12):
    db = get_db()
    rows = db.execute(
        """
        SELECT *
        FROM erinnerungen
        WHERE status=?
        ORDER BY erstellt_am DESC, id DESC
        LIMIT ?
        """,
        (clean_text(status) or "offen", int(limit)),
    ).fetchall()
    db.close()
    return [dict(row) for row in rows]


def create_erinnerung(text):
    text = clean_text(text)
    if not text:
        raise ValueError("Bitte eine Erinnerung eintragen.")
    db = get_db()
    db.execute(
        """
        INSERT INTO erinnerungen (text, status, erstellt_am, erledigt_am)
        VALUES (?, 'offen', ?, '')
        """,
        (text[:600], now_str()),
    )
    db.commit()
    db.close()
    schedule_change_backup("erinnerung")


def mark_erinnerung_erledigt(erinnerung_id):
    db = get_db()
    db.execute(
        "UPDATE erinnerungen SET status='erledigt', erledigt_am=? WHERE id=?",
        (now_str(), int(erinnerung_id)),
    )
    db.commit()
    changed = db.total_changes
    db.close()
    if changed:
        schedule_change_backup("erinnerung-erledigt")
    return bool(changed)


def create_kalender_notiz(datum, titel, notiz="", kategorie="termin", wiederholung="einmalig"):
    parsed = parse_date(datum)
    if not parsed:
        raise ValueError("Bitte ein gültiges Datum eintragen.")
    titel = clean_text(titel)
    if not titel:
        raise ValueError("Bitte einen Titel eintragen.")
    kategorie = clean_text(kategorie) or "termin"
    if kategorie not in KALENDER_KATEGORIEN:
        kategorie = "termin"
    wiederholung = clean_text(wiederholung) or "einmalig"
    if wiederholung not in {"einmalig", "jaehrlich"}:
        wiederholung = "einmalig"

    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO kalender_notizen
                (datum, titel, notiz, kategorie, wiederholung, erstellt_am)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                parsed.strftime(DATE_FMT),
                titel,
                clean_text(notiz),
                kategorie,
                wiederholung,
                now_str(),
            ),
        )
        db.commit()
        return cursor.lastrowid
    finally:
        db.close()


def delete_kalender_notiz(notiz_id):
    db = get_db()
    try:
        db.execute("DELETE FROM kalender_notizen WHERE id=?", (notiz_id,))
        db.commit()
    finally:
        db.close()


def list_kalender_notizen_raw():
    db = get_db()
    try:
        rows = db.execute(
            """
            SELECT *
            FROM kalender_notizen
            ORDER BY datum ASC, id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        db.close()


def kalender_jahre(auftraege):
    heute = date.today()
    years = {heute.year, heute.year + 1}
    for auftrag in auftraege:
        for feld, _, _ in EVENT_FELDER:
            event_date = auftrag.get(f"{feld}_obj")
            if event_date:
                years.add(event_date.year)
    for notiz in list_kalender_notizen_raw():
        parsed = parse_date(notiz.get("datum"))
        if parsed:
            years.add(parsed.year)
            if notiz.get("wiederholung") == "jaehrlich":
                years.add(heute.year)
                years.add(heute.year + 1)
    for urlaub in list_mitarbeiter_urlaub_raw(active_only=True):
        for feld in ("start_datum", "end_datum"):
            parsed = parse_date(urlaub.get(feld))
            if parsed:
                years.add(parsed.year)
    for news in list_werkstatt_news(limit=200):
        for feld in ("start_datum", "end_datum"):
            parsed = parse_date(news.get(feld))
            if parsed:
                years.add(parsed.year)
    return sorted(years)


def format_kalender_notiz(row, occurrence_date):
    kategorie = clean_text(row.get("kategorie")) or "termin"
    meta = KALENDER_KATEGORIEN.get(kategorie, KALENDER_KATEGORIEN["termin"])
    return {
        "id": row.get("id"),
        "datum": occurrence_date,
        "datum_text": occurrence_date.strftime(DATE_FMT),
        "titel": clean_text(row.get("titel")) or "Termin",
        "notiz": clean_text(row.get("notiz")),
        "kategorie": kategorie,
        "kategorie_label": meta["label"],
        "farbe": meta["farbe"],
        "wiederholung": clean_text(row.get("wiederholung")) or "einmalig",
        "system": False,
    }


def list_kalender_notizen(years):
    occurrences = []
    for row in list_kalender_notizen_raw():
        parsed = parse_date(row.get("datum"))
        if not parsed:
            continue
        if row.get("wiederholung") == "jaehrlich":
            for year in years:
                try:
                    occurrences.append(format_kalender_notiz(row, date(year, parsed.month, parsed.day)))
                except ValueError:
                    continue
        elif parsed.year in years:
            occurrences.append(format_kalender_notiz(row, parsed))
    return occurrences


def mitarbeiter_zeitraum_text(start, end):
    start_text = start.strftime(DATE_FMT) if start else ""
    end_text = end.strftime(DATE_FMT) if end else start_text
    if start_text and end_text and start_text != end_text:
        return f"{start_text} bis {end_text}"
    return start_text or end_text


def hydrate_mitarbeiter_urlaub(row):
    urlaub = dict(row)
    start = parse_date(urlaub.get("start_datum"))
    end = parse_date(urlaub.get("end_datum")) or start
    if start and end and end < start:
        start, end = end, start
    heute = date.today()
    urlaub["start_obj"] = start
    urlaub["end_obj"] = end
    urlaub["start_iso"] = iso_date(start) if start else ""
    urlaub["end_iso"] = iso_date(end) if end else ""
    urlaub["zeitraum_text"] = mitarbeiter_zeitraum_text(start, end)
    urlaub["ist_aktuell"] = bool(start and end and start <= heute <= end)
    urlaub["ist_vergangen"] = bool(end and end < heute)
    urlaub["notiz"] = clean_text(urlaub.get("notiz"))
    return urlaub


def hydrate_mitarbeiter(row):
    mitarbeiter = dict(row)
    mitarbeiter["aktiv"] = bool(mitarbeiter.get("aktiv"))
    mitarbeiter["name"] = clean_text(mitarbeiter.get("name"))
    mitarbeiter["rolle"] = clean_text(mitarbeiter.get("rolle"))
    mitarbeiter["telefon"] = clean_text(mitarbeiter.get("telefon"))
    mitarbeiter["email"] = clean_text(mitarbeiter.get("email"))
    mitarbeiter["adresse"] = clean_text(mitarbeiter.get("adresse"))
    mitarbeiter["geburtsdatum"] = clean_text(mitarbeiter.get("geburtsdatum"))
    mitarbeiter["geburtsort"] = clean_text(mitarbeiter.get("geburtsort"))
    mitarbeiter["staatsangehoerigkeit"] = clean_text(mitarbeiter.get("staatsangehoerigkeit"))
    mitarbeiter["eintritt_datum"] = clean_text(mitarbeiter.get("eintritt_datum"))
    mitarbeiter["austritt_datum"] = clean_text(mitarbeiter.get("austritt_datum"))
    mitarbeiter["beschaeftigung"] = clean_text(mitarbeiter.get("beschaeftigung"))
    mitarbeiter["qualifikation"] = clean_text(mitarbeiter.get("qualifikation"))
    mitarbeiter["arbeitszeit"] = clean_text(mitarbeiter.get("arbeitszeit"))
    mitarbeiter["urlaubsanspruch"] = clean_text(mitarbeiter.get("urlaubsanspruch"))
    mitarbeiter["ordner_pfad"] = clean_text(mitarbeiter.get("ordner_pfad"))
    mitarbeiter["dokumente_notiz"] = clean_text(mitarbeiter.get("dokumente_notiz"))
    mitarbeiter["notiz"] = clean_text(mitarbeiter.get("notiz"))
    return mitarbeiter


def list_mitarbeiter(include_inactive=True):
    db = get_db()
    try:
        if include_inactive:
            rows = db.execute(
                """
                SELECT *
                FROM mitarbeiter
                ORDER BY aktiv DESC, name ASC, id ASC
                """
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT *
                FROM mitarbeiter
                WHERE aktiv=1
                ORDER BY name ASC, id ASC
                """
            ).fetchall()
        urlaub_rows = db.execute(
            """
            SELECT u.*
            FROM mitarbeiter_urlaub u
            ORDER BY u.id DESC
            """
        ).fetchall()
    finally:
        db.close()

    heute = date.today()
    urlaube_by_mitarbeiter = defaultdict(list)
    for row in urlaub_rows:
        urlaub = hydrate_mitarbeiter_urlaub(row)
        urlaube_by_mitarbeiter[urlaub["mitarbeiter_id"]].append(urlaub)

    mitarbeiter_liste = []
    for row in sorted(
        rows,
        key=lambda item: (
            0 if item["aktiv"] else 1,
            clean_text(item["name"]).lower(),
            item["id"],
        ),
    ):
        mitarbeiter = hydrate_mitarbeiter(row)
        urlaube = urlaube_by_mitarbeiter.get(mitarbeiter["id"], [])
        urlaube.sort(
            key=lambda item: (
                item["end_obj"] < heute if item.get("end_obj") else True,
                item.get("start_obj") or date.max,
                item.get("id") or 0,
            )
        )
        mitarbeiter["urlaube"] = urlaube
        mitarbeiter["aktuelle_urlaube"] = [item for item in urlaube if item["ist_aktuell"]]
        mitarbeiter["kommende_urlaube"] = [
            item for item in urlaube if item.get("start_obj") and item["start_obj"] >= heute
        ]
        mitarbeiter_liste.append(mitarbeiter)
    return mitarbeiter_liste


def get_mitarbeiter(mitarbeiter_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM mitarbeiter WHERE id=?", (int(mitarbeiter_id),)).fetchone()
    finally:
        db.close()
    return hydrate_mitarbeiter(row) if row else None


def create_mitarbeiter(
    name,
    rolle="",
    telefon="",
    email="",
    adresse="",
    geburtsdatum="",
    geburtsort="",
    staatsangehoerigkeit="",
    eintritt_datum="",
    austritt_datum="",
    beschaeftigung="",
    qualifikation="",
    arbeitszeit="",
    urlaubsanspruch="",
    ordner_pfad="",
    dokumente_notiz="",
    notiz="",
    aktiv=True,
):
    name = clean_text(name)
    if not name:
        raise ValueError("Bitte den Namen des Mitarbeiters eintragen.")
    now = now_str()
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO mitarbeiter
                (name, rolle, telefon, email, adresse, geburtsdatum, geburtsort,
                 staatsangehoerigkeit, eintritt_datum, austritt_datum,
                 beschaeftigung, qualifikation, arbeitszeit, urlaubsanspruch,
                 ordner_pfad, dokumente_notiz, notiz, aktiv, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                clean_text(rolle),
                clean_text(telefon),
                clean_text(email),
                clean_text(adresse),
                format_date(geburtsdatum),
                clean_text(geburtsort),
                clean_text(staatsangehoerigkeit),
                format_date(eintritt_datum),
                format_date(austritt_datum),
                clean_text(beschaeftigung),
                clean_text(qualifikation),
                clean_text(arbeitszeit),
                clean_text(urlaubsanspruch),
                clean_text(ordner_pfad),
                clean_text(dokumente_notiz),
                clean_text(notiz),
                1 if aktiv else 0,
                now,
                now,
            ),
        )
        db.commit()
        schedule_change_backup("mitarbeiter")
        return cursor.lastrowid
    finally:
        db.close()


def update_mitarbeiter(
    mitarbeiter_id,
    name,
    rolle="",
    telefon="",
    email="",
    adresse="",
    geburtsdatum="",
    geburtsort="",
    staatsangehoerigkeit="",
    eintritt_datum="",
    austritt_datum="",
    beschaeftigung="",
    qualifikation="",
    arbeitszeit="",
    urlaubsanspruch="",
    ordner_pfad="",
    dokumente_notiz="",
    notiz="",
    aktiv=True,
):
    name = clean_text(name)
    if not name:
        raise ValueError("Bitte den Namen des Mitarbeiters eintragen.")
    db = get_db()
    try:
        cursor = db.execute(
            """
            UPDATE mitarbeiter
            SET name=?, rolle=?, telefon=?, email=?, adresse=?, geburtsdatum=?,
                geburtsort=?, staatsangehoerigkeit=?, eintritt_datum=?,
                austritt_datum=?, beschaeftigung=?, qualifikation=?, arbeitszeit=?,
                urlaubsanspruch=?, ordner_pfad=?, dokumente_notiz=?, notiz=?,
                aktiv=?, geaendert_am=?
            WHERE id=?
            """,
            (
                name,
                clean_text(rolle),
                clean_text(telefon),
                clean_text(email),
                clean_text(adresse),
                format_date(geburtsdatum),
                clean_text(geburtsort),
                clean_text(staatsangehoerigkeit),
                format_date(eintritt_datum),
                format_date(austritt_datum),
                clean_text(beschaeftigung),
                clean_text(qualifikation),
                clean_text(arbeitszeit),
                clean_text(urlaubsanspruch),
                clean_text(ordner_pfad),
                clean_text(dokumente_notiz),
                clean_text(notiz),
                1 if aktiv else 0,
                now_str(),
                int(mitarbeiter_id),
            ),
        )
        db.commit()
        schedule_change_backup("mitarbeiter")
        return getattr(cursor, "rowcount", 1)
    finally:
        db.close()


def create_mitarbeiter_urlaub(mitarbeiter_id, start_datum, end_datum="", notiz=""):
    mitarbeiter = get_mitarbeiter(mitarbeiter_id)
    if not mitarbeiter:
        raise ValueError("Mitarbeiter wurde nicht gefunden.")
    start = parse_date(start_datum)
    end = parse_date(end_datum) or start
    if not start:
        raise ValueError("Bitte ein gültiges Startdatum für den Urlaub eintragen.")
    if end and end < start:
        start, end = end, start
    if (end - start).days > 369:
        raise ValueError("Bitte den Urlaubszeitraum auf maximal 370 Tage begrenzen.")
    now = now_str()
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO mitarbeiter_urlaub
                (mitarbeiter_id, start_datum, end_datum, notiz, erstellt_am, geaendert_am)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(mitarbeiter_id),
                start.strftime(DATE_FMT),
                end.strftime(DATE_FMT),
                clean_text(notiz),
                now,
                now,
            ),
        )
        db.commit()
        schedule_change_backup("mitarbeiter-urlaub")
        return cursor.lastrowid
    finally:
        db.close()


def delete_mitarbeiter_urlaub(urlaub_id):
    db = get_db()
    try:
        db.execute("DELETE FROM mitarbeiter_urlaub WHERE id=?", (int(urlaub_id),))
        db.commit()
        schedule_change_backup("mitarbeiter-urlaub-loeschen")
    finally:
        db.close()


def list_mitarbeiter_urlaub_raw(active_only=True):
    db = get_db()
    try:
        where = "WHERE m.aktiv=1" if active_only else ""
        rows = db.execute(
            f"""
            SELECT u.*, m.name AS mitarbeiter_name, m.rolle AS mitarbeiter_rolle,
                   m.aktiv AS mitarbeiter_aktiv
            FROM mitarbeiter_urlaub u
            JOIN mitarbeiter m ON m.id = u.mitarbeiter_id
            {where}
            ORDER BY u.id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        db.close()


def mitarbeiter_urlaub_calendar_items(years):
    years = {int(year) for year in years}
    meta = KALENDER_KATEGORIEN["urlaub"]
    items = []
    for row in list_mitarbeiter_urlaub_raw(active_only=True):
        start = parse_date(row.get("start_datum"))
        end = parse_date(row.get("end_datum")) or start
        if not start:
            continue
        if end < start:
            start, end = end, start
        current = start
        days = 0
        name = clean_text(row.get("mitarbeiter_name")) or "Mitarbeiter"
        rolle = clean_text(row.get("mitarbeiter_rolle"))
        notiz_parts = []
        if rolle:
            notiz_parts.append(rolle)
        if clean_text(row.get("notiz")):
            notiz_parts.append(clean_text(row.get("notiz")))
        while current <= end and days < 370:
            if current.year in years:
                items.append(
                    {
                        "datum": current,
                        "datum_text": current.strftime(DATE_FMT),
                        "titel": f"{name} im Urlaub",
                        "notiz": " · ".join(notiz_parts),
                        "kategorie": "urlaub",
                        "kategorie_label": meta["label"],
                        "farbe": meta["farbe"],
                        "system": True,
                        "mitarbeiter_urlaub": True,
                        "mitarbeiter_id": row.get("mitarbeiter_id"),
                        "urlaub_id": row.get("id"),
                    }
                )
            current += timedelta(days=1)
            days += 1
    return items


def admin_mitarbeiter_urlaub_count():
    heute = date.today()
    try:
        count = 0
        for row in list_mitarbeiter_urlaub_raw(active_only=True):
            start = parse_date(row.get("start_datum"))
            end = parse_date(row.get("end_datum")) or start
            if start and end and end < start:
                start, end = end, start
            if start and end and start <= heute <= end:
                count += 1
        return count
    except Exception:
        return 0


def mitarbeiter_urlaub_summary(mitarbeiter_liste):
    heute = date.today()
    aktuelle = []
    kommende = []
    for mitarbeiter in mitarbeiter_liste:
        if not mitarbeiter.get("aktiv"):
            continue
        for urlaub in mitarbeiter.get("urlaube", []):
            item = dict(urlaub)
            item["mitarbeiter"] = mitarbeiter
            if item.get("ist_aktuell"):
                aktuelle.append(item)
            elif item.get("start_obj") and item["start_obj"] >= heute:
                kommende.append(item)
    kommende.sort(key=lambda item: (item.get("start_obj") or date.max, item.get("end_obj") or date.max))
    return {
        "aktuelle": aktuelle,
        "kommende": kommende[:8],
        "aktive_count": sum(1 for item in mitarbeiter_liste if item.get("aktiv")),
    }


def werkstatt_news_zeitraum_text(news):
    start_text = format_date(news.get("start_datum"))
    end_text = format_date(news.get("end_datum"))
    if start_text and end_text and start_text != end_text:
        return f"{start_text} bis {end_text}"
    return start_text or end_text


def hydrate_werkstatt_news(row):
    news = dict(row)
    news["start_datum_text"] = format_date(news.get("start_datum"))
    news["end_datum_text"] = format_date(news.get("end_datum"))
    news["start_datum_iso"] = iso_date(news.get("start_datum"))
    news["end_datum_iso"] = iso_date(news.get("end_datum"))
    news["zeitraum_text"] = werkstatt_news_zeitraum_text(news)
    news["kategorie"] = clean_text(news.get("kategorie")) or "betrieb"
    news["kategorie_meta"] = KALENDER_KATEGORIEN.get(news["kategorie"], KALENDER_KATEGORIEN["betrieb"])
    news["sichtbar"] = bool(news.get("sichtbar"))
    news["pinned"] = bool(news.get("pinned"))
    return news


def list_werkstatt_news(limit=80, visible_only=True):
    limit = max(1, min(int(limit or 80), 500))
    db = get_db()
    try:
        if visible_only:
            rows = db.execute(
                """
                SELECT *
                FROM werkstatt_news
                WHERE sichtbar=1
                ORDER BY pinned DESC,
                         CASE WHEN start_datum='' THEN '99.99.9999' ELSE start_datum END ASC,
                         id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT *
                FROM werkstatt_news
                ORDER BY sichtbar DESC, pinned DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    finally:
        db.close()
    return [hydrate_werkstatt_news(row) for row in rows]


def is_betriebsurlaub_news(news):
    text = normalize_document_text(
        " ".join(
            clean_text(part)
            for part in (
                news.get("titel"),
                news.get("nachricht"),
                news.get("kategorie"),
                (news.get("kategorie_meta") or {}).get("label"),
            )
            if clean_text(part)
        )
    )
    return "betriebsurlaub" in text or (
        "urlaub" in text and clean_text(news.get("kategorie")) in {"betrieb", "urlaub"}
    )


def planungszeitraum_from_values(*values):
    dates = [parse_date(value) for value in values if parse_date(value)]
    if not dates:
        return None, None
    return min(dates), max(dates)


def list_betriebsurlaub_konflikte(*date_values):
    plan_start, plan_end = planungszeitraum_from_values(*date_values)
    if not plan_start or not plan_end:
        return []
    konflikte = []
    seen = set()
    try:
        news_items = list_werkstatt_news(limit=200)
    except sqlite3.Error:
        return []
    for news in news_items:
        if not is_betriebsurlaub_news(news):
            continue
        start = parse_date(news.get("start_datum"))
        end = parse_date(news.get("end_datum")) or start
        if not start:
            continue
        if end < start:
            start, end = end, start
        if plan_start <= end and plan_end >= start and news.get("id") not in seen:
            konflikte.append(news)
            seen.add(news.get("id"))
    return konflikte


def flash_betriebsurlaub_planungshinweis(*date_values):
    for news in list_betriebsurlaub_konflikte(*date_values):
        zeitraum = news.get("zeitraum_text") or werkstatt_news_zeitraum_text(news)
        titel = clean_text(news.get("titel")) or "Betriebsurlaub"
        detail = f" {zeitraum}" if zeitraum else ""
        flash(
            f"Achtung Betriebsurlaub: {titel}{detail}. Bitte stimmen Sie die Einplanung mit der Werkstatt ab.",
            "warning",
        )


def analysis_loading_news(limit=6):
    items = []
    try:
        news_items = list_werkstatt_news(limit=limit)
    except sqlite3.Error:
        news_items = []
    for news in news_items:
        text = clean_text(news.get("nachricht"))
        if not text and news.get("zeitraum_text"):
            text = news["zeitraum_text"]
        if not text:
            continue
        items.append(
            {
                "title": clean_text(news.get("titel")) or "Werkstatt-News",
                "text": text,
            }
        )
    if items:
        return items
    return [
        {
            "title": clean_text(item.get("titel")) or "Werkstatt-News",
            "text": clean_text(item.get("nachricht")) or clean_text(item.get("start_datum")),
        }
        for item in DEFAULT_WERKSTATT_NEWS
        if clean_text(item.get("titel")) and (clean_text(item.get("nachricht")) or clean_text(item.get("start_datum")))
    ]


def get_werkstatt_news(news_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM werkstatt_news WHERE id=?", (int(news_id),)).fetchone()
    finally:
        db.close()
    return hydrate_werkstatt_news(row) if row else None


def create_werkstatt_news(titel, nachricht="", start_datum="", end_datum="", kategorie="betrieb", pinned=1):
    titel = clean_text(titel)
    if not titel:
        raise ValueError("Bitte einen Titel für die Werkstatt-News eintragen.")
    start = parse_date(start_datum)
    end = parse_date(end_datum) or start
    if start and end and end < start:
        start, end = end, start
    kategorie = clean_text(kategorie) or "betrieb"
    if kategorie not in KALENDER_KATEGORIEN:
        kategorie = "betrieb"
    now = now_str()
    db = get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO werkstatt_news
              (news_key, titel, nachricht, start_datum, end_datum, kategorie,
               sichtbar, pinned, erstellt_am, geaendert_am)
            VALUES ('', ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                titel,
                clean_text(nachricht),
                start.strftime(DATE_FMT) if start else "",
                end.strftime(DATE_FMT) if end else "",
                kategorie,
                1 if pinned else 0,
                now,
                now,
            ),
        )
        db.commit()
        schedule_change_backup("werkstatt-news")
        return cursor.lastrowid
    finally:
        db.close()


def archive_werkstatt_news(news_id):
    db = get_db()
    try:
        db.execute(
            "UPDATE werkstatt_news SET sichtbar=0, geaendert_am=? WHERE id=?",
            (now_str(), int(news_id)),
        )
        db.commit()
        schedule_change_backup("werkstatt-news-archiv")
    finally:
        db.close()


def werkstatt_news_calendar_items(years):
    years = {int(year) for year in years}
    items = []
    for news in list_werkstatt_news(limit=200):
        start = parse_date(news.get("start_datum"))
        end = parse_date(news.get("end_datum")) or start
        if not start:
            continue
        if end and end < start:
            end = start
        current = start
        days = 0
        meta = KALENDER_KATEGORIEN.get(news.get("kategorie"), KALENDER_KATEGORIEN["betrieb"])
        while current <= end and days < 370:
            if current.year in years:
                items.append(
                    {
                        "datum": current,
                        "datum_text": current.strftime(DATE_FMT),
                        "titel": clean_text(news.get("titel")) or "Werkstatt-News",
                        "notiz": clean_text(news.get("nachricht")),
                        "kategorie": news.get("kategorie") or "betrieb",
                        "kategorie_label": meta["label"],
                        "farbe": meta["farbe"],
                        "system": True,
                        "news": True,
                    }
                )
            current += timedelta(days=1)
            days += 1
    return items


def kalender_systemeintraege(years):
    items = []
    for year in years:
        for tag, titel in bw_feiertage(year).items():
            items.append(
                {
                    "datum": tag,
                    "datum_text": tag.strftime(DATE_FMT),
                    "titel": titel,
                    "notiz": "Gesetzlicher Feiertag in Baden-Württemberg.",
                    "kategorie": "feiertag",
                    "kategorie_label": KALENDER_KATEGORIEN["feiertag"]["label"],
                    "farbe": KALENDER_KATEGORIEN["feiertag"]["farbe"],
                    "system": True,
                }
            )
        for hinweis in urlaubs_hinweise(year):
            items.append(
                {
                    "datum": hinweis["datum"],
                    "datum_text": hinweis["datum"].strftime(DATE_FMT),
                    "titel": hinweis["titel"],
                    "notiz": hinweis["notiz"],
                    "kategorie": "hinweis",
                    "kategorie_label": KALENDER_KATEGORIEN["hinweis"]["label"],
                    "farbe": KALENDER_KATEGORIEN["hinweis"]["farbe"],
                    "system": True,
                }
            )
    items.extend(mitarbeiter_urlaub_calendar_items(years))
    items.extend(werkstatt_news_calendar_items(years))
    return items


def dashboard_daten(auftraege):
    heute = date.today()
    offene_verzoegerungen = []
    offene_reklamationen = []
    postfach_items = list_admin_postfach_items()
    offene_chat_nachrichten = [
        item for item in postfach_items if item.get("typ") == "Chat"
    ]

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
        "postfach_items": postfach_items,
        "postfach_count": len(postfach_items),
        "naechste_events": naechste_events[:12],
    }


def build_werkstatt_auftragsuebersicht(auftraege):
    autohaus_counts = {}
    planungsgruppen_defs = [
        {
            "key": "overdue",
            "label": "Überfällig",
            "hinweis": "Termine, die schon hätten passieren müssen.",
            "filter": "focus",
        },
        {
            "key": "today",
            "label": "Heute",
            "hinweis": "Anlieferung, Start, Fertigstellung oder Rückgabe heute.",
            "filter": "focus",
        },
        {
            "key": "upcoming",
            "label": "Als Nächstes",
            "hinweis": "Kommende Termine nach Datum sortiert.",
            "filter": "week",
        },
        {
            "key": "done",
            "label": "Fertig / Zurückgegeben",
            "hinweis": "Erledigte Fahrzeuge, teils bereit fürs Archiv.",
            "filter": "done",
        },
        {
            "key": "date-warning",
            "label": "Termin prüfen",
            "hinweis": "Auffällige Alt- oder Zukunftsdaten korrigieren.",
            "filter": "date-warning",
        },
        {
            "key": "empty",
            "label": "Ohne Termin",
            "hinweis": "Noch kein verwertbarer Termin gesetzt.",
            "filter": "empty",
        },
    ]
    planungsgruppen = {
        group["key"]: {**group, "count": 0} for group in planungsgruppen_defs
    }
    counts = {
        "gesamt": len(auftraege or []),
        "fokus": 0,
        "ueberfaellig": 0,
        "heute": 0,
        "woche": 0,
        "in_arbeit": 0,
        "fertig": 0,
        "zurueckgegeben": 0,
        "ohne_termin": 0,
        "termin_pruefen": 0,
        "reklamation": 0,
    }

    for auftrag in auftraege or []:
        planung = auftrag.get("planung") or {}
        group_key = planung.get("group_key") or "empty"
        status = int(auftrag.get("status") or 1)
        if group_key not in planungsgruppen:
            group_key = "empty"
        planungsgruppen[group_key]["count"] += 1

        if group_key == "overdue":
            counts["ueberfaellig"] += 1
        if group_key == "today":
            counts["heute"] += 1
        if group_key in {"overdue", "today"}:
            counts["fokus"] += 1
        if planung.get("is_week_relevant"):
            counts["woche"] += 1
        if status in {2, 3}:
            counts["in_arbeit"] += 1
        if status == 4:
            counts["fertig"] += 1
        if status >= 5:
            counts["zurueckgegeben"] += 1
        if group_key == "empty":
            counts["ohne_termin"] += 1
        if planung.get("has_date_warning"):
            counts["termin_pruefen"] += 1
        if auftrag.get("hat_offene_reklamation"):
            counts["reklamation"] += 1

        autohaus_id = int(auftrag.get("autohaus_id") or 0)
        name = clean_text(auftrag.get("autohaus_name")) or "Ohne Autohaus"
        if autohaus_id not in autohaus_counts:
            autohaus_counts[autohaus_id] = {"id": autohaus_id, "name": name, "count": 0}
        autohaus_counts[autohaus_id]["count"] += 1

    def group_items(filter_key, predicate, limit=3):
        matches = [auftrag for auftrag in auftraege or [] if predicate(auftrag)]
        return {
            "filter": filter_key,
            "count": len(matches),
            "items": matches[:limit],
            "more_count": max(0, len(matches) - limit),
        }

    arbeitsgruppen_defs = [
        {
            "key": "focus",
            "label": "Heute & fällig",
            "description": "Direkt entscheiden oder erledigen.",
            "empty": "Nichts Dringendes.",
            "tone": "urgent",
            **group_items(
                "focus",
                lambda auftrag: (auftrag.get("planung") or {}).get("group_key")
                in {"overdue", "today"},
            ),
        },
        {
            "key": "work",
            "label": "In Arbeit",
            "description": "Aktive Werkstatt-Aufträge.",
            "empty": "Gerade nichts aktiv.",
            "tone": "work",
            **group_items(
                "work",
                lambda auftrag: int(auftrag.get("status") or 1) in {2, 3},
            ),
        },
        {
            "key": "week",
            "label": "Nächste 7 Tage",
            "description": "Was in der Woche geplant ist.",
            "empty": "Keine Termine in den nächsten Tagen.",
            "tone": "week",
            **group_items(
                "week",
                lambda auftrag: bool((auftrag.get("planung") or {}).get("is_week_relevant")),
            ),
        },
        {
            "key": "date-warning",
            "label": "Prüfen",
            "description": "Unplausible oder fehlende Planung.",
            "empty": "Keine auffälligen Termine.",
            "tone": "check",
            **group_items(
                "date-warning",
                lambda auftrag: bool((auftrag.get("planung") or {}).get("has_date_warning")),
            ),
        },
        {
            "key": "empty",
            "label": "Ohne Termin",
            "description": "Noch nicht sauber eingeplant.",
            "empty": "Alle aktiven Fahrzeuge haben Termine.",
            "tone": "empty",
            **group_items(
                "empty",
                lambda auftrag: (auftrag.get("planung") or {}).get("group_key") == "empty",
            ),
        },
    ]

    autohaeuser = sorted(
        autohaus_counts.values(),
        key=lambda item: (-int(item["count"]), item["name"].lower()),
    )

    return {
        "counts": counts,
        "autohaeuser": autohaeuser,
        "arbeitsgruppen": arbeitsgruppen_defs,
        "planungsgruppen": list(planungsgruppen.values()),
        "planungsgruppen_by_key": planungsgruppen,
    }


def build_autohaus_uebersicht(autohaeuser, auftraege, angebotsanfragen=None, limit=12):
    heute = date.today()
    angebotsanfragen = angebotsanfragen or []
    rows = []
    for autohaus in autohaeuser or []:
        autohaus_id = int(autohaus.get("id") or 0)
        aktive = [a for a in auftraege if int(a.get("autohaus_id") or 0) == autohaus_id]
        offene_angebote = [
            a for a in angebotsanfragen if int(a.get("autohaus_id") or 0) == autohaus_id
        ]
        future_dates = []
        for auftrag in aktive + offene_angebote:
            for feld, _, _ in EVENT_FELDER:
                event_date = auftrag.get(f"{feld}_obj")
                if event_date and event_date >= heute:
                    future_dates.append(event_date)
        next_date = min(future_dates) if future_dates else None
        rows.append(
            {
                "id": autohaus_id,
                "name": clean_text(autohaus.get("name")) or "Autohaus",
                "slug": autohaus.get("slug"),
                "portal_label": autohaus.get("portal_label"),
                "aktiv_count": len(aktive),
                "angebot_count": len(offene_angebote),
                "postfach_count": partner_postfach_count(autohaus_id, autohaus.get("slug")),
                "naechster_termin": next_date.strftime(DATE_FMT) if next_date else "",
                "_sort_date": next_date or date.max,
            }
        )
    rows.sort(
        key=lambda item: (
            0 if item["postfach_count"] or item["angebot_count"] or item["aktiv_count"] else 1,
            item["_sort_date"],
            item["name"].lower(),
        )
    )
    for item in rows:
        item.pop("_sort_date", None)
    return rows[: max(1, int(limit or 12))]


def start_inbox_daten(postfach_items, email_items, limit=6, email_count_total=None):
    fahrzeuge = [item for item in postfach_items if item.get("typ") == "Fahrzeug"]
    anfragen = [item for item in postfach_items if item.get("typ") == "Anfrage"]
    sonstige_aufgaben = [
        item for item in postfach_items if item.get("typ") not in {"Fahrzeug", "Anfrage"}
    ]
    email_aufgaben = []
    for email in email_items:
        if not email.get("is_neu"):
            continue
        email_aufgaben.append(
            {
                "item_key": f"admin-email-{email.get('id')}",
                "typ": "E-Mail",
                "titel": email.get("betreff") or "Neue E-Mail",
                "nachricht": email.get("excerpt") or "Neue Aufgabe aus dem Werkstatt-Postfach.",
                "erstellt_am": email.get("empfangen_am") or email.get("erstellt_am"),
                "autohaus_name": email.get("absender_display"),
                "fahrzeug": "",
                "kennzeichen": "",
                "ziel_url": url_for("admin_emails"),
            }
        )

    email_count = len(email_aufgaben) if email_count_total is None else int(email_count_total or 0)
    alle_eingaenge = list(postfach_items) + email_aufgaben
    return {
        "fahrzeuge": fahrzeuge,
        "anfragen": anfragen,
        "aufgaben": sonstige_aufgaben + email_aufgaben,
        "fahrzeuge_count": len(fahrzeuge),
        "anfragen_count": len(anfragen),
        "aufgaben_count": len(sonstige_aufgaben) + email_count,
        "email_count": email_count,
        "items": sort_postfach_items(alle_eingaenge, limit),
    }


def kalender_daten(auftraege):
    tage = defaultdict(lambda: {"auftraege": {}, "notizen": [], "system": []})
    for auftrag in auftraege:
        for priority, (feld, label, farbe) in enumerate(EVENT_FELDER):
            event_date = auftrag.get(f"{feld}_obj")
            if event_date:
                eintraege = tage[event_date]["auftraege"]
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

    years = kalender_jahre(auftraege)
    for notiz in list_kalender_notizen(years):
        tage[notiz["datum"]]["notizen"].append(notiz)
    for item in kalender_systemeintraege(years):
        tage[item["datum"]]["system"].append(item)

    kalender = []
    for tag in sorted(tage.keys()):
        day_data = tage[tag]
        events = sorted(
            day_data["auftraege"].values(),
            key=lambda item: (
                clean_text(item["auftrag"].get("autohaus_name")).lower(),
                clean_text(item["auftrag"].get("kennzeichen")).lower(),
            ),
        )
        notizen = sorted(
            day_data["notizen"],
            key=lambda item: (clean_text(item.get("titel")).lower(), item.get("id") or 0),
        )
        system = sorted(
            day_data["system"],
            key=lambda item: (clean_text(item.get("kategorie")), clean_text(item.get("titel")).lower()),
        )
        kalender.append(
            {
                "datum": tag,
                "datum_lang": day_label(tag),
                "datum_text": tag.strftime(DATE_FMT),
                "events": events,
                "notizen": notizen,
                "system": system,
                "gesamt_count": len(events) + len(notizen) + len(system),
            }
        )
    return kalender


def filter_kalender_items(kalender_items, vergangenheit=False, reference_date=None):
    heute = reference_date or date.today()
    gefilterte_items = []
    for item in kalender_items:
        visible_system = [
            system_item
            for system_item in item.get("system", [])
            if system_item.get("kategorie") not in {"feiertag", "hinweis"}
        ]
        visible_item = dict(item)
        visible_item["system"] = visible_system
        visible_item["gesamt_count"] = (
            len(visible_item.get("events", []))
            + len(visible_item.get("notizen", []))
            + len(visible_system)
        )
        if visible_item["gesamt_count"] > 0:
            gefilterte_items.append(visible_item)

    if vergangenheit:
        return sorted(
            [item for item in gefilterte_items if item["datum"] < heute],
            key=lambda item: item["datum"],
            reverse=True,
        )
    return [item for item in gefilterte_items if item["datum"] >= heute]


def kalender_suchtext_auftrag(auftrag, event, day):
    texte = [
        day.get("datum_text"),
        day.get("datum_lang"),
        auftrag.get("id"),
        auftrag.get("fahrzeug"),
        auftrag.get("kennzeichen"),
        auftrag.get("kunde_name"),
        auftrag.get("autohaus_name"),
        auftrag.get("auftragsnummer"),
        auftrag.get("fin_nummer"),
        auftrag.get("analyse_text"),
        auftrag.get("beschreibung"),
    ]
    for feld, _, _ in EVENT_FELDER:
        texte.append(auftrag.get(feld))
    for termin in event.get("termine", []):
        texte.append(termin.get("label"))
        texte.append(termin.get("feld"))
    return " ".join(clean_text(text).lower() for text in texte if clean_text(text))


def kalender_suchtext_item(item, day):
    texte = [
        day.get("datum_text"),
        day.get("datum_lang"),
        item.get("titel"),
        item.get("notiz"),
        item.get("kategorie"),
        item.get("kategorie_label"),
    ]
    return " ".join(clean_text(text).lower() for text in texte if clean_text(text))


def filter_kalender_suche(kalender_items, query):
    suchtext = clean_text(query).lower()
    if not suchtext:
        return [], set()
    result = []
    event_ids = set()
    for day in kalender_items:
        day_match = suchtext in clean_text(day.get("datum_text")).lower() or suchtext in clean_text(day.get("datum_lang")).lower()
        events = []
        for event in day.get("events", []):
            auftrag = event.get("auftrag") or {}
            if day_match or suchtext in kalender_suchtext_auftrag(auftrag, event, day):
                events.append(event)
                event_ids.add(auftrag.get("id"))
        notizen = []
        for item in day.get("notizen", []):
            if day_match or suchtext in kalender_suchtext_item(item, day):
                notizen.append(item)
        system = []
        for item in day.get("system", []):
            if item.get("kategorie") == "feiertag":
                continue
            if day_match or suchtext in kalender_suchtext_item(item, day):
                system.append(item)
        gesamt_count = len(events) + len(notizen) + len(system)
        if gesamt_count:
            visible_day = dict(day)
            visible_day["events"] = events
            visible_day["notizen"] = notizen
            visible_day["system"] = system
            visible_day["gesamt_count"] = gesamt_count
            result.append(visible_day)
    return result, {event_id for event_id in event_ids if event_id}


def kalender_wochenuebersicht(kalender_items, reference_date=None):
    heute = reference_date or date.today()
    week_start = heute - timedelta(days=heute.weekday())
    week_end = week_start + timedelta(days=6)
    by_date = {}
    for item in kalender_items:
        parsed = item.get("datum") or parse_date(item.get("datum_text"))
        if parsed:
            by_date[parsed] = item

    days = []
    total_tasks = 0
    total_calendar_points = 0
    for offset in range(7):
        current = week_start + timedelta(days=offset)
        source = by_date.get(
            current,
            {"events": [], "notizen": [], "system": [], "gesamt_count": 0},
        )
        tasks = []
        for event in source.get("events", []):
            auftrag = event["auftrag"]
            for termin in event["termine"]:
                tasks.append(
                    {
                        "label": termin["label"],
                        "farbe": termin["farbe"],
                        "fahrzeug": clean_text(auftrag.get("fahrzeug")) or "Fahrzeug",
                        "kennzeichen": clean_text(auftrag.get("kennzeichen")),
                        "autohaus": clean_text(auftrag.get("autohaus_name")) or "Ohne Autohaus",
                        "auftrag_id": auftrag["id"],
                    }
                )
        calendar_points = sorted(
            list(source.get("system", [])) + list(source.get("notizen", [])),
            key=lambda item: (
                0
                if item.get("kategorie") in {"feiertag", "geburtstag", "urlaub"}
                else 1,
                clean_text(item.get("titel")).lower(),
            ),
        )
        total_tasks += len(tasks)
        total_calendar_points += len(calendar_points)
        days.append(
            {
                "datum": current,
                "datum_text": current.strftime(DATE_FMT),
                "wochentag": WOCHENTAGE[current.weekday()],
                "wochentag_kurz": WOCHENTAGE[current.weekday()][:2],
                "is_today": current == heute,
                "is_weekend": current.weekday() >= 5,
                "tasks": tasks,
                "calendar_points": calendar_points,
                "gesamt_count": len(tasks) + len(calendar_points),
            }
        )

    return {
        "kw": heute.isocalendar().week,
        "start_text": week_start.strftime(DATE_FMT),
        "end_text": week_end.strftime(DATE_FMT),
        "heute_text": heute.strftime(DATE_FMT),
        "days": days,
        "total_tasks": total_tasks,
        "total_calendar_points": total_calendar_points,
    }


def parse_mini_calendar_month(value):
    cleaned = clean_text(value)
    if cleaned:
        match = re.match(r"^(\d{4})-(\d{1,2})$", cleaned)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                try:
                    return date(year, month, 1)
                except ValueError:
                    pass
    today = date.today()
    return date(today.year, today.month, 1)


def shift_month(month_start, delta):
    month_index = (month_start.year * 12) + (month_start.month - 1) + int(delta)
    year = month_index // 12
    month = (month_index % 12) + 1
    return date(year, month, 1)


def build_mini_monatskalender(
    auftraege,
    month_value="",
    selected_day_value="",
    endpoint="",
    route_values=None,
    include_internal_notes=False,
    only_arrival_events=False,
):
    selected_day = parse_date(selected_day_value)
    month_start = parse_mini_calendar_month(month_value)
    if selected_day:
        month_start = date(selected_day.year, selected_day.month, 1)
    today = date.today()
    month_end = shift_month(month_start, 1) - timedelta(days=1)
    route_values = dict(route_values or {})
    cal = calendar.Calendar(firstweekday=0)
    event_dates = defaultdict(list)
    event_day_items = defaultdict(list)
    for auftrag in auftraege or []:
        event_fields = EVENT_FELDER
        if only_arrival_events:
            event_fields = (("annahme_datum", "Anlieferung", "secondary"),)
        for feld, label, farbe in event_fields:
            event_date = auftrag.get(f"{feld}_obj")
            if event_date and month_start <= event_date <= month_end:
                fahrzeug = clean_text(auftrag.get("fahrzeug")) or "Fahrzeug"
                kennzeichen = clean_text(auftrag.get("kennzeichen"))
                title = f"{label}: {fahrzeug}"
                if kennzeichen:
                    title = f"{title} | {kennzeichen}"
                subtitle_parts = []
                if clean_text(auftrag.get("autohaus_name")):
                    subtitle_parts.append(clean_text(auftrag.get("autohaus_name")))
                if only_arrival_events:
                    rueckgabe = (
                        auftrag.get("abholtermin_obj")
                        or parse_date(auftrag.get("abholtermin"))
                        or parse_date(auftrag.get("fertig_datum"))
                    )
                    if rueckgabe:
                        subtitle_parts.append(f"Rückgabe: {rueckgabe.strftime(DATE_FMT)}")
                item_url = ""
                if has_request_context():
                    if endpoint == "partner_dashboard" and route_values.get("slug"):
                        item_url = url_for(
                            "partner_auftrag",
                            slug=route_values["slug"],
                            auftrag_id=auftrag["id"],
                        )
                    elif endpoint == "betriebs_cockpit":
                        item_url = url_for("auftrag_detail", auftrag_id=auftrag["id"], back="kalender")
                event_dates[event_date].append(title)
                event_day_items[event_date].append(
                    {
                        "label": label,
                        "farbe": farbe,
                        "title": title,
                        "subtitle": " | ".join(subtitle_parts),
                        "url": item_url,
                    }
                )

    holidays = bw_feiertage(month_start.year)
    betriebsurlaub_dates = defaultdict(list)
    for news in list_werkstatt_news(limit=200):
        if not is_betriebsurlaub_news(news):
            continue
        start = parse_date(news.get("start_datum"))
        end = parse_date(news.get("end_datum")) or start
        if not start:
            continue
        if end < start:
            start, end = end, start
        current = max(start, month_start)
        last_day = min(end, month_end)
        while current <= last_day:
            betriebsurlaub_dates[current].append(clean_text(news.get("titel")) or "Betriebsurlaub")
            current += timedelta(days=1)

    note_dates = defaultdict(list)
    note_day_items = defaultdict(list)
    if include_internal_notes:
        for note in list_kalender_notizen([month_start.year]):
            note_date = note.get("datum") or parse_date(note.get("datum_text"))
            if note_date and month_start <= note_date <= month_end:
                title = clean_text(note.get("titel")) or "Kalendereintrag"
                note_dates[note_date].append(title)
                note_day_items[note_date].append(
                    {
                        "label": note.get("kategorie_label") or "Notiz",
                        "farbe": note.get("farbe") or "secondary",
                        "title": title,
                        "subtitle": clean_text(note.get("notiz")),
                        "url": "",
                    }
                )

    weeks = []
    for week in cal.monthdatescalendar(month_start.year, month_start.month):
        row = []
        for current in week:
            holiday_title = holidays.get(current)
            labels = []
            if holiday_title:
                labels.append(holiday_title)
            labels.extend(betriebsurlaub_dates.get(current, []))
            labels.extend(event_dates.get(current, [])[:3])
            labels.extend(note_dates.get(current, [])[:3])
            day_items = []
            if holiday_title:
                day_items.append(
                    {
                        "label": "Feiertag",
                        "farbe": "danger",
                        "title": holiday_title,
                        "subtitle": "",
                        "url": "",
                    }
                )
            for title in betriebsurlaub_dates.get(current, []):
                day_items.append(
                    {
                        "label": "Betriebsurlaub",
                        "farbe": "info",
                        "title": title,
                        "subtitle": "",
                        "url": "",
                    }
                )
            day_items.extend(event_day_items.get(current, []))
            day_items.extend(note_day_items.get(current, []))
            row.append(
                {
                    "datum": current,
                    "tag": current.day,
                    "datum_text": current.strftime(DATE_FMT),
                    "aria_label": day_label(current),
                    "in_month": current.month == month_start.month,
                    "is_selected": current == selected_day,
                    "is_today": current == today,
                    "is_weekend": current.weekday() >= 5,
                    "is_holiday": bool(holiday_title),
                    "has_betriebsurlaub": bool(betriebsurlaub_dates.get(current)),
                    "has_events": bool(event_dates.get(current)),
                    "has_notes": bool(note_dates.get(current)),
                    "tooltip": " | ".join(labels),
                    "items": day_items,
                    "url": (
                        url_for(
                            endpoint,
                            **route_values,
                            monat=current.strftime("%Y-%m"),
                            tag=current.strftime("%Y-%m-%d"),
                        )
                        if endpoint and has_request_context()
                        else ""
                    ),
                }
            )
        weeks.append(row)

    prev_month = shift_month(month_start, -1)
    next_month = shift_month(month_start, 1)
    prev_url = next_url = ""
    if endpoint and has_request_context():
        prev_url = url_for(endpoint, **route_values, monat=prev_month.strftime("%Y-%m"))
        next_url = url_for(endpoint, **route_values, monat=next_month.strftime("%Y-%m"))
    selected_day_data = None
    if selected_day:
        selected_items = []
        holiday_title = holidays.get(selected_day)
        if holiday_title:
            selected_items.append(
                {
                    "label": "Feiertag",
                    "farbe": "danger",
                    "title": holiday_title,
                    "subtitle": "",
                    "url": "",
                }
            )
        for title in betriebsurlaub_dates.get(selected_day, []):
            selected_items.append(
                {
                    "label": "Betriebsurlaub",
                    "farbe": "info",
                    "title": title,
                    "subtitle": "",
                    "url": "",
                }
            )
        selected_items.extend(event_day_items.get(selected_day, []))
        selected_items.extend(note_day_items.get(selected_day, []))
        selected_day_data = {
            "value": selected_day.strftime("%Y-%m-%d"),
            "datum_text": selected_day.strftime(DATE_FMT),
            "datum_lang": day_label(selected_day),
            "items": selected_items,
        }
    return {
        "title": f"{MONATSNAMEN[month_start.month]} {month_start.year}",
        "month_param": month_start.strftime("%Y-%m"),
        "prev_url": prev_url,
        "next_url": next_url,
        "today_text": today.strftime(DATE_FMT),
        "selected_day": selected_day_data,
        "weekdays": ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"],
        "weeks": weeks,
        "event_count": sum(len(items) for items in event_dates.values()),
        "holiday_count": sum(1 for day in holidays if day.month == month_start.month),
        "betriebsurlaub_count": len(betriebsurlaub_dates),
        "show_notes": include_internal_notes,
        "event_label": "Anlieferung" if only_arrival_events else "Fahrzeugtermin",
    }


def naechste_kalender_tage(auftraege, limit=6):
    result = []
    for tag in filter_kalender_items(kalender_daten(auftraege), vergangenheit=False):
        result.append(tag)
        if len(result) >= limit:
            break
    return result


def partner_termin_label(auftrag, feld, fallback):
    transport_art = clean_text(auftrag.get("transport_art"))
    if feld == "annahme_datum":
        if transport_art == "hol_und_bring":
            return "Abholung durch Werkstatt geplant"
        return "Anlieferung geplant"
    if feld == "start_datum":
        return "Arbeitsstart geplant"
    if feld == "fertig_datum":
        return "Fertigstellung geplant"
    if feld == "abholtermin":
        if transport_art == "hol_und_bring":
            return "Rückgabe geplant"
        return "Abholung geplant"
    return fallback


def partner_termin_tage(auftraege, limit=6, vergangenheit=False):
    heute = date.today()
    eintraege = []
    for auftrag in auftraege:
        termine = []
        felder = set()
        termin_daten = []
        for priority, (feld, label, farbe) in enumerate(EVENT_FELDER):
            event_date = auftrag.get(f"{feld}_obj")
            if not event_date:
                continue
            if vergangenheit and event_date >= heute:
                continue
            if not vergangenheit and event_date < heute:
                continue
            if feld not in felder:
                termine.append(
                    {
                        "feld": feld,
                        "label": label,
                        "termin_label": partner_termin_label(auftrag, feld, label),
                        "farbe": farbe,
                        "datum": event_date,
                        "datum_text": event_date.strftime(DATE_FMT),
                        "priority": priority,
                    }
                )
                felder.add(feld)
                termin_daten.append(event_date)
        if not termine:
            continue
        termine = sorted(termine, key=lambda termin: (termin["datum"], termin["priority"]))
        if vergangenheit:
            aktueller_termin = sorted(
                termine,
                key=lambda termin: (termin["datum"], termin["priority"]),
                reverse=True,
            )[0]
        else:
            aktueller_termin = termine[0]
        termin_daten = sorted(termin_daten)
        start = termin_daten[0]
        ende = termin_daten[-1]
        zeitraum_text = start.strftime(DATE_FMT)
        if ende != start:
            zeitraum_text = f"{start.strftime(DATE_FMT)} bis {ende.strftime(DATE_FMT)}"
        eintraege.append(
            {
                "auftrag": auftrag,
                "termine": termine,
                "aktueller_termin": aktueller_termin,
                "start_datum": start,
                "ende_datum": ende,
                "zeitraum_text": zeitraum_text,
                "gesamt_count": len(termine),
            }
        )
    if vergangenheit:
        eintraege.sort(
            key=lambda item: (
                item["ende_datum"],
                item["start_datum"],
                clean_text(item["auftrag"].get("fahrzeug")).lower(),
                clean_text(item["auftrag"].get("kennzeichen")).lower(),
            ),
            reverse=True,
        )
    else:
        eintraege.sort(
            key=lambda item: (
                item["start_datum"],
                item["ende_datum"],
                clean_text(item["auftrag"].get("fahrzeug")).lower(),
                clean_text(item["auftrag"].get("kennzeichen")).lower(),
            )
        )
    return eintraege[:limit]


def partner_naechste_termin_tage(auftraege, limit=6):
    return partner_termin_tage(auftraege, limit=limit)


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
    if request.method == "GET" and session.get("admin"):
        return redirect(url_for("betriebs_cockpit"))
    if request.method == "POST":
        limited, wait_seconds = login_rate_limit_status("admin", "admin")
        if limited:
            flash(f"Zu viele Fehlversuche. Bitte in {login_wait_label(wait_seconds)} erneut versuchen.", "danger")
            return render_template("login.html"), 429
        submitted_password = request.form.get("password") or request.form.get("passwort")
        if admin_password_matches(submitted_password):
            clear_login_attempts("admin", "admin")
            session.clear()
            session.permanent = True
            session["admin"] = True
            response = redirect(url_for("betriebs_cockpit"))
            remember_authenticated_login(response, "admin")
            return response
        record_failed_login("admin", "admin")
        flash("Falsches Passwort.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    response = redirect(url_for("login"))
    clear_remember_login_cookie(response, "admin")
    clear_remember_login_cookie(response, "partner")
    return response


@app.route("/session/ping", methods=["POST"])
def session_ping():
    if not session_is_authenticated():
        return jsonify({"ok": False, "authenticated": False}), 401
    mark_authenticated_session_active()
    return jsonify(
        {
            "ok": True,
            "authenticated": True,
            "idle_timeout_seconds": SESSION_IDLE_TIMEOUT_MINUTES * 60,
        }
    )


@app.route("/webhooks/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    if request.method == "GET":
        mode = clean_text(request.args.get("hub.mode"))
        token = clean_text(request.args.get("hub.verify_token"))
        challenge = clean_text(request.args.get("hub.challenge"))
        if mode == "subscribe" and WHATSAPP_VERIFY_TOKEN and hmac.compare_digest(token, WHATSAPP_VERIFY_TOKEN):
            return challenge, 200, {"Content-Type": "text/plain; charset=utf-8"}
        abort(403)

    if not whatsapp_bridge_enabled():
        return jsonify({"ok": False, "enabled": False, "processed": 0})
    if not verify_whatsapp_signature():
        abort(403)
    payload = request.get_json(silent=True) or {}
    processed = process_whatsapp_webhook(payload)
    return jsonify({"ok": True, "processed": processed})


@app.route("/favicon.ico")
def favicon():
    logo_path = BASE / "static" / "logo.png"
    if not logo_path.exists():
        abort(404)
    return send_file(logo_path, mimetype="image/png")


def get_mosbach_weather_payload(force_refresh=False):
    now = time.time()
    with WEATHER_CACHE_LOCK:
        cached_payload = WEATHER_CACHE.get("payload")
        if cached_payload and not force_refresh and WEATHER_CACHE.get("expires_at", 0) > now:
            return cached_payload, 200

    requests_module = get_requests()
    if requests_module is None:
        if cached_payload:
            stale_payload = dict(cached_payload)
            stale_payload["stale"] = True
            return stale_payload, 200
        return {"ok": False, "error": "HTTP-Client fuer Wetterdaten nicht verfuegbar."}, 503

    try:
        response = requests_module.get(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude": MOSBACH_WEATHER_LOCATION["latitude"],
                "longitude": MOSBACH_WEATHER_LOCATION["longitude"],
                "current": "temperature_2m,weather_code,wind_speed_10m",
                "hourly": "temperature_2m,weather_code,wind_speed_10m",
                "timezone": "Europe/Berlin",
                "forecast_days": 2,
            },
            timeout=WEATHER_API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        raw_payload = response.json()
        payload = {
            "ok": True,
            "location": MOSBACH_WEATHER_LOCATION,
            "current": raw_payload.get("current") or {},
            "hourly": raw_payload.get("hourly") or {},
            "units": {
                "current": raw_payload.get("current_units") or {},
                "hourly": raw_payload.get("hourly_units") or {},
            },
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        with WEATHER_CACHE_LOCK:
            WEATHER_CACHE["payload"] = payload
            WEATHER_CACHE["expires_at"] = now + WEATHER_CACHE_SECONDS
        return payload, 200
    except Exception as exc:
        if cached_payload:
            stale_payload = dict(cached_payload)
            stale_payload["stale"] = True
            return stale_payload, 200
        return {"ok": False, "error": clean_text(str(exc))[:240]}, 503


@app.route("/api/wetter/mosbach")
def wetter_mosbach():
    payload, status_code = get_mosbach_weather_payload()
    return jsonify(payload), status_code


@app.route("/")
@app.route("/admin")
@admin_required
def dashboard():
    alle_auftraege = list_auftraege(include_archived=True)
    auftraege = [a for a in alle_auftraege if not a["archiviert"]]
    archivierte_auftraege = [a for a in alle_auftraege if a["archiviert"]]
    return render_template(
        "dashboard.html",
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        zurueck_archivierbar_count=sum(1 for a in auftraege if a.get("archivierbar_zurueckgegeben")),
    )


@app.route("/admin/start")
@app.route("/admin/cockpit")
@admin_required
def betriebs_cockpit():
    alle_auftraege = list_auftraege(include_archived=True)
    auftraege = [a for a in alle_auftraege if not a["archiviert"]]
    autohaeuser = list_autohaeuser()
    angebotsanfragen = list_angebotsanfragen()
    cockpit_data = dashboard_daten(auftraege)
    current_datetime = datetime.now()
    mini_calendar = build_mini_monatskalender(
        auftraege,
        request.args.get("monat"),
        request.args.get("tag"),
        endpoint="betriebs_cockpit",
        include_internal_notes=True,
    )
    mini_calendar["full_url"] = url_for("kalender")
    return render_template(
        "cockpit.html",
        auftraege=auftraege,
        archivierte_auftraege=[a for a in alle_auftraege if a["archiviert"]],
        angebotsanfragen=angebotsanfragen,
        autohaeuser=autohaeuser,
        cockpit=cockpit_data,
        start_inbox=start_inbox_daten(
            cockpit_data["postfach_items"],
            [],
            email_count_total=0,
        ),
        erinnerungen=list_erinnerungen(limit=8),
        ki_status=get_ai_status(),
        database_status=get_database_status(),
        zurueck_archivierbar_count=sum(1 for a in auftraege if a.get("archivierbar_zurueckgegeben")),
        mini_calendar=mini_calendar,
        current_datetime_iso=current_datetime.isoformat(timespec="seconds"),
        current_time_label=current_datetime.strftime("%H:%M:%S"),
        current_date_label=f"{WOCHENTAGE[current_datetime.weekday()]}, {current_datetime.strftime(DATE_FMT)}",
        ki_assistent_chat_url=url_for("admin_ki_chat"),
        ki_assistent_clear_url=url_for("admin_ki_chat_loeschen"),
        ki_assistent_subtitle="Fragen zu Auftrag, Upload oder Portal.",
    )


@app.route("/admin/cockpit/auftraege-suche")
@admin_required
def admin_cockpit_auftraege_suche():
    return jsonify({"items": list_admin_auftrag_suche(request.args.get("q"), limit=8)})


@app.route("/admin/postfach")
@admin_required
def admin_postfach():
    return render_template("postfach_admin.html", items=list_admin_postfach_items(limit=200))


@app.route("/admin/postfach/<path:item_key>/oeffnen")
@admin_required
def admin_postfach_oeffnen(item_key):
    item_key = clean_text(item_key)
    item = next(
        (entry for entry in list_admin_postfach_items(limit=200) if entry.get("item_key") == item_key),
        None,
    )
    if not item:
        flash("Die Meldung ist bereits erledigt oder nicht mehr vorhanden.", "info")
        return redirect(url_for("admin_postfach"))
    hide_postfach_item("admin", item_key, 0)
    return redirect(item.get("ziel_url") or url_for("admin_postfach"))


@app.route("/admin/zugaenge")
@admin_required
def admin_zugaenge():
    return render_template("zugaenge.html", autohaeuser=list_autohaeuser())


@app.route("/admin/ki/chat", methods=["POST"])
@admin_required
def admin_ki_chat():
    payload = request.get_json(silent=True) or {}
    question = clean_text(payload.get("message"))[:900]
    if not question:
        return jsonify({"error": "Bitte eine Frage eingeben."}), 400
    save_ki_assistent_message(0, 0, "kunde", question)
    answer, source = ask_admin_assistant(question)
    save_ki_assistent_message(0, 0, "ki", answer)
    schedule_change_backup("admin-ki-assistent")
    return jsonify({"answer": answer, "source": source})


@app.route("/admin/ki/chat/loeschen", methods=["POST"])
@admin_required
def admin_ki_chat_loeschen():
    clear_ki_assistent_history(0, 0)
    schedule_change_backup("admin-ki-assistent-loeschen")
    return jsonify({"ok": True})


@app.route("/admin/postfach/<path:item_key>/loeschen", methods=["POST"])
@admin_required
def admin_postfach_loeschen(item_key):
    hide_postfach_item("admin", item_key, 0)
    flash("Nachricht aus dem Werkstatt-Postfach gelöscht.", "info")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("admin_postfach"))


@app.route("/admin/cockpit/eingang/<path:item_key>/oeffnen")
@admin_required
def admin_cockpit_eingang_oeffnen(item_key):
    item_key = clean_text(item_key)
    if item_key.startswith("admin-email-"):
        email_id_text = item_key.removeprefix("admin-email-")
        if email_id_text.isdigit() and get_werkstatt_email(int(email_id_text)):
            update_werkstatt_email_status(int(email_id_text), "erledigt")
        return redirect(url_for("admin_emails"))
    item = next(
        (entry for entry in list_admin_postfach_items(limit=200) if entry.get("item_key") == item_key),
        None,
    )
    if item:
        hide_postfach_item("admin", item_key, 0)
        return redirect(item.get("ziel_url") or url_for("admin_postfach"))
    flash("Die Meldung ist bereits erledigt oder nicht mehr vorhanden.", "info")
    return redirect(url_for("betriebs_cockpit"))


@app.route("/admin/cockpit/eingang/<path:item_key>/loeschen", methods=["POST"])
@admin_required
def admin_cockpit_eingang_loeschen(item_key):
    item_key = clean_text(item_key)
    if item_key.startswith("admin-email-"):
        email_id_text = item_key.removeprefix("admin-email-")
        if not email_id_text.isdigit() or not get_werkstatt_email(int(email_id_text)):
            abort(404)
        update_werkstatt_email_status(int(email_id_text), "erledigt")
        flash("E-Mail aus dem Cockpit entfernt.", "info")
    else:
        hide_postfach_item("admin", item_key, 0)
        flash("Meldung aus dem Cockpit entfernt.", "info")

    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin"):
        return redirect(next_url)
    return redirect(url_for("betriebs_cockpit"))


@app.route("/admin/einkauf")
@admin_required
def admin_einkauf():
    raw_offene_items = list_einkauf_items("offen", limit=200)
    offene_items, unklare_items = split_einkauf_items_by_orderability(raw_offene_items)
    bestellte_items = list_einkauf_items("bestellt", limit=80)
    raw_rechnung_pruef_items = list_einkauf_items(EINKAUF_RECHNUNG_PRUEF_STATUS, limit=500)
    rechnung_pruef_items = filter_rechnung_pruef_items_without_artikel(raw_rechnung_pruef_items)
    rechnungsbelege = list_einkauf_belege(limit=20)
    gespeicherte_artikel = list_einkauf_artikel(limit=200)
    artikel_altlasten = list_einkauf_artikel_altlasten(limit=200)
    artikel_gruppen = group_einkauf_artikel_by_kategorie(gespeicherte_artikel)
    topcolor_email = get_topcolor_email()
    mail_draft = build_topcolor_order_draft(offene_items, topcolor_email)
    api_draft = build_supplier_api_request_draft(topcolor_email)
    return render_template(
        "einkauf.html",
        offene_items=offene_items,
        unklare_items=unklare_items,
        bestellte_items=bestellte_items,
        rechnung_pruef_items=rechnung_pruef_items,
        rechnung_pruef_hidden_count=max(0, len(raw_rechnung_pruef_items) - len(rechnung_pruef_items)),
        rechnungsbelege=rechnungsbelege,
        gespeicherte_artikel=gespeicherte_artikel,
        artikel_altlasten=artikel_altlasten,
        verworfene_artikel_count=count_verworfene_einkauf_artikel(),
        artikel_gruppen=artikel_gruppen,
        topcolor_email=topcolor_email,
        mail_draft=mail_draft,
        api_draft=api_draft,
        kategorien=EINKAUF_KATEGORIEN,
        ve_optionen=EINKAUF_VE_OPTIONEN,
        angebotsstatus=EINKAUF_ANGEBOTSSTATUS,
        items_by_lieferant=group_einkauf_items_by_lieferant(offene_items),
        einkauf_offer_groups=build_einkauf_offer_groups(offene_items, limit=20),
        beleg_items=rechnungsbelege,
        artikel_items=gespeicherte_artikel,
    )


@app.route("/admin/rechnungen")
@admin_required
def admin_rechnungen():
    sync_result = maybe_sync_lexware_rechnungen()
    summary = lexware_rechnungen_summary()
    return render_template(
        "rechnungen_admin.html",
        rechnungen=list_lexware_rechnungen("kritisch", limit=250),
        bezahlte_rechnungen=list_lexware_rechnungen("bezahlt", limit=40),
        summary=summary,
        sync_result=sync_result,
        lexware_api_ready=bool(LEXWARE_API_KEY),
        lexware_api_base_url=LEXWARE_API_BASE_URL,
        auto_sync_minutes=LEXWARE_AUTO_SYNC_MINUTES,
        kontoauszug_importe=list_kontoauszug_importe(limit=8),
        kontoauszug_buchungen=list_kontoauszug_buchungen(limit=80),
        kontoauszug_auswertung=kontoauszug_auswertung(),
    )


@app.route("/admin/rechnungen/sync", methods=["POST"])
@admin_required
def admin_rechnungen_sync():
    result = sync_lexware_rechnungen()
    flash(result["message"], "success" if result.get("ok") else "warning")
    if result.get("errors"):
        flash(result["errors"][0], "warning")
    return redirect(url_for("admin_rechnungen"))


@app.route("/admin/rechnungen/kontoauszug", methods=["POST"])
@admin_required
def admin_rechnungen_kontoauszug_upload():
    files = [
        file
        for file in request.files.getlist("kontoauszug_dateien")
        if clean_text(getattr(file, "filename", ""))
    ]
    if not files:
        flash("Bitte mindestens einen Kontoauszug hochladen.", "warning")
        return redirect(url_for("admin_rechnungen") + "#kontoauszug")

    total_rows = 0
    total_matched = 0
    errors = []
    for file in files:
        try:
            result = import_kontoauszug(file)
            total_rows += int(result.get("buchungen") or 0)
            total_matched += int(result.get("matched") or 0)
            flash(f"{clean_text(getattr(file, 'filename', 'Kontoauszug'))}: {result['message']}", "success")
        except ValueError as exc:
            errors.append(str(exc))
        except Exception as exc:
            errors.append(f"{clean_text(getattr(file, 'filename', 'Kontoauszug'))}: {exc}")

    for error in errors:
        flash(error, "warning")
    if total_rows:
        flash(f"Insgesamt {total_rows} Buchung(en) analysiert, {total_matched} Rechnung(en) als erledigt markiert.", "info")
    return redirect(url_for("admin_rechnungen") + "#kontoauszug")


@app.route("/admin/einkauf/<int:item_id>")
@admin_required
def admin_einkauf_detail(item_id):
    item = get_einkauf_item(item_id)
    if not item:
        abort(404)
    return render_template(
        "einkauf_detail.html",
        item=item,
        kategorien=EINKAUF_KATEGORIEN,
        ve_optionen=EINKAUF_VE_OPTIONEN,
        angebotsstatus=EINKAUF_ANGEBOTSSTATUS,
    )


@app.route("/admin/einkauf/<int:item_id>/vergleich/<anbieter>")
@admin_required
def admin_einkauf_vergleich(item_id, anbieter):
    item = get_einkauf_item(item_id)
    if not item:
        abort(404)
    url_key = EINKAUF_VERGLEICH_ANBIETER.get(clean_text(anbieter).lower())
    if not url_key:
        abort(404)
    ziel_url = clean_text(item.get(url_key))
    if not ziel_url.startswith("https://"):
        abort(404)
    return redirect(ziel_url)


@app.route("/admin/einkauf/artikel/<int:artikel_id>/vergleich/<anbieter>")
@admin_required
def admin_einkauf_artikel_vergleich(artikel_id, anbieter):
    artikel = get_einkauf_artikel(artikel_id)
    if not artikel:
        abort(404)
    url_key = EINKAUF_VERGLEICH_ANBIETER.get(clean_text(anbieter).lower())
    if not url_key:
        abort(404)
    ziel_url = clean_text(artikel.get(url_key))
    if not ziel_url.startswith("https://"):
        abort(404)
    return redirect(ziel_url)


@app.route("/admin/einkauf/artikel/<int:artikel_id>/verwerfen", methods=["POST"])
@admin_required
def admin_einkauf_artikel_verwerfen(artikel_id):
    if not mark_einkauf_artikel_verworfen(artikel_id):
        abort(404)
    flash("Artikel wurde aus dem Artikelstamm ausgeblendet.", "info")
    return redirect(url_for("admin_einkauf") + "#angelegte-teile")


@app.route("/admin/einkauf/artikel/altlasten-ausblenden", methods=["POST"])
@admin_required
def admin_einkauf_artikel_altlasten_ausblenden():
    count = cleanup_einkauf_artikel_altlasten()
    if count:
        flash(f"{count} alte OCR-/Rechnungszeile(n) im Artikelstamm ausgeblendet.", "info")
    else:
        flash("Keine OCR-Altlasten im Artikelstamm gefunden.", "success")
    return redirect(url_for("admin_einkauf") + "#angelegte-teile")


@app.route("/admin/einkauf/topcolor-email", methods=["POST"])
@admin_required
def admin_einkauf_topcolor_email():
    set_topcolor_email(request.form.get("topcolor_email"))
    flash("Topcolor-E-Mail gespeichert.", "success")
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/neu", methods=["POST"])
@admin_required
def admin_einkauf_neu():
    payload = einkauf_form_payload(request.form)
    files = [
        file
        for file in request.files.getlist("qr_bilder")
        if clean_text(getattr(file, "filename", ""))
    ]
    has_text_item = any(
        clean_text(payload.get(key))
        for key in (
            "titel",
            "produkt_name",
            "artikelnummer",
            "menge",
            "notiz",
            "angebotsnotiz",
        )
    )
    if not files and not has_text_item:
        flash("Bitte mindestens ein QR-Bild, eine Datei oder eine Materialnotiz eintragen.", "warning")
        return redirect(url_for("admin_einkauf"))

    created = 0
    errors = []
    if files:
        for file in files:
            try:
                create_einkauf_item(file_storage=file, **payload)
                created += 1
            except ValueError as exc:
                errors.append(str(exc))
    else:
        create_einkauf_item(**payload)
        created = 1

    for error in errors:
        flash(error, "warning")
    if created:
        flash(f"{created} Einkaufsposition gespeichert.", "success")
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/rechnung", methods=["POST"])
@admin_required
def admin_einkauf_rechnung_upload():
    lieferant = clean_text(request.form.get("lieferant")) or "Auto-Color / Topcolor"
    files = [
        file
        for file in (request.files.getlist("rechnung_dateien") or request.files.getlist("rechnungen"))
        if clean_text(getattr(file, "filename", ""))
    ]
    next_url = clean_text(request.form.get("next"))
    target_url = next_url if next_url.startswith("/admin") else url_for("admin_einkauf") + "#rechnungen"
    if not files:
        flash("Bitte mindestens eine Rechnung oder einen Beleg auswählen.", "warning")
        return redirect(target_url)

    imported = 0
    saved = 0
    errors = []
    for file in files:
        try:
            result = import_einkauf_rechnung(file, lieferant=lieferant)
            saved += 1 if result.get("beleg") else 0
            imported += int(result.get("created") or 0)
            flash(result["message"], "success" if result.get("ok") else "warning")
        except ValueError as exc:
            errors.append(str(exc))

    for error in errors:
        flash(error, "warning")
    if saved and not imported:
        flash("Die Rechnung wurde nur fuer die Produktanlage gespeichert. Bitte Artikel manuell ergaenzen, falls nichts erkannt wurde.", "info")
    elif imported:
        flash(f"{imported} Produktposition(en) warten auf Artikel anlegen.", "success")
    return redirect(target_url)


@app.route("/admin/einkauf/<int:item_id>/artikel-anlegen", methods=["POST"])
@admin_required
def admin_einkauf_artikel_anlegen(item_id):
    item = get_einkauf_item(item_id)
    if not item:
        abort(404)
    if not update_einkauf_item(item_id, sync_artikel=False, **einkauf_form_payload(request.form)):
        abort(404)
    artikel_id = sync_einkauf_item_to_artikel(item_id, increment_count=1)
    if not artikel_id:
        flash("Artikel konnte nicht angelegt werden. Bitte Produktname oder Artikelnummer pruefen.", "warning")
        return redirect(url_for("admin_einkauf") + "#produktanlage")
    db = get_db()
    try:
        db.execute(
            "UPDATE einkaufsliste SET status='bestellt', angebotsstatus='bestellt', bestellt_am=? WHERE id=?",
            (now_str(), int(item_id)),
        )
        db.commit()
    finally:
        db.close()
    schedule_change_backup("einkauf-artikel-anlegen")
    flash("Artikel wurde im Artikelstamm angelegt.", "success")
    return redirect(url_for("admin_einkauf") + "#angelegte-teile")


@app.route("/admin/einkauf/<int:item_id>/bearbeiten", methods=["POST"])
@admin_required
def admin_einkauf_bearbeiten(item_id):
    if not update_einkauf_item(item_id, **einkauf_form_payload(request.form)):
        abort(404)
    flash("Einkaufsposition aktualisiert.", "success")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin/einkauf"):
        return redirect(next_url)
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/<int:item_id>/analysieren", methods=["POST"])
@admin_required
def admin_einkauf_analysieren(item_id):
    result = analyse_einkauf_item(item_id, force=True)
    flash(result["message"], "success" if result["ok"] else "warning")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin/einkauf"):
        return redirect(next_url)
    return redirect(url_for("admin_einkauf") + f"#einkauf-position-{item_id}")


@app.route("/admin/einkauf/alle-analysieren", methods=["POST"])
@admin_required
def admin_einkauf_alle_analysieren():
    ok_count, total_count = analyse_offene_einkauf_items(force=True)
    if total_count:
        flash(f"{ok_count} von {total_count} Einkaufsbildern analysiert.", "success" if ok_count else "warning")
    else:
        flash("Keine offenen Einkaufsbilder für die Analyse gefunden.", "info")
    return redirect(url_for("admin_einkauf") + "#offene-einkaufspositionen")


@app.route("/admin/einkauf/<int:item_id>/bestellt", methods=["POST"])
@admin_required
def admin_einkauf_bestellt(item_id):
    if not get_einkauf_item(item_id):
        abort(404)
    mark_einkauf_item_bestellt(item_id)
    flash("Einkaufsposition als bestellt markiert.", "success")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin/einkauf"):
        return redirect(next_url)
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/alle-bestellt", methods=["POST"])
@admin_required
def admin_einkauf_alle_bestellt():
    mark_all_einkauf_items_bestellt()
    flash("Alle offenen Einkaufspositionen wurden als bestellt markiert.", "success")
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/<int:item_id>/loeschen", methods=["POST"])
@admin_required
def admin_einkauf_loeschen(item_id):
    if not delete_einkauf_item(item_id):
        abort(404)
    flash("Einkaufsposition gelöscht.", "info")
    return redirect(url_for("admin_einkauf"))


@app.route("/admin/einkauf/datei/<int:item_id>")
@admin_required
def admin_einkauf_datei(item_id):
    item = get_einkauf_item(item_id)
    if not item or not clean_text(item.get("stored_name")):
        abort(404)
    path = UPLOAD_DIR / pathlib.Path(item["stored_name"]).name
    if not path.exists() or not path.is_file():
        abort(404)
    mimetype = clean_text(item.get("mime_type")) or mimetypes.guess_type(item["original_name"])[0]
    return send_file(
        path,
        mimetype=mimetype,
        as_attachment=False,
        download_name=clean_text(item.get("original_name")) or path.name,
    )


@app.route("/admin/einkauf/beleg/datei/<int:beleg_id>")
@admin_required
def admin_einkauf_beleg_datei(beleg_id):
    beleg = get_einkauf_beleg(beleg_id)
    if not beleg or not clean_text(beleg.get("stored_name")):
        abort(404)
    path = UPLOAD_DIR / pathlib.Path(beleg["stored_name"]).name
    if not path.exists() or not path.is_file():
        abort(404)
    mimetype = clean_text(beleg.get("mime_type")) or mimetypes.guess_type(beleg["original_name"])[0]
    return send_file(
        path,
        mimetype=mimetype,
        as_attachment=False,
        download_name=clean_text(beleg.get("original_name")) or path.name,
    )


@app.route("/admin/emails")
@admin_required
def admin_emails():
    return render_template(
        "emails_admin.html",
        emails=list_werkstatt_emails("aktiv", limit=200),
        api_token_configured=bool(get_werkstatt_email_api_token()),
        imap_status=werkstatt_imap_status(),
    )


@app.route("/admin/emails/sync", methods=["POST"])
@admin_required
def admin_emails_sync():
    try:
        summary = sync_werkstatt_imap()
    except ValueError as exc:
        flash(str(exc), "warning")
        return redirect(url_for("admin_emails"))
    except Exception as exc:
        flash(f"Postfach konnte nicht abgerufen werden: {exc}", "danger")
        return redirect(url_for("admin_emails"))
    created = int(summary.get("created") or 0)
    skipped = int(summary.get("skipped") or 0)
    checked = int(summary.get("checked") or 0)
    errors = summary.get("errors") or []
    if created:
        flash(f"{created} neue E-Mail(s) aus dem Postfach übernommen.", "success")
    else:
        flash(f"Postfach geprüft: {checked} Mail(s), keine neuen Einträge. {skipped} Dublette(n) übersprungen.", "info")
    if errors:
        flash("Einzelne Mails konnten nicht gelesen werden: " + "; ".join(errors[:3]), "warning")
    return redirect(url_for("admin_emails"))


@app.route("/admin/emails/neu", methods=["POST"])
@admin_required
def admin_email_neu():
    try:
        create_werkstatt_email(
            absender_name=request.form.get("absender_name"),
            absender_email=request.form.get("absender_email"),
            empfaenger=request.form.get("empfaenger"),
            betreff=request.form.get("betreff"),
            nachricht=request.form.get("nachricht"),
            empfangen_am=request.form.get("empfangen_am"),
            quelle="manuell",
        )
        flash("E-Mail/Notiz gespeichert.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(url_for("admin_emails"))


@app.route("/admin/emails/<int:email_id>/erledigt", methods=["POST"])
@admin_required
def admin_email_erledigt(email_id):
    if not get_werkstatt_email(email_id):
        abort(404)
    update_werkstatt_email_status(email_id, "erledigt")
    flash("E-Mail als erledigt markiert.", "success")
    return redirect(url_for("admin_emails"))


@app.route("/admin/emails/<int:email_id>/einkauf-analysieren", methods=["POST"])
@admin_required
def admin_email_einkauf_analysieren(email_id):
    result = analyse_lieferantenangebot_email(email_id, force=True)
    if result["reason"] == "existing":
        flash("Dieses Lieferantenangebot ist bereits im Einkauf sichtbar.", "info")
        return redirect(url_for("admin_einkauf") + f"#{einkauf_offer_anchor(f'email-{email_id}')}")
    if result["ok"] and result["items"]:
        flash(f"{len(result['items'])} Einkaufsposition(en) aus dem Lieferantenangebot bereitgestellt.", "success")
        first_item = result["items"][0]
        anchor_key = f"email-{first_item.get('quelle_email_id')}" if first_item.get("quelle_email_id") else "offen"
        return redirect(url_for("admin_einkauf") + f"#{einkauf_offer_anchor(anchor_key)}")
    flash("Ich konnte aus dieser E-Mail noch keine Preispositionen erkennen. Bitte Text/Preise prüfen.", "warning")
    return redirect(url_for("admin_emails"))


@app.route("/admin/emails/<int:email_id>/archivieren", methods=["POST"])
@admin_required
def admin_email_archivieren(email_id):
    if not get_werkstatt_email(email_id):
        abort(404)
    update_werkstatt_email_status(email_id, "archiviert")
    flash("E-Mail archiviert.", "info")
    return redirect(url_for("admin_emails"))


@app.route("/api/werkstatt/emails", methods=["GET", "POST"])
def api_werkstatt_emails():
    is_admin = bool(session.get("admin"))
    is_token = werkstatt_api_token_valid()
    if not is_admin and not is_token:
        return jsonify({"error": "Nicht autorisiert."}), 401
    if request.method == "GET":
        emails = list_werkstatt_emails("aktiv", limit=80)
        return jsonify({"emails": emails, "count": len(emails)})
    try:
        email_id = create_werkstatt_email(**email_payload_from_request())
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, "id": email_id}), 201


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
    flash("OpenAI-Key wurde gespeichert. Uploads und KI-Helfer nutzen jetzt OpenAI.", "success")
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
        BytesIO(backup_path.read_bytes()),
        download_name=backup_path.name,
        mimetype="application/zip",
        as_attachment=True,
    )


@app.route("/admin/autohaus/neu", methods=["POST"])
@admin_required
def autohaus_neu():
    name = clean_text(request.form.get("name"))
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith("/admin") else url_for("betriebs_cockpit")
    if not name:
        flash("Bitte einen Autohaus-Namen eintragen.", "warning")
        return redirect(redirect_url)

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
    return redirect(redirect_url)


@app.route("/admin/autohaus/<int:autohaus_id>/update", methods=["POST"])
@admin_required
def autohaus_update(autohaus_id):
    autohaus = get_autohaus(autohaus_id)
    if not autohaus:
        abort(404)
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith("/admin") else url_for("betriebs_cockpit")

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
    return redirect(redirect_url)


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

    back_context = clean_text(request.values.get("back"))
    detail_back_url = url_for("kalender") if back_context == "kalender" else url_for("dashboard")
    detail_self_url = (
        url_for("auftrag_detail", auftrag_id=auftrag_id, back=back_context)
        if back_context == "kalender"
        else url_for("auftrag_detail", auftrag_id=auftrag_id)
    )

    autohaeuser = list_autohaeuser()
    if request.method == "POST":
        form = request.form
        aktion = form.get("aktion", "speichern")
        analyse = clean_text(form.get("analyse_text")) or analyse_text(form.get("beschreibung"))
        bonus_netto_betrag = positive_money_amount(form.get("bonus_netto_betrag")) or 0.0
        bisheriger_bonus = positive_money_amount(auftrag.get("bonus_netto_betrag")) or 0.0
        bonus_preis_aktualisiert_am = clean_text(auftrag.get("bonus_preis_aktualisiert_am"))
        if round(bonus_netto_betrag, 2) != round(bisheriger_bonus, 2):
            bonus_preis_aktualisiert_am = now_str() if bonus_netto_betrag else ""
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
                bonus_netto_betrag=?,
                bonus_preis_aktualisiert_am=?,
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
                bonus_netto_betrag,
                bonus_preis_aktualisiert_am,
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
            return redirect(detail_self_url)
        if aktion == "upload_analyze" and not any(file and file.filename for file in dateien):
            flash("Bitte zuerst eine Datei auswählen.", "warning")
            return redirect(detail_self_url)
        if aktion == "upload_analyze" and not erlaubte_dateien:
            flash("Dateityp nicht unterstützt. Bitte PDF, JPG, PNG, HEIC, DOCX oder XLSX verwenden.", "warning")
            return redirect(detail_self_url)
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
        return redirect(detail_self_url)

    dateien = list_dateien(auftrag_id)
    standard_dateien = dateien_mit_kategorie(dateien, "standard")
    bonusrechnungen = dateien_mit_kategorie(dateien, "bonusrechnung")
    rechnungsdateien = [
        datei
        for datei in dateien
        if clean_text(datei.get("kategorie")) in {"rechnung", "bonusrechnung"}
    ]
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
        bonusrechnungen=bonusrechnungen,
        rechnungsdateien=rechnungsdateien,
        fertigbilder=fertigbilder,
        dokument_pruefung=list_document_review_items(auftrag_id, auftrag),
        reklamationen=list_reklamationen(auftrag_id),
        verzoegerungen=list_verzoegerungen(auftrag_id),
        benachrichtigungen=list_benachrichtigungen(auftrag_id),
        chat_nachrichten=chat_nachrichten,
        detail_back_url=detail_back_url,
        detail_self_url=detail_self_url,
        back_context=back_context,
    )


@app.route("/admin/auftrag/<int:auftrag_id>/bonusrechnung", methods=["POST"])
@admin_required
def admin_bonusrechnung_upload(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith("/admin") else url_for("auftrag_detail", auftrag_id=auftrag_id)
    dateien = get_allowed_uploads(request.files.getlist("bonusrechnung"))
    if not dateien:
        flash("Bitte eine Rechnung als PDF, Bild oder Textdatei auswählen.", "warning")
        return redirect(redirect_url)

    result = save_bonusrechnung_upload(auftrag_id, dateien)
    if result.get("amount"):
        amount_label = format_bonus_money(result["amount"])
        add_benachrichtigung(
            auftrag_id,
            "Bonusstand aktualisiert",
            f"Der Rechnungsbetrag wurde auf {amount_label} gesetzt. Der Monatsbonus wird automatisch neu berechnet.",
            quelle="werkstatt",
        )
        flash(
            f"Rechnung gespeichert. {amount_label} wurde als Bonus-/Rechnungsbetrag übernommen.",
            "success",
        )
    elif result.get("saved"):
        message = "Rechnung gespeichert, aber der Gesamtbetrag wurde nicht sicher erkannt. Bitte Betrag manuell prüfen."
        if result.get("error"):
            message += f" Hinweis: {clean_text(result['error'])[:220]}"
        flash(message, "warning")
    else:
        flash(result.get("error") or "Rechnung konnte nicht gespeichert werden.", "danger")
    return redirect(redirect_url)


@app.route("/admin/auftrag/<int:auftrag_id>/rechnung/upload", methods=["POST"])
@admin_required
def admin_rechnung_upload(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith("/admin") else url_for("auftrag_detail", auftrag_id=auftrag_id)
    dateien = get_allowed_uploads(
        request.files.getlist("rechnung_dateien") or request.files.getlist("rechnung")
    )
    if not dateien:
        flash("Bitte eine Rechnung als PDF, Bild, TXT, DOCX oder XLSX auswählen.", "warning")
        return redirect(redirect_url)

    result = save_werkstatt_rechnung_upload(auftrag_id, dateien)
    if result.get("verified"):
        details = []
        if result.get("invoice_number"):
            details.append(f"Rechnungsnummer {result['invoice_number']}")
        if result.get("amount"):
            details.append(f"{format_bonus_money(result['amount'])} netto")
        suffix = f" ({', '.join(details)})" if details else ""
        add_benachrichtigung(
            auftrag_id,
            "Rechnung geprüft",
            "Die Werkstatt hat die Rechnung hochgeladen und dem Auftrag zugeordnet.",
            quelle="werkstatt",
        )
        flash(f"Rechnung geprüft und gespeichert{suffix}. Status und Rechnungsbetrag wurden aktualisiert.", "success")
    elif result.get("saved"):
        message = "Rechnung gespeichert, aber nicht sicher dem Auftrag zugeordnet. Bitte Original gegen Auftrag/Kennzeichen prüfen."
        if result.get("warning"):
            message += f" Hinweis: {clean_text(result['warning'])[:220]}"
        flash(message, "warning")
    else:
        flash(result.get("error") or "Rechnung konnte nicht geprüft werden.", "danger")
    return redirect(redirect_url)


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
    if neuer_status >= 5 and not auftrag_bonus_preis(auftrag)[0]:
        flash("Status aktualisiert. Bitte noch den Netto-Preis für Bonus und Rechnung ergänzen.", "warning")
    else:
        flash("Status aktualisiert.", "success")
    ziel = clean_text(request.form.get("next"))
    if ziel.startswith("/"):
        return redirect(ziel)
    return redirect(request.referrer or url_for("auftrag_detail", auftrag_id=auftrag_id))


@app.route("/admin/auftrag/<int:auftrag_id>/reklamation-neu-planen", methods=["POST"])
@admin_required
def reklamation_neu_planen(auftrag_id):
    auftrag = get_auftrag(auftrag_id)
    if not auftrag:
        abort(404)
    if not auftrag_has_offene_reklamation(auftrag_id):
        flash("Für diesen Auftrag ist keine offene Reklamation vorhanden.", "info")
        return redirect(request.referrer or url_for("dashboard") + "#auftraege")

    start_datum = format_date(
        request.form.get(f"start_datum_{auftrag_id}") or request.form.get("start_datum")
    )
    fertig_datum = format_date(
        request.form.get(f"fertig_datum_{auftrag_id}") or request.form.get("fertig_datum")
    )
    if not start_datum or not fertig_datum:
        flash("Bitte Start- und Fertig-Datum für die Reklamation eintragen.", "warning")
        return redirect(request.referrer or url_for("dashboard") + "#auftraege")

    db = get_db()
    db.execute(
        """
        UPDATE auftraege
        SET status=2,
            start_datum=?,
            fertig_datum=?,
            abholtermin='',
            archiviert=0,
            geaendert_am=?
        WHERE id=?
        """,
        (start_datum, fertig_datum, now_str(), auftrag_id),
    )
    db.execute(
        "INSERT INTO status_log (auftrag_id, status, zeitstempel) VALUES (?, 2, ?)",
        (auftrag_id, now_str()),
    )
    db.commit()
    db.close()
    add_benachrichtigung(
        auftrag_id,
        "Reklamation neu eingeplant",
        f"Die Reklamation wurde neu geplant: Start {start_datum}, Fertig {fertig_datum}.",
    )
    flash("Reklamation neu eingeplant. Der Auftrag startet wieder im Prozess.", "success")
    return redirect(request.referrer or url_for("dashboard") + "#auftraege")


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
        bonus_netto_betrag = positive_money_amount(request.form.get("bonus_netto_betrag")) or 0.0
        bisheriger_bonus = positive_money_amount(auftrag.get("bonus_netto_betrag")) or 0.0
        bonus_preis_aktualisiert_am = clean_text(auftrag.get("bonus_preis_aktualisiert_am"))
        if round(bonus_netto_betrag, 2) != round(bisheriger_bonus, 2):
            bonus_preis_aktualisiert_am = now_str() if bonus_netto_betrag else ""

        db = get_db()
        db.execute(
            """
            UPDATE auftraege
            SET lexware_kunde_angelegt=?,
                rechnung_status=?,
                rechnung_nummer=?,
                rechnung_geschrieben_am=?,
                bonus_netto_betrag=?,
                bonus_preis_aktualisiert_am=?,
                geaendert_am=?
            WHERE id=?
            """,
            (
                lexware_kunde_angelegt,
                rechnung_status,
                rechnung_nummer,
                geschrieben_am,
                bonus_netto_betrag,
                bonus_preis_aktualisiert_am,
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

    net_amount = parse_money_amount(request.form.get("netto_betrag"))
    if not net_amount or net_amount <= 0:
        flash("Bitte einen Netto-Rechnungsbetrag eintragen.", "warning")
        return redirect(url_for("rechnung_schreiben", auftrag_id=auftrag_id))
    rechnung = build_lexware_rechnung_context(auftrag, invoice_net_amount=net_amount)

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
            bonus_netto_betrag=?,
            bonus_preis_aktualisiert_am=?,
            geaendert_am=?
        WHERE id=?
        """,
        (
            result["contact_id"],
            result["invoice_id"],
            result["invoice_url"],
            net_amount,
            now_str(),
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


@app.route("/admin/auftraege/zurueckgegeben-archivieren", methods=["POST"])
@admin_required
def admin_zurueckgegebene_archivieren():
    anzahl = archive_zurueckgegebene_ohne_offene_reklamation()
    if anzahl:
        flash(f"{anzahl} zurückgegebene Auftrag/Aufträge ohne offene Reklamation archiviert.", "info")
    else:
        flash("Keine archivierbaren Rückgaben gefunden. Offene Reklamationen bleiben sichtbar.", "info")
    return redirect(request.referrer or url_for("dashboard") + "#auftraege")


@app.route("/admin/mitarbeiter")
@admin_required
def admin_mitarbeiter():
    mitarbeiter_liste = list_mitarbeiter(include_inactive=True)
    return render_template(
        "mitarbeiter.html",
        mitarbeiter_liste=mitarbeiter_liste,
        summary=mitarbeiter_urlaub_summary(mitarbeiter_liste),
    )


@app.route("/admin/mitarbeiter/neu", methods=["POST"])
@admin_required
def admin_mitarbeiter_neu():
    try:
        mitarbeiter_id = create_mitarbeiter(
            name=request.form.get("name"),
            rolle=request.form.get("rolle"),
            telefon=request.form.get("telefon"),
            email=request.form.get("email"),
            adresse=request.form.get("adresse"),
            geburtsdatum=request.form.get("geburtsdatum"),
            geburtsort=request.form.get("geburtsort"),
            staatsangehoerigkeit=request.form.get("staatsangehoerigkeit"),
            eintritt_datum=request.form.get("eintritt_datum"),
            austritt_datum=request.form.get("austritt_datum"),
            beschaeftigung=request.form.get("beschaeftigung"),
            qualifikation=request.form.get("qualifikation"),
            arbeitszeit=request.form.get("arbeitszeit"),
            urlaubsanspruch=request.form.get("urlaubsanspruch"),
            ordner_pfad=request.form.get("ordner_pfad"),
            dokumente_notiz=request.form.get("dokumente_notiz"),
            notiz=request.form.get("notiz"),
        )
        flash("Mitarbeiter gespeichert.", "success")
        return redirect(url_for("admin_mitarbeiter") + f"#mitarbeiter-{mitarbeiter_id}")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(url_for("admin_mitarbeiter"))


@app.route("/admin/mitarbeiter/<int:mitarbeiter_id>/bearbeiten", methods=["POST"])
@admin_required
def admin_mitarbeiter_bearbeiten(mitarbeiter_id):
    if not get_mitarbeiter(mitarbeiter_id):
        abort(404)
    try:
        update_mitarbeiter(
            mitarbeiter_id,
            name=request.form.get("name"),
            rolle=request.form.get("rolle"),
            telefon=request.form.get("telefon"),
            email=request.form.get("email"),
            adresse=request.form.get("adresse"),
            geburtsdatum=request.form.get("geburtsdatum"),
            geburtsort=request.form.get("geburtsort"),
            staatsangehoerigkeit=request.form.get("staatsangehoerigkeit"),
            eintritt_datum=request.form.get("eintritt_datum"),
            austritt_datum=request.form.get("austritt_datum"),
            beschaeftigung=request.form.get("beschaeftigung"),
            qualifikation=request.form.get("qualifikation"),
            arbeitszeit=request.form.get("arbeitszeit"),
            urlaubsanspruch=request.form.get("urlaubsanspruch"),
            ordner_pfad=request.form.get("ordner_pfad"),
            dokumente_notiz=request.form.get("dokumente_notiz"),
            notiz=request.form.get("notiz"),
            aktiv=request.form.get("aktiv") == "1",
        )
        flash("Mitarbeiter aktualisiert.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(url_for("admin_mitarbeiter") + f"#mitarbeiter-{mitarbeiter_id}")


@app.route("/admin/mitarbeiter/<int:mitarbeiter_id>/urlaub", methods=["POST"])
@admin_required
def admin_mitarbeiter_urlaub_neu(mitarbeiter_id):
    if not get_mitarbeiter(mitarbeiter_id):
        abort(404)
    try:
        create_mitarbeiter_urlaub(
            mitarbeiter_id,
            start_datum=request.form.get("start_datum"),
            end_datum=request.form.get("end_datum"),
            notiz=request.form.get("notiz"),
        )
        flash("Urlaub gespeichert und im Kalender übernommen.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(url_for("admin_mitarbeiter") + f"#mitarbeiter-{mitarbeiter_id}")


@app.route("/admin/mitarbeiter/urlaub/<int:urlaub_id>/loeschen", methods=["POST"])
@admin_required
def admin_mitarbeiter_urlaub_loeschen(urlaub_id):
    delete_mitarbeiter_urlaub(urlaub_id)
    flash("Urlaubseintrag gelöscht.", "info")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin/mitarbeiter"):
        return redirect(next_url)
    return redirect(url_for("admin_mitarbeiter"))


@app.route("/admin/news")
@admin_required
def admin_news():
    return render_template(
        "werkstatt_news.html",
        news_items=list_werkstatt_news(limit=200),
        kalender_kategorien=KALENDER_KATEGORIEN,
    )


@app.route("/admin/news/neu", methods=["POST"])
@admin_required
def admin_news_neu():
    try:
        create_werkstatt_news(
            titel=request.form.get("titel"),
            nachricht=request.form.get("nachricht"),
            start_datum=request.form.get("start_datum"),
            end_datum=request.form.get("end_datum"),
            kategorie=request.form.get("kategorie") or "betrieb",
            pinned=bool(request.form.get("pinned")),
        )
        flash("Werkstatt-News gespeichert.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(url_for("admin_news"))


@app.route("/admin/news/<int:news_id>/archivieren", methods=["POST"])
@admin_required
def admin_news_archivieren(news_id):
    if not get_werkstatt_news(news_id):
        abort(404)
    archive_werkstatt_news(news_id)
    flash("Werkstatt-News archiviert.", "info")
    return redirect(url_for("admin_news"))


@app.route("/admin/kalender/notiz", methods=["POST"])
@admin_required
def kalender_notiz_neu():
    schnelleintrag = clean_text(request.form.get("quick_text"))
    if clean_text(request.form.get("aktion")) == "suchen":
        if schnelleintrag:
            return redirect(url_for("kalender", suche=schnelleintrag))
        flash("Bitte geben Sie einen Suchbegriff ein.", "warning")
        return redirect(url_for("betriebs_cockpit"))
    try:
        if schnelleintrag:
            daten = parse_kalender_schnelleintrag(schnelleintrag)
        else:
            daten = {
                "datum": format_date(request.form.get("datum")),
                "titel": clean_text(request.form.get("titel")),
                "notiz": clean_text(request.form.get("notiz")),
                "kategorie": clean_text(request.form.get("kategorie")) or "termin",
                "wiederholung": clean_text(request.form.get("wiederholung")) or "einmalig",
            }
        create_kalender_notiz(**daten)
        repeat_text = " jährlich" if daten.get("wiederholung") == "jaehrlich" else ""
        flash(f"Kalendereintrag für {daten['datum']}{repeat_text} gespeichert.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")

    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("kalender"))


@app.route("/admin/kalender/notiz/<int:notiz_id>/loeschen", methods=["POST"])
@admin_required
def kalender_notiz_loeschen(notiz_id):
    delete_kalender_notiz(notiz_id)
    flash("Kalendereintrag gelöscht.", "info")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("kalender"))


@app.route("/admin/erinnerungen/neu", methods=["POST"])
@admin_required
def admin_erinnerung_neu():
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith("/admin") else url_for("betriebs_cockpit")
    try:
        create_erinnerung(request.form.get("text"))
        flash("Erinnerung gespeichert.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    return redirect(redirect_url)


@app.route("/admin/erinnerungen/<int:erinnerung_id>/erledigt", methods=["POST"])
@admin_required
def admin_erinnerung_erledigt(erinnerung_id):
    if mark_erinnerung_erledigt(erinnerung_id):
        flash("Erinnerung erledigt.", "success")
    else:
        flash("Erinnerung nicht gefunden.", "warning")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/admin"):
        return redirect(next_url)
    return redirect(url_for("betriebs_cockpit"))


@app.route("/admin/kalender")
@admin_required
def kalender():
    auftraege = list_auftraege(include_archived=True, include_angebote=True)
    zeige_vergangenheit = request.args.get("ansicht") == "vergangenheit"
    kalender_suche = clean_text(request.args.get("suche"))
    alle_kalender_items = kalender_daten(auftraege)
    if kalender_suche:
        kalender_items, gefundene_auftrag_ids = filter_kalender_suche(
            alle_kalender_items,
            kalender_suche,
        )
        event_count = sum(len(item["events"]) for item in kalender_items)
        calendar_point_count = sum(len(item["notizen"]) + len(item["system"]) for item in kalender_items)
        if event_count >= 1 and calendar_point_count == 0 and len(gefundene_auftrag_ids) == 1:
            return redirect(url_for("auftrag_detail", auftrag_id=next(iter(gefundene_auftrag_ids)), back="kalender"))
    else:
        kalender_items = filter_kalender_items(
            alle_kalender_items,
            vergangenheit=zeige_vergangenheit,
        )
    return render_template(
        "kalender.html",
        kalender_items=kalender_items,
        kalender_woche=kalender_wochenuebersicht(alle_kalender_items),
        zeige_vergangenheit=zeige_vergangenheit,
        kalender_suche=kalender_suche,
        kalender_kategorien=KALENDER_KATEGORIEN,
    )


@app.route("/portal")
@app.route("/portal/")
def portal_redirect():
    return redirect(url_for("partner_login"))


@app.route("/partner", methods=["GET", "POST"])
def partner_login():
    autohaeuser = list_autohaeuser()
    if request.method == "GET" and session.get("partner_autohaus_id"):
        autohaus = get_autohaus(session.get("partner_autohaus_id"))
        if autohaus:
            return redirect(url_for("partner_dashboard", slug=autohaus["slug"]))
    if request.method == "POST":
        portal_key = clean_text(request.form.get("portal_key"))
        zugangscode = clean_text(request.form.get("password") or request.form.get("zugangscode"))
        autohaus = get_autohaus_by_portal_key(portal_key)
        limit_identifier = portal_key or "zentral"
        limited, wait_seconds = login_rate_limit_status("partner", limit_identifier)
        if limited:
            flash(f"Zu viele Fehlversuche. Bitte in {login_wait_label(wait_seconds)} erneut versuchen.", "danger")
            return render_template("partner_index.html", autohaeuser=autohaeuser), 429
        if autohaus and partner_access_code_matches(zugangscode, autohaus):
            clear_login_attempts("partner", limit_identifier)
            session.clear()
            session.permanent = True
            session["partner_autohaus_id"] = autohaus["id"]
            response = redirect(url_for("partner_dashboard_key", portal_key=autohaus["portal_key"]))
            remember_authenticated_login(response, "partner", autohaus_id=autohaus["id"])
            return response
        record_failed_login("partner", limit_identifier)
        flash("Autohaus oder Passwort/Zugangscode stimmt nicht.", "danger")
    return render_template("partner_index.html", autohaeuser=autohaeuser)


@app.route("/portal/<portal_key>", methods=["GET", "POST"])
def partner_login_key(portal_key):
    autohaus = get_autohaus_by_portal_key(portal_key)
    if not autohaus:
        abort(404)

    if request.method == "POST":
        limited, wait_seconds = login_rate_limit_status("partner", portal_key)
        if limited:
            flash(f"Zu viele Fehlversuche. Bitte in {login_wait_label(wait_seconds)} erneut versuchen.", "danger")
            return render_template("partner_login.html", autohaus=autohaus), 429
        submitted_code = request.form.get("password") or request.form.get("zugangscode")
        if partner_access_code_matches(submitted_code, autohaus):
            clear_login_attempts("partner", portal_key)
            session.clear()
            session.permanent = True
            session["partner_autohaus_id"] = autohaus["id"]
            response = redirect(url_for("partner_dashboard_key", portal_key=portal_key))
            remember_authenticated_login(response, "partner", autohaus_id=autohaus["id"])
            return response
        record_failed_login("partner", portal_key)
        flash("Falscher Zugangscode.", "danger")

    if session.get("partner_autohaus_id") == autohaus["id"]:
        return redirect(url_for("partner_dashboard_key", portal_key=portal_key))

    return render_template("partner_login.html", autohaus=autohaus)


@app.route("/partner/logout")
def partner_logout():
    session.pop("partner_autohaus_id", None)
    response = redirect(url_for("partner_login"))
    clear_remember_login_cookie(response, "partner")
    return response


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
    current_datetime = datetime.now()
    alle_auftraege = list_auftraege(autohaus["id"], include_archived=True)
    auftraege = [a for a in alle_auftraege if not a["archiviert"]]
    archivierte_auftraege = [a for a in alle_auftraege if a["archiviert"]]
    postfach_items = list_partner_postfach_items(autohaus["id"], autohaus["slug"], limit=8)
    return render_template(
        "partner_dashboard.html",
        autohaus=autohaus,
        auftraege=auftraege,
        archivierte_auftraege=archivierte_auftraege,
        benachrichtigungen=list_autohaus_benachrichtigungen(autohaus["id"]),
        postfach_items=postfach_items,
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
        cockpit=autohaus_dashboard_daten(auftraege),
        werkstatt_news=list_werkstatt_news(limit=5),
        mini_calendar=build_mini_monatskalender(
            auftraege,
            request.args.get("monat"),
            request.args.get("tag"),
            endpoint="partner_dashboard",
            route_values={"slug": autohaus["slug"]},
            include_internal_notes=False,
            only_arrival_events=True,
        ),
        rahmenvertrag=rahmenvertrag_context(autohaus),
        statusliste=STATUSLISTE,
        current_datetime_iso=current_datetime.isoformat(timespec="seconds"),
        current_time_label=current_datetime.strftime("%H:%M:%S"),
        current_date_label=f"{WOCHENTAGE[current_datetime.weekday()]}, {current_datetime.strftime(DATE_FMT)}",
    )


@app.route("/partner/<slug>/hinweis/<int:hinweis_id>/entfernen", methods=["POST"])
def partner_hinweis_entfernen(slug, hinweis_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    mark_autohaus_postfach_item_erledigt(autohaus["id"], f"autohaus-hinweis-{hinweis_id}")
    flash("Hinweis entfernt.", "info")
    return redirect(url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/bonusmodell")
def partner_bonusmodell(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    alle_auftraege = list_auftraege(autohaus["id"], include_archived=True)
    bonus_reference = parse_bonusmonat(request.args.get("monat")) or date.today()
    return render_template(
        "partner_bonusmodell.html",
        autohaus=autohaus,
        bonusmodell=build_bonusmodell(alle_auftraege, bonus_reference),
        bonus_monate=build_bonus_month_options(alle_auftraege, bonus_reference),
        archivierte_auftraege=prepare_archivierte_bonus_auftraege(alle_auftraege),
        rahmenvertrag=rahmenvertrag_context(autohaus),
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
    )


@app.route("/partner/<slug>/auftrag/<int:auftrag_id>/bonusrechnung", methods=["POST"])
def partner_bonusrechnung_upload(slug, auftrag_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    auftrag = get_auftrag(auftrag_id)
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)

    reference = auftrag_bonus_reference_date(auftrag) or date.today()
    fallback_url = url_for(
        "partner_bonusmodell",
        slug=slug,
        monat=reference.strftime("%Y-%m"),
        _anchor="archivierte-auftraege",
    )
    next_url = clean_text(request.form.get("next"))
    redirect_url = next_url if next_url.startswith(f"/partner/{slug}/bonusmodell") else fallback_url
    dateien = get_allowed_uploads(request.files.getlist("bonusrechnung"))
    if not dateien:
        flash("Bitte eine Rechnung als PDF, Bild, TXT, DOCX oder XLSX auswählen.", "warning")
        return redirect(redirect_url)

    result = save_bonusrechnung_upload(
        auftrag_id,
        dateien,
        quelle="autohaus",
        notiz="Vom Autohaus im Bonusmodell hochgeladen.",
    )
    if result.get("amount"):
        amount_label = format_bonus_money(result["amount"])
        details = []
        if result.get("invoice_number"):
            details.append(f"Rechnungsnummer {result['invoice_number']}")
        if result.get("filename"):
            details.append(result["filename"])
        suffix = f" ({', '.join(details)})" if details else ""
        add_benachrichtigung(
            auftrag_id,
            "Bonusrechnung hochgeladen",
            f"Das Autohaus hat eine Rechnung hochgeladen. {amount_label} netto wurde als Bonus-/Rechnungsbetrag übernommen.",
            quelle="autohaus",
        )
        flash(
            f"Rechnung gespeichert. {amount_label} netto wurde für den Bonus übernommen{suffix}.",
            "success",
        )
    elif result.get("saved"):
        message = "Rechnung gespeichert, aber der Netto-Gesamtbetrag wurde nicht sicher erkannt. Bitte Betrag manuell prüfen."
        if result.get("error"):
            message += f" Hinweis: {clean_text(result['error'])[:220]}"
        flash(message, "warning")
    else:
        flash(result.get("error") or "Rechnung konnte nicht gespeichert werden.", "danger")
    return redirect(redirect_url)


@app.route("/partner/<slug>/rahmenvertrag-anfragen", methods=["POST"])
def partner_rahmenvertrag_anfragen(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    _, created = add_rahmenvertrag_anfrage(
        autohaus["id"],
        f"{autohaus['name']} möchte den Rahmenvertrag, Partnerpreise und mögliche Bonusstufen gemeinsam besprechen.",
    )
    if created:
        flash("Anfrage gesendet. Wir melden uns für den Rahmenvertrag und Partnerpreis.", "success")
    else:
        flash("Die Anfrage ist schon im Werkstatt-Postfach sichtbar.", "info")
    return redirect(url_for("partner_bonusmodell", slug=slug))


@app.route("/partner/<slug>/auftraege-suche")
def partner_auftraege_suche(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    return jsonify({"items": list_partner_auftrag_suche(autohaus, request.args.get("q"), limit=8)})


@app.route("/partner/<slug>/postfach")
def partner_postfach(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    return render_template(
        "partner_postfach.html",
        autohaus=autohaus,
        items=list_partner_postfach_items(autohaus["id"], autohaus["slug"], limit=200),
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
    )


@app.route("/partner/<slug>/postfach/<path:item_key>/oeffnen")
def partner_postfach_oeffnen(slug, item_key):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    item_key = clean_text(item_key)
    item = next(
        (
            entry
            for entry in list_partner_postfach_items(autohaus["id"], autohaus["slug"], limit=200)
            if entry.get("item_key") == item_key
        ),
        None,
    )
    if not item:
        flash("Die Meldung ist bereits erledigt oder nicht mehr vorhanden.", "info")
        return redirect(url_for("partner_dashboard", slug=slug))
    mark_autohaus_postfach_item_erledigt(autohaus["id"], item_key)
    return redirect(item.get("ziel_url") or url_for("partner_dashboard", slug=slug))


@app.route("/partner/<slug>/postfach/<path:item_key>/loeschen", methods=["POST"])
def partner_postfach_loeschen(slug, item_key):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    mark_autohaus_postfach_item_erledigt(autohaus["id"], item_key)
    flash("Nachricht aus dem Postfach gelöscht.", "info")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("partner_postfach", slug=slug))


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
    auftrag_id = int((auftrag or {}).get("id") or 0)
    save_ki_assistent_message(autohaus["id"], auftrag_id, "kunde", question)
    answer, source = ask_partner_assistant(question, autohaus, auftrag)
    save_ki_assistent_message(autohaus["id"], auftrag_id, "ki", answer)
    schedule_change_backup("ki-assistent")
    return jsonify({"answer": answer, "source": source})


@app.route("/partner/<slug>/ki/chat/loeschen", methods=["POST"])
def partner_ki_chat_loeschen(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return jsonify({"error": "Nicht eingeloggt."}), 401
    payload = request.get_json(silent=True) or {}
    auftrag = get_partner_assistant_auftrag(payload.get("auftrag_id"), autohaus["id"])
    clear_ki_assistent_history(autohaus["id"], int((auftrag or {}).get("id") or 0))
    schedule_change_backup("ki-assistent-loeschen")
    return jsonify({"ok": True})


@app.route("/partner/<slug>/lackierauftrag-vorlage.pdf")
def partner_lackierauftrag_vorlage(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    entwurf = get_lackierauftrag_entwurf(autohaus)
    return send_lackierauftrag_pdf(autohaus, entwurf["daten"])


@app.route("/partner/<slug>/lackierauftrag", methods=["GET", "POST"])
def partner_lackierauftrag_bearbeiten(slug):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    entwurf = get_lackierauftrag_entwurf(autohaus)
    if request.method == "POST":
        daten = parse_lackierauftrag_form(request.form, autohaus)
        daten = save_lackierauftrag_entwurf(autohaus, daten)
        if request.form.get("aktion") == "download":
            return send_lackierauftrag_pdf(autohaus, daten)
        flash("Lackierauftrag gespeichert. Sie können ihn später weiterbearbeiten oder als PDF herunterladen.", "success")
        return redirect(url_for("partner_lackierauftrag_bearbeiten", slug=slug))
    return render_template(
        "partner_lackierauftrag.html",
        autohaus=autohaus,
        entwurf=entwurf,
        daten=entwurf["daten"],
        abrechnung_optionen=LACKIERAUFTRAG_ABRECHNUNG,
        position_count=LACKIERAUFTRAG_POSITION_COUNT,
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
    )


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


@app.route("/portal/<portal_key>/lackierauftrag")
def partner_lackierauftrag_bearbeiten_key(portal_key):
    autohaus, redirect_response = partner_session_required_by_key(portal_key)
    if redirect_response:
        return redirect_response
    return redirect(url_for("partner_lackierauftrag_bearbeiten", slug=autohaus["slug"]))


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
            upload_result = save_uploads(
                auftrag_id,
                erlaubte_dateien,
                "autohaus",
                "standard",
                upload_note=form.get("upload_notiz"),
            )
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
        flash_betriebsurlaub_planungshinweis(
            form.get("annahme_datum"),
            form.get("start_datum"),
            form.get("fertig_datum"),
            form.get("abholtermin"),
        )
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
        upload_result = save_uploads(
            angebot_id,
            erlaubte_dateien,
            "autohaus",
            "standard",
            upload_note=form.get("upload_notiz"),
        )
        refresh_offer_texts(angebot_id, kunden_kurz, kunden_text)
        flash_upload_analysis_result(
            upload_result,
            "Angebotsanfrage analysiert. Bitte prüfen und danach absenden.",
        )
        flash_betriebsurlaub_planungshinweis(
            form.get("annahme_datum"),
            form.get("abholtermin"),
        )
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=angebot_id))

    return render_template(
        "partner_angebot.html",
        autohaus=autohaus,
        angebot=None,
        dateien=[],
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
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
        if aktion == "quick_upload_analyze":
            saved = save_partner_standard_uploads(
                auftrag_id,
                dateien,
                upload_note=form.get("upload_notiz"),
                success_message="Unterlage gespeichert, analysiert und zur Angebotsanfrage hinzugefügt.",
            )
            if saved:
                refresh_offer_texts(
                    auftrag_id,
                    angebot.get("analyse_text"),
                    angebot.get("beschreibung"),
                )
            return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=auftrag_id))
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
        upload_result = save_uploads(
            auftrag_id,
            erlaubte_dateien,
            "autohaus",
            "standard",
            upload_note=form.get("upload_notiz"),
        )
        refresh_offer_texts(auftrag_id, kunden_kurz, kunden_text)
        if aktion == "submit_offer":
            submit_offer_request(auftrag_id)
            flash("Angebotsanfrage abgesendet. Die Werkstatt kann sie jetzt prüfen.", "success")
        else:
            flash_upload_analysis_result(
                upload_result,
                "Angebotsanfrage analysiert. Bitte prüfen und danach absenden.",
            )
        flash_betriebsurlaub_planungshinweis(
            form.get("annahme_datum"),
            form.get("abholtermin"),
        )
        return redirect(url_for("partner_angebot_detail", slug=slug, auftrag_id=auftrag_id))

    sichtbare_dateien = [d for d in list_dateien(auftrag_id) if d.get("quelle") in {"autohaus", "intern"}]
    return render_template(
        "partner_angebot.html",
        autohaus=autohaus,
        angebot=angebot,
        dateien=sichtbare_dateien,
        dokument_pruefung=list_document_review_items(auftrag_id, angebot),
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
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
        if aktion == "quick_upload_analyze":
            save_partner_standard_uploads(
                auftrag_id,
                request.files.getlist("dateien"),
                upload_note=form.get("upload_notiz"),
                success_message="Unterlage gespeichert, analysiert und für Autohaus und Werkstatt sichtbar.",
            )
            return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag_id))
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
        flash_betriebsurlaub_planungshinweis(
            form.get("annahme_datum"),
            start_datum,
            fertig_datum,
            form.get("abholtermin"),
        )
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
            upload_result = save_uploads(
                auftrag_id,
                erlaubte_dateien,
                "autohaus",
                "standard",
                upload_note=form.get("upload_notiz"),
            )
        except Exception as exc:
            upload_result = (
                0,
                {"_analysis_error": f"Upload/Analyse konnte nicht abgeschlossen werden: {clean_text(str(exc))[:300]}"},
            )
        fertig_upload_result = save_uploads(
            auftrag_id,
            get_allowed_finish_uploads(request.files.getlist("fertigbilder")),
            "autohaus",
            "fertigbild",
        )
        standard_saved = upload_result[0] if isinstance(upload_result, tuple) else int(upload_result or 0)
        standard_updates = upload_result[1] if isinstance(upload_result, tuple) and len(upload_result) > 1 else {}
        fertig_saved = (
            fertig_upload_result[0]
            if isinstance(fertig_upload_result, tuple)
            else int(fertig_upload_result or 0)
        )
        if aktion == "upload_analyze":
            flash_upload_analysis_result(
                upload_result,
                "Unterlage gespeichert, analysiert und für Autohaus und Werkstatt sichtbar.",
            )
        else:
            analysis_error = clean_text((standard_updates or {}).get("_analysis_error"))
            if analysis_error and standard_saved:
                flash(
                    f"Auftrag gespeichert. Unterlage ist sichtbar, aber die Analyse ist abgebrochen. {analysis_error}",
                    "warning",
                )
            elif standard_saved or fertig_saved:
                teile = []
                if standard_saved:
                    teile.append(f"{standard_saved} Unterlage(n)")
                if fertig_saved:
                    teile.append(f"{fertig_saved} Fertigbild(er)")
                flash(
                    f"Auftrag gespeichert. {' und '.join(teile)} sind jetzt für Autohaus und Werkstatt sichtbar.",
                    "success",
                )
            else:
                flash("Auftrag gespeichert.", "success")
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
        benachrichtigungen=list_benachrichtigungen(auftrag_id, nur_ungelesen=True),
        reklamationen=list_reklamationen(auftrag_id),
        verzoegerungen=list_verzoegerungen(auftrag_id),
        transport_arten=TRANSPORT_ARTEN,
        statusliste=STATUSLISTE,
        chat_nachrichten=chat_nachrichten,
        postfach_count=partner_postfach_count(autohaus["id"], autohaus["slug"]),
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

    chat_id = add_chat_nachricht(auftrag_id, "autohaus", nachricht)
    notify_workshop_whatsapp_for_chat(
        auftrag_id,
        chat_id,
        nachricht,
        absender_label=autohaus.get("name") or "Autohaus",
    )
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


@app.route("/partner/<slug>/datei/<int:datei_id>/notiz", methods=["POST"])
def partner_datei_notiz(slug, datei_id):
    autohaus, redirect_response = partner_session_required(slug)
    if redirect_response:
        return redirect_response
    datei = get_datei(datei_id)
    if not datei:
        abort(404)
    auftrag = get_auftrag(datei["auftrag_id"])
    if not auftrag or auftrag.get("autohaus_id") != autohaus["id"]:
        abort(404)
    if clean_text(datei.get("quelle")) != "autohaus":
        flash("Zu Werkstatt-Dateien kann das Autohaus keine Bildnotiz ändern.", "warning")
        return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag["id"]))

    notiz = clean_text(request.form.get("datei_notiz"))[:500]
    db = get_db()
    db.execute(
        """
        UPDATE dateien
        SET notiz=?
        WHERE id=? AND auftrag_id=? AND quelle='autohaus'
        """,
        (notiz, datei_id, auftrag["id"]),
    )
    analyse_from_note = analyse_text(notiz) or notiz[:220]
    if notiz and analyse_from_note and (
        not clean_text(auftrag.get("analyse_text"))
        or len(clean_text(auftrag.get("analyse_text"))) < 10
    ):
        db.execute(
            """
            UPDATE auftraege
            SET analyse_text=?, geaendert_am=?
            WHERE id=?
            """,
            (analyse_from_note[:220], now_str(), auftrag["id"]),
        )
    else:
        db.execute("UPDATE auftraege SET geaendert_am=? WHERE id=?", (now_str(), auftrag["id"]))
    db.commit()
    db.close()

    if clean_text(datei.get("kategorie")) == "standard":
        reset_document_review_checks(
            auftrag["id"],
            "Bild-/Dateihinweis wurde ergänzt. Bitte die erkannten Daten kurz prüfen.",
        )
    flash("Bild-/Dateihinweis gespeichert. Die Werkstatt sieht ihn direkt im Auftrag.", "success")
    next_url = clean_text(request.form.get("next"))
    if next_url.startswith("/partner/"):
        return redirect(next_url)
    return redirect(url_for("partner_auftrag", slug=slug, auftrag_id=auftrag["id"]))


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
