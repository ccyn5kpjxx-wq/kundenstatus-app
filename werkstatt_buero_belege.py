"""Private office receipts: capture, explicit management review, payment evidence.

Originals stay byte-identical in the database (base64 TEXT, as employee originals).
This module neither reads bank accounts nor executes payments or OCR/AI requests.
Every mutation rechecks the current office/admin identity under the originals lock.
"""
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import base64
import hashlib
import hmac
import io
import json
import re
import secrets
import sqlite3
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit
from uuid import UUID

from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, session


TABLES = ("buero_belege", "buero_belege_audit", "buero_belege_seen")
KINDS = {"rechnung": "Rechnung", "lieferschein": "Lieferschein", "bescheid": "Bescheid", "post": "Sonstige Post",
         "zahlungsauftrag": "Zahlungsauftrag – keine Rechnung"}
CATEGORIES = {"rechnung": "Rechnungen", "gebuehren": "Gebühren", "steuern": "Steuern", "sonstige": "Sonstige Post", "privat": "Privat",
              "miete": "Miete", "kaution": "Kaution"}
AREAS = {"unklar": "Noch zuzuordnen", "geschaeftlich": "Geschäftlich", "privat": "Privat"}
STATES = {"entwurf": "In Erfassung", "eingereicht": "Zur Cheffreigabe", "freigegeben": "Freigegeben / geprüft",
          "zurueckgewiesen": "Zurück zur Bearbeitung", "zahlung_in_bearbeitung": "Überweisung extern in Bearbeitung",
          "bezahlt": "Zahlung dokumentiert"}
MAX_ORIGINAL_BYTES = 10 * 1024 * 1024
MAX_REQUEST_BYTES = MAX_ORIGINAL_BYTES + 128 * 1024
FIELDS = {"typ", "kategorie", "lieferant", "belegnummer", "belegdatum", "betrag_brutto", "faelligkeit", "notiz",
          "zahlungsempfaenger", "iban", "bic", "verwendungszweck", "zahlungsmodus"}
_WRITE_FIELDS = ("typ", "kategorie", "lieferant", "belegnummer", "belegdatum", "betrag_cent", "faelligkeit", "notiz",
                 "zahlungsempfaenger", "iban", "bic", "verwendungszweck", "zahlungsmodus")
TRANSFER_FIELDS = ("zahlungsempfaenger", "iban", "bic", "verwendungszweck", "faelligkeit", "zahlungsmodus")
_BANK_CONFLICT_HINT = "Bankangaben passen nicht eindeutig zur vorhandenen Kontoverbindung; manuelle Angaben bleiben erhalten. "
_KEY = re.compile(r"[a-f0-9]{32}")
_META = ("id,typ,lieferant,belegnummer,belegdatum,betrag_cent,faelligkeit,notiz,status,revision,"
         "filename,mime,size_bytes,sha256,created_by,created_at,updated_at,submitted_at,"
         "decided_at,decided_by,decision_note,payable,paid_date,payment_reference,"
         "kategorie,kategorie_vorschlag,typ_vorschlag,klassifizierung_hinweis,analyse_status,"
         "lexware_beleg_id,lexware_status,lexware_url,business_confirmed,bereich,verarbeitung_hinweis,source_ref_hash,"
         "dedup_key,zahlungsempfaenger,iban,bic,verwendungszweck,zahlungsmodus,quelle_geprueft")
_NEW_COLUMNS = {"kategorie": "TEXT NOT NULL DEFAULT ''", "kategorie_vorschlag": "TEXT NOT NULL DEFAULT ''",
                "typ_vorschlag": "TEXT NOT NULL DEFAULT ''", "klassifizierung_hinweis": "TEXT NOT NULL DEFAULT ''",
                "analyse_status": "TEXT NOT NULL DEFAULT ''", "lexware_beleg_id": "TEXT NOT NULL DEFAULT ''",
                "lexware_status": "TEXT NOT NULL DEFAULT ''", "lexware_url": "TEXT NOT NULL DEFAULT ''",
                "business_confirmed": "INTEGER NOT NULL DEFAULT 0", "bereich": "TEXT NOT NULL DEFAULT 'unklar'",
                "verarbeitung_hinweis": "TEXT NOT NULL DEFAULT ''", "source_ref_hash": "TEXT NOT NULL DEFAULT ''",
                "dedup_key": "TEXT NOT NULL DEFAULT ''", "zahlungsempfaenger": "TEXT NOT NULL DEFAULT ''",
                "iban": "TEXT NOT NULL DEFAULT ''", "bic": "TEXT NOT NULL DEFAULT ''",
                "verwendungszweck": "TEXT NOT NULL DEFAULT ''", "zahlungsmodus": "TEXT NOT NULL DEFAULT ''",
                "quelle_geprueft": "INTEGER NOT NULL DEFAULT 0"}
_VISIBLE = "(b.created_by=? OR (b.bereich IN ('geschaeftlich','privat') AND b.status IN ('freigegeben','zahlung_in_bearbeitung','bezahlt') AND b.decided_by='admin'))"
_CATEGORY_SQL = "CASE WHEN b.bereich='privat' THEN 'privat' ELSE COALESCE(NULLIF(b.kategorie,''),NULLIF(b.kategorie_vorschlag,''),'sonstige') END"


class RevisionConflict(ValueError):
    """A stale form must never overwrite a newer review or payment record."""


def _beleg_ddl(table="buero_belege"):
    if table not in ("buero_belege", "buero_belege_schema_next"):
        raise ValueError("Unbekannte Belegtabelle.")
    extras = ",".join(name + " " + definition for name, definition in _NEW_COLUMNS.items())
    kinds = ",".join("'" + kind + "'" for kind in KINDS)
    return """CREATE TABLE IF NOT EXISTS """ + table + """ (
        id TEXT PRIMARY KEY, typ TEXT NOT NULL,
        lieferant TEXT NOT NULL DEFAULT '', belegnummer TEXT NOT NULL DEFAULT '',
        belegdatum TEXT NOT NULL DEFAULT '', betrag_cent BIGINT,
        faelligkeit TEXT NOT NULL DEFAULT '', notiz TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'entwurf', revision INTEGER NOT NULL DEFAULT 1,
        filename TEXT NOT NULL, mime TEXT NOT NULL, size_bytes INTEGER NOT NULL,
        sha256 TEXT NOT NULL, original_base64 TEXT NOT NULL,
        created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        submitted_at TEXT NOT NULL DEFAULT '', decided_at TEXT NOT NULL DEFAULT '',
        decided_by TEXT NOT NULL DEFAULT '', decision_note TEXT NOT NULL DEFAULT '',
        payable INTEGER NOT NULL DEFAULT 0, paid_date TEXT NOT NULL DEFAULT '',
        payment_reference TEXT NOT NULL DEFAULT '',""" + extras + """,
        CHECK (typ IN (""" + kinds + """)),
        CHECK (status IN ('entwurf','eingereicht','freigegeben','zurueckgewiesen','zahlung_in_bearbeitung','bezahlt')),
        CHECK (betrag_cent IS NULL OR betrag_cent > 0), CHECK (payable IN (0,1)), CHECK (business_confirmed IN (0,1)))"""


def _migrate_sqlite_receipts(db):
    previous = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='buero_belege'").fetchone()
    if not previous:
        return
    ddl = previous["sql"] if hasattr(previous, "keys") else previous[0]
    old_hash_unique = bool(re.search(r"sha256\s+TEXT\s+NOT\s+NULL\s+UNIQUE", ddl, re.I))
    if not old_hash_unique and "zahlungsauftrag" in ddl:
        return
    if not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
    db.execute("SAVEPOINT buero_belege_schema")
    try:
        exists = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='buero_belege_schema_next'").fetchone()
        if exists:
            raise RuntimeError("Belegschema-Migration wegen unerwarteter Zwischentabelle angehalten.")
        db.execute(_beleg_ddl("buero_belege_schema_next"))
        old_cols = [r[1] for r in db.execute("PRAGMA table_info(buero_belege)").fetchall()]
        new_cols = {r[1] for r in db.execute("PRAGMA table_info(buero_belege_schema_next)").fetchall()}
        if not set(old_cols) <= new_cols:
            raise RuntimeError("Belegschema enthält unbekannte Spalten; Migration ohne Datenverlust angehalten.")
        columns = ",".join('"' + c + '"' for c in old_cols)
        db.execute("INSERT INTO buero_belege_schema_next(" + columns + ") SELECT " + columns + " FROM buero_belege")
        before = db.execute("SELECT COUNT(*) AS n FROM buero_belege").fetchone()["n"]
        after = db.execute("SELECT COUNT(*) AS n FROM buero_belege_schema_next").fetchone()["n"]
        different = " OR ".join('o."' + c + '" IS NOT n."' + c + '"' for c in old_cols)
        changed = db.execute("SELECT COUNT(*) AS n FROM buero_belege o JOIN buero_belege_schema_next n ON o.id=n.id WHERE " + different).fetchone()["n"]
        if before != after or changed:
            raise RuntimeError("Belegmigration hat Originalwerte nicht unverändert kopiert; zurückgerollt.")
        db.execute("DROP TABLE buero_belege")
        db.execute("ALTER TABLE buero_belege_schema_next RENAME TO buero_belege")
        db.execute("RELEASE buero_belege_schema")
    except BaseException:
        db.execute("ROLLBACK TO buero_belege_schema")
        db.execute("RELEASE buero_belege_schema")
        raise


def init_office_documents_schema(db, ensure_column=None, *, postgres=False):
    if not postgres:
        _migrate_sqlite_receipts(db)
    db.execute(_beleg_ddl())
    if callable(ensure_column):
        for column, definition in _NEW_COLUMNS.items():
            ensure_column(db, "buero_belege", column, definition)
    else:
        columns = ({row["column_name"] for row in db.execute("SELECT column_name FROM information_schema.columns WHERE table_name='buero_belege' AND table_schema=current_schema()").fetchall()}
                   if postgres else {row[1] for row in db.execute("PRAGMA table_info(buero_belege)").fetchall()})
        for column, definition in _NEW_COLUMNS.items():
            if column not in columns:
                db.execute("ALTER TABLE buero_belege ADD COLUMN " + column + " " + definition)
    if postgres:
        constraints = db.execute("""SELECT c.conname,c.contype,pg_get_constraintdef(c.oid) AS definition
            FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE t.relname='buero_belege' AND n.nspname=current_schema()""").fetchall()
        replace_kind = False
        for constraint in constraints:
            definition = constraint["definition"]
            old_hash = constraint["contype"] == "u" and re.fullmatch(r"UNIQUE\s*\(\s*sha256\s*\)", definition, re.I)
            old_kind = constraint["contype"] == "c" and re.search(r"\btyp\b", definition) and "zahlungsauftrag" not in definition
            if old_hash or old_kind:
                name = constraint["conname"].replace('"', '""')
                db.execute('ALTER TABLE buero_belege DROP CONSTRAINT "' + name + '"')
                replace_kind = replace_kind or bool(old_kind)
        if replace_kind:
            kinds = ",".join("'" + kind + "'" for kind in KINDS)
            db.execute("ALTER TABLE buero_belege ADD CONSTRAINT buero_belege_typ_v2_check CHECK (typ IN (" + kinds + "))")
    rows = db.execute("""SELECT id,sha256,typ,kategorie,zahlungsempfaenger,faelligkeit,verwendungszweck
        FROM buero_belege WHERE dedup_key=''""").fetchall()
    for row in rows:
        db.execute("UPDATE buero_belege SET dedup_key=? WHERE id=? AND dedup_key=''", (_dedup_key(row["sha256"], row), row["id"]))
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_buero_belege_dedup ON buero_belege(dedup_key)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_buero_belege_hash ON buero_belege(sha256)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_buero_belege_status ON buero_belege(status,updated_at)")
    db.execute("""CREATE TABLE IF NOT EXISTS buero_belege_audit (
        id TEXT PRIMARY KEY, beleg_id TEXT NOT NULL, actor TEXT NOT NULL, aktion TEXT NOT NULL,
        details_json TEXT NOT NULL, created_at TEXT NOT NULL)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_buero_belege_audit ON buero_belege_audit(beleg_id,created_at)")
    db.execute("""CREATE TABLE IF NOT EXISTS buero_belege_seen (
        id TEXT PRIMARY KEY, beleg_id TEXT NOT NULL, actor TEXT NOT NULL,
        seen_revision INTEGER NOT NULL, seen_at TEXT NOT NULL, UNIQUE(beleg_id,actor))""")


def _now():
    return datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="microseconds")


def _text(value, maximum, label, *, multiline=False):
    if not isinstance(value, str) or len(value) > maximum or any(
            ord(c) < 32 and not (multiline and c in "\n\r\t") for c in value):
        raise ValueError(f"{label}: höchstens {maximum} Zeichen normalen Text angeben.")
    return value.strip()


def _day(value, label, *, required=False):
    if not value and not required:
        return ""
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value or not 1900 <= parsed.year <= 2100:
            raise ValueError()
    except (TypeError, ValueError):
        raise ValueError(f"{label}: gültiges Datum angeben.") from None
    return value


def _amount(value):
    value = _text(value, 24, "Bruttobetrag")
    if not value:
        return None
    # One unambiguous decimal separator; do not silently reinterpret thousands.
    if not re.fullmatch(r"(?:0|[1-9]\d{0,8})(?:[.,]\d{1,2})?", value):
        raise ValueError("Bruttobetrag ohne Tausendertrennzeichen mit höchstens zwei Nachkommastellen eingeben.")
    try:
        amount = Decimal(value.replace(",", "."))
    except InvalidOperation:
        raise ValueError("Gültigen Bruttobetrag eingeben.") from None
    if amount <= 0:
        raise ValueError("Bruttobetrag muss größer als null sein.")
    return int(amount * 100)


# Registered national lengths, SWIFT ISO 13616 registry release 101 (Dec 2025).
# Validation is local only; it does not verify ownership or account existence.
_IBAN_LENGTHS = dict(pair.split(":") for pair in (
    "AD:24 AE:23 AL:28 AT:20 AZ:28 BA:20 BE:16 BG:22 BH:22 BI:27 BR:29 BY:28 "
    "CH:21 CR:22 CY:28 CZ:24 DE:22 DJ:27 DK:18 DO:28 EE:20 EG:29 ES:24 FI:18 "
    "FK:18 FO:18 FR:27 GB:22 GE:22 GI:23 GL:18 GR:27 GT:28 HN:28 HR:21 HU:28 "
    "IE:22 IL:23 IQ:23 IS:26 IT:27 JO:30 KW:30 KZ:20 LB:28 LC:32 LI:21 LT:20 "
    "LU:20 LV:21 LY:25 MC:27 MD:24 ME:22 MK:19 MN:20 MR:27 MT:31 MU:30 NI:28 "
    "NL:18 NO:15 OM:23 PK:24 PL:28 PS:29 PT:25 QA:29 RO:24 RS:22 RU:33 SA:24 "
    "SC:31 SD:18 SE:24 SI:19 SK:24 SM:27 SO:23 ST:25 SV:28 TL:23 TN:24 TR:26 "
    "UA:29 VA:22 VG:24 XK:20 YE:30"
).split())


def normalize_iban(value, required=False):
    if not isinstance(value, str) or len(value) > 100 or not re.sub(r"\s+", "", value).isascii():
        raise ValueError("IBAN im vorgesehenen Format angeben.")
    value = re.sub(r"\s+", "", value).upper()
    if not value:
        if required:
            raise ValueError("IBAN aus der Original-Zahlungsgrundlage ergänzen.")
        return ""
    if (not re.fullmatch(r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}", value)
            or len(value) != int(_IBAN_LENGTHS.get(value[:2], 0))
            or not 2 <= int(value[2:4]) <= 98
            or value.startswith("DE") and not value[4:].isdigit()):
        raise ValueError("IBAN: Länderformat oder Länge ist ungültig. Am Original prüfen.")
    numeric = "".join(str(ord(c) - 55) if c.isalpha() else c for c in value[4:] + value[:4])
    if int(numeric) % 97 != 1:
        raise ValueError("IBAN-Prüfziffer ist ungültig. Am Original prüfen.")
    return value


def normalize_bic(value):
    if not isinstance(value, str) or len(value) > 30 or not re.sub(r"\s+", "", value).isascii():
        raise ValueError("BIC muss aus 8 oder 11 Zeichen bestehen.")
    value = re.sub(r"\s+", "", value).upper()
    if value and not re.fullmatch(r"[A-Z0-9]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?", value):
        raise ValueError("BIC-Format ist ungültig: 8 oder 11 Zeichen am Original prüfen.")
    return value


def _fields(payload):
    if not isinstance(payload, dict) or set(payload) - FIELDS:
        raise ValueError("Nur die vorgesehenen Belegangaben bearbeiten.")
    kind = payload.get("typ", "")
    if kind not in KINDS:
        raise ValueError("Rechnung, Lieferschein, Bescheid oder sonstige Post auswählen.")
    category = payload.get("kategorie", "")
    if category and category not in CATEGORIES:
        raise ValueError("Vorgesehenen Belegordner auswählen.")
    mode = payload.get("zahlungsmodus", "")
    if mode not in ("", "einzelzahlung", "monatliche_miete") or (mode == "monatliche_miete" and (kind != "zahlungsauftrag" or category != "miete")):
        raise ValueError("Monatliche Mietvorlage nur für einen Zahlungsauftrag im Ordner Miete verwenden.")
    return dict(typ=kind, kategorie=category, lieferant=_text(payload.get("lieferant", ""), 160, "Lieferant / Absender"),
                belegnummer=_text(payload.get("belegnummer", ""), 100, "Belegnummer"),
                belegdatum=_day(payload.get("belegdatum", ""), "Belegdatum"),
                betrag_cent=_amount(payload.get("betrag_brutto", "")),
                faelligkeit=_day(payload.get("faelligkeit", ""), "Fälligkeit"),
                notiz=_text(payload.get("notiz", ""), 2000, "Notiz", multiline=True),
                zahlungsempfaenger=_text(payload.get("zahlungsempfaenger", ""), 160, "Zahlungsempfänger"),
                iban=normalize_iban(payload.get("iban", "")), bic=normalize_bic(payload.get("bic", "")),
                verwendungszweck=_text(payload.get("verwendungszweck", ""), 300, "Verwendungszweck"), zahlungsmodus=mode)


def _dedup_key(digest, fields):
    if fields["typ"] != "zahlungsauftrag":
        return "datei:" + digest
    if fields["kategorie"] == "privat":
        # A private single payment is not a recurring contract template.
        # Completing missing bank/reference data must not create another task.
        return "privatauftrag:" + digest
    # A deposit and a monthly rent may cite the very same contract. They are
    # distinct payment instructions, never newly fabricated invoices. Exclude
    # amount from identity: correcting it must not create a second payment task.
    identity = [digest, fields["kategorie"], fields["zahlungsempfaenger"], fields["faelligkeit"], fields["verwendungszweck"]]
    normalised = [" ".join(str(value).casefold().split()) for value in identity]
    return "auftrag:" + hashlib.sha256(json.dumps(normalised, ensure_ascii=False).encode()).hexdigest()


def _transfer_missing(row):
    """Informational gaps for every document, never approval requirements."""
    return [label for column, label in (("zahlungsempfaenger", "Zahlungsempfänger"), ("iban", "IBAN"),
            ("bic", "BIC"), ("faelligkeit", "Fälligkeit"), ("verwendungszweck", "Verwendungszweck"))
            if row.get(column) in (None, "")]


def _payment_order_ready(row):
    """Check the source-backed task; bank details may legitimately be absent."""
    if row["kategorie"] not in ("miete", "kaution", "privat"):
        raise ValueError("Zahlungsauftrag als Miete, Kaution oder Privat einordnen.")
    if row["betrag_cent"] is None or row["betrag_cent"] <= 0:
        raise ValueError("Für den Zahlungsauftrag ist ein geprüfter positiver Zahlbetrag erforderlich.")
    normalize_iban(row.get("iban", ""))
    normalize_bic(row.get("bic", ""))
    if row["zahlungsmodus"] not in ("einzelzahlung", "monatliche_miete"):
        raise ValueError("Begründete Einzelzahlung oder monatliche Mietvorlage ausdrücklich auswählen.")
    if row["zahlungsmodus"] == "monatliche_miete" and row["kategorie"] != "miete":
        raise ValueError("Eine monatliche Mietvorlage nur im Ordner Miete verwenden.")
    if row["kategorie"] == "privat" and row["zahlungsmodus"] != "einzelzahlung":
        raise ValueError("Privaten Zahlungsauftrag als begründete Einzelzahlung vorbereiten.")
    if row["zahlungsmodus"] == "einzelzahlung" and not row["notiz"]:
        raise ValueError("Einzelzahlung anhand der vorliegenden Quelle in der Notiz begründen.")


def _submission_ready(row):
    """Validate the complete proposed classification without persisting it."""
    payload = {name: row[name] for name in FIELDS if name != "betrag_brutto"}
    payload["betrag_brutto"] = "" if row["betrag_cent"] is None else f"{row['betrag_cent'] // 100}.{row['betrag_cent'] % 100:02d}"
    _fields(payload)  # Also checks classification-dependent rental modes.
    if not row["lieferant"] and row["typ"] in ("rechnung", "bescheid"):
        raise ValueError("Lieferant / Absender vor der Prüfung ergänzen.")
    if row["typ"] in ("rechnung", "bescheid") and not row["belegdatum"]:
        raise ValueError("Belegdatum vor der Prüfung ergänzen.")
    if row["typ"] == "rechnung" and (not row["belegnummer"] or row["betrag_cent"] is None):
        raise ValueError("Rechnungsnummer und Bruttobetrag vor der Prüfung ergänzen.")
    if row["typ"] == "zahlungsauftrag":
        _payment_order_ready(row)


def _review_original(db, row):
    blob = db.execute("SELECT original_base64 FROM buero_belege WHERE id=?", (row["id"],)).fetchone()
    try:
        raw = base64.b64decode(blob["original_base64"], validate=True)
    except (TypeError, ValueError):
        raise ValueError("Das gespeicherte Original ist nicht unverändert prüfbar. Vor der Freigabe klären.") from None
    if len(raw) != row["size_bytes"] or not 0 < len(raw) <= MAX_ORIGINAL_BYTES or hashlib.sha256(raw).hexdigest() != row["sha256"]:
        raise ValueError("Das gespeicherte Original ist nicht unverändert prüfbar. Vor der Freigabe klären.")


def _version(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9]\d{0,9}", value) or int(value) > 2147483647:
        raise RevisionConflict("Dieses Formular ist nicht mehr aktuell. Beleg neu öffnen.")
    return int(value)


def _key(value):
    if not isinstance(value, str) or not _KEY.fullmatch(value):
        raise LookupError("Beleg nicht gefunden.")
    return value


def _display_amount(cents):
    if cents is None:
        return "Noch offen"
    euros, remainder = divmod(int(cents), 100)
    return f"{euros:,}".replace(",", ".") + f",{remainder:02d} €"


def _lexware_url(value):
    try:
        if not isinstance(value, str) or len(value) > 1000 or any(ord(c) < 32 for c in value):
            return ""
        parsed = urlsplit(value)
        return value if (parsed.scheme == "https" and parsed.hostname in ("app.lexware.de", "app.lexoffice.de")
                         and not parsed.username and not parsed.password and parsed.port in (None, 443)) else ""
    except ValueError:
        return ""


class OfficeDocuments:
    def __init__(self, portal):
        self.p = portal
        self.init_schema()

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

    def init_schema(self):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            init_office_documents_schema(db, getattr(self.p, "ensure_column", None), postgres=bool(getattr(self.p, "USE_POSTGRES", False)))

    def identity(self, *, admin=False, db=None, lock=False):
        if admin:
            if not session.get("admin"):
                raise PermissionError("Nur die Werkstattleitung darf Belege freigeben.")
            return {"actor": "admin", "mitarbeiter_name": "Werkstattleitung"}
        access = getattr(self.p, "office_access", None)
        mid = session.get("assistent_mid")
        if lock and db is not None and type(mid) is int and mid > 0:
            # Same order as grant/revocation writes. The current right cannot
            # disappear between the identity check and a committed mutation.
            db.execute("UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?", (mid,))
            db.execute("UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?", (mid,))
            db.execute("UPDATE buero_zugaenge SET version=version WHERE mitarbeiter_id=?", (mid,))
        who = (access.identity(db=db) if db is not None else access.identity()) if access else None
        if (not isinstance(who, dict) or not isinstance(who.get("actor"), str) or not who["actor"]
                or "belege" not in who.get("office_categories", ())):
            raise PermissionError("Mit einem freigeschalteten Bürozugang anmelden.")
        return who

    @staticmethod
    def _row(db, key):
        row = db.execute(f"SELECT {_META} FROM buero_belege WHERE id=?", (_key(key),)).fetchone()
        if not row:
            raise LookupError("Beleg nicht gefunden.")
        return dict(row)

    @staticmethod
    def _view(row, *, admin=False):
        value = dict(row)
        transfer_missing = _transfer_missing(value)
        category = "privat" if value["bereich"] == "privat" else value["kategorie"] or value["kategorie_vorschlag"] or "sonstige"
        recipient = value["status"] in ("entwurf", "eingereicht") if admin else value["status"] in ("freigegeben", "zurueckgewiesen", "zahlung_in_bearbeitung")
        value.update(typ_label=KINDS[value["typ"]], status_label=STATES[value["status"]],
                     display_name=value["zahlungsempfaenger"] if value["typ"] == "zahlungsauftrag" and value["zahlungsempfaenger"] else value["lieferant"],
                     kategorie_effektiv=category, kategorie_label=CATEGORIES.get(category, "Sonstige Post"),
                     bereich_label=AREAS.get(value["bereich"], AREAS["unklar"]),
                     is_new=recipient and value["revision"] > value.get("seen_revision", value["revision"]),
                     lexware_link=_lexware_url(value["lexware_url"]) if value["lexware_beleg_id"] else "",
                     betrag_label=_display_amount(value["betrag_cent"]),
                     betrag_input="" if value["betrag_cent"] is None else f"{value['betrag_cent'] // 100}.{value['betrag_cent'] % 100:02d}",
                     transfer_missing=transfer_missing,
                     transfer_warning="Nicht angegeben: " + ", ".join(transfer_missing) + "." if transfer_missing else "",
                     payment_data_pending=any(label != "BIC" for label in transfer_missing),
                     private_payment_followup=bool(value["status"] == "freigegeben" and value["typ"] == "post"
                                                   and value["bereich"] == "privat" and not value["payable"]),
                     editable=value["status"] in ("entwurf", "zurueckgewiesen"),
                     payment_allowed=bool(value["payable"] and (value["business_confirmed"] or value["bereich"] == "privat")
                                          and value["typ"] in ("rechnung", "bescheid", "zahlungsauftrag")
                                          and (value["typ"] != "zahlungsauftrag" or value["quelle_geprueft"])
                                          and value["status"] in ("freigegeben", "zahlung_in_bearbeitung")))
        return value

    @staticmethod
    def _visibility(row, who, *, admin=False):
        if admin or row["created_by"] == who["actor"] or (row["bereich"] in ("geschaeftlich", "privat")
                and row["status"] in ("freigegeben", "zahlung_in_bearbeitung", "bezahlt") and row["decided_by"] == "admin"):
            return
        raise LookupError("Beleg nicht gefunden.")

    @staticmethod
    def _seen(db, who, key, revision):
        # Never accept a client-supplied future revision: callers pass a snapshot
        # actually read from this record. Concurrent later events remain unread.
        db.execute("""INSERT INTO buero_belege_seen(id,beleg_id,actor,seen_revision,seen_at) VALUES(?,?,?,?,?)
            ON CONFLICT(beleg_id,actor) DO UPDATE SET seen_revision=excluded.seen_revision,seen_at=excluded.seen_at
            WHERE buero_belege_seen.seen_revision < excluded.seen_revision RETURNING id""",
            (secrets.token_hex(16), key, who["actor"], revision, _now()))

    @staticmethod
    def _audit(db, who, key, action, before=None, *, extra=None):
        row = OfficeDocuments._row(db, key)
        metadata = {k: row[k] for k in ("typ", "kategorie", "lieferant", "belegnummer", "belegdatum", "betrag_cent", "faelligkeit", "notiz",
                                      "zahlungsempfaenger", "iban", "bic", "verwendungszweck", "zahlungsmodus", "quelle_geprueft",
                                      "kategorie_vorschlag", "typ_vorschlag", "klassifizierung_hinweis", "lexware_beleg_id", "lexware_status", "lexware_url")}
        details = dict(revision=row["revision"], status=row["status"], vorher_status=before, metadata=metadata, bereich=row["bereich"],
                       payable=row["payable"], business_confirmed=row["business_confirmed"], original_sha256=row["sha256"],
                       metadata_sha256=hashlib.sha256(json.dumps(metadata, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
                       decision_note=row["decision_note"], paid_date=row["paid_date"], payment_reference=row["payment_reference"])
        details.update(kategorie=row["kategorie"], kategorie_vorschlag=row["kategorie_vorschlag"],
                       typ_vorschlag=row["typ_vorschlag"], lexware_beleg_id=row["lexware_beleg_id"], lexware_status=row["lexware_status"])
        if extra:
            details.update(extra)
        db.execute("INSERT INTO buero_belege_audit(id,beleg_id,actor,aktion,details_json,created_at) VALUES(?,?,?,?,?,?)",
                   (secrets.token_hex(16), key, who["actor"], action, json.dumps(details, sort_keys=True, ensure_ascii=False), _now()))
        OfficeDocuments._seen(db, who, key, row["revision"])

    def _backup(self):
        hook = getattr(self.p, "schedule_change_backup", None)
        if callable(hook):
            hook("buero-belege")

    def list(self, *, admin=False, status="alle", category="alle", group="alle"):
        who = self.identity(admin=admin)
        if status != "alle" and status not in STATES:
            raise ValueError("Gültigen Belegstatus auswählen.")
        if category != "alle" and category not in CATEGORIES or group not in ("alle", "ungeprueft", "freigegeben"):
            raise ValueError("Gültigen Ordner und Arbeitsbereich auswählen.")
        with self.db() as db:
            terms, args = [], [who["actor"]]
            if not admin:
                terms.append(_VISIBLE)
                args.append(who["actor"])
            if status != "alle":
                terms.append("b.status=?")
                args.append(status)
            if category != "alle":
                terms.append("(" + _CATEGORY_SQL + ")=?")
                args.append(category)
            if group == "ungeprueft":
                terms.append("b.status IN ('entwurf','eingereicht','zurueckgewiesen')")
            elif group == "freigegeben":
                terms.append("b.status IN ('freigegeben','zahlung_in_bearbeitung','bezahlt')")
            where = " WHERE " + " AND ".join(terms) if terms else ""
            columns = ",".join("b." + column for column in _META.split(","))
            rows = db.execute(f"SELECT {columns},COALESCE(s.seen_revision,0) AS seen_revision FROM buero_belege b "
                              "LEFT JOIN buero_belege_seen s ON s.beleg_id=b.id AND s.actor=?" + where +
                              " ORDER BY b.updated_at DESC,b.id DESC LIMIT 201", tuple(args)).fetchall()
        return {"rows": [self._view(row, admin=admin) for row in rows[:200]], "limited": len(rows) > 200}

    def detail(self, key, *, admin=False):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=admin, db=db, lock=True)
            row = self._row(db, key)
            self._visibility(row, who, admin=admin)
            self._seen(db, who, key, row["revision"])
            audits = db.execute("SELECT actor,aktion,details_json,created_at FROM buero_belege_audit WHERE beleg_id=? ORDER BY created_at,id", (key,)).fetchall()
        result = self._view(row, admin=admin)
        result["audit"] = [dict(a, details=json.loads(a["details_json"])) for a in audits]
        return result

    def counters(self, *, admin=False):
        who = self.identity(admin=admin)
        categories = {key: {"label": label, "neue_anzahl": 0, "offen_anzahl": 0, "zahlung_offen": 0, "gesamt": 0}
                      for key, label in CATEGORIES.items()}
        result = dict(neue_anzahl=0, offen_anzahl=0, zahlung_offen=0, ungeprueft=0, freigegeben=0, kategorien=categories)
        with self.db() as db:
            rows = db.execute("""SELECT b.status,b.typ,b.payable,b.business_confirmed,b.quelle_geprueft,b.bereich,b.kategorie,b.kategorie_vorschlag,b.revision,
                COALESCE(s.seen_revision,0) AS seen_revision FROM buero_belege b
                LEFT JOIN buero_belege_seen s ON s.beleg_id=b.id AND s.actor=?""" + (" WHERE " + _VISIBLE if not admin else ""),
                (who["actor"], who["actor"]) if not admin else (who["actor"],)).fetchall()
        for row in rows:
            category = "privat" if row["bereich"] == "privat" else row["kategorie"] or row["kategorie_vorschlag"] or "sonstige"
            bucket = categories.get(category, categories["sonstige"])
            new = row["revision"] > row["seen_revision"] and (row["status"] in ("entwurf", "eingereicht") if admin else row["status"] in ("freigegeben", "zurueckgewiesen", "zahlung_in_bearbeitung"))
            payment = bool(row["payable"] and (row["business_confirmed"] or row["bereich"] == "privat")
                           and row["status"] in ("freigegeben", "zahlung_in_bearbeitung") and row["typ"] in ("rechnung", "bescheid", "zahlungsauftrag")
                           and (row["typ"] != "zahlungsauftrag" or row["quelle_geprueft"]))
            opened = row["status"] in ("entwurf", "eingereicht") if admin else row["status"] in ("entwurf", "zurueckgewiesen") or payment
            bucket["gesamt"] += 1
            for name, value in (("neue_anzahl", new), ("offen_anzahl", opened), ("zahlung_offen", payment)):
                bucket[name] += int(value)
                result[name] += int(value)
            result["ungeprueft" if row["status"] in ("entwurf", "eingereicht", "zurueckgewiesen") else "freigegeben"] += 1
        return result

    def record_scan_suggestion(self, key, revision, category, hint, suggested_kind="", *, intake=False):
        if category not in CATEGORIES or suggested_kind and suggested_kind not in KINDS:
            raise ValueError("Gültigen Klassifizierungsvorschlag angeben.")
        hint = _text(hint, 1000, "Klassifizierungshinweis", multiline=True)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = {"actor": "system:chef-posteingang"} if intake else self.identity(admin=bool(session.get("admin")), db=db, lock=True)
            row = self._current(db, key, revision, {"entwurf", "zurueckgewiesen", "eingereicht"})
            if row["klassifizierung_hinweis"].startswith(_BANK_CONFLICT_HINT) and not hint.startswith(_BANK_CONFLICT_HINT):
                hint = (_BANK_CONFLICT_HINT + hint)[:1000]
            if intake:
                if row["created_by"] != "system:chef-posteingang" or not row["source_ref_hash"]:
                    raise PermissionError("Kein eigener automatischer Eingang.")
            else:
                self._visibility(row, who, admin=bool(session.get("admin")))
            if (row["kategorie_vorschlag"], row["typ_vorschlag"], row["klassifizierung_hinweis"], row["analyse_status"], row["verarbeitung_hinweis"]) == (category, suggested_kind, hint, "vorschlag", ""):
                return
            changed = db.execute("""UPDATE buero_belege SET kategorie_vorschlag=?,typ_vorschlag=?,klassifizierung_hinweis=?,
                analyse_status='vorschlag',verarbeitung_hinweis='',revision=revision+1,updated_at=? WHERE id=? AND revision=? AND status=?""",
                (category, suggested_kind, hint, _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde während der Analyse geändert. Vorschlag nicht übernommen.")
            self._audit(db, who, key, "klassifizierung_vorgeschlagen", row["status"])
        self._backup()

    def record_extracted_fields(self, key, revision, fields, hint="", source="local", *, intake=False):
        """Fill empty draft metadata; classification remains an unapproved proposal.

        The trusted intake capability is restricted to its own protected review
        copies. No HTTP route accepts extraction payloads or this capability.
        """
        labels = {"lieferant": "Absender", "belegnummer": "Belegnummer", "belegdatum": "Belegdatum",
                  "faelligkeit": "Fälligkeit", "betrag_brutto": "Bruttobetrag", "typ": "Belegart", "kategorie": "Ordner",
                  "zahlungsempfaenger": "Zahlungsempfänger", "iban": "IBAN", "bic": "BIC", "verwendungszweck": "Verwendungszweck"}
        if not isinstance(fields, dict) or set(fields) - set(labels):
            raise ValueError("Nur vorgesehene Belegangaben aus der Analyse übernehmen.")
        if source not in ("local", "ai", "mixed"):
            raise ValueError("Gültige Analysequelle angeben.")
        hint = _text(hint, 1000, "Analysehinweis", multiline=True)
        values, skipped = {}, []
        for name, value in fields.items():
            if value is None or value == "":
                continue
            try:
                if name == "lieferant": value = _text(value, 160, labels[name])
                elif name == "belegnummer": value = _text(value, 100, labels[name])
                elif name in ("belegdatum", "faelligkeit"): value = _day(value, labels[name])
                elif name == "betrag_brutto": value = _amount(value)
                elif name == "zahlungsempfaenger": value = _text(value, 160, labels[name])
                elif name == "iban": value = normalize_iban(value)
                elif name == "bic": value = normalize_bic(value)
                elif name == "verwendungszweck": value = _text(value, 300, labels[name])
                elif name == "typ" and value not in set(KINDS) - {"zahlungsauftrag"}: raise ValueError()
                elif name == "kategorie" and value not in CATEGORIES: raise ValueError()
                if value not in (None, ""):
                    values[name] = value
            except (TypeError, ValueError):
                skipped.append(name)
        if skipped:
            hint = (hint + "\nNicht eindeutig übernommen: " + ", ".join(labels[name] for name in skipped) + ". Bitte am Original prüfen.").strip()[:1000]
        with self.p.portal_originals_operation_lock(), self.db() as db:
            admin = not intake and bool(session.get("admin"))
            who = {"actor": "system:chef-posteingang"} if intake else self.identity(admin=admin, db=db, lock=True)
            row = self._current(db, key, revision, {"entwurf", "zurueckgewiesen", "eingereicht"})
            own_intake = row["created_by"] == "system:chef-posteingang" and bool(row["source_ref_hash"])
            if intake and not own_intake:
                raise PermissionError("Kein eigener automatischer Eingang.")
            if not intake:
                self._visibility(row, who, admin=admin)
            if row["status"] == "eingereicht" and not own_intake:
                raise RevisionConflict("Bereits eingereichte Angaben bleiben unverändert. Zur Korrektur zurückgeben.")
            if row["typ"] == "zahlungsauftrag":
                raise ValueError("Zahlungsaufträge anhand ihrer Original-Zahlungsgrundlage manuell prüfen.")
            bank_fields = {"zahlungsempfaenger", "iban", "bic"}
            bank_conflict = (bool(row["iban"]) and values.get("iban") != row["iban"] or
                             bool(row["bic"]) and values.get("bic") != row["bic"] or
                             bool(row["zahlungsempfaenger"]) and
                             " ".join(str(values.get("zahlungsempfaenger", "")).casefold().split()) !=
                             " ".join(row["zahlungsempfaenger"].casefold().split()))
            if bank_conflict and bank_fields.intersection(values):
                for name in bank_fields:
                    values.pop(name, None)
                hint = (_BANK_CONFLICT_HINT + hint)[:1000]
            changes, filled = {}, []
            for name, value in values.items():
                column = {"betrag_brutto": "betrag_cent", "typ": "typ_vorschlag", "kategorie": "kategorie_vorschlag"}.get(name, name)
                if name in ("typ", "kategorie"):
                    if name == "kategorie" and row["kategorie"]:
                        continue
                elif row[column] not in (None, ""):
                    continue
                if row[column] != value:
                    changes[column] = value
                    filled.append(name)
            for column, value in (("klassifizierung_hinweis", hint), ("analyse_status", "vorschlag"), ("verarbeitung_hinweis", "")):
                if row[column] != value:
                    changes[column] = value
            if not changes:
                return row["revision"]
            changed = db.execute("UPDATE buero_belege SET " + ",".join(column + "=?" for column in changes) +
                                 ",revision=revision+1,updated_at=? WHERE id=? AND revision=? AND status=?",
                                 (*changes.values(), _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde während der Analyse geändert. Neu laden und erneut prüfen.")
            self._audit(db, who, key, "belegangaben_erkannt", row["status"],
                        extra={"analyse_quelle": source, "ergaenzte_felder": filled, "nicht_uebernommene_felder": skipped})
            current_revision = row["revision"] + 1
        self._backup()
        return current_revision

    def record_lexware_reference(self, key, revision, document_id, url, status="bereit"):
        document_id = _text(document_id, 100, "Lexware-Belegreferenz")
        if not document_id or not _lexware_url(url):
            raise ValueError("Geprüfte Lexware-Belegreferenz mit offizieller HTTPS-Adresse angeben.")
        if status not in ("bereit", "uebertragen"):
            raise ValueError("Gültigen Lexware-Belegstatus angeben.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=bool(session.get("admin")), db=db, lock=True)
            row = self._current(db, key, revision, {"freigegeben", "zahlung_in_bearbeitung", "bezahlt"})
            if not row["payable"] or not row["business_confirmed"] or row["typ"] not in ("rechnung", "bescheid"):
                raise ValueError("Nur ausdrücklich zahlungsfreigegebene Belege können diesen Lexware-Verweis erhalten.")
            if row["lexware_beleg_id"] and row["lexware_beleg_id"] != document_id:
                raise RevisionConflict("Beleg ist bereits einer anderen Lexware-Referenz zugeordnet.")
            if (row["lexware_beleg_id"], row["lexware_url"], row["lexware_status"], row["verarbeitung_hinweis"]) == (document_id, url, status, ""):
                return
            changed = db.execute("""UPDATE buero_belege SET lexware_beleg_id=?,lexware_url=?,lexware_status=?,
                verarbeitung_hinweis='',revision=revision+1,updated_at=? WHERE id=? AND revision=? AND status=?""",
                (document_id, url, status, _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Lexware-Verweis erneut prüfen.")
            self._audit(db, who, key, "lexware_verweis_hinterlegt", row["status"])
        self._backup()

    def record_lexware_filing_reference(self, key, revision, document_id, url, status="uebertragen"):
        """Link an explicit filing without granting payment or business approval."""
        try:
            canonical_id = str(UUID(document_id))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Gültige Lexware-Belegreferenz angeben.") from None
        expected_url = "https://app.lexware.de/permalink/vouchers/edit/" + canonical_id
        if document_id.lower() != canonical_id or url != expected_url or status not in ("bereit", "uebertragen"):
            raise ValueError("Geprüfte Lexware-Belegreferenz mit passendem offiziellen Link angeben.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            admin = bool(session.get("admin"))
            who = self.identity(admin=admin, db=db, lock=True)
            row = self._current(db, key, revision, set(STATES))
            self._visibility(row, who, admin=admin)
            _review_original(db, row)
            if row["lexware_beleg_id"] and row["lexware_beleg_id"] != canonical_id:
                raise RevisionConflict("Beleg ist bereits einer anderen Lexware-Referenz zugeordnet.")
            if (row["lexware_beleg_id"], row["lexware_url"], row["lexware_status"], row["verarbeitung_hinweis"]) == (canonical_id, url, status, ""):
                return
            changed = db.execute("""UPDATE buero_belege SET lexware_beleg_id=?,lexware_url=?,lexware_status=?,
                verarbeitung_hinweis='',revision=revision+1,updated_at=? WHERE id=? AND revision=? AND status=?""",
                (canonical_id, url, status, _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Lexware-Ablage erneut prüfen.")
            self._audit(db, who, key, "lexware_ablage_hinterlegt", row["status"],
                        extra={"ablage_zur_pruefung": True, "keine_zahlungsfreigabe": True})
        self._backup()

    def record_processing_notice(self, key, message):
        message = _text(message, 500, "Verarbeitungshinweis")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            admin = bool(session.get("admin"))
            who = self.identity(admin=admin, db=db, lock=True)
            row = self._row(db, key)
            self._visibility(row, who, admin=admin)
            if row["verarbeitung_hinweis"] == message:
                return
            changed = db.execute("UPDATE buero_belege SET verarbeitung_hinweis=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                 (message, _now(), key, row["revision"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Verarbeitungshinweis erneut prüfen.")
            self._audit(db, who, key, "verarbeitungshinweis", row["status"])
        self._backup()

    def intake_snapshot(self, key):
        """Trusted Python-only original reader, restricted to automatic intake."""
        with self.db() as db:
            row = self._row(db, key)
            if row["created_by"] != "system:chef-posteingang" or not row["source_ref_hash"]:
                raise PermissionError("Kein automatischer Eingang.")
            blob = db.execute("SELECT original_base64 FROM buero_belege WHERE id=?", (key,)).fetchone()
            try:
                raw = base64.b64decode(blob["original_base64"], validate=True)
            except (TypeError, ValueError):
                raise LookupError("Original ist nicht unverändert lesbar.") from None
            if len(raw) != row["size_bytes"] or not 0 < len(raw) <= MAX_ORIGINAL_BYTES or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                raise LookupError("Original ist nicht unverändert lesbar.")
        return row, raw

    def pending_invoice_count(self):
        self.identity(admin=True)
        with self.db() as db:
            return db.execute("SELECT COUNT(*) AS anzahl FROM buero_belege WHERE status='eingereicht' AND typ='rechnung'").fetchone()["anzahl"]

    def pending_review_count(self):
        self.identity(admin=True)
        with self.db() as db:
            return db.execute("SELECT COUNT(*) AS anzahl FROM buero_belege WHERE status='eingereicht'").fetchone()["anzahl"]

    def create(self, payload, upload, *, admin=False, bereich="unklar"):
        return self._create(payload, upload, admin=admin, bereich=bereich)

    def create_from_intake(self, payload, upload, *, source_ref, bereich="unklar"):
        """Trusted Python-only CEO intake. No route accepts this capability.

        The source reference is hashed; imports only create a protected review
        item. They never establish business ownership, approval or payment.
        """
        source_ref = _text(source_ref, 500, "Interne Quellenreferenz")
        if not source_ref:
            raise ValueError("Eine interne Quellenreferenz ist erforderlich.")
        return self._create(payload, upload, bereich=bereich, source_ref_hash=hashlib.sha256(source_ref.encode()).hexdigest(), intake=True)

    def find_by_sha256(self, digest):
        """Trusted Python-only dedup adapter; not exposed by an HTTP endpoint."""
        if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("Gültigen Original-Hash angeben.")
        with self.db() as db:
            row = db.execute(f"SELECT {_META} FROM buero_belege WHERE sha256=? ORDER BY created_at,id LIMIT 1", (digest,)).fetchone()
        return dict(row) if row else None

    def _create(self, payload, upload, *, admin=False, bereich="unklar", source_ref_hash="", intake=False):
        if not intake:
            self.identity(admin=admin)
        if bereich not in AREAS or (not admin and not intake and bereich != "unklar"):
            raise ValueError("Den Belegbereich durch die Werkstattleitung zuordnen lassen.")
        fields = _fields(payload)
        # Reuse the existing local PDF/JPEG/PNG integrity validation; no OCR,
        # document text extraction, conversion or external service is invoked.
        from werkstatt_mitarbeiter_portal import _document
        raw, filename, mime = _document(upload, label="Beleg")
        if len(raw) > MAX_ORIGINAL_BYTES:
            raise ValueError("Beleg darf höchstens 10 MB groß sein.")
        digest = hashlib.sha256(raw).hexdigest()
        dedup_key = _dedup_key(digest, fields)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = {"actor": "system:chef-posteingang"} if intake else self.identity(admin=admin, db=db, lock=True)
            existing = db.execute("SELECT id FROM buero_belege WHERE dedup_key=?", (dedup_key,)).fetchone()
            if existing:
                if not intake:
                    self._visibility(self._row(db, existing["id"]), who, admin=admin)
                return existing["id"], True
            key, now = secrets.token_hex(16), _now()
            columns = ("id", *_WRITE_FIELDS, "filename", "mime", "size_bytes", "sha256", "dedup_key", "original_base64", "created_by", "created_at", "updated_at",
                       "bereich", "source_ref_hash", "status", "submitted_at")
            values = (key, *(fields[k] for k in _WRITE_FIELDS), filename, mime, len(raw), digest, dedup_key,
                      base64.b64encode(raw).decode("ascii"), who["actor"], now, now, bereich,
                      source_ref_hash, "eingereicht" if intake else "entwurf", now if intake else "")
            result = db.execute("INSERT INTO buero_belege(" + ",".join(columns) + ") VALUES(" + ",".join("?" for _ in values) +
                                ") ON CONFLICT(dedup_key) DO NOTHING RETURNING id", values).fetchone()
            if not result:
                return db.execute("SELECT id FROM buero_belege WHERE dedup_key=?", (dedup_key,)).fetchone()["id"], True
            self._audit(db, who, key, "automatisch_vorgelegt" if intake else "erfasst")
        self._backup()
        return key, False

    @staticmethod
    def _current(db, key, version, allowed):
        row = OfficeDocuments._row(db, key)
        if row["revision"] != _version(version) or row["status"] not in allowed:
            raise RevisionConflict("Der Beleg wurde bereits geändert oder freigegeben. Neu laden und aktuellen Stand prüfen.")
        return row

    def update(self, key, version, payload, *, admin=False):
        if not isinstance(payload, dict) or set(payload) - FIELDS:
            raise ValueError("Nur die vorgesehenen Belegangaben bearbeiten.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=admin, db=db, lock=True)
            row = self._current(db, key, version, {"entwurf", "zurueckgewiesen", "eingereicht"} if admin else {"entwurf", "zurueckgewiesen"})
            self._visibility(row, who, admin=admin)
            combined = {name: row[name] for name in FIELDS if name != "betrag_brutto"}
            combined["betrag_brutto"] = "" if row["betrag_cent"] is None else f"{row['betrag_cent'] // 100}.{row['betrag_cent'] % 100:02d}"
            combined.update(payload)
            fields = _fields(combined)
            dedup_key = _dedup_key(row["sha256"], fields)
            if db.execute("SELECT id FROM buero_belege WHERE dedup_key=? AND id<>?", (dedup_key, key)).fetchone():
                raise ValueError("Dieser Beleg oder Zahlungsauftrag ist bereits vorhanden. Bestehenden Eintrag prüfen.")
            next_state = "eingereicht" if admin and row["status"] == "eingereicht" else "entwurf"
            changed = db.execute("UPDATE buero_belege SET " + ",".join(name + "=?" for name in _WRITE_FIELDS) + """,
                dedup_key=?,status=?,payable=0,business_confirmed=0,quelle_geprueft=0,decided_at='',decided_by='',decision_note='',revision=revision+1,updated_at=?
                WHERE id=? AND revision=? AND status=?""",
                (*(fields[k] for k in _WRITE_FIELDS), dedup_key, next_state, _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Bitte neu laden.")
            self._audit(db, who, key, "angaben_geaendert", row["status"])
        self._backup()

    def submit(self, key, version, *, admin=False, kind=None, category=None):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=admin, db=db, lock=True)
            row = self._current(db, key, version, {"entwurf", "zurueckgewiesen"})
            self._visibility(row, who, admin=admin)
            if kind is not None:
                if kind not in KINDS:
                    raise ValueError("Belegart vor der Einreichung prüfen.")
                row["typ"] = kind
            if category is not None:
                if category and category not in CATEGORIES:
                    raise ValueError("Belegordner vor der Einreichung prüfen.")
                row["kategorie"] = category
            _submission_ready(row)
            now = _now()
            dedup_key = _dedup_key(row["sha256"], row)
            if db.execute("SELECT id FROM buero_belege WHERE dedup_key=? AND id<>?", (dedup_key, key)).fetchone():
                raise ValueError("Dieser Beleg oder Zahlungsauftrag besteht bereits. Bestehenden Eintrag prüfen.")
            changed = db.execute("UPDATE buero_belege SET typ=?,kategorie=?,dedup_key=?,status='eingereicht',payable=0,business_confirmed=0,revision=revision+1,submitted_at=?,updated_at=? WHERE id=? AND revision=? AND status=?",
                                 (row["typ"], row["kategorie"], dedup_key, now, now, key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Bitte neu laden.")
            self._audit(db, who, key, "eingereicht", row["status"])
        self._backup()

    def decide(self, key, version, *, approve, note="", payment_release=False, business_confirmed=False, bereich="unklar", source_confirmed=False,
               kind=None, category=None, transfer_fields=None):
        note = _text(note, 1000, "Chefnotiz", multiline=True)
        if not approve and not note:
            raise ValueError("Für die Rückgabe bitte den Korrekturgrund angeben.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=True, db=db, lock=True)
            row = self._current(db, key, version, {"eingereicht", "entwurf", "zurueckgewiesen", "freigegeben"} if approve else {"eingereicht"})
            # A reviewed private letter may subsequently become a source-backed
            # single payment task. This is not a general reopening of approvals:
            # inspect the stored classification before applying any form fields.
            private_post_payment = (
                approve is True and row["status"] == "freigegeben" and row["typ"] == "post"
                and row["bereich"] == "privat" and not row["payable"]
                and kind == "zahlungsauftrag" and category == "privat" and bereich == "privat"
                and isinstance(transfer_fields, dict) and transfer_fields.get("zahlungsmodus") == "einzelzahlung"
            )
            if row["status"] == "freigegeben" and not private_post_payment:
                raise RevisionConflict("Der Beleg wurde bereits freigegeben. Nur geprüfte private Post kann ausdrücklich als private Einzelzahlung freigegeben werden.")
            direct = approve and row["status"] in ("entwurf", "zurueckgewiesen")
            if direct and (not isinstance(kind, str) or kind not in KINDS or not isinstance(category, str) or category not in CATEGORIES):
                raise ValueError("Belegart und Ordner vor der direkten Chef-Freigabe ausdrücklich prüfen.")
            old_kind, old_category = row["typ"], row["kategorie"]
            if kind is not None:
                if not approve or not isinstance(kind, str) or kind not in KINDS:
                    raise ValueError("Gültige Belegart für die Chef-Freigabe auswählen.")
                row["typ"] = kind
            if category is not None:
                if not approve or not isinstance(category, str) or category and category not in CATEGORIES:
                    raise ValueError("Gültigen Belegordner für die Chef-Freigabe auswählen.")
                row["kategorie"] = category
            transfer_changed = []
            if approve and row["typ"] != "zahlungsauftrag" and row["zahlungsmodus"]:
                # An explicitly chosen non-order classification retires only
                # the rental mode; retain source bank metadata for the audit.
                row["zahlungsmodus"] = ""
                transfer_changed.append("zahlungsmodus")
            if transfer_fields is not None:
                if not approve or not isinstance(transfer_fields, dict) or set(transfer_fields) - set(TRANSFER_FIELDS):
                    raise ValueError("Nur vorgesehene Überweisungsdaten bei der Chefprüfung übernehmen.")
                combined = {name: row[name] for name in FIELDS if name != "betrag_brutto"}
                combined["betrag_brutto"] = "" if row["betrag_cent"] is None else f"{row['betrag_cent'] // 100}.{row['betrag_cent'] % 100:02d}"
                combined.update(transfer_fields)
                checked = _fields(combined)
                for name in TRANSFER_FIELDS:
                    if row[name] != checked[name]:
                        transfer_changed.append(name)
                    row[name] = checked[name]
            classification_changed = (row["typ"], row["kategorie"]) != (old_kind, old_category)
            if approve:
                if direct or classification_changed:
                    _submission_ready(row)
                _review_original(db, row)
            if approve and bereich not in ("geschaeftlich", "privat"):
                raise ValueError("Den Beleg vor der Freigabe ausdrücklich als geschäftlich oder privat zuordnen.")
            if payment_release and (not approve or row["typ"] not in ("bescheid", "zahlungsauftrag")):
                raise ValueError("Eine besondere Zahlungsfreigabe ist nur für Bescheide oder Zahlungsaufträge vorgesehen.")
            if approve and row["typ"] == "zahlungsauftrag":
                _payment_order_ready(row)
                if row["kategorie"] == "privat" and bereich != "privat":
                    raise ValueError("Privaten Zahlungsauftrag ausdrücklich dem privaten Bereich zuordnen.")
                if not payment_release or source_confirmed is not True:
                    raise ValueError("Original-Zahlungsgrundlage, Zahlbetrag und Zahlungsabsicht ausdrücklich prüfen und die Zahlung freigeben. Fehlende Überweisungsdaten bleiben offen.")
            payable = approve and (row["typ"] == "rechnung" or (row["typ"] in ("bescheid", "zahlungsauftrag") and payment_release))
            if payable and bereich == "geschaeftlich" and business_confirmed is not True:
                raise ValueError("Die betriebliche Zuordnung vor der Zahlungsfreigabe ausdrücklich bestätigen.")
            if payable and row["betrag_cent"] is None:
                raise ValueError("Für eine Zahlungsfreigabe ist ein geprüfter Bruttobetrag erforderlich. Beleg zur Ergänzung zurückgeben.")
            if payable and row["typ"] in ("rechnung", "bescheid") and (not row["lieferant"] or not row["belegdatum"] or (row["typ"] == "rechnung" and not row["belegnummer"])):
                raise ValueError("Lieferant, Belegdatum und gegebenenfalls Rechnungsnummer vor der Zahlungsfreigabe ergänzen.")
            chosen_area = bereich if approve else row["bereich"]
            chosen_category = row["kategorie"] if chosen_area == "privat" else ("sonstige" if row["kategorie"] == "privat" else row["kategorie"])
            row["kategorie"] = chosen_category
            dedup_key = _dedup_key(row["sha256"], row)
            if approve and db.execute("SELECT id FROM buero_belege WHERE dedup_key=? AND id<>?", (dedup_key, key)).fetchone():
                raise ValueError("Dieser Beleg oder Zahlungsauftrag besteht bereits. Bestehenden Eintrag prüfen.")
            state, now = ("freigegeben" if approve else "zurueckgewiesen"), _now()
            changed = db.execute("""UPDATE buero_belege SET typ=?,kategorie=?,dedup_key=?,zahlungsempfaenger=?,iban=?,bic=?,verwendungszweck=?,faelligkeit=?,zahlungsmodus=?,status=?,payable=?,business_confirmed=?,quelle_geprueft=?,bereich=?,decided_at=?,decided_by=?,decision_note=?,
                revision=revision+1,updated_at=? WHERE id=? AND revision=? AND status=?""",
                (row["typ"], chosen_category, dedup_key, *(row[name] for name in TRANSFER_FIELDS), state, int(payable), int(payable and business_confirmed and chosen_area == "geschaeftlich"),
                 int(payable and row["typ"] == "zahlungsauftrag" and source_confirmed), chosen_area, now, who["actor"], note, now, key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig entschieden. Bitte neu laden.")
            self._audit(db, who, key, "freigegeben" if approve else "zurueckgewiesen", row["status"],
                        extra={"direkte_chefpruefung": direct, "klassifizierung_geaendert": classification_changed,
                               "private_post_zahlungsfreigabe": private_post_payment,
                               "transferdaten_geaendert": transfer_changed})
        self._backup()

    def payment(self, key, version, *, paid=False, paid_date="", reference=""):
        if paid:
            paid_date = _day(paid_date, "Zahlungsdatum", required=True)
            if date.fromisoformat(paid_date) > datetime.now(ZoneInfo("Europe/Berlin")).date():
                raise ValueError("Tatsächliches Zahlungsdatum darf nicht in der Zukunft liegen.")
            reference = _text(reference, 200, "Zahlungsreferenz")
            if not reference:
                raise ValueError("Referenz der außerhalb des Portals ausgeführten Zahlung angeben.")
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(db=db, lock=True)
            row = self._current(db, key, version, {"freigegeben", "zahlung_in_bearbeitung"} if paid else {"freigegeben"})
            self._visibility(row, who)
            if (not row["payable"] or not (row["business_confirmed"] or row["bereich"] == "privat") or row["typ"] not in ("rechnung", "bescheid", "zahlungsauftrag")
                    or (row["typ"] == "zahlungsauftrag" and not row["quelle_geprueft"])):
                raise ValueError("Dieser Beleg hat keine Chef-Zahlungsfreigabe. Lieferscheine und sonstige Post sind nicht zahlbar.")
            if row["typ"] == "zahlungsauftrag":
                _payment_order_ready(row)
            state = "bezahlt" if paid else "zahlung_in_bearbeitung"
            changed = db.execute("""UPDATE buero_belege SET status=?,paid_date=?,payment_reference=?,revision=revision+1,updated_at=?
                WHERE id=? AND revision=? AND status=? AND payable=1 AND (business_confirmed=1 OR bereich='privat')
                AND (typ<>'zahlungsauftrag' OR quelle_geprueft=1)""",
                (state, paid_date if paid else "", reference if paid else "", _now(), key, row["revision"], row["status"]))
            if changed.rowcount != 1:
                raise RevisionConflict("Beleg wurde gleichzeitig geändert. Bitte neu laden.")
            self._audit(db, who, key, "zahlung_dokumentiert" if paid else "ueberweisung_extern_in_bearbeitung", row["status"])
        self._backup()

    def original(self, key, *, admin=False):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self.identity(admin=admin, db=db, lock=True)
            row = db.execute("SELECT filename,mime,size_bytes,sha256,original_base64,revision,created_by,bereich,status,decided_by FROM buero_belege WHERE id=?", (_key(key),)).fetchone()
            if not row:
                raise LookupError("Beleg nicht gefunden.")
            self._visibility(row, who, admin=admin)
            try:
                raw = base64.b64decode(row["original_base64"], validate=True)
            except (ValueError, TypeError):
                raise LookupError("Das gespeicherte Original kann nicht unverändert bereitgestellt werden.") from None
            if len(raw) != row["size_bytes"] or not 0 < len(raw) <= MAX_ORIGINAL_BYTES or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                raise LookupError("Das gespeicherte Original kann nicht unverändert bereitgestellt werden.")
            self._seen(db, who, key, row["revision"])
        return dict(row), raw


def register_office_documents(p):
    service = OfficeDocuments(p)
    p.office_documents = service
    p.office_documents_init_schema = service.init_schema
    bp = Blueprint("buero_belege", __name__)

    def admin_route():
        return request.path.startswith("/admin/")

    @bp.before_request
    def protected():
        try:
            service.identity(admin=admin_route())
        except PermissionError:
            abort(403)
        if request.content_length and request.content_length > MAX_REQUEST_BYTES:
            abort(413)
        try:
            request.max_content_length = MAX_REQUEST_BYTES
        except AttributeError:  # The shared application limit protects older Flask.
            pass

    @bp.after_request
    def private(response):
        response.headers.update({"Cache-Control": "private, no-store", "Pragma": "no-cache",
                                 "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
                                 "X-Robots-Tag": "noindex, nofollow, noarchive"})
        response.vary.add("Cookie")
        return response

    def csrf_and_fields(allowed, *, upload=False):
        supplied, expected = request.form.getlist("csrf_token"), session.get("csrf_token")
        if not expected or not supplied or any(not hmac.compare_digest(str(expected), str(v)) for v in supplied):
            abort(400)
        if set(request.form) - set(allowed) - {"csrf_token"} or any(len(request.form.getlist(k)) != 1 for k in request.form if k != "csrf_token"):
            abort(400)
        if upload:
            if set(request.files) != {"original"} or len(request.files.getlist("original")) != 1:
                abort(400)
        elif request.files:
            abort(400)

    def page(*, key=None, error="", status_code=200):
        admin = admin_route()
        try:
            who = service.identity(admin=admin)
            status = request.args.get("status", "alle")
            category = request.args.get("kategorie", "alle")
            group = request.args.get("bereich", "alle")
            if (status != "alle" and status not in STATES) or (category != "alle" and category not in CATEGORIES) or group not in ("alle", "ungeprueft", "freigegeben"):
                raise ValueError("Gültigen Ordner und Arbeitsbereich auswählen.")
            selected = service.detail(key, admin=admin) if key else None
            if selected and error and status_code == 400 and request.method == "POST":
                # Retain the user's unsaved transfer entries for correction;
                # the protected record/revision remains unchanged on failure.
                for name in TRANSFER_FIELDS:
                    if name in request.form:
                        selected[name] = request.form[name][:300]
                if request.form.get("typ") in KINDS:
                    selected["typ"] = request.form["typ"]
                    selected["typ_label"] = KINDS[selected["typ"]]
                    selected["typ_vorschlag"] = ""
                if request.form.get("kategorie") in CATEGORIES:
                    selected["kategorie"] = request.form["kategorie"]
                if request.form.get("bereich") in AREAS:
                    selected["review_bereich"] = request.form["bereich"]
                missing = _transfer_missing(selected)
                selected["transfer_missing"] = missing
                selected["transfer_warning"] = "Nicht angegeben: " + ", ".join(missing) + "." if missing else ""
                selected["payment_data_pending"] = any(label != "BIC" for label in missing)
            listing = service.list(admin=admin, status=status, category=category, group=group)
            counts = service.counters(admin=admin)
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except ValueError:
            abort(400)
        if not isinstance(session.get("csrf_token"), str) or not session["csrf_token"]:
            session["csrf_token"] = secrets.token_urlsafe(32)
        return render_template("buero_belege.html", **listing, selected=selected, error=error,
                               csrf_token=session["csrf_token"], kinds=KINDS, states=STATES, categories=CATEGORIES, counts=counts,
                               areas=AREAS,
                               filter_status=status, filter_category=category, filter_group=group,
                               admin_mode=admin, is_admin=admin, office_identity=who, buero_identity=who,
                               buero_active="belege", active_nav="belege",
                               base_url="/admin/buero/belege" if admin else "/werkstatt/buero/belege"), status_code

    def mutate(action, key=None):
        try:
            result = action()
            if isinstance(result, tuple):
                key, duplicate = result
                saved = ("Beleg geschützt gespeichert. Jetzt Original und Angaben prüfen und hier freigeben." if admin_route() else
                         "Beleg geschützt gespeichert. Jetzt Angaben prüfen und zur Cheffreigabe einreichen.")
                flash("Dieses Original ist bereits vorhanden. Der bestehende Beleg wurde geöffnet; seine Angaben bleiben unverändert." if duplicate else saved,
                      "info" if duplicate else "success")
            else:
                flash("Belegstand gespeichert.", "success")
            base = "/admin/buero/belege" if admin_route() else "/werkstatt/buero/belege"
            return redirect(base + ("/" + key if key else ""), code=303)
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except RevisionConflict as exc:
            return page(key=key, error=str(exc), status_code=409)
        except ValueError as exc:
            return page(key=key, error=str(exc), status_code=400)
        except Exception as exc:
            duplicate = (isinstance(exc, sqlite3.IntegrityError) and "buero_belege.dedup_key" in str(exc)) or (
                getattr(exc, "sqlstate", None) == "23505" and getattr(getattr(exc, "diag", None), "constraint_name", "") == "idx_buero_belege_dedup")
            if duplicate:
                return page(key=key, error="Dieser Beleg oder Zahlungsauftrag ist bereits vorhanden. Bestehenden Eintrag prüfen.", status_code=400)
            raise

    def run_hook(name, key):
        hook = getattr(p, name, None)
        if not callable(hook):
            return
        message = ("Scan-Analyse derzeit nicht abgeschlossen. Beleg ist gespeichert; Vorschlag später erneut prüfen."
                   if name == "office_document_uploaded" else
                   "Lexware-Übertragung derzeit nicht abgeschlossen. Chef-Freigabe bleibt bestehen; Übertragungsstand erneut prüfen.")
        try:
            result = hook(key)
            if not isinstance(result, dict) or result.get("ok") is not False:
                return
        except Exception:
            pass
        # Do not expose provider exceptions, credentials or private mail text.
        try:
            service.record_processing_notice(key, message)
        except Exception:
            pass
        flash(message, "warning")

    @bp.get("/werkstatt/buero/belege")
    def index():
        return page()

    @bp.get("/admin/buero/belege")
    def admin_index():
        return page()

    @bp.get("/werkstatt/buero/belege/<key>")
    def detail(key):
        return page(key=key)

    @bp.get("/admin/buero/belege/<key>")
    def admin_detail(key):
        return page(key=key)

    @bp.post("/werkstatt/buero/belege/erfassen")
    @bp.post("/admin/buero/belege/erfassen")
    def upload():
        csrf_and_fields(FIELDS, upload=True)
        def create_and_analyse():
            payload = {k: request.form.get(k, "") for k in FIELDS}
            payload["typ"] = payload["typ"] or "post"
            result = service.create(payload, request.files["original"], admin=admin_route())
            if not result[1] and request.form.get("typ") != "zahlungsauftrag":
                run_hook("office_document_uploaded", result[0])
            return result
        return mutate(create_and_analyse)

    @bp.post("/werkstatt/buero/belege/<key>/bearbeiten")
    @bp.post("/admin/buero/belege/<key>/bearbeiten")
    def update(key):
        csrf_and_fields(FIELDS | {"revision"})
        return mutate(lambda: service.update(key, request.form.get("revision"), {k: request.form.get(k, "") for k in FIELDS if k in request.form}, admin=admin_route()), key)

    @bp.post("/werkstatt/buero/belege/<key>/einreichen")
    @bp.post("/admin/buero/belege/<key>/einreichen")
    def submit(key):
        csrf_and_fields({"revision", "confirmed", "typ", "kategorie"})
        if request.form.get("confirmed") != "ja":
            abort(400)
        return mutate(lambda: service.submit(key, request.form.get("revision"), admin=admin_route(),
                                             kind=request.form.get("typ"), category=request.form.get("kategorie")), key)

    def download(key):
        try:
            row, raw = service.original(key, admin=admin_route())
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        response = send_file(io.BytesIO(raw), mimetype=row["mime"], as_attachment=True,
                             download_name=row["filename"], max_age=0, conditional=False, etag=False)
        response.headers["Content-Security-Policy"] = "sandbox; default-src 'none'"
        return response

    @bp.get("/werkstatt/buero/belege/<key>/original")
    def original(key):
        return download(key)

    @bp.get("/admin/buero/belege/<key>/original")
    def admin_original(key):
        return download(key)

    @bp.post("/admin/buero/belege/<key>/freigeben")
    def approve(key):
        csrf_and_fields({"revision", "chef_notiz", "zahlungspflichtig", "confirmed", "bereich", "typ", "kategorie"} | set(TRANSFER_FIELDS))
        if request.form.get("confirmed") != "ja" or request.form.get("zahlungspflichtig", "") not in ("", "ja"):
            abort(400)
        def approve_and_transfer():
            service.decide(key, request.form.get("revision"), approve=True,
                           note=request.form.get("chef_notiz", ""), payment_release=request.form.get("zahlungspflichtig") == "ja",
                           business_confirmed=request.form.get("confirmed") == "ja", bereich=request.form.get("bereich", "unklar"),
                           source_confirmed=request.form.get("zahlungspflichtig") == "ja" and request.form.get("confirmed") == "ja",
                           kind=request.form.get("typ"), category=request.form.get("kategorie"),
                           transfer_fields={name: request.form[name] for name in TRANSFER_FIELDS if name in request.form})
            approved = service.detail(key, admin=True)
            if (approved["bereich"] == "geschaeftlich" and approved["business_confirmed"] and approved["payable"]
                    and approved["typ"] in ("rechnung", "bescheid")):
                run_hook("office_document_approved", key)
        return mutate(approve_and_transfer, key)

    @bp.post("/admin/buero/belege/<key>/zurueckweisen")
    def reject(key):
        csrf_and_fields({"revision", "chef_notiz", "confirmed"})
        if request.form.get("confirmed") != "ja":
            abort(400)
        return mutate(lambda: service.decide(key, request.form.get("revision"), approve=False, note=request.form.get("chef_notiz", "")), key)

    @bp.post("/werkstatt/buero/belege/<key>/ueberweisung")
    def start_payment(key):
        csrf_and_fields({"revision", "confirmed"})
        if request.form.get("confirmed") != "ja":
            abort(400)
        return mutate(lambda: service.payment(key, request.form.get("revision")), key)

    @bp.post("/werkstatt/buero/belege/<key>/bezahlt")
    def paid(key):
        csrf_and_fields({"revision", "confirmed", "zahlungsdatum", "zahlungsreferenz"})
        if request.form.get("confirmed") != "ja":
            abort(400)
        return mutate(lambda: service.payment(key, request.form.get("revision"), paid=True,
                                             paid_date=request.form.get("zahlungsdatum", ""), reference=request.form.get("zahlungsreferenz", "")), key)

    p.app.register_blueprint(bp)
    return service
