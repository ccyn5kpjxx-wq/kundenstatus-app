"""Bounded CEO-mail reconciliation. No scheduler, bank action or Lexware write.

All transports are injected. A portal index must explicitly certify complete
coverage, including older scans, before this pipeline creates a review copy.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from email import policy
from email.parser import BytesParser
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import unicodedata
from urllib.parse import urlencode
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from flask import has_request_context, session
from werkzeug.datastructures import FileStorage

BACKUP_TABLES = ("buero_posteingang_receipts", "buero_posteingang_audit")
SCHEMA_FEATURE = "office_inbox_reconciliation_v1"
MAX_ATTACHMENT = 10 * 1024 * 1024
MAX_MESSAGE = 15 * 1024 * 1024
MAX_BATCH = 40 * 1024 * 1024
_HASH = re.compile(r"[a-f0-9]{64}")


class InboxBlocked(RuntimeError):
    pass


def confirmed_ceo_mailbox_user(portal, ceo_address):
    """Resolve only an explicitly configured mailbox, otherwise the CEO address."""
    if not isinstance(ceo_address, str) or not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", ceo_address.strip()):
        raise InboxBlocked("Eine ausdrücklich bestätigte Chef-Mailadresse fehlt. Keine Mailbox wurde gelesen.")
    app = getattr(portal, "app", None)
    config = app.config if app is not None else {}
    value = portal.get_app_setting("OFFICE_CEO_MAILBOX_USER", "")
    if value in (None, ""):
        value = config.get("OFFICE_CEO_MAILBOX_USER", "")
    if value in (None, ""):
        value = ceo_address
    if not isinstance(value, str) or not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", value.strip()):
        raise InboxBlocked("Die ausdrücklich bestätigte Chef-Postfachzuordnung ist ungültig. Keine Mailbox wurde gelesen.")
    return value.strip()


def document_identity(meta):
    """Only independently evidenced identity, never amount/subject alone."""
    if not isinstance(meta, dict) or meta.get("identity_verified") is not True:
        return None
    supplier = "".join(c for c in unicodedata.normalize("NFKC", str(meta.get("lieferant", ""))).casefold() if c.isalnum())
    number = "".join(str(meta.get("belegnummer", "")).casefold().split())
    cents, day = meta.get("betrag_cent"), meta.get("belegdatum")
    if not supplier or not number or type(cents) is not int or cents <= 0 or not isinstance(day, str):
        return None
    try:
        if datetime.strptime(day, "%Y-%m-%d").date().isoformat() != day:
            return None
    except ValueError:
        return None
    return supplier, number, cents, day


def matching_records(digest, meta, records):
    identity = document_identity(meta)
    found = {}
    for record in records:
        if not isinstance(record, dict) or not record.get("id"):
            continue
        if record.get("sha256") == digest or identity is not None and document_identity(record) == identity:
            found[str(record["id"])] = dict(record)
    return list(found.values())


class ReadonlyIMAP:
    """An explicitly injected connection factory; never creates a live default."""
    def __init__(self, factory, config):
        self.factory, self.config = factory, config

    def messages(self, since, until, limit):
        def imap_day(day):
            months = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
            return f"{day.day:02d}-{months[day.month-1]}-{day.year}"
        with self.factory(self.config) as connection:
            status, _ = connection.select("INBOX", readonly=True)
            if status != "OK":
                raise InboxBlocked("Chef-Posteingang kann nicht schreibgeschützt geöffnet werden.")
            _, values = connection.response("UIDVALIDITY")
            try:
                token = values[0].decode("ascii") if values and isinstance(values[0], bytes) else str(values[0]) if values else ""
                validity = str(int(token)) if re.fullmatch(r"[0-9]{1,30}", token) else ""
            except (UnicodeDecodeError, ValueError):
                validity = ""
            if not validity:
                raise InboxBlocked("Stabile Mailboxkennung fehlt.")
            status, result = connection.uid("search", None, "SINCE", imap_day(since), "BEFORE", imap_day(until))
            if status != "OK":
                raise InboxBlocked("Chef-Posteingang konnte nicht gelesen werden.")
            # No UNSEEN filter: previously read messages/scans are reconciled too.
            uids = result[0].split() if result and result[0] else []
            if len(uids) > limit:
                raise InboxBlocked("Mehr Nachrichten als das sichere Lauflimit. Zeitraum verkleinern.")
            for uid in uids:
                if not uid.isdigit():
                    raise InboxBlocked("Ungültige Mailkennung.")
                status, info = connection.uid("fetch", uid, "(RFC822.SIZE)")
                sizes = [int(x) for value in info or [] if isinstance(value, bytes) for x in re.findall(rb"RFC822\.SIZE\s+(\d+)", value)]
                if status != "OK" or not sizes or max(sizes) > MAX_MESSAGE:
                    raise InboxBlocked("Mailgröße fehlt oder überschreitet das Lauflimit.")
                status, data = connection.uid("fetch", uid, "(BODY.PEEK[])")
                raw = next((part[1] for part in data or [] if isinstance(part, tuple) and len(part) == 2 and isinstance(part[1], bytes)), None)
                if status != "OK" or raw is None or len(raw) > MAX_MESSAGE:
                    raise InboxBlocked("Mail konnte nicht begrenzt gelesen werden.")
                yield {"uid": uid.decode("ascii"), "uidvalidity": validity, "raw": raw}


def attachments(raw):
    if not isinstance(raw, bytes) or len(raw) > MAX_MESSAGE:
        raise InboxBlocked("Mail überschreitet die Größenbegrenzung.")
    message = BytesParser(policy=policy.default).parsebytes(raw)
    for index, part in enumerate(message.walk()):
        if index > 100:
            raise InboxBlocked("Zu viele MIME-Teile in einer Mail.")
        name = part.get_filename()
        if not name or part.is_multipart() or part.get_content_disposition() == "inline":
            continue
        name = str(name).replace("\\", "/").split("/")[-1]
        name = "".join(c for c in name if ord(c) >= 32)[:150]
        suffix = Path(name).suffix.lower()
        expected = {".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}.get(suffix)
        if not expected or part.get_content_type() not in {expected, "application/octet-stream"}:
            continue
        payload = part.get_payload(decode=True)
        if not payload or len(payload) > MAX_ATTACHMENT:
            raise InboxBlocked("Anhang ist leer oder zu groß.")
        magic = payload.startswith(b"%PDF-") if suffix == ".pdf" else payload.startswith(b"\x89PNG\r\n\x1a\n") if suffix == ".png" else payload.startswith(b"\xff\xd8\xff")
        if not magic:
            raise InboxBlocked("Dateityp des Anhangs stimmt nicht mit dem Inhalt überein.")
        yield name, expected, payload


class LexwarePurchaseIndex:
    """Read-only official-API adapter; binary download callback is optional.

    request follows portal.lexware_request(method,path,query=...). download
    accepts a validated file UUID and max_bytes, returning bounded bytes.
    """
    def __init__(self, request, download=None, max_pages=10, max_files=40):
        self.request, self.download = request, download
        self.max_pages, self.max_files = max_pages, max_files

    def find_matches(self, digest, meta):
        records, complete, downloads = [], True, 0
        for archived in ("false", "true"):
            for page in range(self.max_pages):
                query = "?" + urlencode({"voucherType": "purchaseinvoice", "voucherStatus": "any", "archived": archived, "page": page, "size": 100})
                result = self.request("GET", "/v1/voucherlist", query=query)
                if not isinstance(result, dict) or not isinstance(result.get("content"), list):
                    raise InboxBlocked("Lexware-Bestandsantwort ist unvollständig.")
                for raw in result["content"]:
                    if raw.get("voucherType") != "purchaseinvoice":
                        continue
                    try:
                        key = str(UUID(str(raw["id"])))
                        amount = Decimal(str(raw.get("totalAmount"))) * 100
                        cents = int(amount) if amount == amount.to_integral_value() else None
                    except (KeyError, ValueError, TypeError, InvalidOperation):
                        complete = False
                        continue
                    record = {"id": key, "lieferant": raw.get("contactName", ""), "belegnummer": raw.get("voucherNumber", ""),
                              "betrag_cent": cents, "belegdatum": str(raw.get("voucherDate", ""))[:10], "identity_verified": True}
                    records.append(record)
                if result.get("last") is True:
                    break
            else:
                complete = False
        known = matching_records(digest, meta, records)
        if known:
            # Metadata targets are verified against the actual bookkeeping voucher.
            checked = []
            for record in known:
                detail = self.request("GET", "/v1/vouchers/" + record["id"])
                try:
                    gross = Decimal(str(detail.get("totalGrossAmount"))) * 100
                    detail_identity = dict(record, belegnummer=detail.get("voucherNumber", ""),
                                           belegdatum=str(detail.get("voucherDate", ""))[:10],
                                           betrag_cent=int(gross) if gross == gross.to_integral_value() else None)
                except (InvalidOperation, TypeError, ValueError):
                    detail_identity = {}
                if detail.get("type") != "purchaseinvoice" or document_identity(detail_identity) != document_identity(record):
                    complete = False
                    continue
                checked.append(record)
            return {"matches": checked, "complete": complete}
        # Different scans of the same document need full evidenced identity.
        # Where identity is unavailable, only an exhaustive file-hash comparison
        # can certify the binary check. A bounded/partial check must block intake.
        for record in records:
            detail = self.request("GET", "/v1/vouchers/" + record["id"])
            if detail.get("type") != "purchaseinvoice":
                complete = False
                continue
            for file_id in detail.get("files", []):
                if downloads >= self.max_files or self.download is None:
                    complete = False
                    break
                try:
                    file_id = str(UUID(str(file_id)))
                except ValueError:
                    complete = False
                    continue
                blob = self.download(file_id, max_bytes=MAX_ATTACHMENT)
                downloads += 1
                if not isinstance(blob, bytes) or len(blob) > MAX_ATTACHMENT:
                    complete = False
                    continue
                if hashlib.sha256(blob).hexdigest() == digest:
                    record["sha256"] = digest
        return {"matches": matching_records(digest, meta, records), "complete": complete}


class PortalInventory:
    """Bridge to office documents plus an explicit older-scan inventory.

    legacy_find(digest, meta) returns {matches:[...], complete:bool}. Missing
    legacy coverage is intentionally incomplete; no new review is created.
    """
    def __init__(self, portal, legacy_find=None):
        self.p, self.legacy_find = portal, legacy_find

    def find_matches(self, digest, meta):
        result = self.p.office_documents.find_by_sha256(digest)
        rows = [result] if result else []
        identity = document_identity(meta)
        if identity:
            with self.p.office_documents.db() as db:
                candidates = db.execute("""SELECT id,sha256,lieferant,belegnummer,belegdatum,betrag_cent,
                    bereich,business_confirmed FROM buero_belege WHERE betrag_cent=? AND belegdatum=?""",
                                        (identity[2], identity[3])).fetchall()
            rows.extend(dict(row, identity_verified=bool(row["business_confirmed"])) for row in candidates)
        legacy = self.legacy_find(digest, meta) if callable(self.legacy_find) else {"matches": [], "complete": False}
        matches = matching_records(digest, meta, rows)
        matches.extend(legacy.get("matches", []))
        unique = {str(row["id"]): row for row in matches}
        return {"matches": list(unique.values()), "complete": legacy.get("complete") is True}

    def create_review(self, meta, upload, *, source_ref, bereich):
        identity = document_identity(meta)
        payload = {"typ": "post", "kategorie": "privat" if bereich == "privat" else "sonstige",
                   "notiz": "Aus bestätigtem Chef-Postfach übernommen. Zuordnung und Belegart durch die Werkstattleitung prüfen."}
        if identity:
            payload.update(lieferant=meta["lieferant"], belegnummer=meta["belegnummer"], belegdatum=meta["belegdatum"],
                           betrag_brutto=format(Decimal(meta["betrag_cent"])/100, ".2f"))
        return self.p.office_documents.create_from_intake(payload, upload, source_ref=source_ref, bereich=bereich)


class OfficeInbox:
    def __init__(self, portal, authorize=None):
        self.p = portal
        self.authorize = authorize or (lambda: has_request_context() and bool(session.get("admin")))

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
            db.execute("""CREATE TABLE IF NOT EXISTS buero_posteingang_receipts (
                source_key TEXT PRIMARY KEY, account_hash TEXT NOT NULL, uidvalidity TEXT NOT NULL,
                mail_uid TEXT NOT NULL, sha256 TEXT NOT NULL, status TEXT NOT NULL,
                portal_key TEXT NOT NULL DEFAULT '',lexware_id TEXT NOT NULL DEFAULT '',
                details_json TEXT NOT NULL,updated_at TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS buero_posteingang_audit (
                id TEXT PRIMARY KEY,source_key TEXT NOT NULL,details_json TEXT NOT NULL,created_at TEXT NOT NULL)""")

    def _account(self, address):
        if not self.authorize():
            raise PermissionError("Chefauftrag für den Posteingangsabgleich erforderlich.")
        if not isinstance(address, str) or not re.fullmatch(r"[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+", address):
            raise InboxBlocked("Eine ausdrücklich bestätigte Chef-Mailadresse fehlt.")
        app = getattr(self.p, "app", None)
        configured = app.config.get("OFFICE_CEO_MAIL_ADDRESS", "") if app is not None else ""
        setting = self.p.get_app_setting("OFFICE_CEO_MAIL_ADDRESS", "") or configured
        config = self.p.get_werkstatt_imap_config()
        mailbox_user = confirmed_ceo_mailbox_user(self.p, address)
        if (not setting or str(setting).strip().casefold() != address.casefold()
                or not isinstance(config, dict) or str(config.get("user", "")).strip().casefold() != mailbox_user.casefold()):
            raise InboxBlocked("Chef-Mailadresse und bestätigte Postfachzuordnung passen nicht zum konfigurierten IMAP-Konto. Keine Mailbox wurde gelesen.")
        return config

    def _record(self, source_key, account_hash, message, digest, result):
        timestamp = datetime.now(ZoneInfo("Europe/Berlin")).isoformat(timespec="seconds")
        encoded = json.dumps(result, sort_keys=True, ensure_ascii=False)
        with self.db() as db:
            old = db.execute("SELECT details_json FROM buero_posteingang_receipts WHERE source_key=?", (source_key,)).fetchone()
            if old and old["details_json"] == encoded:
                return
            db.execute("""INSERT INTO buero_posteingang_receipts
                (source_key,account_hash,uidvalidity,mail_uid,sha256,status,portal_key,lexware_id,details_json,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET
                status=excluded.status,portal_key=excluded.portal_key,lexware_id=excluded.lexware_id,
                details_json=excluded.details_json,updated_at=excluded.updated_at RETURNING source_key""",
                       (source_key, account_hash, message["uidvalidity"], message["uid"], digest, result["status"],
                        result.get("portal_key", ""), result.get("lexware_id", ""), encoded, timestamp))
            db.execute("INSERT INTO buero_posteingang_audit(id,source_key,details_json,created_at) VALUES(?,?,?,?)",
                       (uuid4().hex, source_key, encoded, timestamp))

    def run(self, account_address, *, mailbox=None, portal=None, lexware=None, metadata=None,
            lookback_days=14, max_messages=80, max_attachments=40, now=None):
        account = self._account(account_address)
        if not (1 <= lookback_days <= 90 and 1 <= max_messages <= 200 and 1 <= max_attachments <= 100):
            raise ValueError("Sichere Lauflimits einhalten.")
        mailbox = mailbox or getattr(self.p, "office_inbox_mail_transport", None)
        portal = portal or getattr(self.p, "office_inbox_portal_index", None)
        lexware = lexware or getattr(self.p, "office_inbox_lexware_index", None)
        if mailbox is None or portal is None or lexware is None:
            raise InboxBlocked("Mail-, Portal- und Lexware-Abgleich müssen ausdrücklich mit vollständigen Quellen eingerichtet sein.")
        today = (now or datetime.now(ZoneInfo("Europe/Berlin"))).date()
        account_identity = account_address.casefold()
        mailbox_user = str(account["user"]).strip().casefold()
        if mailbox_user != account_identity:
            # Keep existing strict-account receipts stable, but distinguish an
            # explicitly shared physical inbox from another mailbox's UIDs.
            account_identity += "\0" + mailbox_user
        account_hash = hashlib.sha256(account_identity.encode()).hexdigest()
        results, total_bytes, attachment_count = [], 0, 0
        for message_count, message in enumerate(mailbox.messages(today-timedelta(days=lookback_days), today+timedelta(days=1), max_messages), 1):
            if message_count > max_messages or not all(re.fullmatch(r"[0-9]{1,30}", str(message.get(k, ""))) for k in ("uid", "uidvalidity")):
                raise InboxBlocked("Mailanzahl oder stabile Mailkennung ungültig.")
            for filename, mime, raw in attachments(message["raw"]):
                attachment_count += 1
                total_bytes += len(raw)
                if attachment_count > max_attachments or total_bytes > MAX_BATCH:
                    raise InboxBlocked("Anhangslimit erreicht; vor einer Fortsetzung Zeitraum verkleinern.")
                digest = hashlib.sha256(raw).hexdigest()
                source_key = hashlib.sha256((account_hash+":"+str(message["uidvalidity"])+":"+str(message["uid"])+":"+digest).encode()).hexdigest()
                meta = metadata(filename, mime, raw) if callable(metadata) else {}
                meta = meta if isinstance(meta, dict) else {}
                private = meta.get("bereich_verified") is True and meta.get("bereich") == "privat"
                private_review_only = meta.get("private_review_only") is True
                with self.p.portal_originals_operation_lock():
                    if not self.authorize():
                        raise PermissionError("Chefauftrag wurde entzogen.")
                    pcheck = portal.find_matches(digest, meta)
                    pmatches = pcheck.get("matches", [])
                    result = {"status": "ZU_PRUEFEN", "sha256": digest, "filename": filename}
                    if len(pmatches) > 1:
                        result["status"] = "MEHRDEUTIGER_PORTALTREFFER"
                    else:
                        existing = pmatches[0] if pmatches else None
                        if existing:
                            result["portal_key"] = str(existing["id"])
                        private = private or bool(existing and existing.get("bereich") == "privat")
                        # A local private hint blocks company lookup without
                        # claiming a verified private classification.
                        lcheck = {"matches": [], "complete": True} if private or private_review_only else lexware.find_matches(digest, meta)
                        lmatches = lcheck.get("matches", [])
                        if len(lmatches) == 1:
                            result["lexware_id"] = str(lmatches[0]["id"])
                        if len(lmatches) > 1:
                            result["status"] = "MEHRDEUTIGER_LEXWARETREFFER"
                        elif private and existing:
                            result["status"] = "PRIVAT_BEREITS_IM_PORTAL"
                        elif existing:
                            result["status"] = "PORTAL_BESTEHT_WORKFLOW_FORTSETZEN" if lcheck.get("complete") is True else "PORTAL_BESTEHT_LEXWARE_ABGLEICH_UNVOLLSTAENDIG"
                            if lmatches:
                                result.update(status="PORTAL_UND_LEXWARE_BESTEHEN", lexware_id=str(lmatches[0]["id"]))
                        elif pcheck.get("complete") is not True or lcheck.get("complete") is not True:
                            result["status"] = "BESTANDSABGLEICH_UNVOLLSTAENDIG_KEINE_ANLAGE"
                        else:
                            upload = FileStorage(stream=io.BytesIO(raw), filename=filename, content_type=mime)
                            # A trusted portal intake stores only a review copy.
                            # No business confirmation, payment release or booking.
                            key, duplicate = portal.create_review(meta, upload, source_ref="ceo-mail:"+source_key,
                                                                 bereich="privat" if private else "unklar")
                            result.update(portal_key=str(key), status="PRIVAT_ZUR_PRUEFUNG" if private else "PORTAL_NEU_CHEFPRUEFUNG")
                            if lmatches:
                                result.update(status="LEXWARE_BESTEHT_NUR_ZUORDNUNG_PRUEFEN", lexware_id=str(lmatches[0]["id"]))
                            if duplicate:
                                result["concurrent_duplicate_reused"] = True
                    self._record(source_key, account_hash, message, digest, result)
                    results.append(result)
        return {"account_verified": True, "lookback_days": lookback_days, "attachment_count": attachment_count,
                "results": results, "lexware_writes": 0, "bank_actions": 0, "mail_flags_changed": False}


def register_office_inbox(p, *, authorize=None):
    service = OfficeInbox(p, authorize=authorize)
    p.office_inbox = service
    return service


def ensure_office_inbox_for_import(p, *, export=None, imported_db=None, target=None, **_):
    own, source = target is None, None
    db = target if target is not None else p.get_db()
    try:
        with p.portal_originals_operation_lock():
            if imported_db is not None:
                source = sqlite3.connect(Path(imported_db).resolve().as_uri()+"?mode=ro", uri=True)
                source.row_factory = sqlite3.Row
            for table in BACKUP_TABLES:
                current = [dict(row) for row in db.execute("SELECT * FROM "+table).fetchall()]
                if source:
                    exists = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    incoming = [dict(row) for row in source.execute("SELECT * FROM "+table).fetchall()] if exists else []
                else:
                    incoming = (export or {}).get("tables", {}).get(table, [])
                if not isinstance(incoming, list) or any(not isinstance(row, dict) for row in incoming):
                    raise ValueError()
                key = "source_key" if table == BACKUP_TABLES[0] else "id"
                actual = {row.get(key): row for row in incoming}
                if len(actual) != len(incoming) or actual != {row[key]: row for row in current}:
                    raise ValueError()
    except (ValueError, TypeError, KeyError, sqlite3.Error):
        raise ValueError("Import gestoppt: Posteingangs-Abgleichsnachweise würden verändert. Aktuelle vollständige Sicherung verwenden.") from None
    finally:
        if source:
            source.close()
        if own:
            db.close()
