"""Read invoice product proposals without exposing accounting or banking data.

``read_source(portal, source_kind, source_id)`` accepts a local integer row id
from ``einkauf_belege`` (kind ``einkauf``), or the voucher UUID of an existing
``lexware_rechnungen`` row (kind ``lexware``).
Only invoices are eligible; Lexware ids must already be known purchase invoices.
The injected portal supplies get_db(), existing invoice extraction helpers and,
for Lexware, get_requests() and its configured credentials.
Importing this module does not import/start the Flask app or perform any I/O.

Result: {status, source, candidates, coverage, warnings}. Status is ok, partial,
unavailable or error. Source contains kind/id/supplier/reference and the known
invoice date (never ingestion date); candidates
preserve produkt_name, artikelnummer, stueckzahl, ve, produkt_beschreibung, preis
and kategorie, plus page provenance. Every candidate and price is UNVERIFIED.
Quantity evidence is separate from explicit package contents. Old parser
default=1 is never treated as proof; missing or ambiguous quantities stay None.
Generic ``preis`` values have unknown basis. The recognized Topcolor table can
derive a net package price with explicit calculation/provenance, still marked
unverified. Neither kind is an agreed purchase price. Supplier comes exclusively
from the source row, never a default.

Coverage reports files_total/files_read, pages_total/pages_read/pages_attempted, complete and
per-file detail. Unknown page counts are None, not zero. ``complete`` describes
processing coverage, not extraction correctness; a text-only fallback is always
incomplete. PDFs are split into individual pages (30 pages / 10 files per source,
20 MiB per file). Original files and bookkeeping payloads never leave this
function, and temporary downloads/page files are removed on all exit paths.
No database writes, payment/bank endpoints, contact lookup or purchase requests.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import pathlib
import re
from datetime import date
import tempfile
import threading
import time
import uuid

from werkstatt_topcolor_positionen import explicit_package_evidence, material_unit, quantity_value
from werkstatt_rechnungsfreigabe import classify_invoice_source


MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_FILES = 10
MAX_PAGES = 30
MAX_PAGE_TEXT = 120000
MAX_POSITIONS_PER_PAGE = 150
LEXWARE_ORIGIN = "https://api.lexware.io"
_DENIED_STATES = {"deleted", "void", "voided", "cancelled", "canceled", "geloescht", "gelöscht", "storniert"}
_BANK_LINE = re.compile(r"\b(?:iban|bic|swift|bankverbindung|kontonummer|kontostand|bankkonto|zahlung(?:en|sbedingungen|shinweis)?|zahlbar|gesamtbetrag|zwischensumme|umsatzsteuer|mehrwertsteuer|mwst|ust)\b", re.I)
_IBAN = re.compile(r"\b[A-Z]{2}\s*\d{2}(?:[ \t]?[A-Z0-9]){11,30}\b", re.I)
_DOWNLOAD_LOCK = threading.Lock()
_LAST_DOWNLOAD = 0.0


class SourceUnavailable(Exception):
    """Only constructed with fixed, credential-free messages in this module."""


def _text(value, limit=500):
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        return ""
    lines = [line for line in str(value).splitlines() if not _BANK_LINE.search(line)]
    return _IBAN.sub("", " ".join(lines)).strip()[:limit]


def _warn(result, message):
    if message not in result["warnings"]:
        result["warnings"].append(message)


def invoice_date(value):
    """Known invoice dates only; never fall back to ingestion timestamps."""
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:T[^\s]{1,30})?", raw):
        raw = raw[:10]
    elif re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", raw):
        raw = raw[6:] + "-" + raw[3:5] + "-" + raw[:2]
    else:
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _date_from_text(text, result):
    if result["source"].get("date"):
        return
    dates = {invoice_date(match) for match in re.findall(
        r"\bRechnungsdatum\s*[:=]?\s*(\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})\b", text[:12000], re.I)}
    dates.discard(None)
    if len(dates) == 1:
        result["source"]["date"] = next(iter(dates))


def _generic_quantity(position):
    # Both old text and AI normalization use default=1. A supplied stueckzahl
    # therefore is NOT evidence. Only an explicit quantity label in the source
    # excerpt proves the distinction from the product's package size.
    result = {"value": None, "unit": None, "basis": "unknown",
              "source_field": None, "verified": False}
    texts = [_text(position.get(key), 1000) for key in ("quelle", "produkt_beschreibung")]
    matches = []
    pattern = r"\b(?:Rechnungsmenge|Bestellmenge|Menge|Anzahl)\s*[:=]?\s*(\d+(?:[.,]\d{1,6})?)\s*([A-Za-zÄÖÜäöüß.]+)\b"
    for text in texts:
        for match in re.finditer(pattern, text, re.I):
            # A solitary three-digit dot fraction is also a DE thousands form.
            value = None if re.fullmatch(r"[1-9]\d*\.\d{3}", match[1]) else quantity_value(match[1])
            unit = material_unit(match[2])
            if value and unit:
                matches.append((value, unit))
    if len(set(matches)) == 1:
        result.update(value=matches[0][0], unit=matches[0][1], basis="invoice_line", source_field="Menge")
    return result


def _result(kind, row_id):
    return {
        "status": "unavailable",
        "source": {"kind": kind, "id": row_id, "supplier": "", "reference": ""},
        "candidates": [],
        "coverage": {"files_total": 0, "files_read": 0, "pages_total": None,
                     "pages_read": 0, "pages_attempted": 0, "complete": False,
                     "extraction_verified": False, "files": []},
        "warnings": [],
    }


def _row(portal, kind, source_id):
    # Deliberately never select raw_json, payment_status, totals or bank fields.
    if kind == "einkauf":
        sql = ("SELECT id, beleg_typ, lieferant, original_name, stored_name, "
               "status FROM einkauf_belege WHERE id=?")
    elif kind == "lexware":
        sql = ("SELECT id, voucher_id, voucher_type, voucher_status, status, "
               "contact_name, voucher_number, voucher_date FROM lexware_rechnungen WHERE voucher_id=?")
    else:
        raise SourceUnavailable("Diese Rechnungsquelle wird nicht unterstützt.")
    db = portal.get_db()
    try:
        row = db.execute(sql, (source_id,)).fetchone()
    finally:
        db.close()
    if row is None:
        raise SourceUnavailable("Die Rechnung ist in dieser Quelle nicht vorhanden.")
    item = dict(row)
    states = {str(item.get(key) or "").strip().lower() for key in ("status", "voucher_status")}
    if states & _DENIED_STATES:
        raise SourceUnavailable("Gelöschte oder stornierte Rechnungen werden nicht ausgelesen.")
    if kind == "lexware" and str(item.get("voucher_type") or "").lower() != "purchaseinvoice":
        raise SourceUnavailable("Die Quelle ist keine bekannte Lieferantenrechnung.")
    if kind == "einkauf" and str(item.get("beleg_typ") or "").lower() != "rechnung":
        raise SourceUnavailable("Der Einkaufsbeleg ist nicht als Rechnung gekennzeichnet.")
    if classify_invoice_source(item)['decision'] == 'block':
        raise SourceUnavailable("Diese Quelle gehört nicht zu den erlaubten Materialrechnungen.")
    return item


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        raise SourceUnavailable("Die Rechnungs- oder Dateireferenz ist ungültig.") from None


def _throttle():
    global _LAST_DOWNLOAD
    with _DOWNLOAD_LOCK:
        delay = 0.55 - (time.monotonic() - _LAST_DOWNLOAD)
        if delay > 0:
            time.sleep(delay)
        _LAST_DOWNLOAD = time.monotonic()


def _lexware_metadata(portal, voucher_id):
    requests_module = portal.get_requests() if callable(getattr(portal, "get_requests", None)) else None
    if requests_module is None:
        raise SourceUnavailable("Die Unterstützung zum Laden der Rechnungsdaten fehlt.")
    response = None
    try:
        _throttle()
        response = requests_module.get(
            f"{LEXWARE_ORIGIN}/v1/vouchers/{voucher_id}",
            headers={"Authorization": f"Bearer {portal.LEXWARE_API_KEY}", "Accept": "application/json"},
            timeout=(5, 30), stream=True, allow_redirects=False,
        )
        if response.status_code == 429:
            raise SourceUnavailable("Lexware begrenzt den Zugriff; die Rechnung muss später erneut gelesen werden.")
        if response.status_code != 200:
            raise SourceUnavailable("Die Lieferantenrechnung konnte bei Lexware nicht gelesen werden.")
        raw = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            raw.extend(chunk)
            if len(raw) > 1024 * 1024:
                raise SourceUnavailable("Die Rechnungsdaten überschreiten die erlaubte Größe.")
        return json.loads(raw)
    except SourceUnavailable:
        raise
    except Exception:
        raise SourceUnavailable("Die Lieferantenrechnung konnte bei Lexware nicht sicher gelesen werden.") from None
    finally:
        if response is not None:
            response.close()


def _lexware_files(portal, row):
    base = str(getattr(portal, "LEXWARE_API_BASE_URL", "") or "").rstrip("/")
    if base != LEXWARE_ORIGIN:
        raise SourceUnavailable("Der erlaubte Lexware-API-Zugang ist nicht eingerichtet.")
    if not getattr(portal, "LEXWARE_API_KEY", ""):
        raise SourceUnavailable("Der Lexware-Lesezugang ist nicht verfügbar.")
    voucher_id = _uuid(row.get("voucher_id"))
    # The full API object is immediately reduced to invoice identity/type,
    # status and file ids; voucherItems are accounting totals, NOT products.
    payload = _lexware_metadata(portal, voucher_id)
    if not isinstance(payload, dict) or payload.get("type") != "purchaseinvoice":
        raise SourceUnavailable("Lexware bestätigt diese Quelle nicht als Lieferantenrechnung.")
    if _uuid(payload.get("id")) != voucher_id:
        raise SourceUnavailable("Die zurückgegebene Rechnung gehört nicht zur angefragten Quelle.")
    if str(payload.get("voucherStatus") or "").lower() not in {"open", "paid", "paidoff", "transferred", "sepadebit", "unchecked"}:
        raise SourceUnavailable("Diese Lieferantenrechnung ist storniert oder noch nicht lesebereit.")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise SourceUnavailable("Für diese Lieferantenrechnung sind keine Originaldateien verfügbar.")
    file_ids = list(dict.fromkeys(_uuid(value) for value in files))
    return file_ids


def _download(portal, file_id, directory):
    requests_module = portal.get_requests() if callable(getattr(portal, "get_requests", None)) else None
    if requests_module is None:
        raise SourceUnavailable("Die Unterstützung zum Laden der Rechnungsdateien fehlt.")
    # No unbounded retry: a failed/rate-limited file remains visibly incomplete.
    _throttle()
    response = None
    path = directory / f"{file_id}.download"
    try:
        response = requests_module.get(
            f"{LEXWARE_ORIGIN}/v1/files/{_uuid(file_id)}",
            headers={"Authorization": f"Bearer {portal.LEXWARE_API_KEY}", "Accept": "application/pdf"},
            timeout=(5, 30), stream=True, allow_redirects=False,
        )
        if response.status_code == 429:
            raise SourceUnavailable("Lexware begrenzt den Zugriff; diese Datei muss später erneut gelesen werden.")
        if response.status_code != 200:
            raise SourceUnavailable("Eine Rechnungsdatei konnte nicht geladen werden.")
        length = response.headers.get("Content-Length", "")
        if length and (not str(length).isdigit() or int(length) > MAX_FILE_BYTES):
            raise SourceUnavailable("Eine Rechnungsdatei überschreitet die erlaubte Dateigröße.")
        size = 0
        with path.open("wb") as target:
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                size += len(chunk)
                if size > MAX_FILE_BYTES:
                    raise SourceUnavailable("Eine Rechnungsdatei überschreitet die erlaubte Dateigröße.")
                target.write(chunk)
        if not size:
            raise SourceUnavailable("Die heruntergeladene Rechnungsdatei ist leer.")
        return path
    except SourceUnavailable:
        raise
    except Exception:
        raise SourceUnavailable("Eine Rechnungsdatei konnte nicht sicher geladen werden.") from None
    finally:
        if response is not None:
            response.close()


def _candidate(position, source, file_id, page, digest, native=False):
    if not isinstance(position, dict):
        return None
    name = _text(position.get("produkt_name"), 180)
    article = _text(position.get("artikelnummer"), 80)
    if not name:
        return None
    price = _text(position.get("preis"), 60).replace("EUR", "").replace("€", "").strip()
    if price and not re.fullmatch(r"-?\d[\d., ]{0,40}", price):
        price = ""
    quantity = _generic_quantity(position)
    provenance = dict(source, file_id=file_id, page=page, sha256=digest)
    item = {
        "produkt_name": name,
        "artikelnummer": article,
        "stueckzahl": quantity["value"],
        "ve": material_unit(position.get("ve")) or "",
        "gebinde": _text(position.get("gebinde"), 200),
        "groesse": _text(position.get("groesse"), 100),
        "farbe": _text(position.get("farbe"), 100),
        "produkt_beschreibung": _text(position.get("produkt_beschreibung"), 500),
        "preis": price,
        "kategorie": _text(position.get("kategorie"), 80) or "Material",
        "lieferant": source["supplier"],
        "verified": False,
        "price_verified": False,
        "price_evidence": {"value": price, "basis": "unknown", "verified": False},
        "quantity_evidence": quantity,
        "package_evidence": explicit_package_evidence(name, position.get("ve")),
        "source": provenance,
    }
    if native:
        # Only our positional parser reaches this path, never model-supplied
        # flags. Derived prices stay unverified and carry their exact basis.
        item.update({key: position.get(key) for key in (
            "gebinde", "groesse", "preis_geprueft", "pruefen", "auslese_hinweise", "price_evidence",
            "quantity_evidence", "package_evidence")})
        item["stueckzahl"] = position.get("stueckzahl")
        item["source"].update(position["native_source"])
    item["source"]["quantity_version"] = 1
    return item


def _extract_page(portal, path, filename, text, result, file_id, page, digest):
    complete = True
    _date_from_text(text, result)
    if len(text) > MAX_PAGE_TEXT:
        _warn(result, "Ein ungewöhnlich langer Seitentext wurde begrenzt; die Auslese ist unvollständig.")
        text = text[:MAX_PAGE_TEXT]
        complete = False
    positions = []
    # Most supplier PDFs already contain text. Keep bulk imports local and
    # reserve cloud vision for scanned/near-empty pages lacking product rows.
    if text:
        parser = getattr(portal, "extract_einkauf_beleg_positions", None)
        if callable(parser):
            try:
                positions = parser(text, filename=filename) or []
            except Exception:
                complete = False
                _warn(result, "Ein Seitentext konnte nicht in Artikelvorschläge aufgeteilt werden.")
        else:
            complete = False
            _warn(result, "Die Unterstützung zur Artikel-Auslese fehlt.")
    ai = getattr(portal, "extract_einkauf_beleg_positions_openai", None)
    if not positions and len(text.strip()) < 80 and path is not None and callable(ai):
        try:
            positions = ai(path, filename=filename, text=text) or []
        except Exception:
            _warn(result, "Die KI-Auslese war nicht verfügbar; Textpositionen bleiben ungeprüfte Vorschläge.")
    if not positions and not text:
        complete = False
        _warn(result, "Mindestens eine Seite enthält keinen auslesbaren Artikeltext und muss geprüft werden.")
    elif not positions and text:
        complete = False
        _warn(result, "Eine Seite enthält Text, aber keine erkannten Produktpositionen; Tabellenlayout und Inhalt prüfen.")
    if not isinstance(positions, list):
        complete = False
        positions = []
    if len(positions) >= MAX_POSITIONS_PER_PAGE:
        complete = False
        _warn(result, "Eine Seite erreicht die Positionsgrenze; weitere Artikel können fehlen.")
    for position in positions[:MAX_POSITIONS_PER_PAGE]:
        item = _candidate(position, result["source"], file_id, page, digest)
        if item:
            result["candidates"].append(item)
    return complete


def _read_file(portal, path, file_id, result, directory):
    coverage = result["coverage"]
    detail = {"file_id": file_id, "pages_total": None, "pages_read": 0, "pages_attempted": 0, "complete": False}
    coverage["files"].append(detail)
    if path.stat().st_size > MAX_FILE_BYTES:
        raise SourceUnavailable("Eine Rechnungsdatei überschreitet die erlaubte Dateigröße.")
    with path.open("rb") as handle:
        prefix = handle.read(1024)
        digest = hashlib.sha256(prefix)
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    digest = digest.hexdigest()
    remaining = MAX_PAGES - coverage["pages_attempted"]
    if remaining <= 0:
        _warn(result, "Die Seitengrenze wurde erreicht; weitere Dateien bleiben offen.")
        return
    if b"%PDF-" in prefix:
        getter = getattr(portal, "get_fitz", None)
        try:
            fitz = getter() if callable(getter) else importlib.import_module("fitz")
            if fitz is None:
                raise ImportError()
        except Exception:
            raise SourceUnavailable("Die Unterstützung zum seitenweisen PDF-Lesen fehlt.") from None
        with fitz.open(str(path)) as document:
            if document.needs_pass:
                raise SourceUnavailable("Eine Rechnungsdatei ist verschlüsselt und kann nicht gelesen werden.")
            detail["pages_total"] = len(document)
            detail["complete"] = 0 < len(document) <= remaining
            if len(document) > remaining:
                _warn(result, "Die Rechnung überschreitet die Seitengrenze; sie ist noch nicht vollständig ausgelesen.")
            from werkstatt_topcolor_positionen import parse_topcolor_pages
            if len(document):
                _date_from_text(document[0].get_text() or "", result)
            native = parse_topcolor_pages([
                {"page": index + 1, "height": document[index].rect.height,
                 "words": document[index].get_text("words")}
                for index in range(min(len(document), remaining))
            ], result["source"]["supplier"])
            if native is not None:
                pages_read = min(len(document), remaining)
                detail["pages_attempted"] += pages_read
                coverage["pages_attempted"] += pages_read
                detail["pages_read"] += pages_read
                coverage["pages_read"] += pages_read
                detail["complete"] = detail["complete"] and native["complete"]
                for warning in native["warnings"]:
                    _warn(result, warning)
                for position in native["positions"]:
                    candidate = _candidate(position, result["source"], file_id,
                                           position["native_source"]["page"], digest, native=True)
                    if candidate:
                        result["candidates"].append(candidate)
                coverage["files_read"] += 1
                return
            for index in range(min(len(document), remaining)):
                page_path = directory / f"{file_id}-page-{index + 1}.pdf"
                detail["pages_attempted"] += 1
                coverage["pages_attempted"] += 1
                try:
                    with fitz.open() as page_document:
                        page_document.insert_pdf(document, from_page=index, to_page=index)
                        page_document.save(str(page_path))
                    text = document.load_page(index).get_text() or ""
                    if not text.strip() and callable(getattr(portal, "extract_document_text_local", None)):
                        text = portal.extract_document_text_local(page_path, page_path.name) or ""
                    read = _extract_page(portal, page_path, page_path.name, text, result, file_id, index + 1, digest)
                    detail["complete"] = detail["complete"] and read
                    detail["pages_read"] += 1
                    coverage["pages_read"] += 1
                except Exception:
                    detail["complete"] = False
                    _warn(result, "Mindestens eine Rechnungsseite konnte nicht gelesen werden.")
                finally:
                    page_path.unlink(missing_ok=True)
    elif (prefix.startswith(b"\x89PNG\r\n\x1a\n") or prefix.startswith(b"\xff\xd8\xff")
          or (prefix.startswith(b"RIFF") and prefix[8:12] == b"WEBP")):
        suffix = ".png" if prefix.startswith(b"\x89PNG") else (".webp" if prefix[8:12] == b"WEBP" else ".jpg")
        image_path = directory / f"{file_id}{suffix}"
        if image_path != path:
            image_path.write_bytes(path.read_bytes())
        detail["pages_total"] = 1
        detail["pages_attempted"] = 1
        coverage["pages_attempted"] += 1
        text = ""
        if callable(getattr(portal, "extract_document_text_local", None)):
            text = portal.extract_document_text_local(image_path, image_path.name) or ""
        detail["complete"] = _extract_page(portal, image_path, image_path.name, text, result, file_id, 1, digest)
        detail["pages_read"] = 1
        coverage["pages_read"] += 1
    else:
        raise SourceUnavailable("Das Rechnungsformat wird für die seitenweise Auslese noch nicht unterstützt.")
    if detail["pages_read"]:
        coverage["files_read"] += 1


def _stored_text(portal, row, result):
    # Only after the metadata/type gate, and only if the original is missing.
    # Quarantined documents must not even load their persisted OCR fallback.
    db = portal.get_db()
    try:
        stored = db.execute("SELECT extrahierter_text FROM einkauf_belege WHERE id=? AND beleg_typ='rechnung'", (row['id'],)).fetchone()
    finally:
        db.close()
    text = str(stored['extrahierter_text'] or "") if stored else ""
    if not text.strip():
        return
    _warn(result, "Nur gespeicherter Text ist verfügbar; die vollständige Seitenabdeckung ist nicht nachweisbar.")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    # Stored OCR may have been truncated by an earlier importer. Never present
    # it as a successfully read full invoice or invent physical page numbers.
    chunks = text.split("\f")[:MAX_PAGES]
    if len(text.split("\f")) > MAX_PAGES:
        _warn(result, "Der gespeicherte Text überschreitet die Seitengrenze.")
    for chunk in chunks:
        _extract_page(portal, None, "Rechnungstext", chunk, result, "stored-text", None, digest)


def read_source(portal, source_kind, source_id):
    """Return product proposals and explicit coverage; never mutate either source.

    Catchable failures are returned as sanitized warnings, without exception
    payloads, local paths, raw documents, tokens, invoice totals or payments.
    An incomplete read may still contain useful unverified product proposals.
    """
    kind = source_kind if isinstance(source_kind, str) and source_kind in {"einkauf", "lexware"} else "unknown"
    try:
        if kind == "lexware":
            row_id = _uuid(source_id)
        else:
            row_id = int(source_id)
            if isinstance(source_id, bool) or row_id <= 0 or str(source_id).strip() != str(row_id):
                raise ValueError()
    except (TypeError, ValueError, OverflowError, SourceUnavailable):
        result = _result(kind, None)
        _warn(result, "Die Rechnungsreferenz ist ungültig.")
        return result
    result = _result(kind, row_id)
    coverage = result["coverage"]
    try:
        row = _row(portal, kind, row_id)
        result["source"]["supplier"] = _text(row.get("lieferant" if kind == "einkauf" else "contact_name"), 240)
        result["source"]["reference"] = _text(row.get("original_name" if kind == "einkauf" else "voucher_number"), 240)
        result["source"]["date"] = invoice_date(row.get("voucher_date"))
        if not result["source"]["supplier"]:
            _warn(result, "Der Lieferant ist in der Quelle nicht zugeordnet und muss geprüft werden.")
        with tempfile.TemporaryDirectory(prefix="werkstatt-rechnungsquelle-") as temporary:
            directory = pathlib.Path(temporary)
            if kind == "lexware":
                file_ids = _lexware_files(portal, row)
                coverage["files_total"] = len(file_ids)
                if len(file_ids) > MAX_FILES:
                    _warn(result, "Die Dateigrenze wurde erreicht; weitere Rechnungsdateien bleiben offen.")
                for file_id in file_ids[:MAX_FILES]:
                    try:
                        path = _download(portal, file_id, directory)
                        _read_file(portal, path, file_id, result, directory)
                    except SourceUnavailable as exc:
                        _warn(result, str(exc))
                    except Exception:
                        _warn(result, "Eine Rechnungsdatei konnte nicht ausgewertet werden.")
            else:
                coverage["files_total"] = 1
                stored = str(row.get("stored_name") or "")
                upload_root = pathlib.Path(portal.UPLOAD_DIR).resolve()
                path = (upload_root / stored).resolve()
                safe_name = bool(stored and "/" not in stored and "\\" not in stored
                                 and path.is_relative_to(upload_root) and path.parent == upload_root)
                available = safe_name and path.is_file()
                if safe_name and not available:
                    # Restore only the exact known local original from its
                    # private persisted blob. Never reconnect to IMAP, reimport
                    # a message or guess a new receipt when a file disappears.
                    restore = getattr(portal, "assistant_mail_sources_restore_file", None)
                    if callable(restore):
                        try:
                            available = restore(stored) is True and path.is_file() and path.resolve().parent == upload_root
                        except Exception:
                            _warn(result, "Die lokale Sicherung des Rechnungsoriginals konnte nicht geprüft werden.")
                if not available:
                    _warn(result, "Die Originaldatei der Einkaufsrechnung ist nicht verfügbar.")
                    _stored_text(portal, row, result)
                else:
                    try:
                        _read_file(portal, path, f"einkauf-{row_id}", result, directory)
                    except SourceUnavailable as exc:
                        _warn(result, str(exc))
                        if not result["candidates"]:
                            _stored_text(portal, row, result)
        details = coverage["files"]
        all_known = len(details) == coverage["files_total"] and all(x["pages_total"] is not None for x in details)
        coverage["pages_total"] = sum(x["pages_total"] for x in details) if all_known else None
        coverage["complete"] = bool(details) and all_known and all(x["complete"] for x in details)
        if not result["candidates"]:
            _warn(result, "Es wurden keine Artikelpositionen erkannt; die Rechnung muss geprüft werden.")
        result["status"] = "ok" if coverage["complete"] else ("partial" if result["candidates"] or coverage["pages_read"] else "unavailable")
    except SourceUnavailable as exc:
        _warn(result, str(exc))
    except Exception:
        result["status"] = "partial" if result["candidates"] else "error"
        _warn(result, "Die Rechnungsquelle konnte nicht sicher ausgewertet werden.")
    return result
