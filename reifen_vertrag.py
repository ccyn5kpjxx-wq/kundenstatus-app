"""Durable tire contract copies and an independent public withdrawal intake.

No supplier, payment or OTP call occurs here. Withdrawal statements never cancel
orders or trigger refunds. SMTP requires a separately authorized explicit runner.
"""
from collections.abc import Mapping
from contextlib import closing
import base64
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.message import EmailMessage
from email.utils import format_datetime, formataddr, parseaddr
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request, session, url_for

from werkstatt_mail_identity import FOOTER_TEXT
from werkstatt_kontakt import NAME as WORKSHOP_NAME


BACKUP_TABLES = ("reifen_vertrag_outbox", "reifen_vertrag_anhaenge", "reifen_widerruf", "reifen_widerruf_limits")
_ANNEX_PATH = Path(__file__).resolve().parent / "static" / "recht" / "reifen-gewaehrleistung-de.jpg"
_ANNEX_SHA256 = "00e2b5ecc4f86bed59cc1ead77db7e98a318f264df97d77a01b5a160dee93138"
_ANNEX_FILENAME = "EU-Gewaehrleistungsmitteilung-Deutsch.jpg"
_MAX_ANNEX_BYTES = 5 * 1024 * 1024
_TEXT_KEYS = ("agb", "widerruf", "datenschutz", "lieferung")
_FORM_KEYS = {"name", "bestellnummer", "email", "umfang", "teil", "csrf_token"}
_MAX_BODY = 16384


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_vertrag_anhaenge (
        sha256 TEXT PRIMARY KEY, filename TEXT NOT NULL, mimetype TEXT NOT NULL,
        size INTEGER NOT NULL, content_base64 TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_vertrag_outbox (
        id TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
        recipient TEXT NOT NULL, subject TEXT NOT NULL, body TEXT NOT NULL,
        snapshot_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
        message_id TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'queued', attempt_id TEXT NOT NULL DEFAULT '',
        attempted_at TEXT NOT NULL DEFAULT '', sent_at TEXT NOT NULL DEFAULT '',
        detail TEXT NOT NULL DEFAULT ''
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_widerruf (
        id TEXT PRIMARY KEY, owner_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
        payload_hash TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL,
        confirmed_at TEXT NOT NULL DEFAULT ''
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_widerruf_limits (
        id TEXT PRIMARY KEY, attempts INTEGER NOT NULL DEFAULT 0
    )""")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _text(value, maximum, *, multiline=False, required=True):
    if (not isinstance(value, str) or len(value) > maximum
            or any(ord(char) < 32 and not (multiline and char in "\n\r\t") for char in value)):
        raise ValueError("Bitte die Angaben ohne Steuerzeichen und innerhalb der angezeigten Länge eingeben.")
    value = value.strip()
    if required and not value:
        raise ValueError("Bitte alle erforderlichen Angaben ergänzen.")
    return value


def _email(value):
    value = _text(value, 254)
    display, address = parseaddr(value)
    if display or address != value or not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\.[A-Za-z]{2,63}", value):
        raise ValueError("Bitte genau eine gültige E-Mail-Adresse eingeben.")
    return value


def _timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Eingangszeit benötigt eine Zeitzone.")
    return parsed


def _annex_reference():
    return {"sha256": _ANNEX_SHA256, "filename": _ANNEX_FILENAME, "mimetype": "image/jpeg"}


def _annex_bytes(db):
    row = db.execute("SELECT * FROM reifen_vertrag_anhaenge WHERE sha256=?", (_ANNEX_SHA256,)).fetchone()
    if not row:
        raise ValueError("Die unveränderte EU-Gewährleistungsmitteilung fehlt im Bestätigungsarchiv.")
    try:
        if (row["filename"] != _ANNEX_FILENAME or row["mimetype"] != "image/jpeg"
                or type(row["size"]) is not int or not 0 < row["size"] <= _MAX_ANNEX_BYTES
                or not isinstance(row["content_base64"], str)
                or len(row["content_base64"]) > (_MAX_ANNEX_BYTES + 2) // 3 * 4):
            raise ValueError()
        raw = base64.b64decode(row["content_base64"], validate=True)
        if (len(raw) != row["size"] or not raw.startswith(b"\xff\xd8\xff")
                or hashlib.sha256(raw).hexdigest() != _ANNEX_SHA256):
            raise ValueError()
        return raw
    except Exception:
        raise ValueError("Der archivierte EU-Anhang ist unvollständig oder verändert.") from None


def _store_annex(db):
    """Fixed verified original only; never consume a path from order/config data."""
    if db.execute("SELECT sha256 FROM reifen_vertrag_anhaenge WHERE sha256=?", (_ANNEX_SHA256,)).fetchone():
        _annex_bytes(db)
        return _annex_reference()
    try:
        with _ANNEX_PATH.open("rb") as source:
            raw = source.read(_MAX_ANNEX_BYTES + 1)
        if (not 0 < len(raw) <= _MAX_ANNEX_BYTES or not raw.startswith(b"\xff\xd8\xff")
                or hashlib.sha256(raw).hexdigest() != _ANNEX_SHA256):
            raise ValueError()
    except Exception:
        raise ValueError("Die unveränderte offizielle EU-Gewährleistungsmitteilung ist nicht verfügbar.") from None
    db.execute("INSERT INTO reifen_vertrag_anhaenge(sha256,filename,mimetype,size,content_base64) "
               "VALUES(?,?,?,?,?) ON CONFLICT(sha256) DO NOTHING",
               (_ANNEX_SHA256, _ANNEX_FILENAME, "image/jpeg", len(raw), base64.b64encode(raw).decode("ascii")))
    _annex_bytes(db)
    return _annex_reference()


def _legal_texts(value):
    if not isinstance(value, Mapping):
        raise ValueError("Die vollständige Bedingungsfassung fehlt.")
    try:
        if any(not isinstance(key, str) for key in value):
            raise ValueError()
        result = json.loads(json.dumps(dict(value), ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError):
        raise ValueError("Die Bedingungsfassung muss vollständig als Dokumentdaten vorliegen.") from None
    result["version"] = _text(value.get("version"), 200)
    for key in _TEXT_KEYS:
        _text(value.get(key), 30000, multiline=True)
    if len(_json(result).encode("utf-8")) > 120000:
        raise ValueError("Die Bedingungsfassung überschreitet die Dokumentgrenze.")
    return result


def _withdrawal_body(data, received):
    local = _timestamp(received).astimezone(ZoneInfo("Europe/Berlin"))
    return ("Ihre Widerrufserklärung ist bei uns eingegangen.\n\n"
        f"Eingang: {local:%d.%m.%Y %H:%M:%S} Uhr ({local:%z}, Europe/Berlin)\n"
        f"Name: {data['name']}\nBestellung/Vertragsangaben: {data['bestellnummer']}\n"
        f"E-Mail für die Eingangsbestätigung: {data['email']}\n"
        f"Umfang: {'gesamter Vertrag' if data['umfang'] == 'alles' else 'angegebener Teil'}\n"
        f"Teilangaben: {data.get('teil') or '–'}\n\n"
        "Erklärung: Hiermit widerrufe ich den bezeichneten Vertrag im angegebenen Umfang.\n\n"
        "Diese Nachricht bestätigt den Eingang Ihrer Erklärung. Die Zuordnung zur Bestellung "
        "und gegebenenfalls weitere Schritte werden gesondert bearbeitet.\n\n" + FOOTER_TEXT)


def _order_body(order):
    customer = order.get("kunde")
    product = order.get("produkt")
    if not isinstance(customer, Mapping) or not isinstance(product, Mapping):
        raise ValueError("Bestell- und Kundendaten fehlen.")
    status, payment_method, payment_status = order.get("status"), order.get("zahlungsart"), order.get("zahlung_status")
    if (order.get("bestaetigt") is not True
            or not (status == "bestaetigt" and payment_method == "online" and payment_status == "bezahlt"
                    or status == "bestellt_zahlung_offen" and payment_method == "vor_ort" and payment_status == "vor_ort_offen")):
        raise ValueError("Eine Bestellannahme darf nur für eine abschließend bestätigte Bestellung versandt werden.")
    accepted_at = _text(order.get("accepted_at"), 100)
    _timestamp(accepted_at)
    amount, quantity = order.get("amount_total"), order.get("anzahl")
    if (type(amount) is not int or amount <= 0 or amount > 10000000
            or type(quantity) is not int or not 1 <= quantity <= 8
            or order.get("currency") not in {"eur", "EUR"}):
        raise ValueError("Der bestätigte Bestellpreis fehlt.")
    legal = _legal_texts(order.get("rechtstexte"))
    category = order.get("produktart", "reifen")
    if (not isinstance(category, str) or category not in {"reifen", "felgen"}
            or product.get("produktart", category) != category):
        raise ValueError("Die Produktart der Vertragskopie ist nicht eindeutig eingerichtet.")
    scope = legal.get("produktarten")
    if (scope is not None and (not isinstance(scope, list) or not scope
            or any(not isinstance(item, str) or item not in {"reifen", "felgen", "komplettraeder"} for item in scope)
            or len(set(scope)) != len(scope) or category not in scope)
            or category == "felgen" and scope is None):
        raise ValueError("Die Bedingungsfassung gilt nicht für diese Produktart.")
    if category == "felgen" and (order.get("montage_datum") or order.get("montage_uhrzeit")):
        raise ValueError("Eine Felgenmontage ist in diesem Kaufpfad nicht eingerichtet.")
    product_keys = (("artikel_id", "bezeichnung", "groesse", "ean", "saison", "qualitaet")
                    if category == "reifen" else
                    ("artikel_id", "bezeichnung", "ean", "hersteller", "profil", "hersteller_artikelnummer", "material"))
    title, product_label = (("Reifenbestellung", "Reifen") if category == "reifen"
                            else ("Felgenbestellung", "Felgen"))
    snapshot = {
        "id": _text(order.get("id"), 200), "anzahl": quantity, "produktart": category,
        "amount_total": amount, "currency": "EUR", "rechtstexte": legal,
        "kunde": {key: _text(customer.get(key, ""), 300, required=False)
                  for key in ("name", "email", "telefon", "strasse", "hausnummer", "plz", "ort", "land")},
        "produkt": {key: _text("" if product.get(key) is None else product[key], 400, required=False)
                    for key in product_keys},
        "zahlungsart": _text(order.get("zahlungsart", ""), 100),
        "montage_datum": _text(order.get("montage_datum", ""), 100, required=False),
        "montage_uhrzeit": _text(order.get("montage_uhrzeit", ""), 100, required=False),
        "lieferdatum": _text(order.get("lieferdatum", ""), 100, required=False),
        "accepted_at": accepted_at, "status": status, "zahlung_status": payment_status,
        "demo": order.get("demo") is True or order.get("mode") in {"test", "demo"},
        "erklaerungen": order.get("erklaerungen", {}),
        "preis": product.get("preis", {}),
    }
    # Never copy arbitrary provider, purchase-price or browser payload fields.
    if not isinstance(snapshot["preis"], Mapping):
        raise ValueError("Preisaufteilung fehlt.")
    price_keys = (("einzel_brutto", "gesamt_reifen_brutto", "versand_brutto", "montage_brutto", "gesamt_brutto")
                  if category == "reifen" else
                  ("einzel_brutto", "gesamt_felgen_brutto", "versand_brutto", "gesamt_brutto"))
    snapshot["preis"] = {key: _text(snapshot["preis"].get(key, ""), 100, required=False) for key in price_keys}
    if category == "felgen":
        price = snapshot["preis"]
        if (any(not re.fullmatch(r"(?:0|[1-9][0-9]{0,5})\.[0-9]{2}", price[key]) for key in price_keys)
                or Decimal(price["einzel_brutto"]) <= 0
                or Decimal(price["einzel_brutto"]) * quantity != Decimal(price["gesamt_felgen_brutto"])
                or Decimal(price["gesamt_felgen_brutto"]) + Decimal(price["versand_brutto"]) != Decimal(price["gesamt_brutto"])
                or Decimal(price["gesamt_brutto"]) * 100 != amount
                or not snapshot["produkt"]["artikel_id"] or not snapshot["produkt"]["bezeichnung"]):
            raise ValueError("Der Felgenartikel und seine bestätigte Preisaufteilung fehlen oder widersprechen sich.")
    recipient = _email(snapshot["kunde"]["email"])
    lines = ["Ihre " + title + " und Vertragsunterlagen", "",
        ("DEMO – keine echte Bestellung. Wir simulieren die folgende Bestellannahme."
         if snapshot["demo"] else "Wir nehmen Ihre folgende Bestellung verbindlich an."), "",
        "Aufbewahrbare Kopie Ihrer Bestellung und der zugrunde gelegten Bedingungen.",
        "Bestellnummer: " + snapshot["id"], "Name: " + snapshot["kunde"]["name"],
        product_label + ": " + snapshot["produkt"]["bezeichnung"],
        *( ["Größe: " + snapshot["produkt"]["groesse"]] if category == "reifen" else
           ["Herstellerartikelnummer: " + (snapshot["produkt"]["hersteller_artikelnummer"] or "nicht angegeben"),
            "Fahrzeugpassung: nicht geprüft; diese Bestellung enthält keine Passungsfreigabe."] ),
        "Artikel: " + snapshot["produkt"]["artikel_id"], "Menge: " + str(quantity),
        f"Gesamtpreis einschließlich Umsatzsteuer: {amount // 100},{amount % 100:02d} EUR",
        "Zahlungsart: " + snapshot["zahlungsart"],
        ("Zahlung: bestätigt und bezahlt." if payment_status == "bezahlt"
         else "Zahlung: noch offen; Bezahlung vor Ort bei Abholung oder Montage."),
        "Bestätigtes Bereitstellungsdatum: " + (snapshot["lieferdatum"] or "siehe individuelle Vereinbarung"),
        "Bestellannahme: " + (snapshot["accepted_at"] or "siehe Bestellstatus"),
        "Montagetermin: " + (snapshot["montage_datum"] + " " + snapshot["montage_uhrzeit"]
                              if snapshot["montage_datum"] else "ohne Montage"),
        "", "Preisaufteilung (EUR brutto):"]
    lines.extend(key + ": " + value for key, value in snapshot["preis"].items() if value)
    lines.extend(["", "Kundendaten:"] + [key + ": " + value for key, value in snapshot["kunde"].items() if value])
    lines.extend(["", "Bedingungsfassung: " + legal["version"]])
    if snapshot["erklaerungen"]:
        lines.extend(["", "GESPEICHERTE INDIVIDUELLE ERKLÄRUNGEN", _json(snapshot["erklaerungen"])])
    for key, value in legal.items():
        if key != "version":
            lines.extend(["", key.upper(), value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)])
    lines.extend(["", FOOTER_TEXT])
    subject = ("DEMO · " if snapshot["demo"] else "") + "Ihre " + title + " " + snapshot["id"]
    return recipient, subject, "\n".join(lines), snapshot


def enqueue(db, order_dict, kind):
    """Persist an immutable durable copy inside the caller's transaction; no IO."""
    if not isinstance(order_dict, Mapping):
        raise ValueError("Bestelldaten fehlen.")
    identifier = _text(order_dict.get("id"), 200)
    if kind == "bestellbestaetigung":
        recipient, subject, body, snapshot = _order_body(order_dict)
        snapshot["anhaenge"] = [_store_annex(db)]
        body += "\n\nAnhang: Unveränderte EU-Gewährleistungsmitteilung in deutscher Sprache."
    elif kind == "widerrufseingang":
        data = order_dict.get("widerruf")
        if not isinstance(data, Mapping):
            raise ValueError("Widerrufserklärung fehlt.")
        data = dict(data)
        recipient = _email(data["email"])
        received = _text(order_dict.get("eingang_am"), 80)
        body = _withdrawal_body(data, received)
        snapshot = {"id": identifier, "widerruf": data, "eingang_am": received}
        subject = "Eingang Ihrer Widerrufserklärung"
    else:
        raise ValueError("Unbekannte Bestätigungsart.")
    key = kind + ":" + identifier
    snapshot_json = _json(snapshot)
    payload_hash = _hash(_json([recipient, subject, body, snapshot_json]))
    existing = db.execute("SELECT id,payload_hash FROM reifen_vertrag_outbox WHERE dedupe_key=?", (key,)).fetchone()
    if existing:
        if existing["payload_hash"] != payload_hash:
            raise ValueError("Eine bereits gespeicherte Bestätigung darf nicht überschrieben werden.")
        return existing["id"]
    outbox_id = secrets.token_hex(24)
    message_id = "<reifen-" + _hash(key) + "@auto-lackierzentrum.de>"
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""INSERT INTO reifen_vertrag_outbox
        (id,dedupe_key,kind,recipient,subject,body,snapshot_json,payload_hash,message_id,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dedupe_key) DO NOTHING""",
        (outbox_id, key, kind, recipient, subject, body, snapshot_json, payload_hash, message_id, now))
    saved = db.execute("SELECT id,payload_hash FROM reifen_vertrag_outbox WHERE dedupe_key=?", (key,)).fetchone()
    if not saved or saved["payload_hash"] != payload_hash:
        raise ValueError("Die Bestätigung konnte nicht unverändert gespeichert werden.")
    return saved["id"]


def deliver_pending(portal, mailer, limit=10):
    """Explicit runner. Ambiguous sends and confirmed failures are not retried."""
    gate = portal.app.config.get("REIFEN_VERTRAG_MAIL_FREIGABE")
    if not callable(gate) or gate() is not True:
        return {"sent": 0, "failed": 0, "unknown": 0, "enabled": False}
    if not callable(mailer) or type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError("Ein ausdrücklicher Versandadapter und ein begrenzter Lauf sind erforderlich.")
    sender = _email(portal.app.config.get("REIFEN_SMTP_FROM", ""))
    with closing(portal.get_db()) as db:
        candidates = db.execute("SELECT id FROM reifen_vertrag_outbox WHERE state='queued' "
                                "ORDER BY created_at,id LIMIT ?", (limit,)).fetchall()
    result = {"sent": 0, "failed": 0, "unknown": 0, "enabled": True}
    for candidate in candidates:
        if gate() is not True:
            break
        attempt = secrets.token_hex(24)
        with closing(portal.get_db()) as db:
            row = db.execute("UPDATE reifen_vertrag_outbox SET state='sending',attempt_id=?,attempted_at=? "
                "WHERE id=? AND state='queued' RETURNING *", (attempt, datetime.now(timezone.utc).isoformat(), candidate["id"])).fetchone()
            db.commit()
        if row is None:
            continue
        row = dict(row)
        expected = _hash(_json([row["recipient"], row["subject"], row["body"], row["snapshot_json"]]))
        state = "unknown"
        expected_id = "<reifen-" + _hash(row["dedupe_key"]) + "@auto-lackierzentrum.de>"
        if (hmac.compare_digest(expected, row["payload_hash"])
                and hmac.compare_digest(expected_id, row["message_id"])):
            try:
                message = EmailMessage()
                message["From"] = formataddr(("Gärtner GmbH Karosserie + Lack", sender))
                message["To"], message["Subject"] = row["recipient"], row["subject"]
                message["Message-ID"] = row["message_id"]
                message["Date"] = format_datetime(_timestamp(row["created_at"]))
                message.set_content(row["body"])
                if row["kind"] == "bestellbestaetigung":
                    snapshot = json.loads(row["snapshot_json"])
                    if snapshot.get("anhaenge") != [_annex_reference()]:
                        raise ValueError("Der verpflichtende EU-Anhang fehlt im gespeicherten Bestätigungssnapshot.")
                    with closing(portal.get_db()) as db:
                        original = _annex_bytes(db)
                    message.add_attachment(original, maintype="image", subtype="jpeg", filename=_ANNEX_FILENAME)
                outcome = mailer(message)
                state = "sent" if outcome is True or outcome == "sent" else "failed" if outcome == "not_sent" else "unknown"
            except Exception:
                state = "unknown"
        with closing(portal.get_db()) as db:
            db.execute("UPDATE reifen_vertrag_outbox SET state=?,sent_at=?,detail=? "
                "WHERE id=? AND state='sending' AND attempt_id=?",
                (state, datetime.now(timezone.utc).isoformat() if state == "sent" else "",
                 "" if state == "sent" else "vor_annahme_nicht_gesendet" if state == "failed" else "manuell_pruefen",
                 row["id"], attempt))
            db.commit()
        result[state] += 1
    return result


class ReifenVertrag:
    def __init__(self, portal):
        self.p = portal
        self.app = portal.app

    def now(self):
        clock = self.app.config.get("REIFEN_VERTRAG_CLOCK")
        value = clock() if callable(clock) else datetime.now(timezone.utc)
        if value.tzinfo is None:
            raise ValueError("Die Eingangszeit benötigt eine Zeitzone.")
        return value.astimezone(timezone.utc)

    def owner(self):
        raw = session.setdefault("reifen_widerruf_owner", secrets.token_urlsafe(32))
        return hmac.new(str(self.app.secret_key).encode(), raw.encode(), hashlib.sha256).hexdigest()

    def csrf(self):
        return session.setdefault("csrf_token", secrets.token_urlsafe(32))

    def check_post(self):
        expected, values = self.csrf(), request.form.getlist("csrf_token")
        if not values or any(not hmac.compare_digest(expected, value) for value in values):
            abort(400)
        bucket = int(self.now().timestamp()) // 900
        identity = (request.remote_addr or "") + ":" + str(bucket)
        key = hmac.new(str(self.app.secret_key).encode(), identity.encode(), hashlib.sha256).hexdigest()
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO reifen_widerruf_limits(id,attempts) VALUES(?,0) ON CONFLICT(id) DO NOTHING", (key,))
            allowed = db.execute("UPDATE reifen_widerruf_limits SET attempts=attempts+1 "
                                 "WHERE id=? AND attempts<30 RETURNING id", (key,)).fetchone()
            db.commit()
        if not allowed:
            abort(429, description="Bitte versuchen Sie es später erneut oder senden Sie Ihre Erklärung per E-Mail an info@auto-lackierzentrum.de.")

    def draft(self, identifier):
        with closing(self.p.get_db()) as db:
            row = db.execute("SELECT * FROM reifen_widerruf WHERE id=? AND owner_hash=?",
                             (identifier, self.owner())).fetchone()
        if not row:
            abort(404)
        row = dict(row)
        if not row["confirmed_at"] and _timestamp(row["expires_at"]) <= self.now():
            abort(410, description="Bitte die Widerrufserklärung erneut eingeben.")
        return row


def register_reifen_vertrag(portal):
    service = ReifenVertrag(portal)
    bp = Blueprint("reifen_vertrag", __name__)

    @bp.context_processor
    def workshop_brand():
        return {"reifen_vertrag_firma": WORKSHOP_NAME}

    @bp.before_request
    def bounded_body():
        if request.method == "POST":
            request.max_content_length = _MAX_BODY
            if request.content_length is not None and request.content_length > _MAX_BODY:
                abort(413)

    @bp.after_request
    def private(response):
        response.headers["Cache-Control"] = "no-store"
        return response

    @bp.get("/reifen/vertrag-widerrufen")
    def start():
        return render_template("reifen_vertrag_form.html", csrf=service.csrf(), values={}, error="")

    @bp.post("/reifen/widerruf/pruefen")
    def pruefen():
        service.check_post()
        try:
            if set(request.form) - _FORM_KEYS or any(len(request.form.getlist(key)) != 1 for key in _FORM_KEYS - {"csrf_token", "teil"}):
                raise ValueError("Bitte die Erklärung vollständig und eindeutig ausfüllen.")
            if len(request.form.getlist("teil")) > 1:
                raise ValueError("Bitte den Umfang eindeutig angeben.")
            data = {key: _text(request.form.get(key, ""), maximum, required=key != "teil", multiline=key == "teil")
                    for key, maximum in (("name", 200), ("bestellnummer", 200), ("email", 254), ("umfang", 20), ("teil", 2000))}
            data["email"] = _email(data["email"])
            if data["umfang"] not in {"alles", "teilweise"} or data["umfang"] == "teilweise" and not data["teil"]:
                raise ValueError("Bitte den gesamten Vertrag oder die betroffenen Teile angeben.")
        except ValueError as error:
            return render_template("reifen_vertrag_form.html", csrf=service.csrf(), values=request.form, error=str(error)), 400
        identifier, now = secrets.token_hex(24), service.now()
        with closing(portal.get_db()) as db:
            db.execute("INSERT INTO reifen_widerruf(id,owner_hash,payload_json,payload_hash,created_at,expires_at) VALUES(?,?,?,?,?,?)",
                (identifier, service.owner(), _json(data), _hash(_json(data)), now.isoformat(), (now + timedelta(minutes=30)).isoformat()))
            db.commit()
        return render_template("reifen_vertrag_pruefen.html", csrf=service.csrf(), identifier=identifier, data=data)

    @bp.post("/reifen/widerruf/bestaetigen/<identifier>")
    def bestaetigen(identifier):
        service.check_post()
        if set(request.form) != {"csrf_token"}:
            abort(400)
        row = service.draft(identifier)
        if row["confirmed_at"]:
            return redirect(url_for("reifen_vertrag.eingang", identifier=identifier))
        received = row["confirmed_at"] or service.now().isoformat()
        with closing(portal.get_db()) as db:
            # Serialize confirmations; declaration and acknowledgement commit together.
            db.execute("UPDATE reifen_widerruf SET confirmed_at=CASE WHEN confirmed_at='' THEN ? ELSE confirmed_at END "
                       "WHERE id=? AND owner_hash=?", (received, identifier, service.owner()))
            saved = dict(db.execute("SELECT * FROM reifen_widerruf WHERE id=? AND owner_hash=?", (identifier, service.owner())).fetchone())
            if _hash(saved["payload_json"]) != saved["payload_hash"]:
                abort(409)
            enqueue(db, {"id": identifier, "widerruf": json.loads(saved["payload_json"]),
                         "eingang_am": saved["confirmed_at"]}, "widerrufseingang")
            db.commit()
        return redirect(url_for("reifen_vertrag.eingang", identifier=identifier))

    @bp.get("/reifen/widerruf/eingang/<identifier>")
    def eingang(identifier):
        row = service.draft(identifier)
        if not row["confirmed_at"]:
            abort(404)
        return render_template("reifen_vertrag_eingang.html", identifier=identifier,
            data=json.loads(row["payload_json"]), received=_timestamp(row["confirmed_at"]).astimezone(ZoneInfo("Europe/Berlin")))

    portal.app.register_blueprint(bp)
    portal.app.extensions["reifen_vertrag"] = service
    return service
