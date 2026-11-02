"""Isolated tire-checkout preparation. Live purchasing/payment is not implemented.

All mutations require testing, loopback, a dedicated *.reifen-demo.sqlite3 and
explicit demo adapters. No boolean config flag can enable a live checkout.
Supplier, SMS and payment objects are injected; this module performs no HTTP.
"""
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import ipaddress
import json
from pathlib import Path
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.exceptions import HTTPException

from reifen_suche import parse_reifen_suche, suche_reifenangebote


SCHEMA_FEATURE = "reifen_bestellung_demo_v1"
BACKUP_TABLES = ("reifen_bestellungen",)
_TOKEN_SALT = "reifen-auswahl-v1"
_QUERY_FIELDS = ("groesse", "saison", "qualitaet", "anzahl", "montage")
_FINAL = {"demo_bestaetigt", "demo_bestellt_zahlung_offen"}


def erstelle_auswahl_token(secret, query_args, artikel_id):
    parse_reifen_suche(query_args)
    if not isinstance(artikel_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,100}", artikel_id):
        raise ValueError("Ungültige Artikelauswahl.")
    payload = {"query": {key: query_args[key] for key in _QUERY_FIELDS}, "artikel_id": artikel_id}
    return URLSafeTimedSerializer(secret, salt=_TOKEN_SALT).dumps(payload)


def lade_auswahl_token(secret, token):
    try:
        if not isinstance(token, str) or len(token) > 2000:
            raise ValueError("Ungültige Auswahl.")
        payload = URLSafeTimedSerializer(secret, salt=_TOKEN_SALT).loads(token, max_age=900)
        if not isinstance(payload, dict) or set(payload) != {"query", "artikel_id"}:
            raise ValueError("Ungültige Auswahl.")
        parse_reifen_suche(payload["query"])
        if not isinstance(payload["artikel_id"], str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,100}", payload["artikel_id"]):
            raise ValueError("Ungültige Artikelauswahl.")
        return payload
    except (BadSignature, SignatureExpired, TypeError, ValueError) as exc:
        raise ValueError("Die Reifenauswahl ist ungültig oder abgelaufen. Bitte neu suchen.") from exc


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_bestellungen (
        id TEXT PRIMARY KEY, owner_hash TEXT NOT NULL, draft_key TEXT NOT NULL UNIQUE,
        auswahl_token TEXT NOT NULL, query_json TEXT NOT NULL, produkt_json TEXT NOT NULL,
        quote_hash TEXT NOT NULL, kunde_json TEXT NOT NULL, telefon TEXT NOT NULL,
        telefon_bestaetigt_am TEXT NOT NULL DEFAULT '', zahlungsart TEXT NOT NULL,
        montage_datum TEXT NOT NULL DEFAULT '', montage_uhrzeit TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'entwurf', reservierung_json TEXT NOT NULL DEFAULT '{}',
        slot_lead_id INTEGER, payment_id TEXT UNIQUE, payment_url TEXT NOT NULL DEFAULT '',
        payment_status TEXT NOT NULL DEFAULT 'unbezahlt',
        otp_hash TEXT NOT NULL DEFAULT '', otp_gueltig_bis TEXT NOT NULL DEFAULT '',
        otp_versuche INTEGER NOT NULL DEFAULT 0, otp_sendungen INTEGER NOT NULL DEFAULT 0,
        otp_gesendet_am TEXT NOT NULL DEFAULT '', bedingungen_json TEXT NOT NULL DEFAULT '{}',
        erstellt_am TEXT NOT NULL, geaendert_am TEXT NOT NULL
    )""")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Zeitzone fehlt.")
    return parsed.astimezone(timezone.utc)


def _phone(value):
    text = re.sub(r"[ ()/-]", "", str(value or ""))
    if text.startswith("00"):
        text = "+" + text[2:]
    elif text.startswith("0"):
        text = "+49" + text[1:]
    if not re.fullmatch(r"\+[1-9][0-9]{8,14}", text):
        raise ValueError("Bitte eine gültige Mobiltelefonnummer angeben.")
    return text


class ReifenBestellung:
    def __init__(self, portal):
        self.p = portal
        self.app = portal.app

    def now(self):
        clock = self.app.config.get("REIFEN_CHECKOUT_CLOCK")
        value = clock() if callable(clock) else datetime.now(timezone.utc)
        if value.tzinfo is None:
            raise ValueError("Die Checkout-Uhr benötigt eine Zeitzone.")
        return value.astimezone(timezone.utc)

    def texts(self):
        value = self.app.config.get("REIFEN_CHECKOUT_TEXTE", {})
        return value if isinstance(value, dict) else {}

    def gate(self):
        try:
            loopback = ipaddress.ip_address(request.remote_addr or "").is_loopback
        except ValueError:
            loopback = False
        texts = self.texts()
        adapters = {key: self.app.config.get("REIFEN_CHECKOUT_" + key) for key in ("SMS", "RESERVIERUNG", "GATEWAY")}
        ready = (self.app.config.get("REIFEN_CHECKOUT_MODE") == "demo" and self.app.testing
                 and loopback and not self.p.USE_POSTGRES
                 and Path(self.p.DB).resolve().name.endswith(".reifen-demo.sqlite3")
                 and str(texts.get("version", "")).startswith("demo:")
                 and all(isinstance(texts.get(key), str) and texts[key].strip()
                         for key in ("agb", "widerruf", "datenschutz", "lieferung"))
                 and all(getattr(obj, "demo_only", False) is True for obj in adapters.values())
                 and callable(getattr(adapters["SMS"], "send", None))
                 and callable(getattr(adapters["RESERVIERUNG"], "reserve", None))
                 and all(callable(getattr(adapters["GATEWAY"], key, None)) for key in ("create", "retrieve")))
        return {"modus": "demo" if ready else "gesperrt", "demo": bool(ready),
                "zahlung_freigegeben": bool(ready),
                "grund": "DEMO – keine echte Bestellung, Zahlung, SMS oder Terminbestätigung." if ready
                else "Der Online-Bestellabschluss ist noch nicht freigeschaltet. Bitte senden Sie eine persönliche Reifenanfrage."}

    def require_demo(self):
        if not self.gate()["demo"]:
            abort(403, description="Der Bestellabschluss ist noch nicht freigeschaltet.")

    def owner(self, create=False):
        value = session.get("reifen_checkout_owner", "")
        if not value and create:
            value = secrets.token_urlsafe(32)
            session["reifen_checkout_owner"] = value
        return _hash(value) if value else ""

    def row(self, order_id):
        db = self.p.get_db()
        try:
            row = db.execute("SELECT * FROM reifen_bestellungen WHERE id=?", (order_id,)).fetchone()
        finally:
            db.close()
        if not row or not self.owner() or not hmac.compare_digest(row["owner_hash"], self.owner()):
            abort(404)
        return dict(row)

    def save(self, order_id, **fields):
        fields["geaendert_am"] = self.now().isoformat()
        db = self.p.get_db()
        try:
            db.execute("UPDATE reifen_bestellungen SET " + ",".join(key + "=?" for key in fields) + " WHERE id=?",
                       (*fields.values(), order_id))
            db.commit()
        finally:
            db.close()

    def fresh(self, token):
        selected = lade_auswahl_token(self.app.secret_key, token)
        result, status = suche_reifenangebote(selected["query"], self.app.config.get("REIFENANGEBOT_PROVIDER"),
                                            now=self.now(), artikel_id=selected["artikel_id"])
        product = next((p for p in result["produkte"] if p["artikel_id"] == selected["artikel_id"]), None)
        if status != 200 or not product:
            raise ValueError("Der ausgewählte Reifen ist nicht mehr zu diesem aktuellen Onlinepreis verfügbar. Bitte neu suchen.")
        return selected["query"], product

    def quote_hash(self, query, product):
        return _hash(_json({"query": query, "artikel_id": product["artikel_id"],
                           "bezeichnung": product["bezeichnung"], "groesse": product["groesse"], "preis": product["preis"]}))

    def order(self, row):
        query, product, customer = (json.loads(row[key]) for key in ("query_json", "produkt_json", "kunde_json"))
        reservation = json.loads(row["reservierung_json"])
        return {"id": row["id"], "status": row["status"], "kunde": customer, "produkt": product,
                # This table and every state in this module are exclusively
                # demo data, independent of later configuration changes.
                "demo": True,
                "query": query, "quote_hash": row["quote_hash"], "telefon_bestaetigt": bool(row["telefon_bestaetigt_am"]),
                "zahlungsart": row["zahlungsart"], "montage_datum": row["montage_datum"],
                "montage_uhrzeit": row["montage_uhrzeit"], "lieferdatum": reservation.get("lieferdatum", ""),
                "zahlung_status": row["payment_status"], "zahlung_url": row["payment_url"],
                "bestaetigt": row["status"] in _FINAL,
                "aenderbar": row["status"] in {"entwurf", "telefon_bestaetigt", "preis_geaendert"},
                "artikel_id": product["artikel_id"], "anzahl": int(query["anzahl"]),
                "amount_total": int(Decimal(product["preis"]["gesamt_brutto"]) * 100), "currency": "eur"}

    def render(self, *, token="", row=None, errors=None, status=200, formdata=None):
        if row:
            token = token or row["auswahl_token"]
            order = self.order(row)
            product, query = order["produkt"], order["query"]
        else:
            order = None
            query, product = self.fresh(token)
        early = bool(order and order["montage_datum"] and date.fromisoformat(order["montage_datum"])
                     < self.now().astimezone(ZoneInfo("Europe/Berlin")).date() + timedelta(days=14))
        earliest = ((date.fromisoformat(order["lieferdatum"]) + timedelta(days=1)).isoformat()
                    if order and order["lieferdatum"] else self.app.config.get("REIFEN_CHECKOUT_MONTAGE_AB", ""))
        times = []
        if query["montage"] == "ja" and earliest:
            try:
                minimum = date.fromisoformat(earliest)
                day = (formdata or {}).get("montage_datum") or earliest
                requested = date.fromisoformat(day)
                if requested < minimum:
                    requested = minimum
                for _ in range(8):
                    times = self.p.freie_reifen_slots(requested.isoformat())
                    if times:
                        break
                    requested += timedelta(days=1)
                earliest = requested.isoformat()
            except (ValueError, TypeError):
                earliest = ""
        checkout = self.gate()
        if order:
            checkout["demo"] = True
        return render_template("reifen_bestellen.html", auswahl_token=token, produkt=product, query=query,
            checkout=checkout, bestellung=order, rechtstexte=self.texts(), fehler=errors or [],
            formdata=formdata or {}, freie_termine=times, fruehestes_montagedatum=earliest,
            fruehe_montage=early, termine_nur_vorschau=True), status

    def limit(self, suffix):
        scope = "reifen-checkout:" + suffix
        limited, _ = self.p.public_form_rate_limit_status(scope)
        if limited:
            abort(429, description="Bitte warten Sie vor einem weiteren Versuch.")
        self.p.record_public_form_attempt(scope)

    def customer(self, form):
        customer = {}
        for key, maximum in (("name", 120), ("email", 200), ("telefon", 40), ("strasse", 160),
                             ("plz", 10), ("ort", 120), ("fahrzeug", 160), ("kennzeichen", 30)):
            text = str(form.get(key, ""))
            if len(text) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in text):
                raise ValueError("Bitte gültige Kontaktdaten ohne Steuerzeichen angeben.")
            customer[key] = text.strip()
        if any(not customer[key] for key in ("name", "email", "telefon", "strasse", "plz", "ort")):
            raise ValueError("Bitte Namen, E-Mail, Mobiltelefon und vollständige Rechnungsadresse angeben.")
        recipients = self.p.parse_email_recipients(customer["email"])
        if len(recipients) != 1 or recipients[0].lower() != customer["email"].lower():
            raise ValueError("Bitte genau eine gültige E-Mail-Adresse angeben.")
        customer["email"] = customer["email"].lower()
        customer["telefon"] = _phone(customer["telefon"])
        if not re.fullmatch(r"[0-9]{5}", customer["plz"]):
            raise ValueError("Bitte eine gültige fünfstellige Postleitzahl angeben.")
        return customer

    def otp_digest(self, row, code):
        text = row["id"] + "|" + row["telefon"] + "|" + code
        return hmac.new(str(self.app.secret_key).encode(), text.encode(), hashlib.sha256).hexdigest()

    def payment_state(self, row, status, payment_status):
        db = self.p.get_db()
        try:
            changed = db.execute("""UPDATE reifen_bestellungen SET status=?,payment_status=?,geaendert_am=?
                WHERE id=? AND status='zahlung_offen' AND payment_id=? AND quote_hash=?""",
                (status,payment_status,self.now().isoformat(),row["id"],row["payment_id"],row["quote_hash"])).rowcount
            db.commit()
            return changed
        finally:
            db.close()

    def reserve_slot(self, row, reservation):
        query = json.loads(row["query_json"])
        if query["montage"] != "ja":
            return None
        selected_day = date.fromisoformat(row["montage_datum"])
        delivery_day = date.fromisoformat(reservation["lieferdatum"])
        if selected_day <= delivery_day:
            raise ValueError("Der Montagewunsch muss nach dem bestätigten Lieferdatum liegen.")
        customer = json.loads(row["kunde_json"])
        lead_id = self.p.create_lead({"website": "auto-lackierzentrum", "quelle": "website", "status": "neu",
            "kunde_name": "DEMO – " + customer["name"], "kunde_email": customer["email"],
            "kontakt_telefon": customer["telefon"], "fahrzeug": customer["fahrzeug"], "kennzeichen": customer["kennzeichen"],
            "beschreibung": "DEMO Checkout-Terminhold, keine echte Bestellung oder Zahlung.",
            "portal_data": {"anliegen": "reifenbestellung", "demo": True, "bestellung_id": row["id"]}})
        self.save(row["id"], slot_lead_id=lead_id)
        db = self.p.get_db()
        try:
            self.p.reserviere_reifen_slot(db, lead_id, row["montage_datum"], row["montage_uhrzeit"])
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        return lead_id


def register_reifen_bestellung(portal):
    service = ReifenBestellung(portal)
    app = portal.app
    app.config.setdefault("REIFEN_CHECKOUT_MODE", "gesperrt")
    bp = Blueprint("reifen_bestellung", __name__)

    @bp.after_request
    def private(response):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    def go(order_id):
        return redirect(url_for("reifen_bestellung.uebersicht", bestell_id=order_id))

    @bp.get("/reifen/bestellen/<token>")
    def start(token):
        service.owner(create=True)
        try:
            return service.render(token=token)
        except ValueError as exc:
            abort(409, description=str(exc))

    @bp.post("/reifen/bestellen/<token>/entwurf")
    def entwurf(token):
        service.require_demo()
        service.limit("entwurf")
        try:
            query, product = service.fresh(token)
            customer = service.customer(request.form)
            payment = request.form.get("zahlungsart")
            if payment not in {"online", "vor_ort"}:
                raise ValueError("Bitte Onlinezahlung oder Zahlung vor Ort auswählen.")
            day, time = "", ""
            if query["montage"] == "ja":
                if request.form.get("standardmontage") != "ja":
                    raise ValueError("Bitte die Eignung für die angebotene Standardmontage bestätigen; Sonderarbeiten benötigen ein persönliches Angebot.")
                customer["standardmontage_bestaetigt"] = True
                day, time = request.form.get("montage_datum", ""), request.form.get("montage_uhrzeit", "")
                if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day) or time not in portal.reifen_slot_zeiten(day):
                    raise ValueError("Bitte einen gültigen Montagewunschtermin auswählen.")
            order_id = secrets.token_urlsafe(24)
            owner = service.owner(create=True)
            draft_key = _hash(_json([owner, token, customer, payment, day, time, service.quote_hash(query, product)]))
            now = service.now().isoformat()
            db = portal.get_db()
            try:
                db.execute("""INSERT INTO reifen_bestellungen
                    (id,owner_hash,draft_key,auswahl_token,query_json,produkt_json,quote_hash,kunde_json,telefon,
                     zahlungsart,montage_datum,montage_uhrzeit,erstellt_am,geaendert_am)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(draft_key) DO NOTHING""",
                    (order_id, owner, draft_key, token, _json(query), _json(product), service.quote_hash(query, product),
                     _json(customer), customer["telefon"], payment, day, time, now, now))
                found = db.execute("SELECT id,kunde_json,zahlungsart,montage_datum,montage_uhrzeit FROM reifen_bestellungen WHERE draft_key=?", (draft_key,)).fetchone()
                db.commit()
            finally:
                db.close()
            if (found["kunde_json"], found["zahlungsart"], found["montage_datum"], found["montage_uhrzeit"]) != (_json(customer), payment, day, time):
                raise ValueError("Diese Auswahl wurde bereits mit anderen Daten vorbereitet. Bitte neu suchen.")
            return go(found["id"])
        except ValueError as exc:
            try:
                return service.render(token=token, errors=[str(exc)], status=400, formdata=request.form)
            except ValueError:
                abort(409, description="Die Reifenauswahl muss neu geladen werden.")

    @bp.get("/reifen/bestellung/<bestell_id>")
    def uebersicht(bestell_id):
        return service.render(row=service.row(bestell_id))

    @bp.post("/reifen/bestellung/<bestell_id>/telefon-code")
    def telefon_code(bestell_id):
        service.require_demo()
        service.limit("telefon-code")
        row = service.row(bestell_id)
        if row["status"] not in {"entwurf", "telefon_bestaetigt", "preis_geaendert"}:
            abort(409, description="Die Telefonnummer kann in diesem Bestellschritt nicht geändert werden.")
        try:
            phone = _phone(request.form.get("telefon") or row["telefon"])
        except ValueError as exc:
            return service.render(row=row, errors=[str(exc)], status=400)
        code = ""
        try:
            if row["otp_sendungen"] >= 3 or (row["otp_gesendet_am"] and service.now() < _utc(row["otp_gesendet_am"]) + timedelta(seconds=60)):
                abort(429, description="Bitte warten Sie vor einem neuen Bestätigungscode.")
            if row["otp_versuche"] >= 5:
                abort(429, description="Zu viele Codeversuche. Bitte die Werkstatt kontaktieren.")
            customer = json.loads(row["kunde_json"])
            customer["telefon"] = phone
            row["telefon"] = phone
            factory = app.config.get("REIFEN_CHECKOUT_CODE_FACTORY")
            code = factory() if callable(factory) else f"{secrets.randbelow(1000000):06d}"
            if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
                raise ValueError("Code konnte nicht vorbereitet werden.")
            db = portal.get_db()
            try:
                claimed = db.execute("""UPDATE reifen_bestellungen SET telefon=?,kunde_json=?,telefon_bestaetigt_am='',
                    status='entwurf',otp_hash=?,otp_gueltig_bis=?,otp_sendungen=otp_sendungen+1,
                    otp_gesendet_am=?,geaendert_am=? WHERE id=? AND otp_sendungen<3 AND otp_versuche<5
                    AND otp_gesendet_am=? AND status IN ('entwurf','telefon_bestaetigt','preis_geaendert')""",
                    (phone, _json(customer), service.otp_digest(row, code), (service.now()+timedelta(minutes=5)).isoformat(),
                     service.now().isoformat(),service.now().isoformat(),bestell_id,row["otp_gesendet_am"])).rowcount
                db.commit()
            finally:
                db.close()
            if not claimed:
                abort(429, description="Bitte warten Sie vor einem neuen Code.")
            if app.config["REIFEN_CHECKOUT_SMS"].send(phone, code, bestell_id) is not True:
                raise ValueError("Der Demo-Code konnte nicht übergeben werden.")
            return go(bestell_id)
        except HTTPException:
            raise
        except Exception:
            # Do not leak adapter errors or retain a usable code after failure.
            db = portal.get_db()
            try:
                db.execute("""UPDATE reifen_bestellungen SET otp_hash='',otp_gueltig_bis=''
                    WHERE id=? AND status='entwurf' AND otp_hash=? AND telefon=?""",
                    (bestell_id, service.otp_digest(row, code) if isinstance(code, str) else "", phone))
                db.commit()
            finally:
                db.close()
            return service.render(row=service.row(bestell_id), errors=["Der Bestätigungscode konnte gerade nicht gesendet werden."], status=503)

    @bp.post("/reifen/bestellung/<bestell_id>/telefon-pruefen")
    def telefon_pruefen(bestell_id):
        service.require_demo()
        service.limit("telefon-pruefen")
        row = service.row(bestell_id)
        if row["status"] not in {"entwurf", "telefon_bestaetigt", "preis_geaendert"}:
            abort(409)
        code = request.form.get("code", "")
        if row["otp_versuche"] >= 5:
            abort(429, description="Zu viele Codeversuche.")
        good = (row["otp_hash"] and row["otp_gueltig_bis"] and _utc(row["otp_gueltig_bis"]) > service.now()
                and re.fullmatch(r"[0-9]{6}", code) and hmac.compare_digest(row["otp_hash"], service.otp_digest(row, code)))
        db = portal.get_db()
        try:
            extra = ",telefon_bestaetigt_am=?,status='telefon_bestaetigt',otp_hash='',otp_gueltig_bis=''" if good else ""
            args = [service.now().isoformat()] if good else []
            claimed = db.execute("UPDATE reifen_bestellungen SET otp_versuche=otp_versuche+1" + extra
                + " WHERE id=? AND otp_versuche<5 AND otp_hash=? AND telefon=? AND status IN ('entwurf','telefon_bestaetigt','preis_geaendert')",
                (*args,bestell_id,row["otp_hash"],row["telefon"])).rowcount
            db.commit()
        finally:
            db.close()
        if not claimed:
            abort(409, description="Der Code wurde bereits verarbeitet oder muss neu angefordert werden.")
        if not good:
            return service.render(row=service.row(bestell_id), errors=["Der Code ist ungültig oder abgelaufen."], status=400)
        return go(bestell_id)

    @bp.post("/reifen/bestellung/<bestell_id>/zahlung-vorbereiten")
    def zahlung_vorbereiten(bestell_id):
        service.require_demo()
        service.limit("abschluss")
        row = service.row(bestell_id)
        if row["status"] in _FINAL or row["status"] == "zahlung_offen":
            return go(bestell_id)
        if not row["telefon_bestaetigt_am"] or row["status"] not in {"telefon_bestaetigt", "preis_geaendert"}:
            abort(409, description="Bitte zuerst die Mobiltelefonnummer bestätigen.")
        if request.form.get("bestellpflicht") != "ja" or request.form.get("bedingungen") != "ja":
            abort(400, description="Bitte den Bestellinhalt und die Bedingungen ausdrücklich bestätigen.")
        if row["montage_datum"] and json.loads(row["kunde_json"]).get("standardmontage_bestaetigt") is not True:
            abort(400, description="Die Standardmontage wurde nicht bestätigt.")
        if row["montage_datum"] and date.fromisoformat(row["montage_datum"]) < service.now().astimezone(ZoneInfo("Europe/Berlin")).date() + timedelta(days=14):
            if request.form.get("frueher_montagebeginn") != "ja" or request.form.get("kenntnis_montagewiderruf") != "ja":
                abort(400, description="Bitte die gesonderten Erklärungen zum frühen Montagebeginn bestätigen.")
        try:
            query, product = service.fresh(row["auswahl_token"])
            fresh_hash = service.quote_hash(query, product)
            if request.form.get("quote_hash") != row["quote_hash"] or fresh_hash != row["quote_hash"]:
                db = portal.get_db()
                try:
                    db.execute("""UPDATE reifen_bestellungen SET produkt_json=?,quote_hash=?,status='preis_geaendert',geaendert_am=?
                        WHERE id=? AND status=? AND quote_hash=? AND telefon_bestaetigt_am=? AND telefon=?""",
                        (_json(product),fresh_hash,service.now().isoformat(),bestell_id,row["status"],row["quote_hash"],
                         row["telefon_bestaetigt_am"],row["telefon"]))
                    db.commit()
                finally:
                    db.close()
                return service.render(row=service.row(bestell_id), errors=["Preis oder Artikelangaben haben sich geändert. Bitte die neue Übersicht prüfen und erneut bestätigen."], status=409)
        except ValueError as exc:
            return service.render(row=service.row(bestell_id), errors=[str(exc)], status=409)
        db = portal.get_db()
        try:
            claimed = db.execute("""UPDATE reifen_bestellungen SET status='reservierung_laeuft',produkt_json=?,geaendert_am=?
                WHERE id=? AND status=? AND quote_hash=? AND telefon_bestaetigt_am=? AND telefon=?""",
                (_json(product),service.now().isoformat(),bestell_id,row["status"],row["quote_hash"],
                 row["telefon_bestaetigt_am"],row["telefon"])).rowcount
            db.commit()
        finally:
            db.close()
        if not claimed:
            return go(bestell_id)
        row["produkt_json"] = _json(product)
        try:
            order = service.order(row)
            reservation = app.config["REIFEN_CHECKOUT_RESERVIERUNG"].reserve(order)
            if (not isinstance(reservation, dict) or not isinstance(reservation.get("reservierung_id"), str)
                    or not reservation["reservierung_id"].startswith("demo_")
                    or reservation.get("artikel_id") != order["artikel_id"] or type(reservation.get("anzahl")) is not int
                    or reservation["anzahl"] != order["anzahl"]
                    or _utc(reservation.get("gueltig_bis", "")) <= service.now()
                    or date.fromisoformat(reservation.get("lieferdatum", "")) < service.now().date()):
                raise ValueError("Keine belastbare Demo-Lieferreservierung.")
            clean_reservation = {key: reservation[key] for key in ("reservierung_id", "artikel_id", "anzahl", "gueltig_bis", "lieferdatum")}
            service.save(bestell_id, reservierung_json=_json(clean_reservation),
                bedingungen_json=_json({"version": service.texts()["version"], "bestaetigt_am": service.now().isoformat(),
                    "bestellpflicht": True, "bedingungen": True,
                    "frueher_montagebeginn": request.form.get("frueher_montagebeginn") == "ja",
                    "kenntnis_montagewiderruf": request.form.get("kenntnis_montagewiderruf") == "ja"}))
            service.reserve_slot(row, clean_reservation)
            if row["zahlungsart"] == "vor_ort":
                service.save(bestell_id, status="demo_bestellt_zahlung_offen", payment_status="vor_ort_offen")
            else:
                payment = app.config["REIFEN_CHECKOUT_GATEWAY"].create(order)
                if not isinstance(payment, dict) or not re.fullmatch(r"demo_pay_[A-Za-z0-9_-]{1,100}", payment.get("id", "")):
                    raise ValueError("Ungültige Demo-Zahlungsreferenz.")
                payment_url = payment.get("url", "")
                if payment_url and payment_url != "/__preview/zahlung/" + payment["id"]:
                    raise ValueError("Nur eine lokale Demo-Zahlungsseite ist erlaubt.")
                service.save(bestell_id, status="zahlung_offen", payment_id=payment["id"], payment_url=payment_url,
                             payment_status="offen")
            return go(bestell_id)
        except Exception:
            # Reservation or payment may be uncertain: retain every hold until
            # verified/manual resolution, never release on a mere timeout.
            service.save(bestell_id, status="pruefung_noetig")
            return service.render(row=service.row(bestell_id), errors=["Der Demo-Abschluss muss geprüft werden. Es wird keine Zahlung oder Bestellung bestätigt."], status=409)

    @bp.post("/reifen/bestellung/<bestell_id>/zahlung-pruefen")
    def zahlung_pruefen(bestell_id):
        service.require_demo()
        service.limit("zahlung-pruefen")
        row = service.row(bestell_id)
        if row["status"] == "demo_bestaetigt":
            return go(bestell_id)
        if row["status"] != "zahlung_offen" or not row["payment_id"]:
            abort(409)
        try:
            payment = app.config["REIFEN_CHECKOUT_GATEWAY"].retrieve(row["payment_id"])
            order = service.order(row)
            if (not isinstance(payment, dict) or payment.get("id") != row["payment_id"] or payment.get("livemode") is not False
                    or type(payment.get("amount_total")) is not int or payment["amount_total"] != order["amount_total"]
                    or payment.get("currency") != "eur" or payment.get("reference") != bestell_id
                    or payment.get("quote_hash") != row["quote_hash"]):
                raise ValueError("Unstimmiger Zahlungsstatus.")
            if payment.get("status") == "succeeded":
                reservation = json.loads(row["reservierung_json"])
                if _utc(reservation.get("gueltig_bis", "")) <= service.now():
                    raise ValueError("Lieferreservierung muss nach Zahlung geprüft werden.")
                service.payment_state(row, "demo_bestaetigt", "demo_bezahlt")
            elif payment.get("status") != "open":
                raise ValueError("Ungeklärter Zahlungsstatus.")
            return go(bestell_id)
        except Exception:
            service.payment_state(row, "pruefung_noetig", "unklar")
            return service.render(row=service.row(bestell_id), errors=["Der Zahlungsstatus ist unklar. Es wird keine bezahlte Bestellung bestätigt."], status=409)

    @bp.get("/reifen/bestellung/<bestell_id>/erfolg")
    def erfolg(bestell_id):
        return service.render(row=service.row(bestell_id))

    app.register_blueprint(bp)
    app.extensions["reifen_bestellung"] = service
    return service
