"""Real-provider tire checkout, separate from the immutable offline demo.

No external call on import. New purchases need all configured integrations and
the reviewed terms hash. Existing payments and withdrawal receipts remain
processable while new purchases are disabled. Supplier operations are injected:
no undocumented TYREsystem URL or implicit stock reservation exists here.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import json
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException

from reifen_bestellung import _phone
from felgen_katalog import FelgenCatalogProvider
from raeder_auswahl import (FELGEN_TOKEN_PREFIX, RaederAuswahl, RaederAuswahlResolver,
                           erstelle_felgen_auswahl_token)
from reifen_rechtstexte import reifen_rechtstexte
from reifen_tyresystem_auto import (AutoBeschaffung, VERFAHREN as AUTO_VERFAHREN,
    AutoBeschaffungFehler, BACKUP_TABLES as AUTO_TABLES, init_schema as init_auto_schema, sha as auto_sha)


BACKUP_TABLES = ("reifen_kaeufe", "reifen_kauf_events", "reifen_kauf_sms_budget", *AUTO_TABLES)
SCHEMA_FEATURE = "reifen_kauf_v1"
EDITABLE = {"entwurf", "telefon_bestaetigt", "preis_geaendert"}
FINAL = {"bestaetigt", "bestellt_zahlung_offen"}
NOW = lambda: datetime.now(timezone.utc)


class NeueLieferzusage(ValueError):
    pass


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def aware(value):
    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Zeitzone fehlt.")
    return value.astimezone(timezone.utc)


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_kaeufe (
        id TEXT PRIMARY KEY, owner_hash TEXT NOT NULL, draft_key TEXT NOT NULL UNIQUE,
        mode TEXT NOT NULL, auswahl_token TEXT NOT NULL, query_json TEXT NOT NULL,
        produkt_json TEXT NOT NULL, quote_hash TEXT NOT NULL, kunde_json TEXT NOT NULL,
        telefon TEXT NOT NULL, telefon_bestaetigt_am TEXT NOT NULL DEFAULT '',
        sms_starts INTEGER NOT NULL DEFAULT 0, sms_checks INTEGER NOT NULL DEFAULT 0,
        sms_state TEXT NOT NULL DEFAULT '', zahlungsart TEXT NOT NULL,
        montage_datum TEXT NOT NULL DEFAULT '', montage_uhrzeit TEXT NOT NULL DEFAULT '', lieferdatum TEXT NOT NULL DEFAULT '',
        state TEXT NOT NULL DEFAULT 'entwurf', reservierung_json TEXT NOT NULL DEFAULT '{}',
        slot_lead_id INTEGER, payment_id TEXT UNIQUE, payment_intent TEXT UNIQUE, payment_url TEXT NOT NULL DEFAULT '',
        payment_status TEXT NOT NULL DEFAULT 'unbezahlt', payment_expires_at BIGINT,
        payment_attempt TEXT NOT NULL DEFAULT '1', payment_created_at TEXT NOT NULL DEFAULT '',
        rechtstexte_json TEXT NOT NULL, erklaerungen_json TEXT NOT NULL DEFAULT '{}',
        supplier_order_id TEXT UNIQUE, auftrag_id INTEGER UNIQUE, accepted_at TEXT NOT NULL DEFAULT '',
        erstellt_am TEXT NOT NULL, geaendert_am TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_kauf_events (
        event_id TEXT PRIMARY KEY, payment_id TEXT NOT NULL, order_id TEXT NOT NULL,
        verarbeitet_am TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_kauf_sms_budget (
        tag TEXT PRIMARY KEY, starts INTEGER NOT NULL DEFAULT 0
    )""")
    init_auto_schema(db)
    from tyresystem_order import init_schema as init_supplier_schema
    init_supplier_schema(db)


class ReifenKauf:
    def __init__(self, portal):
        self.p, self.app = portal, portal.app

    def now(self):
        clock = self.app.config.get("REIFEN_KAUF_CLOCK", NOW)
        value = clock()
        if value.tzinfo is None:
            raise ValueError("Zeitzone fehlt.")
        return value.astimezone(timezone.utc)

    def terms(self):
        provider = self.app.config.get("REIFEN_KAUF_RECHTSTEXTE", reifen_rechtstexte)
        return provider()

    def readiness(self):
        return self.readiness_for("reifen")

    def auto(self):
        return AutoBeschaffung(self)

    def auto_authorize_inquiry(self, sku, quantity):
        return self.auto().authorize_inquiry(sku, quantity)

    def auto_authorize_order(self, frozen_record):
        return self.auto().authorize_order(frozen_record)

    def auto_pending_ids(self, limit=20):
        return self.auto().pending_ids(limit)

    def readiness_for(self, produktart, *, verfahren=None):
        cfg = self.app.config
        mode = cfg.get("REIFEN_KAUF_MODE", "off")
        terms = self.terms()
        problems = []
        if produktart not in ("reifen", "felgen"):
            problems.append("Diese Produktart ist nicht für Bestellungen eingerichtet")
        scope = terms.get("produktarten")
        valid_scope = (isinstance(scope, list) and bool(scope) and all(type(item) is str
            and item in {"reifen", "felgen", "komplettraeder"} for item in scope) and len(scope) == len(set(scope)))
        if (scope is not None and (not valid_scope or produktart not in scope)) or (produktart == "felgen" and not valid_scope):
            problems.append("Bestellbedingungen gelten nicht für diese Produktart")
        if produktart == "felgen" and (terms.get("freigegeben") is not True or terms.get("status") != "freigegeben"
                or not isinstance(terms.get("version"), str) or not terms["version"]
                or terms["version"].startswith(("entwurf:", "demo:")) or terms.get("offene_entscheidungen")):
            problems.append("Freigegebene Felgenbedingungen fehlen")
        if getattr(self.p, "PUBLIC_SITE_ONLY", False):
            problems.append("Bestellungen benötigen die zentrale Portaldatenbank")
        if mode not in {"test", "live"} or cfg.get("REIFEN_KAUF_ENABLED") is not True:
            problems.append("Neubestellungen nicht aktiviert")
        if (not isinstance(terms.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", terms["sha256"])
                or cfg.get("REIFEN_TERMS_APPROVED_SHA256") != terms["sha256"]):
            problems.append("Bestellbedingungen noch nicht freigegeben")
        flow = verfahren if verfahren is not None else cfg.get("REIFEN_BESCHAFFUNG_VERFAHREN", "reservierung_v1")
        if flow == AUTO_VERFAHREN:
            problems.extend(self.auto().readiness(produktart))
        elif flow == "reservierung_v1":
            supplier = cfg.get("REIFEN_KAUF_SUPPLIER")
            if not all(callable(getattr(supplier, name, None)) for name in ("inspect", "reserve", "commit")):
                problems.append("Lieferanten- und Lieferreservierung fehlen")
            elif getattr(supplier, "livemode", None) is not (mode == "live"):
                problems.append("Lieferantenumgebung stimmt nicht überein")
        else:
            problems.append("Beschaffungsverfahren nicht eingerichtet")
        if produktart == "reifen" and not callable(cfg.get("REIFENANGEBOT_PROVIDER")):
            problems.append("Bestätigte Reifenpreise fehlen")
        if produktart == "felgen" and not isinstance(cfg.get("FELGENANGEBOT_PROVIDER"), FelgenCatalogProvider):
            problems.append("Bestätigte Felgenpreise fehlen")
        if not all(callable(getattr(cfg.get("REIFEN_KAUF_SMS"), name, None)) for name in ("start", "check")):
            problems.append("SMS-Verbindung fehlt")
        gateway = cfg.get("REIFEN_KAUF_GATEWAY")
        if not all(callable(getattr(gateway, name, None)) for name in ("create", "retrieve", "verify_webhook")):
            problems.append("Zahlungsverbindung fehlt")
        elif getattr(gateway, "livemode", None) is not (mode == "live"):
            problems.append("Zahlungsumgebung stimmt nicht überein")
        sms_limit = cfg.get("REIFEN_SMS_MAX_STARTS_PRO_TAG", 0)
        if cfg.get("REIFEN_SMS_KOSTEN_FREIGEGEBEN") is not True or type(sms_limit) is not int or not 0 < sms_limit <= 10000:
            problems.append("SMS-Kostenlimit noch nicht freigegeben")
        if not callable(cfg.get("REIFEN_KAUF_MAILER")) or cfg.get("REIFEN_KAUF_MAIL_ENABLED") is not True:
            problems.append("Vertrags-E-Mail noch nicht eingerichtet")
        if mode == "live" and (self.app.testing or not request.is_secure):
            problems.append("Liveabschluss benötigt HTTPS und Produktionsumgebung")
        if mode == "live" and (terms.get("freigegeben") is not True or terms.get("status") != "freigegeben"
                or str(terms.get("version", "")).startswith(("entwurf:","demo:")) or terms.get("offene_entscheidungen")):
            problems.append("Veröffentlichbare Bedingungsfassung fehlt")
        if mode == "live" and cfg.get("REIFEN_GEWAERLEISTUNG_NOTICE_READY") is not True:
            problems.append("Gewährleistungsmitteilung fehlt")
        if mode == "live":
            worker = self.app.extensions.get("reifen_kauf_worker_thread")
            stop = self.app.extensions.get("reifen_kauf_worker")
            if worker is None or not worker.is_alive() or stop is None or stop.is_set():
                problems.append("Zahlungsabgleich und Vertragsversand laufen noch nicht")
        return {"ready": not problems, "produktart": produktart, "mode": mode, "problems": problems}

    def require_ready(self, produktart="reifen"):
        if not self.readiness_for(produktart)["ready"]:
            abort(503, description="Der Bestellabschluss wird noch eingerichtet.")

    def require_row_ready(self, row):
        record = self.auto().record(row["id"])
        flow = AUTO_VERFAHREN if record else "reservierung_v1"
        kind = self.category(json.loads(row["query_json"]), json.loads(row["produkt_json"]))
        if self.readiness_for(kind, verfahren=flow)["ready"] is not True:
            abort(503, description="Der Bestellabschluss wird noch eingerichtet.")
        if row["mode"] != self.app.config.get("REIFEN_KAUF_MODE"):
            abort(409, description="Diese Bestellung gehört zu einer anderen Zahlungsumgebung.")

    def supplier(self, mode):
        supplier = self.app.config.get("REIFEN_KAUF_SUPPLIER")
        if supplier is None or getattr(supplier, "livemode", None) is not (mode == "live"):
            raise ValueError("Die Lieferantenverbindung gehört zu einer anderen Bestellumgebung.")
        return supplier

    def gateway(self, mode):
        gateway = self.app.config.get("REIFEN_KAUF_GATEWAY")
        if gateway is None or getattr(gateway, "livemode", None) is not (mode == "live"):
            raise ValueError("Die Zahlungsverbindung gehört zu einer anderen Bestellumgebung.")
        return gateway

    def owner(self, create=False):
        if create and not session.get("reifen_kauf_owner"):
            session["reifen_kauf_owner"] = secrets.token_urlsafe(32)
        return digest(session.get("reifen_kauf_owner", "")) if session.get("reifen_kauf_owner") else ""

    def row(self, order_id, *, private=True):
        db = self.p.get_db()
        try:
            found = db.execute("SELECT * FROM reifen_kaeufe WHERE id=?", (order_id,)).fetchone()
        finally:
            db.close()
        if not found or (private and (not self.owner() or not hmac.compare_digest(found["owner_hash"], self.owner()))):
            abort(404)
        return dict(found)

    def cas(self, row, **fields):
        fields["geaendert_am"] = self.now().isoformat()
        db = self.p.get_db()
        try:
            changed = db.execute("UPDATE reifen_kaeufe SET " + ",".join(k + "=?" for k in fields)
                + " WHERE id=? AND state=? AND quote_hash=? AND telefon_bestaetigt_am=? AND sms_state=?"
                + " AND payment_status=? AND COALESCE(payment_id,'')=? AND mode=? AND lieferdatum=?",
                (*fields.values(), row["id"], row["state"], row["quote_hash"], row["telefon_bestaetigt_am"], row["sms_state"],
                 row["payment_status"], row["payment_id"] or "", row["mode"], row["lieferdatum"])).rowcount
            db.commit()
            return changed == 1
        finally:
            db.close()

    @staticmethod
    def category(query, product=None):
        kind = query.get("produktart", "reifen")
        if kind == "reifen":
            if (set(query) != {"groesse", "saison", "qualitaet", "anzahl", "montage"}
                    or query.get("montage") not in {"ja", "nein"}
                    or type(query.get("anzahl")) is not str or not re.fullmatch(r"[1-8]", query["anzahl"])):
                raise ValueError("Die gespeicherte Artikelauswahl ist ungültig.")
        elif kind == "felgen":
            if set(query) != {"produktart", "anzahl"} or type(query.get("anzahl")) is not int or not 1 <= query["anzahl"] <= 8:
                raise ValueError("Die gespeicherte Artikelauswahl ist ungültig.")
            if product is not None and (any(key in product for key in ("groesse", "saison", "montage"))
                    or any(key in product.get("preis", {}) for key in ("montage_brutto", "gesamt_reifen_brutto"))):
                raise ValueError("Die gespeicherte Produktart stimmt nicht überein.")
        else:
            raise ValueError("Diese Produktart ist nicht für Bestellungen eingerichtet.")
        if product is not None and product.get("produktart", kind) != kind:
            raise ValueError("Die gespeicherte Produktart stimmt nicht überein.")
        return kind

    def quote_hash(self, query, product):
        kind = self.category(query, product)
        # Preserve hashes of existing tire drafts; their strict five-field query
        # can never be consumed as a rim selection.
        if kind == "reifen":
            return digest(encode({"query": query, "artikel_id": product["artikel_id"],
                "bezeichnung": product["bezeichnung"], "groesse": product["groesse"], "preis": product["preis"]}))
        selection = RaederAuswahl(kind, product["artikel_id"], query["anzahl"], None, query, product)
        return digest(encode(self.resolver().quote_identity(selection)))

    def resolver(self):
        return RaederAuswahlResolver(self.app.config, self.app.secret_key, clock=self.now)

    def fresh(self, token):
        kind = "felgen" if isinstance(token, str) and token.startswith(FELGEN_TOKEN_PREFIX) else "reifen"
        selection = self.resolver().from_token(token, produktart=kind)
        return self._selected(selection)

    @staticmethod
    def _selected(selection):
        product = selection.mutable_product()
        product["produktart"] = selection.produktart
        return selection.mutable_query(), product

    def _fresh_selection(self, query, artikel_id):
        return self._selected(self.resolver().refresh_owned(query, artikel_id))

    def fresh_order(self, row):
        # An owned persisted draft is the authority after the entry-link TTL.
        # Current SKU, stock and price still need the normal provider checks.
        query = json.loads(row["query_json"])
        product = json.loads(row["produkt_json"])
        self.category(query, product)
        artikel_id = product["artikel_id"]
        return self._fresh_selection(query, artikel_id)

    def limit(self, action):
        key = "reifen-kauf:" + action
        limited, _ = self.p.public_form_rate_limit_status(key)
        if limited:
            abort(429, description="Bitte warten Sie vor einem weiteren Versuch.")
        self.p.record_public_form_attempt(key)

    def customer(self, form):
        data = {}
        for key, maximum in (("name",120),("email",200),("telefon",40),("strasse",160),("plz",5),("ort",120),("fahrzeug",160),("kennzeichen",30)):
            value = form.get(key, "")
            if not isinstance(value, str) or len(value) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in value):
                raise ValueError("Bitte gültige Kontaktdaten angeben.")
            data[key] = value.strip()
        if any(not data[k] for k in ("name","email","telefon","strasse","plz","ort")):
            raise ValueError("Bitte alle Pflichtangaben ergänzen.")
        recipients = self.p.parse_email_recipients(data["email"])
        if len(recipients) != 1 or recipients[0].lower() != data["email"].lower():
            raise ValueError("Bitte eine gültige E-Mail-Adresse angeben.")
        data["email"], data["telefon"] = data["email"].lower(), _phone(data["telefon"])
        if not re.fullmatch(r"[0-9]{5}", data["plz"]):
            raise ValueError("Bitte eine gültige Postleitzahl angeben.")
        return data

    def order(self, row):
        product, query, customer, terms, reservation = (json.loads(row[k]) for k in
            ("produkt_json","query_json","kunde_json","rechtstexte_json","reservierung_json"))
        kind = self.category(query, product)
        if kind == "felgen" and (row["montage_datum"] or row["montage_uhrzeit"]):
            raise ValueError("Für diese Felgenbestellung ist keine Montage vereinbart.")
        sms_status = row["sms_state"] if row["sms_state"] in {
            "", "sending", "pending", "checking", "unknown", "limit", "approved"} else "unknown"
        if not row["telefon_bestaetigt_am"] and row["sms_checks"] >= 5:
            sms_status = "limit"
        edit_allowed = (row["state"] in EDITABLE and row["mode"] == self.app.config.get("REIFEN_KAUF_MODE")
            and row["mode"] in {"test", "live"} and not row["payment_id"] and row["payment_status"] == "unbezahlt"
            and not reservation and sms_status not in {"sending", "pending", "checking", "unknown", "limit"})
        result = {"id":row["id"], "produktart":kind, "status":row["state"], "kunde":customer, "produkt":product, "query":query,
            "demo":row["mode"] != "live", "mode":row["mode"], "quote_hash":row["quote_hash"],
            "telefon_bestaetigt":bool(row["telefon_bestaetigt_am"]), "sms_status":sms_status, "zahlungsart":row["zahlungsart"],
            "montage_datum":row["montage_datum"], "montage_uhrzeit":row["montage_uhrzeit"],
            "lieferdatum":row["lieferdatum"], "zahlung_status":row["payment_status"],
            "zahlung_url":row["payment_url"], "bestaetigt":row["state"] in FINAL,
            "aenderbar":row["state"] in EDITABLE, "bearbeitbar":edit_allowed, "artikel_id":product["artikel_id"],
            "anzahl":int(query["anzahl"]), "amount_total":int(Decimal(product["preis"]["gesamt_brutto"]) * 100),
            "currency":"eur", "payment_expires_at":row["payment_expires_at"],
            "payment_attempt":row["payment_attempt"], "rechtstexte":terms,
            "payment_created_at":int(aware(row["payment_created_at"]).timestamp()) if row["payment_created_at"] else None,
            "reservierung":reservation, "accepted_at":row["accepted_at"],
            "erklaerungen":json.loads(row["erklaerungen_json"]), "auftrag_id":row["auftrag_id"],
            "payment_id":row["payment_id"], "payment_intent":row["payment_intent"]}
        record = self.auto().record(row["id"])
        result["beschaffungsverfahren"] = AUTO_VERFAHREN if record else "reservierung_v1"
        if record:
            result["payment_capture_method"] = "manual"
            result["lieferprognose"] = record["snapshot"]["policy"]["lieferprognose"]
            result["bearbeitbar"] = result["bearbeitbar"] and record["state"] == "draft"
        return result

    def inspect(self, query, product):
        kind = self.category(query, product)
        source = self.supplier(self.app.config.get("REIFEN_KAUF_MODE"))
        if not callable(getattr(source, "inspect", None)):
            return None
        try:
            proof = source.inspect({"query":query, "produktart":kind, "anzahl":int(query["anzahl"]), "artikel_id":product["artikel_id"]})
        except Exception:
            raise ValueError("Die Lieferfreigabe konnte gerade nicht geprüft werden.") from None
        if (not isinstance(proof, dict) or proof.get("artikel_id") != product["artikel_id"]
                or proof.get("paketversandfaehig") is not True or not self.proof_category(proof, kind)):
            raise ValueError("Diese Artikelausführung ist noch nicht für die Onlinebestellung freigegeben.")
        try:
            delivery = proof.get("lieferdatum")
            if not isinstance(delivery, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", delivery):
                raise ValueError()
            delivery_day = date.fromisoformat(delivery)
        except (ValueError, TypeError):
            raise ValueError("Die Lieferfreigabe enthält kein gültiges Bereitstellungsdatum.") from None
        if delivery_day < self.now().astimezone(ZoneInfo("Europe/Berlin")).date():
            raise ValueError("Das bestätigte Lieferdatum ist nicht aktuell.")
        return proof

    @staticmethod
    def proof_category(proof, kind):
        # Historical tire adapters may omit the category. Rims always require
        # an explicit supplier acknowledgement and never a tire fallback.
        return proof.get("produktart", "reifen") == kind

    def early_mounting(self, row):
        if not row or not row["montage_datum"]:
            return False
        # This is the separately agreed service, whose withdrawal starts at
        # acceptance, not the goods' withdrawal period starting on receipt.
        latest_acceptance = self.now() + timedelta(hours=1)
        reservation = json.loads(row["reservierung_json"])
        if reservation.get("gueltig_bis"):
            latest_acceptance = max(latest_acceptance, aware(reservation["gueltig_bis"]))
        end = latest_acceptance.astimezone(ZoneInfo("Europe/Berlin")).date() + timedelta(days=14)
        while end.weekday() >= 5 or end in self.p.bw_feiertage(end.year):
            end += timedelta(days=1)
        return date.fromisoformat(row["montage_datum"]) <= end

    def render(self, *, token="", row=None, form=None, errors=None, status=200, selected=None):
        if row:
            token, order = row["auswahl_token"], self.order(row)
            query, product, terms = order["query"], order["produkt"], order["rechtstexte"]
        else:
            query, product = selected if selected is not None else self.fresh(token)
            order, terms = None, self.terms()
        kind = self.category(query, product)
        flow = order["beschaffungsverfahren"] if order else self.app.config.get("REIFEN_BESCHAFFUNG_VERFAHREN", "reservierung_v1")
        state = self.readiness_for(kind, verfahren=flow)
        if flow == AUTO_VERFAHREN and query.get("montage") == "ja":
            state = {**state, "ready":False}
        formdata = dict(form or {})
        display_errors = [errors] if isinstance(errors, str) else list(errors or [])
        if flow == AUTO_VERFAHREN and query.get("montage") == "ja":
            display_errors.append(str(AutoBeschaffungFehler("montage_nicht_freigegeben")))
        montage_errors = {}
        first_day, slots = "", []
        if flow != AUTO_VERFAHREN and query.get("montage") == "ja" and state["ready"] and not row:
            try:
                proof = self.inspect(query, product)
                minimum = date.fromisoformat(proof["lieferdatum"]) + timedelta(days=1)
                first_day = minimum.isoformat()
                if "montage_datum" in formdata:
                    submitted = formdata["montage_datum"]
                    if not isinstance(submitted, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", submitted):
                        raise ValueError("Montagedatum ungültig")
                    candidate = date.fromisoformat(submitted)
                    if candidate < minimum:
                        raise ValueError("Montagedatum vor Lieferung")
                    slots = self.p.freie_reifen_slots(submitted)
                    if not slots:
                        montage_errors["montage_datum"] = "Am gewählten Tag sind keine freien Montageterminzeiten verfügbar. Bitte wählen Sie einen anderen Tag."
                    elif formdata.get("montage_uhrzeit") and formdata["montage_uhrzeit"] not in slots:
                        montage_errors["montage_uhrzeit"] = "Die gewählte Uhrzeit ist an diesem Tag nicht verfügbar. Bitte wählen Sie eine angebotene Uhrzeit."
                else:
                    candidate = minimum
                    for _ in range(14):
                        slots = self.p.freie_reifen_slots(candidate.isoformat())
                        if slots:
                            formdata["montage_datum"] = candidate.isoformat()
                            break
                        candidate += timedelta(days=1)
            except (ValueError, TypeError):
                slots = []
                montage_errors["montage_datum"] = "Bitte ein gültiges Montagedatum nach der bestätigten Lieferung auswählen."
            except Exception:
                slots = []
                montage_errors["montage_datum"] = "Die Montagezeiten können gerade nicht geprüft werden. Bitte versuchen Sie es erneut."
        early = self.early_mounting(row)
        return render_template("reifen_bestellen.html", auswahl_token=token, produkt=product, query=query,
            produktart=kind,
            auto_beschaffung=flow == AUTO_VERFAHREN,
            lieferprognose=(order or {}).get("lieferprognose", self.app.config.get("REIFEN_TYRESYSTEM_AUTO_POLICY", {}).get("lieferprognose", "") if flow == AUTO_VERFAHREN else ""),
            bestellung=order, checkout_endpoint="reifen_kauf", checkout={"demo":(row["mode"] if row else state["mode"]) != "live",
                "zahlung_freigegeben":state["ready"], "grund":"Zahlung, SMS und Lieferantenverbindung werden noch eingerichtet."},
            formdata=formdata, fehler=display_errors, rechtstexte=terms, fruehe_montage=early, montage_fehler=montage_errors,
            fruehestes_montagedatum=first_day, freie_termine=slots, termine_nur_vorschau=True), status

    def start_sms(self, row):
        self.require_row_ready(row)
        if row["state"] not in EDITABLE or row["sms_starts"] >= 3 or row["sms_checks"] >= 5 or row["sms_state"] in {"unknown","sending","checking","limit"}:
            abort(409)
        if not self.cas(row, sms_state="sending", sms_starts=row["sms_starts"]+1, telefon_bestaetigt_am="", state="entwurf"):
            return
        db = self.p.get_db()
        try:
            day = self.now().astimezone(ZoneInfo("Europe/Berlin")).date().isoformat()
            db.execute("INSERT INTO reifen_kauf_sms_budget(tag,starts) VALUES(?,0) ON CONFLICT(tag) DO NOTHING", (day,))
            allowed = db.execute("UPDATE reifen_kauf_sms_budget SET starts=starts+1 WHERE tag=? AND starts<?",
                (day, self.app.config["REIFEN_SMS_MAX_STARTS_PRO_TAG"])).rowcount == 1
            db.commit()
        finally:
            db.close()
        if not allowed:
            current = self.row(row["id"])
            self.cas(current, sms_state="limit")
            abort(429)
        try:
            result = self.app.config["REIFEN_KAUF_SMS"].start(row["telefon"], row["id"])
            if not isinstance(result, dict) or result.get("status") != "pending":
                raise ValueError("Keine SMS-Bestätigung.")
            current = self.row(row["id"])
            if current["state"] == "entwurf" and current["sms_state"] == "sending":
                self.cas(current, sms_state="pending")
        except Exception:
            current = self.row(row["id"])
            if current["state"] == "entwurf" and current["sms_state"] == "sending":
                self.cas(current, sms_state="unknown")
            raise ValueError("Der SMS-Versand konnte nicht bestätigt werden. Bitte starten Sie keine weiteren Versuche.") from None

    def verify_sms(self, row, code):
        self.require_row_ready(row)
        if row["state"] not in EDITABLE or row["sms_state"] != "pending" or row["sms_checks"] >= 5:
            abort(409)
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4,10}", code):
            raise ValueError("Bitte den gültigen Bestätigungscode eingeben.")
        if not self.cas(row, sms_state="checking", sms_checks=row["sms_checks"]+1):
            return
        try:
            good = self.app.config["REIFEN_KAUF_SMS"].check(row["telefon"], code, row["id"])
        except Exception:
            good = False
        current = self.row(row["id"])
        if current["state"] == row["state"] and current["sms_state"] == "checking":
            self.cas(current, sms_state="approved" if good is True else "limit" if current["sms_checks"] >= 5 else "pending",
                telefon_bestaetigt_am=self.now().isoformat() if good is True else "",
                state="telefon_bestaetigt" if good is True else "entwurf")
        if good is not True:
            raise ValueError("Der Code ist ungültig oder abgelaufen.")

    def hold_slot(self, row, reservation):
        if self.category(json.loads(row["query_json"]), json.loads(row["produkt_json"])) == "felgen":
            if row["montage_datum"] or row["montage_uhrzeit"]:
                raise ValueError("Für Felgen ist keine Montage vereinbart.")
            return
        if not row["montage_datum"]:
            return
        if date.fromisoformat(row["montage_datum"]) <= date.fromisoformat(reservation["lieferdatum"]):
            raise ValueError("Der Montagetermin muss nach der bestätigten Reifenlieferung liegen.")
        customer = json.loads(row["kunde_json"])
        lead_id = self.p.create_lead({"website":"auto-lackierzentrum", "quelle":"website", "status":"neu",
            "kunde_name":customer["name"], "kunde_email":customer["email"], "kontakt_telefon":customer["telefon"],
            "fahrzeug":customer["fahrzeug"], "kennzeichen":customer["kennzeichen"],
            "beschreibung":"Reifenbestellung: Terminhold während des Bestellabschlusses; Zahlung prüfen.",
            "portal_data":{"anliegen":"reifenbestellung", "bestellung_id":row["id"], "zahlung_offen":True}})
        db = self.p.get_db()
        try:
            self.p.reserviere_reifen_slot(db, lead_id, row["montage_datum"], row["montage_uhrzeit"])
            db.execute("UPDATE reifen_kaeufe SET slot_lead_id=? WHERE id=? AND state='reservierung_laeuft'", (lead_id,row["id"]))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def finalize(self, row):
        if self.auto().record(row["id"]) is not None:
            raise ValueError("Der automatische Beschaffungsstand muss gesondert geprüft werden.")
        if row["state"] in FINAL:
            return
        expected_state = "zahlung_bestaetigt" if row["zahlungsart"] == "online" else "reserviert"
        if row["state"] != expected_state:
            raise ValueError("Bestellung ist noch nicht bereit.")
        if row["zahlungsart"] == "online" and (row["payment_status"] != "bezahlt" or not row["payment_intent"]):
            raise ValueError("Ein bestätigter Zahlungseingang fehlt.")
        if aware(json.loads(row["reservierung_json"])["gueltig_bis"]) <= self.now():
            raise ValueError("Die Lieferreservierung muss geprüft werden.")
        supplier = self.supplier(row["mode"])
        if not self.cas(row, state="lieferauftrag_laeuft"):
            return
        try:
            order = self.order(row)
            declarations = order["erklaerungen"]
            if self.early_mounting(row) and any(declarations.get(key) is not True for key in
                    ("frueher_montagebeginn", "kenntnis_montagewiderruf")):
                raise ValueError("Die Erklärungen zum frühen Montagebeginn müssen erneut geprüft werden.")
            receipt = supplier.commit(order)
            if (not isinstance(receipt, dict) or receipt.get("bestaetigt") is not True
                    or not isinstance(receipt.get("bestellnummer"), str) or not receipt["bestellnummer"]
                    or receipt.get("reference") != row["id"] or receipt.get("artikel_id") != order["artikel_id"]
                    or not self.proof_category(receipt, order["produktart"])
                    or type(receipt.get("anzahl")) is not int or receipt["anzahl"] != order["anzahl"]
                    or receipt.get("lieferdatum") != order["reservierung"].get("lieferdatum")
                    or receipt.get("reservierung_id") != order["reservierung"].get("reservierung_id")):
                raise ValueError("Lieferantenauftrag unklar.")
            db = self.p.get_db()
            try:
                stamp = self.now().isoformat()
                changed = db.execute("""UPDATE reifen_kaeufe SET state=?,supplier_order_id=?,accepted_at=?,
                    geaendert_am=? WHERE id=? AND state='lieferauftrag_laeuft'""",
                    ("bestaetigt" if row["zahlungsart"] == "online" else "bestellt_zahlung_offen",
                     receipt["bestellnummer"],stamp,stamp,row["id"])).rowcount
                if changed:
                    if row["montage_datum"]:
                        # A mere unpaid checkout hold is never a repair order.
                        # Only a successfully accepted purchase creates the
                        # scheduled tire order, with explicit tire classification.
                        customer = order["kunde"]
                        intake = {"anliegen":"reifenservice", "reifen_kauf_id":row["id"],
                            "reifen_kauf_demo":row["mode"] != "live",
                            "reifen_buchung_id":row["slot_lead_id"], "zahlung_status":row["payment_status"],
                            "reifen_kauf_produkt":order["produkt"], "reifen_kauf_preis":order["produkt"]["preis"],
                            "kunden_wunsch_annahme_datum":row["montage_datum"], "kunden_wunsch_zeitfenster":row["montage_uhrzeit"]}
                        workshop_order = self.p.create_auftrag("lead", kunde_name=customer["name"],
                            kunde_email=customer["email"], fahrzeug=customer["fahrzeug"] or "Reifenmontage",
                            kennzeichen=customer["kennzeichen"], kontakt_telefon=customer["telefon"],
                            beschreibung=f"{'TEST · ' if row['mode'] != 'live' else ''}Reifenbestellung {row['id']}: {order['anzahl']} × {order['produkt']['bezeichnung']}",
                            schadenart="Reifenservice", schaden_aufnahme_json=encode(intake),
                            annahme_datum=self.p.format_date(row["montage_datum"]), _db=db)
                        db.execute("UPDATE auftraege SET status=2,annahme_uhrzeit=? WHERE id=?", (row["montage_uhrzeit"],workshop_order))
                        db.execute("INSERT INTO status_log(auftrag_id,status,zeitstempel) VALUES(?,2,?)", (workshop_order,self.p.now_str()))
                        db.execute("UPDATE reifen_kaeufe SET auftrag_id=? WHERE id=?", (workshop_order,row["id"]))
                        db.execute("UPDATE leads SET auftrag_id=?,status='gewonnen',portal_json=? WHERE id=?",
                            (workshop_order,encode(intake),row["slot_lead_id"]))
                    from reifen_vertrag import enqueue
                    accepted = self.order(dict(db.execute("SELECT * FROM reifen_kaeufe WHERE id=?", (row["id"],)).fetchone()))
                    enqueue(db, accepted, "bestellbestaetigung")
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
        except Exception:
            current = self.row(row["id"], private=False)
            if current["state"] == "lieferauftrag_laeuft":
                self.cas(current, state="pruefung_noetig")
            raise ValueError("Die Bestellung muss geprüft werden. Bitte keine weitere Bestellung oder Zahlung starten.") from None

    def prepare_payment(self, row, form):
        if self.auto().record(row["id"]) is not None:
            return self.auto().prepare(row, form)
        self.require_row_ready(row)
        if row["state"] in FINAL or row["state"] == "zahlung_offen":
            return
        if row["state"] not in EDITABLE or not row["telefon_bestaetigt_am"] or row["sms_state"] != "approved":
            abort(409, description="Bitte zuerst die Telefonnummer bestätigen.")
        if form.get("bestellpflicht") != "ja" or form.get("bedingungen") != "ja":
            raise ValueError("Bitte Bestellinhalt und Bedingungen ausdrücklich bestätigen.")
        terms = json.loads(row["rechtstexte_json"])
        if terms.get("sha256") != self.terms().get("sha256"):
            raise ValueError("Die Bestellbedingungen wurden geändert. Bitte die Bestellung neu vorbereiten.")
        if self.early_mounting(row):
            if any(form.get(k) != "ja" for k in ("frueher_montagebeginn","kenntnis_montagewiderruf")):
                raise ValueError("Bitte die Erklärungen zum frühen Montagebeginn bestätigen.")
        existing_reservation = json.loads(row["reservierung_json"])
        known_hold = bool(existing_reservation and existing_reservation.get("quote_hash") == row["quote_hash"]
            and row["payment_expires_at"] and existing_reservation.get("gueltig_bis")
            and aware(existing_reservation["gueltig_bis"]).timestamp() >= row["payment_expires_at"]+300
            and aware(existing_reservation["gueltig_bis"]) > self.now())
        if known_hold:
            query, product = json.loads(row["query_json"]), json.loads(row["produkt_json"])
        else:
            query, product = self.fresh_order(row)
        current_hash = self.quote_hash(query, product)
        if form.get("quote_hash") != row["quote_hash"] or current_hash != row["quote_hash"]:
            self.cas(row, produkt_json=encode(product), quote_hash=current_hash, state="preis_geaendert")
            raise ValueError("Der Preis hat sich geändert. Bitte die neue Übersicht erneut bestätigen.")
        inspection = existing_reservation if known_hold else self.inspect(query, product)
        if not inspection:
            raise ValueError("Lieferfreigabe fehlt.")
        if inspection["lieferdatum"] != row["lieferdatum"]:
            self.cas(row, lieferdatum=inspection["lieferdatum"], state="preis_geaendert")
            raise ValueError("Der Bereitstellungstermin hat sich geändert. Bitte die aktualisierte Übersicht erneut bestätigen.")
        expiry = row["payment_expires_at"] if known_hold else int((self.now()+timedelta(minutes=45)).timestamp())
        declarations = {k:form.get(k)=="ja" for k in ("bestellpflicht","bedingungen","frueher_montagebeginn","kenntnis_montagewiderruf")}
        if not self.cas(row, state="reservierung_laeuft", payment_expires_at=expiry,
                payment_created_at=row["payment_created_at"] if known_hold else self.now().isoformat(), erklaerungen_json=encode(declarations)):
            return
        try:
            row = self.row(row["id"])
            order = self.order(row)
            reservation = existing_reservation if known_hold else self.supplier(row["mode"]).reserve(order)
            if (not isinstance(reservation, dict) or reservation.get("artikel_id") != order["artikel_id"]
                    or not self.proof_category(reservation, order["produktart"])
                    or type(reservation.get("anzahl")) is not int or reservation["anzahl"] != order["anzahl"]
                    or reservation.get("quote_hash") != row["quote_hash"]
                    or not isinstance(reservation.get("reservierung_id"), str) or not reservation["reservierung_id"]
                    or reservation.get("paketversandfaehig") is not True
                    or aware(reservation["gueltig_bis"]).timestamp() < expiry+300):
                raise ValueError("Lieferreservierung fehlt oder ist zu kurz gültig.")
            if date.fromisoformat(reservation["lieferdatum"]) < self.now().astimezone(ZoneInfo("Europe/Berlin")).date():
                raise ValueError("Das bestätigte Lieferdatum liegt in der Vergangenheit.")
            clean_reservation = {k:reservation[k] for k in
                ("artikel_id","anzahl","quote_hash","reservierung_id","paketversandfaehig","gueltig_bis","lieferdatum")}
            clean_reservation["produktart"] = order["produktart"]
            if reservation["lieferdatum"] != row["lieferdatum"]:
                current = self.row(row["id"])
                self.cas(current, reservierung_json=encode(clean_reservation), lieferdatum=reservation["lieferdatum"], state="preis_geaendert")
                raise NeueLieferzusage("Die verbindliche Lieferreservierung nennt einen anderen Bereitstellungstermin. Bitte die neue Übersicht prüfen und erneut bestätigen.")
            if self.early_mounting({**row, "reservierung_json":encode(clean_reservation)}) and any(
                    declarations.get(key) is not True for key in ("frueher_montagebeginn", "kenntnis_montagewiderruf")):
                current = self.row(row["id"])
                self.cas(current, reservierung_json=encode(clean_reservation), state="preis_geaendert")
                raise NeueLieferzusage("Durch das mögliche Annahmefenster kann die Montage vor Ablauf der Widerrufsfrist liegen. Bitte die Übersicht und die Erklärungen zum frühen Montagebeginn erneut prüfen.")
            self.hold_slot(row, reservation)
            current = self.row(row["id"])
            self.cas(current, reservierung_json=encode(clean_reservation), state="reserviert")
            current = self.row(row["id"])
            if row["zahlungsart"] == "vor_ort":
                self.cas(current, payment_status="vor_ort_offen")
                self.finalize(self.row(row["id"]))
            else:
                if not self.cas(current, state="zahlung_wird_angelegt"):
                    return
                payment = self.gateway(row["mode"]).create(self.order(self.row(row["id"])))
                current = self.row(row["id"])
                if current["state"] == "zahlung_wird_angelegt":
                    self.cas(current, state="zahlung_offen", payment_id=payment["id"], payment_url=payment["url"], payment_status="offen")
        except NeueLieferzusage:
            raise
        except Exception:
            current = self.row(row["id"])
            if current["state"] not in FINAL:
                self.cas(current, state="pruefung_noetig")
            raise ValueError("Der Abschluss konnte nicht sicher bestätigt werden. Bitte keine zweite Zahlung starten.") from None

    def reconcile(self, order_id):
        if self.auto().record(order_id) is not None:
            return self.auto().reconcile(order_id)
        row = self.row(order_id, private=False)
        if row["state"] in FINAL:
            return
        if row["state"] == "zahlung_bestaetigt":
            return self.finalize(row)
        if row["state"] != "zahlung_offen" or not row["payment_id"]:
            return
        gateway = self.gateway(row["mode"])
        try:
            payment = gateway.retrieve(row["payment_id"], expected_order=self.order(row))
            expected = self.order(row)
            if (not isinstance(payment, dict) or payment.get("id") != row["payment_id"]
                    or payment.get("livemode") is not (row["mode"] == "live")
                    or type(payment.get("amount_total")) is not int or payment["amount_total"] != expected["amount_total"]
                    or payment.get("currency") != "eur" or payment.get("reference") != row["id"]
                    or payment.get("quote_hash") != row["quote_hash"]):
                raise ValueError("Zahlungsreferenz stimmt nicht.")
            if payment.get("status") == "succeeded":
                if aware(json.loads(row["reservierung_json"])["gueltig_bis"]) <= self.now():
                    raise ValueError("Lieferreservierung abgelaufen.")
                intent = payment.get("payment_intent")
                if not isinstance(intent, str) or not re.fullmatch(r"pi_[A-Za-z0-9_]{1,200}", intent):
                    raise ValueError("Zahlungsbeleg fehlt.")
                if self.cas(row, state="zahlung_bestaetigt", payment_status="bezahlt", payment_intent=intent):
                    self.finalize(self.row(row["id"], private=False))
            elif payment.get("status") == "expired":
                # Supplier and calendar release require a verified cancellation
                # workflow. Never free a hold based solely on a local timestamp.
                self.cas(row, state="pruefung_noetig", payment_status="abgelaufen_unbezahlt")
            elif payment.get("status") not in {"open","processing"}:
                raise ValueError("Zahlungsstatus unklar.")
        except Exception:
            current = self.row(row["id"], private=False)
            if current["state"] == "zahlung_offen":
                self.cas(current, state="pruefung_noetig", payment_status="unklar")
            raise ValueError("Der Zahlungseingang muss geprüft werden.") from None

    def accept_auto_order(self, row, receipt):
        if row["state"] in FINAL:
            return
        record = self.auto().record(row["id"])
        if record is None or record["state"] not in {"acknowledged", "capturing"}:
            raise ValueError("Die bestätigte Lieferantenannahme fehlt.")
        order = self.auto().bound(row, record)
        if self.auto().receipt(record, order) != receipt:
            raise ValueError("Die Lieferantenannahme stimmt nicht überein.")
        if row["zahlungsart"] == "online" and (row["payment_status"] != "bezahlt" or not row["payment_intent"]):
            raise ValueError("Ein bestätigter Zahlungseingang fehlt.")
        db = self.p.get_db()
        try:
            stamp = self.now().isoformat()
            count = db.execute("UPDATE reifen_kaeufe SET state=?,supplier_order_id=?,accepted_at=?,geaendert_am=? "
                "WHERE id=? AND state='zahlung_offen' AND quote_hash=? AND mode=? AND accepted_at=''",
                ("bestaetigt" if row["zahlungsart"] == "online" else "bestellt_zahlung_offen",
                 receipt["bestellnummer"],stamp,stamp,row["id"],row["quote_hash"],row["mode"])).rowcount
            if count:
                from reifen_vertrag import enqueue
                accepted = self.order(dict(db.execute("SELECT * FROM reifen_kaeufe WHERE id=?", (row["id"],)).fetchone()))
                enqueue(db, accepted, "bestellbestaetigung")
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


def register_reifen_kauf(portal):
    service = ReifenKauf(portal)
    bp = Blueprint("reifen_kauf", __name__)

    @bp.after_request
    def private_headers(response):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    def go(order_id):
        return redirect(url_for("reifen_kauf.uebersicht", bestell_id=order_id))

    @bp.get("/reifen/bestellen/<token>/kauf")
    @bp.get("/raeder/bestellen/<token>/kauf", endpoint="einstieg")
    def start(token):
        service.limit("produkt")
        service.owner(create=True)
        try:
            return service.render(token=token)
        except ValueError:
            abort(409, description="Die Artikelauswahl ist nicht mehr verfügbar. Bitte neu suchen.")

    @bp.get("/reifen/kauf/<bestell_id>/bearbeiten")
    def bearbeiten(bestell_id):
        service.limit("bearbeiten")
        row = service.row(bestell_id)
        if not service.order(row)["bearbeitbar"]:
            abort(409, description="Diese Bestellung kann gerade nicht geändert werden. Bitte prüfen Sie den Bestellstatus oder wenden Sie sich an uns.")
        try:
            query, product = service.fresh_order(row)
            if service.category(query, product) == "felgen":
                token = erstelle_felgen_auswahl_token(portal.app.secret_key, product["artikel_id"], query["anzahl"], clock=service.now)
            else:
                from reifen_bestellung import erstelle_auswahl_token
                token = erstelle_auswahl_token(portal.app.secret_key, query, product["artikel_id"])
            form = {**json.loads(row["kunde_json"]), "zahlungsart":row["zahlungsart"],
                "montage_datum":row["montage_datum"], "montage_uhrzeit":row["montage_uhrzeit"],
                "standardmontage":"ja" if row["montage_datum"] else ""}
            return service.render(token=token, form=form, selected=(query, product))
        except (ValueError, TypeError, KeyError):
            abort(409, description="Die aktuelle Artikelauswahl konnte nicht geprüft werden. Bitte suchen Sie erneut.")

    @bp.post("/reifen/kauf/<token>/entwurf")
    def entwurf(token):
        kind = "felgen" if token.startswith(FELGEN_TOKEN_PREFIX) else "reifen"
        service.require_ready(kind)
        service.limit("entwurf")
        selected = None
        try:
            query, product = service.fresh(token)
            selected = (query, product)
            automatic = portal.app.config.get("REIFEN_BESCHAFFUNG_VERFAHREN", "reservierung_v1") == AUTO_VERFAHREN
            policy = service.auto().preflight(query, product)[0] if automatic else None
            inspection = {"lieferdatum":""} if automatic else service.inspect(query, product)
            customer, terms = service.customer(request.form), service.terms()
            payment = request.form.get("zahlungsart")
            if payment not in {"online","vor_ort"}:
                raise ValueError("Bitte eine Zahlungsart auswählen.")
            day, hour = "", ""
            if kind == "felgen" and any(request.form.get(key) for key in ("montage_datum", "montage_uhrzeit", "standardmontage")):
                raise ValueError("Für einzelne Felgen ist keine Montage vereinbart.")
            if query.get("montage") == "ja":
                day, hour = request.form.get("montage_datum", ""), request.form.get("montage_uhrzeit", "")
                if request.form.get("standardmontage") != "ja" or hour not in portal.reifen_slot_zeiten(day):
                    raise ValueError("Bitte einen passenden Montagetermin und die Standardmontage bestätigen.")
                if date.fromisoformat(day) <= date.fromisoformat(inspection["lieferdatum"]):
                    raise ValueError("Die Montage muss nach der bestätigten Lieferung liegen.")
            owner, stamp = service.owner(create=True), service.now().isoformat()
            order_id = secrets.token_urlsafe(24)
            quote_hash = service.quote_hash(query, product)
            key = digest(encode([owner,portal.app.config["REIFEN_KAUF_MODE"],token,customer,payment,day,hour,quote_hash,terms["sha256"],inspection["lieferdatum"]]))
            if automatic:
                key = digest(encode([key, AUTO_VERFAHREN, auto_sha(policy)]))
            db = portal.get_db()
            try:
                db.execute("""INSERT INTO reifen_kaeufe (id,owner_hash,draft_key,mode,auswahl_token,query_json,
                    produkt_json,quote_hash,kunde_json,telefon,zahlungsart,montage_datum,montage_uhrzeit,
                    rechtstexte_json,erstellt_am,geaendert_am,lieferdatum) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(draft_key) DO NOTHING""", (order_id,owner,key,portal.app.config["REIFEN_KAUF_MODE"],
                    token,encode(query),encode(product),quote_hash,encode(customer),customer["telefon"],payment,
                    day,hour,encode(terms),stamp,stamp,inspection["lieferdatum"]))
                found = db.execute("SELECT id FROM reifen_kaeufe WHERE draft_key=?", (key,)).fetchone()
                if automatic:
                    service.auto().insert(db, found["id"], query, product, quote_hash, terms, policy)
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
            return go(found["id"])
        except HTTPException:
            raise
        except Exception:
            if selected is None:
                abort(409, description="Die Artikelauswahl ist nicht mehr verfügbar. Bitte neu suchen.")
            return service.render(token=token, form=request.form, selected=selected,
                errors=["Bitte Kontaktdaten, Artikelfreigabe und gegebenenfalls den Termin prüfen."], status=400)

    @bp.get("/reifen/kauf/<bestell_id>")
    def uebersicht(bestell_id):
        return service.render(row=service.row(bestell_id))

    def run_action(bestell_id, action):
        service.limit(action)
        row = service.row(bestell_id)
        try:
            if action == "telefon-code": service.start_sms(row)
            elif action == "telefon-pruefen": service.verify_sms(row, request.form.get("code", ""))
            elif action == "zahlung-vorbereiten": service.prepare_payment(row, request.form)
            else: service.reconcile(bestell_id)
            return go(bestell_id)
        except HTTPException:
            raise
        except ValueError as exc:
            return service.render(row=service.row(bestell_id), errors=[str(exc)], status=409)

    @bp.post("/reifen/kauf/<bestell_id>/telefon-code")
    def telefon_code(bestell_id): return run_action(bestell_id, "telefon-code")

    @bp.post("/reifen/kauf/<bestell_id>/telefon-pruefen")
    def telefon_pruefen(bestell_id): return run_action(bestell_id, "telefon-pruefen")

    @bp.post("/reifen/kauf/<bestell_id>/zahlung-vorbereiten")
    def zahlung_vorbereiten(bestell_id): return run_action(bestell_id, "zahlung-vorbereiten")

    @bp.post("/reifen/kauf/<bestell_id>/zahlung-pruefen")
    def zahlung_pruefen(bestell_id): return run_action(bestell_id, "zahlung-pruefen")

    @bp.get("/reifen/zahlung/erfolg/<bestell_id>")
    @bp.get("/reifen/zahlung/abgebrochen/<bestell_id>")
    def rueckkehr(bestell_id):
        # A redirect and its query parameters are never proof of payment.
        return service.render(row=service.row(bestell_id))

    @bp.post("/webhooks/reifen/stripe")
    def webhook():
        if request.content_length and request.content_length > 262144:
            abort(413)
        gateway = portal.app.config.get("REIFEN_KAUF_GATEWAY")
        if gateway is None:
            abort(503)
        try:
            event = gateway.verify_webhook(request.get_data(cache=False), request.headers.get("Stripe-Signature", ""))
        except Exception:
            abort(400)
        if event.get("ignored"):
            return {"received":True}
        db = portal.get_db()
        try:
            duplicate = db.execute("SELECT event_id FROM reifen_kauf_events WHERE event_id=?", (event["event_id"],)).fetchone()
            row = db.execute("SELECT * FROM reifen_kaeufe WHERE payment_id=?", (event["session_id"],)).fetchone()
        finally:
            db.close()
        if duplicate:
            return {"received":True}
        if not row:
            # Session creation can race its first webhook. A provider retry or
            # the independent reconciliation worker must see the stored binding.
            abort(503)
        try:
            service.reconcile(row["id"])
        except ValueError:
            abort(503)
        db = portal.get_db()
        try:
            db.execute("INSERT INTO reifen_kauf_events VALUES(?,?,?,?) ON CONFLICT(event_id) DO NOTHING",
                (event["event_id"],event["session_id"],row["id"],service.now().isoformat()))
            db.commit()
        finally:
            db.close()
        return {"received":True}

    portal.app.register_blueprint(bp)
    portal.app.extensions["reifen_kauf"] = service
    return service
