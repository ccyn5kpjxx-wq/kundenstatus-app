"""Optional, versioned cost responsibility for existing workshop leads/orders.

No provider, invoice creation, notification, authentication or outbound transport.
The caller supplies an identity obtained from its authenticated server-side session;
form fields must never be used as an ``actor``. Guards take the caller's transaction.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from uuid import uuid4
from werkstatt_kundenflotten import _same_vehicle

BACKUP_TABLES = ("serviceprozesse", "serviceprozess_ereignisse")
SCHEMA_FEATURE = "serviceprozess_v1"
ROLES = {"werkstatt", "fuhrparkleitung", "fahrer"}


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS serviceprozesse (
        id TEXT PRIMARY KEY, lead_id INTEGER UNIQUE, auftrag_id INTEGER UNIQUE,
        firma_id TEXT NOT NULL DEFAULT '', fahrzeug_id TEXT NOT NULL DEFAULT '',
        parteien_json TEXT NOT NULL DEFAULT '{}', revision INTEGER NOT NULL DEFAULT 1,
        angebot_hash TEXT NOT NULL DEFAULT '', angebot_json TEXT NOT NULL DEFAULT '{}',
        freigabe_revision INTEGER, freigabe_hash TEXT NOT NULL DEFAULT '',
        freigabe_akteur TEXT NOT NULL DEFAULT '', freigabe_rolle TEXT NOT NULL DEFAULT '',
        freigabe_beleg TEXT NOT NULL DEFAULT '', freigabe_am TEXT NOT NULL DEFAULT '',
        erstellt_am TEXT NOT NULL, geaendert_am TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS serviceprozess_ereignisse (
        id TEXT PRIMARY KEY, prozess_id TEXT NOT NULL REFERENCES serviceprozesse(id),
        request_id TEXT UNIQUE, aktion TEXT NOT NULL, revision INTEGER NOT NULL,
        fingerprint TEXT NOT NULL, akteur TEXT NOT NULL, rolle TEXT NOT NULL,
        details_json TEXT NOT NULL, erstellt_am TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_serviceprozess_firma ON serviceprozesse(firma_id,fahrzeug_id)')


def _stamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value, maximum=200, required=False):
    value = str(value or "").strip()
    if len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError("Bitte Länge und Zeichen der Eingabe prüfen.")
    if required and not value:
        raise ValueError("Bitte alle Verantwortlichkeiten und Pflichtangaben ergänzen.")
    return value


def _money(value, *, allow_zero=False):
    raw = str("" if value is None else value).strip()
    if not re.fullmatch(r"\d{1,7}(?:[,.]\d{1,2})?", raw):
        raise ValueError("Bitte einen eindeutigen Betrag mit höchstens zwei Nachkommastellen eintragen.")
    try:
        amount = Decimal(raw.replace(",", "."))
        if not amount.is_finite() or amount < 0 or not allow_zero and amount == 0:
            raise ValueError()
        return int(amount * 100)
    except (InvalidOperation, ValueError):
        raise ValueError("Bitte einen gültigen Betrag eintragen.") from None


def _parties(data):
    if not isinstance(data, dict):
        raise ValueError("Bitte die Verantwortlichkeiten prüfen.")
    result = {}
    for key in ("auftraggeber", "freigebender", "rechnungsempfaenger", "zahler"):
        source = data.get(key)
        if not isinstance(source, dict):
            raise ValueError("Auftraggeber, Freigebender, Rechnungsempfänger und Zahler sind getrennt anzugeben.")
        result[key] = {"name": _text(source.get("name"), required=True)}
    result["freigebender"]["identity_id"] = _text(data["freigebender"].get("identity_id"), 100)
    recipient = data["rechnungsempfaenger"]
    for key in ("strasse", "plz", "ort"):
        result["rechnungsempfaenger"][key] = _text(recipient.get(key), required=True)
    country = _text(recipient.get("land") or "DE", 2)
    if not re.fullmatch("[A-Z]{2}", country):
        raise ValueError("Bitte einen gültigen Ländercode angeben.")
    result["rechnungsempfaenger"]["land"] = country
    result["zahlungsweg"] = _text(data.get("zahlungsweg"), 30)
    if result["zahlungsweg"] not in {"vor_ort", "firmenrechnung"}:
        raise ValueError("Zahlung vor Ort oder Firmenrechnung wählen. Onlinezahlung ist noch nicht eingerichtet.")
    result["fallart"] = _text(data.get("fallart") or "direkt", 20)
    if result["fallart"] not in {"direkt", "firma", "extern"}:
        raise ValueError("Bitte Direkt-, Firmen- oder extern gesteuerten Fall wählen.")
    amount = data.get("selbstbeteiligung")
    result["selbstbeteiligung_cent"] = None if amount in (None, "") else _money(amount, allow_zero=True)
    result["selbstbeteiligung_quelle"] = _text(data.get("selbstbeteiligung_quelle"), 500)
    if result["selbstbeteiligung_cent"] is not None and not result["selbstbeteiligung_quelle"]:
        raise ValueError("Zur Selbstbeteiligung bitte den geprüften Nachweis angeben.")
    return result


class ServiceProcess:
    def __init__(self, p, fleet):
        self.p, self.fleet = p, fleet

    @contextmanager
    def _write(self):
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            yield db

    @staticmethod
    def _row(db, case_id, *, lock=False):
        # Acquire the row's write lock before inspecting authority or snapshots.
        if lock:
            db.execute("UPDATE serviceprozesse SET revision=revision WHERE id=?", (case_id,))
        row = db.execute("SELECT * FROM serviceprozesse WHERE id=?", (case_id,)).fetchone()
        if not row:
            raise LookupError("Servicevorgang nicht gefunden.")
        return dict(row)

    @staticmethod
    def _authorize(row, actor, *, write=False):
        if not isinstance(actor, dict) or actor.get("role") not in ROLES or not _text(actor.get("id"), 100):
            raise PermissionError("Ein bestätigter persönlicher Zugang ist erforderlich.")
        if actor["role"] == "werkstatt":
            return
        if not row["firma_id"] or actor.get("company_id") != row["firma_id"]:
            raise PermissionError("Dieser Servicevorgang gehört nicht zu Ihrem Zugang.")
        if actor["role"] == "fahrer":
            # Caller derives this list from current assignments, never request data.
            if write or row["fahrzeug_id"] not in actor.get("vehicle_ids", ()):
                raise PermissionError("Für diesen Vorgang besteht keine Berechtigung.")

    def _fresh_actor(self, db, actor):
        # Root supplies the current access-module adapter. It must verify the
        # server principal's active/version/role/company against this DB, never
        # accept role fields posted by the browser. Workshop internal calls are
        # authenticated by Root before constructing their actor.
        checker=getattr(self.p,"service_actor_current",None)
        if checker is not None and (not callable(checker) or not checker(db,actor)):
            raise PermissionError("Der persönliche Zugang wurde geändert oder gesperrt. Bitte neu anmelden.")

    def _source(self, db, row, *, lock=False):
        if row["auftrag_id"] is not None:
            if lock: db.execute("UPDATE auftraege SET id=id WHERE id=?",(row["auftrag_id"],))
            source = db.execute("SELECT * FROM auftraege WHERE id=?", (row["auftrag_id"],)).fetchone()
            if not source:
                raise ValueError("Der zugehörige Werkstattauftrag fehlt.")
            source = dict(source)
            scope, price = source.get("werkstatt_angebot_text"), source.get("werkstatt_angebot_preis")
        else:
            if lock: db.execute("UPDATE leads SET id=id WHERE id=?",(row["lead_id"],))
            source = db.execute("SELECT * FROM leads WHERE id=?", (row["lead_id"],)).fetchone()
            if not source:
                raise ValueError("Die zugehörige Anfrage fehlt.")
            source = dict(source)
            scope, price = source.get("angebot_text"), source.get("angebot_preis")
            if not source.get("fin_nummer"):
                try: source["fin_nummer"] = json.loads(source.get("portal_json") or "{}").get("fin", "")
                except (ValueError, AttributeError): source["fin_nummer"] = ""
        if row["firma_id"]:
            vehicle=db.execute("SELECT * FROM kundenflotten_fahrzeuge WHERE id=?",(row["fahrzeug_id"],)).fetchone()
            if not vehicle or vehicle["firma_id"] != row["firma_id"] or not _same_vehicle(dict(vehicle),source):
                raise ValueError("Anfrage/Auftrag und Firmenfahrzeug müssen eindeutig dasselbe Fahrzeug betreffen.")
        scope = str(scope or "").strip()
        price = str(price or "").strip()
        if not scope or len(scope) > 10000 or source.get("angebot_status") not in {"angebot_abgegeben", "angenommen"}:
            raise ValueError("Zuerst ein vollständiges Angebot im bestehenden Vorgang erstellen.")
        # Strict interpretation: never guess whether a number includes tax.
        match = re.fullmatch(r"\s*(\d{1,7}(?:[,.]\d{1,2})?)\s*(?:€|EUR)?\s*(netto|brutto)?\s*", price, re.I)
        if not match:
            raise ValueError("Der Angebotspreis muss als eindeutiger EUR-Betrag mit netto oder brutto vorliegen.")
        try: rate = Decimal(str(getattr(self.p, "LEXWARE_TAX_RATE", 19)))
        except (ValueError,InvalidOperation): raise ValueError("Der Umsatzsteuersatz muss geprüft werden.") from None
        confirmed = {}
        if row["auftrag_id"] is not None:
            try: confirmed=json.loads(source.get("preisstand_json") or "{}").get("kunde") or {}
            except (ValueError,AttributeError): raise ValueError("Der bestätigte Kundenpreis muss geprüft werden.") from None
            if confirmed.get("netto"):
                try: rate=Decimal(str(confirmed.get("mwst_satz",rate)))
                except (ValueError,InvalidOperation): raise ValueError("Der Umsatzsteuersatz muss geprüft werden.") from None
        if not rate.is_finite() or rate not in {Decimal("0"), Decimal("7"), Decimal("19")}:
            raise ValueError("Der Umsatzsteuersatz muss geprüft werden.")
        amount = _money(match[1])
        if (match[2] or "netto").lower() == "brutto":
            net = int((Decimal(amount) / (1 + rate / 100)).quantize(Decimal("1"),rounding=ROUND_HALF_UP))
            gross = amount
        else:
            net = amount
            gross = int((Decimal(net) * (1 + rate / 100)).quantize(Decimal("1"),rounding=ROUND_HALF_UP))
        if confirmed.get("netto") and _money(confirmed["netto"]) != net:
            raise ValueError("Angebot und bestätigter Kundenpreis weichen ab. Bitte zuerst abgleichen.")
        return {"umfang": scope, "preis_original": price, "netto_cent": net,
                "mwst_satz": str(rate), "mwst_cent": gross-net, "brutto_cent": gross, "waehrung": "EUR"}

    def _snapshot(self, db, row, *, lock=False):
        if row["firma_id"]:
            if lock: db.execute("UPDATE kundenflotten_fahrzeuge SET id=id WHERE id=?",(row["fahrzeug_id"],))
            vehicle = db.execute("SELECT firma_id FROM kundenflotten_fahrzeuge WHERE id=?", (row["fahrzeug_id"],)).fetchone()
            if not vehicle or vehicle["firma_id"] != row["firma_id"]:
                raise ValueError("Die Firmen-/Fahrzeugzuordnung wurde verändert. Bitte zuerst prüfen.")
        parties = json.loads(row["parteien_json"])
        if not parties:
            raise ValueError("Bitte zuerst die Verantwortlichkeiten vollständig hinterlegen.")
        return {"firma_id": row["firma_id"], "fahrzeug_id": row["fahrzeug_id"],
                "parteien": parties, "angebot": self._source(db, row,lock=lock)}

    @staticmethod
    def _event(db, row, actor, action, details, request_id=None):
        db.execute('''INSERT INTO serviceprozess_ereignisse
            (id,prozess_id,request_id,aktion,revision,fingerprint,akteur,rolle,details_json,erstellt_am)
            VALUES (?,?,?,?,?,?,?,?,?,?)''', (uuid4().hex,row["id"],request_id,action,row["revision"],
            _hash(details),actor["id"],actor["role"],_json(details),_stamp()))

    def bind_case(self, *, lead_id=None, order_id=None, company_id="", vehicle_id="", actor):
        if (lead_id is None) == (order_id is None):
            raise ValueError("Genau eine bestehende Anfrage oder einen Auftrag zuordnen.")
        if actor.get("role") != "werkstatt":
            raise PermissionError("Die Werkstatt muss die Zuordnung prüfen.")
        probe = {"firma_id": company_id, "fahrzeug_id": vehicle_id}
        self._authorize(probe, actor, write=True)
        number = lead_id if lead_id is not None else order_id
        if type(number) is not int or number <= 0 or bool(company_id) != bool(vehicle_id):
            raise ValueError("Bitte eine gültige Vorgangs- und Fahrzeugzuordnung wählen.")
        with self._write() as db:
            self._fresh_actor(db,actor)
            column, table = ("lead_id", "leads") if lead_id is not None else ("auftrag_id", "auftraege")
            if not db.execute(f"SELECT id FROM {table} WHERE id=?", (number,)).fetchone():
                raise LookupError("Der bestehende Vorgang fehlt.")
            if company_id:
                vehicle = db.execute("SELECT firma_id FROM kundenflotten_fahrzeuge WHERE id=?", (vehicle_id,)).fetchone()
                if not vehicle or vehicle["firma_id"] != company_id:
                    raise ValueError("Fahrzeug und Firma stimmen nicht überein.")
            old = db.execute(f"SELECT * FROM serviceprozesse WHERE {column}=?", (number,)).fetchone()
            if old:
                if (old["firma_id"], old["fahrzeug_id"]) != (company_id, vehicle_id):
                    raise ValueError("Der Vorgang wurde bereits einer anderen Akte zugeordnet.")
                return old["id"]
            key = uuid4().hex
            db.execute('''INSERT INTO serviceprozesse(id,lead_id,auftrag_id,firma_id,fahrzeug_id,erstellt_am,geaendert_am)
                VALUES (?,?,?,?,?,?,?)''', (key,lead_id,order_id,company_id,vehicle_id,_stamp(),_stamp()))
            self._event(db,self._row(db,key),actor,"zugeordnet",{"lead_id":lead_id,"auftrag_id":order_id})
            return key

    def save_parties(self, case_id, data, actor, expected_revision):
        parties = _parties(data)
        with self._write() as db:
            row = self._row(db,case_id,lock=True)
            self._authorize(row,actor,write=True)
            self._fresh_actor(db,actor)
            if actor["role"] != "werkstatt":
                raise PermissionError("Die Rechnungs- und Vertragszuordnung pflegt die Werkstatt.")
            if type(expected_revision) is not int or row["revision"] != expected_revision:
                raise ValueError("Der Vorgang wurde geändert. Bitte neu laden.")
            if _json(parties) == row["parteien_json"]:
                return row["revision"]
            db.execute('''UPDATE serviceprozesse SET parteien_json=?,revision=revision+1,
                freigabe_revision=NULL,freigabe_hash='',freigabe_am='',geaendert_am=? WHERE id=?''',(_json(parties),_stamp(),case_id))
            row = self._row(db,case_id)
            self._event(db,row,actor,"verantwortlichkeiten",parties)
            return row["revision"]

    def sync_offer(self, case_id, actor, expected_revision=None):
        with self._write() as db:
            row = self._row(db,case_id,lock=True)
            self._authorize(row,actor,write=True)
            self._fresh_actor(db,actor)
            if actor["role"] != "werkstatt":
                raise PermissionError("Angebote werden von der Werkstatt bereitgestellt.")
            if expected_revision is not None and (type(expected_revision) is not int or row["revision"] != expected_revision):
                raise ValueError("Der Vorgang wurde geändert. Bitte neu laden.")
            snapshot = self._snapshot(db,row,lock=True)
            digest = _hash(snapshot)
            if digest != row["angebot_hash"]:
                db.execute('''UPDATE serviceprozesse SET angebot_json=?,angebot_hash=?,revision=revision+1,
                    freigabe_revision=NULL,freigabe_hash='',freigabe_am='',geaendert_am=? WHERE id=?''',(_json(snapshot),digest,_stamp(),case_id))
                row = self._row(db,case_id)
                self._event(db,row,actor,"angebot_version",snapshot)
            return {"revision":row["revision"],"snapshot_hash":digest,"snapshot":snapshot}

    def approve(self, case_id, actor, expected_revision, snapshot_hash, request_id, external_reference=""):
        if not isinstance(request_id,str) or not re.fullmatch(r"[a-f0-9]{32}",request_id):
            raise ValueError("Bitte das Freigabeformular neu laden.")
        reference = _text(external_reference,500)
        with self._write() as db:
            row = self._row(db,case_id,lock=True)
            self._authorize(row,actor,write=True)
            self._fresh_actor(db,actor)
            if actor["role"] == "werkstatt" and not reference:
                raise ValueError("Bitte den vorhandenen schriftlichen Freigabenachweis dokumentieren.")
            if actor["role"] not in {"werkstatt","fuhrparkleitung"}:
                raise PermissionError("Fahrer dürfen keine Kostenfreigabe erteilen.")
            snapshot = self._snapshot(db,row,lock=True)
            designated = snapshot["parteien"]["freigebender"]["identity_id"]
            if actor["role"] == "fuhrparkleitung" and designated and designated != actor["id"]:
                raise PermissionError("Die Kostenfreigabe liegt bei einer anderen berechtigten Person.")
            if snapshot["parteien"]["fallart"] == "extern" and actor["role"] != "werkstatt":
                raise PermissionError("Die schriftliche externe Freigabe muss von der Werkstatt geprüft werden.")
            digest = _hash(snapshot)
            details={"revision":expected_revision,"snapshot_hash":snapshot_hash,"external_reference":reference}
            previous=db.execute("SELECT * FROM serviceprozess_ereignisse WHERE request_id=?",(request_id,)).fetchone()
            if previous:
                if previous["prozess_id"] != case_id or previous["akteur"] != actor["id"] or previous["rolle"] != actor["role"] or previous["fingerprint"] != _hash(details):
                    raise ValueError("Das Formular wurde bereits für eine andere Freigabe verwendet.")
                # Return only if that historical approval is still valid today.
                self._approved(db,row)
                return row["revision"]
            if type(expected_revision) is not int or row["revision"] != expected_revision or digest != row["angebot_hash"] or digest != snapshot_hash:
                raise ValueError("Angebot oder Verantwortlichkeiten wurden geändert. Bitte neu prüfen.")
            db.execute('''UPDATE serviceprozesse SET freigabe_revision=revision,freigabe_hash=?,
                freigabe_akteur=?,freigabe_rolle=?,freigabe_beleg=?,freigabe_am=?,geaendert_am=? WHERE id=?''',
                (digest,actor["id"],actor["role"],reference,_stamp(),_stamp(),case_id))
            self._event(db,row,actor,"kosten_freigegeben",details,request_id)
            return row["revision"]

    def _approved(self, db, row, *, lock=False):
        if row["freigabe_revision"] != row["revision"] or not row["freigabe_am"] or _hash(self._snapshot(db,row,lock=lock)) != row["freigabe_hash"]:
            raise ValueError("Arbeit erst nach gültiger Kostenfreigabe für den aktuellen Umfang und Preis beginnen.")

    def assert_can_start(self, db, order_id):
        row=db.execute("SELECT id FROM serviceprozesse WHERE auftrag_id=?",(order_id,)).fetchone()
        if row:
            self._approved(db,self._row(db,row["id"],lock=True),lock=True)

    def assert_can_accept(self, db, *, lead_id=None, order_id=None, actor=None):
        column,number=("auftrag_id",order_id) if order_id is not None else ("lead_id",lead_id)
        row=db.execute(f"SELECT id FROM serviceprozesse WHERE {column}=?",(number,)).fetchone()
        if not row:
            return
        row=self._row(db,row["id"],lock=True)
        self._approved(db,row,lock=True)
        # Legacy core acceptance may consume an already approved snapshot; it
        # never creates an approval. Authenticated driver writes remain denied.
        if actor is not None:
            try:
                self._authorize(row,actor,write=True)
                self._fresh_actor(db,actor)
            except PermissionError as exc:
                raise ValueError(str(exc)) from None
            if actor["role"] not in {"werkstatt","fuhrparkleitung"}:
                raise ValueError("Annahme nur durch eine kostenberechtigte Person.")

    def approved_invoice_snapshot(self, db, order_id):
        """Read one complete, current approval in the caller's transaction.

        No second connection or UI projection is used. The process, current
        vehicle association and source order remain locked until the caller
        commits or rolls back. Invoice builders must use this snapshot for both
        scope and money, and may keep this transaction through provider creation
        so concurrent changes cannot mix a new scope with an old recipient.
        Historical orders without a managed service case return ``None``.
        """
        row=db.execute("SELECT id FROM serviceprozesse WHERE auftrag_id=?",(order_id,)).fetchone()
        if not row:
            return None
        row=self._row(db,row["id"],lock=True)
        snapshot=self._snapshot(db,row,lock=True)
        digest=_hash(snapshot)
        if row["freigabe_revision"] != row["revision"] or not row["freigabe_am"] or digest != row["freigabe_hash"]:
            raise ValueError("Rechnung erst nach gültiger Kostenfreigabe für den aktuellen Umfang, Preis und Rechnungsempfänger erstellen.")
        return {"case_id":row["id"],"revision":row["revision"],"snapshot_hash":digest,
                "snapshot":snapshot,"recipient":dict(snapshot["parteien"]["rechnungsempfaenger"])}

    def invoice_recipient(self, db, order_id):
        approved=self.approved_invoice_snapshot(db,order_id)
        return approved["recipient"] if approved else None

    def bind_order(self, lead_id, order_id, db=None):
        if db is None:
            with self._write() as own:
                return self.bind_order(lead_id,order_id,own)
        row=db.execute("SELECT id FROM serviceprozesse WHERE lead_id=?",(lead_id,)).fetchone()
        if not row:
            return None
        row=self._row(db,row["id"],lock=True)
        self._approved(db,row,lock=True)
        lead=db.execute("SELECT * FROM leads WHERE id=?",(lead_id,)).fetchone()
        if not lead or lead["auftrag_id"] != order_id:
            raise ValueError("Auftrag und Anfrage müssen bereits eindeutig verbunden sein.")
        if row["auftrag_id"] not in (None,order_id):
            raise ValueError("Die Anfrage ist bereits mit einem anderen Auftrag verbunden.")
        db.execute("UPDATE serviceprozesse SET auftrag_id=?,geaendert_am=? WHERE id=?",(order_id,_stamp(),row["id"]))
        self._approved(db,self._row(db,row["id"]),lock=True)
        if row["firma_id"]:
            previous=db.execute("SELECT fahrzeug_id FROM kundenflotten_auftraege WHERE auftrag_id=?",(order_id,)).fetchone()
            if previous and previous["fahrzeug_id"] != row["fahrzeug_id"]:
                raise ValueError("Der Werkstattauftrag gehört bereits zu einer anderen Fahrzeugakte.")
            if not previous:
                try: intake=json.loads(dict(lead).get("portal_json") or "{}")
                except ValueError: intake={}
                kind={"reifenservice":"reifen","reifen":"reifen","reifeneinlagerung":"reifen",
                      "bremsen":"bremsen","bremsenservice":"bremsen","inspektion":"service","mechanik":"service",
                      "oelwechsel":"oel","oelservice":"oel","hu":"hu","lackierung":"karosserie","unfallschaden":"karosserie",
                      "smart-repair":"karosserie","smartrepair":"karosserie"}.get(intake.get("anliegen"),"sonstiges")
                db.execute("INSERT INTO kundenflotten_auftraege (id,fahrzeug_id,auftrag_id,art,erstellt_am) VALUES (?,?,?,?,?)",(uuid4().hex,row["fahrzeug_id"],order_id,kind,_stamp()))
        return row["id"]

    def find(self, *, lead_id=None, order_id=None):
        column,number=("auftrag_id",order_id) if order_id is not None else ("lead_id",lead_id)
        with self.fleet.db() as db:
            row=db.execute(f"SELECT id FROM serviceprozesse WHERE {column}=?",(number,)).fetchone()
            return row["id"] if row else None

    def case_for_lead(self, lead_id):
        return self.find(lead_id=lead_id)

    def case_for_order(self, order_id):
        return self.find(order_id=order_id)

    def for_vehicle(self, company_id, vehicle_id, actor):
        self._authorize({"firma_id":company_id,"fahrzeug_id":vehicle_id},actor)
        with self.fleet.db() as db:
            rows=db.execute("SELECT id FROM serviceprozesse WHERE firma_id=? AND fahrzeug_id=? ORDER BY erstellt_am DESC,id",(company_id,vehicle_id)).fetchall()
        return [self.safe_case_view(row["id"],actor) for row in rows]

    def safe_case_view(self, case_id, actor):
        with self.fleet.db() as db:
            row=self._row(db,case_id)
            self._authorize(row,actor)
            try:
                self._approved(db,row)
                approved=True
            except ValueError:
                approved=False
            result={"id":row["id"],"lead_id":row["lead_id"],"order_id":row["auftrag_id"],
                    "company_id":row["firma_id"],"vehicle_id":row["fahrzeug_id"],
                    "freigegeben":approved,"status":"Kostenfreigabe liegt vor" if approved else "Kostenfreigabe offen",
                    "online_payment_enabled":False,"can_approve":False}
            if actor["role"] != "fahrer":
                snapshot=json.loads(row["angebot_json"])
                current=False
                try: current=_hash(self._snapshot(db,row)) == row["angebot_hash"] and bool(row["angebot_hash"])
                except ValueError: pass
                parties=json.loads(row["parteien_json"])
                designated=parties.get("freigebender",{}).get("identity_id","")
                may_approve=actor["role"] == "werkstatt" or (parties.get("fallart") != "extern" and (not designated or designated == actor["id"]))
                result.update(revision=row["revision"],snapshot_hash=row["angebot_hash"],snapshot=snapshot,
                              parteien=parties,angebot_aktuell=current,freigabe_am=row["freigabe_am"] if approved else "",
                              can_approve=current and not approved and may_approve)
            return result

    view = safe_case_view


def ensure_serviceprocess_for_import(*, get_db, export=None, imported_db=None):
    """Conservatively retain all responsibility revisions and approval history."""
    current=get_db()
    source=None
    try:
        if imported_db is not None:
            source=sqlite3.connect(Path(imported_db).resolve().as_uri()+"?mode=ro",uri=True)
            source.row_factory=sqlite3.Row
        for table in BACKUP_TABLES:
            rows=[dict(r) for r in current.execute(f"SELECT * FROM {table}").fetchall()]
            if not rows: continue
            incoming=[dict(r) for r in source.execute(f"SELECT * FROM {table}").fetchall()] if source else (export or {}).get("tables",{}).get(table,[])
            indexed={r.get("id"):r for r in incoming}
            if any(indexed.get(r["id"]) != r for r in rows):
                raise ValueError()
    except (ValueError,TypeError,KeyError,sqlite3.Error):
        raise ValueError("Import gestoppt: Service-Verantwortlichkeiten oder Freigaben würden zurückgesetzt. Bitte eine aktuelle vollständige Sicherung verwenden.") from None
    finally:
        current.close()
        if source is not None: source.close()
