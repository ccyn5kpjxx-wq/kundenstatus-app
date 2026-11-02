"""Explicit configuration of tire integrations; no provider calls on import.

Credentials are read only from REIFEN_* settings. No rental or insurance-mail
credential is copied. The supplier object is deliberately not invented here.
"""
from email.utils import parseaddr
import hashlib
import json
import os
from pathlib import Path
import threading

from flask import has_request_context, request

from reifen_rechtstexte import reifen_rechtstexte


def flag(value):
    return str(value).strip().lower() in {"1","true","yes","ja"}


def read_terms(path):
    """Read a reviewed immutable JSON snapshot, never execute a config file."""
    if not path:
        return reifen_rechtstexte()
    try:
        raw = Path(path).read_bytes()
        if len(raw) > 300000:
            raise ValueError()
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError()
        supplied = value.pop("sha256", "")
        canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        calculated = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if supplied != calculated or any(not isinstance(value.get(k), str) or not value[k].strip()
                for k in ("version","agb","widerruf","widerrufsformular","datenschutz","lieferung")):
            raise ValueError()
        value["sha256"] = calculated
        return value
    except Exception:
        # A bad file must never silently substitute different approved terms.
        raise ValueError("Die hinterlegte Bedingungsfassung konnte nicht unverändert geladen werden.") from None


def configure_integrations(portal, *, environment=None):
    env = os.environ if environment is None else environment
    cfg = portal.app.config
    errors = []
    mode = env.get("REIFEN_KAUF_MODE", "off")
    cfg["REIFEN_KAUF_MODE"] = mode
    cfg["REIFEN_KAUF_ENABLED"] = flag(env.get("REIFEN_KAUF_ENABLED", "0"))
    notice = Path(__file__).resolve().parent / "static" / "recht" / "reifen-gewaehrleistung-de.jpg"
    cfg["REIFEN_GEWAERLEISTUNG_NOTICE_READY"] = notice.is_file() and hashlib.sha256(notice.read_bytes()).hexdigest() == "00e2b5ecc4f86bed59cc1ead77db7e98a318f264df97d77a01b5a160dee93138"
    cfg["REIFEN_TERMS_APPROVED_SHA256"] = env.get("REIFEN_TERMS_APPROVED_SHA256", "")
    terms_path = env.get("REIFEN_TERMS_FILE", "")
    if terms_path:
        try:
            snapshot = read_terms(terms_path)
            cfg["REIFEN_KAUF_RECHTSTEXTE"] = lambda: json.loads(json.dumps(snapshot))
        except ValueError:
            errors.append("Bedingungsdatei ungültig")
            cfg["REIFEN_TERMS_APPROVED_SHA256"] = ""
    cfg["REIFEN_SMS_KOSTEN_FREIGEGEBEN"] = flag(env.get("REIFEN_SMS_KOSTEN_FREIGEGEBEN", "0"))
    try:
        cfg["REIFEN_SMS_MAX_STARTS_PRO_TAG"] = max(0, min(10000, int(env.get("REIFEN_SMS_MAX_STARTS_PRO_TAG", "0"))))
    except (ValueError, TypeError):
        cfg["REIFEN_SMS_MAX_STARTS_PRO_TAG"] = 0
    sms_test_numbers = {v.strip() for v in env.get("REIFEN_SMS_TESTNUMMERN", "").split(",") if v.strip()}

    def sms_authorized(operation, reference):
        if not has_request_context() or request.method != "POST" or operation not in {"start","check"}:
            return False
        if request.endpoint != {"start":"reifen_kauf.telefon_code", "check":"reifen_kauf.telefon_pruefen"}.get(operation):
            return False
        service = portal.app.extensions.get("reifen_kauf")
        if service is None or not service.readiness()["ready"]:
            return False
        row = service.row(reference)
        if row["mode"] != cfg.get("REIFEN_KAUF_MODE"):
            return False
        return row["mode"] == "live" or row["telefon"] in sms_test_numbers

    if any(env.get(key) for key in ("REIFEN_TWILIO_ACCOUNT_SID","REIFEN_TWILIO_AUTH_TOKEN","REIFEN_TWILIO_VERIFY_SERVICE_SID")):
        try:
            from reifen_sms import TwilioVerifySMS
            cfg["REIFEN_KAUF_SMS"] = TwilioVerifySMS.from_env(portal.get_db, environment=env, authorize=sms_authorized)
        except ValueError:
            errors.append("SMS-Zugang unvollständig")
    if mode in {"test","live"} and any(env.get(k) for k in ("REIFEN_STRIPE_SECRET_KEY","REIFEN_STRIPE_WEBHOOK_SECRET")):
        try:
            from reifen_zahlung import StripeReifenGateway
            settings = {key:value for key,value in env.items() if key.startswith("REIFEN_STRIPE_")}
            settings["REIFEN_STRIPE_MODE"] = mode
            settings["REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS"] = [v.strip() for v in env.get(
                "REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS", "auto-lackierzentrum.de").split(",") if v.strip()]
            settings.setdefault("REIFEN_STRIPE_PUBLIC_ORIGIN", "https://auto-lackierzentrum.de")
            cfg["REIFEN_KAUF_GATEWAY"] = StripeReifenGateway(settings)
        except ValueError:
            errors.append("Stripe-Zugang oder Rückleitung unvollständig")

    cfg["REIFEN_SMTP_FROM"] = env.get("REIFEN_SMTP_FROM", "info@auto-lackierzentrum.de")
    mail_requested = flag(env.get("REIFEN_KAUF_MAIL_ENABLED", "0"))
    mail_ready = all(env.get(k) for k in ("REIFEN_SMTP_HOST","REIFEN_SMTP_USER","REIFEN_SMTP_PASSWORD"))
    cfg["REIFEN_KAUF_MAIL_ENABLED"] = mail_requested and mail_ready
    cfg["REIFEN_VERTRAG_MAIL_FREIGABE"] = lambda: cfg.get("REIFEN_KAUF_MAIL_ENABLED") is True
    test_recipients = {v.strip().lower() for v in env.get("REIFEN_MAIL_TESTEMPFAENGER", "").split(",") if v.strip()}
    if mail_ready:
        try:
            port = int(env.get("REIFEN_SMTP_PORT", "465"))
            if not 1 <= port <= 65535:
                raise ValueError()
            tls_mode = env.get("REIFEN_SMTP_TLS_MODE", "ssl")
            if tls_mode not in {"ssl","starttls"}:
                raise ValueError()
            settings = {"smtp_host":env["REIFEN_SMTP_HOST"], "smtp_port":port,
                "smtp_ssl":tls_mode=="ssl", "smtp_tls":tls_mode=="starttls",
                "smtp_user":env["REIFEN_SMTP_USER"], "_smtp_password":env["REIFEN_SMTP_PASSWORD"],
                "from_address":cfg["REIFEN_SMTP_FROM"]}

            def mailer(message):
                if cfg.get("REIFEN_KAUF_MAIL_ENABLED") is not True:
                    return "not_sent"
                recipient = parseaddr(message.get("To", ""))[1].lower()
                if mode != "live" and recipient not in test_recipients:
                    return "not_sent"
                from mos_contract_delivery import _smtp_send
                return _smtp_send(message, settings)

            cfg["REIFEN_KAUF_MAILER"] = mailer
        except (ValueError, TypeError):
            cfg["REIFEN_KAUF_MAIL_ENABLED"] = False
            errors.append("Sichere SMTP-Konfiguration fehlt")
    elif mail_requested:
        errors.append("Vertragsmail-Zugang fehlt")
    cfg["REIFEN_ANBINDUNG_ERRORS"] = errors
    # A supplier and its real catalog/stock/order protocol must be wired after
    # TYREsystem supplies authorized current documentation. No placeholder.
    return errors


def run_pending(portal):
    """One bounded reconciliation/mail run; never starts an SMS or a purchase."""
    service = portal.app.extensions.get("reifen_kauf")
    if service is None:
        return
    db = portal.get_db()
    try:
        ids = [row["id"] for row in db.execute("SELECT id FROM reifen_kaeufe WHERE state IN ('zahlung_offen','zahlung_bestaetigt') ORDER BY erstellt_am LIMIT 20").fetchall()]
    finally:
        db.close()
    for order_id in ids:
        try:
            service.reconcile(order_id)
        except Exception:
            # The durable order status exposes review needs; no provider detail
            # or customer contact is logged by the background runner.
            continue
    mailer = portal.app.config.get("REIFEN_KAUF_MAILER")
    if callable(mailer):
        from reifen_vertrag import deliver_pending
        deliver_pending(portal, mailer, limit=10)


def start_worker(portal):
    if not flag(os.environ.get("REIFEN_KAUF_WORKER_ENABLED", "0")):
        return None
    if portal.app.extensions.get("reifen_kauf_worker"):
        return portal.app.extensions["reifen_kauf_worker"]
    stop = threading.Event()

    def worker():
        while not stop.wait(30):
            try:
                run_pending(portal)
            except Exception:
                continue

    thread = threading.Thread(target=worker, name="reifen-kauf-abgleich", daemon=True)
    portal.app.extensions["reifen_kauf_worker"] = stop
    portal.app.extensions["reifen_kauf_worker_thread"] = thread
    thread.start()
    return stop
