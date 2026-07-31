"""Isolierter Test fuer SMTP-Versand plus IMAP-Kopie im Gesendet-Ordner."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TEMP_DIR = Path(tempfile.mkdtemp(prefix="lead_mail_sent_copy_"))
sys.path.insert(0, str(ROOT))
os.environ.update(
    {
        "RENDER": "local-lead-mail-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "FLASK_SECRET_KEY": "lead-mail-test",
        "ADMIN_PASS": "lead-mail-test",
        "SCHADEN_MAIL_ADDRESS": "",
        "SCHADEN_SMTP_HOST": "",
        "SCHADEN_SMTP_USER": "",
        "SCHADEN_SMTP_PASS": "",
        "SCHADEN_IMAP_HOST": "",
        "SCHADEN_IMAP_USER": "",
        "SCHADEN_IMAP_PASS": "",
    }
)

import app as portal  # noqa: E402


FEHLER = []


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    if not passed:
        FEHLER.append(label)


class FakeSMTP:
    instances = []

    def __init__(self, host, port, local_hostname=None, timeout=None):
        self.host = host
        self.port = port
        self.local_hostname = local_hostname
        self.timeout = timeout
        self.started_tls = False
        self.login_data = None
        self.message = None
        self.__class__.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def starttls(self):
        self.started_tls = True

    def login(self, user, password):
        self.login_data = (user, password)

    def send_message(self, message):
        self.message = message


class FakeIMAP:
    instances = []
    append_status = "OK"

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.login_data = None
        self.append_data = None
        self.logged_out = False
        self.__class__.instances.append(self)

    def login(self, user, password):
        self.login_data = (user, password)
        return "OK", []

    def list(self):
        return "OK", [b'(\\HasNoChildren \\Sent) "/" "Sent"']

    def append(self, folder, flags, date_time, raw_message):
        self.append_data = (folder, flags, date_time, raw_message)
        return self.__class__.append_status, []

    def logout(self):
        self.logged_out = True
        return "BYE", []


class FakeDB:
    calls = []

    def execute(self, sql, params=()):
        self.__class__.calls.append((sql, params))
        return self

    def commit(self):
        return None

    def close(self):
        return None


def mail_config():
    return {
        "website": "auto-lackierzentrum",
        "address": "info@auto-lackierzentrum.de",
        "display_name": "Gaertner Karosserie und Lack",
        "host": "smtp.example.test",
        "port": 587,
        "user": "info@auto-lackierzentrum.de",
        "password": "smtp-secret",
        "ssl": False,
        "tls": True,
        "configured": True,
        "imap_host": "imap.example.test",
        "imap_port": 993,
        "imap_user": "info@auto-lackierzentrum.de",
        "imap_password": "imap-secret",
        "imap_ssl": False,
        "imap_sent_folder": "",
        "imap_timeout": 20,
        "sent_copy_configured": True,
        "uses_shared_login": False,
    }


def main():
    originals = {
        "smtp": portal.smtplib.SMTP,
        "imap": portal.imaplib.IMAP4,
        "config": portal.get_lead_mail_config,
        "base_config": portal.get_schaden_mail_config,
        "db": portal.get_db,
        "log": portal.log_lead_mail,
    }
    mail_logs = []
    portal.get_schaden_mail_config = lambda: {
        "address": "info@auto-lackierzentrum.de",
        "display_name": "Gaertner Karosserie und Lack",
        "smtp_host": "smtp.ionos.de",
        "smtp_port": 587,
        "smtp_user": "info@auto-lackierzentrum.de",
        "smtp_ssl": False,
        "smtp_tls": True,
        "imap_host": "",
        "imap_port": 993,
        "imap_user": "",
        "imap_ssl": True,
        "imap_sent_folder": "",
        "imap_timeout": 20,
        "_smtp_password": "shared-mail-secret",
        "_imap_password": "",
    }
    derived_config = portal.get_lead_mail_config("auto-lackierzentrum")
    check(
        "IONOS-IMAP wird ohne weitere Render-Secrets aus SMTP abgeleitet",
        derived_config["imap_host"] == "imap.ionos.de"
        and derived_config["imap_user"] == "info@auto-lackierzentrum.de"
        and derived_config["imap_password"] == "shared-mail-secret"
        and derived_config["sent_copy_configured"],
    )
    portal.smtplib.SMTP = FakeSMTP
    portal.imaplib.IMAP4 = FakeIMAP
    portal.get_lead_mail_config = lambda _website: mail_config()
    portal.get_db = lambda: FakeDB()
    portal.log_lead_mail = lambda *args: mail_logs.append(args)
    lead = {"id": 12, "website": "auto-lackierzentrum"}
    try:
        FakeIMAP.append_status = "OK"
        success = portal.send_lead_email(
            lead,
            "kunde@example.test",
            "Reparaturfreigabe",
            "Guten Tag, die Reparaturfreigabe liegt vor.",
        )
        smtp = FakeSMTP.instances[-1]
        imap = FakeIMAP.instances[-1]
        appended = imap.append_data
        raw_message = appended[3] if appended else b""
        check("SMTP-Nachricht wurde versendet", smtp.message is not None)
        check("Antwortadresse zeigt auf das sichtbare IONOS-Postfach", smtp.message["Reply-To"] == "info@auto-lackierzentrum.de")
        check("TLS und SMTP-Login wurden verwendet", smtp.started_tls and smtp.login_data == ("info@auto-lackierzentrum.de", "smtp-secret"))
        check("IMAP-Ordner mit \\Sent-Kennzeichnung wurde gefunden", appended and appended[0] == "Sent")
        check("Gesendet-Kopie ist als gelesen markiert", appended and appended[1] == r"(\Seen)")
        check("Gesendet-Kopie ist dieselbe MIME-Nachricht", b"Subject: Reparaturfreigabe" in raw_message and b"Reply-To: info@auto-lackierzentrum.de" in raw_message)
        check("Erfolgreiche Kopie wird im Ergebnis gemeldet", success["sent_folder"] == "Sent" and not success["sent_copy_error"])
        check("IMAP-Verbindung wird sauber geschlossen", imap.logged_out)
        check("Versandprotokoll bleibt erfolgreich", mail_logs[-1][5] == "gesendet" and not mail_logs[-1][6])

        FakeIMAP.append_status = "NO"
        failure = portal.send_lead_email(
            lead,
            "kunde@example.test",
            "Zweiter Test",
            "Diese Nachricht prueft einen Fehler bei der IMAP-Kopie.",
        )
        check("SMTP-Erfolg bleibt trotz IMAP-Fehler erhalten", FakeSMTP.instances[-1].message is not None and failure["address"] == "info@auto-lackierzentrum.de")
        check("IMAP-Fehler wird ohne Doppelversand-Risiko gemeldet", bool(failure["sent_copy_error"]) and mail_logs[-1][5] == "gesendet" and "Kopie" in mail_logs[-1][6])
        check("Lead-Versandzeit wird weiterhin gespeichert", len(FakeDB.calls) == 2)
    finally:
        portal.smtplib.SMTP = originals["smtp"]
        portal.imaplib.IMAP4 = originals["imap"]
        portal.get_lead_mail_config = originals["config"]
        portal.get_schaden_mail_config = originals["base_config"]
        portal.get_db = originals["db"]
        portal.log_lead_mail = originals["log"]

    print(f"Temporaere Testdaten: {TEMP_DIR}")
    if FEHLER:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
