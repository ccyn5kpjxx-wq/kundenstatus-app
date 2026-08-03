# -*- coding: utf-8 -*-
"""Isolierter Regressionstest fuer den expliziten IONOS-Terminversand.

Alle SMTP- und IMAP-Zugriffe werden durch Fakes ersetzt. Der Test kann keine
echte E-Mail versenden und arbeitet ausschliesslich auf einer temporaeren DB.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import date, timedelta
import json
import os
from pathlib import Path
import shutil
import ssl
import sys
import tempfile
import threading


ROOT = Path(__file__).resolve().parents[1]
TEMP_DIR = Path(tempfile.mkdtemp(prefix="kunden_termin_ionos_"))
PUBLIC_URL = "https://kundenstatus.example.test"
IONOS_ADDRESS = "info@auto-lackierzentrum.de"
IONOS_SMTP_HOST = "smtp.ionos.de"
IONOS_IMAP_HOST = "imap.ionos.de"

sys.path.insert(0, str(ROOT))
os.environ.update(
    {
        "RENDER": "local-kunden-termin-ionos-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "OPENAI_API_KEY": "",
        "FLASK_SECRET_KEY": "kunden-termin-ionos-test",
        "ADMIN_PASS": "kunden-termin-ionos-test",
        "PORTAL_BASE_URL": PUBLIC_URL,
        "SCHADEN_MAIL_ADDRESS": IONOS_ADDRESS,
        "SCHADEN_MAIL_DISPLAY_NAME": "Gärtner Karosserie & Lack",
        "SCHADEN_SMTP_HOST": IONOS_SMTP_HOST,
        "SCHADEN_SMTP_PORT": "587",
        "SCHADEN_SMTP_USER": IONOS_ADDRESS,
        "SCHADEN_SMTP_PASS": "nur-test-passwort",
        "SCHADEN_SMTP_SSL": "0",
        "SCHADEN_SMTP_TLS": "1",
        "SCHADEN_IMAP_HOST": IONOS_IMAP_HOST,
        "SCHADEN_IMAP_PORT": "993",
        "SCHADEN_IMAP_USER": IONOS_ADDRESS,
        "SCHADEN_IMAP_PASS": "nur-test-passwort",
        "SCHADEN_IMAP_SSL": "1",
        "SCHADEN_IMAP_SENT_FOLDER": "",
    }
)

import app as portal  # noqa: E402


FEHLER = []


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    if not passed:
        FEHLER.append(label)
    return passed


def csrf_token(client):
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME)


def admin_client(auftrag_id):
    client = portal.app.test_client()
    with client.session_transaction() as session:
        session["admin"] = True
    client.get(f"/admin/auftrag/{auftrag_id}")
    return client, csrf_token(client)


def count_rows(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return int(row[0] or 0)
    finally:
        db.close()


def one_row(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


class FakeSMTP:
    instances = []
    send_calls = 0
    fail_send = False
    fail_login = False
    refuse_recipient = False
    block_send = False
    entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()

    @classmethod
    def reset(
        cls, *, fail_send=False, fail_login=False, refuse_recipient=False, block_send=False
    ):
        cls.instances = []
        cls.send_calls = 0
        cls.fail_send = bool(fail_send)
        cls.fail_login = bool(fail_login)
        cls.refuse_recipient = bool(refuse_recipient)
        cls.block_send = bool(block_send)
        cls.entered = threading.Event()
        cls.release = threading.Event()

    def __init__(self, host, port, local_hostname=None, timeout=None, context=None):
        self.host = host
        self.port = port
        self.local_hostname = local_hostname
        self.timeout = timeout
        self.context = context
        self.started_tls = False
        self.starttls_context = None
        self.login_data = None
        self.message = None
        self.__class__.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def starttls(self, context=None):
        self.started_tls = True
        self.starttls_context = context

    def login(self, user, password):
        self.login_data = (user, password)
        if self.__class__.fail_login:
            raise OSError("simulierter IONOS-Loginfehler")

    def send_message(self, message):
        self.message = message
        with self.__class__.lock:
            self.__class__.send_calls += 1
        if self.__class__.block_send:
            self.__class__.entered.set()
            if not self.__class__.release.wait(10):
                raise TimeoutError("Fake-SMTP wurde nicht freigegeben")
        if self.__class__.fail_send:
            raise OSError("simulierter IONOS-SMTP-Fehler")
        if self.__class__.refuse_recipient:
            raise portal.smtplib.SMTPRecipientsRefused(
                {message["To"]: (550, b"Empfaenger abgelehnt")}
            )


class UnexpectedSMTPSSL:
    def __init__(self, *_args, **_kwargs):
        raise AssertionError("Der TLS-Test darf SMTP_SSL nicht verwenden")


class FakeIMAP:
    instances = []
    append_calls = 0
    fail_append = False

    @classmethod
    def reset(cls, *, fail_append=False):
        cls.instances = []
        cls.append_calls = 0
        cls.fail_append = bool(fail_append)

    def __init__(self, host, port, timeout=None, ssl_context=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
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
        self.__class__.append_calls += 1
        self.append_data = (folder, flags, date_time, raw_message)
        return ("NO" if self.__class__.fail_append else "OK"), []

    def logout(self):
        self.logged_out = True
        return "BYE", []


class UnexpectedIMAP:
    def __init__(self, *_args, **_kwargs):
        raise AssertionError("Der SSL-IMAP-Test darf IMAP4 ohne SSL nicht verwenden")


def create_pending_order(suffix, email=None):
    annahme_wunsch = (date.today() + timedelta(days=20)).isoformat()
    abholung_wunsch = (date.today() + timedelta(days=24)).isoformat()
    intake = {
        "kunden_angebot_angenommen_am": portal.now_str(),
        "kunden_wunsch_annahme_datum": portal.format_date(annahme_wunsch),
        "kunden_wunsch_abholung_datum": portal.format_date(abholung_wunsch),
        "kunden_wunsch_transport_art": "standard",
        "kunden_wunsch_ersatzfahrzeug": "nein",
        "kunden_wunsch_bestaetigt_am": "",
    }
    auftrag_id = portal.create_auftrag(
        "lead",
        kunde_name=f"IONOS Test {suffix}",
        kunde_email=email or f"kunde-{suffix.lower()}@example.test",
        fahrzeug="VW Golf",
        kennzeichen=f"MOS-{suffix[:3].upper()} 1",
        schaden_aufnahme_json=json.dumps(intake, ensure_ascii=False),
        werkstatt_angebot_text="Instandsetzung und Lackierung",
        werkstatt_angebot_preis="1000,00",
    )
    db = portal.get_db()
    try:
        db.execute(
            "UPDATE auftraege SET angebot_status='angenommen', angebotsphase=0 WHERE id=?",
            (auftrag_id,),
        )
        db.commit()
    finally:
        db.close()
    return auftrag_id, annahme_wunsch, abholung_wunsch


def confirm_order(client, token, auftrag_id, annahme_wunsch, abholung_wunsch, email):
    annahme = (date.today() + timedelta(days=21)).isoformat()
    abholung = (date.today() + timedelta(days=25)).isoformat()
    response = client.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: token,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": annahme_wunsch,
            "kunden_wunsch_abholung_datum": abholung_wunsch,
            "annahme_datum": annahme,
            "annahme_uhrzeit": "08:30",
            "abholtermin": abholung,
            "abhol_uhrzeit": "16:15",
            "kunde_email": email,
        },
        follow_redirects=False,
    )
    return response, annahme, abholung


def create_confirmed_order(suffix):
    email = f"kunde-{suffix.lower()}@example.test"
    auftrag_id, wunsch, wunsch_abholung = create_pending_order(suffix, email)
    client, token = admin_client(auftrag_id)
    response, annahme, abholung = confirm_order(
        client, token, auftrag_id, wunsch, wunsch_abholung, email
    )
    if response.status_code != 302:
        raise RuntimeError(f"Testauftrag {suffix} konnte nicht bestätigt werden")
    return auftrag_id, email, annahme, abholung


def send_path(auftrag_id):
    return f"/admin/auftrag/{auftrag_id}/kundentermin-mail-senden"


def send_post(client, token, auftrag_id, extra=None):
    payload = {portal.CSRF_FIELD_NAME: token}
    try:
        mail_payload = portal.build_kundentermin_mail_payload(
            portal.get_auftrag(auftrag_id)
        )
    except ValueError:
        mail_payload = {}
    if mail_payload:
        payload.update(
            {
                "idempotenz_key": mail_payload["idempotenz_key"],
                "payload_sha256": mail_payload["payload_sha256"],
            }
        )
    payload.update(extra or {})
    return client.post(
        send_path(auftrag_id), data=payload, follow_redirects=False
    )


def portal_snapshot(auftrag_id, customer):
    auftrag = portal.get_auftrag(auftrag_id)
    response = customer.get(f"/status/{auftrag['kunden_status_token']}")
    fields = {
        name: auftrag.get(name)
        for name in (
            "status",
            "annahme_datum",
            "annahme_uhrzeit",
            "abholtermin",
            "abhol_uhrzeit",
            "kunde_email",
            "kunden_status_token",
            "schaden_aufnahme_json",
        )
    }
    return {
        "status_code": response.status_code,
        "html": response.get_data(as_text=True),
        "fields": fields,
        "status_log": count_rows(
            "SELECT COUNT(*) FROM status_log WHERE auftrag_id=?", (auftrag_id,)
        ),
        "notifications": count_rows(
            "SELECT COUNT(*) FROM benachrichtigungen WHERE auftrag_id=?", (auftrag_id,)
        ),
    }


def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()

    originals = {
        "smtp": portal.smtplib.SMTP,
        "smtp_ssl": portal.smtplib.SMTP_SSL,
        "imap": portal.imaplib.IMAP4,
        "imap_ssl": portal.imaplib.IMAP4_SSL,
        "mail_config": portal.get_lead_mail_config,
        "backup": portal.schedule_change_backup,
    }
    portal.smtplib.SMTP = FakeSMTP
    portal.smtplib.SMTP_SSL = UnexpectedSMTPSSL
    portal.imaplib.IMAP4 = UnexpectedIMAP
    portal.imaplib.IMAP4_SSL = FakeIMAP
    portal.schedule_change_backup = lambda _reason: None
    FakeSMTP.reset()
    FakeIMAP.reset()

    try:
        # Ein normaler Termin wird erst bestaetigt. Dieser Schritt und das reine
        # Anzeigen des Entwurfs duerfen noch keinen Versand ausloesen.
        erfolg_email = "kunde-erfolg@example.test"
        erfolg_id, wunsch, wunsch_abholung = create_pending_order(
            "Erfolg", erfolg_email
        )
        admin, admin_csrf = admin_client(erfolg_id)

        gast = portal.app.test_client()
        gast.get("/login")
        gast_csrf = csrf_token(gast)
        ohne_admin = send_post(gast, gast_csrf, erfolg_id)
        ohne_csrf = admin.post(send_path(erfolg_id), data={}, follow_redirects=False)
        checks_before = count_rows(
            "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (erfolg_id,),
        )
        check(
            "IONOS-Versandroute ist admin- und CSRF-geschützt",
            ohne_admin.status_code == 302
            and "/login" in ohne_admin.headers.get("Location", "")
            and ohne_csrf.status_code == 400
            and checks_before == 0
            and FakeSMTP.send_calls == 0,
        )

        bestaetigt, annahme, abholung = confirm_order(
            admin,
            admin_csrf,
            erfolg_id,
            wunsch,
            wunsch_abholung,
            erfolg_email,
        )
        detail = admin.get(f"/admin/auftrag/{erfolg_id}")
        detail_html = detail.get_data(as_text=True)
        check(
            "Terminbestätigung und Seitenaufruf senden noch keine E-Mail",
            bestaetigt.status_code == 302
            and detail.status_code == 200
            and FakeSMTP.send_calls == 0
            and FakeIMAP.append_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (erfolg_id,),
            )
            == 0,
        )
        check(
            "Adminoberfläche nutzt IONOS statt mailto oder Outlook",
            "Mit IONOS senden" in detail_html
            and "Outlook wird nicht geöffnet" in detail_html
            and "mailto:" not in detail_html
            and send_path(erfolg_id) in detail_html,
        )

        stale_id, stale_email, _s_annahme, _s_abholung = create_confirmed_order(
            "VeralteterEntwurf"
        )
        stale_admin, stale_csrf = admin_client(stale_id)
        stale_payload = portal.build_kundentermin_mail_payload(
            portal.get_auftrag(stale_id)
        )
        db = portal.get_db()
        try:
            db.execute(
                "UPDATE auftraege SET kunde_email=? WHERE id=?",
                ("neue-adresse@example.test", stale_id),
            )
            db.commit()
        finally:
            db.close()
        FakeSMTP.reset()
        stale_response = send_post(
            stale_admin,
            stale_csrf,
            stale_id,
            {
                "idempotenz_key": stale_payload["idempotenz_key"],
                "payload_sha256": stale_payload["payload_sha256"],
            },
        )
        check(
            "Versand ist an den vom Admin angezeigten Entwurf gebunden",
            stale_response.status_code == 302
            and FakeSMTP.send_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (stale_id,),
            )
            == 0
            and portal.get_auftrag(stale_id)["kunde_email"] != stale_email,
        )

        customer = portal.app.test_client()
        before_send = portal_snapshot(erfolg_id, customer)
        send_success = send_post(
            admin,
            admin_csrf,
            erfolg_id,
            {
                "empfaenger": "angreifer@example.test",
                "betreff": "Gefälschter Betreff",
                "text": "Dieser Text darf nicht gesendet werden.",
            },
        )
        versand = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (erfolg_id,),
        )
        smtp = FakeSMTP.instances[-1] if FakeSMTP.instances else None
        imap = FakeIMAP.instances[-1] if FakeIMAP.instances else None
        message = smtp.message if smtp else None
        raw_sent_copy = imap.append_data[3] if imap and imap.append_data else b""
        check(
            "Expliziter Klick sendet genau eine geprüfte Nachricht über IONOS",
            send_success.status_code == 302
            and FakeSMTP.send_calls == 1
            and smtp is not None
            and smtp.host == IONOS_SMTP_HOST
            and smtp.port == 587
            and smtp.started_tls
            and isinstance(smtp.starttls_context, ssl.SSLContext)
            and smtp.starttls_context.check_hostname
            and smtp.starttls_context.verify_mode == ssl.CERT_REQUIRED
            and smtp.login_data == (IONOS_ADDRESS, "nur-test-passwort")
            and message is not None
            and message["To"] == erfolg_email
            and len(portal.parse_email_recipients(message["To"])) == 1
            and message["Reply-To"] == IONOS_ADDRESS
            and message["Date"]
            and message["Message-ID"]
            and message["X-Gaertner-Auftrag-ID"] == str(erfolg_id)
            and message["X-Gaertner-Mail-Typ"] == "terminbestaetigung"
            and portal.format_date(annahme) in message.get_content()
            and "08:30 Uhr" in message.get_content()
            and portal.get_auftrag(erfolg_id)["kunden_status_url"]
            in message.get_content(),
        )
        check(
            "Gefälschte Formfelder können Empfänger und Inhalt nicht überschreiben",
            "angreifer@example.test" not in message.get_content()
            and message["To"] != "angreifer@example.test"
            and message["Subject"] != "Gefälschter Betreff"
            and "Dieser Text darf nicht gesendet werden." not in message.get_content(),
        )
        check(
            "Dieselbe MIME-Nachricht liegt im IONOS-Gesendet-Ordner",
            FakeIMAP.append_calls == 1
            and imap.host == IONOS_IMAP_HOST
            and isinstance(imap.ssl_context, ssl.SSLContext)
            and imap.ssl_context.check_hostname
            and imap.ssl_context.verify_mode == ssl.CERT_REQUIRED
            and imap.login_data == (IONOS_ADDRESS, "nur-test-passwort")
            and imap.append_data[0] == "Sent"
            and imap.append_data[1] == r"(\Seen)"
            and b"X-Gaertner-Mail-Typ: terminbestaetigung" in raw_sent_copy
            and portal.get_auftrag(erfolg_id)["kunden_status_url"].encode("ascii")
            in raw_sent_copy
            and imap.logged_out,
        )
        check(
            "Erfolgreicher Versand wird revisionsfest protokolliert",
            versand is not None
            and versand["status"] == "gesendet"
            and versand["empfaenger"] == erfolg_email
            and versand["absender"] == IONOS_ADDRESS
            and versand["imap_ordner"] == "Sent"
            and not versand["imap_fehler"]
            and bool(versand["gesendet_am"])
            and bool(versand["payload_sha256"])
            and bool(versand["idempotenz_key"]),
        )

        after_send = portal_snapshot(erfolg_id, customer)
        check(
            "E-Mail-Versand verändert Kundenportal und Auftrag nicht",
            before_send == after_send
            and before_send["status_code"] == 200
            and portal.format_date(annahme) in after_send["html"]
            and portal.format_date(abholung) in after_send["html"],
        )

        duplicate = send_post(admin, admin_csrf, erfolg_id)
        check(
            "Sequenter Doppelklick bleibt idempotent",
            duplicate.status_code == 302
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 1
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (erfolg_id,),
            )
            == 1,
        )
        sent_detail = admin.get(f"/admin/auftrag/{erfolg_id}").get_data(as_text=True)
        check(
            "Nach Versand ist der IONOS-Knopf gesperrt",
            "Über IONOS gesendet" in sent_detail
            and "Bereits über IONOS gesendet" in sent_detail
            and "mailto:" not in sent_detail,
        )

        # Zwei echte parallele Requests: Der erste haelt Fake-SMTP offen. Der
        # zweite muss dank vorheriger DB-Reservierung ohne zweiten SMTP-Aufruf
        # fertig werden, waehrend der erste noch blockiert ist.
        parallel_id, _parallel_email, _p_annahme, _p_abholung = create_confirmed_order(
            "Parallel"
        )
        parallel_clients = [admin_client(parallel_id), admin_client(parallel_id)]
        FakeSMTP.reset(block_send=True)
        FakeIMAP.reset()
        second_finished_while_blocked = False
        parallel_status = [None, None]
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                send_post,
                parallel_clients[0][0],
                parallel_clients[0][1],
                parallel_id,
            )
            entered = FakeSMTP.entered.wait(10)
            second = pool.submit(
                send_post,
                parallel_clients[1][0],
                parallel_clients[1][1],
                parallel_id,
            )
            try:
                response_two = second.result(timeout=4)
                parallel_status[1] = response_two.status_code
                second_finished_while_blocked = True
            except FutureTimeout:
                pass
            finally:
                FakeSMTP.release.set()
            parallel_status[0] = first.result(timeout=10).status_code
            if parallel_status[1] is None:
                parallel_status[1] = second.result(timeout=10).status_code
        parallel_row = one_row(
            "SELECT status FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (parallel_id,),
        )
        check(
            "Paralleler Doppelklick reserviert vor dem Netzwerkzugriff",
            entered
            and second_finished_while_blocked
            and parallel_status == [302, 302]
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 1
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (parallel_id,),
            )
            == 1
            and bool(parallel_row)
            and parallel_row.get("status") == "gesendet",
        )

        # Ein Fehler rund um SMTP ist nach der Uebergabe nicht sicher als
        # ungesendet beweisbar. Deshalb bleibt er ungeklaert und wird nie
        # automatisch oder durch denselben Doppelklick wiederholt.
        fehler_id, _fehler_email, _f_annahme, _f_abholung = create_confirmed_order(
            "SmtpFehler"
        )
        fehler_admin, fehler_csrf = admin_client(fehler_id)
        FakeSMTP.reset(fail_send=True)
        FakeIMAP.reset()
        failed = send_post(fehler_admin, fehler_csrf, fehler_id)
        fehler_row = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (fehler_id,),
        )
        failed_duplicate = send_post(fehler_admin, fehler_csrf, fehler_id)
        fehler_html = fehler_admin.get(
            f"/admin/auftrag/{fehler_id}"
        ).get_data(as_text=True)
        check(
            "SMTP-Fehler wird unklar markiert und niemals automatisch wiederholt",
            failed.status_code == 302
            and failed_duplicate.status_code == 302
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 0
            and fehler_row["status"] == "versand_ungeklaert"
            and "simulierter IONOS-SMTP-Fehler" in fehler_row["fehler"]
            and not fehler_row["gesendet_am"]
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (fehler_id,),
            )
            == 1
            and "Versandstatus unklar" in fehler_html
            and "Nicht erneut senden" in fehler_html,
        )

        # Ein Loginfehler liegt sicher vor SMTP-DATA. Er darf nach einer
        # Konfigurationskorrektur durch einen neuen Admin-Klick wiederholt werden.
        login_id, _login_email, _l_annahme, _l_abholung = create_confirmed_order(
            "LoginFehler"
        )
        login_admin, login_csrf = admin_client(login_id)
        FakeSMTP.reset(fail_login=True)
        FakeIMAP.reset()
        login_failed = send_post(login_admin, login_csrf, login_id)
        login_row_failed = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (login_id,),
        )
        login_html = login_admin.get(
            f"/admin/auftrag/{login_id}"
        ).get_data(as_text=True)
        FakeSMTP.reset()
        login_retry = send_post(login_admin, login_csrf, login_id)
        login_row_sent = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (login_id,),
        )
        check(
            "Eindeutiger IONOS-Loginfehler erlaubt einen geprüften Neuversuch",
            login_failed.status_code == 302
            and login_retry.status_code == 302
            and login_row_failed["status"] == "fehlgeschlagen"
            and "simulierter IONOS-Loginfehler" in login_row_failed["fehler"]
            and "Erneut über IONOS senden" in login_html
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 1
            and login_row_sent["status"] == "gesendet"
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (login_id,),
            )
            == 1,
        )

        ablehnung_id, _ablehnung_email, _a_annahme, _a_abholung = (
            create_confirmed_order("EmpfaengerAblehnung")
        )
        ablehnung_admin, ablehnung_csrf = admin_client(ablehnung_id)
        FakeSMTP.reset(refuse_recipient=True)
        FakeIMAP.reset()
        ablehnung = send_post(ablehnung_admin, ablehnung_csrf, ablehnung_id)
        ablehnung_row = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (ablehnung_id,),
        )
        check(
            "Eindeutige SMTP-Empfängerablehnung bleibt korrigierbar",
            ablehnung.status_code == 302
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 0
            and ablehnung_row["status"] == "fehlgeschlagen",
        )

        # Ein IMAP-Fehler tritt erst nach erfolgreichem SMTP auf. Die Nachricht
        # bleibt deshalb gesendet; auch hier darf kein zweiter SMTP-Lauf folgen.
        imap_id, _imap_email, _i_annahme, _i_abholung = create_confirmed_order(
            "ImapFehler"
        )
        imap_admin, imap_csrf = admin_client(imap_id)
        FakeSMTP.reset()
        FakeIMAP.reset(fail_append=True)
        imap_failed = send_post(imap_admin, imap_csrf, imap_id)
        imap_row = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (imap_id,),
        )
        imap_duplicate = send_post(imap_admin, imap_csrf, imap_id)
        check(
            "IMAP-Fehler bleibt gesendet und löst keinen SMTP-Neuversand aus",
            imap_failed.status_code == 302
            and imap_duplicate.status_code == 302
            and FakeSMTP.send_calls == 1
            and FakeIMAP.append_calls == 1
            and imap_row["status"] == "gesendet"
            and bool(imap_row["gesendet_am"])
            and bool(imap_row["imap_fehler"])
            and not imap_row["imap_ordner"],
        )

        fremdimap_id, _fremdimap_email, _fi_annahme, _fi_abholung = (
            create_confirmed_order("FremderImap")
        )
        fremdimap_admin, fremdimap_csrf = admin_client(fremdimap_id)
        fremdimap_config = dict(
            originals["mail_config"]("auto-lackierzentrum"),
            imap_host="mailhost.webjoker.biz",
            sent_copy_configured=True,
        )
        portal.get_lead_mail_config = lambda _website: fremdimap_config
        FakeSMTP.reset()
        FakeIMAP.reset()
        try:
            fremdimap_response = send_post(
                fremdimap_admin, fremdimap_csrf, fremdimap_id
            )
        finally:
            portal.get_lead_mail_config = originals["mail_config"]
        fremdimap_row = one_row(
            "SELECT * FROM kunden_termin_mail_versand WHERE auftrag_id=?",
            (fremdimap_id,),
        )
        check(
            "Kundenmail wird niemals an einen fremden IMAP-Server kopiert",
            fremdimap_response.status_code == 302
            and FakeSMTP.send_calls == 1
            and not FakeIMAP.instances
            and FakeIMAP.append_calls == 0
            and fremdimap_row["status"] == "gesendet"
            and "kein IONOS-IMAP-Server" in fremdimap_row["imap_fehler"],
        )

        # Fehlende Konfiguration wird vor der DB-Reservierung abgewiesen.
        config_id, _config_email, _c_annahme, _c_abholung = create_confirmed_order(
            "ConfigFehlt"
        )
        config_admin, config_csrf = admin_client(config_id)
        FakeSMTP.reset()
        FakeIMAP.reset()
        portal.get_lead_mail_config = lambda _website: {
            "configured": False,
            "address": IONOS_ADDRESS,
        }
        try:
            config_html = config_admin.get(
                f"/admin/auftrag/{config_id}"
            ).get_data(as_text=True)
            config_missing = send_post(config_admin, config_csrf, config_id)
        finally:
            portal.get_lead_mail_config = originals["mail_config"]
        check(
            "Fehlende IONOS-Konfiguration erzeugt weder Reservierung noch Netzwerkzugriff",
            config_missing.status_code == 302
            and "IONOS nicht eingerichtet" in config_html
            and FakeSMTP.send_calls == 0
            and FakeIMAP.append_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (config_id,),
            )
            == 0,
        )

        fremdserver_config = dict(
            originals["mail_config"]("auto-lackierzentrum"),
            configured=True,
            host="mailhost.webjoker.biz",
        )
        portal.get_lead_mail_config = lambda _website: fremdserver_config
        try:
            fremdserver_html = config_admin.get(
                f"/admin/auftrag/{config_id}"
            ).get_data(as_text=True)
            fremdserver_response = send_post(config_admin, config_csrf, config_id)
        finally:
            portal.get_lead_mail_config = originals["mail_config"]
        check(
            "Terminbestätigung akzeptiert ausschließlich einen IONOS-SMTP-Server",
            fremdserver_response.status_code == 302
            and "kein IONOS-SMTP-Server" in fremdserver_html
            and FakeSMTP.send_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (config_id,),
            )
            == 0,
        )

        unverschluesselt_config = dict(
            originals["mail_config"]("auto-lackierzentrum"),
            configured=True,
            host=IONOS_SMTP_HOST,
            ssl=False,
            tls=False,
        )
        portal.get_lead_mail_config = lambda _website: unverschluesselt_config
        try:
            unverschluesselt_html = config_admin.get(
                f"/admin/auftrag/{config_id}"
            ).get_data(as_text=True)
            unverschluesselt_response = send_post(
                config_admin, config_csrf, config_id
            )
        finally:
            portal.get_lead_mail_config = originals["mail_config"]
        check(
            "Unverschlüsselter IONOS-SMTP-Versand bleibt gesperrt",
            unverschluesselt_response.status_code == 302
            and "SSL oder STARTTLS" in unverschluesselt_html
            and FakeSMTP.send_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (config_id,),
            )
            == 0,
        )

        unsicherer_link_gesperrt = False
        unsicherer_auftrag = dict(portal.get_auftrag(config_id))
        unsicherer_auftrag["kunden_status_url"] = (
            "https://zufall.trycloudflare.com/status/"
            + unsicherer_auftrag["kunden_status_token"]
        )
        try:
            portal.build_kundentermin_mail_payload(unsicherer_auftrag)
        except ValueError:
            unsicherer_link_gesperrt = True
        check(
            "Temporäre oder fremde Statuslinks werden nicht versendet",
            unsicherer_link_gesperrt,
        )

        # Auch ein manipuliertes Formular darf eine ungueltige, gespeicherte
        # Mehrfachadresse nicht durch eine eingeschleuste Einzeladresse ersetzen.
        multi_id, _multi_email, _m_annahme, _m_abholung = create_confirmed_order(
            "Mehrfach"
        )
        db = portal.get_db()
        try:
            db.execute(
                "UPDATE auftraege SET kunde_email=? WHERE id=?",
                ("eins@example.test, zwei@example.test", multi_id),
            )
            db.commit()
        finally:
            db.close()
        multi_admin, multi_csrf = admin_client(multi_id)
        FakeSMTP.reset()
        FakeIMAP.reset()
        multi_response = send_post(
            multi_admin,
            multi_csrf,
            multi_id,
            {"empfaenger": "eingeschleust@example.test"},
        )
        check(
            "Server akzeptiert ausschließlich eine gespeicherte Einzeladresse",
            multi_response.status_code == 302
            and FakeSMTP.send_calls == 0
            and FakeIMAP.append_calls == 0
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (multi_id,),
            )
            == 0,
        )

        # Das Versandprotokoll gehoert zum Auftrag und muss vor dessen Loeschung
        # entfernt werden. Es ist ausserdem Bestandteil vollstaendiger Backups.
        check(
            "IONOS-Versandprotokoll ist im Backup-Schema registriert",
            "kunden_termin_mail_versand" in portal.BACKUP_TABLES
            and "kunden_termin_mail_versand" in portal.BACKUP_SCHEMA_FEATURES,
        )
        tabellen_ohne_versand = {
            table_name: []
            for table_name in portal.BACKUP_TABLES
            if table_name not in {"datei_backups", "kunden_termin_mail_versand"}
        }
        alte_sicherung_ok = True
        try:
            portal.validate_backup_binary_reference_completeness(
                {"format_version": 4, "tables": tabellen_ohne_versand}, {}
            )
        except ValueError:
            alte_sicherung_ok = False
        neue_sicherung_abgewiesen = False
        try:
            portal.validate_backup_binary_reference_completeness(
                {
                    "format_version": 4,
                    "schema_features": ["kunden_termin_mail_versand"],
                    "tables": tabellen_ohne_versand,
                },
                {},
            )
        except ValueError as exc:
            neue_sicherung_abgewiesen = "kunden_termin_mail_versand" in str(exc)
        check(
            "Neue Backups dürfen die Idempotenzhistorie nicht verlieren",
            alte_sicherung_ok and neue_sicherung_abgewiesen,
        )
        portal.delete_auftrag(erfolg_id, safety_backup=False)
        check(
            "delete_auftrag entfernt das zugehörige Versandprotokoll",
            portal.get_auftrag(erfolg_id) is None
            and count_rows(
                "SELECT COUNT(*) FROM kunden_termin_mail_versand WHERE auftrag_id=?",
                (erfolg_id,),
            )
            == 0,
        )
    finally:
        FakeSMTP.release.set()
        portal.smtplib.SMTP = originals["smtp"]
        portal.smtplib.SMTP_SSL = originals["smtp_ssl"]
        portal.imaplib.IMAP4 = originals["imap"]
        portal.imaplib.IMAP4_SSL = originals["imap_ssl"]
        portal.get_lead_mail_config = originals["mail_config"]
        portal.schedule_change_backup = originals["backup"]
        shutil.rmtree(TEMP_DIR, ignore_errors=True)

    print("Temporäre IONOS-Testdaten wurden entfernt.")
    return 0 if not FEHLER else 1


if __name__ == "__main__":
    raise SystemExit(main())
