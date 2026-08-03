# -*- coding: utf-8 -*-
"""Regressionstest fuer Kundenwunsch, Werkstatttermin und E-Mail-Entwurf."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import json
import os
import pathlib
import sys
import tempfile
import threading
from urllib.parse import parse_qs, urlsplit


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="kunden_termin_mail_"))
PUBLIC_URL = "https://kundenstatus.example.test"
os.environ.update(
    {
        "RENDER": "local-kunden-termin-mail-test",
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
        "FLASK_SECRET_KEY": "kunden-termin-mail-test",
        "ADMIN_PASS": "kunden-termin-mail-test",
        "PORTAL_BASE_URL": PUBLIC_URL,
        "SCHADEN_SMTP_PASS": "",
    }
)

import app as portal  # noqa: E402


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    return passed


def csrf_token(client):
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME)


def count_rows(sql, params=()):
    db = portal.get_db()
    row = db.execute(sql, params).fetchone()
    db.close()
    return int(row[0] or 0)


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    portal.schedule_change_backup = lambda _reason: None
    portal.ENDKUNDEN_MAIL_TESTLOG.clear()

    admin = portal.app.test_client()
    kunde = portal.app.test_client()
    gast = portal.app.test_client()
    checks = []

    wunsch_alt = (date.today() + timedelta(days=10)).isoformat()
    wunsch_abholung_alt = (date.today() + timedelta(days=14)).isoformat()
    wunsch_neu = (date.today() + timedelta(days=11)).isoformat()
    wunsch_abholung_neu = (date.today() + timedelta(days=15)).isoformat()
    werkstatt_annahme = (date.today() + timedelta(days=12)).isoformat()
    werkstatt_abholung = (date.today() + timedelta(days=16)).isoformat()
    zu_fruehe_abholung = (date.today() + timedelta(days=9)).isoformat()
    wunsch_spaeter = (date.today() + timedelta(days=18)).isoformat()
    wunsch_abholung_spaeter = (date.today() + timedelta(days=20)).isoformat()
    ersatz_annahme = (date.today() + timedelta(days=19)).isoformat()
    ersatz_abholung = (date.today() + timedelta(days=21)).isoformat()
    parallel_annahme = (date.today() + timedelta(days=22)).isoformat()
    parallel_abholung = (date.today() + timedelta(days=24)).isoformat()

    intake = {
        "kunden_angebot_angenommen_am": portal.now_str(),
        "kunden_wunsch_annahme_datum": portal.format_date(wunsch_alt),
        "kunden_wunsch_abholung_datum": portal.format_date(wunsch_abholung_alt),
        "kunden_wunsch_transport_art": "hol_und_bring",
        "kunden_wunsch_ersatzfahrzeug": "ja",
        "kunden_wunsch_abhol_adresse": "Musterstraße 12, 74821 Mosbach",
        "kunden_wunsch_hinweis": "Bitte morgens anrufen.",
        "kunden_wunsch_bestaetigt_am": "",
    }
    auftrag_id = portal.create_auftrag(
        "lead",
        kunde_name="Klara Termin",
        kunde_email="klara.alt@example.test",
        fahrzeug="VW Golf VII",
        kennzeichen="MOS-T 123",
        kontakt_telefon="0151 23456789",
        schaden_aufnahme_json=json.dumps(intake, ensure_ascii=False),
        schaden_mietwagen="ja",
        transport_art="hol_und_bring",
        werkstatt_angebot_text="Instandsetzung und Lackierung",
        werkstatt_angebot_preis="1250,00",
    )
    db = portal.get_db()
    db.execute(
        "UPDATE auftraege SET angebot_status='angenommen', angebotsphase=0 WHERE id=?",
        (auftrag_id,),
    )
    db.commit()
    db.close()

    with admin.session_transaction() as session:
        session["admin"] = True
    detail = admin.get(f"/admin/auftrag/{auftrag_id}")
    admin_csrf = csrf_token(admin)
    detail_html = detail.get_data(as_text=True)
    checks.append(
        check(
            "Admin sieht getrennte Wunsch- und Werkstatttermine",
            detail.status_code == 200
            and 'name="kunden_wunsch_annahme_datum"' in detail_html
            and 'name="annahme_datum"' in detail_html
            and 'name="kunde_email"' in detail_html
            and 'value="wunsch_speichern"' in detail_html
            and 'value="bestaetigen"' in detail_html,
        )
    )

    gast.get("/login")
    gast_csrf = csrf_token(gast)
    ohne_admin = gast.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={portal.CSRF_FIELD_NAME: gast_csrf},
        follow_redirects=False,
    )
    checks.append(check("Terminroute ist admin-geschuetzt", ohne_admin.status_code == 302 and "/login" in ohne_admin.headers.get("Location", "")))

    ohne_csrf = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={"aktion": "bestaetigen"},
        follow_redirects=False,
    )
    checks.append(check("Terminroute ist CSRF-geschuetzt", ohne_csrf.status_code == 400))

    invalid = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_alt,
            "kunden_wunsch_abholung_datum": wunsch_abholung_alt,
            "annahme_datum": werkstatt_annahme,
            "annahme_uhrzeit": "08:00",
            "abholtermin": zu_fruehe_abholung,
            "abhol_uhrzeit": "16:00",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    unveraendert = portal.get_auftrag(auftrag_id)
    checks.append(
        check(
            "Abholung vor Annahme wird ohne Teil-Speicherung abgewiesen",
            invalid.status_code == 302
            and not unveraendert["annahme_datum"]
            and not unveraendert["schaden_aufnahme"].get("kunden_wunsch_bestaetigt_am")
            and unveraendert["kunde_email"] == "klara.alt@example.test",
        )
    )

    invalid_email = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "wunsch_speichern",
            "kunden_wunsch_annahme_datum": wunsch_neu,
            "kunden_wunsch_abholung_datum": wunsch_abholung_neu,
            "kunde_email": "foo bar@example.com",
        },
        follow_redirects=False,
    )
    nach_falscher_email = portal.get_auftrag(auftrag_id)
    checks.append(
        check(
            "Unbrauchbare Einzeladresse wird serverseitig abgewiesen",
            invalid_email.status_code == 302
            and nach_falscher_email["kunde_email"] == "klara.alt@example.test"
            and nach_falscher_email["schaden_aufnahme"].get("kunden_wunsch_annahme_datum")
            == portal.format_date(wunsch_alt)
            and not portal.parse_single_email_recipient("foo bar@example.com")
            and not portal.parse_single_email_recipient("@example.com")
            and not portal.parse_single_email_recipient("a@example.com?subject=x"),
        )
    )

    same_day_invalid = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_alt,
            "kunden_wunsch_abholung_datum": wunsch_abholung_alt,
            "annahme_datum": werkstatt_annahme,
            "annahme_uhrzeit": "16:00",
            "abholtermin": werkstatt_annahme,
            "abhol_uhrzeit": "08:00",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    time_without_date_invalid = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_alt,
            "kunden_wunsch_abholung_datum": wunsch_abholung_alt,
            "annahme_datum": werkstatt_annahme,
            "annahme_uhrzeit": "08:00",
            "abholtermin": "",
            "abhol_uhrzeit": "16:00",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    nach_falscher_uhrzeit = portal.get_auftrag(auftrag_id)
    checks.append(
        check(
            "Widersprüchliche Uhrzeiten werden ohne Teil-Speicherung abgewiesen",
            same_day_invalid.status_code == 302
            and time_without_date_invalid.status_code == 302
            and not nach_falscher_uhrzeit["annahme_datum"]
            and not nach_falscher_uhrzeit["schaden_aufnahme"].get("kunden_wunsch_bestaetigt_am"),
        )
    )

    wunsch_speichern = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "wunsch_speichern",
            "kunden_wunsch_annahme_datum": wunsch_neu,
            "kunden_wunsch_abholung_datum": wunsch_abholung_neu,
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    nach_wunsch = portal.get_auftrag(auftrag_id)
    nach_wunsch_intake = nach_wunsch["schaden_aufnahme"]
    checks.append(
        check(
            "Werkstatt kann Kundenwunsch und E-Mail korrigieren",
            wunsch_speichern.status_code == 302
            and nach_wunsch_intake.get("kunden_wunsch_annahme_datum") == portal.format_date(wunsch_neu)
            and nach_wunsch_intake.get("kunden_wunsch_abholung_datum") == portal.format_date(wunsch_abholung_neu)
            and nach_wunsch_intake.get("kunden_wunsch_annahme_datum_original") == portal.format_date(wunsch_alt)
            and nach_wunsch_intake.get("kunden_wunsch_abholung_datum_original") == portal.format_date(wunsch_abholung_alt)
            and nach_wunsch["kunde_email"] == "klara@example.test"
            and not nach_wunsch["annahme_datum"]
            and not nach_wunsch_intake.get("kunden_wunsch_bestaetigt_am"),
        )
    )

    confirm = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_neu,
            "kunden_wunsch_abholung_datum": wunsch_abholung_neu,
            "annahme_datum": werkstatt_annahme,
            "annahme_uhrzeit": "08:30",
            "abholtermin": werkstatt_abholung,
            "abhol_uhrzeit": "16:15",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag_id)
    confirmed_page = kunde.get(f"/status/{auftrag['kunden_status_token']}")
    confirmed_html = confirmed_page.get_data(as_text=True)
    checks.append(
        check(
            "Abweichender Werkstatttermin wird verbindlich gespeichert und angezeigt",
            confirm.status_code == 302
            and auftrag["status"] == 2
            and auftrag["annahme_datum"] == portal.format_date(werkstatt_annahme)
            and auftrag["annahme_uhrzeit"] == "08:30"
            and auftrag["abholtermin"] == portal.format_date(werkstatt_abholung)
            and auftrag["abhol_uhrzeit"] == "16:15"
            and auftrag["schaden_aufnahme"].get("kunden_wunsch_annahme_datum") == portal.format_date(wunsch_neu)
            and bool(auftrag["schaden_aufnahme"].get("kunden_wunsch_bestaetigt_am"))
            and "Termine bestätigt" in confirmed_html
            and f"{portal.format_date(werkstatt_annahme)} · 08:30 Uhr" in confirmed_html
            and f"{portal.format_date(werkstatt_abholung)} · 16:15 Uhr" in confirmed_html,
        )
    )

    draft = portal.build_kundentermin_mail_entwurf(auftrag)
    mailto_query = parse_qs(urlsplit(draft["mailto_url"]).query)
    checks.append(
        check(
            "E-Mail-Entwurf enthält Empfänger, Werkstatttermin und öffentlichen Statuslink",
            draft["empfaenger"] == "klara@example.test"
            and portal.format_date(werkstatt_annahme) in draft["text"]
            and "08:30 Uhr" in draft["text"]
            and auftrag["kunden_status_url"].startswith(PUBLIC_URL + "/status/")
            and auftrag["kunden_status_url"] in draft["text"]
            and draft["text"] in mailto_query.get("body", [])
            and draft["betreff"] in mailto_query.get("subject", []),
        )
    )

    admin_page = admin.get(f"/admin/auftrag/{auftrag_id}")
    admin_html = admin_page.get_data(as_text=True)
    checks.append(
        check(
            "Adminseite bietet geprüften E-Mail-Entwurf statt automatischem Versand",
            admin_page.status_code == 200
            and "E-Mail öffnen &amp; senden" in admin_html
            and "Entwurf – noch nicht gesendet" in admin_html
            and auftrag["kunden_status_url"] in admin_html
            and not portal.ENDKUNDEN_MAIL_TESTLOG,
        )
    )

    notifications_before = count_rows(
        "SELECT COUNT(*) FROM benachrichtigungen WHERE auftrag_id=? AND titel IN ('Termin und Auftragsdaten bestätigt', 'Werkstatttermin aktualisiert')",
        (auftrag_id,),
    )
    status_before = count_rows(
        "SELECT COUNT(*) FROM status_log WHERE auftrag_id=? AND status=2", (auftrag_id,)
    )
    duplicate = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_neu,
            "kunden_wunsch_abholung_datum": wunsch_abholung_neu,
            "annahme_datum": werkstatt_annahme,
            "annahme_uhrzeit": "08:30",
            "abholtermin": werkstatt_abholung,
            "abhol_uhrzeit": "16:15",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    checks.append(
        check(
            "Doppelter Bestätigungsklick erzeugt keinen zweiten Status oder Hinweis",
            duplicate.status_code == 302
            and notifications_before
            == count_rows(
                "SELECT COUNT(*) FROM benachrichtigungen WHERE auftrag_id=? AND titel IN ('Termin und Auftragsdaten bestätigt', 'Werkstatttermin aktualisiert')",
                (auftrag_id,),
            )
            and status_before
            == count_rows("SELECT COUNT(*) FROM status_log WHERE auftrag_id=? AND status=2", (auftrag_id,))
            and not portal.ENDKUNDEN_MAIL_TESTLOG,
        )
    )

    neuer_wunsch = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "wunsch_speichern",
            "kunden_wunsch_annahme_datum": wunsch_spaeter,
            "kunden_wunsch_abholung_datum": wunsch_abholung_spaeter,
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    waehrend_neuabstimmung = portal.get_auftrag(auftrag_id)
    kunden_seite_alt = kunde.get(f"/status/{waehrend_neuabstimmung['kunden_status_token']}")
    kunden_html_alt = kunden_seite_alt.get_data(as_text=True)
    admin_neuabstimmung = admin.get(f"/admin/auftrag/{auftrag_id}").get_data(as_text=True)
    checks.append(
        check(
            "Neuer Wunsch lässt den bisher bestätigten Termin widerspruchsfrei gültig",
            neuer_wunsch.status_code == 302
            and bool(
                waehrend_neuabstimmung["schaden_aufnahme"].get(
                    "kunden_wunsch_neuabstimmung_offen_am"
                )
            )
            and bool(
                waehrend_neuabstimmung["schaden_aufnahme"].get("kunden_wunsch_bestaetigt_am")
            )
            and waehrend_neuabstimmung["annahme_datum"]
            == portal.format_date(werkstatt_annahme)
            and f"{portal.format_date(werkstatt_annahme)} · 08:30 Uhr" in kunden_html_alt
            and portal.format_date(wunsch_spaeter) not in kunden_html_alt
            and "Neue Abstimmung offen" in admin_neuabstimmung
            and "E-Mail öffnen &amp; senden" not in admin_neuabstimmung,
        )
    )

    ersatz_bestaetigen = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "aktion": "bestaetigen",
            "kunden_wunsch_annahme_datum": wunsch_spaeter,
            "kunden_wunsch_abholung_datum": wunsch_abholung_spaeter,
            "annahme_datum": ersatz_annahme,
            "annahme_uhrzeit": "09:00",
            "abholtermin": ersatz_abholung,
            "abhol_uhrzeit": "15:30",
            "kunde_email": "klara@example.test",
        },
        follow_redirects=False,
    )
    nach_ersatz = portal.get_auftrag(auftrag_id)
    checks.append(
        check(
            "Ersatztermin schließt die Neuabstimmung und erneuert den Mail-Entwurf",
            ersatz_bestaetigen.status_code == 302
            and nach_ersatz["annahme_datum"] == portal.format_date(ersatz_annahme)
            and nach_ersatz["abholtermin"] == portal.format_date(ersatz_abholung)
            and not nach_ersatz["schaden_aufnahme"].get(
                "kunden_wunsch_neuabstimmung_offen_am"
            )
            and portal.format_date(ersatz_annahme)
            in portal.build_kundentermin_mail_entwurf(nach_ersatz)["text"],
        )
    )

    parallel_clients = [portal.app.test_client(), portal.app.test_client()]
    parallel_csrf = []
    for client in parallel_clients:
        with client.session_transaction() as session:
            session["admin"] = True
        client.get(f"/admin/auftrag/{auftrag_id}")
        parallel_csrf.append(csrf_token(client))

    parallel_payload = {
        "aktion": "bestaetigen",
        "kunden_wunsch_annahme_datum": wunsch_spaeter,
        "kunden_wunsch_abholung_datum": wunsch_abholung_spaeter,
        "annahme_datum": parallel_annahme,
        "annahme_uhrzeit": "08:15",
        "abholtermin": parallel_abholung,
        "abhol_uhrzeit": "16:45",
        "kunde_email": "klara@example.test",
    }
    notifications_parallel_before = count_rows(
        "SELECT COUNT(*) FROM benachrichtigungen WHERE auftrag_id=? AND titel='Werkstatttermin aktualisiert'",
        (auftrag_id,),
    )
    original_get_auftrag = portal.get_auftrag
    initial_reads = threading.Barrier(2, timeout=10)
    local_state = threading.local()

    def synchronized_get_auftrag(current_id):
        result = original_get_auftrag(current_id)
        if current_id == auftrag_id and not getattr(local_state, "initial_read_done", False):
            local_state.initial_read_done = True
            initial_reads.wait()
        return result

    def parallel_confirm(index):
        payload = dict(parallel_payload)
        payload[portal.CSRF_FIELD_NAME] = parallel_csrf[index]
        return parallel_clients[index].post(
            f"/admin/auftrag/{auftrag_id}/kundenwuensche-bestaetigen",
            data=payload,
            follow_redirects=False,
        ).status_code

    portal.get_auftrag = synchronized_get_auftrag
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            parallel_status = list(pool.map(parallel_confirm, (0, 1)))
    finally:
        portal.get_auftrag = original_get_auftrag

    nach_parallel = portal.get_auftrag(auftrag_id)
    notifications_parallel_after = count_rows(
        "SELECT COUNT(*) FROM benachrichtigungen WHERE auftrag_id=? AND titel='Werkstatttermin aktualisiert'",
        (auftrag_id,),
    )
    checks.append(
        check(
            "Zwei parallele Bestätigungen erzeugen atomar nur eine Aktualisierung",
            parallel_status == [302, 302]
            and nach_parallel["annahme_datum"] == portal.format_date(parallel_annahme)
            and nach_parallel["annahme_uhrzeit"] == "08:15"
            and notifications_parallel_after == notifications_parallel_before + 1
            and not portal.ENDKUNDEN_MAIL_TESTLOG,
        )
    )

    print(f"Temporäre Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
