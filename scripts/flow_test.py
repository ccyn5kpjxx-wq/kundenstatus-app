from pathlib import Path
from io import BytesIO
import json
import shutil
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def check(label, condition, detail=""):
    status = "OK" if condition else "FEHLER"
    message = f"[{status}] {label}"
    if detail:
        message += f" - {detail}"
    print(message)
    if not condition:
        raise AssertionError(f"{label}: {detail}")


def extract_id_from_location(location):
    last = (location or "").rstrip("/").split("/")[-1]
    return int(last) if last.isdigit() else 0


def with_csrf(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token") or "test-csrf-token"
        session["csrf_token"] = token
    payload["csrf_token"] = token
    return payload


def main():
    original_db = portal.DB
    original_upload_dir = portal.UPLOAD_DIR
    if not original_db.exists():
        raise SystemExit(f"Datenbank nicht gefunden: {original_db}")

    with tempfile.TemporaryDirectory() as tmp:
        test_db = Path(tmp) / "auftraege-test.db"
        test_uploads = Path(tmp) / "uploads"
        test_uploads.mkdir()
        shutil.copy2(original_db, test_db)

        portal.DB = test_db
        portal.UPLOAD_DIR = test_uploads
        portal.app.config["TESTING"] = True
        portal.init_db()

        gutachten_text = """
        Schadensgutachten
        Schadensfeststellung
        Hauptbeschädigungsbereich
        Schadensbeschreibung
        Durch den Anstoß im Heckbereich rechts wurden die beiden unteren Stoßfängerteile stark eingedrückt.
        Die Heckklappenblende rechts unten wurde gestaucht und verschrammt.
        Vorschäden
        Unreparierte Vorschäden
        An dem Fahrzeug wurden folgende unreparierte Vorschäden festgestellt:
        Tür hinten links etwas verschrammt
        DEKRA-Nr: TEST
        Seite 5 von 7
        Instandsetzung
        ARB.POS.NR/ INSTANDSETZUNGS-/EINZEL-/VERBUNDARBEITEN AW
        REIFEN 235/55 R19 FELGE 7.5 J X 19 ALU NOTRAD 18 X 4 T
        NA02R0 STOSSFAENGER H ERSETZEN 9 148.50
        2581 STOSSFAENGER H NEUTEILLACK ST K1R 9
        """
        gutachten_felder = portal.parse_document_fields(gutachten_text, "schaden-gutachten.pdf")
        gutachten_analyse = portal.normalize_document_text(gutachten_felder.get("analyse_text"))
        check(
            "Gutachten ignoriert Vorschaden",
            "tuer hinten links" not in gutachten_analyse,
            gutachten_felder.get("analyse_text"),
        )
        check(
            "Gutachten erkennt aktuellen Heckschaden",
            "stossstange hinten" in gutachten_analyse,
            gutachten_felder.get("analyse_text"),
        )
        gutachten_mit_ki = portal.merge_document_fields(
            {
                "analyse_text": "Tür hinten links lackieren, Stoßstange hinten lackieren",
                "bauteile_override": "Tür hinten links\nStoßstange hinten",
                "analyse_confidence": 0.9,
            },
            gutachten_felder,
        )
        gutachten_ki_analyse = portal.normalize_document_text(gutachten_mit_ki.get("analyse_text"))
        check(
            "KI-Ergebnis wird um Vorschaden bereinigt",
            "tuer hinten links" not in gutachten_ki_analyse
            and "stossstange hinten" in gutachten_ki_analyse,
            gutachten_mit_ki.get("analyse_text"),
        )
        check(
            "OpenAI-Fehler wird benutzerfreundlich angezeigt",
            "https://api.openai.com" not in portal.friendly_analysis_error(
                "401 Client Error: Unauthorized for url: https://api.openai.com/v1/chat/completions",
                "OpenAI",
            )
            and "OpenAI-Zugang" in portal.friendly_analysis_error("401 Unauthorized", "OpenAI"),
        )

        autohaus = portal.get_autohaus_by_slug("kaesmann")
        check("Autohaus Käsmann vorhanden", bool(autohaus))

        admin = portal.app.test_client()
        partner = portal.app.test_client()
        with admin.session_transaction() as session:
            session["admin"] = True
        with partner.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]

        route_checks = (
            ("Admin Dashboard", admin, "/admin"),
            ("Admin Kalender", admin, "/admin/kalender"),
            ("Partner Dashboard", partner, "/partner/kaesmann/dashboard"),
            ("Partner Neues Fahrzeug", partner, "/partner/kaesmann/neu"),
            ("Partner Angebot neu", partner, "/partner/kaesmann/angebot/neu"),
        )
        for label, client, url in route_checks:
            response = client.get(url)
            check(label, response.status_code == 200, f"Status {response.status_code}")

        response = partner.post(
            "/partner/kaesmann/angebot/neu",
            data=with_csrf(partner, {
                "kunde_name": "Testkunde Codex",
                "telefon": "01234",
                "fahrzeug": "Audi Q7",
                "kennzeichen": "MOS-T 123",
                "fin_nummer": "WAUZZZTEST0000001",
                "auftragsnummer": "TEST-ANGEBOT-1",
                "analyse_text": "Kotflügel links lackieren",
                "beschreibung": "Kotflügel links lackieren. Bitte Angebot erstellen.",
                "transport_art": "standard",
                "annahme_datum": "02.05.2026",
                "abholtermin": "04.05.2026",
            }),
            follow_redirects=False,
        )
        check(
            "Angebotsanfrage erstellen leitet weiter",
            response.status_code in {302, 303},
            f"Status {response.status_code}",
        )
        angebot_id = extract_id_from_location(response.headers.get("Location", ""))
        check("Neue Angebots-ID erkannt", angebot_id > 0, response.headers.get("Location", ""))

        angebot = portal.get_auftrag(angebot_id)
        check("Neue Anfrage ist Angebotsphase", angebot["angebotsphase"])
        check("Anfrage ist noch nicht abgesendet", not angebot["angebot_abgesendet"])
        check(
            "Preisvorschlag wird nur berechnet",
            angebot["preisvorschlag"]["empfehlung"] == "190 € netto",
            str(angebot["preisvorschlag"]),
        )

        response = partner.post(
            f"/partner/kaesmann/angebot/{angebot_id}",
            data=with_csrf(partner, {
                "aktion": "submit_offer",
                "kunde_name": "Testkunde Codex",
                "fahrzeug": "Audi Q7",
                "kennzeichen": "MOS-T 123",
                "fin_nummer": "WAUZZZTEST0000001",
                "auftragsnummer": "TEST-ANGEBOT-1",
                "analyse_text": "Kotflügel links lackieren",
                "beschreibung": "Kotflügel links lackieren. Bitte Angebot erstellen.",
                "transport_art": "standard",
                "annahme_datum": "02.05.2026",
                "abholtermin": "04.05.2026",
            }),
            follow_redirects=False,
        )
        check("Kunde sendet Angebotsanfrage ab", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Angebot wartet auf Werkstatt",
            angebot["angebot_abgesendet"] and angebot["angebot_status"] == "angefragt",
            angebot["angebot_status"],
        )

        response = admin.get(f"/admin/auftrag/{angebot_id}")
        html = response.get_data(as_text=True)
        check("Admin-Angebotsseite lädt", response.status_code == 200)
        check(
            "Preisvorschlag im Admin sichtbar",
            "Preisvorschlag nach interner Preisliste" in html and "190 € netto" in html,
        )

        response = admin.post(
            f"/admin/angebot/{angebot_id}/senden",
            data=with_csrf(admin, {
                "werkstatt_angebot_preis": "190 € netto",
                "werkstatt_angebot_text": "Reparatur gemäß Anfrage: Kotflügel links lackieren.",
                "werkstatt_angebot_notiz": "Preis netto, inklusive Material.",
            }),
            follow_redirects=False,
        )
        check("Werkstatt sendet Angebot", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Angebot abgegeben gespeichert",
            angebot["angebot_status"] == "angebot_abgegeben"
            and angebot["werkstatt_angebot_preis"] == "190 € netto"
            and angebot["werkstatt_angebot_notiz"] == "Preis netto, inklusive Material.",
            angebot["angebot_status"],
        )

        response = admin.post(
            f"/admin/angebot/{angebot_id}/senden",
            data=with_csrf(admin, {
                "werkstatt_angebot_preis": "210 € netto",
                "werkstatt_angebot_text": "Reparatur gemäß Anfrage: Kotflügel links lackieren.",
                "werkstatt_angebot_notiz": "Preis netto, zusätzlich kleine Beilackierung.",
            }),
            follow_redirects=False,
        )
        check("Werkstatt ändert Angebot", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Geändertes Angebot gespeichert",
            angebot["angebot_status"] == "angebot_abgegeben"
            and angebot["werkstatt_angebot_preis"] == "210 € netto"
            and angebot["werkstatt_angebot_notiz"] == "Preis netto, zusätzlich kleine Beilackierung.",
            (
                f"Status {angebot['angebot_status']}, "
                f"Preis {angebot['werkstatt_angebot_preis']}, "
                f"Notiz {angebot['werkstatt_angebot_notiz']}"
            ),
        )

        response = partner.get(f"/partner/kaesmann/angebot/{angebot_id}")
        partner_html = response.get_data(as_text=True)
        check(
            "Partner sieht geänderte Angebotsnotiz",
            response.status_code == 200
            and "210 € netto" in partner_html
            and "zusätzlich kleine Beilackierung" in partner_html,
            f"Status {response.status_code}",
        )

        response = partner.post(
            f"/partner/kaesmann/angebot/{angebot_id}/annehmen",
            data=with_csrf(partner),
            follow_redirects=False,
        )
        check("Kunde nimmt Angebot an", response.status_code in {302, 303})
        auftrag = portal.get_auftrag(angebot_id)
        check(
            "Annahme macht normalen Auftrag",
            not auftrag["angebotsphase"] and auftrag["angebot_status"] == "angenommen",
            auftrag["angebot_status"],
        )

        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        check("Partner-Auftrag nach Annahme öffnet", response.status_code == 200)

        response = partner.post(
            f"/partner/kaesmann/auftrag/{angebot_id}/chat",
            data=with_csrf(partner, {"nachricht": "Ist die Rückgabe am Nachmittag möglich?"}),
            follow_redirects=False,
        )
        check("Partner sendet Chat-Nachricht", response.status_code in {302, 303})
        db = portal.get_db()
        chat = db.execute(
            """
            SELECT *
            FROM chat_nachrichten
            WHERE auftrag_id=? AND absender='autohaus'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Chat-Nachricht ist für Admin ungelesen", bool(chat) and chat["gelesen_admin"] == 0, str(chat))
        response = admin.get("/admin")
        admin_dashboard_html = response.get_data(as_text=True)
        check(
            "Admin-Dashboard zeigt neue Chat-Nachricht",
            "Neue Chat-Nachrichten" in admin_dashboard_html
            and "Ist die Rückgabe am Nachmittag möglich?" in admin_dashboard_html,
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}")
        admin_auftrag_html = response.get_data(as_text=True)
        check(
            "Admin sieht Chat im Auftrag",
            response.status_code == 200 and "Chat mit Autohaus" in admin_auftrag_html,
            f"Status {response.status_code}",
        )
        db = portal.get_db()
        chat = db.execute("SELECT gelesen_admin FROM chat_nachrichten WHERE id=?", (chat["id"],)).fetchone()
        db.close()
        check("Admin-Öffnen markiert Chat als gelesen", chat["gelesen_admin"] == 1)
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/chat",
            data=with_csrf(admin, {"nachricht": "Ja, Rückgabe am Nachmittag passt."}),
            follow_redirects=False,
        )
        check("Admin antwortet im Chat", response.status_code in {302, 303})
        db = portal.get_db()
        hinweis = db.execute(
            """
            SELECT *
            FROM benachrichtigungen
            WHERE auftrag_id=? AND titel='Neue Chat-Nachricht'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Admin-Chat erzeugt Autohaus-Hinweis", bool(hinweis), str(hinweis))
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        partner_chat_html = response.get_data(as_text=True)
        check(
            "Partner sieht Werkstatt-Antwort im Chat",
            "Ja, Rückgabe am Nachmittag passt." in partner_chat_html,
            partner_chat_html[:300],
        )

        db = portal.get_db()
        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, original_name, stored_name, mime_type, size, quelle, kategorie,
             dokument_typ, extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json,
             analyse_hinweis, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                angebot_id,
                "lackierauftrag-test.pdf",
                "codex-test.pdf",
                "application/pdf",
                0,
                "autohaus",
                "standard",
                "Lackierauftrag",
                "Audi Q7 FIN WAUZZZTEST0000001 Kennzeichen MOS-T 123 Kotflügel links lackieren",
                "Kotflügel links lackieren",
                "test",
                json.dumps(
                    {
                        "document_type": "Lackierauftrag",
                        "vehicle_type": "Audi Q7",
                        "fin_nummer": "WAUZZZTEST0000001",
                        "auftragsnummer": "TEST-ANGEBOT-1",
                        "kennzeichen": "MOS-T 123",
                        "auftrags_datum": "02.05.2026",
                        "fertig_bis": "04.05.2026",
                        "rep_max_kosten": "",
                        "offene_bauteile": ["Kotflügel links"],
                        "erledigte_bauteile": [],
                        "kurzanalyse": "Kotflügel links lackieren",
                        "lesefassung": "Lackierauftrag Audi Q7, Kotflügel links lackieren.",
                        "needs_review": False,
                        "review_reason": "",
                        "confidence": 0.91,
                    },
                    ensure_ascii=False,
                ),
                "",
                portal.now_str(),
            ),
        )
        db.commit()
        db.close()
        portal.reset_document_review_checks(angebot_id, "Testunterlage zur Prüfung")
        auftrag = portal.get_auftrag(angebot_id)
        pruefung = portal.list_document_review_items(angebot_id, auftrag)
        check("Dokument-Prüfansicht liefert Felder", bool(pruefung and pruefung[0]["items"]))
        check(
            "Dokument-Prüfansicht erkennt übernommene FIN",
            any(item["key"] == "fin_nummer" and item["active"] for item in pruefung[0]["items"]),
            str(pruefung[0]["items"]),
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}")
        check(
            "Admin zeigt Dokument-Prüfansicht",
            "Erkannte Felder aus Unterlagen" in response.get_data(as_text=True),
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        check(
            "Partner zeigt Dokument-Prüfansicht",
            "Erkannte Felder aus Unterlagen" in response.get_data(as_text=True),
        )
        response = partner.post(
            f"/partner/kaesmann/auftrag/{angebot_id}/dokumente/geprueft",
            data=with_csrf(partner, {}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Autohaus-Prüfung speicherbar", response.status_code in {302, 303}, response.headers.get("Location", ""))
        check(
            "Nach Autohaus-Prüfung bleibt Werkstatt-Prüfung offen",
            auftrag["analyse_autohaus_geprueft"] == 1
            and auftrag["analyse_werkstatt_geprueft"] == 0
            and auftrag["analyse_pruefen"],
            f"autohaus={auftrag['analyse_autohaus_geprueft']} werkstatt={auftrag['analyse_werkstatt_geprueft']} pruefen={auftrag['analyse_pruefen']}",
        )
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/dokumente/geprueft",
            data=with_csrf(admin, {}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Werkstatt-Prüfung speicherbar", response.status_code in {302, 303})
        check(
            "Doppelte Dokumentprüfung schließt Prüfwarnung",
            auftrag["analyse_autohaus_geprueft"] == 1
            and auftrag["analyse_werkstatt_geprueft"] == 1
            and not auftrag["analyse_pruefen"],
            f"autohaus={auftrag['analyse_autohaus_geprueft']} werkstatt={auftrag['analyse_werkstatt_geprueft']} pruefen={auftrag['analyse_pruefen']}",
        )

        response = admin.post(
            f"/admin/status/{angebot_id}/2",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status vorwärts möglich", response.status_code in {302, 303})
        check("Status ist Eingeplant", portal.get_auftrag(angebot_id)["status"] == 2)

        response = admin.post(
            f"/admin/status/{angebot_id}/1",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status rückwärts möglich", response.status_code in {302, 303})
        check("Status ist wieder Angelegt", portal.get_auftrag(angebot_id)["status"] == 1)

        response = admin.post(
            f"/admin/status/{angebot_id}/4",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status Fertig möglich", response.status_code in {302, 303})
        auftrag = portal.get_auftrag(angebot_id)
        check("Status ist Fertig", auftrag["status"] == 4)
        autohaus_cockpit = portal.autohaus_dashboard_daten([auftrag])
        check(
            "Partner-Dashboard zählt heute fertig nach Statuswechsel",
            any(a["id"] == angebot_id for a in autohaus_cockpit["heute_fertig"]),
            str([a["id"] for a in autohaus_cockpit["heute_fertig"]]),
        )
        admin_cockpit = portal.dashboard_daten([auftrag])
        check(
            "Admin-Dashboard zählt heute fertig nach Statuswechsel",
            any(a["id"] == angebot_id for a in admin_cockpit["heute_fertig"]),
            str([a["id"] for a in admin_cockpit["heute_fertig"]]),
        )

        response = admin.post(
            f"/admin/status/{angebot_id}/5",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Status Zurückgegeben möglich", response.status_code in {302, 303})
        check("Status ist Zurückgegeben", auftrag["status"] == 5)
        check("Rückgabetermin wird gesetzt", bool(auftrag["abholtermin"]), auftrag["abholtermin"])

        response = admin.get(f"/admin/auftrag/{angebot_id}")
        admin_auftrag_html = response.get_data(as_text=True)
        check(
            "Zurückgegebener Auftrag zeigt Rechnungslink",
            response.status_code == 200 and "Rechnung schreiben in Lexware" in admin_auftrag_html,
            f"Status {response.status_code}",
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}/rechnung")
        rechnung_html = response.get_data(as_text=True)
        check(
            "Rechnungsseite nach Rückgabe erreichbar",
            response.status_code == 200 and "Rechnung schreiben" in rechnung_html and "Direkt in Lexware" in rechnung_html,
            f"Status {response.status_code}",
        )

        response = admin.post(
            f"/admin/auftrag/{angebot_id}/fertigbilder",
            data=with_csrf(
                admin,
                {
                    "fertigbilder": (
                        BytesIO(b"codex-test-fertigbild"),
                        "fertigbild-test.jpg",
                    )
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Admin lädt Fertigbild hoch", response.status_code in {302, 303})
        db = portal.get_db()
        fertigbild = db.execute(
            """
            SELECT *
            FROM dateien
            WHERE auftrag_id=? AND kategorie='fertigbild'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        hinweis = db.execute(
            """
            SELECT *
            FROM benachrichtigungen
            WHERE auftrag_id=? AND titel='Neue Fertigbilder'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Fertigbild als Fertigbild gespeichert", bool(fertigbild), str(fertigbild))
        check("Fertigbild-Datei liegt im Upload-Ordner", (test_uploads / fertigbild["stored_name"]).exists())
        check("Fertigbild erzeugt Autohaus-Hinweis", bool(hinweis), str(hinweis))
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        partner_html = response.get_data(as_text=True)
        check(
            "Partner sieht Fertigbild",
            response.status_code == 200 and "fertigbild-test.jpg" in partner_html,
            f"Status {response.status_code}",
        )
        check(
            "Partner sieht Werkstatt-Hinweis",
            "Neue Fertigbilder" in partner_html,
            partner_html[:300],
        )

        db = portal.get_db()
        datei = db.execute("SELECT id FROM dateien LIMIT 1").fetchone()
        db.close()
        if datei:
            response = admin.get(f"/admin/datei/{datei['id']}")
            check("Admin Originaldatei öffnen Route", response.status_code in {200, 404})
            response = admin.get(f"/admin/datei/{datei['id']}/download")
            check("Admin Originaldatei Download Route", response.status_code in {200, 404})
        else:
            print("[INFO] Keine Datei in Testdatenbank gefunden, Datei-Routen übersprungen.")

    portal.DB = original_db
    portal.UPLOAD_DIR = original_upload_dir
    print("Flow-Test erfolgreich.")


if __name__ == "__main__":
    main()
