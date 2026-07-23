"""Isolierter E2E-Test für Fahrzeugeinkauf: Queue, Heartbeat, Import und Retry."""
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    ok = True

    def check(label, condition, detail=""):
        nonlocal ok
        passed = bool(condition)
        print(("[OK] " if passed else "[FEHLER] ") + label + (f" — {detail}" if detail and not passed else ""))
        ok &= passed

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        os.environ["RENDER"] = "1"
        os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "0"
        os.environ["DATABASE_URL"] = ""
        os.environ["DATA_DIR"] = str(tmp_path)
        os.environ["SQLITE_DB_PATH"] = str(tmp_path / "scan-test.db")
        os.environ["UPLOAD_DIR"] = str(tmp_path / "uploads")
        os.environ["BACKUP_DIR"] = str(tmp_path / "backups")
        os.environ["DELETED_UPLOAD_DIR"] = str(tmp_path / "deleted-uploads")
        os.environ["AUTO_BACKUP_ENABLED"] = "0"
        os.environ["AUTO_CHANGE_BACKUP_ENABLED"] = "0"
        os.environ["CODEX_BRIDGE_ENABLED"] = "0"
        os.environ["SESSION_COOKIE_SECURE"] = "0"
        os.environ["FLASK_SECRET_KEY"] = "scan-test-secret"
        os.environ["ADMIN_PASS"] = "scan-test-admin"
        os.environ["WERKSTATT_EMAIL_API_TOKEN"] = "scan-test-token"
        os.environ["FAHRZEUGEINKAUF_SCANNER_API_TOKEN"] = "scan-dedicated-token"

        import app as portal  # noqa: E402

        portal.app.config["TESTING"] = True
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True

        def csrf_token():
            client.get("/admin/fahrzeugeinkauf")
            with client.session_transaction() as session:
                return session["csrf_token"]

        def db_row(sql, params=()):
            with portal.app.test_request_context("/"):
                db = portal.get_db()
                try:
                    row = db.execute(sql, params).fetchone()
                    return dict(row) if row else None
                finally:
                    db.close()

        def latest_request():
            return db_row("SELECT * FROM fahrzeugeinkauf_scan_anfragen ORDER BY id DESC LIMIT 1")

        def request_scan(follow_redirects=False):
            return client.post(
                "/admin/fahrzeugeinkauf/scan-anfordern",
                data={"csrf_token": csrf_token()},
                follow_redirects=follow_redirects,
            )

        api_headers = {"X-Werkstatt-Token": "scan-dedicated-token"}

        # 1) Leerer Ausgangszustand.
        response = client.get("/admin/fahrzeugeinkauf/status")
        data = response.get_json() or {}
        check("Status-Endpoint antwortet ohne Anfrage", response.status_code == 200 and data.get("anfrage_offen") is False)

        # 2) Admin legt genau eine Queue-Anfrage an; offline wird ehrlich angezeigt.
        response = request_scan()
        first = latest_request()
        check("Scan-Anfrage wird angelegt", response.status_code in {302, 303} and first and first["status"] == "offen")
        first_id = int(first["id"])
        page = client.get(f"/admin/fahrzeugeinkauf?anfrage_id={first_id}").get_data(as_text=True)
        check("UI nennt wartenden Offline-Status statt laufenden Scan", "Scanner ist derzeit offline" in page and "Scan l&auml;uft" not in page)
        request_scan()
        active_count = db_row(
            "SELECT COUNT(*) AS n FROM fahrzeugeinkauf_scan_anfragen WHERE status IN ('offen', 'laeuft')"
        )
        check("Doppelklick erzeugt keine zweite aktive Anfrage", active_count and int(active_count["n"]) == 1)

        # 3) Der echte API-Poll claimt die Anfrage und setzt den Heartbeat.
        check("Queue-API schützt sich ohne Token", client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen").status_code == 401)
        check(
            "Breiter Werkstatt-Schlüssel darf Scanner-Queue nicht lesen",
            client.get(
                "/api/werkstatt/fahrzeugeinkauf/scan-anfragen",
                headers={"X-Werkstatt-Token": "scan-test-token"},
            ).status_code == 401,
        )
        response = client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers)
        queue = (response.get_json() or {}).get("anfragen") or []
        claimed = latest_request()
        first_versuch = int(claimed["versuche"])
        check("Queue-Poll liefert konkrete Anfrage", response.status_code == 200 and [item["id"] for item in queue] == [first_id])
        check("Queue-Poll setzt running + Start + Heartbeat", claimed["status"] == "laeuft" and claimed["gestartet_am"] and claimed["letztes_lebenszeichen"])
        second_poll = client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers)
        check(
            "Frisch geclaimter Auftrag wird nicht parallel erneut ausgegeben",
            second_poll.status_code == 200 and not (second_poll.get_json() or {}).get("anfragen"),
        )
        heartbeat = client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{first_id}/lebenszeichen",
            headers=api_headers,
            json={"versuch": first_versuch},
        )
        check("Worker kann den Request-Lease verlängern", heartbeat.status_code == 200)
        status_data = (client.get(f"/admin/fahrzeugeinkauf/status/{first_id}").get_json() or {})
        check("Anfragespezifischer Status erkennt aktiven Scanner", status_data.get("status") == "laeuft" and status_data.get("scanner_online") is True)
        offline_time = (
            datetime.now(timezone.utc)
            - timedelta(minutes=portal.FAHRZEUGEINKAUF_SCANNER_OFFLINE_MINUTES + 2)
        ).isoformat(timespec="seconds")
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute(
                "UPDATE fahrzeugeinkauf_scan_anfragen SET letztes_lebenszeichen=? WHERE id=?",
                (offline_time, first_id),
            )
            db.execute(
                "UPDATE app_settings SET value=? WHERE key=?",
                (offline_time, portal.FAHRZEUGEINKAUF_SCANNER_LAST_SEEN_SETTING),
            )
            db.commit()
            db.close()
        offline_status = client.get(f"/admin/fahrzeugeinkauf/status/{first_id}").get_json() or {}
        check(
            "Running ohne aktuellen Heartbeat wird nicht als aktive Suche behauptet",
            offline_status.get("scanner_online") is False
            and offline_status.get("status_label") == "Scanner ohne Rückmeldung"
            and offline_status.get("ueberfaellig") is False,
        )
        client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{first_id}/lebenszeichen",
            headers=api_headers,
            json={"versuch": first_versuch},
        )

        # 4) Import schließt exakt diese Anfrage ab und verknüpft den Treffer.
        vehicle = {
            "titel": "TEST Golf 7 R-Line",
            "preis": "14.200 €",
            "verhandlungsziel": "13.000 €",
            "marge": "4.000 €",
            "ampel": "gruen",
            "ez": "08/2019",
            "km": "107.000",
            "ps": "150",
            "ort": "Mosbach",
            "schaden": "Wildschaden, fahrbereit",
            "inserat_url": "https://www.kleinanzeigen.de/s-anzeige/test-fahrzeug/1/",
            "bild_url": "https://img.kleinanzeigen.de/api/v1/prod-ads/images/test.jpg",
            "verkaeufer_telefon": "');alert(1);//",
            "verkaeufer_email": "ungueltig'@example.test",
        }
        invalid_link_vehicle = dict(vehicle)
        invalid_link_vehicle["titel"] = "Darf nicht importiert werden"
        invalid_link_vehicle["inserat_url"] = "javascript:alert(2)"
        payload = {
            "anfrage_id": first_id,
            "versuch": first_versuch,
            "titel": "TEST Scan",
            "scan_datum": "19.07.2026",
            "quelle": "test-scanner",
            "fahrzeuge": [vehicle, invalid_link_vehicle],
        }
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={"payload": json.dumps(payload)},
        )
        imported = response.get_json() or {}
        check("Import legt einen neuen Treffer an", response.status_code == 200 and imported.get("fahrzeuge_angelegt") == 1)
        scan_id = int(imported.get("scan_id") or 0)
        completed = db_row("SELECT * FROM fahrzeugeinkauf_scan_anfragen WHERE id=?", (first_id,))
        hit = db_row("SELECT * FROM fahrzeugeinkauf_scan_treffer WHERE scan_id=?", (scan_id,))
        check("Nur die konkrete Anfrage wird abgeschlossen", completed["status"] == "erledigt" and int(completed["scan_id"]) == scan_id)
        check("Scan-Treffer ist mit Art neu verknüpft", hit and hit["art"] == "neu")
        safe_page = client.get(f"/admin/fahrzeugeinkauf?anfrage_id={first_id}").get_data(as_text=True)
        check(
            "Importdaten landen weder als JavaScript-URL noch in Inline-Handlern",
            "javascript:alert" not in safe_page
            and "onclick=" not in safe_page
            and "alert(1)" not in safe_page
            and "tel:1" not in safe_page
            and portal.fahrzeugeinkauf_telefon("');alert(1);//") == "",
        )
        status_data = (client.get(f"/admin/fahrzeugeinkauf/status/{first_id}").get_json() or {})
        check("Status meldet persistierte Ergebniszahlen", status_data.get("terminal") is True and status_data.get("fahrzeuge_angelegt") == 1)

        # Derselbe Worker-Retry muss idempotent bleiben.
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={"payload": json.dumps(payload)},
        )
        idem = response.get_json() or {}
        scans_count = db_row("SELECT COUNT(*) AS n FROM fahrzeugeinkauf_scans")
        check("Wiederholter Import bleibt idempotent", idem.get("idempotent") is True and int(scans_count["n"]) == 1)
        late_failure = client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{first_id}/fehlgeschlagen",
            headers=api_headers,
            json={"fehler": "Zu spät", "versuch": first_versuch},
        )
        check(
            "Verspätete Fehlermeldung überschreibt keinen Erfolg",
            late_failure.status_code == 409
            and db_row("SELECT status FROM fahrzeugeinkauf_scan_anfragen WHERE id=?", (first_id,))["status"] == "erledigt",
        )

        # 5) Ein späterer Scan zählt dasselbe Inserat als aktualisiert und zeigt es im neuen Scan.
        request_scan()
        second_id = int(latest_request()["id"])
        second_claim = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        second_versuch = int(second_claim[0]["versuche"])
        updated_payload = dict(payload)
        updated_payload["anfrage_id"] = second_id
        updated_payload["versuch"] = second_versuch
        updated_payload["titel"] = "TEST Kontakt-Update"
        updated_vehicle = dict(vehicle)
        updated_vehicle["preis"] = "13.900 €"
        updated_payload["fahrzeuge"] = [updated_vehicle]
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={"payload": json.dumps(updated_payload)},
        )
        updated = response.get_json() or {}
        check("Deduplizierter Treffer wird als aktualisiert gezählt", updated.get("fahrzeuge_angelegt") == 0 and updated.get("fahrzeuge_aktualisiert") == 1)
        page = client.get(f"/admin/fahrzeugeinkauf?anfrage_id={second_id}").get_data(as_text=True)
        check("Archiv zeigt aktualisierte Treffer statt 0 Fahrzeuge", "0 neu" in page and "1 aktualisiert" in page and "AKTUALISIERT" in page)

        # 6) Null Treffer ist ein eigener erfolgreicher Zustand, kein Fehler.
        request_scan()
        empty_id = int(latest_request()["id"])
        empty_claim = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        empty_versuch = int(empty_claim[0]["versuche"])
        empty_payload = {
            "anfrage_id": empty_id,
            "versuch": empty_versuch,
            "titel": "TEST Leerer Scan",
            "fahrzeuge": [],
        }
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={"payload": json.dumps(empty_payload)},
        )
        empty_data = response.get_json() or {}
        check("Leerer Scan wird sauber als leer abgeschlossen", response.status_code == 200 and empty_data.get("status") == "leer")

        # 7) Fehlerhafte Daten schließen den Auftrag nicht still ab; Worker kann Fehler melden.
        request_scan()
        failed_id = int(latest_request()["id"])
        standalone_payload = {
            "titel": "TEST Unabhängiger Tageslauf",
            "fahrzeuge": [
                dict(
                    vehicle,
                    titel="Standalone Treffer",
                    inserat_url="https://www.kleinanzeigen.de/s-anzeige/standalone/2",
                )
            ],
        }
        standalone_response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={"payload": json.dumps(standalone_payload)},
        )
        check(
            "Requestloser Tagesimport kapert keine aktive Button-Anfrage",
            standalone_response.status_code == 200
            and (standalone_response.get_json() or {}).get("anfrage_id") == 0
            and latest_request()["status"] == "offen",
        )
        failed_claim = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        failed_versuch = int(failed_claim[0]["versuche"])
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={
                "payload": json.dumps(
                    {"anfrage_id": failed_id, "versuch": failed_versuch, "fahrzeuge": ["kaputt"]}
                )
            },
        )
        check("Ungültige Fahrzeugdaten werden abgewiesen", response.status_code == 400 and latest_request()["status"] == "laeuft")
        response = client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{failed_id}/fehlgeschlagen",
            headers=api_headers,
            json={"fehler": "Testfehler im Browserlauf", "versuch": failed_versuch},
        )
        failed = latest_request()
        check("Worker kann einen sichtbaren Fehlerstatus melden", response.status_code == 200 and failed["status"] == "fehlgeschlagen" and "Testfehler" in failed["fehler"])

        # Ein nach Lease-Ablauf ersetzter Worker darf nicht mehr schreiben.
        request_scan()
        lease_id = int(latest_request()["id"])
        lease_claim_1 = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        lease_versuch_1 = int(lease_claim_1[0]["versuche"])
        lease_stale_time = (
            datetime.now(timezone.utc)
            - timedelta(minutes=portal.FAHRZEUGEINKAUF_SCANNER_OFFLINE_MINUTES + 2)
        ).isoformat(timespec="seconds")
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute(
                "UPDATE fahrzeugeinkauf_scan_anfragen SET letztes_lebenszeichen=? WHERE id=?",
                (lease_stale_time, lease_id),
            )
            db.commit()
            db.close()
        lease_claim_2 = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        lease_versuch_2 = int(lease_claim_2[0]["versuche"])
        stale_heartbeat_response = client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{lease_id}/lebenszeichen",
            headers=api_headers,
            json={"versuch": lease_versuch_1},
        )
        stale_failure_response = client.post(
            f"/api/werkstatt/fahrzeugeinkauf/scan-anfragen/{lease_id}/fehlgeschlagen",
            headers=api_headers,
            json={"versuch": lease_versuch_1, "fehler": "alter Worker"},
        )
        stale_import_response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={
                "payload": json.dumps(
                    {"anfrage_id": lease_id, "versuch": lease_versuch_1, "fahrzeuge": []}
                )
            },
        )
        check(
            "Reclaim sperrt Heartbeat, Fehler und Import des alten Workers",
            lease_versuch_2 == lease_versuch_1 + 1
            and stale_heartbeat_response.status_code == 409
            and stale_failure_response.status_code == 409
            and stale_import_response.status_code == 409,
        )
        current_import_response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={
                "payload": json.dumps(
                    {"anfrage_id": lease_id, "versuch": lease_versuch_2, "fahrzeuge": []}
                )
            },
        )
        check("Nur der aktuelle Lease kann abschließen", current_import_response.status_code == 200)

        # Zwei echte Parallel-Retries derselben ID dürfen nur einen Scan erzeugen.
        request_scan()
        race_id = int(latest_request()["id"])
        race_claim = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        race_versuch = int(race_claim[0]["versuche"])
        race_payload = {
            "anfrage_id": race_id,
            "versuch": race_versuch,
            "titel": "TEST Parallelimport",
            "fahrzeuge": [
                dict(
                    vehicle,
                    titel="Parallel Treffer",
                    inserat_url="https://www.kleinanzeigen.de/s-anzeige/parallel/3",
                )
            ],
        }

        def parallel_import(_index):
            thread_client = portal.app.test_client()
            return thread_client.post(
                "/api/werkstatt/fahrzeugeinkauf/import",
                headers=api_headers,
                data={"payload": json.dumps(race_payload)},
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            race_responses = list(pool.map(parallel_import, range(2)))
        race_scans = db_row(
            "SELECT COUNT(*) AS n FROM fahrzeugeinkauf_scans WHERE anfrage_id=?", (race_id,)
        )
        check(
            "Parallele Import-Retries bleiben datenbankweit idempotent",
            all(response.status_code == 200 for response in race_responses)
            and int(race_scans["n"]) == 1
            and any((response.get_json() or {}).get("idempotent") for response in race_responses),
        )

        # Ungültige PDF-Datei wird vor dem DB-Import verworfen.
        request_scan()
        bad_pdf_id = int(latest_request()["id"])
        bad_pdf_claim = (
            client.get("/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers).get_json()
            or {}
        ).get("anfragen") or []
        bad_pdf_versuch = int(bad_pdf_claim[0]["versuche"])
        response = client.post(
            "/api/werkstatt/fahrzeugeinkauf/import",
            headers=api_headers,
            data={
                "payload": json.dumps(
                    {
                        "anfrage_id": bad_pdf_id,
                        "versuch": bad_pdf_versuch,
                        "fahrzeuge": [],
                    }
                ),
                "pdf": (BytesIO(b"keine pdf"), "scan.pdf"),
            },
            content_type="multipart/form-data",
        )
        check("PDF-Upload prüft den Dateiinhalt", response.status_code == 400 and latest_request()["status"] == "laeuft")

        # 8) Auch wiederholtes Reclaiming darf die absolute Laufzeit nicht endlos verlängern.
        old_time = (
            datetime.now(timezone.utc)
            - timedelta(minutes=portal.FAHRZEUGEINKAUF_SCAN_MAX_RUNTIME_MINUTES + 5)
        ).isoformat(timespec="seconds")
        stale_heartbeat = (
            datetime.now(timezone.utc)
            - timedelta(minutes=portal.FAHRZEUGEINKAUF_SCANNER_OFFLINE_MINUTES + 2)
        ).isoformat(timespec="seconds")
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute(
                """
                UPDATE fahrzeugeinkauf_scan_anfragen
                SET status='laeuft', gestartet_am=?, angefordert_am=?, letztes_lebenszeichen=?
                WHERE id=?
                """,
                (old_time, old_time, stale_heartbeat, bad_pdf_id),
            )
            db.commit()
            db.close()
        overdue_poll = client.get(
            "/api/werkstatt/fahrzeugeinkauf/scan-anfragen", headers=api_headers
        ).get_json() or {}
        overdue_status = client.get(
            f"/admin/fahrzeugeinkauf/status/{bad_pdf_id}"
        ).get_json() or {}
        check(
            "Absolute Laufzeit bleibt trotz Reclaim-Versuch überfällig",
            not overdue_poll.get("anfragen") and overdue_status.get("ueberfaellig") is True,
        )
        response = request_scan()
        replacement = latest_request()
        stale = db_row("SELECT * FROM fahrzeugeinkauf_scan_anfragen WHERE id=?", (bad_pdf_id,))
        check("Timeout markiert alten Auftrag als fehlgeschlagen", stale["status"] == "fehlgeschlagen")
        check("Retry erzeugt eine neue aktive Anfrage", response.status_code in {302, 303} and int(replacement["id"]) != bad_pdf_id and replacement["status"] == "offen")

        # 9) Die neue Treffer-Verknüpfung muss im Backup enthalten und restorebar sein.
        v2_kompatibel = True
        try:
            v2_tables = {
                name: []
                for name in portal.BACKUP_TABLES
                if name not in {"datei_backups", "fahrzeugeinkauf_scan_treffer"}
            }
            portal.validate_backup_binary_reference_completeness(
                {"format_version": 2, "tables": v2_tables}, {}
            )
        except Exception:
            v2_kompatibel = False
        backup_path = portal.create_backup_package("scan-test")
        with zipfile.ZipFile(backup_path, "r") as archive:
            names = set(archive.namelist())
            export = json.loads(archive.read("backup.json").decode("utf-8"))
            exported_hits = export.get("tables", {}).get("fahrzeugeinkauf_scan_treffer") or []
            portal.import_backup_json_rows_into_current_database(export, archive, names)
        restored_hit = db_row(
            "SELECT * FROM fahrzeugeinkauf_scan_treffer WHERE scan_id=?", (scan_id,)
        )
        check(
            "Backup v3 enthält und restauriert Scan-Treffer; v2 bleibt lesbar",
            portal.BACKUP_FORMAT_VERSION == 3
            and v2_kompatibel
            and bool(exported_hits)
            and restored_hit is not None,
        )

        print("\nErgebnis:", "ALLE CHECKS GRÜN" if ok else "MINDESTENS EIN CHECK ROT")
        return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
