"""Kleine isolierte Sicherheits- und Renderingtests für die reine Designvorschau."""

import os
import pathlib
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    with tempfile.TemporaryDirectory(prefix="kundenstatus-schaden-test-") as temp_name:
        temp_dir = pathlib.Path(temp_name)
        preview_db = temp_dir / "test.db"
        os.environ.update(
            {
                "RENDER": "local-preview-test",
                "REQUIRE_POSTGRES_ON_RENDER": "0",
                "DATA_DIR": str(temp_dir),
                "SQLITE_DB_PATH": str(preview_db),
                "UPLOAD_DIR": str(temp_dir / "uploads"),
                "BACKUP_DIR": str(temp_dir / "backups"),
                "DELETED_UPLOAD_DIR": str(temp_dir / "deleted_uploads"),
                "AUTO_BACKUP_ENABLED": "0",
                "AUTO_CHANGE_BACKUP_ENABLED": "0",
                "LEXWARE_API_KEY": "",
                "OPENAI_API_KEY": "",
                "FLASK_SECRET_KEY": "schaden-vorschau-test",
                "ADMIN_PASS": "schaden-vorschau-test",
            }
        )

        import app as portal

        portal.app.config["TESTING"] = True
        client = portal.app.test_client()
        assert pathlib.Path(portal.DB).resolve() == preview_db.resolve()

        os.environ["SCHADEN_VORSCHAU_AKTIV"] = "0"
        assert client.get("/schaden-vorschau").status_code == 404

        os.environ["SCHADEN_VORSCHAU_AKTIV"] = "1"
        db_before = (preview_db.stat().st_size, preview_db.stat().st_mtime_ns)
        response = client.get("/schaden-vorschau")
        db_after = (preview_db.stat().st_size, preview_db.stat().st_mtime_ns)
        html = response.get_data(as_text=True)

        assert response.status_code == 200
        assert response.headers.get("Cache-Control") == "no-store"
        assert response.headers.get("X-Robots-Tag") == "noindex, nofollow, noarchive"
        assert "Lokale Designvorschau" in html
        assert "Meldung unverbindlich senden" in html
        assert "keine Reparatur- oder Versicherungsfreigabe" in html
        assert "Ersatzmobilität" in html
        assert db_after == db_before
        post_before = (preview_db.stat().st_size, preview_db.stat().st_mtime_ns)
        post_response = client.post("/schaden-vorschau", data={"kunde_name": "Test"})
        post_after = (preview_db.stat().st_size, preview_db.stat().st_mtime_ns)
        assert post_response.status_code in {400, 403, 405}
        assert post_after == post_before

        os.environ["SCHADEN_VORSCHAU_AKTIV"] = "0"
        with client.session_transaction() as test_session:
            test_session["admin"] = True
        admin_response = client.get("/admin/versicherungsschaden")
        admin_html = admin_response.get_data(as_text=True)
        assert admin_response.status_code == 200
        assert "Werkstatt-Cockpit · Schadenaufnahme, Fallakte und Kundenstatus direkt verbunden" in admin_html
        assert 'method="POST"' in admin_html
        assert 'enctype="multipart/form-data"' in admin_html
        assert 'name="dateien"' in admin_html
        assert "Schadenfall sicher speichern" in admin_html
        assert "lokale Arbeitsversion ohne Speicherung" not in admin_html
        assert "Alle Versicherungsfälle" in admin_html
        assert 'href="/admin/versicherung/faelle"' in admin_html

        old_entry_response = client.get("/admin/versicherung")
        assert old_entry_response.status_code == 302
        assert old_entry_response.headers["Location"].endswith("/admin/versicherungsschaden")
        cases_response = client.get("/admin/versicherung/faelle")
        assert cases_response.status_code == 200
        assert "Aktuelle Versicherungsschäden" in cases_response.get_data(as_text=True)

        cockpit_response = client.get("/admin/cockpit")
        cockpit_html = cockpit_response.get_data(as_text=True)
        assert cockpit_response.status_code == 200
        assert cockpit_html.count('href="/admin/versicherungsschaden"') >= 2
        assert "Versicherungsschaden aufnehmen und danach im Portal weiterbearbeiten." in cockpit_html

    print("Schadenvorschau/Cockpit-Einbau: alle Pruefungen erfolgreich")


if __name__ == "__main__":
    main()
