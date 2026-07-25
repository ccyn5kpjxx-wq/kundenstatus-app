from datetime import datetime, timedelta
from pathlib import Path
import os
import sqlite3
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# app.py initialisiert die Datenbank bereits beim Import. Deshalb zeigen alle
# Pfade und Hintergrunddienste schon vorher ausdrücklich auf eine Testumgebung.
TEST_RUNTIME = tempfile.TemporaryDirectory(prefix="gaertner-analytics-test-")
TEST_ROOT = Path(TEST_RUNTIME.name)
os.environ["DATABASE_URL"] = ""
os.environ["RENDER"] = "1"
os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "0"
os.environ["DATA_DIR"] = str(TEST_ROOT)
os.environ["SQLITE_DB_PATH"] = str(TEST_ROOT / "app-test.db")
os.environ["UPLOAD_DIR"] = str(TEST_ROOT / "uploads")
os.environ["BACKUP_DIR"] = str(TEST_ROOT / "backups")
os.environ["DELETED_UPLOAD_DIR"] = str(TEST_ROOT / "deleted-uploads")
os.environ["AUTO_BACKUP_ENABLED"] = "0"
os.environ["AUTO_CHANGE_BACKUP_ENABLED"] = "0"
os.environ["LEXWARE_API_KEY"] = ""

import app as portal  # noqa: E402


def check(label, passed):
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    return bool(passed)


def main():
    portal.app.config["TESTING"] = True
    original_get_db = portal.get_db
    original_render_template = portal.render_template
    original_public_only = portal.PUBLIC_SITE_ONLY
    ok = True

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "besucher-test.db"

        def temp_get_db():
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            return connection

        database = temp_get_db()
        database.executescript(
            """
            CREATE TABLE besucher_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                erstellt_am TEXT NOT NULL,
                visitor_hash TEXT NOT NULL,
                website TEXT DEFAULT 'auto-lackierzentrum',
                seite TEXT DEFAULT '',
                referrer_domain TEXT DEFAULT '',
                geraet TEXT DEFAULT '',
                browser TEXT DEFAULT '',
                ist_bot INTEGER DEFAULT 0
            );
            """
        )
        database.execute(
            """
            INSERT INTO besucher_events
                (erstellt_am, visitor_hash, website, seite, referrer_domain, geraet, browser, ist_bot)
            VALUES (?, 'alt', 'auto-lackierzentrum', '/', 'Direkt / unbekannt', 'Desktop', 'Sonstiges', 0)
            """,
            ((datetime.now() - timedelta(days=91)).strftime("%Y-%m-%d %H:%M:%S"),),
        )
        database.commit()
        database.close()

        portal.get_db = temp_get_db
        try:
            client = portal.app.test_client()
            chrome_ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36"
            )
            common = {
                "json": {"website": "auto-lackierzentrum", "seite": "/homepage", "referrer": "www.google.de"},
                "headers": {"User-Agent": chrome_ua, "CF-Connecting-IP": "203.0.113.10", "Origin": "https://auto-lackierzentrum.de"},
                "base_url": "https://auto-lackierzentrum.de",
            }
            first = client.post("/api/besucher", **common)
            second = client.post("/api/besucher", **common)
            mobile = client.post(
                "/api/besucher",
                json={"website": "auto-lackierzentrum", "seite": "/team", "referrer": "auto-lackierzentrum.de"},
                headers={
                    "User-Agent": "Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
                    "CF-Connecting-IP": "2001:db8::2",
                    "Origin": "https://www.auto-lackierzentrum.de",
                },
                base_url="https://auto-lackierzentrum.de",
            )
            bot = client.post(
                "/api/besucher",
                json={"website": "auto-lackierzentrum", "seite": "/", "referrer": ""},
                headers={"User-Agent": "Googlebot/2.1", "CF-Connecting-IP": "203.0.113.99", "Origin": "https://auto-lackierzentrum.de"},
                base_url="https://auto-lackierzentrum.de",
            )
            tomorrowworks = client.post(
                "/api/besucher",
                data='{"website":"tomorrowworks","seite":"/websites.html","referrer":"www.google.de"}',
                content_type="text/plain;charset=UTF-8",
                headers={"User-Agent": chrome_ua, "CF-Connecting-IP": "203.0.113.10", "Origin": "https://www.tomorrowworks-agentur.de"},
                base_url="https://kundenstatus-app.onrender.com",
            )
            invalid = client.post(
                "/api/besucher",
                json={"website": "tomorrowworks", "seite": "/admin", "referrer": ""},
                headers={"User-Agent": chrome_ua, "CF-Connecting-IP": "203.0.113.55", "Origin": "https://auto-lackierzentrum.de"},
                base_url="https://kundenstatus-app.onrender.com",
            )
            statistik = portal.list_besucher_statistik(7, website="alle")
            auto_statistik = portal.list_besucher_statistik(7, website="auto-lackierzentrum")
            tomorrowworks_statistik = portal.list_besucher_statistik(7, website="tomorrowworks")
            database = temp_get_db()
            rows = database.execute(
                "SELECT visitor_hash, website, seite, referrer_domain, geraet, browser, ist_bot FROM besucher_events ORDER BY id"
            ).fetchall()
            database.close()

            ok &= check("Beacon-Endpunkt antwortet ohne Inhalt", all(
                response.status_code == 204 for response in (first, second, mobile, bot, tomorrowworks, invalid)
            ))
            ok &= check("CORS erlaubt nur die passende Website", tomorrowworks.headers.get("Access-Control-Allow-Origin") == "https://www.tomorrowworks-agentur.de" and not invalid.headers.get("Access-Control-Allow-Origin"))
            ok &= check("Drei Websites werden gemeinsam ausgewertet", statistik["aufrufe"] == 4 and statistik["besucher"] == 3 and len(statistik["websites"]) == 3)
            ok &= check("Auto-Lackierzentrum separat auswertbar", auto_statistik["aufrufe"] == 3 and auto_statistik["besucher"] == 2)
            ok &= check("Tomorrowworks separat auswertbar", tomorrowworks_statistik["aufrufe"] == 1 and tomorrowworks_statistik["besucher"] == 1)
            ok &= check("Bots werden separat ausgewiesen", statistik["bot_aufrufe"] == 1)
            ok &= check("Homepage-Pfade werden zusammengeführt", auto_statistik["seiten"][0]["seite"] == "/")
            ok &= check("Herkunft nur als Domain gespeichert", rows[0]["referrer_domain"] == "www.google.de")
            ok &= check("Interne Herkunft erkannt", rows[2]["referrer_domain"] == "Intern")
            ok &= check("Gerät und Browser klassifiziert", rows[2]["geraet"] == "Mobil" and rows[2]["browser"] == "Safari")
            ok &= check("Keine Roh-IP in Ereignisdaten", all("203.0.113" not in row["visitor_hash"] for row in rows))
            ok &= check("Unpassende Herkunft wird ignoriert", len(rows) == 5)
            ok &= check("Besucher-Hashes bleiben je Website getrennt", rows[0]["visitor_hash"] != rows[4]["visitor_hash"])
            ok &= check("90-Tage-Löschfrist greift", all(row["visitor_hash"] != "alt" for row in rows))

            captured = {}
            portal.render_template = lambda name, **context: captured.update(
                {"name": name, "statistik": context.get("statistik")}
            ) or "analytics-ok"
            unauthenticated = client.get("/admin/besucherstatistik")
            with client.session_transaction() as session:
                session["admin"] = True
                session["last_active"] = datetime.now().timestamp()
            admin_view = client.get("/admin/besucherstatistik?tage=7&website=tomorrowworks")
            before_admin_beacon = len(rows)
            admin_beacon = client.post(
                "/api/besucher",
                json=common["json"],
                headers=common["headers"],
            )
            database = temp_get_db()
            after_admin_beacon = database.execute("SELECT COUNT(*) AS n FROM besucher_events").fetchone()["n"]
            database.close()
            ok &= check("Analytics ist nur für Admins erreichbar", unauthenticated.status_code in {302, 303})
            ok &= check("Admin-Auswertung nutzt Website- und Zeitraumfilter", admin_view.status_code == 200 and captured.get("name") == "besucherstatistik_admin.html" and captured.get("statistik", {}).get("tage") == 7 and captured.get("statistik", {}).get("website") == "tomorrowworks")
            ok &= check("Eigene Admin-Aufrufe werden nicht gezählt", admin_beacon.status_code == 204 and after_admin_beacon == before_admin_beacon)

            portal.PUBLIC_SITE_ONLY = True
            public_client = portal.app.test_client()
            public_response = public_client.post(
                "/api/besucher",
                json={"website": "auto-lackierzentrum", "seite": "/", "referrer": ""},
                headers={"User-Agent": "UptimeRobot/2.0", "CF-Connecting-IP": "203.0.113.77", "Origin": "https://auto-lackierzentrum.de"},
                base_url="https://auto-lackierzentrum.de",
            )
            ok &= check("Beacon ist im reinen Homepage-Betrieb freigegeben", public_response.status_code == 204)
        finally:
            portal.get_db = original_get_db
            portal.render_template = original_render_template
            portal.PUBLIC_SITE_ONLY = original_public_only

        render_client = portal.app.test_client()
        with render_client.session_transaction() as session:
            session["admin"] = True
            session["last_active"] = datetime.now().timestamp()
        rendered = render_client.get("/admin/besucherstatistik?tage=30")
        rendered_html = rendered.get_data(as_text=True)
        ok &= check(
            "Admin-Dashboard rendert vollständig",
            rendered.status_code == 200
            and "Besucher-Analytics" in rendered_html
            and "Besucher-Summe" in rendered_html
            and "Tomorrowworks" in rendered_html
            and "Autovermietung MOS" in rendered_html
            and "Erkannte Bots" in rendered_html,
        )
        mietwagen_info = render_client.get("/mietwagen-info")
        mietwagen_anfrage = render_client.get("/mietwagen")
        ok &= check(
            "Öffentliche Mietwagen-Seiten werden mitgezählt",
            mietwagen_info.status_code == 200
            and mietwagen_anfrage.status_code == 200
            and "/api/besucher" in mietwagen_info.get_data(as_text=True)
            and "/api/besucher" in mietwagen_anfrage.get_data(as_text=True),
        )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
