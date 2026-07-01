"""Feature-Test: Felgen-Service in den Leistungen + News-Seeds (Felgen & PPG DigiMatch)."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    ok = True

    def check(label, cond):
        nonlocal ok
        print(("[OK] " if cond else "[FEHLER] ") + label)
        ok &= bool(cond)

    # 1) Seeds vorhanden und sichtbar
    with portal.app.test_request_context("/"):
        news = portal.list_werkstatt_news(limit=200)
    keys = {item.get("news_key") for item in news}
    check("News-Seed 'felgen-service-2026' sichtbar", "felgen-service-2026" in keys)
    check("News-Seed 'ppg-digimatch-farbtonmessung' sichtbar", "ppg-digimatch-farbtonmessung" in keys)

    felgen = next((n for n in news if n.get("news_key") == "felgen-service-2026"), {})
    digimatch = next((n for n in news if n.get("news_key") == "ppg-digimatch-farbtonmessung"), {})
    check("Felgen-News gepinnt", bool(felgen.get("pinned")))
    check("DigiMatch-News gepinnt", bool(digimatch.get("pinned")))
    check("Felgen-News nennt Bordsteinkratzer + Pulverbeschichtung",
          "Bordstein" in (felgen.get("nachricht") or "") and "Pulverbeschichtung" in (felgen.get("nachricht") or ""))
    check("DigiMatch-News nennt PPG DigiMatch", "PPG DigiMatch" in (digimatch.get("titel") or ""))

    # 2) Seed ist idempotent (zweiter Lauf erzeugt keine Duplikate)
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        portal.seed_default_werkstatt_news(db)
        db.commit()
        rows = db.execute(
            "SELECT COUNT(*) AS c FROM werkstatt_news WHERE news_key=?",
            ("felgen-service-2026",),
        ).fetchone()
    check("Seed idempotent (genau 1 Felgen-News)", rows["c"] == 1)

    # 3) Partner-Dashboard zeigt neue Leistung + beide News
    autohaus = portal.get_autohaus_by_slug("kaesmann")
    if not autohaus:
        print("[WARN] Kein Käsmann-Autohaus in der Dev-DB — Dashboard-Render-Check übersprungen")
    else:
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]
        response = client.get(f"/partner/{autohaus['slug']}/dashboard")
        check("Partner-Dashboard lädt (200)", response.status_code == 200)
        html = response.get_data(as_text=True)
        check("Leistung 'Felgenaufbereitung' im Dashboard", "Felgenaufbereitung" in html)
        check("Leistungstext nennt Felgen abdrehen + Pulverbeschichtung",
              "Felgen abdrehen bei Bordsteinsch" in html and "Pulverbeschichtung" in html)
        check("News 'Felgen abdrehen & Pulverbeschichten' im Dashboard",
              "Felgen abdrehen &amp; Pulverbeschichten" in html or "Felgen abdrehen & Pulverbeschichten" in html)
        check("News 'PPG DigiMatch' im Dashboard", "PPG DigiMatch" in html)

    print("Feature-Test erfolgreich." if ok else "Feature-Test FEHLGESCHLAGEN.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
