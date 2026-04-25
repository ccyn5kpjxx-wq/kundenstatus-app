from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def check(label, response, expected_statuses):
    if response.status_code not in expected_statuses:
        print(f"[FEHLER] {label}: Status {response.status_code}, erwartet {sorted(expected_statuses)}")
        return False
    print(f"[OK] {label}: Status {response.status_code}")
    return True


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()
    ok = True

    ok &= check("Login-Seite", client.get("/login"), {200})
    ok &= check("Admin ohne Login geschuetzt", client.get("/admin"), {302})
    ok &= check("Partner-Einstieg", client.get("/partner"), {200})

    with client.session_transaction() as session:
        session["admin"] = True
    ok &= check("Admin mit Login", client.get("/admin"), {200})

    autohaus = portal.get_autohaus_by_slug("kaesmann")
    if autohaus:
        client = portal.app.test_client()
        ok &= check(
            "Käsmann-Dashboard ohne Partner-Login geschuetzt",
            client.get("/partner/kaesmann/dashboard"),
            {302},
        )
        with client.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]
        ok &= check(
            "Käsmann-Dashboard mit Partner-Login",
            client.get("/partner/kaesmann/dashboard"),
            {200},
        )
    else:
        print("[INFO] Autohaus 'kaesmann' existiert lokal nicht, Partner-Dashboard-Test uebersprungen.")

    if not ok:
        raise SystemExit(1)
    print("Smoke-Test erfolgreich.")


if __name__ == "__main__":
    main()
