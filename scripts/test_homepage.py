from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def print_checks(checks):
    for label, passed in checks.items():
        print(f"[{'OK' if passed else 'FEHLER'}] {label}")


def main():
    portal.app.config["TESTING"] = True
    client = portal.app.test_client()
    response = client.get("/homepage")
    html = response.get_data(as_text=True)
    checks = {
        "Status 200": response.status_code == 200,
        "Homepage nicht veraltet cachen": "no-store" in response.headers.get("Cache-Control", ""),
        "Eine Hauptueberschrift im Hero": html.count("<h1") == 1 and "Karosserie, Lack &amp; Schadenservice in Mosbach." in html,
        "Hero hat zwei klare Aktionen": "Schaden melden" in html and "Termin &amp; Beratung" in html,
        "Vier Hauptkategorien": html.count('class="route-card') == 4 and all(
            f'href="#{anchor}"' in html for anchor in ("schaden", "mietwagen", "fahrzeugcheck", "werkstatt")
        ),
        "Portale nicht als Kundenkategorie": 'href="#portale"' not in html and 'href="/portale"' in html,
        "Details auf Wunsch erreichbar": 'class="category-disclosure"' in html and "Leistungen und Möglichkeiten im Detail" in html,
        "Fahrzeugcheck klar vermarktet": all(label in html for label in (
            "Gebrauchtwagen-Kaufcheck", "Leasingfahrzeug vor der Rückgabe kontrollieren",
            "Besichtigung beim Verkäufer gegen Aufpreis", "Fotodokumentation",
            "kein amtliches Gutachten", "endgültige Leasingbewertung"
        )),
        "Zentrales Anfrageformular verlinkt": '/anfrage?anliegen=' in html and "mailto:info@auto-lackierzentrum.de?subject=" not in html,
        "Mobile Navigation vorhanden": all(marker in html for marker in (
            'class="nav-toggle"', 'aria-controls="hauptmenue"', 'class="nav-menu"', "aria-expanded"
        )),
        "Mobile Schnellleiste reduziert": 'aria-label="Mobile Schnellaktionen"' in html and html.count('class="outline"') == 1,
        "Fruehe Vertrauenssignale": 'class="proof-strip"' in html and all(label in html for label in (
            "Google-Bewertungen", "Mosbach-Lohrbach", "Reparaturstatus online", "Persönlich erreichbar"
        )),
        "Unbestaetigte Oeffnungszeiten entfernt": "Mo–Sa:" not in html and '"openingHoursSpecification"' not in html,
        "Technik kompakt kundenorientiert": all(label in html for label in (
            "Farbton treffen statt schätzen", "Digitale Mehrwinkel-Messung",
            "/static/homepage/ppg-logo.svg", "/static/homepage/top-color-logo.png"
        )) and 'class="technology-grid"' not in html,
        "Vertrauen gebuendelt": 'class="confidence"' in html and 'class="trust-grid"' not in html and 'class="personal-card"' not in html,
        "Team kompakt auf der Homepage": all(marker in html for marker in (
            'class="team-preview"', "homepage/team-daniel-hannes-abdul-v2.png",
            "Wir arbeiten als Team", "Gute Arbeit entsteht", "Unser Team kennenlernen",
            "Digital starten", "Persönlich abstimmen", "Wir kümmern uns"
        )) and 'class="process-step"' not in html,
        "Google-Sterne dynamisch": "{{ '★' * rounded_rating }}" not in html and 'aria-label="' in html,
        "Strukturierte Unternehmensdaten": '"@type":"AutoBodyShop"' in html,
        "Sprunglink vorhanden": 'class="skip-link" href="#inhalt"' in html,
        "Hero-Bild vorgeladen": 'rel="preload" as="image"' in html,
        "Optimiertes Hero eingebunden": '/static/homepage/werkstatt-hero-v3.webp' in html
        and (ROOT / "static" / "homepage" / "werkstatt-hero-v3.webp").stat().st_size < 200_000,
        "Lazy Loading": html.count('loading="lazy"') >= 7,
        "Vorschau nicht indexierbar": 'name="robots" content="noindex, nofollow"' in html,
    }
    print_checks(checks)

    captured = {}
    original_create_lead = portal.create_lead
    original_backup = portal.schedule_change_backup
    def fake_create_lead(payload):
        captured["payload"] = payload
        return 991

    portal.create_lead = fake_create_lead
    portal.schedule_change_backup = lambda _reason: None
    try:
        form_get = client.get("/anfrage?anliegen=leasingrueckgabe")
        form_html = form_get.get_data(as_text=True)
        with client.session_transaction() as sess:
            csrf_token = sess.get(portal.CSRF_FIELD_NAME)
        form_post = client.post(
            "/anfrage",
            data={
                portal.CSRF_FIELD_NAME: csrf_token,
                "anliegen": "leasingrueckgabe",
                "besichtigungsart": "vor_ort",
                "name": "Test Kunde",
                "telefon": "0171 1234567",
                "email": "kunde@example.test",
                "fahrzeug": "VW Golf 8",
                "wunschdatum": "",
                "fahrzeug_link": "https://example.test/fahrzeug",
                "nachricht": "Bitte vor Rückgabe prüfen.",
                "website": "",
            },
            follow_redirects=False,
        )
        success = client.get(form_post.headers.get("Location", "")) if form_post.status_code == 302 else None
        form_checks = {
            "Anfrageformular Status 200": form_get.status_code == 200,
            "Leasing vorausgewaehlt": 'value="leasingrueckgabe" selected' in form_html,
            "CSRF und Honeypot": 'name="csrf_token"' in form_html and 'name="website"' in form_html,
            "Anfrage PRG": form_post.status_code == 302 and "gesendet=1" in form_post.headers.get("Location", ""),
            "Website-Lead gespeichert": captured.get("payload", {}).get("quelle") == "website",
            "Vor-Ort-Aufpreis dokumentiert": "gegen Aufpreis" in captured.get("payload", {}).get("beschreibung", ""),
            "Keine WhatsApp-Einwilligung behauptet": "keine WhatsApp-Einwilligung" in captured.get("payload", {}).get("notiz", ""),
            "Bestaetigung ohne Lead-ID": success is not None and success.status_code == 200 and "Ihre Anfrage ist angekommen" in success.get_data(as_text=True) and "991" not in success.get_data(as_text=True),
        }
    finally:
        portal.create_lead = original_create_lead
        portal.schedule_change_backup = original_backup
    print_checks(form_checks)

    team_html = client.get("/team").get_data(as_text=True)
    leistungen_html = client.get("/leistungen").get_data(as_text=True)
    category_checks = {
        "Geschaeftsfuehrer auf Teamseite": all(marker in team_html for marker in (
            "Christopher Gärtner", "Geschäftsführer", "homepage/portrait.webp", "member is-lead"
        )),
        "Gruppenfoto auf Teamseite": all(marker in team_html for marker in (
            "team-group", "homepage/team-daniel-hannes-abdul-v2.png", "Daniel, Hannes &amp; Abdul",
            "Gute Arbeit entsteht", "Unser Anspruch bei Gärtner"
        )),
        "Echtes Hannes-Foto in der Einzelkarte": all(marker in team_html for marker in (
            "member is-hannes", "member-photo", 'alt="Hannes, Auszubildender zum Fahrzeuglackierer"'
        )) and "hannes-azubi-fahrzeuglackierer-v2.webp" not in team_html,
        "Teamseite nutzt Anfrageformular": "Dellenreparatur anfragen" in team_html and "/anfrage?anliegen=dellenreparatur" in team_html,
        "Leistungsseite nutzt Anfrageformular": "Fahrzeugcheck anfragen" in leistungen_html and "/anfrage?anliegen=leasingrueckgabe" in leistungen_html,
        "Leasingrueckgabe auf Leistungsseite": all(label in leistungen_html for label in (
            "Leasingrückgabe-Check", "sichtbare Schäden und Verschleiß", "wirtschaftlich sinnvoll"
        )),
    }
    print_checks(category_checks)

    original_public_only = portal.PUBLIC_SITE_ONLY
    original_indexable = portal.PUBLIC_SITE_INDEXABLE
    original_portal_base = portal.PORTAL_BASE_URL
    original_public_base = portal.PUBLIC_BASE_URL
    try:
        portal.PUBLIC_SITE_ONLY = True
        portal.PUBLIC_SITE_INDEXABLE = False
        portal.PORTAL_BASE_URL = "https://portal.example.test"
        portal.PUBLIC_BASE_URL = "https://www.example.test"
        public_client = portal.app.test_client()
        public_home = public_client.get("/")
        public_html = public_home.get_data(as_text=True)
        public_checks = {
            "Public-only Startseite": public_home.status_code == 200,
            "Public-only sperrt Admin": public_client.get("/admin").status_code == 404,
            "Public-only sperrt Formularspeicherung": public_client.get("/anfrage").status_code == 404,
            "Homepage verlinkt persistentes Portal": "https://portal.example.test/anfrage?anliegen=" in public_html,
            "Vorschau blockiert Suchmaschinen": "Disallow: /" in public_client.get("/robots.txt").get_data(as_text=True),
        }
        portal.PUBLIC_SITE_INDEXABLE = True
        public_checks["Live-Sitemap vorbereitet"] = (
            public_client.get("/sitemap.xml").status_code == 200
            and "https://www.example.test/team" in public_client.get("/sitemap.xml").get_data(as_text=True)
        )
    finally:
        portal.PUBLIC_SITE_ONLY = original_public_only
        portal.PUBLIC_SITE_INDEXABLE = original_indexable
        portal.PORTAL_BASE_URL = original_portal_base
        portal.PUBLIC_BASE_URL = original_public_base
    print_checks(public_checks)

    all_checks = (checks, form_checks, category_checks, public_checks)
    return 0 if all(all(group.values()) for group in all_checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
