from pathlib import Path
from datetime import date, datetime
from html.parser import HTMLParser
import sys
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import smoke_test as isolated  # noqa: E402

portal = isolated.portal


def print_checks(checks):
    for label, passed in checks.items():
        print(f"[{'OK' if passed else 'FEHLER'}] {label}")


def main():
    portal.app.config["TESTING"] = True
    client = portal.app.test_client()
    response = client.get("/homepage")
    html = response.get_data(as_text=True)
    team_html = client.get("/team").get_data(as_text=True)
    leistungen_html = client.get("/leistungen").get_data(as_text=True)
    partner_html = client.get("/geschaeftspartner").get_data(as_text=True)
    css = client.get("/static/homepage/homepage-kompakt.css")
    hero_html = html.split('<section class="audiences',1)[0]
    services_html = html.split('id="leistungen"',1)[1].split('</section>',1)[0]
    oil_form = client.get('/anfrage?anliegen=mechanikanfrage&mechanik_kategorie=oelwechsel').get_data(as_text=True)
    care_form = client.get('/anfrage?anliegen=fahrzeugpflege').get_data(as_text=True)
    saison = portal.homepage_jahreszeit()
    class DeckMarkup(HTMLParser):
        def __init__(self):
            super().__init__()
            self.cards, self.controls, self.choices = [], [], []
        def handle_starttag(self, tag, attrs):
            data = dict(attrs)
            if "data-current-card" in data:
                self.cards.append(data)
            if "data-deck-prev" in data or "data-deck-next" in data:
                self.controls.append((tag, data))
            if "data-deck-index" in data:
                self.choices.append((tag, data))
    deck = DeckMarkup()
    deck.feed(hero_html)
    deck_script = client.get("/static/homepage/aktuelles-karten.js")
    checks = {
        "Status 200": response.status_code == 200,
        "Homepage nicht veraltet cachen": "no-store" in response.headers.get("Cache-Control", ""),
        "Eine Hauptueberschrift mit Fahrzeug und Fuhrpark": html.count("<h1") == 1 and all(x in html for x in ("Ihr Fahrzeug.", "Ihr Fuhrpark.", "Wir kümmern uns.")),
        "Versicherungsabwicklung im Titel und Einstieg": "Auch im Versicherungsfall." in html and "Abstimmung mit Ihrer Versicherung" in html,
        "Fotoanfrage und Anruf direkt erreichbar": "Anfrage mit Foto" in html and 'href="tel:+4915227706694"' in html,
        "Alle drei Kundengruppen erreichbar": all(x in html for x in ("Privatkunden", "Autohäuser &amp;", "Werkstätten", "Firmenflotten", "/geschaeftspartner")),
        "Beide Partner-Kacheln starten am Seitenanfang": html.count('class="audience" href="/geschaeftspartner"') == 2,
        "Interne Partner-Sprunglinks bleiben erhalten": all(x in partner_html for x in ('href="#autohaeuser"', 'href="#flotten"', 'id="autohaeuser"', 'id="flotten"')),
        "Portalzugang getrennt von Fotoanfrage": 'href="/portale"' in html and "Portale &amp; Zugänge" in html and "/anfrage?anliegen=auswahl" in html,
        "Ausfuehrliche Leistungen ueber Unterseite erreichbar": 'href="/leistungen"' in html and 'id="unsere-arbeiten"' not in html and all(f'id="arbeit-{slug}"' in leistungen_html for slug in ("reifenmontage", "bremsenservice", "oelservice", "inspektion", "lackierung", "karosserie", "smart-repair", "dellen-hagel", "autoglas", "reifenlager", "schadenabwicklung", "mobilitaet")),
        "Werkstatt und Geschichte auf Ueber-uns-Seite": '/team#neue-werkstatt' in html and '/team#unsere-geschichte' in html and all(f'id="{anchor}"' in team_html for anchor in ("neue-werkstatt", "unsere-geschichte")),
        "Zentrales Anfrageformular verlinkt": '/anfrage?anliegen=' in html and "mailto:info@auto-lackierzentrum.de?subject=" not in html,
        "Mobile Navigation und Portale vorhanden": all(x in html for x in ('class="menu-toggle"', 'aria-controls="hauptmenue"', 'class="mobile-portal-link"', "aria-expanded")),
        "Vertrauen und persoenlicher Kontakt gebuendelt": all(x in html for x in ('id="vertrauen"', "Meisterbetrieb für Karosserie", "Margita Höfer", "Google-Bewertungen ansehen")),
        "Unbestaetigte Oeffnungszeiten entfernt": "Mo–Sa:" not in html and '"openingHoursSpecification"' not in html,
        "HU-AU korrekt ueber externen Pruefpartner": "externen Prüfpartner" in html and "externen Prüfpartner" in leistungen_html,
        "Strukturierte Unternehmensdaten": '"@type":"AutoBodyShop"' in html,
        "Sprunglink vorhanden": 'class="skip-link" href="#inhalt"' in html,
        "Hero-Bild vorgeladen": 'rel="preload" as="image"' in html,
        "Passendes saisonales Titelbild vorhanden": f'/static/{saison["hero_bild"]}' in html and (ROOT / "static" / saison["hero_bild"]).is_file() and 'werkstatt-hero-v3.webp' not in html,
        "Aktuelle Reifenbuchung im ersten Homepagebereich": all(x in hero_html for x in ('Aktuell bei uns',saison['reifen_titel'],'Reifentermin online buchen','/anfrage?anliegen=reifenservice#kurzanfrage','Ölcheck','Aufbereitung')),
        "Vier Themen im Kartenstapel, Reifen und Bremsen zuerst": [card.get("data-card-title") for card in deck.cards] == ["Reifen", "Bremsen", "Ölcheck", "Aufbereitung"],
        "Alle Karten ohne JavaScript lesbar und verlinkt": all("hidden" not in card and "inert" not in card for card in deck.cards) and hero_html.count('class="button current-booking"') == 4,
        "Kartenpfeile und Themen sind beschriftete native Buttons": len(deck.controls) == 2 and all(tag == "button" and data.get("type") == "button" and data.get("aria-label") and data.get("aria-controls") == "aktuelle-karten" for tag, data in deck.controls) and len(deck.choices) == 4 and all(tag == "button" and data.get("aria-pressed") in ("true", "false") for tag, data in deck.choices),
        "Kartenwechsel erreichbar und Status barrierearm": deck_script.status_code == 200 and '/static/homepage/aktuelles-karten.js' in html and 'aria-live="polite" aria-atomic="true" data-deck-status' in hero_html,
        "Bremsenkarte mit Terminlink und vorhandenem Werkstattfoto": '/anfrage?anliegen=bremsenservice#kurzanfrage' in hero_html and 'Bremsentermin online buchen' in hero_html and '/static/homepage/leistungsfotos-v2/bremsenservice.jpg' in hero_html and client.get('/static/homepage/leistungsfotos-v2/bremsenservice.jpg').status_code == 200,
        "Newslinks starten mit passendem Anliegen": 'value="oelwechsel" checked' in oil_form and 'name="pflege_kategorie"' in care_form,
        "Leistungsuebersicht bleibt vollstaendig ohne zweiten Buchungsbutton": services_html.count('class="service-item"') == 9 and 'href="/leistungen/reifenservice"' in services_html and 'data-reifen-buchung' not in services_html,
        "Umzugshinweis und Stylesheet verfuegbar": "1. November 2026" in html and "Alte Neckarelzer Straße 18" in html and css.status_code == 200,
        "Lazy Loading ausserhalb des Titelbilds": html.count('loading="lazy"') >= 4,
        "Cookielose Besucherstatistik eingebunden": "/api/besucher" in html and "localStorage" not in html and "document.cookie" not in html,
        "Vorschau nicht indexierbar": 'name="robots" content="noindex, nofollow"' in html,
    }
    print_checks(checks)

    season_checks = {}
    for month, key in enumerate(("winter", "winter", "fruehling", "fruehling", "fruehling", "sommer", "sommer", "sommer", "herbst", "herbst", "herbst", "winter"), 1):
        season_checks[f"Jahreszeit im Monat {month:02d}"] = portal.homepage_jahreszeit(date(2028, month, 1))["key"] == key
    for tag, key in ((date(2028, 2, 29), "winter"), (date(2026, 5, 31), "fruehling"), (date(2026, 8, 31), "sommer"), (date(2026, 11, 30), "herbst")):
        season_checks[f"Saisonende am {tag.isoformat()}"] = portal.homepage_jahreszeit(tag)["key"] == key
    class BerlinBoundary(datetime):
        @classmethod
        def now(cls, tz=None):
            # UTC is still February; the homepage must already show March in Berlin.
            return datetime(2026, 2, 28, 23, 30, tzinfo=portal.timezone.utc).astimezone(tz)
    with patch.object(portal, "datetime", BerlinBoundary):
        season_checks["Saisonwechsel nach Berliner Ortszeit"] = portal.homepage_jahreszeit()["key"] == "fruehling"
    for month, key in ((1, "winter"), (4, "fruehling"), (7, "sommer"), (10, "herbst")):
        expected = portal.homepage_jahreszeit(date(2026, month, 15))
        with patch.object(portal, "homepage_jahreszeit", return_value=expected):
            seasonal_response = client.get("/homepage")
        seasonal_html = seasonal_response.get_data(as_text=True)
        hero_path = '/static/' + expected['hero_bild']
        season_checks[f"Homepage mit vollstaendigen {key}-Motiven"] = (
            seasonal_response.status_code == 200
            and f'data-jahreszeit="{key}"' in seasonal_html
            and f'rel="preload" as="image" href="{hero_path}"' in seasonal_html
            and f'class="intro-photo"><img src="{hero_path}"' in seasonal_html
            and expected['reifen_titel'] in seasonal_html
            and '/static/' + expected['reifen_bild'] in seasonal_html
            and client.get(hero_path).status_code == 200
            and client.get('/static/' + expected['reifen_bild']).status_code == 200
        )
    season_checks["Oel- und Pflegekarten mit erreichbaren Bildern"] = all(
        '/static/homepage/' + name in hero_html and client.get('/static/homepage/' + name).status_code == 200
        for name in ('karte-oelservice-20261021.webp', 'fahrzeugpflege.webp')
    )
    checks.update(season_checks)
    print_checks(season_checks)

    captured = {}
    original_create_lead = portal.create_lead
    original_get_lead = portal.get_lead
    original_backup = portal.schedule_change_backup
    original_lead_mail = portal.sende_website_lead_benachrichtigung
    original_lead_notice = portal.lead_notice
    original_notify_lead_workshop = portal.notify_lead_workshop
    def fake_create_lead(payload):
        captured["payload"] = payload
        return 991

    def fake_lead_mail(lead_id, payload, anliegen_label=""):
        captured["mail"] = {
            "lead_id": lead_id,
            "payload": payload,
            "anliegen_label": anliegen_label,
        }

    portal.create_lead = fake_create_lead
    portal.get_lead = lambda lead_id: {
        "id": lead_id,
        "kunden_status_url": "https://portal.example.test/lead/test-token",
    }
    portal.schedule_change_backup = lambda _reason: None
    portal.sende_website_lead_benachrichtigung = fake_lead_mail
    portal.lead_notice = lambda *_args, **_kwargs: None
    portal.notify_lead_workshop = lambda *_args, **_kwargs: None
    try:
        form_get = client.get("/anfrage?anliegen=leasingrueckgabe")
        form_html = form_get.get_data(as_text=True)
        farbton_html = client.get("/anfrage?anliegen=farbton").get_data(as_text=True)
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
        leasing_payload = dict(captured.get("payload", {}))
        farbton_post = client.post(
            "/anfrage",
            data={
                portal.CSRF_FIELD_NAME: csrf_token,
                "anliegen": "farbton",
                "besichtigungsart": "vor_ort",
                "name": "Test Kunde",
                "telefon": "0171 1234567",
                "email": "kunde@example.test",
                "fahrzeug": "VW Golf 8",
                "wunschdatum": "",
                "fahrzeug_link": "https://example.test/darf-nicht-gespeichert-werden",
                "nachricht": "Beratung zum passenden Farbton.",
                "website": "",
            },
            follow_redirects=False,
        )
        farbton_beschreibung = captured.get("payload", {}).get("beschreibung", "")
        success = client.get(form_post.headers.get("Location", "")) if form_post.status_code == 302 else None
        form_checks = {
            "Anfrageformular Status 200": form_get.status_code == 200,
            "Leasing vorausgewaehlt": 'value="leasingrueckgabe" selected' in form_html,
            "CSRF und Honeypot": 'name="csrf_token"' in form_html and 'name="website"' in form_html,
            "Mehrfachbild-Upload im Standardformular": all(marker in form_html for marker in (
                'enctype="multipart/form-data"', 'name="bilder"', 'multiple', "Bis zu 5 Bilder",
            )),
            "Prueffelder nur fuer Fahrzeugchecks": 'data-anliegen-visible="kaufberatung leasingrueckgabe"' in form_html
            and 'data-anliegen-visible="kaufberatung"' in form_html
            and 'data-anliegen-visible="kaufberatung" hidden' in farbton_html
            and 'data-anliegen-visible="kaufberatung leasingrueckgabe" hidden' in farbton_html,
            "Anfrage PRG": form_post.status_code == 302 and "gesendet=1" in form_post.headers.get("Location", ""),
            "Website-Lead gespeichert": leasing_payload.get("quelle") == "website",
            "Werkstatt per E-Mail benachrichtigt": captured.get("mail", {}).get("lead_id") == 991
            and captured.get("mail", {}).get("payload", {}).get("kunde_email") == "kunde@example.test",
            "Vor-Ort-Aufpreis dokumentiert": "gegen Aufpreis" in leasing_payload.get("beschreibung", ""),
            "Unpassende Pruefwerte verworfen": farbton_post.status_code == 302
            and "Prüfort:" not in farbton_beschreibung
            and "Fahrzeuganzeige:" not in farbton_beschreibung,
            "Keine WhatsApp-Einwilligung behauptet": "keine WhatsApp-Einwilligung" in leasing_payload.get("notiz", ""),
            "Bestaetigung ohne Lead-ID": success is not None and success.status_code == 200 and "Ihre Anfrage ist angekommen" in success.get_data(as_text=True) and "991" not in success.get_data(as_text=True),
        }
    finally:
        portal.create_lead = original_create_lead
        portal.get_lead = original_get_lead
        portal.schedule_change_backup = original_backup
        portal.sende_website_lead_benachrichtigung = original_lead_mail
        portal.lead_notice = original_lead_notice
        portal.notify_lead_workshop = original_notify_lead_workshop
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
        "Leistungsseite nutzt Anfrageformular": "Anfrage mit Foto" in leistungen_html
        and "/anfrage?anliegen=leasingrueckgabe" in leistungen_html,
        "Autoglas und Schadenabwicklung auf Leistungsseite": all(marker in leistungen_html for marker in (
            "Autoglas", "Glasschäden", 'id="arbeit-autoglas"', 'id="arbeit-schadenabwicklung"',
        )),
        "Leasingrueckgabe und Kaufcheck erreichbar": all(label in leistungen_html for label in (
            "Leasingrückgabe", "Kaufcheck", "/anfrage?anliegen=leasingrueckgabe", "/anfrage?anliegen=kaufberatung"
        )),

    }
    print_checks(category_checks)

    original_public_only = portal.PUBLIC_SITE_ONLY
    original_indexable = portal.PUBLIC_SITE_INDEXABLE
    original_portal_base = portal.PORTAL_BASE_URL
    original_public_base = portal.PUBLIC_BASE_URL
    original_public_hosts = portal.PUBLIC_HOSTS.copy()
    original_datenschutz_freigabe = portal.DATENSCHUTZ_RECHTLICH_FREIGEGEBEN
    try:
        portal.PUBLIC_SITE_ONLY = True
        portal.PUBLIC_SITE_INDEXABLE = False
        portal.PORTAL_BASE_URL = "https://portal.example.test"
        portal.PUBLIC_BASE_URL = "https://www.example.test"
        portal.PUBLIC_HOSTS = {"example.test", "www.example.test"}
        public_client = portal.app.test_client()
        public_home = public_client.get("/")
        public_html = public_home.get_data(as_text=True)
        public_checks = {
            "Public-only Startseite": public_home.status_code == 200,
            "Public-only sperrt Admin": public_client.get("/admin").status_code == 404,
            "Public-only sperrt Formularspeicherung": public_client.get("/anfrage").status_code == 404,
            "Homepage verlinkt persistentes Portal": "https://portal.example.test/anfrage?anliegen=" in public_html,
            "Bremsenbuchung auf oeffentlicher Seite verlinkt Portalhost": 'https://portal.example.test/anfrage?anliegen=bremsenservice#kurzanfrage' in public_html,
            "Vorschau blockiert Suchmaschinen": "Disallow: /" in public_client.get("/robots.txt").get_data(as_text=True),
        }
        portal.PUBLIC_SITE_INDEXABLE = True
        live_home = public_client.get("/", base_url="https://www.example.test")
        live_html = live_home.get_data(as_text=True)
        public_checks["Live-Sitemap vorbereitet"] = (
            public_client.get("/sitemap.xml", base_url="https://www.example.test").status_code == 200
            and "https://www.example.test/team" in public_client.get(
                "/sitemap.xml", base_url="https://www.example.test"
            ).get_data(as_text=True)
        )
        public_checks["Oeffentliche Domain indexierbar"] = (
            live_home.status_code == 200
            and 'name="robots" content="index, follow"' in live_html
            and "Disallow: /partner" in public_client.get(
                "/robots.txt", base_url="https://www.example.test"
            ).get_data(as_text=True)
        )
        portal.PUBLIC_SITE_ONLY = False
        shared_client = portal.app.test_client()
        public_host_root = shared_client.get("/", base_url="https://www.example.test")
        portal_host_root = shared_client.get("/", base_url="https://portal.example.test")
        public_host_admin = shared_client.get("/admin", base_url="https://www.example.test")
        portal_host_homepage = shared_client.get("/homepage", base_url="https://portal.example.test")
        portal_host_robots = shared_client.get("/robots.txt", base_url="https://portal.example.test")
        portal_host_sitemap = shared_client.get("/sitemap.xml", base_url="https://portal.example.test")
        portal.DATENSCHUTZ_RECHTLICH_FREIGEGEBEN = True
        datenschutz = shared_client.get("/datenschutz", base_url="https://www.example.test")
        datenschutz_html = datenschutz.get_data(as_text=True)
        public_checks["Hauptdomain zeigt Homepage"] = public_host_root.status_code == 200 and "Karosserie, Lack" in public_host_root.get_data(as_text=True)
        public_checks["Portalhost behaelt Portal-Start"] = portal_host_root.status_code == 302 and "/admin" in portal_host_root.headers.get("Location", "")
        public_checks["Hauptdomain sperrt interne Routen"] = public_host_admin.status_code == 404
        public_checks["Portalhost bleibt von Suchmaschinen gesperrt"] = (
            'name="robots" content="noindex, nofollow"' in portal_host_homepage.get_data(as_text=True)
            and "Disallow: /" in portal_host_robots.get_data(as_text=True)
            and portal_host_sitemap.status_code == 404
        )
        public_checks["Datenschutz ohne Entwurfsplatzhalter"] = (
            datenschutz.status_code == 200
            and 'name="robots" content="index, follow"' in datenschutz_html
            and 'rel="canonical" href="https://www.example.test/datenschutz"' in datenschutz_html
            and "Prüffassung" not in datenschutz_html
            and "muss vor der rechtlichen Freigabe" not in datenschutz_html
        )
    finally:
        portal.PUBLIC_SITE_ONLY = original_public_only
        portal.PUBLIC_SITE_INDEXABLE = original_indexable
        portal.PORTAL_BASE_URL = original_portal_base
        portal.PUBLIC_BASE_URL = original_public_base
        portal.PUBLIC_HOSTS = original_public_hosts
        portal.DATENSCHUTZ_RECHTLICH_FREIGEGEBEN = original_datenschutz_freigabe
    print_checks(public_checks)

    all_checks = (checks, form_checks, category_checks, public_checks)
    return 0 if all(all(group.values()) for group in all_checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
