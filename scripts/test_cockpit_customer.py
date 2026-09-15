"""Customer/partner UI regression with synthetic template data and intercepted HTTP.

Run: python scripts/test_cockpit_customer.py (Playwright + Edge required).
Does not import app.py, open a database, send messages, or contact live services.
Backend authorization and phase filtering are covered separately by integration tests.
"""
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode, urlparse

from jinja2 import ChainableUndefined, ChoiceLoader, DictLoader, Environment, FileSystemLoader
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://customer-ui.invalid"
FULL_SCOPE = "Tür und Seitenteil instand setzen. " * 14 + "Vollständiger Abschluss des Leistungsumfangs."


def url_for(endpoint, **values):
    if endpoint == "static":
        return "/static/" + values["filename"]
    return "/" + endpoint + ("?" + urlencode(values) if values else "")


def template_environment():
    # Unrelated widgets are excluded to keep this UI test independent of APIs.
    ignored = {
        name: "" for name in (
            "_live_clock_widget.html", "_mini_month_calendar.html", "_session_keepalive.html",
            "_klick_tracking.html", "_fin_analyse_script.html", "_partner_bonusmodell_panel.html",
            "ki_assistent_widget.html",
        )
    }
    env = Environment(
        loader=ChoiceLoader([DictLoader(ignored), FileSystemLoader(ROOT / "templates")]),
        autoescape=True, undefined=ChainableUndefined,
    )
    env.filters["iso_date"] = lambda value: value or ""
    env.globals.update(
        url_for=url_for, csrf_field=lambda: '<input name="csrf_token" value="synthetic-csrf" type="hidden">',
        get_flashed_messages=lambda **_: [], analysis_loading_news=lambda: [],
    )
    return env


def fixture(status=3):
    transport = {"standard": {"label": "Kunde bringt und holt", "angebot_annahme_label": "Bringtermin", "angebot_abholung_label": "Abholtermin"}}
    intake = {"kunden_wunsch_bestaetigt_am": "01.09.2026", "kunden_wunsch_annahme_datum": "02.09.2026", "kunden_angebot_angenommen_am": "01.09.2026"}
    order = dict(
        id=12, fahrzeug="Synthetisches Testfahrzeug", kunde_name="Testperson", kennzeichen="TEST-12",
        quelle="lead", status=status, angebot_status="angenommen", schaden_aufnahme=intake,
        kunden_status_token="synthetic-token", status_meta={"label": "In Arbeit", "icon": "", "farbe": "primary"},
        annahme_datum="02.09.2026", annahme_uhrzeit="08:00", abholtermin="20.09.2026", abhol_uhrzeit="15:30",
        transport_art="standard", transport_meta=transport["standard"], schaden_mietwagen_label="Nein",
        partner_annahme_label="Bringtermin", partner_abholung_label="Abholtermin",
        angebot_annahme_label="Bringtermin", angebot_abholung_label="Abholtermin",
        werkstatt_angebot_preis="1250", werkstatt_angebot_preis_label="1.250,00 € brutto",
        werkstatt_angebot_text_display=FULL_SCOPE, werkstatt_angebot_am="01.09.2026",
        analyse_confidence=0, geaendert_am="15.09.2026 10:15", versicherung_id=None,
    )
    pickup = dict(key="abholung", label="Abholung", datum_zeit_text="20.09.2026 · 15:30 Uhr", detail="Bestätigt", is_open=False, can_change=True)
    history = [
        dict(titel="Antwort der Werkstatt", nachricht="Ihre Unterlagen sind vollständig.", erstellt_am="15.09.2026 10:15", quelle="werkstatt"),
        dict(titel="Ihre Nachricht", nachricht='Bitte <script>window.injected=true</script> prüfen.', erstellt_am="14.09.2026 10:00", quelle="kunde"),
    ]
    documents = [dict(original_name=f"Unterlage-{i:02}.pdf", customer_url=f"/files/{i}", hochgeladen_am="14.09.2026", kategorie_label="Unterlage", pruefstatus_label="Gespeichert") for i in range(12)]
    documents[0]["original_name"] = '<img src=x onerror="window.injected=true">.pdf'
    lead = dict(order, status="unterlagen_fehlen", angebot_status="angebot_abgegeben", portal_data={}, angebot_preis_label="1.250,00 € brutto", angebot_text_display=FULL_SCOPE)
    offer = dict(order, angebot_status="angebot_abgegeben")
    return dict(
        auftrag=order, lead=lead, angebot=offer,
        autohaus=dict(id=1, slug="test", name="Testpartner", portal_label="Partnerportal", portal_welcome="Willkommen"),
        transport_arten=transport, dateien=[], fertigbilder=[], chat_nachrichten=[], reklamationen=[],
        werkstattangebote=[offer], auftraege=[order], archivierte_auftraege=[], versicherungen=[],
        kunden_termine={"next": pickup if status < 5 else None, "items": [pickup] if status < 5 else []},
        terminfreigabe={"can_request": True, "termin_gesetzt": False, "label": "Besichtigung offen", "detail": "Besichtigung anfragen", "termin_art": "besichtigung"},
        kunden_verlauf=history, kunden_dokumente=documents, kunden_aktualisiert_am="15.09.2026 10:15",
        kunden_nachrichten=[{"titel": "INTERNAL_RAW_NOTE", "nachricht": "INTERNAL_RAW_BODY"}],
        portal_events=[{"titel": "INTERNAL_LEGACY_EVENT"}],
        kunden_unterlagen=[{"original_name": "INTERNAL_LEGACY_FILE"}],
        kunden_auftragsdokumente=[{"original_name": "INTERNAL_OLD_DOCUMENT"}],
        lead_dateien=[{"original_name": "INTERNAL_LEGACY_LEAD_FILE"}],
        kunden_bilder=[], kunden_timeline=[],
        kunden_heute="2026-09-15", kunden_wunsch_annahme_iso="2026-09-20", kunden_wunsch_abholung_iso="2026-09-22",
        schadenaufnahme_max_dateien=20, schadenaufnahme_max_upload_mb=25,
        werkstatt_kontakt=dict(name="Gärtner Test", adresse="Teststraße 1", telefon="000", telefon_url="tel:000", whatsapp_url="#", oeffnungszeiten="Mo–Fr 08–17 Uhr"),
        kunden_status_qr_url="/qr", kunden_status_link=BASE + "/test",
        request=SimpleNamespace(path="/test", args={}),
    )


def render(name, **changes):
    data = fixture()
    data.update(changes)
    return template_environment().get_template(name).render(**data)


class Scenario:
    def __init__(self, page, html):
        self.page, self.html, self.loads, self.posts = page, html, 0, []
        page.route("**/*", self.route)

    def route(self, route):
        path = urlparse(route.request.url).path
        if route.request.method == "POST":
            self.posts.append(route.request.post_data or "")
            route.fulfill(content_type="text/html", body="<p>Nur synthetische Testannahme</p>")
        elif path == "/test":
            self.loads += 1
            route.fulfill(content_type="text/html", body=self.html)
        elif path == "/static/kundenportal.css":
            route.fulfill(content_type="text/css", body=(ROOT / "static/kundenportal.css").read_text(encoding="utf-8"))
        else:
            # External CDN/assets are never requested over the network.
            route.fulfill(status=204)


def run():
    # Rendering every changed full page catches Jinja branches before browser checks.
    pages = {name: render(name) for name in (
        "kunden_status.html", "lead_kundenportal.html", "partner_dashboard.html",
        "partner_auftrag.html", "partner_angebot.html",
    )}
    print("PASS all customer/partner templates render with synthetic contracts")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)

        def fresh(html, width=1280):
            context = browser.new_context(viewport={"width": width, "height": 900}, reduced_motion="reduce")
            page = context.new_page()
            page.set_default_timeout(5000)
            scenario = Scenario(page, html)
            page.goto(BASE + "/test")
            return context, page, scenario

        for name in ("kunden_status.html", "lead_kundenportal.html"):
            context, page, scenario = fresh(pages[name])
            if name == "kunden_status.html":
                page.get_by_role('tab', name='Nachrichten').click()
            expect(page.get_by_label("Ihr Nachrichtenverlauf")).to_contain_text("Ihre Unterlagen sind vollständig.")
            expect(page.get_by_label("Ihr Nachrichtenverlauf")).to_contain_text("<script>window.injected=true</script>")
            assert page.locator('#unterlagen a[href^="/files/"]').count() == 12
            assert "INTERNAL_" not in page.content()
            assert page.evaluate("window.injected === undefined")
            print(f"PASS {name}: full authorized history/docs, escaped contents, no raw-data fallback")
            page.locator('#nachrichten textarea[name="nachricht"]').fill("Entwurf bleibt erhalten")
            page.locator('[data-portal-refresh]').click()
            expect(page.locator('[data-refresh-warning]')).to_be_visible()
            assert scenario.loads == 1
            expect(page.locator('#nachrichten textarea[name="nachricht"]')).to_have_value("Entwurf bleibt erhalten")
            page.locator('[data-refresh-discard]').click()
            expect(page.locator('#nachrichten textarea[name="nachricht"]')).to_have_value("")
            assert scenario.loads == 2 and not scenario.posts
            print(f"PASS {name}: refresh preserves draft until explicit discard")
            context.close()

        context, page, scenario = fresh(pages["kunden_status.html"])
        expect(page.locator('.customer-date')).to_contain_text("20.09.2026 · 15:30 Uhr")
        expect(page.locator('.customer-date')).not_to_contain_text("02.09.2026")
        expect(page.locator('#termine')).not_to_contain_text("Besichtigung")
        expect(page.locator('#termine')).not_to_contain_text("Wunschtermin senden")
        page.locator('[data-portal-refresh]').click()
        assert scenario.loads == 2
        print("PASS in-work view prioritizes pickup, omits obsolete inspection and supports clean refresh")
        context.close()

        done = fixture(status=5)
        context, page, _ = fresh(render("kunden_status.html", **done))
        expect(page.locator('.customer-date')).to_contain_text("Fahrzeug zurückgegeben")
        expect(page.locator('.customer-next')).to_contain_text("abgeschlossen")
        expect(page.locator('#termine')).not_to_contain_text("offen")
        print("PASS completed order has no misleading pending appointment")
        context.close()

        pending = fixture(status=1)
        pending["auftrag"]["schaden_aufnahme"]["kunden_wunsch_bestaetigt_am"] = ""
        context, page, _ = fresh(render("kunden_status.html", **pending))
        expect(page.locator('.customer-date')).to_contain_text("Terminwunsch – Bestätigung folgt")
        expect(page.locator('.customer-next')).to_contain_text("prüft Ihren Terminwunsch")
        print("PASS unconfirmed appointment remains a wish before scheduling")
        context.close()

        ready = fixture(status=4)
        context, page, _ = fresh(render("kunden_status.html", **ready))
        expect(page.locator('.customer-next')).to_contain_text("Rückgabe abstimmen")
        expect(page.locator('.customer-next a')).to_have_attribute("href", "#werkstatt-kontakt")
        print("PASS ready vehicle offers a concrete handover action")
        context.close()

        context, page, scenario = fresh(pages["kunden_status.html"])
        page.get_by_role('tab', name='Unterlagen').click()
        page.locator('.portal-upload summary').click()
        page.locator('#kundenstatus-dateien').set_input_files(dict(name="test.pdf", mimeType="application/pdf", buffer=b"%PDF synthetic"))
        page.locator('[data-portal-refresh]').click()
        expect(page.locator('[data-refresh-warning]')).to_be_visible()
        assert page.locator('#kundenstatus-dateien').evaluate("input => input.files.length") == 1
        assert scenario.loads == 1 and not scenario.posts
        print("PASS selected attachment is preserved by refresh protection")
        context.close()

        context, page, scenario = fresh(pages['kunden_status.html'])
        expect(page.locator('.portal-updates')).to_contain_text('Ihre Unterlagen sind vollständig.')
        expect(page.locator('.portal-updates')).not_to_contain_text('<script>window.injected=true</script>')
        expect(page.locator('#nachrichten')).not_to_be_visible()
        page.get_by_role('tab', name='Nachrichten').click()
        page.locator('#kundenstatus-nachricht').fill('Testentwurf im Nachrichtenbereich')
        page.get_by_role('tab', name='Termine & Auftrag').click()
        expect(page.locator('#termine')).to_be_visible()
        expect(page.locator('#nachrichten')).not_to_be_visible()
        page.get_by_role('tab', name='Termine & Auftrag').press('End')
        expect(page.get_by_role('tab', name='Nachrichten')).to_be_focused()
        expect(page.locator('#kundenstatus-nachricht')).to_have_value('Testentwurf im Nachrichtenbereich')
        assert scenario.posts == []
        page.goto(BASE + '/test#auftragsbestaetigung')
        expect(page.locator('#auftragsbestaetigung')).to_have_attribute('open', '')
        expect(page.locator('#auftragsbestaetigung')).to_contain_text(FULL_SCOPE)
        page.goto(BASE + '/test#nachrichten')
        expect(page.locator('#nachrichten')).to_be_visible()
        page.goto(BASE + '/test#werkstatt-kontakt')
        expect(page.get_by_role('tab', name='Übersicht')).to_have_attribute('aria-selected','true')
        expect(page.locator('#nachrichten')).not_to_be_visible()
        expect(page.locator('#werkstatt-kontakt')).to_be_visible()
        print('PASS customer tabs preserve drafts, support keyboard and restore direct section links')
        context.close()

        open_offer = fixture(status=1)
        open_offer['auftrag']['angebot_status'] = 'angebot_abgegeben'
        context, page, scenario = fresh(render('kunden_status.html', **open_offer))
        expect(page.locator('#angebot')).to_be_visible()
        expect(page.locator('#angebot')).to_contain_text(FULL_SCOPE)
        expect(page.locator('#angebot input[name="angebot_annehmen_bestaetigt"]')).not_to_be_checked()
        assert scenario.posts == []
        print('PASS open customer offer stays fully visible and still requires conscious acceptance')
        context.close()

        context = browser.new_context(java_script_enabled=False, viewport={'width':390,'height':844})
        page = context.new_page()
        scenario = Scenario(page, pages['kunden_status.html'])
        page.goto(BASE + '/test')
        expect(page.locator('#termine')).to_be_visible()
        expect(page.locator('#nachrichten')).to_be_visible()
        expect(page.locator('#unterlagen')).to_be_visible()
        print('PASS customer sections remain usable without JavaScript')
        context.close()

        context, page, _ = fresh(pages["partner_dashboard.html"])
        assert page.locator('form[action^="/partner_angebot_annehmen"]').count() == 0
        expect(page.get_by_role("link", name="Vollständiges Angebot prüfen")).to_have_attribute("href", "/partner_angebot_detail?slug=test&auftrag_id=12#werkstatt-angebot")
        print("PASS dashboard has review navigation instead of direct acceptance")
        context.close()

        context, page, scenario = fresh(pages["partner_angebot.html"])
        offer = page.locator('#werkstatt-angebot')
        expect(offer).to_contain_text(FULL_SCOPE)
        checkbox = offer.locator('input[name="angebot_annehmen_bestaetigt"]')
        expect(checkbox).not_to_be_checked()
        offer.get_by_role("button", name="Angebot verbindlich annehmen").click()
        assert not scenario.posts
        checkbox.check()
        offer.get_by_role("button", name="Angebot verbindlich annehmen").click()
        assert len(scenario.posts) == 1 and "angebot_annehmen_bestaetigt=1" in scenario.posts[0]
        print("PASS full offer requires explicit confirmation before submission")
        context.close()

        context, page, scenario = fresh(pages["partner_auftrag.html"])
        expect(page.locator('[data-portal-freshness]')).to_contain_text("15.09.2026 10:15")
        assert "Änderungen sehen Sie automatisch hier" not in page.content()
        page.get_by_role("button", name="Daten öffnen", exact=True).click()
        page.locator('input[name="kennzeichen"]').fill("UNSAVED")
        page.locator('[data-portal-refresh]').click()
        expect(page.locator('[data-refresh-warning]')).to_be_visible()
        assert scenario.loads == 1
        print("PASS partner freshness is honest and draft-safe")
        context.close()

        # Without a CDN the page must still be usable and must not overflow at mobile width.
        for name in ("kunden_status.html", "lead_kundenportal.html", "partner_angebot.html"):
            context, page, _ = fresh(pages[name], width=390)
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), name
            print(f"PASS {name}: mobile 390px no horizontal overflow")
            context.close()
        browser.close()


if __name__ == "__main__":
    run()
