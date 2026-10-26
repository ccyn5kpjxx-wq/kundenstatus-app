"""Synthetic headless browser checks; no app import, real data, or live requests.

Run with Playwright and an installed Edge browser. Every request is intercepted.
"""
import base64
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode, urlparse
import unittest

from jinja2 import ChainableUndefined, ChoiceLoader, DictLoader, Environment, FileSystemLoader
from PIL import Image, ImageChops
from playwright.sync_api import expect, sync_playwright


ROOT = Path(__file__).resolve().parents[1]
BASE = "http://navigation-ui.invalid"
PIXEL = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j8ZkAAAAASUVORK5CYII=")


def url_for(endpoint, **values):
    if endpoint == "static":
        return "/static/" + values["filename"]
    if endpoint == "admin_datei":
        return "/media/document-" + str(values["datei_id"])
    if endpoint == "kunden_status_qr":
        return "/media/qr"
    if endpoint == "admin_lackier_statistik":
        return "/stats"
    return "/" + endpoint + ("?" + urlencode(values) if values else "")


def static_asset_url(filename):
    if filename not in {"admin-sidebar.css", "admin-sidebar.js", "werkstatt_theme.css",
                        "inspektionscheckliste.css", "logo-nav-192.webp"}:
        raise AssertionError("Only public core assets use this helper")
    return "/static/" + filename + "?v=synthetic-core"


def render(template, *, full_base=False, **values):
    loaders = [] if full_base else [DictLoader({
        "base.html": "<!doctype html><html><head><meta charset='utf-8'>{% block extra_css %}{% endblock %}</head>"
                     "<body>{% block content %}{% endblock %}{% block extra_js %}{% endblock %}</body></html>"})]
    loaders.append(FileSystemLoader(ROOT / "templates"))
    env = Environment(loader=ChoiceLoader(loaders), autoescape=True, undefined=ChainableUndefined)
    env.globals.update(url_for=url_for, csrf_token=lambda: "synthetic-csrf",
                       static_asset_url=static_asset_url,
                       csrf_field=lambda: '<input type="hidden" name="csrf_token" value="synthetic-csrf">',
                       document_visible=lambda *args: True,
                       request=SimpleNamespace(args={}), session={}, auftragsarten={}, config={},
                       get_flashed_messages=lambda **kwargs: [], analysis_loading_news=lambda: [])
    for name in ("mahnungen_faellig_count", "aufgaben_offen_heute_count", "admin_leads_count",
                 "admin_bewerbungen_count", "fahrzeugsuche_auktion_alert_count", "fahrzeugsuche_count",
                 "fahrzeugverkauf_count", "fahrzeugeinkauf_count", "mietwagen_anfragen_count",
                 "mietfahrzeuge_unterwegs_count", "admin_versicherung_count", "admin_postfach_count"):
        env.globals[name] = lambda: 0
    env.filters["iso_date"] = lambda value: value or ""
    return env.get_template(template).render(**values)


def detail_html():
    order = dict(id=71, status=3, quelle="intern", fahrzeug="Synthetic vehicle",
                 auftragsart_label="Lackierauftrag", auftragsart_bg="#eee", auftragsart_farbe="#222",
                 status_meta=dict(label="In Arbeit", farbe="primary", icon=""),
                 kunden_status_url=BASE + "/status/synthetic", kunden_status_local_url=BASE + "/status/synthetic",
                 kunden_status_token="synthetic", kunden_status_aktiv=True,
                 kunden_whatsapp_status={}, schaden_aufnahme={}, planung={}, produktion_schritte=[],
                 transport_art="standard", transport_meta={})
    files = [dict(id=1, original_name="Synthetic document.png", is_browser_image=True,
                  mime_type="image/png", quelle="intern", kategorie="standard"),
             dict(id=2, original_name="Synthetic finished.png", is_browser_image=True,
                  mime_type="image/png", quelle="intern", kategorie="fertigbild")]
    return render("auftrag_detail.html", auftrag=order,
                  statusliste={i: dict(label="Synthetic status", farbe="primary", icon="") for i in range(1, 7)},
                  dateien=files[:1], fertigbilder=files[1:], alle_dateien=files,
                  autohaeuser=[], transport_arten={}, preisstand={}, dokument_pruefung=[],
                  rechnungsdateien=[], reklamationen=[], verzoegerungen=[], benachrichtigungen=[],
                  chat_nachrichten=[], log=[], detail_self_url="/detail", detail_back_url="/dashboard")


def statistics(version=1):
    months = [dict(key=key, label=label, count=version, aktuell=index == 0,
                   items=[dict(id=version, fahrzeug="Synthetic " + label, fertig_datum="01.10.2026")])
              for index, (key, label) in enumerate((("2026-10", "Oktober"), ("2026-09", "September"), ("2026-08", "August")))]
    return dict(monate=months, alle_monate=months, gesamt=3 * version, aktualisierung_sekunden=60)


class NavigationBrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        cls.addClassCleanup(cls.playwright.stop)
        cls.browser = cls.playwright.chromium.launch(channel="msedge", headless=True)
        cls.addClassCleanup(cls.browser.close)
        cls.detail = detail_html()
        cls.dashboard = render("dashboard.html", auftraege=[], archivierte_auftraege=[],
                               angebotsanfragen=[], cockpit={}, lackier_statistik=statistics())

    def open_page(self, html, *, suffix="/detail", javascript=True, clock=False, no_observer=False):
        context = self.browser.new_context(java_script_enabled=javascript, viewport=dict(width=1280, height=900))
        self.addCleanup(context.close)
        page = context.new_page()
        page.set_default_timeout(5000)
        requests, pending_stats = [], []
        page.on("request", lambda request: requests.append(request))

        def route_request(route):
            path = urlparse(route.request.url).path
            if path in {"/detail", "/dashboard"}:
                route.fulfill(body=html, content_type="text/html")
            elif path.startswith("/media/"):
                route.fulfill(body=PIXEL, content_type="image/png")
            elif path == "/stats":
                pending_stats.append(route)
            elif path.startswith("/static/"):
                asset = ROOT / path.lstrip("/")
                content_type = {".css": "text/css", ".js": "application/javascript", ".webp": "image/webp"}.get(asset.suffix)
                if asset.is_file() and content_type:
                    route.fulfill(body=asset.read_bytes(), content_type=content_type)
                else:
                    route.abort()
            else:
                # No accidental provider requests or form submissions leave the test.
                route.abort()

        page.route("**/*", route_request)
        if clock:
            page.clock.install()
            page.add_init_script("window.testHidden=false;Object.defineProperty(document,'hidden',{get:()=>window.testHidden});")
        if no_observer:
            page.add_init_script("delete window.IntersectionObserver;")
        page.goto(BASE + suffix)
        return page, requests, pending_stats

    @staticmethod
    def media_paths(requests):
        return [urlparse(request.url).path for request in requests if "/media/" in request.url]

    def test_hidden_media_wait_for_selected_visible_area_and_keep_drafts(self):
        page, requests, _ = self.open_page(self.detail)
        expect(page.locator("#detail-uebersicht")).to_be_visible()
        self.assertEqual(self.media_paths(requests), [])
        self.assertEqual(page.locator("img[data-preview-src][src]").count(), 0)
        page.get_by_role("tab", name="Arbeiten & Termine", exact=True).click()
        qr = page.locator('img[alt="Kundenstatus QR-Code"]')
        qr.scroll_into_view_if_needed()
        expect(qr).to_have_attribute("src", "/media/qr")
        page.get_by_text("Auftragsart, Kunde und Fahrzeug bearbeiten", exact=True).click()
        page.locator('input[name="fahrzeug"]').fill("Unsaved synthetic change")
        page.locator('input[name="dateien"]').set_input_files(dict(name="draft.png", mimeType="image/png", buffer=PIXEL))
        page.get_by_role("tab", name="Dokumente", exact=True).click()
        image = page.locator('img[alt="Synthetic document.png"]')
        image.scroll_into_view_if_needed()
        expect(image).to_have_attribute("src", "/media/document-1")
        finished = page.locator('img[alt="Synthetic finished.png"]')
        finished.scroll_into_view_if_needed()
        expect(finished).to_have_attribute("src", "/media/document-2")
        page.get_by_role("tab", name="Arbeiten & Termine", exact=True).click()
        expect(page.locator('input[name="fahrzeug"]')).to_have_value("Unsaved synthetic change")
        self.assertEqual(page.locator('input[name="dateien"]').evaluate("input=>input.files[0].name"), "draft.png")
        page.get_by_role("tab", name="Dokumente", exact=True).click()
        self.assertEqual(self.media_paths(requests).count("/media/document-1"), 1)
        self.assertEqual(self.media_paths(requests).count("/media/qr"), 1)
        self.assertFalse(any(request.method != "GET" for request in requests))

    def test_hash_opens_right_area_and_closed_details_wait(self):
        # Use the real tab/preview script in a small disclosure fixture to cover
        # both a direct hash and a QR inside a closed details element.
        script = self.detail.split("<script>\ndocument.addEventListener('DOMContentLoaded'", 1)[1].split("</script>", 1)[0]
        html = """<!doctype html><meta charset="utf-8"><div id="order-sections"></div><div class="auftrag-detail-grid"><div class="col-xl-7"><div class="page-card">
        <div class="process-panel">Overview</div><details id="kundenkommunikation"><summary>QR öffnen</summary>
        <img data-preview-src="/media/qr" loading="lazy" decoding="async" width="120" height="120"></details>
        <div id="dateifreigaben">Documents<div style="height:1400px">Synthetic spacer</div><img data-preview-src="/media/document-1" loading="lazy" decoding="async" width="120" height="120"></div>
        </div></div><div class="col-xl-5"></div></div><script>document.addEventListener('DOMContentLoaded'""" + script + "</script>"
        for no_observer in (False, True):
            with self.subTest(intersection_observer=not no_observer):
                page, requests, _ = self.open_page(html, suffix="/detail#dateifreigaben", no_observer=no_observer)
                expect(page.locator("#detail-dokumente")).to_be_visible()
                if not no_observer:
                    self.assertIsNone(page.locator("#dateifreigaben img").get_attribute("src"), "Offscreen images still wait in an active tab")
                page.locator("#dateifreigaben img").scroll_into_view_if_needed()
                expect(page.locator("#dateifreigaben img")).to_have_attribute("src", "/media/document-1")
                self.assertNotIn("/media/qr", self.media_paths(requests))
                page.get_by_role("tab", name="Arbeiten & Termine", exact=True).click()
                self.assertNotIn("/media/qr", self.media_paths(requests))
                page.get_by_text("QR öffnen", exact=True).click()
                expect(page.locator("#kundenkommunikation img")).to_have_attribute("src", "/media/qr")

    def test_no_javascript_preserves_original_links_and_image_fallbacks(self):
        page, _, _ = self.open_page(self.detail, javascript=False)
        self.assertGreaterEqual(page.locator('a[href="/media/document-1"]').count(), 2)
        image = page.locator('noscript img[alt="Synthetic document.png"]')
        image.scroll_into_view_if_needed()
        expect(image).to_be_visible()
        expect(image).to_have_attribute("src", "/media/document-1")
        expect(page.locator('img[data-preview-src="/media/document-1"]')).not_to_be_visible()
        self.assertEqual(page.locator('noscript img[src="/media/qr"]').count(), 1)

    def test_statistics_wait_pause_and_never_overlap_or_reset_selection(self):
        page, requests, pending = self.open_page(self.dashboard, suffix="/dashboard", clock=True)
        calls = lambda: [request for request in requests if urlparse(request.url).path == "/stats"]
        self.assertEqual(len(calls()), 0)
        page.get_by_role("button", name="September", exact=False).click()
        expect(page.locator("[data-detail-title]")).to_contain_text("September")
        with page.expect_request(BASE + "/stats"):
            page.clock.fast_forward(60000)
        page.wait_for_timeout(1)
        self.assertEqual(len(calls()), 1)
        self.assertEqual(len(pending), 1)
        page.clock.fast_forward(120000)
        self.assertEqual(len(calls()), 1, "An unfinished request must not overlap")
        pending.pop().fulfill(json=statistics(version=2))
        expect(page.locator("[data-detail-count]")).to_have_text("2 Fahrzeuge")
        expect(page.locator("[data-detail-title]")).to_contain_text("September")
        page.evaluate("window.testHidden=true;document.dispatchEvent(new Event('visibilitychange'))")
        page.clock.fast_forward(120000)
        self.assertEqual(len(calls()), 1, "Hidden tabs must not poll")
        with page.expect_request(BASE + "/stats"):
            page.evaluate("window.testHidden=false;document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_timeout(1)
        self.assertEqual(len(calls()), 2)
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        self.assertEqual(len(calls()), 2, "Repeated visibility events must not overlap")
        pending.pop().fulfill(status=503, body="synthetic unavailable")
        expect(page.locator("[data-stat-live-text]")).to_contain_text("nächster Versuch")
        expect(page.locator("[data-detail-title]")).to_contain_text("September")
        with page.expect_request(BASE + "/stats"):
            page.clock.fast_forward(60000)
        page.wait_for_timeout(1)
        self.assertEqual(len(calls()), 3, "A failure must allow the next scheduled refresh")
        pending.pop().fulfill(json=statistics(version=3))
        expect(page.locator("[data-detail-count]")).to_have_text("3 Fahrzeuge")
        self.assertFalse(any(request.method != "GET" for request in requests))

    def test_versioned_public_core_assets_keep_logo_content_and_display_size(self):
        with Image.open(ROOT / "static/logo.png") as original, Image.open(ROOT / "static/logo-nav-192.webp") as optimized:
            self.assertEqual(original.size, (1536, 1024))
            self.assertEqual(optimized.size, (192, 128))
            resized = original.resize(optimized.size, Image.Resampling.LANCZOS).convert("RGB")
            self.assertIsNone(ImageChops.difference(resized, optimized.convert("RGB")).getbbox())
        self.assertLess((ROOT / "static/logo-nav-192.webp").stat().st_size, 12000)
        for admin, expected_size in ((True, (48, 32)), (False, (57, 38))):
            with self.subTest(admin=admin):
                html = render("base.html", full_base=True, session={"admin": admin},
                              request=SimpleNamespace(path="/admin/auftrag/71", endpoint="auftrag_detail", args={}))
                page, _, _ = self.open_page(html)
                logo = page.locator(".navbar-brand img")
                expect(logo).to_have_attribute("src", static_asset_url("logo-nav-192.webp"))
                box = logo.bounding_box()
                self.assertAlmostEqual(box["width"], expected_size[0], delta=0.1)
                self.assertAlmostEqual(box["height"], expected_size[1], delta=0.1)
                for name in ("admin-sidebar.css", "admin-sidebar.js") + (("werkstatt_theme.css",) if admin else ()):
                    self.assertEqual(page.locator(f'[href="{static_asset_url(name)}"], [src="{static_asset_url(name)}"]').count(), 1)
                self.assertEqual(page.locator('script[src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"]').count(), 1)
        self.assertIn(static_asset_url("inspektionscheckliste.css"), self.detail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
