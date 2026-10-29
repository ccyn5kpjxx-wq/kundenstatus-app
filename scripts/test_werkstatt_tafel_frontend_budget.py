"""Synthetic board/network regressions: no app import or external traffic.

Browser requests are intercepted on a reserved .invalid origin; the redirect
case uses its own short-lived loopback server. Public repository assets are
served from memory; no external traffic, real portal, accounts or DBs.
Requires Playwright and an installed Edge browser, like the existing UI suite.
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import time
from urllib.parse import urlparse
import unittest

from jinja2 import ChainableUndefined, Environment, FileSystemLoader
from playwright.sync_api import Error, expect, sync_playwright


ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://synthetic-board.invalid'
BOARD = '/werkstatt/tafel'
GROUP = '/static/homepage/optimized/homepage/team-daniel-hannes-abdul-v2.png-w1448-a217ce7d6290.webp'
LOGO = '/static/logo-nav-192.webp'
MUSIC = '/static/audio/werkstatt-team-cinematic-v1.mp3'


def url_for(endpoint, **values):
    return '/static/' + values['filename'] if endpoint == 'static' else '/synthetic/' + endpoint


def render_board():
    env = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=True, undefined=ChainableUndefined)
    env.globals.update(url_for=url_for, static_asset_url=lambda name: '/static/' + name + '?v=synthetic',
                       csrf_token=lambda: 'synthetic', get_flashed_messages=lambda **kwargs: [])
    order = dict(id=71, status=2, leistungsart='lackierung', planung={},
                 fahrzeug='Synthetic vehicle', kennzeichen='TEST-071', auftragsart_label='Lackierung')
    return env.get_template('werkstatt_tafel.html').render(
        session={}, server_now_iso='2026-10-29T12:00:00+01:00', stand_label='12:00', datum_label='Synthetic',
        spalten=[dict(key='eingeplant', titel='Eingeplant', ziel_status=2, auftraege=[order])])


class BoardFrontendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        cls.addClassCleanup(cls.playwright.stop)
        cls.browser = cls.playwright.chromium.launch(channel='msedge', headless=True, args=['--mute-audio'], timeout=15000)
        cls.addClassCleanup(cls.browser.close)
        cls.html = render_board()
        expect.set_options(timeout=2000)

    def open_board(self, *, music=True, showcase_only=False):
        context = self.browser.new_context(viewport=dict(width=1280, height=900), service_workers='block')
        page = context.new_page()
        page.set_default_timeout(3000)
        state = dict(requests=[], heads=[], gets=0, bytes={}, forbidden=[])
        def close_context():
            for route in state['heads']:
                try:
                    route.abort()
                except Error:
                    pass  # Completed/aborted requests are already handled.
            page.unroute_all(behavior='ignoreErrors')
            context.close()
        self.addCleanup(close_context)
        html = self.html
        if showcase_only:
            # The include also works independently of the board's inline script.
            env = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=True)
            env.globals['url_for'] = url_for
            html = ('<!doctype html><meta charset="utf-8"><button data-team-oeffnen>Team</button>'
                    '<link rel="stylesheet" href="/static/werkstatt_team_showcase.css">'
                    + env.get_template('_werkstatt_team_showcase.html').render()
                    + '<script defer src="/static/werkstatt_team_showcase.js"></script>')

        def intercept(route):
            req = route.request
            parsed = urlparse(req.url)
            path = parsed.path
            state['requests'].append((req.method, path))
            if parsed.netloc != 'synthetic-board.invalid':
                # Font/weather requests may exist, but never leave this fixture.
                state['forbidden'].append(req.url)
                route.abort()
            elif path == BOARD:
                if req.method == 'HEAD':
                    state['heads'].append(route)
                else:
                    state['gets'] += 1
                    if state['gets'] == 1:
                        route.fulfill(body=html, content_type='text/html')
                    else:
                        route.fulfill(body='<p>Fresh synthetic board</p>', content_type='text/html')
            elif path.startswith('/static/'):
                asset = (ROOT / path.lstrip('/')).resolve()
                if ROOT / 'static' not in asset.parents or not asset.is_file():
                    route.abort()
                    return
                payload = asset.read_bytes()
                state['bytes'].setdefault(path, len(payload))
                mime = {'.js': 'application/javascript', '.css': 'text/css', '.png': 'image/png',
                        '.webp': 'image/webp', '.mp3': 'audio/mpeg'}.get(asset.suffix, 'application/octet-stream')
                route.fulfill(body=payload, content_type=mime)
            else:
                route.abort()

        page.route('**/*', intercept)
        page.clock.install()
        page.add_init_script("""window.testHidden=false;
          Object.defineProperty(document,'visibilityState',{get:()=>window.testHidden?'hidden':'visible'});
          Object.defineProperty(document,'hidden',{get:()=>window.testHidden});""")
        if not music:
            page.add_init_script("localStorage.setItem('werkstatt-team-showcase-ton-v1','aus');")
        page.goto(BASE + BOARD, wait_until='domcontentloaded', timeout=5000)
        return page, state

    def wait_for(self, page, predicate):
        deadline = time.monotonic() + 3
        while not predicate() and time.monotonic() < deadline:
            page.wait_for_timeout(10)
        self.assertTrue(predicate(), 'Synthetic browser condition did not arrive')

    def start_probe(self, page, state, *, advance=60001, count=1):
        page.clock.fast_forward(advance)
        self.wait_for(page, lambda: len(state['heads']) >= count)

    @staticmethod
    def visibility(page, hidden):
        page.evaluate('(hidden)=>{window.testHidden=hidden;document.dispatchEvent(new Event("visibilitychange"));}', hidden)

    def test_initial_media_budget_and_logo_geometry(self):
        page, state = self.open_board()
        expect(page.locator('[data-team-showcase]')).to_be_hidden()
        self.assertEqual(page.locator('[data-team-showcase] img[src], [data-team-showcase] audio[src]').count(), 0)
        images = {path: size for path, size in state['bytes'].items() if path.endswith(('.webp', '.png'))}
        self.assertEqual(set(images), {GROUP, LOGO})
        self.assertLess(sum(images.values()), 100000)
        self.assertNotIn(MUSIC, state['bytes'])
        logo = page.locator('.topbar .titel img').bounding_box()
        self.assertAlmostEqual(logo['width'] / logo['height'], 1.5, places=2)
        self.assertEqual(page.locator('[data-team-showcase] audio').get_attribute('preload'), 'none')

    def test_showcase_opens_loads_current_slide_and_music_then_stops_and_reopens(self):
        page, state = self.open_board()
        page.locator('[data-team-oeffnen]').click()
        expect(page.locator('[data-team-showcase]')).to_be_visible()
        expect(page.locator('[data-team-slide]').nth(0).locator('img')).to_have_attribute('src', GROUP)
        self.assertIsNone(page.locator('[data-team-slide]').nth(1).locator('img').get_attribute('src'))
        expect(page.locator('[data-team-musik]')).to_have_attribute('src', MUSIC)
        self.wait_for(page, lambda: MUSIC in state['bytes'])
        page.clock.fast_forward(1900)
        expect(page.locator('[data-team-slide]').nth(1).locator('img')).to_have_attribute('src', '/static/homepage/portrait.webp')
        page.keyboard.press('Escape')
        expect(page.locator('[data-team-showcase]')).to_be_hidden()
        self.assertTrue(page.locator('[data-team-musik]').evaluate('(audio)=>audio.paused'))
        before = list(state['requests'])
        page.locator('[data-team-oeffnen]').click()
        expect(page.locator('[data-team-showcase]')).to_be_visible()
        self.assertEqual(sum(path == GROUP for _, path in state['requests']), sum(path == GROUP for _, path in before))

    def test_muted_standalone_include_keeps_five_minute_automatic_cycle(self):
        page, state = self.open_board(music=False, showcase_only=True)
        page.clock.fast_forward(299000)
        expect(page.locator('[data-team-showcase]')).to_be_hidden()
        page.clock.fast_forward(1100)
        expect(page.locator('[data-team-showcase]')).to_be_visible()
        self.assertNotIn(MUSIC, state['bytes'])
        page.clock.run_for(22000)
        expect(page.locator('[data-team-showcase]')).to_be_hidden()
        self.assertEqual(page.locator('[data-team-slide] img[src]').count(), 6)

    def test_visible_refresh_has_one_probe_then_one_navigation(self):
        page, state = self.open_board()
        self.start_probe(page, state)
        page.clock.fast_forward(5000)
        self.assertEqual(len(state['heads']), 1)
        self.assertEqual(state['gets'], 1)
        state['heads'][0].fulfill(status=200)
        expect(page.get_by_text('Fresh synthetic board')).to_be_visible()
        self.assertEqual(state['gets'], 2)

    def test_hidden_page_waits_then_refreshes_when_visible(self):
        page, state = self.open_board()
        self.visibility(page, True)
        page.clock.fast_forward(65000)
        self.assertEqual(len(state['heads']), 0)
        self.visibility(page, False)
        self.start_probe(page, state, advance=5000)
        state['heads'][0].fulfill(status=200)
        expect(page.get_by_text('Fresh synthetic board')).to_be_visible()

    def test_hiding_aborts_inflight_without_offline_warning_and_can_retry(self):
        page, state = self.open_board()
        self.start_probe(page, state)
        self.visibility(page, True)
        page.clock.fast_forward(10000)
        self.assertEqual(page.locator('[data-verbindungs-hinweis]').count(), 0)
        self.assertEqual(state['gets'], 1)
        self.visibility(page, False)
        self.start_probe(page, state, advance=5000, count=2)
        state['heads'][1].fulfill(status=200)
        expect(page.get_by_text('Fresh synthetic board')).to_be_visible()

    def test_timeout_releases_probe_and_http_failure_uses_one_warning_then_recovers(self):
        page, state = self.open_board()
        self.start_probe(page, state)
        page.clock.fast_forward(8500)
        expect(page.locator('[data-verbindungs-hinweis]')).to_be_visible()
        self.assertEqual(len(state['heads']), 1)
        self.start_probe(page, state, advance=35000, count=2)
        state['heads'][1].fulfill(status=503)
        expect(page.locator('[data-verbindungs-hinweis]')).to_have_count(1)
        self.start_probe(page, state, advance=35000, count=3)
        state['heads'][2].fulfill(status=200)
        expect(page.get_by_text('Fresh synthetic board')).to_be_visible()

    def test_network_failure_releases_probe_for_retry(self):
        page, state = self.open_board()
        self.start_probe(page, state)
        state['heads'][0].abort('failed')
        expect(page.locator('[data-verbindungs-hinweis]')).to_be_visible()
        self.start_probe(page, state, advance=35000, count=2)
        state['heads'][1].fulfill(status=200)
        expect(page.get_by_text('Fresh synthetic board')).to_be_visible()

    def test_late_user_interaction_dialog_panel_and_drag_prevent_navigation(self):
        for change in ('click', 'panel', 'team', 'drag'):
            with self.subTest(change=change):
                page, state = self.open_board(music=False)
                self.start_probe(page, state)
                if change == 'click':
                    page.locator('[data-uhr]').click()
                elif change == 'panel':
                    page.evaluate("document.body.insertAdjacentHTML('beforeend','<button data-prod-toggle aria-expanded=\"true\"></button>')")
                elif change == 'team':
                    page.locator('[data-team-showcase]').evaluate('(el)=>{el.hidden=false;}')
                else:
                    page.locator('[data-auftrag="71"].karte').evaluate('(el)=>el.dispatchEvent(new DragEvent("dragstart",{dataTransfer:new DataTransfer()}))')
                state['heads'][0].fulfill(status=200)
                page.wait_for_timeout(50)
                self.assertEqual(state['gets'], 1)
                if change == 'panel':
                    page.locator('[data-prod-toggle]').evaluate('(el)=>el.remove()')
                elif change == 'team':
                    page.locator('[data-team-showcase]').evaluate('(el)=>{el.hidden=true;}')
                elif change == 'drag':
                    page.locator('[data-auftrag="71"].karte').dispatch_event('dragend')
                self.start_probe(page, state, advance=65000 if change == 'click' else 5000, count=2)
                state['heads'][1].fulfill(status=200)
                expect(page.get_by_text('Fresh synthetic board')).to_be_visible()

    def test_expired_session_follows_normal_login_redirect(self):
        # Browser interception deliberately does not re-route fetch redirects.
        # A short-lived own loopback server proves the real redirect chain.
        state, html = dict(gets=0, requests=[]), self.html.encode()
        class LoginFixture(BaseHTTPRequestHandler):
            def respond(handler, head=False):
                state['requests'].append((handler.command, handler.path))
                if handler.path == BOARD:
                    if not head:
                        state['gets'] += 1
                    if head or state['gets'] > 1:
                        handler.send_response(302)
                        handler.send_header('Location', '/werkstatt')
                        handler.end_headers()
                        return
                    body = html
                else:
                    body = b'<p>Synthetic login</p>'
                handler.send_response(200)
                handler.send_header('Content-Type', 'text/html; charset=utf-8')
                handler.send_header('Content-Length', str(len(body)))
                handler.end_headers()
                if not head:
                    handler.wfile.write(body)
            def do_GET(handler): handler.respond()
            def do_HEAD(handler): handler.respond(head=True)
            def log_message(*args): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), LoginFixture)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def stop_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.addCleanup(stop_server)
        origin = 'http://127.0.0.1:' + str(server.server_port)
        context = self.browser.new_context(service_workers='block')
        self.addCleanup(context.close)
        page = context.new_page()
        # Only our server's exact two synthetic paths may reach a network.
        page.route('**/*', lambda route: route.continue_() if route.request.url in
                   {origin + BOARD, origin + '/werkstatt'} else route.abort())
        page.clock.install()
        page.goto(origin + BOARD, wait_until='domcontentloaded', timeout=5000)
        page.clock.fast_forward(60001)
        expect(page.get_by_text('Synthetic login')).to_be_visible()
        expect(page).to_have_url(origin + '/werkstatt')
        self.assertEqual(state['gets'], 2)
        self.assertIn(('HEAD', '/werkstatt'), state['requests'])
        self.assertIn(('GET', '/werkstatt'), state['requests'])


if __name__ == '__main__':
    unittest.main()
