"""Mailbox recovery regression with synthetic HTTP responses; no real mail access.

Run: python scripts/test_mailbox_browser.py (Playwright + Edge required).
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlparse

from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:59991"


def rendered_mailbox(flags=False):
    env = Environment(loader=ChoiceLoader([
        DictLoader({"base.html": "<!doctype html><html><head><meta charset='utf-8'>{% block extra_css %}{% endblock %}</head><body>{% block content %}{% endblock %}</body></html>"}),
        FileSystemLoader(ROOT / "templates"),
    ]), autoescape=True)
    return env.get_template("mailbox.html").render(
        mail_write=False, mail_move=True, mail_flags=flags, mail_address="test@example.invalid",
        csrf_token=lambda: "synthetic-csrf",
        url_for=lambda endpoint, **kw: "/login" if endpoint == "login" else "/" + endpoint,
    )


def message(uid):
    return dict(uid=str(uid), sender="Werkstatt-Test", subject=f"Testnachricht {uid}",
                date="2026-09-01T12:00:00+02:00", unread=True, to="test@example.invalid",
                reply_to="test@example.invalid", body="Synthetische Testnachricht",
                message_id=f"test-{uid}", attachments=[])


class Scenario:
    def __init__(self, page, html):
        self.page, self.html = page, html
        self.responses, self.requests, self.pending = [], [], []
        self.hold = False
        self.hold_detail = False
        self.pending_detail = []
        self.move_calls = 0
        page.route("**/*", self.route)

    def listing(self, url):
        query = parse_qs(urlparse(url).query)
        number = int(query.get("page", ["1"])[0])
        return dict(folder=query.get("folder", ["INBOX"])[0], page=number, total=90,
                    validity="7", folders=[dict(id="INBOX", label="INBOX", flags=""),
                                           dict(id="Trash", label="Trash", flags="\\Trash")],
                    messages=[message(100 - (number - 1) * 30 - i) for i in range(30)])

    def route(self, route):
        path = urlparse(route.request.url).path
        if path == "/admin/mail/messages":
            self.requests.append(route.request.url)
            if self.hold:
                self.pending.append(route)
                return
            response = self.responses.pop(0) if self.responses else "ok"
            if response == "redirect":
                route.fulfill(status=302, headers={"Location": BASE + "/login"})
            elif response == "network":
                route.abort("failed")
            elif response == "brokenbody":
                route.continue_()
            elif isinstance(response, int):
                route.fulfill(status=response, content_type="text/html", body="Temporary test failure")
            else:
                route.fulfill(json=self.listing(route.request.url))
        elif path.endswith("/move"):
            self.move_calls += 1
            route.fulfill(status=503, content_type="text/html", body="Temporary test failure")
        elif path.startswith("/admin/mail/message/"):
            if self.hold_detail:
                self.pending_detail.append(route)
            else:
                route.fulfill(json=message(path.rsplit("/", 1)[1]))
        elif path == "/login":
            route.fulfill(content_type="text/html", body="<h1>Anmelden</h1>")
        elif path == "/test":
            route.fulfill(content_type="text/html", body=self.html)
        else:
            route.fulfill(status=204)

    def start(self):
        self.page.goto(BASE + "/test")

    def loaded(self, number=1):
        expect(self.page.locator("#count")).to_have_text(f"90 Nachrichten · Seite {number}")
        expect(self.page.locator("#messages button")).to_have_count(30)
        expect(self.page.locator("#messages")).to_have_attribute("aria-busy", "false")


def run():
    # Chromium follows a fulfilled redirect outside the first request's route.
    # Serve only the synthetic login page on an isolated ephemeral loopback port.
    class LoginHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith('/admin/mail/messages?'):
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', '99999')
                self.end_headers()
                self.wfile.write(b'{"messages":[')
                self.wfile.flush()
                self.close_connection = True
                return
            self.send_response(200 if self.path == '/login' else 404)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'<h1>Anmelden</h1>')
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), LoginHandler)
    Thread(target=server.serve_forever, daemon=True).start()
    global BASE
    BASE = f'http://127.0.0.1:{server.server_port}'
    html = rendered_mailbox()
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        context.set_default_timeout(12000)

        page = context.new_page(); s = Scenario(page, html)
        s.responses = [502, 503, "ok"]
        s.start(); s.loaded()
        assert len(s.requests) == 3
        expect(page.locator("#mail-status")).to_have_text("")
        for width in (1440, 390):
            page.set_viewport_size({"width": width, "height": 900})
            box = page.locator("#messages")
            assert box.evaluate("e=>e.scrollHeight>e.clientHeight")
            box.hover(); page.mouse.wheel(0, 1200)
            page.wait_for_function("document.querySelector('#messages').scrollTop > 0")
            expect(page.locator("#next")).to_be_visible()
        page.set_viewport_size({"width": 1440, "height": 900})
        print("PASS transient HTML gateway errors recover; desktop/mobile scroll")

        s.responses = [502, 502, 502]
        page.locator("#next").click()
        expect(page.get_by_role("button", name="Erneut laden", exact=True)).to_be_visible()
        s.loaded(1)
        assert page.locator("#messages .subject").first.inner_text() == "Testnachricht 100"
        page.get_by_role("button", name="Erneut laden", exact=True).click(); s.loaded(2)
        assert parse_qs(urlparse(s.requests[-1]).query)["page"] == ["2"]
        print("PASS failed page turn retains previous mail and retries requested page")

        s.hold = True
        page.locator("#refresh").click()
        expect(page.locator("#refresh")).to_be_disabled()
        expect(page.locator("#next")).to_be_disabled()
        expect(page.locator("#folders button").first).to_be_disabled()
        before = len(s.requests)
        page.locator("#query").fill("Test")
        page.locator("#query").press("Enter")
        page.locator("#query").press("Enter")
        assert len(s.requests) == before and len(s.pending) == 1
        route = s.pending.pop(); s.hold = False
        route.fulfill(json=s.listing(route.request.url)); s.loaded(2)
        page.locator("#query").fill("Test")
        page.locator("#query").press("Enter"); s.loaded(1)
        assert parse_qs(urlparse(s.requests[-1]).query)["q"] == ["Test"]
        print("PASS duplicate loads blocked; search retains q parameter")

        page.locator('#compose').click()
        page.locator('#compose-body').fill('Unfertige Antwort behalten')
        page.locator('#refresh').click(); s.loaded(1)
        expect(page.locator('#compose-body')).to_have_value('Unfertige Antwort behalten')
        print('PASS refreshing inbox preserves unsaved composer')

        page.locator("#messages button").first.click()
        page.get_by_role("button", name="In den Papierkorb", exact=True).click()
        expect(page.locator("#mail-status")).to_contain_text("vorübergehend")
        expect(page.get_by_role("button", name="In den Papierkorb", exact=True)).to_be_enabled()
        assert s.move_calls == 1
        print("PASS write request not automatically retried")

        s.hold_detail = True
        page.locator('#messages button').first.click()
        expect(page.locator('#reader')).to_contain_text('Nachricht wird geladen')
        page.locator('#next').click(); s.loaded(2)
        assert len(s.pending_detail) == 1
        s.pending_detail.pop().fulfill(json=message(100))
        expect(page.locator('#reader')).to_have_text('Wähle eine Nachricht zum Lesen.')
        print('PASS late message response cannot replace current reader')
        page.close()

        for error in ("redirect", 401):
            page = context.new_page(); s = Scenario(page, html); s.responses = [error]
            s.start()
            expect(page.get_by_role("link", name="Neu anmelden", exact=True)).to_have_attribute("href", "/login")
            expect(page.locator("#mail-status")).to_contain_text("Anmeldung ist abgelaufen")
            assert len(s.requests) == 1
            expect(page.locator("#next")).to_be_disabled()
            page.close()
        print("PASS login redirect and 401 explain session expiry without retry")

        page = context.new_page(); s = Scenario(page, html); s.responses = ["network", "ok"]
        s.start(); s.loaded(); assert len(s.requests) == 2
        print("PASS interrupted read connection recovers")
        page.close()
        page = context.new_page(); s = Scenario(page, html); s.responses = ['brokenbody', 'ok']
        s.start(); s.loaded(); assert len(s.requests) == 2
        print('PASS connection interrupted after HTTP headers recovers')
        page.close()
        page = context.new_page(); s = Scenario(page, html); s.responses = [502, 502, 502]
        s.start()
        expect(page.get_by_role('button', name='Erneut laden', exact=True)).to_be_visible()
        page.locator('#compose').click()
        page.locator('#compose-body').fill('Entwurf nach fehlgeschlagenem Start')
        page.get_by_role('button', name='Erneut laden', exact=True).click(); s.loaded()
        expect(page.locator('#compose-body')).to_have_value('Entwurf nach fehlgeschlagenem Start')
        print('PASS first successful load also preserves unsaved composer')
        browser.close()
    server.shutdown()
    server.server_close()


if __name__ == "__main__":
    run()
