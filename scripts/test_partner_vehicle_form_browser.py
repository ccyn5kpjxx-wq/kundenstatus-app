"""Offline Edge browser regressions, synthetic fixtures and intercepted requests."""
from pathlib import Path
from types import SimpleNamespace
import json
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright
from test_cockpit_customer import template_environment, fixture

ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://localhost:5087'


def render(form=None, completed=None, post=False):
    data = fixture()
    data.update(form=form or {}, session={'partner_completed_drafts': completed or []},
                request=SimpleNamespace(method='POST' if post else 'GET', path='/neu', args={}))
    data['transport_arten']['hol_und_bring'] = {'label': 'Hol- und Bringservice'}
    return template_environment().get_template('partner_neu.html').render(**data)


class Scenario:
    def __init__(self, page):
        self.html = render()
        self.posts = []
        self.analysis_ok = True
        self.page = page
        page.route('**/*', self.route)

    def route(self, route):
        path = urlparse(route.request.url).path
        if 'analysieren' in path:
            route.fulfill(status=200 if self.analysis_ok else 422, content_type='application/json', body=json.dumps({
                'ok': self.analysis_ok, 'fields': {'farbcode': 'OCR-WERT', 'kunde_name': 'OCR Test'},
                'analysis_token': 'synthetic-only', 'error': 'Test: Analyse fehlgeschlagen',
            }))
        elif route.request.method == 'POST':
            self.posts.append(route.request.post_data_buffer)
            route.fulfill(body='<p>Auftrag angelegt (synthetischer Test)</p>', content_type='text/html')
        elif path == '/neu':
            route.fulfill(body=self.html, content_type='text/html')
        elif path.startswith('/static/'):
            asset = (ROOT / path.lstrip('/')).resolve()
            if asset.is_relative_to(ROOT / 'static') and asset.is_file():
                route.fulfill(body=asset.read_bytes(), content_type='application/javascript')
            else:
                route.fulfill(status=404)
        elif 'bootstrap' in path and (ROOT / '.agent-hub/bootstrap.min.css').exists():
            route.fulfill(body=(ROOT / '.agent-hub/bootstrap.min.css').read_bytes(), content_type='text/css')
        else:
            route.fulfill(body='<p>Andere Seite</p>', content_type='text/html')


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel='msedge', headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 900}, reduced_motion='reduce')
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.on('dialog', lambda dialog: dialog.accept())
        scenario = Scenario(page)
        page.goto(BASE + '/neu')
        expect(page.locator('#fahrzeug')).to_be_visible()
        expect(page.locator('[data-document-options]')).not_to_have_attribute('open', '')
        page.locator('#fahrzeug').fill('Testwagen')
        page.locator('#farbcode').fill('LB9A / Z5Z5')
        page.locator('#farbcode').press('Enter')
        expect(page.locator('#partner-step-2')).to_be_visible()
        assert not scenario.posts, 'Enter in step one must never create an order or trigger analysis'
        page.locator('#annahme_datum').fill('2026-10-01')
        page.locator('#abholtermin').fill('2026-10-02')
        page.locator('#annahme_uhrzeit').fill('09:15')
        page.locator('#transport_art').select_option('hol_und_bring')
        page.locator('#abhol_adresse').fill('Synthetischer Testweg 1')
        page.locator('[data-step-back]').click()
        expect(page.locator('#farbcode')).to_have_value('LB9A / Z5Z5')
        page.reload()
        expect(page.locator('#farbcode')).to_have_value('LB9A / Z5Z5')
        expect(page.locator('#annahme_uhrzeit')).to_have_value('09:15')
        expect(page.locator('#abhol_adresse')).to_have_value('Synthetischer Testweg 1')
        expect(page.locator('#fertig_datum')).to_have_value('2026-10-02')
        print('PASS step changes, Enter and reload preserve paint code, dates and transport')

        page.locator('[data-document-options] summary').click()
        expect(page.get_by_role('link', name='Online ausfüllen (neuer Tab)')).to_have_attribute('target', '_blank')
        file = {'name': 'synthetisch.txt', 'mimeType': 'text/plain', 'buffer': b'Synthetic'}
        page.locator('#dateien').set_input_files(file)
        page.locator('[data-upload-analyze]').click()
        expect(page.locator('[data-analysis-result]')).to_be_visible()
        expect(page.locator('#farbcode')).to_have_value('LB9A / Z5Z5')
        expect(page.locator('#kunde_name')).to_have_value('OCR Test')
        page.locator('[data-remove-files]').click()
        expect(page.locator('[data-analysis-completed]')).to_have_value('')
        page.locator('#dateien').set_input_files(file)
        scenario.analysis_ok = False
        page.locator('[data-upload-analyze]').click()
        expect(page.locator('[data-analysis-error]')).to_contain_text('fehlgeschlagen')
        expect(page.locator('#farbcode')).to_have_value('LB9A / Z5Z5')
        page.reload()
        expect(page.locator('[data-draft-status]')).to_contain_text('Dateien erneut')
        expect(page.locator('#farbcode')).to_have_value('LB9A / Z5Z5')
        page.locator('[data-step-next]').click()
        page.locator('[data-save-vehicle]').click()
        expect(page.locator('[data-file-error]')).to_be_visible()
        assert not scenario.posts, 'A previously selected file must not silently disappear'
        page.locator('[data-remove-files]').click()
        expect(page.locator('[data-analysis-file-required]')).to_have_value('')
        print('PASS OCR preserves manual paint code; failure/reload explains file reselection')

        # Server-rendered POST values take precedence over an older browser draft.
        scenario.html = render({'fahrzeug': 'Serverwert', 'farbcode': 'SAW'}, post=True)
        page.reload()
        expect(page.locator('#farbcode')).to_have_value('SAW')
        draft_id = page.locator('[name="_draft_id"]').input_value()
        page.locator('[data-step-next]').click()
        page.locator('[data-save-vehicle]').click()
        expect(page.get_by_text('Auftrag angelegt (synthetischer Test)')).to_be_visible()
        assert b'name="farbcode"\r\n\r\nSAW' in scenario.posts[-1]
        scenario.html = render(completed=[draft_id])
        page.goto(BASE + '/neu')
        expect(page.locator('#farbcode')).to_have_value('')
        print('PASS server validation wins; successful submission is not restored as a new draft')

        # Explicit discard and blocked storage must both leave a usable form.
        page.locator('#farbcode').fill('Discard me')
        page.locator('[data-discard-draft]').click()
        expect(page.locator('#farbcode')).to_have_value('')
        for width in (390, 768, 1280):
            page.set_viewport_size({'width': width, 'height': 900})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
        if (ROOT / '.agent-hub/bootstrap.min.css').exists():
            scenario.html = render()
            page.goto(BASE + '/neu')
            page.set_viewport_size({'width': 1280, 'height': 900})
            page.screenshot(path=str(ROOT / '.agent-hub/partner-neu-desktop.png'), full_page=True)
            page.set_viewport_size({'width': 390, 'height': 844})
            page.screenshot(path=str(ROOT / '.agent-hub/partner-neu-mobile.png'), full_page=True)
        assert not errors, errors
        context.close()

        blocked = browser.new_context()
        blocked.add_init_script("Object.defineProperty(window, 'sessionStorage', {get(){throw new Error('blocked')}})")
        page = blocked.new_page()
        Scenario(page)
        page.goto(BASE + '/neu')
        page.locator('#fahrzeug').fill('Testwagen')
        expect(page.locator('[data-draft-status]')).to_contain_text('nicht möglich')
        page.locator('[data-step-next]').click()
        expect(page.locator('#partner-step-2')).to_be_visible()
        blocked.close()
        print('PASS explicit discard, responsive widths and blocked browser storage')
        browser.close()


if __name__ == '__main__':
    main()
