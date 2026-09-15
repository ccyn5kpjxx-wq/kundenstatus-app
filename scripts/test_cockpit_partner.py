"""Offline partner order UI checks. Synthetic data; every request intercepted."""
from pathlib import Path
import mimetypes
import re
import subprocess
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright
from test_cockpit_customer import BASE, fixture, template_environment

ROOT = Path(__file__).resolve().parents[1]
BASELINE = '9f463c96aba98b794faba5becf58f2ecac155242'


def render(source=None, *, review=True, status=3):
    data = fixture(status=status)
    data['autohaus'].update(name='Autohaus Beispiel', portal_label='Portal Autohaus Beispiel',
                            portal_welcome='Willkommen im persönlichen Terminbereich.',
                            partner_logo='partner_logos/autohaus-guenther.svg', partner_logo_alt='Partnerlogo')
    data['auftrag'].update(fahrzeug='Audi A3', kennzeichen='DEMO-A 123', analyse_pruefen=review,
                          analyse_text='Stoßfänger instand setzen und lackieren',
                          produktion_schritt_label='Lackierung', beschreibung='Vollständiger Arbeitsumfang',
                          transport_art='hol_und_bring', quelle='autohaus',
                          fin_nummer='SYNTHETIC000000001', kontakt_telefon='0000000000')
    data['transport_arten']['hol_und_bring'] = {
        'label': 'Hol- und Bringservice', 'partner_annahme_label': 'Wir holen Ihr Fahrzeug',
        'partner_abholung_label': 'Wir bringen Ihr Fahrzeug zurück',
    }
    data['chat_nachrichten'] = [dict(id=1, absender='werkstatt', nachricht='Die Lackierung ist vorbereitet.', erstellt_am='15.09.2026 12:00')]
    env = template_environment()
    # Keep the real clock: its original fixed positioning used to cover the partner logo.
    env.loader.loaders[0].mapping.pop('_live_clock_widget.html', None)
    return (env.from_string(source) if source else env.get_template('partner_auftrag.html')).render(**data)


class Scenario:
    def __init__(self, page, html):
        self.html, self.loads, self.posts = html, 0, []
        page.route('**/*', self.route)

    def route(self, route):
        path = urlparse(route.request.url).path
        if route.request.method == 'POST':
            self.posts.append(route.request.post_data or '')
            route.fulfill(content_type='text/html', body='<p>Nur synthetische Testantwort</p>')
        elif path == '/test':
            self.loads += 1
            route.fulfill(content_type='text/html', body=self.html)
        elif path.startswith('/static/'):
            asset = (ROOT / path.lstrip('/')).resolve()
            if asset.is_relative_to(ROOT / 'static') and asset.is_file():
                route.fulfill(content_type=mimetypes.guess_type(str(asset))[0] or 'application/octet-stream', body=asset.read_bytes())
            else:
                route.fulfill(status=404)
        else:
            route.fulfill(status=204)


def signatures(page):
    return page.locator('form').evaluate_all("""forms => forms.map(form => JSON.stringify({
      method:form.method, action:form.getAttribute('action') || '', enctype:form.enctype,
      fields:Array.from(form.querySelectorAll('input,select,textarea,button')).filter(e=>e.name && e.name!=='csrf_token')
        .map(e=>[e.tagName,e.name,e.type,e.tagName==='BUTTON'?e.value:'']).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)))
    })).sort()""")


def run():
    old_source = subprocess.check_output(['git', 'show', BASELINE+':templates/partner_auftrag.html'], cwd=ROOT).decode('utf-8')
    html = render()
    with sync_playwright() as p:
        browser = p.chromium.launch(channel='msedge', headless=True)

        def fresh(markup=html, width=1280, js=True, anchor=''):
            context = browser.new_context(viewport={'width':width,'height':900}, java_script_enabled=js, reduced_motion='reduce')
            page = context.new_page()
            page.set_default_timeout(5000)
            scenario = Scenario(page, markup)
            errors=[]
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(BASE+'/test'+anchor)
            return context,page,scenario,errors

        old_context,old_page,_,_ = fresh(render(old_source))
        old_signatures = signatures(old_page)
        old_context.close()
        context,page,scenario,errors = fresh()
        assert signatures(page) == old_signatures, 'A form, endpoint, or submitted field contract changed'
        assert page.get_by_role('tab').count() >= 4
        expect(page.get_by_role('heading',name='Audi A3',exact=True)).to_be_visible()
        assert 'Symbolbild' in page.inner_text('body')
        assert not errors, errors
        assert page.locator('[data-live-clock-root]').evaluate('(e)=>getComputedStyle(e).position') == 'static'
        print('PASS modern partner page preserves every form, endpoint and submitted field')

        # A review action must still lead straight to the complete original form.
        page.get_by_role('button',name='Daten prüfen',exact=True).click()
        expect(page.locator('#auftrag-daten')).to_be_visible()
        expect(page.locator('#auftrag-daten')).to_have_attribute('open','')
        page.locator('#auftrag-daten input[name="kennzeichen"]').fill('UNSAVED')
        page.get_by_role('tab',name=re.compile('Nachrichten')).click()
        expect(page.locator('#nachrichten textarea[name="nachricht"]')).to_be_visible()
        page.locator('#nachrichten textarea[name="nachricht"]').fill('Ungesendeter Entwurf')
        page.get_by_role('tab',name=re.compile('Auftrag|Termine')).click()
        expect(page.locator('#auftrag-daten input[name="kennzeichen"]')).to_have_value('UNSAVED')
        assert not scenario.posts
        print('PASS tabs preserve complete order fields and an unsent message')

        page.get_by_role('tab',name=re.compile('Unterlagen')).click()
        upload = page.locator('#pflicht-upload input[type="file"]')
        if not upload.is_visible():
            page.locator('#schnell-upload > summary').click()
        upload.set_input_files(dict(name='test.pdf',mimeType='application/pdf',buffer=b'%PDF synthetic'))
        page.get_by_role('tab',name='Übersicht',exact=True).click()
        assert upload.evaluate('(input)=>input.files.length') == 1
        page.locator('[data-portal-refresh]').click()
        expect(page.locator('[data-refresh-warning]')).to_be_visible()
        assert scenario.loads == 1 and not scenario.posts
        print('PASS selected attachment and drafts survive tabs and guarded refresh')
        context.close()

        for anchor in ('#nachrichten','#pflicht-schadennummer','#pflicht-upload','#reklamation'):
            context,page,scenario,errors = fresh(anchor=anchor)
            expect(page.locator(anchor)).to_be_visible()
            assert not errors, errors
            assert not scenario.posts
            print('PASS direct link reveals target and all parent sections: '+anchor)
            context.close()

        context,page,_,_ = fresh()
        first_tab=page.get_by_role('tab',name='Übersicht',exact=True)
        first_tab.focus()
        first_tab.press('End')
        expect(page.get_by_role('tab').last).to_be_focused()
        page.get_by_role('tab').last.press('Home')
        expect(first_tab).to_be_focused()
        print('PASS keyboard tab navigation follows Home and End')
        context.close()

        context,page,scenario,errors=fresh(render(status=2))
        expect(page.get_by_role('button',name=re.compile('abholbereit',re.I))).to_be_visible()
        assert not scenario.posts
        print('PASS pickup readiness remains a deliberate visible action')
        context.close()

        for width in (390,768):
            context,page,_,errors = fresh(width=width)
            assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth'), width
            expect(page.get_by_role('tab',name='Übersicht',exact=True)).to_be_visible()
            assert not errors, errors
            print(f'PASS responsive partner page at {width}px without horizontal overflow')
            context.close()

        context,page,scenario,_=fresh(js=False,width=390)
        for target in ('#auftrag-daten','#nachrichten','#reklamation','#schnell-upload'):
            expect(page.locator(target+' > summary')).to_be_visible()
        assert not scenario.posts
        print('PASS all original workflows remain accessible without JavaScript')
        context.close()
        browser.close()


if __name__=='__main__':
    run()
