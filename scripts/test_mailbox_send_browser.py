"""Synthetic send recovery in a real browser. Never contacts SMTP or IONOS."""
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, expect
from test_mailbox_browser import Scenario, rendered_mailbox, BASE


class SendScenario(Scenario):
    def __init__(self,page):
        super().__init__(page,rendered_mailbox(send=True))
        self.posts=[];self.copies=[];self.checks=[];self.hold_send=False;self.pending_send=[]
        self.response='sent';self.status='sent';self.token='';self.body=''

    def result(self,state):
        return dict(state=state,token=self.token,subject='Testangebot',to='customer@example.invalid',created_at='2026-09-07 00:00',
                    message={'sent':'E-Mail versandt und in Gesendet abgelegt.','copy_pending':'E-Mail versandt. Kopie in Gesendet fehlt.',
                             'not_sent':'E-Mail wurde nicht versandt.','uncertain':'Versandstatus ist unklar. Nicht erneut senden.',
                             'partial':'Nur ein Teil der Empfänger wurde erreicht.'}.get(state,state),
                    accepted=['yes@example.invalid'] if state=='partial' else [], refused={'no@example.invalid':{'code':550}} if state=='partial' else {},
                    can_retry_copy=state=='copy_pending',copy_pending=state=='copy_pending',payload_available=state!='sent')

    def route(self,route):
        path=urlparse(route.request.url).path
        if path=='/admin/mail/send':
            self.posts.append(route.request);self.body=route.request.post_data or ''
            import re
            self.token=re.search(r'name="send_token"\r\n\r\n([^\r]+)',self.body)[1]
            if self.hold_send:self.pending_send.append(route);return
            if self.response=='network':route.abort('failed')
            elif isinstance(self.response,int):route.fulfill(status=self.response,content_type='text/html',body='Rejected request')
            else:route.fulfill(json=self.result(self.response))
        elif path=='/admin/mail/outbox':route.fulfill(json=dict(items=[self.result(self.status)] if self.token else []))
        elif path.startswith('/admin/mail/outbox/'):
            if path.endswith('/copy'):
                self.copies.append(route.request);self.status='sent';route.fulfill(json=self.result('sent'))
            else:self.checks.append(route.request);route.fulfill(json=self.result(self.status))
        else:super().route(route)

    def compose(self,body='Ein synthetisches Testangebot.'):
        self.page.locator('#compose').click()
        self.page.locator('#compose-to').fill('customer@example.invalid')
        self.page.locator('#compose-subject').fill('Testangebot')
        self.page.locator('#compose-body').fill(body)

    def send(self):self.page.get_by_role('button',name='E-Mail senden',exact=True).click()


def run():
    with sync_playwright() as p:
        browser=p.chromium.launch(channel='msedge',headless=True)
        def fresh():
            ctx=browser.new_context(viewport=dict(width=1440,height=900));ctx.set_default_timeout(8000)
            page=ctx.new_page();s=SendScenario(page);s.start();s.loaded();return ctx,page,s

        ctx,page,s=fresh();s.compose();page.locator('#compose-files').set_input_files(dict(name='angebot.pdf',mimeType='application/pdf',buffer=b'%PDF synthetic'))
        assert not s.posts
        expect(page.locator('.mail-signature')).to_contain_text('Christopher Gärtner')
        expect(page.get_by_role('button',name='Entwurf speichern',exact=True)).to_be_disabled()
        s.send();expect(page.locator('.mail-compose-result')).to_contain_text('in Gesendet abgelegt')
        assert len(s.posts)==1 and 'angebot.pdf' in s.body and 'synthetic-csrf' in s.body
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_disabled()
        print('PASS explicit send only, attachment, signature, no duplicate click');ctx.close()

        for response in (400,403,413,'not_sent'):
            ctx,page,s=fresh();s.response=response;s.compose('Text bleibt erhalten')
            page.locator('#compose-files').set_input_files(dict(name='bild.png',mimeType='image/png',buffer=b'synthetic'))
            s.send();expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_enabled()
            expect(page.locator('#compose-body')).to_have_value('Text bleibt erhalten')
            assert page.locator('#compose-files').evaluate('e=>e.files[0].name')=='bild.png'
            assert len(s.posts)==1
            ctx.close()
        print('PASS known failure, invalid request, CSRF and size rejection retain draft and attachments')

        ctx,page,s=fresh();s.response='network';s.status='copy_pending';s.compose();s.send()
        expect(page.get_by_role('button',name='Versandstatus prüfen',exact=True)).to_be_visible()
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_disabled()
        page.get_by_role('button',name='Versandstatus prüfen',exact=True).click()
        expect(page.locator('.mail-compose-result')).to_contain_text('Kopie in Gesendet fehlt')
        page.locator('#refresh').click();s.loaded()
        expect(page.locator('.mail-compose-result')).to_contain_text('Kopie in Gesendet fehlt')
        page.get_by_role('button',name='Kopie in Gesendet nachholen',exact=True).click()
        expect(page.locator('.mail-compose-result')).to_contain_text('in Gesendet abgelegt')
        assert len(s.posts)==1 and len(s.copies)==1
        print('PASS lost HTTP reply recovers durable status; copy retry never resends');ctx.close()

        ctx,page,s=fresh();s.hold_send=True;s.compose();s.send()
        page.locator('#messages button').first.click();expect(page.locator('#reader h2')).to_have_text('Testnachricht 100')
        s.pending_send.pop().fulfill(json=s.result('sent'))
        expect(page.locator('#reader h2')).to_have_text('Testnachricht 100')
        page.locator('#send-history').click();expect(page.locator('#reader h2')).to_have_text('Versandverlauf')
        expect(page.locator('#reader')).to_contain_text('in Gesendet abgelegt')
        print('PASS late send response does not overwrite opened message; durable history available');ctx.close()

        ctx,page,s=fresh();s.response='uncertain';s.status='uncertain';s.compose();s.send()
        expect(page.get_by_role('button',name='Andere E-Mail schreiben',exact=True)).to_be_visible()
        page.get_by_role('button',name='Andere E-Mail schreiben',exact=True).click()
        expect(page.locator('#compose-body')).to_have_value('')
        page.locator('#send-history').click();expect(page.locator('#reader')).to_contain_text('Nicht erneut senden')
        assert len(s.posts)==1
        page.reload();s.loaded();page.locator('#send-history').click()
        expect(page.locator('#reader')).to_contain_text('Nicht erneut senden')
        print('PASS uncertain send preserved in history across reload; other composing remains possible');ctx.close()

        ctx,page,s=fresh();s.response='network';s.status='not_found';s.compose();s.send()
        first=s.token
        page.get_by_role('button',name='Versandstatus prüfen',exact=True).click()
        expect(page.locator('#compose-body')).to_be_disabled()
        s.response='sent';page.get_by_role('button',name='Dieselbe Nachricht erneut übergeben',exact=True).click()
        expect(page.locator('.mail-compose-result')).to_contain_text('in Gesendet abgelegt')
        assert len(s.posts)==2 and s.token==first
        print('PASS request lost before arrival can be consciously resubmitted with same token');ctx.close()

        ctx,page,s=fresh();s.response='partial';s.status='partial';s.compose();s.send()
        expect(page.locator('.mail-compose-result')).to_contain_text('Nicht angenommen: no@example.invalid')
        expect(page.locator('.mail-compose-result')).to_contain_text('Vom Mailserver angenommen: yes@example.invalid')
        page.locator('#send-history').click();expect(page.locator('#reader')).to_contain_text('Nicht angenommen: no@example.invalid')
        assert len(s.posts)==1
        print('PASS partial acceptance names refused recipients in composer and history');ctx.close()

        ctx,page,s=fresh();s.compose('Entwurf nach Neuladen')
        page.locator('#compose-files').set_input_files(dict(name='bild.png',mimeType='image/png',buffer=b'synthetic'))
        page.reload();s.loaded();expect(page.locator('#compose-body')).to_have_value('Entwurf nach Neuladen')
        expect(page.locator('.mail-attachment-note')).to_contain_text('Bitte Anhänge erneut auswählen')
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_disabled()
        page.get_by_role('button',name='Anhänge entfernen',exact=True).click()
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_enabled()
        page.set_viewport_size(dict(width=390,height=844))
        assert page.locator('body').evaluate('e=>e.scrollWidth<=window.innerWidth')
        assert not s.posts
        print('PASS reload preserves text, missing files clearly flagged, mobile composer fits');ctx.close()

        ctx,page,s=fresh();s.compose()
        files=[dict(name=name,mimeType='application/pdf',buffer=b'synthetic') for name in ('A.pdf','B.pdf')]
        page.locator('#compose-files').set_input_files(files);page.reload();s.loaded()
        page.locator('#compose-files').set_input_files(files[0]);page.locator('#compose-files').set_input_files(files[1])
        expect(page.locator('.mail-attachment-note')).to_contain_text('A.pdf')
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_disabled()
        page.locator('#compose-files').set_input_files(files)
        expect(page.get_by_role('button',name='E-Mail senden',exact=True)).to_be_enabled()
        print('PASS replacing file selection cannot silently drop a restored attachment');ctx.close()
        browser.close()


if __name__=='__main__':run()
