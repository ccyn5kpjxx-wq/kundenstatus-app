"""Browser flag synchronization regressions with disposable synthetic mail only."""
from urllib.parse import parse_qs, urlparse
import re
from playwright.sync_api import sync_playwright, expect
from test_mailbox_browser import Scenario, message, rendered_mailbox


class FlagScenario(Scenario):
    def __init__(self,page):
        super().__init__(page,rendered_mailbox(flags=True))
        self.flags={};self.flag_calls=[];self.pending_flags=[];self.hold_flags=False;self.flag_failure=None
    def item(self,uid,folder='INBOX'):
        m=message(uid);m['unread']=self.flags.get((folder,str(uid)),True);return m
    def listing(self,url):
        data=super().listing(url)
        data['messages']=[self.item(m['uid'],data['folder']) for m in data['messages']]
        return data
    def finish_flag(self,route,data,*,lost=False):
        uid=urlparse(route.request.url).path.split('/')[-2]
        unread=data['read'][0]=='0';folder=data['folder'][0]
        self.flags[(folder,uid)]=unread
        if lost:route.abort('failed')
        else:route.fulfill(json=dict(ok=True,uid=uid,folder=folder,validity=data['validity'][0],unread=unread))
    def route(self,route):
        path=urlparse(route.request.url).path
        if path.endswith('/read'):
            data={name:[value] for name,value in re.findall(r'name="([^"]+)"\r\n\r\n([^\r]*)',route.request.post_data)}
            self.flag_calls.append((path,data))
            if self.hold_flags:self.pending_flags.append((route,data))
            elif self.flag_failure=='lost':self.finish_flag(route,data,lost=True)
            elif self.flag_failure:route.fulfill(status=502,json={'error':'Lesestatus nicht bestätigt. Bitte aktualisieren.'})
            else:self.finish_flag(route,data)
        elif path.startswith('/admin/mail/message/') and not path.endswith('/move'):
            if self.hold_detail:self.pending_detail.append(route)
            else:
                folder=parse_qs(urlparse(route.request.url).query).get('folder',['INBOX'])[0]
                route.fulfill(json=self.item(path.rsplit('/',1)[-1],folder))
        else:super().route(route)
    def release_flags(self):
        pending,self.pending_flags=self.pending_flags,[];self.hold_flags=False
        for route,data in pending:self.finish_flag(route,data)


def run():
    with sync_playwright() as p:
        browser=p.chromium.launch(channel='msedge',headless=True)
        context=browser.new_context(viewport={'width':1440,'height':900});context.set_default_timeout(12000)
        def start():
            page=context.new_page();scenario=FlagScenario(page);scenario.start();scenario.loaded();return page,scenario
        def row(page,uid):return page.locator(f'.mail-row[data-uid="{uid}"]')
        def read_button(page,name):return page.get_by_role('button',name=name,exact=True)

        page,s=start();row(page,100).click()
        expect(read_button(page,'Als ungelesen markieren')).to_be_enabled()
        expect(row(page,100)).not_to_have_class(__import__('re').compile('unread'))
        assert len(s.flag_calls)==1 and s.flag_calls[0][1]['read']==['1']
        read_button(page,'Als ungelesen markieren').click()
        expect(read_button(page,'Als gelesen markieren')).to_be_enabled()
        expect(row(page,100)).to_have_class(__import__('re').compile('unread'))
        expect(page.locator('#reader h2')).to_have_text('Testnachricht 100')
        assert len(s.flag_calls)==2
        s.flags[('INBOX','100')]=False;page.locator('#refresh').click();s.loaded()
        expect(read_button(page,'Als ungelesen markieren')).to_be_enabled()
        expect(row(page,100)).not_to_have_class(__import__('re').compile('unread'))
        expect(page.locator('#reader h2')).to_have_text('Testnachricht 100')
        page.locator('#compose').click();page.locator('#compose-body').fill('Mein unveröffentlichter Entwurf')
        page.locator('#refresh').click();s.loaded()
        expect(page.locator('#compose-body')).to_have_value('Mein unveröffentlichter Entwurf')
        expect(read_button(page,'E-Mail senden')).to_be_disabled();expect(read_button(page,'Entwurf speichern')).to_be_disabled()
        assert len(s.flag_calls)==2
        print('PASS auto-read, explicit unread, external refresh and composer preservation');page.close()

        page,s=start();s.hold_flags=True;s.flags[('INBOX','99')]=False
        row(page,100).click();expect(read_button(page,'Lesestatus wird gespeichert …')).to_be_disabled()
        row(page,99).click();expect(page.locator('#reader h2')).to_have_text('Testnachricht 99')
        s.release_flags();expect(row(page,100)).not_to_have_class(__import__('re').compile('unread'))
        expect(page.locator('#reader h2')).to_have_text('Testnachricht 99');assert len(s.flag_calls)==1
        print('PASS pending auto-read updates old visible row without replacing current reader');page.close()

        page,s=start();s.flags[('INBOX','100')]=False;row(page,100).click()
        expect(read_button(page,'Als ungelesen markieren')).to_be_enabled();s.hold_flags=True
        read_button(page,'Als ungelesen markieren').dblclick(delay=30)
        expect(read_button(page,'Lesestatus wird gespeichert …')).to_be_disabled()
        s.hold_detail=True;row(page,100).click()
        expect(page.locator('#reader')).to_contain_text('Nachricht wird geladen')
        assert len(s.pending_detail)==0  # Detail cannot capture flags from before the pending write.
        s.release_flags()
        page.wait_for_timeout(100)
        assert len(s.pending_detail)==1
        s.pending_detail.pop().fulfill(json=s.item(100))
        expect(read_button(page,'Als gelesen markieren')).to_be_enabled()
        expect(row(page,100)).to_have_class(__import__('re').compile('unread'));assert len(s.flag_calls)==1
        print('PASS repeated clicks and reopening join one pending manual unread operation');page.close()

        page,s=start();s.hold_flags=True;row(page,100).click()
        expect(read_button(page,'Lesestatus wird gespeichert …')).to_be_disabled()
        before=len(s.requests);page.locator('#refresh').click();expect(page.locator('#refresh')).to_be_disabled()
        assert len(s.requests)==before
        s.release_flags();s.loaded();expect(read_button(page,'Als ungelesen markieren')).to_be_enabled()
        expect(row(page,100)).not_to_have_class(__import__('re').compile('unread'))
        print('PASS refresh waits for pending writes before fetching authoritative list');page.close()

        page,s=start();s.hold_flags=True;row(page,100).click()
        expect(read_button(page,'Lesestatus wird gespeichert …')).to_be_disabled()
        s.responses=[502,502,502];page.locator('#refresh').click();s.release_flags()
        expect(page.get_by_role('button',name='Erneut laden',exact=True)).to_be_visible(timeout=10000)
        expect(read_button(page,'Als ungelesen markieren')).to_be_enabled()
        expect(row(page,100)).not_to_have_class(re.compile('unread'))
        print('PASS confirmed flag survives subsequent failed list refresh');page.close()

        page,s=start();s.flags[('INBOX','100')]=False;row(page,100).click()
        expect(read_button(page,'Als ungelesen markieren')).to_be_enabled();s.hold=True;page.locator('#refresh').click()
        expect(read_button(page,'Als ungelesen markieren')).to_be_disabled()
        expect(read_button(page,'In den Papierkorb')).to_be_disabled();assert len(s.flag_calls)==0
        page.locator('#compose').click();page.locator('#compose-body').fill('Während Aktualisierung geschrieben')
        route=s.pending.pop();data=s.listing(route.request.url);data['messages']=data['messages'][1:];s.hold=False;route.fulfill(json=data)
        expect(page.locator('#messages')).to_have_attribute('aria-busy','false')
        expect(page.locator('#compose-body')).to_have_value('Während Aktualisierung geschrieben')
        print('PASS refresh locks retained controls and cannot destroy a newly opened composer');page.close()

        for failure in ('refused','lost'):
            page,s=start();s.flag_failure=failure;row(page,100).click()
            expect(page.get_by_role('button',name='Erneut laden',exact=True)).to_be_visible()
            expect(row(page,100)).to_have_class(__import__('re').compile('unread'));assert len(s.flag_calls)==1
            page.get_by_role('button',name='Erneut laden',exact=True).click()
            expect(read_button(page,'Als gelesen markieren' if failure=='refused' else 'Als ungelesen markieren')).to_be_enabled()
            assert len(s.flag_calls)==1
            print('PASS failed/uncertain write reconciles by PEEK without automatic POST replay:',failure);page.close()

        page,s=start();s.hold_detail=True
        row(page,100).click();expect(page.locator('#reader')).to_contain_text('Nachricht wird geladen')
        row(page,99).click();expect(page.locator('#reader')).to_contain_text('Nachricht wird geladen')
        assert len(s.pending_detail)==2
        s.pending_detail.pop(0).fulfill(json=s.item(100));assert len(s.flag_calls)==0
        s.pending_detail.pop(0).fulfill(json=s.item(99));expect(read_button(page,'Als ungelesen markieren')).to_be_enabled()
        assert len(s.flag_calls)==1 and '/99/read' in s.flag_calls[0][0]
        print('PASS stale detail response never automatically marks a different message');page.close()

        page,s=start();s.hold_flags=True;row(page,100).click()
        expect(read_button(page,'Lesestatus wird gespeichert …')).to_be_disabled()
        page.get_by_role('button',name='Papierkorb',exact=True).click();s.release_flags();s.loaded()
        expect(row(page,100)).to_have_class(__import__('re').compile('unread'))
        assert s.flag_calls[0][1]['folder']==['INBOX']
        print('PASS pending flag write retains captured folder across navigation');page.close()

        page,s=start();page.set_viewport_size({'width':390,'height':844});row(page,100).click()
        expect(read_button(page,'Als ungelesen markieren')).to_be_visible()
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        page.screenshot(path='C:/tmp/gaertner-mail-read-mobile.png',full_page=True)
        read_button(page,'← Zur Mailliste').click();expect(row(page,100)).to_be_visible()
        print('PASS mobile reader controls, back navigation and no horizontal overflow');page.close()
        browser.close()

if __name__=='__main__':run()
