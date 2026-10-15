"""Compact cockpit regressions. Synthetic templates and a mocked browser only."""
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import shutil
import subprocess
import unittest

from test_cockpit_planning import template_environment


class Page(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.tags = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))


def context():
    vehicle = {"id": 71, "fahrzeug": "Testwagen", "kennzeichen": "DEMO 71", "annahme_uhrzeit": "08:30", "abhol_uhrzeit": "16:15"}
    return {
        "tages_gruss": "Guten Tag", "current_date_label": "Donnerstag, 15.10.2026",
        "current_datetime_iso": "2026-10-15T12:00:00", "current_time_label": "12:00:00",
        "cockpit": {"heute_bringen": [vehicle], "heute_starten": [], "heute_fertig": [], "heute_abholen": [vehicle], "quelle_mix": []},
        "aktionsuebersicht": {"total": 1, "waiting": 2, "groups": {"antworten": [{"name": "Testperson", "vehicle": "Testwagen", "detail": "Neue Nachricht", "title": "Antwort prüfen", "url": "/admin/auftrag/71"}]}},
        "start_inbox": {"fahrzeuge_count": 1, "anfragen_count": 2, "aufgaben_count": 3, "items": []},
        "mietwagen_heute": {"heute_start": [{"bezeichnung": "Ersatzwagen", "kennzeichen": "DEMO 10", "kunde_name": "Testkunde"}], "heute_rueckgabe": []},
        "erinnerungen": [],
    }


class CompactCockpitTest(unittest.TestCase):
    def setUp(self):
        self.env = template_environment()

    def render(self, data=None):
        return self.env.get_template("cockpit.html").render(**(data or context()))

    def test_secondary_sections_are_collapsed_but_counts_and_content_remain(self):
        html = self.render()
        tags = Page(html).tags
        for flag in ("data-cockpit-eingang", "data-cockpit-mietwagen", "data-cockpit-notizen", "data-cockpit-kalender"):
            element = next(attrs for tag, attrs in tags if tag == "details" and flag in attrs)
            self.assertNotIn("open", element, flag)
        groups = [attrs for tag, attrs in tags if tag == "details" and "data-action-group" in attrs]
        self.assertEqual(len(groups), 4)
        self.assertTrue(all("open" not in attrs for attrs in groups))
        for text in ("Neue Nachricht", "Ersatzwagen", "08:30 Uhr", "16:15 Uhr", "2 Angebote warten", "1 offene Aktion"):
            self.assertIn(text, html)
        self.assertNotIn('class="cockpit-rail"', html)
        self.assertNotIn('class="page-card module-tile"', html)

    def test_warning_links_remain_outside_collapsed_sections(self):
        self.env.globals["mahnungen_faellig_count"] = lambda: 2
        data = context()
        data["cockpit"].update({"offene_reklamationen": [{"id": 8}], "offene_verzoegerungen": [{"auftrag_id": 72, "kennzeichen": "DEMO 72"}], "ueberfaellig": [{"id": 73, "kennzeichen": "DEMO 73"}]})
        html = self.render(data)
        alert_start = html.index('aria-label="Dringende Hinweise"')
        alert_end = html.index('aria-label="Fahrzeuge heute"')
        alerts = html[alert_start:alert_end]
        for text in ("2 Mahnungen fällig", "1 offene Reklamation", "1 · Verzögerung(en) prüfen", "1 · Fertigstellung überfällig", "/admin_mahnungen", "/dashboard#reklamationen", "auftrag_id=72", "auftrag_id=73"):
            self.assertIn(text, alerts)

    def test_existing_inbox_actions_keep_csrf_and_return_target(self):
        data = context()
        data["start_inbox"]["items"] = [{"item_key": "admin-email-12", "typ": "E-Mail", "titel": "Testnachricht", "erstellt_am": "15.10.2026", "autohaus_name": "Testfirma", "fahrzeug": "Testwagen", "kennzeichen": "DEMO 71", "nachricht": "Bitte prüfen", "whatsapp_auftrag_id": 71}]
        data["erinnerungen"] = [{"id": 7, "text": "Rückruf prüfen", "erstellt_am": "15.10.2026"}]
        tags = Page(self.render(data)).tags
        forms = [attrs for tag, attrs in tags if tag == "form"]
        self.assertEqual(len(forms), 4)
        self.assertTrue(all(attrs["method"] == "POST" for attrs in forms))
        inputs = [attrs for tag, attrs in tags if tag == "input"]
        self.assertEqual(sum(a.get("name") == "csrf_token" and a.get("value") == "synthetic-token" for a in inputs), 4)
        self.assertEqual(sum(a.get("name") == "next" and a.get("value") == "/betriebs_cockpit" for a in inputs), 4)

    def test_customer_text_is_escaped_and_search_is_read_only(self):
        data = context()
        data["aktionsuebersicht"]["groups"]["antworten"][0]["name"] = '<script>alert("x")</script>'
        html = self.render(data)
        self.assertNotIn('<script>alert("x")</script>', html)
        self.assertIn("&lt;script&gt;", html)
        page = Page(html)
        search = next(attrs for tag, attrs in page.tags if "data-cockpit-search" in attrs)
        self.assertEqual(search["data-search-url"], "/admin_cockpit_auftraege_suche")
        script = html.split("// Cockpit lookup only reads", 1)[1]
        self.assertNotIn("innerHTML", script)
        self.assertNotIn("method: 'POST'", script)

    def test_empty_state_does_not_claim_all_work_is_finished(self):
        data = context()
        data["aktionsuebersicht"] = {"groups": {}, "total": 0, "waiting": 0}
        data["start_inbox"] = {"items": []}
        html = self.render(data)
        self.assertIn("Keine neuen Eingänge in dieser Übersicht.", html)
        self.assertNotIn("Alles erledigt", html)
        self.assertIn("Fahrzeuge <strong>0</strong>", html)
        self.assertIn("Raus <strong>1</strong>", html)

    @unittest.skipUnless(shutil.which("node"), "Node.js needed for browser interaction test")
    def test_search_rejects_stale_responses_and_external_links(self):
        source = (Path(__file__).resolve().parents[1] / "templates/cockpit.html").read_text(encoding="utf-8")
        script = source.split("// Cockpit lookup only reads the existing admin search endpoint.", 1)[1].split("</script>", 1)[0]
        runner = r'''
const assert = require('node:assert/strict');
const vm = require('node:vm');
const timers = new Map(); let sequence=0; const pending=[]; let requests=0;
function node(kind) { return {kind, childNodes:[], textContent:'', hidden:false, handlers:{}, append(...items){for(const item of items) this.childNodes.push(...(item.kind==='fragment'?item.childNodes:[item]));}, replaceChildren(...items){this.childNodes=[];this.append(...items);}, addEventListener(name,fn){this.handlers[name]=fn;}}; }
const input=node('input'); input.value=''; const results=node('results'); results.hidden=true;
const root={dataset:{searchUrl:'/admin/cockpit/auftraege-suche'},querySelector:q=>q==='input'?input:results, contains:target=>target===input};
const doc={querySelector:()=>root,createElement:node,createDocumentFragment:()=>node('fragment'),addEventListener(){}};
vm.runInNewContext(SCRIPT,{document:doc,window:{location:{origin:'http://localhost:5071'}},URL,AbortController,
 setTimeout:fn=>{timers.set(++sequence,fn);return sequence;},clearTimeout:id=>timers.delete(id),
 fetch:(url,options)=>{requests++;assert.equal(options.method,undefined);return new Promise(resolve=>pending.push(resolve));}});
function type(value){input.value=value;input.handlers.input();}
function tick(){const entries=[...timers.values()];timers.clear();entries.forEach(fn=>fn());}
const flush=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
 type('A');tick();assert.equal(requests,0);assert.equal(results.hidden,true);
 type('AB');tick();type('CD');tick();assert.equal(requests,2);
 pending[1]({ok:true,json:async()=>({items:[{url:'/admin/auftrag/72',fahrzeug:'New result',kennzeichen:'TEST',meta:'<b>Text</b>',status:'Offen'},{url:'https://external.invalid/admin/auftrag/9',fahrzeug:'Unsafe'}]})});await flush();
 assert.equal(results.childNodes.length,1);assert.equal(results.childNodes[0].href,'/admin/auftrag/72');assert.equal(results.childNodes[0].childNodes[1].textContent,'<b>Text</b> · Offen');
 pending[0]({ok:true,json:async()=>({items:[{url:'/admin/auftrag/71',fahrzeug:'Old result'}]})});await flush();
 assert.equal(results.childNodes[0].href,'/admin/auftrag/72');
 type('EF');input.handlers.keydown({key:'Escape'});tick();assert.equal(requests,2);assert.equal(results.hidden,true);
 type('GH');tick();pending[2]({ok:false});await flush();assert.match(results.childNodes[0].textContent,/nicht verfügbar/);
 type('');tick();assert.equal(results.hidden,true);assert.equal(results.childNodes.length,0);
 console.log('PASS: search debounce, stale response, URL safety, text rendering, Escape and failure handling');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run([shutil.which("node"), "-"], input="const SCRIPT=" + json.dumps(script) + ";\n" + runner, text=True, encoding="utf-8", capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
