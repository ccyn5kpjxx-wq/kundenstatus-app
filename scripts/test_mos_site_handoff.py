"""Offline handoff validation, real browser-JS logic and isolated Flask routes."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import mos_site_handoff as handoff


NOW = datetime(2026, 10, 19, 10, tzinfo=timezone.utc)
CFG = {'fleet': {'i10': {'id': 1}, 'kona': {'id': 3}}, 'max_days': 5}
FIELDS = {'vehicle_id': '1', 'start': '2026-10-22T10:00', 'end': '2026-10-23T10:00',
          'name': 'Synthetic Test', 'email': 'test@example.invalid', 'planned_km': '700'}


class HandoffTests(unittest.TestCase):
    def test_prefill_maps_only_fixed_vehicles_and_never_accepts_site_price(self):
        for vehicle, slug in (('1', 'i10'), ('3', 'kona')):
            value = handoff.validated_prefill({**FIELDS, 'vehicle_id': vehicle, 'amount_cents': '1'}, CFG, NOW)
            self.assertEqual(value['vehicle'], slug)
            self.assertEqual(value['start'], '2026-10-22T10:00:00+02:00')
            self.assertEqual(value['planned_km'], 700)
            self.assertNotIn('amount_cents', value)
        for vehicle in ('2', 'c3', 'kona', '', 1):
            with self.subTest(vehicle=vehicle), self.assertRaises(ValueError):
                handoff.validated_prefill({**FIELDS, 'vehicle_id': vehicle}, CFG, NOW)
        with self.assertRaises(ValueError):
            handoff.validated_prefill(FIELDS, {**CFG, 'fleet': {'i10': {'id': 7}}}, NOW)

    def test_hours_weekdays_future_and_elapsed_five_day_cap(self):
        for values in ({'start': '2026-10-22T07:00'}, {'end': '2026-10-23T21:00'},
                       {'start': '2026-10-22T10:30'}, {'start': '2026-10-25T10:00'},
                       {'start': '2026-10-19T08:00'}, {'end': FIELDS['start']},
                       {'end': '2026-10-28T10:00'}, {'start': '2026-02-30T10:00'}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                handoff.validated_prefill({**FIELDS, **values}, CFG, NOW)
        # DST ends within this interval: five calendar days are 121 actual hours.
        with self.assertRaises(ValueError):
            handoff.validated_prefill({**FIELDS, 'end': '2026-10-27T10:00'}, CFG, NOW)
        accepted = handoff.validated_prefill({**FIELDS, 'end': '2026-10-27T09:00'}, CFG, NOW)
        self.assertEqual(accepted['end'], '2026-10-27T09:00:00+01:00')

    def test_contact_and_distance_caps(self):
        for values in ({'name': 'x' * 151}, {'name': 'Test\nInjected'}, {'email': 'x' * 255},
                       {'email': 'a@example.invalid,b@example.invalid'}, {'email': 'Name <a@example.invalid>'},
                       {'planned_km': '-1'}, {'planned_km': '1000001'}, {'planned_km': {}}, {'name': []}):
            with self.subTest(field=next(iter(values))), self.assertRaises(ValueError):
                handoff.validated_prefill({**FIELDS, **values}, CFG, NOW)
        self.assertIsNone(handoff.validated_prefill({**FIELDS, 'planned_km': ''}, CFG, NOW)['planned_km'])

    def test_nonce_is_session_bound_expiring_one_use_and_contains_no_contact_data(self):
        state = {}
        nonce = handoff.issue_nonce(state, 100)
        self.assertNotIn('name', json.dumps(state))
        with self.assertRaises(ValueError):
            handoff.consume_nonce({}, nonce, 101)
        handoff.consume_nonce(state, nonce, 101)
        with self.assertRaises(ValueError):
            handoff.consume_nonce(state, nonce, 102)
        expired = handoff.issue_nonce(state, 100)
        with self.assertRaises(ValueError):
            handoff.consume_nonce(state, expired, 401)
        for tick in range(10):
            handoff.issue_nonce(state, 500 + tick)
        self.assertEqual(len(state[handoff.SESSION_KEY]), 4)

    def test_rendered_message_handler_rejects_wrong_origin_source_nonce_and_duplicate(self):
        node = shutil.which('node')
        self.assertIsNotNone(node, 'Node is required to execute the handoff JS security test')
        env = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=True)
        page = env.get_template('mos_public/site_start.html').render(
            nonce='synthetic-nonce', site_origins=handoff.SITE_ORIGINS, csrf_token=lambda: 'synthetic-csrf',
            url_for=lambda endpoint: '/' + endpoint, live=True, booking_config={}, config={})
        script = re.search(r'<script>(.*?)</script>', page, re.S)[1]
        harness = r'''
const vm=require('node:vm'); const assert=require('node:assert/strict');
function setup(hasOpener=true) {
  const sent=[],forms=[],listeners={}; const status={textContent:''}; const timeouts=[];
  const source={postMessage:(data,origin)=>sent.push({data,origin})};
  const document={getElementById:()=>status,body:{appendChild:form=>forms.push(form)},
    createElement:tag=>tag==='form'?{children:[],appendChild(input){this.children.push(input)},submit(){this.submitted=true}}:{}};
  const window={opener:hasOpener?source:null,addEventListener:(kind,fn)=>listeners[kind]=fn,
    removeEventListener:kind=>delete listeners[kind]};
  const context={window,document,setInterval:()=>1,clearInterval:()=>{},
    setTimeout:fn=>{timeouts.push(fn);return 2},clearTimeout:()=>{}};
  vm.runInNewContext(SCRIPT,context);
  return {sent,forms,listeners,status,source,window,timeouts};
}
const s=setup(); assert.equal(s.sent.length,3);
assert.deepEqual(s.sent.map(x=>x.origin),ORIGINS);
assert(s.sent.every(x=>x.data.type==='mos-booking-ready'&&x.data.version===1&&x.data.nonce==='synthetic-nonce'));
const data={type:'mos-booking-data',version:1,nonce:'synthetic-nonce',payload:PAYLOAD};
const emit=updates=>s.listeners.message?.({source:s.source,origin:ORIGINS[0],data,...updates});
emit({origin:ORIGINS[0]+'.evil.invalid'}); emit({source:{}}); emit({data:{...data,nonce:'wrong'}});
emit({data:{...data,version:2}}); emit({data:{...data,payload:{...PAYLOAD,name:9}}});
const cyclic={...PAYLOAD}; cyclic.self=cyclic; emit({data:{...data,payload:cyclic}});
assert.equal(s.forms.length,0);
emit({origin:ORIGINS[1],data:{...data,payload:{...PAYLOAD,amount_cents:'1'}}});
assert.equal(s.forms.length,1); assert.equal(s.forms[0].method,'post'); assert.equal(s.forms[0].submitted,true);
const fields=Object.fromEntries(s.forms[0].children.map(x=>[x.name,x.value]));
assert.equal(fields.csrf_token,'synthetic-csrf'); assert.equal(fields.handoff_nonce,'synthetic-nonce');
assert.equal(fields.name,PAYLOAD.name); assert.equal(fields.amount_cents,undefined);
assert.equal(s.window.opener,null); assert.equal(s.listeners.message,undefined); emit({});
assert.equal(s.forms.length,1);
const closed=setup(false); assert.equal(closed.sent.length,0); assert.match(closed.status.textContent,/nicht übernommen/);
const expired=setup(); expired.timeouts[0](); assert.equal(expired.listeners.message,undefined);
assert.match(expired.status.textContent,/zu lange/); console.log('message security passed');
'''
        program = ('const SCRIPT=' + json.dumps(script) + ';const ORIGINS=' + json.dumps(handoff.SITE_ORIGINS)
                   + ';const PAYLOAD=' + json.dumps(FIELDS) + ';\n' + harness)
        result = subprocess.run([node, '-e', program], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('message security passed', result.stdout)

    def test_actual_flask_routes_preserve_csrf_pricing_and_no_financial_objects(self):
        # A child process keeps the existing app test builder and temp inventory isolated.
        program = r'''
from pathlib import Path
import html,json,re,socket,sys,tempfile
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
from unittest.mock import patch
def deny(*a,**k):raise AssertionError('network forbidden')
patch('socket.socket.connect',deny).start();patch('socket.socket.connect_ex',deny).start()
patch('socket.create_connection',deny).start()
from scripts.run_mos_public_test import build_test_app
with tempfile.TemporaryDirectory(prefix='mos-handoff-test-') as temp:
 p=build_test_app(temp,origin='http://localhost');app=p.app;app.config['TESTING']=True
 cfg=app.config['MOS_PUBLIC_BOOKING'];cfg['max_days']=5
 db=p.get_db();db.execute('DELETE FROM mietfahrzeuge')
 for vid,name in ((1,'TEST i10'),(3,'TEST KONA')):
  db.execute('INSERT INTO mietfahrzeuge (id,kennzeichen,bezeichnung,erstellt_am,geaendert_am) VALUES (?,?,?,?,?)',(vid,'TEST',name,p.now_str(),p.now_str()))
 db.commit();db.close();cfg['fleet']['i10']['id']=1;cfg['fleet']['kona']['id']=3
 start=(datetime.now(ZoneInfo('Europe/Berlin'))+timedelta(days=1)).replace(hour=10,minute=0,second=0,microsecond=0)
 while start.weekday()>3:start+=timedelta(days=1)
 end=start+timedelta(days=1);cfg['slots']=[start.isoformat(),end.isoformat()]
 client=app.test_client()
 with client.session_transaction() as state:state['admin']=True;state['sentinel']='existing-session'
 def entry(c=client):
  response=c.get('/mietwagen-test/start');assert response.status_code==200
  assert response.headers['Cache-Control']=='no-store'
  nonce=json.loads(re.search(r'const nonce = (.*?);',response.get_data(as_text=True))[1])
  with c.session_transaction() as state:csrf=state['csrf_token']
  return nonce,csrf
 nonce,csrf=entry()
 fields={'handoff_nonce':nonce,'vehicle_id':'1','start':start.strftime('%Y-%m-%dT%H:%M'),
 'end':end.strftime('%Y-%m-%dT%H:%M'),'name':'<Synthetic & Test>','email':'test@example.invalid',
 'planned_km':'700','amount_cents':'1'}
 # No special exemption: a valid handoff still fails without the ordinary CSRF.
 assert client.post('/mietwagen-test/quote',data=fields).status_code==400
 response=client.post('/mietwagen-test/quote',data={**fields,'csrf_token':csrf})
 assert response.status_code==200,response.get_data(as_text=True)
 page=response.get_data(as_text=True)
 assert 'value="&lt;Synthetic &amp; Test&gt;"' in page and 'value="test@example.invalid"' in page
 assert '700 km' in page and '39,00' in page
 token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',page)[1])
 from itsdangerous import URLSafeTimedSerializer
 quote=URLSafeTimedSerializer(app.secret_key,salt='mos-public-quote-v1').loads(token)['quote']
 assert quote['amount_cents']==3900 and quote['rental_cents']==3900
 assert quote['included_km']==150 and quote['extra_km_cents']==25
 assert 'planned_km' not in quote
 with client.session_transaction() as state:assert state['admin'] is True and state['sentinel']=='existing-session'
 invalid=client.post('/mietwagen-test/quote',data={**fields,'csrf_token':csrf})
 assert invalid.status_code==400 and 'name="quote_token"' not in invalid.get_data(as_text=True)
 # The nonce cannot be transferred to a different valid-CSRF session.
 nonce,csrf=entry();other=app.test_client();_,othercsrf=entry(other)
 assert other.post('/mietwagen-test/quote',data={**fields,'handoff_nonce':nonce,'csrf_token':othercsrf}).status_code==400
 # Server-side availability remains authoritative after the Site's selection.
 db=p.get_db();db.execute('UPDATE miet_checkout_slots SET active=0 WHERE slot=?',(start.isoformat(),));db.commit();db.close()
 invalid=client.post('/mietwagen-test/quote',data={**fields,'handoff_nonce':nonce,'csrf_token':csrf})
 assert invalid.status_code==400 and 'name="quote_token"' not in invalid.get_data(as_text=True)
 db=p.get_db()
 for table in ('miet_checkout_holds','mietvorgaenge','miet_checkout_deposit_auths','miet_checkout_contracts'):
  assert db.execute('SELECT COUNT(*) AS n FROM '+table).fetchone()['n']==0,table
 db.close()
 cfg['enabled']=False
 assert client.get('/mietwagen-test/start').status_code==404
 assert client.post('/mietwagen-test/quote',data={**fields,'csrf_token':csrf}).status_code==404
 print('isolated real routes passed')
'''
        result = subprocess.run([sys.executable, '-c', program], cwd=ROOT, capture_output=True,
                                text=True, encoding='utf-8', errors='replace', timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout[-1500:])
        self.assertIn('isolated real routes passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
