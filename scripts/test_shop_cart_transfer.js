// Offline browser messages and origin-separated stores, with no API or checkout adapter.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const script = fs.readFileSync(path.join(__dirname, '../static/shop_v2/shop.js'), 'utf8');
const shop = 'https://auto-lackierzentrum.de', portal = 'https://kundenstatus-app.onrender.com';
const cartPath = '/shop/warenkorb/', key = 'gaertner-shop-v2-selection';
const newToken='bbbbbbbb-bbbb-4bbb-abbb-bbbbbbbbbbbb';
const makeItem = (n, quantity=1) => ({id:'tyre:'+String(n).repeat(24), quantity, services:{}});
function tab(send, rows, search='', opener=null) {
  const origin = send ? portal : shop, peer = send ? shop : portal;
  const local = new Map([[key, JSON.stringify(rows)]]), session = new Map(), listeners = {}, nodes = {}, navigations = [];
  const node = id => nodes[id] ||= {hidden:true, addEventListener(name, listener){ this[name] = listener; }};
  node('official-cart').href = shop + cartPath;
  const storage = data => ({getItem:k=>data.get(k) ?? null, setItem:(k,v)=>data.set(k,v), removeItem:k=>data.delete(k)});
  const win = {opener, document:{body:{dataset:{transfer:send?'send':'receive', peerOrigin:peer, alias:send?'portal':''}}, getElementById:node},
    location:{origin, search, replace(url){navigations.push(url);}}, localStorage:storage(local), sessionStorage:storage(session),crypto:{randomUUID:()=>newToken},
    addEventListener:(type,fn)=>{listeners[type]=fn;},removeEventListener:(type,fn)=>{if(listeners[type]===fn)delete listeners[type];},
    open:(url)=>{navigations.push(url);return null;}};
  const start = () => vm.runInNewContext(script, {window:win, URL, URLSearchParams});
  const message = (source, from, data) => listeners.message?.({source,origin:from,data});
  return {win, local, session, nodes, navigations, start, message};
}
const link = 'set|alloy_wheel:'+ 'a'.repeat(24) + '|tyre:' + '1'.repeat(24);
const items = [{...makeItem(1,4), selection_pair:link},
  {id:'alloy_wheel:'+ 'a'.repeat(24), quantity:4,services:{},selection_pair:link}];
const receiver = {postMessage(data,origin){this.last={data,origin};}};
const opener = {postMessage(data,origin){this.last={data,origin};}};
const from = tab(true,items);
from.win.open = (url, target) => {assert.equal(url,shop+cartPath+'?uebernahme=portal');assert.equal(target,'_blank');return receiver;};
from.start(); assert.equal(from.nodes['transfer-start'].hidden,false);from.nodes['transfer-start'].click();
from.message(receiver,'https://untrusted.test','gaertner-shop-cart-ready');
from.message({},shop,'gaertner-shop-cart-ready');assert.equal(receiver.last,undefined);
from.message(receiver,shop,'gaertner-shop-cart-ready');
assert.deepEqual(JSON.parse(JSON.stringify(receiver.last)),{data:{type:'gaertner-shop-cart-items',rows:items,partial:false},origin:shop});
const dest = tab(false,[makeItem(2)],'',opener); dest.start();
assert.deepEqual(opener.last,{data:'gaertner-shop-cart-ready',origin:portal});
dest.message(opener,'https://untrusted.test',receiver.last.data);dest.message({},portal,receiver.last.data);
assert.deepEqual(JSON.parse(dest.local.get(key)),[makeItem(2)]);
dest.message(opener,portal,receiver.last.data);
assert.deepEqual(JSON.parse(dest.local.get(key)),[makeItem(2),...items]);
assert.deepEqual(dest.navigations,[shop+cartPath]); assert.equal(dest.win.opener,null);
from.message(receiver,shop,opener.last.data);assert.equal(from.nodes['transfer-start'].hidden,true);
assert.deepEqual(JSON.parse(from.local.get(key)),items);
for(const saved of [[...items],[makeItem(1,2)],Array.from({length:40},(_,n)=>({id:'tyre:'+n.toString(16).padStart(24,'0'),quantity:1,services:{}}))]){
  const retry=tab(false,saved,'',opener);retry.start();retry.message(opener,portal,receiver.last.data);
  assert.deepEqual(JSON.parse(retry.local.get(key)),saved);
  assert.equal(retry.navigations.length, saved[0].selection_pair ? 1 : 0);
}
const token='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
const selected=tab(true,[...items,makeItem(2)],'?auswahl='+token);
selected.session.set('gaertner-shop-v2-separate:'+token,JSON.stringify(items));
selected.start();selected.win.open=()=>receiver;selected.nodes['transfer-start'].click();
selected.message(receiver,shop,'gaertner-shop-cart-ready');assert.deepEqual(JSON.parse(JSON.stringify(receiver.last.data.rows)),items);
assert.equal(receiver.last.data.partial,true);
const partialDest=tab(false,[makeItem(3),items[0]],'',opener);
partialDest.start();partialDest.message(opener,portal,receiver.last.data);
assert.deepEqual(partialDest.navigations,[shop+cartPath+'?auswahl='+newToken]);
assert.deepEqual(JSON.parse(partialDest.session.get('gaertner-shop-v2-separate:'+newToken)),items);
assert.deepEqual(JSON.parse(partialDest.local.get(key)),[makeItem(3),...items]);
for (const failingStore of ['sessionStorage','localStorage']) {
  const failed=tab(false,[makeItem(3)],'',opener);
  failed.win[failingStore].setItem=()=>{throw new Error('Storage unavailable');};
  failed.start();failed.message(opener,portal,receiver.last.data);
  assert.equal(failed.navigations.length,0);
  assert.equal(failed.session.size,0);
  assert.deepEqual(JSON.parse(failed.local.get(key)),[makeItem(3)]);
}
const gone=tab(true,items,'?auswahl='+token);gone.start();assert.equal(gone.nodes['full-cart'].hidden,false);assert.equal(gone.navigations.length,0);
const empty=tab(true,[]);empty.start();assert.deepEqual(empty.navigations,[shop+cartPath]);
console.log('Cart origin transfer checks passed.');
