/* Synthetic DOM verifies the actual modal text sink, with no network client. */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
const ID = 'parts:' + 'a'.repeat(24);
class Node {
  constructor(tag = 'div') { this.tagName = tag.toUpperCase(); this.children = []; this.listeners = {}; this.dataset = {}; this.open = false; this.textContent = ''; this.value = ''; this.isConnected = true; this.classList = {toggle(){}}; this.elements = {namedItem(){return null;}}; }
  set innerHTML(_) { throw Error('HTML sink forbidden'); }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = [...nodes]; }
  addEventListener(key, fn) { this.listeners[key] = fn; }
  setAttribute(key, value) { this[key] = value; }
  showModal() { this.open = true; }
  close() { this.open = false; }
}
const walk = node => [node, ...node.children.flatMap(child => typeof child === 'object' ? walk(child) : [])];
async function render(notes, department = 'parts') {
  const nodes = Object.fromEntries(['close-product', 'product-dialog', 'product-detail', 'shop-announcement'].map(key => [key, new Node()]));
  const body = new Node('body'); body.dataset = {api:'/shop/api/v2', page:'ersatzteile'};
  const product = {id:ID,category:'accessory',department,name:'SYNTHETIC PART',size:'',images:[],
    description:'SYNTHETIC DESCRIPTION',specifications:{Ausführung:'SYNTHETIC SPEC'},purchase_status:'inquiry_only',
    application_notes:notes,fitment:{status:'not_confirmed',vehicles:[],reports:[]}};
  const calls = [], window = {document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key] || null,
    querySelector:()=>({content:''}),querySelectorAll:()=>[]},localStorage:{getItem:()=>null},
    location:{pathname:'/shop/ersatzteile/'},setTimeout(){},clearTimeout(){},setInterval(){},
    fetch:async (url, options) => { calls.push({url, options}); assert.match(url, /\/products\/parts%3A[a-f0-9]{24}\?quantity=1$/); return {ok:true,text:async()=>JSON.stringify({item:product})}; }};
  vm.runInNewContext(source, {window, URL, URLSearchParams, Intl, Date, console});
  await window.ShopStorefront.openProduct(ID);
  return {all:walk(nodes['product-detail']), calls};
}
(async () => {
  const literal = '<img src=x onerror="PRIVATE_CANARY"> Nur Reparatur, kein Umbau.';
  const visible = await render([literal, 'Montage durch Fachpersonal.']);
  const section = visible.all.find(node => node.className === 'shop-application-notes');
  assert.ok(section, 'notes appear in the opened product modal');
  assert.equal(section.children[0].textContent, 'Hinweise zu diesem Ersatzteil');
  assert.deepEqual(section.children[1].children.map(node => node.textContent), [literal, 'Montage durch Fachpersonal.']);
  assert.equal(walk(section).some(node => ['IMG','SCRIPT'].includes(node.tagName)), false, 'text never creates HTML elements');
  const texts = visible.all.map(node => node.textContent);
  assert.ok(texts.indexOf('SYNTHETIC DESCRIPTION') < texts.indexOf('Hinweise zu diesem Ersatzteil'));
  assert.ok(texts.indexOf('SYNTHETIC SPEC') < texts.indexOf('Hinweise zu diesem Ersatzteil'));
  for (const notes of [undefined, [], ['x'.repeat(2001)], Array(13).fill('x'), [true]]) {
    const value = await render(notes);
    assert.equal(value.all.some(node => node.className === 'shop-application-notes'), false);
  }
  assert.equal((await render(['Not a vehicle-parts notice'], 'accessories')).all.some(node => node.className === 'shop-application-notes'), false);
  assert.ok(visible.calls.every(call => !call.options.method || call.options.method === 'GET'));
  console.log('Parts application-notes modal checks passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
