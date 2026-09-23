// Exercise the actual inline calculator without network, browser or customer data.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../static/mietwagen_vorschau/index.html'), 'utf8');
const start = html.indexOf('function preisBerechnen(){');
const end = html.indexOf('    if(quickFrom&&quickTo)', start);
assert(start > 0 && end > start, 'Calculator must exist in the shipped page');
const context = vm.createContext({
  carSelect: { selectedOptions: [{ dataset: { price: '59', priceLong: '49' } }] },
  quickFrom: { value: '' }, quickTo: { value: '' }, quickKm: { value: '' },
  quickTotal: {}, quickDuration: {}, quickIncluded: {}, quickKmCost: {},
  euro: value => new Intl.NumberFormat('de-DE', { style: 'currency', currency: 'EUR' }).format(value),
});
vm.runInContext(html.slice(start, end), context);
function calculate(from, to, km = '') {
  context.quickFrom.value = from;
  context.quickTo.value = to;
  context.quickKm.value = km;
  return vm.runInContext('preisBerechnen()', context);
}
assert.equal(calculate('', ''), null);
assert.match(context.quickTotal.textContent, /59,00.*für 1 Miettag/);
assert.equal(calculate('2026-09-22', '2026-09-22').total, 59);
assert.equal(calculate('2026-09-22', '2026-09-24').total, 118);
assert.equal(calculate('2026-09-22', '2026-09-25').total, 147);
const extra = calculate('2026-09-22', '2026-09-25', '501');
assert.equal(extra.includedKm, 450);
assert.equal(extra.extraKm, 51);
assert.equal(extra.total, 159.75);
assert.equal(calculate('2026-09-25', '2026-09-22'), null);
assert.match(context.quickKmCost.textContent, /Mehrkilometer: 0,25/);
assert.equal(calculate('2026-10-24', '2026-10-26').days, 2, 'Winter time must not charge an extra day');
assert.equal(calculate('2026-03-28', '2026-03-30').days, 2, 'Summer time must preserve calendar days');
assert.equal(calculate('2028-02-28', '2028-03-01').days, 2);
context.carSelect.selectedOptions[0].dataset = { price: '39', priceLong: '39' };
assert.equal(calculate('2026-09-22', '2026-09-25').total, 117);
console.log('PASS: rental estimates, discount boundary, extra kilometres, invalid dates, DST and leap day');
