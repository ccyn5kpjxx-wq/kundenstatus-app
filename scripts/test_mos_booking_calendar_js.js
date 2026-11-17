/* Execute the shipped calendar script against browser-like form controls. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'static', 'mos_booking_calendar.js'), 'utf8');

class Control {
  constructor(type, value = '') {
    this.type = type; this.raw = value; this.options = []; this.listeners = {};
    this.validationMessage = ''; this.textContent = ''; this.className = '';
    this.min = ''; this.max = ''; this.required = true;
  }
  get value() { return this.raw; }
  set value(value) {
    this.raw = this.type === 'select' && !this.options.some(option => option.value === value) ? '' : value;
  }
  append(option) {
    this.options.push(option);
    if (this.options.length === 1) this.raw = option.value;
  }
  replaceChildren() { this.options = []; this.raw = ''; }
  setCustomValidity(message) { this.validationMessage = message; }
  addEventListener(type, listener) { this.listeners[type] = listener; }
  checkValidity() {
    return !this.validationMessage && (!this.required || Boolean(this.value)) &&
      (!this.min || this.value >= this.min) && (!this.max || this.value <= this.max);
  }
}

function regularSlots(first, last) {
  const slots = [];
  const day = new Date(first + 'T12:00:00Z');
  const end = new Date(last + 'T12:00:00Z');
  const offsetFormat = new Intl.DateTimeFormat('en-GB', {timeZone: 'Europe/Berlin', timeZoneName: 'shortOffset'});
  for (; day <= end; day.setUTCDate(day.getUTCDate() + 1)) {
    if (day.getUTCDay() === 0) continue;
    const offset = offsetFormat.formatToParts(day).find(part => part.type === 'timeZoneName').value;
    const hours = Number(offset.replace('GMT', ''));
    for (let hour = 8; hour <= 20; hour += 1) {
      slots.push(day.toISOString().slice(0, 10) + 'T' + String(hour).padStart(2, '0') + ':00:00+' + String(hours).padStart(2, '0') + ':00');
    }
  }
  return slots;
}

function calendar(slots, values, maxDays = 5) {
  const controls = {};
  const ids = {'vehicle': 'vehicle', 'start_date': 'start-date', 'start_time': 'start-time', 'end_date': 'end-date', 'end_time': 'end-time'};
  for (const [key, id] of Object.entries(ids)) {
    const control = controls[id] = new Control(key.endsWith('_time') || key === 'vehicle' ? 'select' : 'date', values[key] || '');
    if (key === 'vehicle') control.options = [{value: 'kona'}, {value: 'i10'}];
    else if (key.endsWith('_time')) {
      const day = values[key.replace('_time', '_date')];
      control.options = slots.filter(slot => slot.slice(0, 10) === day).map(slot => ({value: slot.slice(11, 16)}));
      // The server explicitly preserves a submitted but unavailable time.
      if (!control.options.some(option => option.value === control.raw)) control.options.push({value: control.raw});
    }
  }
  controls['booking-slots'] = {textContent: JSON.stringify(slots)};
  controls['date-message'] = new Control('message');
  controls['date-message'].required = false;
  const form = controls['booking-dates'] = new Control('form');
  form.required = false; form.dataset = {maxDays: String(maxDays)}; form.reports = 0;
  form.checkValidity = () => Object.values(ids).every(id => controls[id].checkValidity());
  form.reportValidity = () => { form.reports += 1; return form.checkValidity(); };
  const windowListeners = {};
  const document = {getElementById: id => controls[id], createElement: () => ({value: '', textContent: ''})};
  vm.runInNewContext(source, {document, window: {addEventListener: (type, listener) => { windowListeners[type] = listener; }}, Date, JSON, Map});
  return {
    controls, form,
    change(key, value) { const control = controls[ids[key]]; control.value = value; control.listeners.change(); },
    values() { return Object.fromEntries(Object.entries(ids).map(([key, id]) => [key, controls[id].value])); },
    message() { return controls['date-message'].textContent; },
    submit() { const event = {prevented: false, preventDefault() { this.prevented = true; }}; form.listeners.submit(event); return event; },
    pageshow() { windowListeners.pageshow(); }
  };
}

const tests = [];
function test(name, callback) { tests.push([name, callback]); }
const selection = (start_date, start_time, end_date, end_time) => ({vehicle: 'i10', start_date, start_time, end_date, end_time});

test('ten-day server error preserves all five choices and names chosen duration and limit', () => {
  const chosen = selection('2026-11-17', '14:00', '2026-11-27', '14:00');
  const page = calendar(regularSlots('2026-11-17', '2026-12-01'), chosen);
  assert.deepEqual(page.values(), chosen);
  assert.match(page.message(), /10 Miettage.*höchstens 5 Miettage/);
  assert.equal(page.submit().prevented, true);
  assert.equal(page.form.reports, 1);
  assert.equal(page.controls['end-date'].max, '2026-11-21');
});

test('five days exactly allowed; one extra hour blocked immediately on return-time change', () => {
  const page = calendar(regularSlots('2026-11-23', '2026-12-01'), selection('2026-11-23', '14:00', '2026-11-28', '14:00'));
  assert.equal(page.submit().prevented, false);
  page.change('end_time', '15:00');
  assert.equal(page.values().end_time, '15:00');
  assert.match(page.message(), /6 Miettage.*höchstens 5 Miettage/);
  assert.equal(page.submit().prevented, true);
  page.change('end_time', '14:00');
  assert.equal(page.submit().prevented, false);
});

test('fall-back uses elapsed time: five calendar days plus DST hour is six rental days', () => {
  const page = calendar(regularSlots('2027-10-27', '2027-11-03'), selection('2027-10-27', '14:00', '2027-11-01', '14:00'));
  assert.match(page.message(), /6 Miettage/);
  assert.equal(page.submit().prevented, true);
  page.change('end_time', '13:00');
  assert.match(page.message(), /Gewählte Mietdauer: 5 Miettage/);
  assert.equal(page.submit().prevented, false);
});

test('spring-forward uses elapsed time: five calendar days and one wall-clock hour is 120 hours', () => {
  const page = calendar(regularSlots('2027-03-24', '2027-03-31'), selection('2027-03-24', '14:00', '2027-03-29', '15:00'));
  assert.match(page.message(), /Gewählte Mietdauer: 5 Miettage/);
  assert.equal(page.submit().prevented, false);
  page.change('end_time', '16:00');
  assert.match(page.message(), /6 Miettage/);
  assert.equal(page.submit().prevented, true);
});

test('Saturday evening to Monday morning is valid; Sunday return stays selected and blocked', () => {
  const page = calendar(regularSlots('2026-11-21', '2026-11-28'), selection('2026-11-21', '20:00', '2026-11-23', '08:00'));
  assert.match(page.message(), /Gewählte Mietdauer: 2 Miettage/);
  assert.equal(page.submit().prevented, false);
  page.change('end_date', '2026-11-22');
  assert.equal(page.values().end_date, '2026-11-22');
  assert.equal(page.values().end_time, '08:00');
  assert.match(page.message(), /Rückgabedatum.*kein Übergabetermin/);
  assert.equal(page.submit().prevented, true);
});

test('moving pickup ten months forward from 20:00 preserves hour and announces one-day return', () => {
  const slots = regularSlots('2026-11-17', '2027-11-17').filter(slot => slot.slice(0, 10) !== '2026-11-17' || slot.slice(11, 16) === '20:00');
  const page = calendar(slots, selection('2026-11-17', '20:00', '2026-11-18', '20:00'));
  page.change('start_date', '2027-09-20');
  assert.deepEqual(page.values(), selection('2027-09-20', '20:00', '2027-09-21', '20:00'));
  assert.match(page.message(), /Rückgabe wurde.*angepasst.*Bitte prüfen/);
  assert.equal(page.submit().prevented, false);
});

test('Back/pageshow retains a restored over-limit choice without automatic correction', () => {
  const chosen = selection('2026-11-17', '14:00', '2026-11-27', '14:00');
  const page = calendar(regularSlots('2026-11-17', '2026-12-01'), chosen);
  page.pageshow();
  assert.deepEqual(page.values(), chosen);
  assert.match(page.message(), /10 Miettage/);
});

test('maximum duration comes from configuration, including seven-day configuration', () => {
  const page = calendar(regularSlots('2026-11-23', '2026-12-03'), selection('2026-11-23', '14:00', '2026-11-30', '14:00'), 7);
  assert.match(page.message(), /Gewählte Mietdauer: 7 Miettage; maximal 7/);
  assert.equal(page.submit().prevented, false);
  page.change('end_time', '15:00');
  assert.match(page.message(), /8 Miettage.*höchstens 7 Miettage/);
  assert.equal(page.submit().prevented, true);
});

test('a configured 28-day limit permits the reported ten-day choice and explains it before submit', () => {
  const chosen = selection('2026-11-17', '14:00', '2026-11-27', '14:00');
  const page = calendar(regularSlots('2026-11-17', '2026-12-22'), chosen, 28);
  assert.deepEqual(page.values(), chosen);
  assert.match(page.message(), /Gewählte Mietdauer: 10 Miettage; maximal 28/);
  assert.equal(page.controls['date-message'].className, 'small');
  assert.equal(page.submit().prevented, false);
});

test('configured 28-day duration updates on selection and still blocks its 29th rental day', () => {
  const page = calendar(regularSlots('2026-11-23', '2026-12-22'), selection('2026-11-23', '14:00', '2026-11-24', '14:00'), 28);
  page.change('end_date', '2026-12-21');
  assert.match(page.message(), /Gewählte Mietdauer: 28 Miettage; maximal 28/);
  assert.equal(page.submit().prevented, false);
  page.change('end_time', '15:00');
  assert.match(page.message(), /29 Miettage.*höchstens 28 Miettage/);
  assert.equal(page.values().end_time, '15:00');
  assert.equal(page.submit().prevented, true);
});

test('last available pickup hour without any later return is blocked clearly', () => {
  const page = calendar(['2027-11-17T20:00:00+01:00'], selection('2027-11-17', '20:00', '2027-11-17', ''));
  assert.match(page.message(), /kein späterer Rückgabetermin verfügbar/);
  assert.equal(page.values().end_time, '');
  assert.equal(page.submit().prevented, true);
});

test('return change never silently replaces an equal or earlier selection', () => {
  const page = calendar(regularSlots('2026-11-17', '2026-11-24'), selection('2026-11-17', '14:00', '2026-11-18', '14:00'));
  page.change('end_date', '2026-11-17');
  assert.deepEqual(page.values(), selection('2026-11-17', '14:00', '2026-11-17', '14:00'));
  assert.match(page.message(), /Rückgabe muss nach der Abholung/);
  assert.equal(page.submit().prevented, true);
});

test('pickup change may repair an invalid return but always announces the change', () => {
  const page = calendar(regularSlots('2026-11-17', '2026-11-24'), selection('2026-11-17', '10:00', '2026-11-17', '11:00'));
  page.change('start_time', '11:00');
  assert.deepEqual(page.values(), selection('2026-11-17', '11:00', '2026-11-18', '11:00'));
  assert.match(page.message(), /angepasst.*Bitte prüfen/);
});

test('closed and malformed submitted times remain visible and cannot be submitted', () => {
  const slots = regularSlots('2026-11-17', '2026-11-24').filter(slot => !slot.startsWith('2026-11-18T10:00'));
  const chosen = selection('2026-11-17', '10:00', '2026-11-18', '10:00');
  const page = calendar(slots, chosen);
  assert.deepEqual(page.values(), chosen);
  assert.match(page.message(), /verfügbare Rückgabezeit/);
  assert.equal(page.submit().prevented, true);
  const invalid = calendar(slots, selection('2026-11-17', '10:30', '2026-11-18', '11:00'));
  assert.equal(invalid.values().start_time, '10:30');
  assert.match(invalid.message(), /verfügbare Abholzeit/);
});

test('submit revalidates current fields rather than keeping a stale custom error', () => {
  const page = calendar(regularSlots('2026-11-23', '2026-12-01'), selection('2026-11-23', '14:00', '2026-11-28', '15:00'));
  assert.equal(page.submit().prevented, true);
  page.controls['end-time'].value = '14:00';
  assert.equal(page.form.noValidate, true);
  assert.equal(page.submit().prevented, false);
});

let failures = 0;
for (const [name, callback] of tests) {
  try { callback(); console.log('PASS ' + name); }
  catch (error) { failures += 1; console.error('FAIL ' + name); console.error(error.stack); }
}
console.log(`${tests.length - failures}/${tests.length} calendar tests passed`);
process.exitCode = failures ? 1 : 0;
