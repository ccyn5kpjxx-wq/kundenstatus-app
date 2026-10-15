// Exercise the actual browser script with a minimal DOM and synthetic file metadata.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function element() {
  const handlers = {};
  return {
    handlers, children: [], dataset: {},
    classList: { add() {}, remove() {} },
    addEventListener(type, fn) { handlers[type] = fn; },
    appendChild(child) { this.children.push(child); },
    replaceChildren() { this.children = []; },
    focus() {},
  };
}
const input = element(), preview = element(), form = element();
input.dataset = { maxFiles: '5', maxFileMb: '50', maxTotalMb: '100' };
input.files = [];
input.closest = () => form;
Object.defineProperty(input, 'value', { set(value) { if (value === '') input.files = []; } });
let created = 0, revoked = 0;
vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../static/homepage-anfrage-upload.js'), 'utf8'), {
  document: {
    getElementById: (id) => id === 'bilder' ? input : preview,
    createElement: element,
  },
  URL: { createObjectURL: () => `blob:${++created}`, revokeObjectURL: () => revoked++ },
});
const MB = 1024 * 1024;
function select(...sizes) {
  input.files = sizes.map((size) => ({ name: 'photo.jpg', type: 'image/jpeg', size }));
  input.handlers.change();
}
function blocked() {
  let prevented = false;
  form.handlers.submit({ preventDefault() { prevented = true; } });
  return prevented;
}
assert.equal(blocked(), false, 'No photos are optional');
select(20 * MB, 20 * MB);
assert.equal(blocked(), false, 'Phone photos above old limits work');
select(50 * MB);
assert.equal(blocked(), false, 'Exact per-file limit works');
select(...Array(5).fill(20 * MB));
assert.equal(blocked(), false, 'Five photos at total limit work');
select(50 * MB + 1);
assert.equal(blocked(), true, 'Oversized photo is visibly rejected');
const remove = preview.children.find((item) => item.className === 'photo-remove');
assert.equal(remove.type, 'button');
remove.handlers.click();
assert.equal(input.files.length, 0, 'Remove button clears selected files');
assert.equal(preview.dataset.invalid, '');
assert.equal(blocked(), false, 'Can send without photos after removing invalid selection');
select(50 * MB + 1);
select();
assert.equal(preview.dataset.invalid, '', 'Empty selection resets stale error');
assert.equal(blocked(), false);
select(40 * MB, 40 * MB, 21 * MB);
assert.equal(blocked(), true, 'Total limit is enforced');
select(...Array(6).fill(MB));
assert.equal(blocked(), true, 'Photo count is enforced');
select(MB);
assert.equal(blocked(), false, 'Valid replacement clears error');
input.files = [{ name: 'changed.jpg', size: 51 * MB, type: 'image/jpeg' }];
assert.equal(blocked(), true, 'Submit validates actual selection again');
select();
assert.equal(created, revoked, 'Preview URLs are released');
console.log('Homepage upload UI: all checks passed.');
