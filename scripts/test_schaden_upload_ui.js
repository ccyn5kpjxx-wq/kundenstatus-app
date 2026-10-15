// Run the actual inline upload functions and submit handlers with synthetic files.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../templates/schadenmeldung_vorschau.html'), 'utf8');
const MB = 1024 * 1024;
function extract(name) {
  const match = source.match(new RegExp('      const ' + name + ' = [\\s\\S]*?\\n      };'));
  assert.ok(match, `Production function ${name} exists`);
  return match[0];
}
const input = { files: [], focus() {} };
Object.defineProperty(input, 'value', { set(value) { if (value === '') this.files = []; } });
const state = {
  maxFiles: 10, maxFileMb: 50, maxTotalMb: 100,
  uploadInput: input, uploadFiles: {}, uploadError: {}, removeFiles: {},
  panels: [4, 5].map(step => ({ dataset: { step: String(step) }, querySelectorAll: () => [] })),
  showToast: () => {}, refreshSummary: () => {}, liveSubmit: true,
  goToStep: step => { state.currentStep = step; }, showView: view => { state.view = view; },
  form: { addEventListener: (event, fn) => { state.formSubmit = fn; }, submit: () => { state.submitted = true; } },
  submitButton: { addEventListener: (event, fn) => { state.submitClick = fn; } },
  DataTransfer: class {
    constructor() { this.files = []; this.items = { add: file => this.files.push(file) }; }
  },
};
vm.createContext(state);
vm.runInContext('let selectedFiles = []; let uploadSelectionError = "";\n' +
  ['uploadProblems', 'renderFiles', 'clearUploadFiles', 'setUploadFiles', 'validateStep'].map(extract).join('\n') +
  '\nthis.api = {setUploadFiles, clearUploadFiles, validateStep, selected:()=>selectedFiles, error:()=>uploadSelectionError};', state);
for (const target of ['form', 'submitButton']) {
  const event = target === 'form' ? 'submit' : 'click';
  const pattern = new RegExp('      ' + target + '\\.addEventListener\\("' + event + '", \\(.*?[\\s\\S]*?\\n      \\}\\);');
  const handler = source.match(pattern);
  assert.ok(handler, `Actual ${target} handler exists`);
  vm.runInContext(handler[0], state);
}
function files(...sizes) {
  return sizes.map((size, index) => ({ name: `foto-${index}.jpg`, size }));
}
function select(list) { input.files = list; state.api.setUploadFiles(input.files); }
select(files(...Array(11).fill(MB)));
assert.equal(state.api.selected().length, 11, 'No selected photo is silently truncated');
assert.equal(input.files.length, 11, 'Native file list is not shortened');
assert.match(state.api.error(), /11.*10/, 'Actual excess count is visible');
assert.equal(state.uploadError.hidden, false, 'Persistent error shown');
assert.equal(state.api.validateStep(4), false, 'Cannot proceed with excess photos');
state.submitClick();
assert.equal(state.submitted, undefined, 'Final click cannot bypass invalid uploads');
assert.equal(state.currentStep, 4);
let prevented = false;
state.formSubmit({ preventDefault() { prevented = true; } });
assert.equal(prevented, true, 'Native submit/Enter also checks upload limits');
state.api.clearUploadFiles();
assert.equal(input.files.length, 0);
assert.equal(state.api.selected().length, 0);
assert.equal(state.api.error(), '');
assert.equal(state.api.validateStep(4), true, 'Can continue without files after clearing');
select(files(50 * MB));
assert.equal(state.api.validateStep(4), true, 'Exact per-file limit works');
select(files(...Array(10).fill(10 * MB)));
assert.equal(state.api.validateStep(4), true, 'Ten photos at total limit work');
select(files(50 * MB + 1));
assert.match(state.api.error(), /50 MB/);
assert.equal(state.api.validateStep(4), false);
select(files(40 * MB, 40 * MB, 21 * MB));
assert.match(state.api.error(), /100 MB/);
assert.equal(state.api.validateStep(4), false);
select([{ name: 'empty.png', size: 0 }]);
assert.match(state.api.error(), /leer/);
select([{ name: 'bad.exe', size: MB }]);
assert.equal(state.api.validateStep(4), false, 'Unsupported file type rejected');
select(files(20 * MB));
assert.equal(state.api.validateStep(4), true, 'Valid replacement clears error');
input.files = files(51 * MB);
assert.equal(state.api.validateStep(4), false, 'Actual transmitted file list is rechecked');
state.api.setUploadFiles(files(15 * MB, 16 * MB), true);
assert.equal(input.files.length, 2, 'Drag-drop native list matches preview');
assert.equal(state.api.selected().length, 2);
state.DataTransfer = undefined;
state.api.setUploadFiles(files(MB), true);
assert.equal(input.files.length, 0, 'Unsupported drag-drop cannot leave untransmittable preview');
assert.equal(state.api.selected().length, 0);
assert.equal(state.api.validateStep(4), false, 'Unsupported drag-drop is visibly rejected');
state.api.clearUploadFiles();
state.liveSubmit = false;
state.submitClick();
assert.equal(state.submitted, undefined, 'Design preview still never submits');
assert.equal(state.view, 'success');
state.liveSubmit = true;
state.submitClick();
assert.equal(state.submitted, true, 'Valid no-file submission remains possible');
console.log('Damage upload UI: all checks passed.');
