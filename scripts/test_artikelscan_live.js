'use strict';
// Reuse the scanner's existing synthetic camera/DOM lifecycle harness.
const fs = require('node:fs');
const path = require('node:path');
let harness = fs.readFileSync(path.join(__dirname, 'test_materialscanner_ui.js'), 'utf8').split("test('unsupported", 1)[0];
harness = harness.replace('blob=null, play=null}', 'blob=null, play=null, zxing=null}');
harness = harness.replace('BarcodeDetector:supported?Detector:null,', 'BarcodeDetector:supported?Detector:null, ZXingBrowser:zxing,');
const tests = String.raw`
function bundledStub(decode) {
  return {BarcodeFormat:{QR_CODE:11,EAN_13:7,EAN_8:6,UPC_A:14,UPC_E:15},
    BrowserMultiFormatReader:class {constructor(hints){this.hints=hints;} decodeFromCanvas(canvas){return decode(canvas);}}};
}
test('Safari without BarcodeDetector live-decodes using the bundled library and captures only pixels', async () => {
  let decoded=0;
  const zxing=bundledStub(canvas=>{decoded++;assert.ok(canvas.width>0);return {getText:()=> '10035120',getBarcodeFormat:()=>11};});
  const f=fixture({supported:false,zxing});await f.scanner.open();await settle();
  assert.equal(f.getUserMediaCalls.length,1);assert.equal(decoded,1);assert.equal(f.received.length,1);
  assert.equal(f.received[0].name,'artikelcode.jpg');assert.equal('rawValue' in f.received[0],false);
  assert.equal(f.stops,1);assert.equal(f.timers.size,0);
});
test('bundled decoder retries unreadable frames and stops cleanly on cancellation', async () => {
  const zxing=bundledStub(()=>{const error=new Error('no code');error.getKind=()=> 'NotFoundException';throw error;});
  const f=fixture({supported:false,zxing});await f.scanner.open();await settle();
  assert.equal(f.received.length,0);assert.equal(f.ids.dialog.hidden,false);
  f.ids.close.click();assert.equal(f.stops,1);assert.equal(f.timers.size,0);
  f.fireTimer(250);await settle();assert.equal(f.received.length,0);
});
test('cancel before Safari camera permission resolves cannot reopen or capture', async () => {
  const camera=deferred();let decoded=0;
  const zxing=bundledStub(()=>{decoded++;return {getText:()=> '10035120',getBarcodeFormat:()=>11};});
  const f=fixture({supported:false,zxing,camera});const opening=f.scanner.open();await settle();
  f.ids.close.click();camera.resolve(f.stream);await opening;
  assert.equal(decoded,0);assert.equal(f.stops,1);assert.equal(f.received.length,0);assert.equal(f.timers.size,0);
});
test('the pinned vendor library decodes actual QR pixels, not a mocked identifier', () => {
  const {execFileSync}=require('node:child_process');
  let ZXing,pixels;
  try {
    global.window=globalThis;
    ZXing=require('../static/vendor/zxing-browser.min.js');
    pixels=JSON.parse(execFileSync(process.env.PYTHON || 'python',['-c',
      'import cv2,json; from PIL import Image,ImageOps; im=Image.fromarray(cv2.QRCodeEncoder_create().encode("10035120")).convert("RGBA"); im=ImageOps.expand(im,border=4,fill="white").resize((296,296),Image.Resampling.NEAREST); print(json.dumps({"width":im.width,"height":im.height,"data":list(im.tobytes())}))'],
      {encoding:'utf8',maxBuffer:2*1024*1024}));
    const canvas={width:pixels.width,height:pixels.height,getContext:()=>({getImageData:()=>({data:Uint8ClampedArray.from(pixels.data)})})};
    const reader=new ZXing.BrowserMultiFormatReader(new Map());
    reader.possibleFormats=[ZXing.BarcodeFormat.QR_CODE];
    const result=reader.decodeFromCanvas(canvas);
    assert.equal(result.getText(),'10035120');assert.equal(result.getBarcodeFormat(),ZXing.BarcodeFormat.QR_CODE);
  } catch(error) {assert.fail('Actual QR decode failed: '+error.name+': '+error.message);}
});
`;
new Function('require', harness + tests)(require);
