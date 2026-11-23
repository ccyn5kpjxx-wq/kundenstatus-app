/* Browser-local optical wheel preview. No photo upload, persistence or cart mutation. */
(function(root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) { root.ShopWheelStudio = api; if (root.document) root.ShopWheelStudioController = api.mount(root); }
})(typeof window !== 'undefined' ? window : null, function() {
  'use strict';
  const TYPES = new Set(['image/jpeg', 'image/png', 'image/webp']);
  const clamp = (n, a, b) => Math.min(b, Math.max(a, n));
  function loadStudioImage(win, url, active = () => true, img = new win.Image()) {
    return new Promise((resolve, reject) => {
      let retries = 0, timer = null;
      const live = () => active() === true;
      img.onload = () => { if (timer !== null) win.clearTimeout(timer); live() ? resolve(img) : reject(new Error('image_superseded')); };
      img.onerror = () => {
        if (timer !== null) return;
        if (!live() || !safePhoto(url) || retries >= 5) { reject(new Error('image_decode')); return; }
        const delay = Math.min(5000, 400 * (2 ** retries)) + Math.floor(Math.random() * 300); retries++;
        timer = win.setTimeout(() => { timer = null; if (live()) img.src = url + '?shop_retry=' + retries; else reject(new Error('image_superseded')); }, delay);
      };
      if (img.fetchPriority !== 'low') img.fetchPriority = 'high';
      img.src = url;
    });
  }
  function validatePhotoMeta(file) {
    if (!file || !TYPES.has(file.type) || !/\.(jpe?g|png|webp)$/i.test(file.name || '')) return 'Bitte ein JPG-, PNG- oder WebP-Foto wählen.';
    if (!Number.isSafeInteger(file.size) || file.size <= 0 || file.size > 12 * 1024 * 1024) return 'Das Foto darf höchstens 12 MB groß sein.';
    return '';
  }
  function validSignature(bytes, type) {
    if (!(bytes instanceof Uint8Array)) return false;
    if (type === 'image/jpeg') return bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255;
    if (type === 'image/png') return [137,80,78,71,13,10,26,10].every((n,i) => bytes[i] === n);
    return type === 'image/webp' && [82,73,70,70].every((n,i) => bytes[i] === n) && [87,69,66,80].every((n,i) => bytes[i + 8] === n);
  }
  function fittedDimensions(width, height, maxEdge = 2048) {
    if (!Number.isInteger(width) || !Number.isInteger(height) || width < 150 || height < 100 || width * height > 24000000) return null;
    const scale = Math.min(1, maxEdge / Math.max(width, height));
    return {width: Math.round(width * scale), height: Math.round(height * scale)};
  }
  function normalizeWheel(value = {}) {
    const r = clamp(Number.isFinite(Number(value.r)) ? Number(value.r) : .08, .03, .18);
    return {x: clamp(Number.isFinite(Number(value.x)) ? Number(value.x) : .5, 0, 1),
      y: clamp(Number.isFinite(Number(value.y)) ? Number(value.y) : .72, 0, 1), r,
      face: targetFace(value.face)};
  }
  function mapPoint(point, bounds) {
    if (!bounds || bounds.width <= 0 || bounds.height <= 0 || !Number.isFinite(point?.clientX) || !Number.isFinite(point?.clientY)) return null;
    return {x: clamp((point.clientX - bounds.left) / bounds.width, 0, 1), y: clamp((point.clientY - bounds.top) / bounds.height, 0, 1)};
  }
  function safePhoto(value) { return /^\/shop\/media\/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|webp)$/.test(value || ''); }
  function textureDimensions(width, height, maxEdge = 1536) {
    if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 || width * height > 24000000) return null;
    const scale = Math.min(1, maxEdge / Math.max(width, height));
    return {width: Math.max(1, Math.round(width * scale)), height: Math.max(1, Math.round(height * scale))};
  }
  function targetFace(value = {}) {
    return {rxRatio:clamp(Number.isFinite(value.rxRatio)?value.rxRatio:.73,.45,.95),
      ryRatio:clamp(Number.isFinite(value.ryRatio)?value.ryRatio:.73,.45,.95),
      rotation:clamp(Number.isFinite(value.rotation)?value.rotation:0,-25,25)};
  }
  function photoFace(value, width = value?.width, height = value?.height) {
    if(!value || value.reviewed!==true || ![width,height,value.width,value.height].every(Number.isInteger)
      || width!==value.width || height!==value.height || width<100 || height<100 || width*height>24000000
      || ![value.cx,value.cy,value.rx,value.ry,value.rotation,value.hub?.x,value.hub?.y].every(Number.isFinite)
      || value.cx<0 || value.cx>width || value.cy<0 || value.cy>height
      || value.rx<width*.15 || value.rx>width*.7 || value.ry<height*.15 || value.ry>height*.7
      || Math.abs(value.rotation)>30 || value.hub.x<0 || value.hub.x>width || value.hub.y<0 || value.hub.y>height)return null;
    const openings=interiorPolygons(value.openings,width,height);if(!openings)return null;
    const face={cx:value.cx,cy:value.cy,rx:value.rx,ry:value.ry,rotation:value.rotation,width,height,
      hub:{x:value.hub.x,y:value.hub.y},openings,reviewed:true};
    const hub=ellipseUnit(face,face.hub);
    return hub.x*hub.x+hub.y*hub.y<.25 ? face : null;
  }
  function ellipseUnit(face, point) {
    const angle=face.rotation*Math.PI/180,c=Math.cos(angle),s=Math.sin(angle),dx=point.x-face.cx,dy=point.y-face.cy;
    return {x:(c*dx+s*dy)/face.rx,y:(-s*dx+c*dy)/face.ry};
  }
  function ellipsePixel(face, point) {
    const angle=face.rotation*Math.PI/180,c=Math.cos(angle),s=Math.sin(angle),x=point.x*face.rx,y=point.y*face.ry;
    return {x:face.cx+c*x-s*y,y:face.cy+s*x+c*y};
  }
  function diskBoost(point, offset) {
    const length=offset.x*offset.x+offset.y*offset.y;
    if(length<1e-14)return {x:point.x,y:point.y};
    if(!Number.isFinite(length)||length>=1)return null;
    const gamma=1/Math.sqrt(1-length),dot=offset.x*point.x+offset.y*point.y,
      factor=(gamma-1)*dot/length+gamma,denominator=gamma*(1+dot);
    if(denominator<=0)return null;
    return {x:(point.x+offset.x*factor)/denominator,y:(point.y+offset.y*factor)/denominator};
  }
  function faceToSource(face, point) {
    const hub=ellipseUnit(face,face.hub),source=diskBoost(point,hub);
    return source?ellipsePixel(face,source):null;
  }
  function sourceToFace(face, point) {
    const hub=ellipseUnit(face,face.hub);
    return diskBoost(ellipseUnit(face,point),{x:-hub.x,y:-hub.y});
  }
  function interiorPolygons(value,width,height) {
    if(value===undefined)return [];
    if(!Array.isArray(value)||value.length>48)return null;
    let points=0;
    const result=[];
    for(const polygon of value){
      if(!Array.isArray(polygon)||polygon.length<3||polygon.length>128||(points+=polygon.length)>2048)return null;
      if(!polygon.every(p=>Array.isArray(p)&&p.length===2&&p.every(Number.isFinite)&&p[0]>=0&&p[0]<=width&&p[1]>=0&&p[1]<=height))return null;
      result.push(polygon.map(p=>[p[0],p[1]]));
    }
    return result;
  }
  function maskInteriorPixels(data,width,height,polygons=[]) {
    const checked=interiorPolygons(polygons,width,height);
    if(!(data instanceof Uint8ClampedArray)||data.length!==width*height*4||!checked)return null;
    if(!checked.length)return data;
    const out=new Uint8ClampedArray(data);
    for(const polygon of checked){
      const left=Math.max(0,Math.floor(Math.min(...polygon.map(p=>p[0])))),right=Math.min(width-1,Math.ceil(Math.max(...polygon.map(p=>p[0])))),
        top=Math.max(0,Math.floor(Math.min(...polygon.map(p=>p[1])))),bottom=Math.min(height-1,Math.ceil(Math.max(...polygon.map(p=>p[1]))));
      for(let y=top;y<=bottom;y++)for(let x=left;x<=right;x++){
        let inside=false;const px=x+.5,py=y+.5;
        for(let i=0,j=polygon.length-1;i<polygon.length;j=i++){
          const a=polygon[i],b=polygon[j];
          if((a[1]>py)!==(b[1]>py)&&px<(b[0]-a[0])*(py-a[1])/(b[1]-a[1])+a[0])inside=!inside;
        }
        if(!inside)continue;
        const k=(y*width+x)*4,r=data[k],g=data[k+1],b=data[k+2];
        // Keep the supplier's orange mark above the geometric gap mask.
        const mark=y>height*.52&&x>width*.2&&x<width*.8&&r>120&&g>35&&g<220&&b<140&&r>g*1.2&&g>b*1.25;
        if(!mark)out[k+3]=0;
      }
    }
    return out;
  }
  function rectifyPixels(data,width,height,rawFace) {
    const face=photoFace(rawFace,width,height);
    if(!face || !(data instanceof Uint8ClampedArray) || data.length!==width*height*4)return null;
    data=maskInteriorPixels(data,width,height,face.openings);
    const edge=Math.max(100,Math.min(1536,Math.round(Math.max(face.rx,face.ry)*2))),out=new Uint8ClampedArray(edge*edge*4),half=edge/2;
    const hub=ellipseUnit(face,face.hub),length=hub.x*hub.x+hub.y*hub.y,gamma=length>1e-14?1/Math.sqrt(1-length):1,
      angle=face.rotation*Math.PI/180,c=Math.cos(angle),s=Math.sin(angle);
    for(let y=0;y<edge;y++)for(let x=0;x<edge;x++){
      let qx=(x+.5-half)/half,qy=(y+.5-half)/half;const distance=Math.hypot(qx,qy),coverage=clamp((1-distance)*half+.5,0,1);
      if(coverage<=0)continue;if(distance>1){qx/=distance;qy/=distance;}
      const dot=hub.x*qx+hub.y*qy,factor=length>1e-14?(gamma-1)*dot/length+gamma:0,den=gamma*(1+dot),
        ux=length>1e-14?(qx+hub.x*factor)/den:qx,uy=length>1e-14?(qy+hub.y*factor)/den:qy,
        sx=face.cx+c*ux*face.rx-s*uy*face.ry,sy=face.cy+s*ux*face.rx+c*uy*face.ry;
      if(sx<0||sy<0||sx>width-1||sy>height-1)continue;
      const ix=Math.floor(sx),iy=Math.floor(sy),fx=sx-ix,fy=sy-iy,jx=Math.min(width-1,ix+1),jy=Math.min(height-1,iy+1),
        indexes=[(iy*width+ix)*4,(iy*width+jx)*4,(jy*width+ix)*4,(jy*width+jx)*4],
        weights=[(1-fx)*(1-fy),fx*(1-fy),(1-fx)*fy,fx*fy];
      let alpha=0,red=0,green=0,blue=0;
      for(let n=0;n<4;n++){const index=indexes[n],a=data[index+3]*weights[n];alpha+=a;red+=data[index]*a;green+=data[index+1]*a;blue+=data[index+2]*a;}
      const dest=(y*edge+x)*4;
      if(alpha>0){out[dest]=red/alpha;out[dest+1]=green/alpha;out[dest+2]=blue/alpha;out[dest+3]=alpha*coverage;}
    }
    return {width:edge,height:edge,data:out};
  }
  function rimPlacement(width, height, outerRadius, target = {}) {
    if (!Number.isFinite(width) || !Number.isFinite(height) || !Number.isFinite(outerRadius) || width <= 0 || height <= 0 || width!==height || outerRadius <= 0) return null;
    const face=targetFace(target);
    return {rx:outerRadius*face.rxRatio,ry:outerRadius*face.ryRatio,rotation:face.rotation};
  }
  function rimBackdrop(product) {
    const colours=(Array.isArray(product?.detail_fields)?product.detail_fields:[])
      .filter(field=>/^farbe$/i.test(String(field?.label||'').trim())).map(field=>field.value).join(' ');
    const finish=String(colours||product?.name||'').normalize('NFKC').toLocaleLowerCase('de');
    return /black|schwarz|anthra[cz]it|graphit|gunmetal/.test(finish)?'contrast':'dark';
  }
  function paintRimFace(context, texture, x, y, outerRadius, target = {}, backdrop = 'dark') {
    const face = rimPlacement(texture?.width, texture?.height, outerRadius,target);
    if (!face) return false;
    context.save(); context.translate(x, y); context.rotate(face.rotation*Math.PI/180);
    // Fully conceal the old wheel. Dark finishes get a lighter neutral recess
    // so the spokes remain visible without revealing any old brake hardware.
    context.save();context.scale(face.rx+outerRadius*.014,face.ry+outerRadius*.014);
    const contrast=backdrop==='contrast',inner=contrast?'#727b82':'#090c0f',edge=contrast?'#4a535b':'#07090c',fade=contrast?'rgba(74,83,91,0)':'rgba(7,9,12,0)';
    const depth = context.createRadialGradient?.(-.15,-.15,0,0,0,1);
    if (depth) { depth.addColorStop(0, inner); depth.addColorStop(.98, edge); depth.addColorStop(1, fade); }
    context.beginPath(); context.arc(0,0,1,0,Math.PI*2);
    context.globalCompositeOperation = 'source-over'; context.fillStyle = depth || edge; context.fill();context.restore();
    context.globalCompositeOperation = 'source-over';
    context.imageSmoothingEnabled = true; context.imageSmoothingQuality = 'high';
    context.drawImage(texture, -face.rx, -face.ry, face.rx * 2, face.ry * 2);
    context.restore(); return true;
  }
  function viewportDimensions(width, height, maxEdge = 2400) {
    if (!Number.isFinite(width) || !Number.isFinite(height) || width < 1 || height < 1) return null;
    const edge = clamp(Number.isFinite(maxEdge) ? maxEdge : 2400, 1, 2400);
    const scale = Math.min(1, edge / Math.max(width, height), Math.sqrt(6000000 / (width * height)));
    return {width: Math.max(1, Math.round(width * scale)), height: Math.max(1, Math.round(height * scale))};
  }
  function viewportTransform(sceneWidth, sceneHeight, width, height, options = {}) {
    if (![sceneWidth,sceneHeight,width,height].every(value=>Number.isFinite(value)&&value>0)) return null;
    const zoom = clamp(Number.isFinite(Number(options.zoom)) ? Number(options.zoom) : 1, 1, 6);
    const scale = Math.min(width / sceneWidth, height / sceneHeight) * zoom;
    const halfX = Math.min(.5, width / (2 * sceneWidth * scale));
    const halfY = Math.min(.5, height / (2 * sceneHeight * scale));
    const focusX = clamp(Number.isFinite(Number(options.focusX)) ? Number(options.focusX) : .5, halfX, 1-halfX);
    const focusY = clamp(Number.isFinite(Number(options.focusY)) ? Number(options.focusY) : .5, halfY, 1-halfY);
    return {zoom,focusX,focusY,scale,offsetX:width/2-focusX*sceneWidth*scale,offsetY:height/2-focusY*sceneHeight*scale,width,height,sceneWidth,sceneHeight};
  }
  function mount(win) {
    const doc = win.document, template = doc.querySelector('#ws-data');
    if (!template || doc.documentElement.dataset.wheelStudioMounted) return null;
    let config; try { config = JSON.parse(template.content.textContent); } catch { return null; }
    const G = win.ShopRimFamilies, P = win.ShopPreviewPricing;
    if (!G) return null;
    doc.documentElement.dataset.wheelStudioMounted = 'true';
    const $ = id => doc.getElementById(id), node = (tag, text = '', cls = '') => { const n = doc.createElement(tag); n.textContent = text; if (cls) n.className = cls; return n; };
    let products = [], allFamilies = [], families = [];
    const canvas = $('ws-canvas'), ctx = canvas.getContext('2d');
    if (!ctx) { $('ws-status').textContent = 'Die Fotovorschau ist in diesem Browser nicht verfügbar. Der Felgenkatalog bleibt nutzbar.'; return null; }
    const demo = config.demo || {}, positions = {front: normalizeWheel(demo.front), rear: normalizeWheel(demo.rear)};
    const state = {base: null, rim: null, backdrop:'dark', rimNotice:'', photoKind:'demo', family: null, product: null, active: 'front', original: false, compare: 100, limit: 12, serial: 0, rimSerial: 0, photoUrl: null, disposed: false};
    const rimCache=new Map();
    // The media service may only have one upstream download slot. Give the selected
    // design priority and fetch visible thumbnails one at a time.
    const visibleThumbnails = new Set();
    let thumbnailLoading = false;
    let selectedImagePending = false;
    const thumbnailObserver = win.IntersectionObserver && new win.IntersectionObserver(entries => {
      for (const {target, isIntersecting} of entries) {
        if (isIntersecting && target.isConnected) visibleThumbnails.add(target);
        else visibleThumbnails.delete(target);
      }
      loadNextThumbnail();
    });
    async function loadNextThumbnail() {
      if (thumbnailLoading || selectedImagePending || state.disposed) return;
      for (const img of visibleThumbnails) {
        visibleThumbnails.delete(img);
        if (!img.isConnected || !img.dataset.studioPhoto) continue;
        thumbnailObserver?.unobserve(img);
        const url = img.dataset.studioPhoto;
        delete img.dataset.studioPhoto;
        thumbnailLoading = true;
        try {
          await loadStudioImage(win, url, () => img.isConnected && !state.disposed, img);
        } catch {
          if (img.isConnected && !state.disposed) img.replaceWith(node('span', 'Bild gerade nicht verfügbar', 'shop-no-image'));
        } finally {
          thumbnailLoading = false;
        }
        loadNextThumbnail();
        return;
      }
    }
    const exportUrls = new Set();
    const status = text => { $('ws-status').textContent = text; };
    const fold = value => String(value || '').normalize('NFKC').toLocaleLowerCase('de');
    const queryFold = value => fold(value).replace(/schwarz/g,'black').replace(/silber/g,'silver').replace(/grau/g,'grey');
    function revokePhoto() { if (state.photoUrl) { win.URL.revokeObjectURL(state.photoUrl); state.photoUrl = null; } }
    function resetPositions() {
      const custom=state.photoKind==='customer';
      positions.front=normalizeWheel(custom?{...demo.front,face:{}}:demo.front);
      positions.rear=normalizeWheel(custom?{...demo.rear,face:{}}:demo.rear);syncControls();
    }
    function syncControls() {
      const wheel = positions[state.active]; $('ws-radius').value = String(wheel.r * 100); $('ws-x').value = String(wheel.x * 100); $('ws-y').value = String(wheel.y * 100);
    }
    function wheelRadius(wheel) { return wheel.r * canvas.width; }
    function drawWheel(context, wheel) {
      const x = wheel.x * canvas.width, y = wheel.y * canvas.height, r = wheelRadius(wheel);
      if (state.rim) paintRimFace(context, state.rim, x, y, r,wheel.face,state.backdrop);
    }
    function paintScene(context, {guides = true} = {}) {
      context.drawImage(state.base,0,0,canvas.width,canvas.height);
      if (state.rim && !state.original && state.compare > 0) {
        context.save(); context.beginPath(); context.rect(canvas.width * (1 - state.compare / 100),0,canvas.width,canvas.height); context.clip();
        drawWheel(context, positions.front); drawWheel(context, positions.rear); context.restore();
      }
      const adjusting = $('ws-stage').classList.contains('ws-positioning') || $('ws-adjust-panel')?.open || doc.querySelector('.ws-adjust')?.open;
      if (guides && adjusting && !state.original) {
        const wheel = positions[state.active], x = wheel.x * canvas.width, y = wheel.y * canvas.height, r = wheelRadius(wheel);
        context.save(); context.beginPath(); context.arc(x,y,r * 1.04,0,Math.PI * 2); context.strokeStyle = '#ef4a3d'; context.lineWidth = Math.max(2,canvas.width / 500); context.setLineDash([7,5]); context.stroke(); context.setLineDash([]);
        context.beginPath(); context.moveTo(x-r*.13,y); context.lineTo(x+r*.13,y); context.moveTo(x,y-r*.13); context.lineTo(x,y+r*.13); context.stroke(); context.restore();
      }
    }
    function draw(options = {}) {
      if (state.disposed) return;
      ctx.clearRect(0,0,canvas.width,canvas.height);
      if (!state.base) { ctx.fillStyle = '#e9e6df'; ctx.fillRect(0,0,canvas.width,canvas.height); }
      else paintScene(ctx, options);
      win.dispatchEvent(new win.CustomEvent('shop:wheel-studio-change'));
    }
    function renderViewport(target, options = {}) {
      if (!state.base || state.disposed || !target || target === canvas || typeof target.getContext !== 'function') return null;
      const dims = viewportDimensions(Number(options.width) || target.width || state.base.width, Number(options.height) || target.height || state.base.height);
      if (!dims) return null;
      const view = viewportTransform(state.base.width,state.base.height,dims.width,dims.height,options);
      if (!view) return null;
      const context = target.getContext('2d'); if (!context) return null;
      target.width=dims.width; target.height=dims.height;
      context.imageSmoothingEnabled=true; context.imageSmoothingQuality='high';
      context.clearRect(0,0,dims.width,dims.height); context.fillStyle='#0d151a'; context.fillRect(0,0,dims.width,dims.height);
      context.save(); context.translate(view.offsetX,view.offsetY); context.scale(view.scale,view.scale);
      paintScene(context,{guides:false}); context.restore();
      return view;
    }
    win.ShopWheelStudioViewport=Object.freeze({
      getScene:()=>({ready:Boolean(state.base&&!state.disposed),width:state.base?.width||0,height:state.base?.height||0}),
      render:renderViewport,
    });
    function image(url, active = () => !state.disposed) { return loadStudioImage(win,url,active); }
    async function applyPhoto(url, kind, serial) {
      const img = await image(url,()=>serial===state.serial&&!state.disposed), fit = fittedDimensions(img.naturalWidth, img.naturalHeight);
      if (!fit) throw new Error('photo_dimensions');
      if (serial !== state.serial || state.disposed) return;
      const base = doc.createElement('canvas'); base.width = fit.width; base.height = fit.height;
      const context = base.getContext('2d'); if (!context) throw new Error('canvas_unavailable'); context.imageSmoothingEnabled=true; context.imageSmoothingQuality='high'; context.drawImage(img,0,0,fit.width,fit.height);
      state.base = base; canvas.width = fit.width; canvas.height = fit.height; state.original = false;state.photoKind=kind;
      $('ws-stage').style.aspectRatio = `${fit.width} / ${fit.height}`;
      $('ws-photo-empty').hidden = true; $('ws-image-kind').textContent = kind === 'demo' ? 'KI-generiertes Beispielauto' : 'Dein Foto · bleibt lokal';
      $('ws-before').setAttribute('aria-pressed','false'); $('ws-before').textContent = 'Original ansehen'; resetPositions(); draw();
    }
    async function useDemo() {
      const serial = ++state.serial; revokePhoto(); $('ws-photo-input').value = '';
      if (demo.src !== '/static/shop_v2/felgenstudio-demo.png') return;
      try { await applyPhoto(demo.src,'demo',serial); if (serial === state.serial) status(state.rimNotice||'Beispielauto geladen. Wähle einen Look oder nutze dein eigenes Seitenfoto.'); }
      catch { if (serial === state.serial) { state.base = null; $('ws-photo-empty').hidden = false; draw(); status('Eigenes Seitenfoto wählen, um die Vorschau zu starten.'); } }
    }
    async function usePhoto(file) {
      const error = validatePhotoMeta(file); if (error) { $('ws-photo-input').value=''; status(error); return false; }
      const serial = ++state.serial; revokePhoto();
      try {
        const signature = new Uint8Array(await file.slice(0,12).arrayBuffer()); if (!validSignature(signature,file.type)) throw new Error('signature');
        if (serial !== state.serial || state.disposed) return false;
        const url = win.URL.createObjectURL(file); state.photoUrl = url;
        try { await applyPhoto(url,'customer',serial); }
        finally { win.URL.revokeObjectURL(url); if (state.photoUrl === url) state.photoUrl = null; }
        if (serial === state.serial) status(state.rimNotice||'Dein Foto ist lokal geladen. Bei Bedarf die Radmitten unter „Radpositionen anpassen“ einstellen.');
        return serial === state.serial;
      } catch (err) {
        if (serial === state.serial) status(err.message === 'photo_dimensions' ? 'Bitte ein Foto bis 24 Megapixel wählen, mindestens 150 × 100 Pixel.' : 'Dieses Bild konnte nicht gelesen werden. Bitte ein anderes JPG-, PNG- oder WebP-Foto wählen.');
        return false;
      } finally { $('ws-photo-input').value = ''; }
    }
    function rimTexture(img,rawFace) {
      const face=photoFace(rawFace,img.naturalWidth,img.naturalHeight);if(!face)return null;
      const source=doc.createElement('canvas');source.width=face.width;source.height=face.height;
      const original=source.getContext('2d');if(!original)return null;
      original.drawImage(img,0,0,face.width,face.height);
      const pixels=original.getImageData(0,0,face.width,face.height),rectified=rectifyPixels(pixels.data,face.width,face.height,face);
      if(!rectified)return null;
      const texture=doc.createElement('canvas');texture.width=rectified.width;texture.height=rectified.height;
      const paint=texture.getContext('2d');if(!paint)return null;
      const output=paint.createImageData(rectified.width,rectified.height);output.data.set(rectified.data);paint.putImageData(output,0,0);
      return texture;
    }
    async function showRim(product) {
      const serial = ++state.rimSerial;
      selectedImagePending = true;
      state.rim=null;state.backdrop=rimBackdrop(product);state.rimNotice='';$('ws-look-name').textContent=product.name;draw();
      const face=photoFace(product.photo_face);
      const cacheKey=JSON.stringify([product.photo,face]);
      try {
        if(!face){state.rimNotice='Für dieses Design ist die Fotoansicht noch nicht vorbereitet.';status(state.rimNotice);return;}
        let texture=rimCache.get(cacheKey);
        if(!texture){const img=await image(product.photo,()=>serial===state.rimSerial&&!state.disposed);if(serial!==state.rimSerial||state.disposed)return;texture=rimTexture(img,face);if(!texture)throw new Error('photo_face');
          rimCache.set(cacheKey,texture);if(rimCache.size>8)rimCache.delete(rimCache.keys().next().value);}
        if(serial!==state.rimSerial||state.disposed)return;state.rim=texture;status('Felgenansicht geladen. Klicke auf ein Rad, um es näher anzusehen.');draw();
      } catch { if (serial === state.rimSerial) { state.rim = null;state.rimNotice='Das Felgenbild konnte nicht geladen werden. Wähle das Design erneut.'; draw(); status(state.rimNotice); } }
      finally { if (serial === state.rimSerial) { selectedImagePending = false; loadNextThumbnail(); } }
    }
    function price(product) {
      $('ws-price').replaceChildren();
      if (!product) return;
      if (win.ShopStorefront && product.public_product) {
        $('ws-price').append(win.ShopStorefront.priceElement(product.public_product, 4));
        return;
      }
      win.ShopLiveQuotes?.register([product]);
      const owner = node('div'); owner.dataset.product = JSON.stringify(product);
      const amount = node('strong'); amount.setAttribute('data-product-unit-price','');
      const note = node('span'); note.setAttribute('data-product-price-note','');
      const quantity = node('input'); quantity.type = 'hidden'; quantity.value = '4'; quantity.setAttribute('data-product-quantity','');
      const sizeNote = node('small',`Für ${product.size} · pro Stück bei 4 Stück`); owner.append(amount,note,sizeNote,quantity); $('ws-price').append(owner); P?.updateProductDisplay(owner); win.ShopAutoPrices?.scan(owner);
    }
    function populateVariants() {
      state.product = null; price(null); $('ws-configure').href = doc.body.dataset.completeUrl;
      $('ws-variant').replaceChildren(new win.Option('Ausführung wählen',''));
      const selected = state.family?.rows.filter(p => p.diameter === $('ws-size').value) || [];
      const labels = selected.map(p => { const facts=(p.detail_fields||[]).filter(f=>['Ausführung','Herstellerartikelnummer','EAN'].includes(f.label)&&!/^nicht /i.test(f.value)); return facts.map(f=>f.value).join(' · ')||p.size; });
      const duplicates = new Map(); labels.forEach(label=>duplicates.set(label,(duplicates.get(label)||0)+1)); const indexes=new Map();
      selected.forEach((product,i)=>{const label=labels[i],index=(indexes.get(label)||0)+1;indexes.set(label,index);$('ws-variant').add(new win.Option(label+(duplicates.get(label)>1?` · Artikel ${index}`:''),product.selection_id));});
      $('ws-variant').disabled = !selected.length;
    }
    function selectFamily(family, initialProduct = family.representative) {
      state.family = family; state.product = null; $('ws-selected-name').textContent = family.representative.name;
      $('ws-size').replaceChildren(new win.Option('Größe wählen','')); family.diameters.forEach(d=>$('ws-size').add(new win.Option(`${d} Zoll`,d)));
      if (family.diameters.length===1) $('ws-size').value=family.diameters[0]; populateVariants(); showRim(initialProduct);
      doc.querySelectorAll('[data-ws-family]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.wsFamily===family.key)));
    }
    function renderFamilies() {
      const terms=queryFold($('ws-family-query').value).split(/\s+/).filter(Boolean),matches=families.filter(f=>terms.every(term=>fold(f.representative.name).includes(term))),shown=matches.slice(0,state.limit);
      thumbnailObserver?.disconnect();
      visibleThumbnails.clear();
      $('ws-family-grid').replaceChildren(...shown.map(family=>{
        const b=node('button','','ws-family');b.type='button';b.dataset.wsFamily=family.key;b.setAttribute('aria-pressed',String(state.family?.key===family.key));b.setAttribute('aria-label',`${family.representative.name} am Auto ansehen`);
        const img = node('img');
        img.alt = '';
        img.decoding = 'async';
        img.fetchPriority = 'low';
        img.dataset.studioPhoto = family.representative.photo;
        b.append(img,node('span',family.representative.name)); b.addEventListener('click',()=>selectFamily(family));return b;
      }));
      for (const img of $('ws-family-grid').querySelectorAll('img')) {
        if (thumbnailObserver) thumbnailObserver.observe(img);
        else visibleThumbnails.add(img);
      }
      if (!thumbnailObserver) win.setTimeout(loadNextThumbnail, 0);
      $('ws-family-count').textContent=matches.length?`${shown.length} von ${matches.length} Designs für die Fotoansicht`:'Kein vorbereitetes Design gefunden. Alle Felgen stehen im Katalog.'; $('ws-family-more').hidden=shown.length>=matches.length;
    }
    $('ws-family-query').addEventListener('input',()=>{state.limit=12;renderFamilies();}); $('ws-family-more').addEventListener('click',()=>{state.limit+=12;renderFamilies();});
    $('ws-size').addEventListener('change',populateVariants); $('ws-variant').addEventListener('change',()=>{
      const selected=state.family?.rows.find(p=>p.selection_id===$('ws-variant').value&&p.diameter===$('ws-size').value)||null;
      state.product=selected;price(selected);$('ws-configure').href=doc.body.dataset.completeUrl+(selected?'?artikel='+encodeURIComponent(selected.selection_id):''); if(selected)showRim(selected);
    });
    $('ws-photo-input').addEventListener('change',()=>{const file=$('ws-photo-input').files?.[0];if(file)usePhoto(file);}); $('ws-demo').addEventListener('click',useDemo);
    $('ws-clear').addEventListener('click',()=>{state.serial++;revokePhoto();state.base=null;$('ws-photo-input').value='';$('ws-image-kind').textContent='Dein Seitenfoto';$('ws-photo-empty').hidden=false;draw();status('Das Foto wurde aus der Vorschau entfernt.');});
    $('ws-before').addEventListener('click',()=>{state.original=!state.original;$('ws-before').setAttribute('aria-pressed',String(state.original));$('ws-before').textContent=state.original?'Neuen Look ansehen':'Original ansehen';draw();});
    $('ws-compare').addEventListener('input',()=>{state.compare=Number($('ws-compare').value);draw();});
    doc.querySelectorAll('[name="ws-wheel"]').forEach(input=>input.addEventListener('change',()=>{if(input.checked){state.active=input.value;syncControls();draw();}}));
    for (const [id,axis,divisor] of [['ws-radius','r',100],['ws-x','x',100],['ws-y','y',100]]) $(id).addEventListener('input',()=>{positions[state.active]=normalizeWheel({...positions[state.active],[axis]:Number($(id).value)/divisor});draw();});
    $('ws-reset-position').addEventListener('click',()=>{resetPositions();draw();}); doc.querySelector('.ws-adjust').addEventListener('toggle',()=>draw());
    let dragging=false;
    function place(event) { const point=mapPoint(event,canvas.getBoundingClientRect());if(!point||!state.base)return;positions[state.active]=normalizeWheel({...positions[state.active],...point});syncControls();draw(); }
    canvas.addEventListener('pointerdown',event=>{
      if(!state.base||!doc.querySelector('.ws-adjust')?.open)return;event.preventDefault();canvas.focus();
      const point=mapPoint(event,canvas.getBoundingClientRect());
      if(point){const nearest=['front','rear'].map(key=>({key,d:Math.hypot((point.x-positions[key].x)*canvas.width,(point.y-positions[key].y)*canvas.height)})).sort((a,b)=>a.d-b.d)[0];if(nearest.d<wheelRadius(positions[nearest.key])*1.3){state.active=nearest.key;doc.querySelectorAll('[name="ws-wheel"]').forEach(input=>{input.checked=input.value===state.active;});}}
      dragging=true;$('ws-stage').classList.add('ws-positioning');canvas.setPointerCapture?.(event.pointerId);place(event);
    });
    canvas.addEventListener('pointermove',event=>{if(dragging)place(event);});
    const endDrag=()=>{dragging=false;$('ws-stage').classList.remove('ws-positioning');draw();};canvas.addEventListener('pointerup',endDrag);canvas.addEventListener('pointercancel',endDrag);
    canvas.addEventListener('keydown',event=>{const changes={ArrowLeft:[-.002,0],ArrowRight:[.002,0],ArrowUp:[0,-.002],ArrowDown:[0,.002]};if(!changes[event.key]||!state.base||!doc.querySelector('.ws-adjust')?.open)return;event.preventDefault();const [dx,dy]=changes[event.key],wheel=positions[state.active];positions[state.active]=normalizeWheel({...wheel,x:wheel.x+dx*(event.shiftKey?5:1),y:wheel.y+dy*(event.shiftKey?5:1)});syncControls();draw();});
    $('ws-download').addEventListener('click',()=>{if(!state.base){status('Bitte zuerst ein Seitenfoto wählen.');return;}draw({guides:false});canvas.toBlob(blob=>{if(!blob){status('Die Vorschau konnte nicht gespeichert werden.');return;}const url=win.URL.createObjectURL(blob),link=node('a');exportUrls.add(url);link.href=url;link.download='Gaertner-Felgenstudio-optische-Vorschau.png';link.click();win.setTimeout(()=>{win.URL.revokeObjectURL(url);exportUrls.delete(url);},1000);},'image/png');draw();});
    win.addEventListener('pagehide',()=>{state.disposed=true;state.serial++;state.rimSerial++;revokePhoto();exportUrls.forEach(url=>win.URL.revokeObjectURL(url));exportUrls.clear();});
    win.addEventListener('pageshow',event=>{state.disposed=false;if(event.persisted){const selected=state.product||state.family?.representative;if(selected)showRim(selected);renderFamilies();draw();}});
    function setProducts(rows) {
      products = (Array.isArray(rows) ? rows.slice(0,25) : []).filter(p => p && ['alloy_wheel','steel_wheel'].includes(p.category) && /^(alloy_wheel|steel_wheel):[a-f0-9]{24}$/.test(p.selection_id || '') && safePhoto(p.photo));
      allFamilies=G.group(products);
      families=allFamilies.flatMap(family=>{const representative=family.rows.find(product=>photoFace(product.photo_face));return representative?[{...family,representative}]:[];});
      state.family=null;state.product=null;state.rim=null;state.rimSerial++;state.limit=12;selectedImagePending=false;
      price(null);renderFamilies();draw();
      const article=new win.URLSearchParams(win.location.search).get('artikel');
      const requested=products.find(p=>p.selection_id===article);
      const family=requested?allFamilies.find(f=>f.rows.includes(requested)):families[0];
      if(family)selectFamily(family, requested || family.representative);
      if(requested){$('ws-size').value=requested.diameter;populateVariants();$('ws-variant').value=requested.selection_id;state.product=requested;price(requested);$('ws-configure').href=doc.body.dataset.completeUrl+'?artikel='+encodeURIComponent(requested.selection_id);}
      return products.length;
    }
    setProducts(config.products);
    useDemo();
    return {usePhoto,useDemo,draw,positions,state,renderViewport,setProducts};
  }
  return {loadStudioImage,validatePhotoMeta,validSignature,fittedDimensions,normalizeWheel,mapPoint,safePhoto,textureDimensions,targetFace,photoFace,ellipseUnit,ellipsePixel,diskBoost,faceToSource,sourceToFace,interiorPolygons,maskInteriorPixels,rectifyPixels,rimPlacement,rimBackdrop,paintRimFace,viewportDimensions,viewportTransform,mount};
});
