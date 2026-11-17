/* Local photo zoom. Re-renders the photo and original rim cutout, never uploads it. */
(function(root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) { root.ShopWheelStudioZoom = api; if (root.document) api.mount(root); }
})(typeof window !== 'undefined' ? window : null, function() {
  'use strict';
  const clamp = (n, a, b) => Math.min(b, Math.max(a, n));
  const zoomValue = n => clamp(Number.isFinite(Number(n)) ? Number(n) : 1, 1, 6);
  function targetSize(width, height, dpr = 1) {
    if (![width,height,dpr].every(Number.isFinite) || width <= 0 || height <= 0) return null;
    const factor = Math.min(clamp(dpr,1,2), 2048 / Math.max(width,height));
    return {width: Math.max(1,Math.round(width*factor)),height: Math.max(1,Math.round(height*factor))};
  }
  function pointInView(point,bounds,width,height) {
    if (!bounds || ![point?.clientX,point?.clientY,bounds.left,bounds.top,bounds.width,bounds.height,width,height].every(Number.isFinite) || bounds.width <= 0 || bounds.height <= 0) return null;
    return {x:clamp((point.clientX-bounds.left)/bounds.width,0,1)*width,y:clamp((point.clientY-bounds.top)/bounds.height,0,1)*height};
  }
  function scenePoint(point,frame) {
    if (!point || !frame || ![frame.scale,frame.sceneWidth,frame.sceneHeight].every(n=>Number.isFinite(n)&&n>0)) return null;
    return {x:clamp((point.x-frame.offsetX)/(frame.scale*frame.sceneWidth),0,1),y:clamp((point.y-frame.offsetY)/(frame.scale*frame.sceneHeight),0,1)};
  }
  function panFocus(frame,dx,dy) {
    if (!frame || ![dx,dy,frame.scale,frame.sceneWidth,frame.sceneHeight].every(Number.isFinite) || frame.scale<=0 || frame.sceneWidth<=0 || frame.sceneHeight<=0) return null;
    return {focusX:clamp(frame.focusX-dx/(frame.sceneWidth*frame.scale),0,1),focusY:clamp(frame.focusY-dy/(frame.sceneHeight*frame.scale),0,1)};
  }
  function anchoredFocus(frame,point,newZoom) {
    const source=scenePoint(point,frame);if(!source)return null;
    const zoom=zoomValue(newZoom),scale=frame.scale*zoom/frame.zoom;
    return {zoom,focusX:clamp(source.x-(point.x-frame.width/2)/(frame.sceneWidth*scale),0,1),focusY:clamp(source.y-(point.y-frame.height/2)/(frame.sceneHeight*scale),0,1)};
  }
  function mount(win) {
    const doc=win.document,source=doc.getElementById('ws-canvas'),trigger=doc.getElementById('ws-enlarge');
    if(!source||!trigger||doc.documentElement.dataset.wheelStudioZoomMounted)return null;
    doc.documentElement.dataset.wheelStudioZoomMounted='true';
    const el=(tag,text='',cls='')=>{const n=doc.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;};
    const button=(text,label,cls='')=>{const n=el('button',text,cls);n.type='button';n.setAttribute('aria-label',label);return n;};
    const dialog=el('dialog','','ws-large-dialog');dialog.setAttribute('aria-labelledby','ws-large-title');
    const header=el('div','','ws-large-header'),heading=el('h2','Dein Auto aus der Nähe');heading.id='ws-large-title';
    const close=button('Schließen ×','Großansicht schließen');header.append(heading,close);
    const toolbar=el('div','','ws-zoom-toolbar'),minus=button('−','Verkleinern','ws-zoom-minus'),plus=button('+','Vergrößern','ws-zoom-plus'),reset=button('Ganzes Auto','Ganzes Auto','ws-zoom-reset'),level=el('output','1×','ws-zoom-level');level.setAttribute('aria-live','polite');level.setAttribute('aria-label','Vergrößerung');
    toolbar.append(minus,level,plus,reset,el('span','Klicken zum Zoomen · Ziehen zum Verschieben','ws-zoom-hint'));
    const figure=el('figure','','ws-large-figure'),canvas=el('canvas');canvas.tabIndex=0;canvas.setAttribute('role','img');canvas.setAttribute('aria-label','Vergrößerte Felgenvorschau. Klicken oder Plus zum Zoomen, ziehen oder Pfeiltasten zum Verschieben.');figure.append(canvas);
    const note=el('p','','ws-large-note');dialog.append(header,toolbar,figure,note);doc.body.append(dialog);
    const state={zoom:1,focusX:.5,focusY:.5,frame:null,opener:trigger,drag:null,dragged:false,sourceDown:null};
    const engine=()=>win.ShopWheelStudioViewport;
    const adjusting=()=>Boolean(doc.querySelector('.ws-adjust')?.open);
    const initialZoom=()=>win.innerWidth<600?5:3;
    function paint() {
      if(!dialog.open)return;
      const bounds=figure.getBoundingClientRect(),size=targetSize(bounds.width||win.innerWidth-60,bounds.height||win.innerHeight-220,win.devicePixelRatio||1);
      if(!size)return;
      const frame=engine()?.render?.(canvas,{...size,zoom:state.zoom,focusX:state.focusX,focusY:state.focusY});
      if(!frame)return;
      state.frame=frame;state.zoom=frame.zoom;state.focusX=frame.focusX;state.focusY=frame.focusY;
      level.textContent=`${Number(state.zoom.toFixed(1))}×`;minus.disabled=state.zoom<=1;plus.disabled=state.zoom>=6;dialog.dataset.zoom=String(state.zoom);
      note.textContent=`${doc.getElementById('ws-look-name')?.textContent||'Felgenvorschau'} · Optische Simulation. Mit Esc schließen.`;
    }
    function openAt(focus={x:.5,y:.5},zoom=1,opener=trigger) {
      if(!engine()?.getScene?.()?.ready || !doc.getElementById('ws-photo-empty')?.hidden) {doc.getElementById('ws-status').textContent='Bitte zuerst ein Seitenfoto oder das Beispielauto wählen.';return false;}
      state.zoom=zoomValue(zoom);state.focusX=clamp(focus.x,0,1);state.focusY=clamp(focus.y,0,1);state.opener=opener;
      state.drag=null;state.dragged=false;if(!dialog.open)dialog.showModal();paint();canvas.focus();return true;
    }
    function setZoom(zoom,point=null) {
      const next=zoomValue(zoom),focus=point&&state.frame?anchoredFocus(state.frame,point,next):{zoom:next,focusX:state.focusX,focusY:state.focusY};
      if(focus)Object.assign(state,focus);paint();
    }
    trigger.addEventListener('click',()=>openAt({x:.5,y:.5},1,trigger));
    source.addEventListener('pointerdown',event=>{state.sourceDown={x:event.clientX,y:event.clientY};});
    source.addEventListener('click',event=>{
      if(adjusting()){state.sourceDown=null;return;}
      if(state.sourceDown&&Math.hypot(event.clientX-state.sourceDown.x,event.clientY-state.sourceDown.y)>8){state.sourceDown=null;return;}
      const bounds=source.getBoundingClientRect(),p=pointInView(event,bounds,1,1);state.sourceDown=null;
      if(p)openAt(p,initialZoom(),source);
    });
    source.addEventListener('keydown',event=>{if(!adjusting()&&['Enter',' '].includes(event.key)){event.preventDefault();openAt({x:.5,y:.5},initialZoom(),source);}});
    plus.addEventListener('click',()=>setZoom(state.zoom*1.4));minus.addEventListener('click',()=>setZoom(state.zoom/1.4));reset.addEventListener('click',()=>openAt({x:.5,y:.5},1,state.opener));
    canvas.addEventListener('wheel',event=>{event.preventDefault();const point=pointInView(event,canvas.getBoundingClientRect(),canvas.width,canvas.height);setZoom(state.zoom*Math.exp(-clamp(event.deltaY,-150,150)*.004),point);},{passive:false});
    canvas.addEventListener('pointerdown',event=>{if(event.button!==undefined&&event.button!==0)return;event.preventDefault();canvas.focus();state.drag={x:event.clientX,y:event.clientY};state.dragged=false;canvas.setPointerCapture?.(event.pointerId);});
    canvas.addEventListener('pointermove',event=>{
      if(!state.drag||!state.frame)return;
      const bounds=canvas.getBoundingClientRect(),dx=(event.clientX-state.drag.x)*canvas.width/bounds.width,dy=(event.clientY-state.drag.y)*canvas.height/bounds.height;
      if(Math.hypot(event.clientX-state.drag.x,event.clientY-state.drag.y)>3)state.dragged=true;
      if(!state.dragged)return;
      const next=panFocus(state.frame,dx,dy);state.drag={x:event.clientX,y:event.clientY};if(next)Object.assign(state,next);paint();
    });
    const finishDrag=()=>{state.drag=null;};canvas.addEventListener('pointerup',finishDrag);canvas.addEventListener('pointercancel',()=>{finishDrag();state.dragged=true;});
    canvas.addEventListener('click',event=>{
      if(state.dragged){state.dragged=false;return;}
      const point=pointInView(event,canvas.getBoundingClientRect(),canvas.width,canvas.height),focus=scenePoint(point,state.frame);
      if(state.zoom>=6){openAt({x:.5,y:.5},1,state.opener);return;}
      if(focus)openAt(focus,Math.min(6,state.zoom*1.5),state.opener);
    });
    canvas.addEventListener('keydown',event=>{
      if(['+','=','-','_','0'].includes(event.key)){event.preventDefault();if(event.key==='0')openAt({x:.5,y:.5},1,state.opener);else setZoom(state.zoom*(event.key==='-'||event.key==='_'?1/1.4:1.4));return;}
      const move={ArrowLeft:[.08,0],ArrowRight:[-.08,0],ArrowUp:[0,.08],ArrowDown:[0,-.08]}[event.key];
      if(move&&state.frame){event.preventDefault();const next=panFocus(state.frame,move[0]*canvas.width,move[1]*canvas.height);if(next)Object.assign(state,next);paint();}
    });
    close.addEventListener('click',()=>dialog.close());dialog.addEventListener('close',()=>{canvas.width=1;canvas.height=1;state.frame=null;state.drag=null;state.opener?.focus?.();});
    dialog.addEventListener('click',event=>{if(event.target!==dialog)return;const box=dialog.getBoundingClientRect();if(event.clientX<box.left||event.clientX>box.right||event.clientY<box.top||event.clientY>box.bottom)dialog.close();});
    win.addEventListener('resize',paint);win.addEventListener('shop:wheel-studio-change',paint);
    win.addEventListener('pagehide',()=>{if(dialog.open)dialog.close();canvas.width=1;canvas.height=1;});
    return {openAt,paint,state,dialog,canvas};
  }
  return {zoomValue,targetSize,pointInView,scenePoint,panFocus,anchoredFocus,mount};
});
