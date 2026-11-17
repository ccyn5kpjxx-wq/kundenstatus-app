/* Public catalogue bridge for the approved browser-local photo/zoom engine. */
(function(root,factory){'use strict';const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;if(root&&root.document)api.mount(root);})(typeof window!=='undefined'?window:null,function(){
  'use strict';
  const RIM=/^(alloy_wheel|steel_wheel):[a-f0-9]{24}$/;
  const media=value=>typeof value==='string'&&/^\/shop\/media\/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|webp)$/.test(value);
  function publicPrice(value){
    if(!value||typeof value!=='object')return null;
    const result={};for(const key of ['currency','vat_percent','observed_at','expires_at','stock_reported'])result[key]=value[key];
    result.prices_by_quantity={};for(let q=1;q<=4;q++){const line=value.prices_by_quantity?.[q];if(line&&typeof line==='object')result.prices_by_quantity[q]={quantity:line.quantity,unit_gross_cent:line.unit_gross_cent,items_gross_cent:line.items_gross_cent};}
    return result;
  }
  function publicFace(value){
    if(!value||typeof value!=='object'||value.reviewed!==true)return null;
    const face={reviewed:true};for(const key of ['width','height','cx','cy','rx','ry','rotation']){if(!Number.isFinite(value[key]))return null;face[key]=value[key];}
    if(!Number.isFinite(value.hub?.x)||!Number.isFinite(value.hub?.y))return null;face.hub={x:value.hub.x,y:value.hub.y};
    if(value.openings!==undefined){if(!Array.isArray(value.openings)||value.openings.length>48)return null;let count=0;face.openings=[];for(const polygon of value.openings){if(!Array.isArray(polygon)||polygon.length<3||polygon.length>128||(count+=polygon.length)>2048||!polygon.every(point=>Array.isArray(point)&&point.length===2&&point.every(Number.isFinite)))return null;face.openings.push(polygon.map(point=>[...point]));}}
    return face;
  }
  function normaliseProduct(value){
    if(!value||typeof value!=='object'||typeof value.id!=='string'||!RIM.test(value.id)||!['alloy_wheel','steel_wheel'].includes(value.category)||typeof value.name!=='string')return null;
    const photo=Array.isArray(value.images)&&value.images.find(media);if(!photo)return null;
    const diameter=String(value.geometry?.diameter||value.geometry?.diameter_in||'');
    if(!/^\d{1,2}(?:[.,]\d{1,2})?$/.test(diameter)||Number(diameter.replace(',','.'))<8||Number(diameter.replace(',','.'))>30)return null;
    const product={selection_id:value.id,category:value.category,name:value.name,manufacturer_name:typeof value.brand==='string'?value.brand:'',size:typeof value.size==='string'?value.size:'',diameter,photo,
      detail_fields:Object.entries(value.specifications||{}).filter(([key,text])=>['Farbe','Ausführung','Einpresstiefe','Lochkreis','Herstellerartikelnummer','EAN'].includes(key)&&typeof text==='string').map(([label,text])=>({label,value:text})),
      public_product:{id:value.id,category:value.category,name:value.name,unit_label:value.unit_label,price_status:value.price_status,price:publicPrice(value.price)}};
    if(['inquiry_only','supplier_check_required'].includes(value.purchase_status))product.public_product.purchase_status=value.purchase_status;
    const face=publicFace(value.photo_face);if(face)product.photo_face=face;
    return product;
  }
  function mount(win){
    const doc=win.document;if(doc.body.dataset.page!=='felgenstudio'||!win.ShopStorefront||!win.ShopWheelStudioController)return null;
    const $=id=>doc.getElementById(id),engine=win.ShopWheelStudioController;
    let page=1,sequence=0,timer=null,requestedLoaded=false;
    const linked=new win.URLSearchParams(win.location.search).get('artikel');
    async function load(){
      const run=++sequence;$('ws-api-status').textContent='Vorbereitete Felgenansichten werden geladen …';$('ws-next').disabled=true;$('ws-previous').disabled=true;
      try{
        const query=new win.URLSearchParams({department:'wheels',studio:'1',page:String(page),page_size:'24'});
        const q=$('ws-family-query').value.trim();if(q)query.set('q',q);
        const result=await win.ShopStorefront.read('/products?'+query);
        if(run!==sequence)return;
        if(!Array.isArray(result.items)||result.items.length>24||result.page!==page||!Number.isInteger(result.total))throw new Error('invalid_public_page');
        const products=result.items.map(normaliseProduct).filter(Boolean);
        if(!requestedLoaded&&typeof linked==='string'&&RIM.test(linked)&&!products.some(row=>row.selection_id===linked)){
          requestedLoaded=true;
          try{const item=normaliseProduct((await win.ShopStorefront.read('/products/'+encodeURIComponent(linked)+'?quantity=4')).item);if(run!==sequence)return;if(item)products.unshift(item);}catch(_){/* only released products may be shown */}
        }
        engine.setProducts(products);
        $('ws-api-status').textContent=result.total?result.total+' vorbereitete Ausführungen · Fotoansicht ohne Fahrzeugfreigabe':'Für diese Suche ist noch keine geprüfte Fotoansicht vorhanden. Alle Felgen bleiben im Katalog sichtbar.';
        $('ws-page-label').textContent='Seite '+page;$('ws-previous').disabled=page<=1;$('ws-next').disabled=result.has_more!==true;
      }catch(_){if(run===sequence)$('ws-api-status').textContent='Die Felgenansichten sind gerade nicht abrufbar. Ihr eigenes Foto bleibt nur in diesem Browser.';}
    }
    $('ws-family-query').addEventListener('input',()=>{if(timer!==null)win.clearTimeout(timer);timer=win.setTimeout(()=>{page=1;load();},450);});
    $('ws-previous').addEventListener('click',()=>{if(page>1){page--;load();}});$('ws-next').addEventListener('click',()=>{page++;load();});
    load();return {load};
  }
  return {normaliseProduct,mount};
});
