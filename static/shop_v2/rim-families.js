/* Display-only rim families. Exact articles, quotes and approvals stay separate. */
(function(root,factory){
  'use strict';
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root)root.ShopRimFamilies=api;
})(typeof window!=='undefined'?window:null,function(){
  'use strict';
  const norm=value=>typeof value==='string'?value.normalize('NFKC').trim().replace(/\s+/gu,' ').toLocaleLowerCase('de'):'';
  function finish(product){
    const fields=Array.isArray(product?.detail_fields)?product.detail_fields:[];
    return norm(fields.find(row=>row?.label==='Farbe')?.value||product?.details?.Farbe||'');
  }
  function key(product){
    if(!product||!['alloy_wheel','steel_wheel'].includes(product.category))return null;
    const manufacturer=norm(product.manufacturer_name),name=norm(product.name);
    if(!manufacturer||!name)return typeof product.selection_id==='string'&&product.selection_id?JSON.stringify(['article',product.selection_id]):null;
    return JSON.stringify([product.category,manufacturer,name,finish(product)]);
  }
  function group(rows,{productFor=row=>row}={}){
    const groups=[],byKey=new Map();
    for(const [index,row] of (Array.isArray(rows)?rows:[]).entries()){
      const product=productFor(row),identity=key(product)||JSON.stringify(['ungrouped',index]);
      let family=byKey.get(identity);
      if(!family){family={key:identity,rows:[],representative:row,diameters:[]};byKey.set(identity,family);groups.push(family);}
      family.rows.push(row);
      const diameter=String(product?.diameter||'').trim();
      if(/^\d{1,2}(?:[.,]\d{1,2})?$/.test(diameter)&&!family.diameters.includes(diameter))family.diameters.push(diameter);
    }
    for(const family of groups)family.diameters.sort((a,b)=>Number(a.replace(',','.'))-Number(b.replace(',','.')));
    return groups;
  }
  return {key,group};
});
