(() => {
  'use strict';
  const host=window.AssistantWorkflowHost, $=id=>document.getElementById(id);
  if(!host||!$('workflow-dialog'))return;
  const dialog=$('workflow-dialog'), root=$('assistant');
  const documents=root.dataset.documentsEnabled==='true', offers=root.dataset.offersEnabled==='true';
  const fields=['kunde_name','fahrzeug','kennzeichen','fin_nummer','hsn_nummer','tsn_nummer','kunde_email','kontakt_telefon','beschreibung','farbcode','farbton','farbton_2'];
  const labels={kunde_name:'Auftraggeber',fahrzeug:'Fahrzeug',kennzeichen:'Kennzeichen',fin_nummer:'FIN',hsn_nummer:'HSN',tsn_nummer:'TSN',kunde_email:'Kunden-E-Mail',kontakt_telefon:'Telefon',beschreibung:'Gewünschte Arbeiten',farbcode:'Farbcode',farbton:'Farbton',farbton_2:'Zweiter Farbton',analyse_text:'Auslesehinweis',bauteile_override:'Erkannte Bauteile'};
  const uploadStates={bereit:'Gespeichert, noch nicht ausgewertet',auswertung:'Auswertung läuft',pruefen:'Zur Prüfung',zugeordnet:'Auftrag zugeordnet',fehler:'Auswertung bitte erneut starten'};
  const purposes={fahrzeugschein:'Fahrzeugschein',schaden:'Schadenfoto',angebot:'Lieferantenangebot',sonstiges:'Arbeitsunterlage'};
  let generation=0, reviewRevision=0, uploads=[], selected=null, newSource=null, uploadRequest=null, currentTab='upload', newIntake=false;
  let editorOrderId=null, mailOrderId=null, mailReady=false;
  const mailDrafts=new Map();
  const busy=new Set();
  const note=text=>{$('workflow-status').textContent=text;};
  const dataList=(value,...keys)=>Array.isArray(value)?value:keys.map(key=>value?.[key]).find(Array.isArray)||[];
  const item=value=>value?.upload||value?.unterlage||value;
  const order=()=>{const value=host.getOrder();if(newIntake||!value?.id)throw new Error('Bitte zuerst den passenden Auftrag auswählen oder einen neuen Auftrag vorbereiten.');return value;};
  const current=run=>run===generation&&dialog.open;
  function guarded(element,fn){
    return async event=>{
      event?.preventDefault();if(busy.has(element))return;
      const run=generation;busy.add(element);
      const buttons=element.tagName==='BUTTON'?[element]:[...element.querySelectorAll('button')];
      const disabled=buttons.map(button=>button.disabled);buttons.forEach(button=>button.disabled=true);
      try{await fn(event,run);}catch(error){if(current(run))note(error.message);}
      finally{busy.delete(element);buttons.forEach((button,index)=>button.disabled=disabled[index]);}
    };
  }
  function orderLabel(){
    const value=newIntake?null:host.getOrder();
    $('workflow-order').textContent=value?`Auftrag ${value.id} · ${value.fahrzeug||''} · ${value.kennzeichen||''}`:'Kein Auftrag ausgewählt. Einen neuen Auftrag kannst du hier vorbereiten.';
  }
  function editor(){
    const value=host.getOrder()||{};
    for(const id of ['workflow-color-form','workflow-contact-form'])for(const input of $(id).querySelectorAll('[data-edit-field]')){
      if(editorOrderId!==value.id||input.dataset.changed!=='true'){
        input.value=value[input.name]||'';input.dataset.changed='false';
      }
    }
    editorOrderId=value.id;
  }
  async function proposed(payload,run){
    host.stopVoice();note('Geprüften Vorschlag vorbereiten …');
    const result=await host.api('/vorschlag',payload);
    if(!current(run))return;
    dialog.close();await host.proposed(result);
  }
  function tab(name){
    if((['upload','new','edit'].includes(name)&&!documents)||(['mail','sources'].includes(name)&&!offers))return;
    currentTab=name;
    dialog.querySelectorAll('[data-workflow-panel]').forEach(panel=>panel.hidden=panel.dataset.workflowPanel!==name);
    dialog.querySelectorAll('[data-workflow-tab]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.workflowTab===name)));
    if(name==='edit')editor();
    if(name==='mail'){mailKind();void loadMail(generation);}
    if(name==='sources')void loadSources(generation);
  }
  function open(name,forNew=false){
    newIntake=forNew;
    host.stopVoice();generation++;note('');orderLabel();
    if(!dialog.open)dialog.showModal();tab(name);
    if(name==='upload')void loadUploads(generation);
  }
  function close(){generation++;dialog.close();}
  $('workflow-close').onclick=close;
  dialog.addEventListener('close',()=>{generation++;});
  dialog.addEventListener('cancel',()=>{generation++;});
  $('workflow-upload')?.addEventListener('click',()=>open('upload'));
  $('workflow-new')?.addEventListener('click',()=>open('new'));
  $('workflow-offer')?.addEventListener('click',()=>open('mail'));
  function resetNew(){
    newSource=null;$('workflow-new-form').reset();$('workflow-new-source').textContent='';
  }
  $('workflow-new-reset').onclick=()=>{resetNew();note('Leerer neuer Auftrag ohne Vorlage.');};
  window.AssistantWorkflow={
    openUpload:value=>{if(documents)open('upload',!value?.id);},
    created:()=>{resetNew();selected=null;$('workflow-review').hidden=true;newIntake=false;orderLabel();},
  };
  dialog.querySelectorAll('[data-workflow-tab]').forEach(button=>button.onclick=()=>{generation++;note('');tab(button.dataset.workflowTab);});
  document.addEventListener('assistant-order-changed',()=>{
    generation++;newIntake=false;orderLabel();editor();
    if(dialog.open){note('Auftrag gewechselt. Ziel vor dem Vorschlag prüfen.');if(currentTab==='mail')void loadMail(generation);if(currentTab==='sources')void loadSources(generation);}
  });
  function button(text,fn){const el=document.createElement('button');el.type='button';el.className='secondary';el.textContent=text;el.onclick=guarded(el,fn);return el;}
  function showUploads(){
    const list=$('workflow-uploads');list.replaceChildren();
    if(!uploads.length){const p=document.createElement('p');p.textContent='Noch keine privaten Unterlagen hochgeladen.';list.append(p);}
    for(const upload of uploads){
      const card=document.createElement('article');card.className='workflow-card';
      const title=document.createElement('strong');title.textContent=upload.original_name||'Unterlage';
      const description=document.createElement('p');description.textContent=`${purposes[upload.zweck]||'Unterlage'} · ${uploadStates[upload.status]||'Status prüfen'}${upload.auftrag_id?' · Auftrag '+upload.auftrag_id:''}`;
      const original=document.createElement('a');original.textContent='Original ansehen';original.href='/werkstatt/assistent/unterlagen/'+encodeURIComponent(upload.id)+'/original';original.target='_blank';original.rel='noopener';
      card.append(title,description,original);
      card.append(button('Angaben auslesen',async(event,run)=>analyse(upload,run)));
      card.append(button('Angaben prüfen',async()=>review(upload)));
      if(!upload.auftrag_id&&upload.status==='pruefen'&&!newIntake)card.append(button('Original diesem Auftrag zuordnen',async(event,run)=>proposed({art:'datei',auftrag_id:order().id,upload_id:upload.id},run)));
      list.append(card);
    }
  }
  async function analyse(upload,run){
    const intent=++reviewRevision;
    note('Auswertung läuft …');
    let value;
    try{value=item(await host.api('/unterlagen/'+encodeURIComponent(upload.id)+'/analyse',{}));}
    catch(error){if(current(run)&&intent===reviewRevision)throw error;return;}
    if(!current(run))return;
    uploads=uploads.map(row=>row.id===value.id?value:row);showUploads();
    if(intent!==reviewRevision)return;
    review(value);
    note(value.status==='pruefen'||value.status==='zugeordnet'?'Auslese prüfen. Es wurde noch kein Auftrag verändert.':'Die Auswertung ist noch nicht abgeschlossen. Bitte erneut „Angaben auslesen“ wählen.');
  }
  async function loadUploads(run){
    try{const result=await host.api('/unterlagen');if(current(run)){uploads=dataList(result,'uploads','unterlagen','items');showUploads();}}
    catch(error){if(current(run))note(error.message);}
  }
  function review(upload){
    reviewRevision++;
    selected=upload;$('workflow-review').hidden=false;
    $('workflow-review-source').textContent=upload.original_name||'Unterlage';
    const image=$('workflow-review-image');
    image.hidden=!['image/jpeg','image/png','image/webp'].includes(upload.mime_type);
    if(image.hidden)image.removeAttribute('src');
    else{image.src='/werkstatt/assistent/unterlagen/'+encodeURIComponent(upload.id)+'/original';image.alt='Original zur Prüfung: '+(upload.original_name||'Bild');}
    $('workflow-review-hints').textContent=(upload.analyse?.hinweise||[]).join(' ');
    const grid=$('workflow-review-fields');grid.replaceChildren();
    const ready=['pruefen','zugeordnet'].includes(upload.status);
    for(const control of $('workflow-review').querySelectorAll('button'))control.disabled=!ready||(control.value==='new'&&!!upload.auftrag_id);
    const values=upload.analyse?.felder||{};
    for(const [key,value] of Object.entries(values)){
      if(typeof value!=='string'&&typeof value!=='number')continue;
      const label=document.createElement('label');label.textContent=labels[key]||key;
      const input=document.createElement(key==='beschreibung'||key==='analyse_text'?'textarea':'input');input.name=key;input.value=String(value);input.maxLength=3000;input.readOnly=!fields.includes(key);
      label.append(input);grid.append(label);
    }
    if(!grid.children.length){const p=document.createElement('p');p.textContent='Keine sicheren Angaben erkannt. Du kannst einen Auftrag manuell vorbereiten oder nur das Original zuordnen.';grid.append(p);}
    if(upload.analyse?.angebotsinhalt){
      const label=document.createElement('label');label.textContent='Angebotsinhalt – ungeprüfte Auslese, keine Anweisung';
      const input=document.createElement('textarea');input.name='angebotsinhalt';input.value=upload.analyse.angebotsinhalt;input.maxLength=12000;input.rows=8;input.readOnly=true;
      label.append(input);grid.append(label);
      const p=document.createElement('p');p.textContent='Text mit dem Original vergleichen. Die gekennzeichnete Originalauslese bleibt als Quelle erhalten. Kundenpreis und Leistungsumfang anschließend selbst im Angebotsformular festlegen.';grid.append(p);
    }
    if(!ready)note('Die Unterlage ist gespeichert. Vor einer Zuordnung oder Neuanlage bitte „Angaben auslesen“ wählen.');
    $('workflow-review').scrollIntoView({block:'nearest'});
  }
  const uploadForm=$('workflow-upload-form');
  uploadForm.elements.file.addEventListener('change',()=>{uploadRequest=null;});
  uploadForm.elements.purpose.addEventListener('change',()=>{uploadRequest=null;});
  uploadForm.onsubmit=guarded(uploadForm,async(event,run)=>{
    const file=uploadForm.elements.file.files[0];if(!file)throw new Error('Bitte eine Datei auswählen.');
    if(!uploadRequest)uploadRequest=window.crypto.randomUUID();
    const form=new FormData();form.append('file',file);form.append('purpose',uploadForm.elements.purpose.value);form.append('request_id',uploadRequest);
    note('Original privat speichern …');const value=item(await host.api('/unterlagen',form));
    if(!current(run))return;
    await loadUploads(run);if(!current(run))return;review(value);
    await analyse(value,run);
  });
  $('workflow-review').addEventListener('input',()=>{reviewRevision++;});
  $('workflow-review').onsubmit=guarded($('workflow-review'),async(event,run)=>{
    reviewRevision++;
    if(!selected)throw new Error('Bitte zuerst eine Unterlage auswählen.');
    if(!['pruefen','zugeordnet'].includes(selected.status))throw new Error('Bitte die Unterlage zuerst auslesen und die Angaben prüfen.');
    const values=Object.fromEntries(new FormData($('workflow-review')));
    const use=event.submitter?.value;
    if(use==='attach')return proposed({art:'datei',auftrag_id:order().id,upload_id:selected.id},run);
    if(use==='color'){
      const colors=Object.fromEntries(['farbcode','farbton','farbton_2'].filter(key=>String(values[key]||'').trim()).map(key=>[key,values[key]]));
      if(!Object.keys(colors).length)throw new Error('Keine Farbdaten erkannt. Unter „Farbe & Kontakt“ selbst ergänzen.');
      return proposed({art:'farbe',auftrag_id:order().id,felder:colors},run);
    }
    if(use!=='new')return;
    if(selected.auftrag_id)throw new Error('Diese Unterlage gehört bereits zu einem Auftrag. Für eine neue Aufnahme eine neue Unterlage hochladen.');
    const target=$('workflow-new-form');for(const key of fields)target.elements[key].value=values[key]||'';
    newSource=selected.id;$('workflow-new-source').textContent='Original wird nach Bestätigung mit zugeordnet: '+selected.original_name;
    tab('new');note('Angaben und Auftraggeber prüfen, fehlende Werte ergänzen.');
  });
  $('workflow-new-form').onsubmit=guarded($('workflow-new-form'),async(event,run)=>{
    const values=Object.fromEntries(new FormData($('workflow-new-form')));
    await proposed({art:'auftrag_neu',auftrag_id:0,felder:values,...(newSource?{upload_id:newSource}:{})},run);
  });
  for(const [id,kind] of [['workflow-color-form','farbe'],['workflow-contact-form','kontakt']]){
    const form=$(id);form.querySelectorAll('[data-edit-field]').forEach(input=>input.addEventListener('input',()=>{input.dataset.changed='true';}));
    form.onsubmit=guarded(form,async(event,run)=>{
      const changed=[...form.querySelectorAll('[data-edit-field]')].filter(input=>input.dataset.changed==='true');
      if(!changed.length)throw new Error('Bitte die zu ändernden Angaben eingeben.');
      await proposed({art:kind,auftrag_id:order().id,felder:Object.fromEntries(changed.map(input=>[input.name,input.value]))},run);
    });
  }
  function mailKind(){
    const customer=$('workflow-mail-form').elements.art.value==='kundenangebot';
    $('workflow-supplier-label').hidden=customer;$('workflow-gross-label').hidden=!customer;
    $('workflow-mail-form').elements.gesamt_brutto.required=customer;
    $('workflow-recipient-help').textContent=customer?'Der Empfänger wird aus der Kunden-E-Mail des Auftrags übernommen. Fehlt sie, zuerst unter „Farbe & Kontakt“ ergänzen.':'Nur bestätigte Lieferantenkontakte sind nutzbar. Fehlt K-Parts, muss die Werkstattleitung die richtige Adresse hinterlegen.';
  }
  $('workflow-mail-form').elements.art.addEventListener('change',mailKind);
  function mailDraft(){
    const form=$('workflow-mail-form');
    return {art:form.elements.art.value,text:form.elements.text.value,gross:form.elements.gesamt_brutto.value,
      supplier:form.elements.supplier_id.value,
      attachments:[...$('workflow-attachments').querySelectorAll('input')].filter(input=>input.checked).map(input=>String(input.value))};
  }
  function bindMailOrder(){
    const id=newIntake?null:host.getOrder()?.id||null;
    if(mailOrderId===id)return null;
    if(mailOrderId!==null){
      const previous=mailDraft();
      if(!mailReady)previous.attachments=mailDrafts.get(mailOrderId)?.attachments||[];
      mailDrafts.set(mailOrderId,previous);
    }
    const form=$('workflow-mail-form'), draft=mailDrafts.get(id)||{};
    mailOrderId=id;mailReady=false;
    form.elements.art.value=draft.art||'lieferantenanfrage';form.elements.text.value=draft.text||'';
    form.elements.gesamt_brutto.value=draft.gross||'';form.elements.supplier_id.value=draft.supplier||'';
    $('workflow-attachments').querySelectorAll('label').forEach(label=>label.remove());
    mailKind();
    return draft;
  }
  async function loadMail(run){
    const restored=bindMailOrder();
    const checked=restored?(restored.attachments||[]):mailReady?mailDraft().attachments:mailDrafts.get(mailOrderId)?.attachments||[];
    if(mailOrderId!==null)mailDrafts.set(mailOrderId,{...mailDraft(),attachments:checked});
    mailReady=false;
    const form=$('workflow-mail-form'), buttons=[...form.querySelectorAll('button')];buttons.forEach(button=>button.disabled=true);
    const holder=$('workflow-attachments');holder.querySelectorAll('label').forEach(label=>label.remove());
    try{
      order();
      const result=await host.api('/lieferanten');if(!current(run))return;
      const select=$('workflow-mail-form').elements.supplier_id, chosen=select.value;select.replaceChildren();
      const none=document.createElement('option');none.value='';none.textContent='Kein bestätigter Kontakt ausgewählt';select.append(none);
      for(const contact of dataList(result,'lieferanten','contacts')){
        if(!(contact.verified||contact.verified_at))continue;
        const option=document.createElement('option');option.value=contact.id;option.textContent=`${contact.name} · ${contact.recipient||contact.email||''}`;select.append(option);
      }
      select.value=chosen;
      if(documents){const loaded=await host.api('/unterlagen');if(!current(run))return;uploads=dataList(loaded,'uploads','unterlagen','items');}
      for(const upload of uploads.filter(u=>u.zweck==='schaden'&&u.datei_id&&u.auftrag_id===mailOrderId)){
        const label=document.createElement('label');label.className='inline';const input=document.createElement('input');input.type='checkbox';input.name='attachment_ids';input.value=upload.datei_id;
        input.checked=checked.includes(String(upload.datei_id));
        label.append(input,document.createTextNode(upload.original_name));holder.append(label);
      }
      mailReady=true;buttons.forEach(button=>button.disabled=false);
    }catch(error){if(current(run))note(error.message);}
  }
  $('workflow-mail-form').onsubmit=guarded($('workflow-mail-form'),async(event,run)=>{
    if(!mailReady||order().id!==mailOrderId)throw new Error('Bitte warten, bis Empfänger und Anhänge für diesen Auftrag geladen sind.');
    const values=new FormData($('workflow-mail-form')), kind=values.get('art');
    await proposed({art:kind,auftrag_id:order().id,text:values.get('text'),
      ...(kind==='kundenangebot'?{gesamt_brutto:values.get('gesamt_brutto')}:{supplier_id:values.get('supplier_id')||null}),
      attachment_ids:values.getAll('attachment_ids').map(Number)},run);
  });
  async function loadSources(run){
    try{
      const selectedOrder=order();note('Vorhandene Angebotsquellen laden …');
      const result=await host.api('/angebote/'+selectedOrder.id);if(!current(run))return;
      const list=$('workflow-sources');list.replaceChildren();
      const sources=dataList(result,'sources','quellen');
      for(const source of sources){const card=document.createElement('article');card.className='workflow-card';const heading=document.createElement('h4');heading.textContent=source.title||`Quelle ${source.id}`;const meta=document.createElement('p');meta.textContent=[source.type,source.date,source.sender,source.source].filter(Boolean).join(' · ');const text=document.createElement('pre');text.textContent=source.text||'Noch keine lesbare Auswertung hinterlegt.';card.append(heading,meta,text);list.append(card);}
      note(sources.length?(result.hinweis||'Quellen prüfen. Kundenpreis selbst festlegen.'):'Keine zugeordneten Angebotsquellen vorhanden. Angebotsdatei hochladen oder die Mail im Cockpit zuordnen.');
    }catch(error){if(current(run))note(error.message);}
  }
  $('workflow-sources-reload').onclick=()=>{void loadSources(generation);};
})();
