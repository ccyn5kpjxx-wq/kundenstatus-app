(() => {
  'use strict';
  const $=id=>document.getElementById(id), form=$('mail-source-form');
  if(!form)return;
  let running=false, paused=false, viewRevision=0, lastReport=null, activeQuery='';
  const labels={new:'Noch nicht gestartet',active:'Einlesen läuft',paused:'Pausiert',done:'Durchlauf beendet',pending:'Noch nicht erfasst',headers:'Absender werden erfasst',excluded:'Ausgeschlossen',changed:'Ordner geändert – neue Momentaufnahme nötig',queued:'Anhänge offen',review:'Absender zuordnen',review_files:'Dateien prüfen',files:'Rechnungsdateien übernommen',other:'Weitere Quelle',stored:'Neu gespeichert',duplicate:'Bereits gespeichert'};
  const node=(tag,value)=>{const el=document.createElement(tag);if(value!==undefined)el.textContent=value;return el;};
  async function post(path,data){
    const response=await fetch('/admin/assistent-mailquellen'+path,{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'X-CSRF-Token':form.elements.csrf_token.value},body:data});
    if(!response.ok){let detail;try{detail=await response.json();}catch{}throw new Error(detail?.error||'Einlesen unterbrochen. Erneut starten setzt den gespeicherten Stand fort.');}
    return response.json();
  }
  async function readStatus(query){
    const response=await fetch('/admin/assistent-mailquellen/status?'+new URLSearchParams({q:query}),{credentials:'same-origin',cache:'no-store'});
    if(!response.ok)throw new Error('Absendersuche nicht verfügbar. Bitte erneut versuchen.');
    return response.json();
  }
  function searchControls(){
    const allowed=!running&&['paused','done'].includes(lastReport?.state);
    $('mail-source-search-button').disabled=!allowed;$('mail-source-query').disabled=!allowed;
    if(!allowed)$('mail-source-search-note').textContent='Zum Suchen den Durchlauf pausieren oder abwarten.';
  }
  function render(report){
    lastReport=report;activeQuery=report.query||'';searchControls();
    const counts=report.counts||{}, folders=report.folders||[];
    const seen=folders.reduce((sum,item)=>sum+item.cursor,0),total=folders.reduce((sum,item)=>sum+item.total,0);
    $('mail-source-progress').textContent=`${labels[report.state]||'Stand prüfen'} · ${seen} von ${total} bislang gezählten Nachrichten erfasst · ${counts.queued||0} Nachrichten mit offenen Anhängen · ${counts.review||0} Nachrichten mit ungeklärtem Absender · ${counts.files||0} Nachrichten mit Rechnungsdateien · ${counts.review_files||0} Nachrichten mit Dateien zur Prüfung · ${counts.other||0} weitere Quellen · ${report.quarantined_files||0} Zahlungsbelege für die Artikelauslese gesperrt${report.error?' · '+report.error:''}`;
    if(!running&&['paused','done'].includes(report.state))$('mail-source-search-note').textContent=(report.unknown_senders||[]).length?`${report.unknown_senders.length} Absendergruppen angezeigt${activeQuery?' für „'+activeQuery+'“':''}.`:'Keine passenden ungeklärten Absender gefunden.';
    $('mail-source-hint').textContent=report.hint||'';
    $('mail-source-folders').replaceChildren(...folders.map(item=>node('li',`${item.label} · ${labels[item.state]||item.state}${item.state==='excluded'?'':` · ${item.cursor}/${item.total}${item.missing?' · '+item.missing+' Header nicht lesbar':''}`}`)));
    const unknown=$('mail-source-unknown');unknown.replaceChildren();
    for(const source of report.unknown_senders||[]){
      const section=node('section'),title=node('h3',source.sender_name||source.sender||'Absender nicht eindeutig');
      section.append(title,node('p',`${source.sender||'Keine eindeutige Adresse'} · ${source.n} Nachrichten`));
      if(source.sender){
        const classify=node('form'),label=node('label','Name des Materiallieferanten'),input=node('input');input.name='lieferant';input.required=true;input.maxLength=180;input.value=source.sender_name||'';label.append(input);
        const button=node('button','Diesem Materiallieferanten zum Lesen zuordnen');
        input.disabled=button.disabled=running&&!paused&&report.state==='active';classify.append(label,button);
        classify.onsubmit=async event=>{event.preventDefault();button.disabled=true;const revision=++viewRevision,query=activeQuery;try{const result=await post('/absender/'+source.id+'/freigeben',new FormData(classify));if(revision!==viewRevision)return;const report=query?await readStatus(query):result;if(revision===viewRevision)render(report);}catch(error){if(revision===viewRevision)$('mail-source-progress').textContent=error.message;button.disabled=false;}};
        section.append(classify);
      }
      unknown.append(section);
    }
    const results=$('mail-source-results');results.replaceChildren();
    for(const source of report.sources||[]){
      const section=node('section');section.append(node('h3',source.supplier),node('p',`${source.subject} · ${labels[source.state]||source.state}`),node('p',source.note));
      for(const attachment of source.attachments||[])section.append(node('p',[attachment.name,attachment.state==='review'?'Datei prüfen':labels[attachment.state]||attachment.state,attachment.note].filter(Boolean).join(' · ')));
      results.append(section);
    }
  }
  form.onsubmit=async event=>{
    event.preventDefault();if(running)return;viewRevision++;running=true;paused=false;searchControls();$('mail-source-start').disabled=true;$('mail-source-stop').disabled=false;
    try{
      let report=await post('/start');render(report);
      while(!paused&&report.state==='active'){
        if(report.busy)await new Promise(resolve=>setTimeout(resolve,1500));
        if(paused)break;
        report=await post('/weiter');render(report);
      }
    }catch(error){$('mail-source-progress').textContent=error.message;}
    finally{running=false;searchControls();$('mail-source-start').disabled=false;$('mail-source-stop').disabled=true;}
  };
  $('mail-source-stop').onclick=async()=>{viewRevision++;paused=true;$('mail-source-stop').disabled=true;try{render(await post('/pause'));}catch(error){$('mail-source-progress').textContent=error.message;}};
  $('mail-source-search').onsubmit=async event=>{
    event.preventDefault();if(running||!['paused','done'].includes(lastReport?.state))return;
    const revision=++viewRevision,query=$('mail-source-query').value.trim();$('mail-source-search-button').disabled=true;
    try{const report=await readStatus(query);if(revision===viewRevision&&!running)render(report);}
    catch(error){if(revision===viewRevision)$('mail-source-search-note').textContent=error.message;}
    finally{if(revision===viewRevision)searchControls();}
  };
  render(JSON.parse($('mail-source-initial').textContent));
  if(lastReport.state==='active')$('mail-source-stop').disabled=false;
})();
