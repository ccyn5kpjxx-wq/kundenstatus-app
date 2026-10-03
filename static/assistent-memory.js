'use strict';
// Personal history stays on the server. Nothing here uses browser storage.
window.AssistantMemory = class {
  constructor({request,onStatus=()=>{},onStop=()=>{},onReset=()=>{}}) {
    Object.assign(this,{request,onStatus,onStop,onReset});
    this.generation=null;this.epoch=0;this.queue=Promise.resolve();this.failed=0;this.pending=0;this.mutating=false;
  }
  notice(message) {this.onStatus(message);}
  async call(path,data,method='GET') {
    const abort=new AbortController();let timer;
    const timeout=new Promise((_,reject)=>{timer=setTimeout(()=>{abort.abort();reject(new Error('Das persönliche Gedächtnis ist gerade nicht erreichbar.'));},10000);});
    try{return await Promise.race([this.request(path,data,method,abort.signal),timeout]);}
    finally{clearTimeout(timer);}
  }
  async begin() {
    const epoch=this.epoch;
    // Previously completed turns must arrive before the next provider context.
    await this.queue;
    if(this.mutating||epoch!==this.epoch)throw new Error('Das Gedächtnis wird geändert. Bitte das Gespräch anschließend neu starten.');
    try {
      const data=await this.call('/gedaechtnis');
      if(this.mutating||epoch!==this.epoch)throw new Error('Gedächtnis wurde inzwischen geändert.');
      if(typeof data.generation!=='string'||!data.generation)throw new Error('Ungültiger Gedächtnisstand.');
      this.generation=data.generation;
      const session=window.crypto?.randomUUID?.();
      if(!session)throw new Error('Sichere Gesprächskennung fehlt.');
      this.notice(this.failed?'Einzelne Gesprächsbeiträge wurden nicht gespeichert. Das Gespräch bleibt nutzbar.':'Gesprächsverlauf wird in deinem persönlichen Konto gespeichert.');
      return {generation:data.generation,epoch,session,seen:new Set()};
    } catch (_) {
      const message='Gedächtnis konnte nicht geladen werden. Bitte erneut starten oder schreiben.';
      if(epoch===this.epoch)this.notice(message);
      throw new Error(message);
    }
  }
  record(handle,{role,text,eventId}) {
    if(!handle||handle.disabled||handle.epoch!==this.epoch||this.mutating)return;
    if(!['user','assistant'].includes(role)||typeof text!=='string'||!text.trim())return;
    if(text.length>8000||typeof eventId!=='string'||!eventId||eventId.length>150) {
      this.failed++;this.notice('Ein Gesprächsbeitrag konnte nicht gespeichert werden. Das Gespräch bleibt nutzbar.');return;
    }
    const id=handle.session+':'+role+':'+eventId;
    if(handle.seen.has(id))return;
    handle.seen.add(id);this.pending++;
    const payload={generation:handle.generation,event_id:id,role,text:text.trim()};
    this.queue=this.queue.then(async()=>{
      if(handle.disabled||handle.epoch!==this.epoch)return;
      try {
        await this.call('/gedaechtnis/gespraech',payload,'POST');
      } catch(error) {
        if(handle.epoch!==this.epoch)return;
        handle.disabled=true;
        this.failed++;
        if(error.status===409){++this.epoch;this.generation=null;this.onStop();this.notice('Das Gedächtnis wurde geändert. Das Gespräch wurde beendet; bitte neu starten.');}
        else this.notice('Einzelne Gesprächsbeiträge wurden nicht gespeichert. Bitte Verbindung prüfen. Das Gespräch bleibt nutzbar.');
      }
    }).catch(()=>{this.failed++;this.notice('Einzelne Gesprächsbeiträge wurden nicht gespeichert.');}).finally(()=>{this.pending--;});
    // No automatic retry: a delayed retry must never restore forgotten content.
    return this.queue;
  }
  async read(search='',before=null) {
    const params=new URLSearchParams();if(search.trim())params.set('suche',search.trim());if(before)params.set('before_id',String(before));
    const data=await this.call('/gedaechtnis'+(params.size?'?'+params:''));
    if(typeof data.generation!=='string'||!data.generation)throw new Error('Gedächtnis konnte nicht geladen werden.');
    return data;
  }
  async mutate(path,data,method,expectedGeneration=this.generation) {
    if(this.mutating)throw new Error('Bitte die laufende Änderung abwarten.');
    if(!expectedGeneration)throw new Error('Bitte das persönliche Gedächtnis zuerst neu laden.');
    this.mutating=true;this.onStop();
    try {
      await this.queue;
      const generation=expectedGeneration;
      ++this.epoch;this.generation=null;
      const result=await this.call(path,{...data,generation},method);
      if(typeof result.generation==='string')this.generation=result.generation;
      this.failed=0;this.onReset();this.notice('Persönliches Gedächtnis geändert. Ein neues Gespräch nutzt den aktualisierten Stand.');
      return result;
    } finally {this.mutating=false;}
  }
  mount(doc=document) {
    const $=id=>doc.getElementById(id),dialog=$('memory-dialog');
    if(!dialog||!$('memory-open'))return;
    let loadRun=0,next=null,editing=null,editingGeneration=null,loading=false;
    const message=text=>{$('memory-status').textContent=text;};
    const safely=fn=>async event=>{event?.preventDefault();try{await fn(event);}catch(error){message(error.message||'Gedächtnis nicht erreichbar.');}};
    const button=(label,handler)=>{const b=doc.createElement('button');b.type='button';b.className='secondary';b.textContent=label;b.onclick=safely(handler);return b;};
    const timestamp=value=>{
      if(typeof value!=='string'||!value)return '';
      // Cockpit timestamps use DD.MM.YYYY; native Date would silently swap
      // German month/day fields on several browsers (03.10 -> March 10).
      const german=/^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
      if(german){
        const [,day,month,year,hour,minute,second='00']=german;
        const date=new Date(+year,+month-1,+day,+hour,+minute,+second);
        return date.getFullYear()===+year&&date.getMonth()===+month-1&&date.getDate()===+day&&date.getHours()===+hour&&date.getMinutes()===+minute?date.toLocaleString('de-DE'):value;
      }
      if(!/^\d{4}-\d{2}-\d{2}(?:T| )/.test(value))return value;
      const date=new Date(value.replace(' ','T'));return Number.isNaN(date.getTime())?value:date.toLocaleString('de-DE');
    };
    const resetForm=()=>{editing=null;editingGeneration=null;$('memory-note-text').value='';$('memory-note-cancel').hidden=true;$('memory-note-save').textContent='Notiz speichern';};
    const change=async(path,data,method,expectedGeneration=dialog.dataset.generation)=>{
      ++loadRun;loading=false;
      const buttons=dialog.querySelectorAll('button');buttons.forEach(b=>b.disabled=true);
      try{await this.mutate(path,data,method,expectedGeneration);resetForm();await load();message(method==='DELETE'?(path==='/gedaechtnis'?'Dein persönliches Gedächtnis wurde gelöscht.':'Eintrag gelöscht.'):'Persönliche Notiz gespeichert.');}
      finally{buttons.forEach(b=>b.disabled=false);$('memory-more').hidden=!next;}
    };
    const confirm=(question)=>window.confirm(question);
    const load=async(more=false)=>{
      const run=++loadRun;loading=true;$('memory-more').disabled=true;
      message('Persönliches Gedächtnis wird geladen …');
      try {
        const data=await this.read($('memory-search-text').value,more?next:null);
        if(run!==loadRun)return;
        if(more&&dialog.dataset.generation!==data.generation){await load();return;}
        dialog.dataset.generation=data.generation;
        this.generation=data.generation;
        if(!more)$('memory-entries').replaceChildren();
        $('memory-notes').replaceChildren();
        for(const item of data.notes||[]) {
          const card=doc.createElement('article'),text=doc.createElement('p'),meta=doc.createElement('small');
          card.className='memory-card';text.textContent=item.text;meta.textContent=timestamp(item.updated_at||item.zeit||item.created_at);
          card.append(text,meta,button('Bearbeiten',()=>{editing=item.id;editingGeneration=data.generation;$('memory-note-text').value=item.text;$('memory-note-cancel').hidden=false;$('memory-note-save').textContent='Änderung speichern';$('memory-note-text').focus();}),button('Notiz löschen',async()=>{if(confirm('Diese persönliche Notiz löschen?'))await change('/gedaechtnis/notiz/'+encodeURIComponent(item.id),{},'DELETE',data.generation);}));
          $('memory-notes').append(card);
        }
        for(const item of data.entries||[]) {
          const card=doc.createElement('article'),text=doc.createElement('p'),meta=doc.createElement('small');
          card.className='memory-card';text.textContent=item.text;meta.textContent=(item.role==='user'?'Du':'KI')+' · '+(item.source==='voice'?'Gespräch':'Text')+' · '+timestamp(item.zeit);
          card.append(meta,text,button('Beitrag löschen',async()=>{if(confirm('Diesen Gesprächsbeitrag aus deinem Gedächtnis löschen?'))await change('/gedaechtnis/eintrag/'+encodeURIComponent(item.id),{},'DELETE',data.generation);}));$('memory-entries').append(card);
        }
        next=data.next_before_id||null;$('memory-more').hidden=!next;
        message((data.entries||[]).length?'Nur dein persönlicher Verlauf. Frühere Aussagen können inzwischen überholt sein.':'Keine passenden Gesprächsbeiträge gespeichert.');
      } finally{if(run===loadRun){loading=false;$('memory-more').disabled=false;}}
    };
    this.open=safely(async()=>{if(!dialog.open)dialog.showModal();await load();});
    $('memory-open').onclick=this.open;
    $('memory-close').onclick=()=>dialog.close();
    $('memory-search').onsubmit=safely(()=>load());
    $('memory-more').onclick=safely(()=>{if(!loading&&next)return load(true);});
    $('memory-reload').onclick=safely(()=>load());
    $('memory-note-cancel').onclick=resetForm;
    $('memory-note-form').onsubmit=safely(async()=>{const text=$('memory-note-text').value.trim();if(!text||text.length>1000)throw new Error('Bitte eine Notiz mit höchstens 1.000 Zeichen eingeben.');await change('/gedaechtnis/notiz',{text,...(editing?{id:editing}:{})},'POST',editingGeneration||dialog.dataset.generation);});
    $('memory-clear').onclick=safely(async()=>{if(confirm('Alle persönlichen Gesprächsbeiträge und Notizen endgültig löschen? Das laufende Gespräch wird beendet. Aufträge und bestätigte Aktionen bleiben erhalten.'))await change('/gedaechtnis',{},'DELETE');});
  }
};
