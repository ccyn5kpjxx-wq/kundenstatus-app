'use strict';
(() => {
  const $ = id => document.getElementById(id), base = '/werkstatt/assistent';
  const readOnly = $('assistant').dataset.readOnly === 'true';
  const statusEnabled = $('assistant').dataset.statusEnabled === 'true';
  const purchaseEnabled = $('assistant').dataset.purchaseEnabled === 'true';
  const documentsEnabled = $('assistant').dataset.documentsEnabled === 'true';
  const offersEnabled = $('assistant').dataset.offersEnabled === 'true';
  const personalEnabled = $('assistant').dataset.personalEnabled === 'true';
  const personalKinds = ['urlaub','arbeitszeit'];
  const mailKinds = ['lieferantenanfrage','kundenangebot'];
  const workflowKinds = ['farbe','kontakt','auftrag_neu','datei',...mailKinds];
  const actionAllowed = kind => personalKinds.includes(kind)?personalEnabled:mailKinds.includes(kind)?offersEnabled:workflowKinds.includes(kind)?documentsEnabled:kind==='status'?statusEnabled:kind==='bestellung'?purchaseEnabled:!readOnly&&['notiz','einkauf','anfrage'].includes(kind);
  if(readOnly)document.querySelectorAll('[data-write-section]').forEach(el=>{el.hidden=true;el.querySelectorAll('input,button,textarea,select').forEach(field=>field.disabled=true);});
  const token = document.querySelector('meta[name="csrf-token"]').content;
  let current=null, photoOrder=null, photo=null, stream=null, recorder=null, micStream=null;
  let busy=false, audioUrl=null, photoUrl=null, pending=null, cancelPlayback=null;
  let playbackGeneration=0, busyGeneration=null, recordingGeneration=0, micPending=false;
  let resumeOutput=null, helpTimer=null, cancelMicrophone=null, recordingTimer=null;
  function voicePhase(phase){
    clearTimeout(helpTimer);helpTimer=null;$('assistant').dataset.voicePhase=phase;
    if(phase==='microphone')helpTimer=setTimeout(()=>{if($('assistant').dataset.voicePhase==='microphone')$('voice-help').open=true;},8000);
    else if(phase==='connected')$('voice-help').open=false;
  }
  // Audio is optional: a missing asset must not prevent text/menu initialization.
  const audioFailures=new Set();
  let outputMeter=null, voiceAvailable=false, realtimeAvailable=false, audioRetryUsed=false;
  const unavailableVoice=()=>({active:false,generation:0,stop(){this.active=false;this.generation++;},async start(){throw new Error('Diese Sprachfunktion wurde nicht geladen. Bitte „Sprachfunktionen erneut laden“ wählen oder eine Nachricht schreiben.');},async resumePlayback(){return false;}});
  let voice=unavailableVoice(), realtime=unavailableVoice();
  function audioLevel(level){try{window.AssistantAvatar?.instance?.animation?.setAudioLevel?.(level);}catch{}}
  function meter(method,...args){
    try{const result=outputMeter?.[method]?.(...args);if(result?.catch)result.catch(()=>{outputMeter=null;});}
    catch{outputMeter=null;}
  }
  const unlockOutput=()=>meter('unlock');
  const normalized = text => text.toLocaleLowerCase('de-DE').replace(/[^\p{L}\p{N}\s]/gu,'').trim();
  const status = text => {$('status').textContent=text;};
  const state = (name,text) => {$('avatar').dataset.state=name;$('avatar-status').textContent=text;if(name!=='speaking')audioLevel(0);};
  async function api(path,data,raw=false,signal) {
    const options={headers:{'X-CSRF-Token':token},signal};
    if(data!==undefined){options.method='POST';if(data instanceof FormData)options.body=data;else{options.headers['Content-Type']='application/json';options.body=JSON.stringify(data);}}
    const response=await fetch(base+path,options);
    if(!response.ok){let message='Anfrage fehlgeschlagen. Bitte Anmeldung und Verbindung prüfen.';try{message=(await response.json()).error||message;}catch{}throw new Error(message);}
    return raw?response.blob():response.json();
  }
  const stalePlaybackError = error => error.playbackGeneration!==undefined&&error.playbackGeneration!==playbackGeneration;
  function reportError(error){if(stalePlaybackError(error))return;if(error.phase){voicePhase(error.phase);clearTimeout(helpTimer);if(error.phase==='microphone')$('voice-help').open=true;}status(error.message);state('error',error.message);}
  function safe(fn){return async event=>{event?.preventDefault();try{await fn(event);}catch(e){reportError(e);}};}
  function log(who,text){const p=document.createElement('p');p.textContent=who+': '+text;$('conversation').append(p);}
  function showOrder(data){current=data;if(typeof CustomEvent!=='undefined')document.dispatchEvent(new CustomEvent('assistant-order-changed',{detail:data}));$('order-form').elements.id.value=data.id;$('active-order').textContent=`Auftrag ${data.id} · ${data.kennzeichen||'ohne Kennzeichen'}`;const pre=document.createElement('pre');pre.textContent=`Auftrag ${data.id} · ${data.kennzeichen||'ohne Kennzeichen'} · ${data.fahrzeug}\nAngebotsstatus: ${data.angebot_status}\nVersicherungsfreigabe: ${data.versicherung_freigabe_status}\nAngebotstext: ${data.werkstatt_angebot_text||'nicht hinterlegt'}\nBeschreibung (keine Freigabe): ${data.beschreibung||'–'}\nTeile-Aktenstand: ${JSON.stringify(data.teile,null,2)}\n${data.hinweis}`;$('order').replaceChildren(pre);if(data.quelle){pre.textContent=`Auftrag ${data.id} · ${data.fahrzeug} · ${data.kennzeichen||'ohne Kennzeichen'}\nFarbcode: ${data.farbcode||'nicht hinterlegt'} · Farbton: ${data.farbton||'nicht hinterlegt'}${data.farbton_2?' · Zweiter Farbton: '+data.farbton_2:''}\nStatus im Cockpit: ${data.status}\nArbeiten / Beschreibung:\n${data.beschreibung||'Keine Angaben in der Übersicht.'}\nTermine: ${data.termine||('Annahme: '+(data.annahme_datum||'offen')+' · Fertig: '+(data.fertig_datum||'offen')+' · Rückgabe: '+(data.abholtermin||'offen'))}\n${data.modus==='lesestand'?'Freigaben und Teilebestand: im Lesestand nicht erhoben.':'Angebotsstatus: '+(data.angebot_status||'unbekannt')+' · Versicherungsfreigabe: '+(data.versicherung_freigabe_status||'unbekannt')+'\nDokumente: '+(data.dokumente||[]).map(d=>d.id+' · '+d.original_name).join(', ')}`;const note=document.createElement('p');note.textContent=(data.modus==='live'?'Cockpit, abgerufen: ':'Cockpit-Lesestand: ')+new Date(data.stand).toLocaleString('de-DE')+' · '+(data.modus==='live'?'Direkter Portalabruf':data.detail_gelesen?'Beschreibung aus Auftragsdetails':'Listenübersicht, möglicherweise gekürzt');const link=document.createElement('a');link.href=data.quelle;link.target='_blank';link.rel='noopener';link.textContent='Originalauftrag im Cockpit öffnen';$('order').prepend(note);if($('assistant').dataset.admin==='true')$('order').prepend(link);if(data.uebersicht){const summary=document.createElement('p');summary.textContent='Cockpit-Kurzfassung: '+data.uebersicht;$('order').append(summary);}}}
  document.addEventListener('assistant-open-order',safe(async event=>{stopVoice();pending=null;showOrder(await api('/auftrag/'+Number(event.detail.id)));$('order').scrollIntoView({behavior:'smooth'});}));
  function clearOrder(){current=null;$('order-form').elements.id.value='';$('active-order').textContent='Auftrag auswählen';$('order').replaceChildren();if(typeof CustomEvent!=='undefined')document.dispatchEvent(new CustomEvent('assistant-order-changed',{detail:null}));}
  function needOrder(){if(!current)throw new Error('Zuerst den richtigen Auftrag aufrufen.');return current.id;}
  function stopCamera(){stream?.getTracks().forEach(t=>t.stop());stream=null;$('video').srcObject=null;$('video').hidden=true;$('capture').disabled=true;}
  async function camera(){needOrder();if(!window.isSecureContext||!navigator.mediaDevices)throw new Error('Kamera benötigt HTTPS und Browserfreigabe.');stopCamera();const result=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}},audio:false});if(document.hidden){result.getTracks().forEach(t=>t.stop());return;}stream=result;$('video').srcObject=stream;$('video').hidden=false;await $('video').play();$('capture').disabled=false;}
  function preview(blob){photo=blob;photoOrder={...current};if(photoUrl)URL.revokeObjectURL(photoUrl);photoUrl=URL.createObjectURL(blob);$('photo-preview').src=photoUrl;$('photo-preview').hidden=false;$('photo-save').disabled=false;status(`Aufnahme prüfen: Auftrag ${photoOrder.id} · ${photoOrder.kennzeichen}.`);}
  async function capture(){needOrder();const video=$('video');if(!stream||!video.videoWidth)throw new Error('Kamerabild noch nicht bereit. Sage gleich erneut: Foto aufnehmen.');const canvas=document.createElement('canvas');const scale=Math.min(1,1600/video.videoWidth);canvas.width=video.videoWidth*scale;canvas.height=video.videoHeight*scale;canvas.getContext('2d').drawImage(video,0,0,canvas.width,canvas.height);const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/jpeg',0.88));if(!blob)throw new Error('Aufnahme fehlgeschlagen.');preview(blob);}
  async function savePhoto(){if(!photo||!photoOrder)throw new Error('Foto fehlt.');if(needOrder()!==photoOrder.id)throw new Error('Auftrag wurde gewechselt. Ursprünglichen Auftrag wieder aufrufen oder neues Foto aufnehmen.');$('photo-save').disabled=true;try{const form=new FormData();form.append('foto',photo,'aufnahme.jpg');form.append('analyse',$('vision').checked?'1':'0');const r=await api('/foto/'+photoOrder.id,form);photo=null;pending=null;$('photo-preview').hidden=true;stopCamera();await refresh();return r.hinweis+(r.sichtung?' KI-Sichtung: '+r.sichtung:'');}catch(e){$('photo-save').disabled=false;throw e;}}
  function stopPlayback(){
    ++playbackGeneration;
    resumeOutput=null;$('audio-resume').hidden=true;
    const cancel=cancelPlayback;cancelPlayback=null;cancel?.();
    meter('clear');
  }
  function say(text){
    stopPlayback();
    const generation=playbackGeneration, abort=new AbortController(), audio=$('speech');
    const current=()=>generation===playbackGeneration&&!abort.signal.aborted;
    let timer=null, settled=false, url=null, playAttempt=0;
    const listeners=[];
    state('thinking','Antwort wird vorbereitet.');voiceButtons(true);
    return new Promise((resolve,reject)=>{
      const done=(error,cancelled=false)=>{
        if(settled)return;settled=true;clearTimeout(timer);
        if(current())voiceButtons(realtime.active||voice.active);
        if(error){error.playbackGeneration=generation;reject(error);}else resolve(!cancelled);
      };
      cancelPlayback=()=>{
        abort.abort();clearTimeout(timer);
        for(const [name,handler] of listeners)audio.removeEventListener(name,handler);
        // The audio element is shared with WebRTC; never pause its newer stream.
        if(url&&audio.srcObject===null&&audio.src===url){audio.pause();audio.removeAttribute('src');audio.load();}
        if(url){URL.revokeObjectURL(url);if(audioUrl===url)audioUrl=null;}
        done(null,true);
      };
      const ownsAudio=()=>current()&&audio.srcObject===null&&audio.src===url;
      const fail=message=>{if(!current())return;resumeOutput=null;$('audio-resume').hidden=true;state('error',message);done(new Error(message));};
      const listen=(name,handler)=>{
        const guarded=()=>{if(ownsAudio())handler();};
        listeners.push([name,guarded]);audio.addEventListener(name,guarded);
      };
      api('/sprechen',{text},true,abort.signal).then(blob=>{
        if(!current())return;
        if(document.hidden){stopPlayback();return;}
        if(audioUrl)URL.revokeObjectURL(audioUrl);
        url=audioUrl=URL.createObjectURL(blob);audio.srcObject=null;audio.src=url;audio.hidden=false;
        meter('useBlob',blob,url);
        // Keep these listeners for manual replay, until stop or the next output.
        listen('playing',()=>{resumeOutput=null;$('audio-resume').hidden=true;state('speaking','Ich spreche. Das Mikrofon nimmt gerade nicht auf.');});
        listen('pause',()=>state('idle',audio.ended?'Bereit für deine nächste Nachricht.':'Sprachausgabe pausiert.'));
        listen('waiting',()=>state('thinking','Sprachausgabe lädt.'));
        listen('ended',()=>{state('idle','Bereit für deine nächste Nachricht.');done();});
        listen('error',()=>fail('Audio konnte nicht abgespielt werden.'));
        const play=async()=>{
          const attempt=++playAttempt;
          clearTimeout(timer);timer=setTimeout(()=>{if(ownsAudio()){audio.pause();fail('Sprachausgabe unterbrochen. Bitte erneut starten.');}},90000);
          try{await audio.play();if(!ownsAudio()||attempt!==playAttempt)return false;resumeOutput=null;$('audio-resume').hidden=true;return true;}
          catch(error){
            if(!ownsAudio()||attempt!==playAttempt)return false;
            if(error.name==='NotAllowedError'){
              clearTimeout(timer);timer=null;
              resumeOutput=play;$('audio-resume').hidden=false;
              state('idle','Antwort ist bereit. Tippe auf „Ton einschalten“, um sie zu hören.');
            }else fail('Audio konnte nicht abgespielt werden. Bitte erneut versuchen.');
            return false;
          }
        };
        void play();
      }).catch(error=>{if(current()){state('error',error.message);done(error);}});
    });
  }
  async function prepareReadback(item){
    pending=null;
    if(!actionAllowed(item.art))throw new Error('Für diese Aktion fehlt die aktuelle Freigabe.');
    if(!voiceAvailable){showActions();status('Sprachbestätigung ist gerade nicht verfügbar. Bitte den Vorschlag im Menü prüfen und dort ausdrücklich bestätigen.');return false;}
    if(workflowKinds.includes(item.art))showActions();
    if(item.daten?.missing_fields?.length){status('Die Mail ist noch unvollständig. Bitte die fehlenden Angaben im Vorschlag ergänzen.');return false;}
    const requestedGeneration=playbackGeneration;
    const challenge=await api('/vorlesen/'+item.id,{});
    if(requestedGeneration!==playbackGeneration)return false;
    log('Bestätigung',challenge.text);
    const playback=say(challenge.text), generation=playbackGeneration;
    if(!await playback||generation!==playbackGeneration)return false;
    pending={type:'action',...challenge};
    voiceButtons(false);
    return true;
  }
  const euros = value => Number.isSafeInteger(value)&&value>=0?(value/100).toLocaleString('de-DE',{style:'currency',currency:'EUR'}):'nicht geklärt';
  function confirmationResult(result){
    if(result.ok!==true)throw new Error(result.hinweis||'Die Aktion wurde nicht bestätigt.');
    if(result.neuer_auftrag)window.AssistantWorkflow?.created();
    if(result.auftrag&&(result.neuer_auftrag||!current||Number(current.id)===Number(result.auftrag.id)))showOrder(result.auftrag);
    const message=[result.hinweis,result.versandstatus?.message].filter((text,index,items)=>typeof text==='string'&&text&&items.indexOf(text)===index).join(' ');
    return message||'Serverantwort erhalten. Bitte den Aktionsstatus prüfen.';
  }
  async function refresh(){
    const items=await api('/aktionen');$('actions').replaceChildren();
    for(const item of items){if(!actionAllowed(item.art))continue;const div=document.createElement('div');div.className='action';const pre=document.createElement('pre');const data=item.daten||{};let details=data.text||'';
      if(['einkauf','anfrage','bestellung'].includes(item.art)){
        const order=data.versand||{};
        details=`Lieferant: ${data.lieferant||'nicht geklärt'}\nArtikel: ${data.teilenummer||'nicht geklärt'} · ${data.bezeichnung||'nicht geklärt'}\nMenge: ${data.menge??'nicht geklärt'} ${order.unit||'Stück'}\n`;
        if(item.art==='bestellung')details+=`Variante: ${order.variant||'nicht geklärt'}\nEmpfänger: ${order.recipient||'nicht geklärt'}\n`;
        details+=item.art==='anfrage'?'Preis und Verfügbarkeit werden angefragt. Keine Bestellung.':`Stückpreis brutto: ${euros(data.stueckpreis_brutto_cent)} · Versand: ${euros(data.versand_brutto_cent)} · Nebenkosten: ${euros(data.nebenkosten_brutto_cent)}\nGesamt brutto: ${euros(data.gesamt_cent)}\n`;
        if(item.art==='bestellung')details+=`Kostenrahmen: ${euros(order.max_total_cents)}\nPreise: brutto in EUR.\nPreisquelle: ${data.preisquelle||order.price_source||'nicht geklärt'}\n${order.urgent===true?'Dringend: Versand nach Bestätigung.':order.urgent===false?'Sammelversand: Montag um 12:00 Uhr (Europe/Berlin).':'Dringlichkeit nicht geklärt.'}`;
        else if(item.art==='einkauf')details+=`${data.preisquelle||'Preisquelle nicht geklärt'}\nKein Bestellversand.`;
      }
      if(mailKinds.includes(item.art))details=`Empfänger: ${data.mail?.recipient||'fehlt'}\nAbsender: ${data.mail?.from||'fehlt'}\nBetreff: ${data.mail?.subject||'fehlt'}\n${item.art==='kundenangebot'?'Kundenpreis gesamt brutto: '+euros(data.gross_total_cents)+'\n':''}\n${data.mail?.body||''}\n\nAnhänge: ${(data.attachments||[]).map(a=>a.name||a.original_name||a.filename).join(', ')||'keine'}\n${data.missing_fields?.length?'Noch offen: '+data.missing_fields.map(k=>({recipient:'Empfänger',gross_total_cents:'Kundenpreis brutto',sender:'Absender',configuration:'Postfach'}[k]||k)).join(', '):''}`;
      const labels={status:'Statusänderung',bestellung:'Verbindliche Bestellung',notiz:'Interne Notiz',einkauf:'Einkaufsentwurf',anfrage:'Teileanfrage',farbe:'Farbdaten',kontakt:'Kundenkontakt',auftrag_neu:'Neuer Auftrag',datei:'Bild / Unterlage zuordnen',lieferantenanfrage:'Unverbindliche Lieferantenanfrage',kundenangebot:'Angebot an Kunden',urlaub:'Mein Urlaubsantrag',arbeitszeit:'Meine Arbeitszeit'};
      pre.textContent=`${item.auftrag_id?'Auftrag '+item.auftrag_id:item.art==='auftrag_neu'?'Neue Auftragsnummer beim Speichern':personalKinds.includes(item.art)?'Mein Mitarbeiterkonto':'Werkstattmaterial'} · ${labels[item.art]} · ${item.status==='vorschlag'?'Zur Prüfung':item.status}\n${details}`;div.append(pre);
      if(item.versandstatus?.message){const message=document.createElement('p');message.textContent=item.versandstatus.message;div.append(message);}
      if(item.status==='vorschlag'){
        const button=document.createElement('button');button.textContent=item.art==='urlaub'?'Urlaub verbindlich beantragen':item.art==='arbeitszeit'?'Zeitstempel jetzt erfassen':mailKinds.includes(item.art)?'Geprüfte E-Mail senden':workflowKinds.includes(item.art)?'Geprüft speichern':item.art==='status'?'Status ändern':item.art==='bestellung'?'Verbindlich bestellen':item.art==='notiz'?'Geprüfte Notiz speichern':'Geprüften Entwurf intern freigeben';
        button.disabled=Boolean(data.missing_fields?.length);
        button.onclick=safe(async()=>{button.disabled=true;stopVoice();try{const r=await api('/bestaetigen/'+item.id,{});pending=null;status(confirmationResult(r));await refresh();}finally{button.disabled=Boolean(data.missing_fields?.length);}});div.append(button);
        const read=document.createElement('button');read.className='secondary';read.textContent='Vorlesen & per Sprache bestätigen';read.dataset.voiceConfirmation='true';read.disabled=!voiceAvailable||$('assistant').dataset.ready!=='true';read.onclick=safe(async()=>{unlockOutput();stopVoice();if(await prepareReadback(item))status('Vorgelesen. Gespräch starten und die genannte Bestätigung sprechen.');});div.append(read);
      }
      if(item.art==='bestellung'&&item.status!=='vorschlag'&&['sent','copy_pending'].includes(item.versandstatus?.state)){
        const again=document.createElement('button');again.className='secondary';again.textContent='Erneut vorbereiten';let requestId=null;
        again.onclick=safe(async()=>{
          if(again.disabled)return;again.disabled=true;stopVoice();pending=null;
          try{
            if(!requestId){
              if(window.crypto?.randomUUID)requestId=window.crypto.randomUUID();
              else if(window.crypto?.getRandomValues)requestId=Array.from(window.crypto.getRandomValues(new Uint8Array(16)),byte=>byte.toString(16).padStart(2,'0')).join('');
              else throw new Error('Sichere Vorgangsnummer nicht verfügbar. Bitte die Seite neu öffnen.');
            }
            await api('/erneut-vorbereiten/'+item.id,{request_id:requestId});await refresh();
            status('Neue Bestellung vorbereitet, noch nicht ausgelöst. Bitte den neuen Vorschlag prüfen und erneut ausdrücklich bestätigen.');
          }finally{again.disabled=false;}
        });div.append(again);
        const note=document.createElement('p');note.textContent='Erstellt einen neuen Bestellvorschlag. Versand erst nach erneuter Bestätigung.';div.append(note);
      }
      if(['anfrage','einkauf'].includes(item.art)){const a=document.createElement('a');a.href=base+'/email/'+item.id;a.textContent='E-Mail-Anfrage als Entwurf öffnen';a.className='email-download';div.append(a);}
      $('actions').append(div);
    }
  }
  async function chat(text,spoken=false){
    if(busy)throw new Error('Bitte laufende Antwort abwarten.');
    const generation=playbackGeneration, currentChat=()=>generation===playbackGeneration&&!document.hidden;
    busy=true;busyGeneration=generation;log('Du',text);state('thinking','Ich prüfe deine Nachricht.');
    try{
      const clean=normalized(text);
      if(['gespräch beenden','sprachmodus beenden','stop'].includes(clean)){stopVoice();return;}
      if(clean==='abbrechen'){pending=null;photo=null;$('photo-preview').hidden=true;$('photo-save').disabled=true;stopCamera();await say('Vorschlag nicht bestätigt. Du kannst einen neuen Auftrag nennen.');return;}
      if(pending){
        if(clean!==normalized(pending.phrase)){await say('Nicht bestätigt. Sage genau: '+pending.phrase+'. Oder Abbrechen.');return;}
        let message;
        if(pending.type==='photo')message=await savePhoto();
        else {const r=await api('/sprache-bestaetigen',{nonce:pending.nonce,text});message=confirmationResult(r);pending=null;await refresh();}
        if(!currentChat())return;
        log('KI',message);status(message);await say(message);return;
      }
      if(clean==='foto aufnehmen'&&stream){await capture();if(!currentChat())return;const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;if(await say(`Aufnahme für Auftrag ${photoOrder.id}, ${photoOrder.kennzeichen}. Bitte Bild und Zuordnung prüfen. Sage: ${phrase}. Oder Abbrechen.`))pending={type:'photo',phrase};return;}
      const r=await api('/dialog',{text,auftrag_id:current?.id});
      if(!currentChat()||(spoken&&!voice.active))return;
      log('KI',r.text);
      let proposal=null,photoReady=false;
      for(const event of r.events){if(!currentChat())return;if(event.type==='materialfoto'){stopVoice();window.AssistantMaterialPhoto?.open();return;}if(event.type==='unterlage'){stopVoice();if(event.data)showOrder(event.data);else clearOrder();window.AssistantWorkflow?.openUpload(event.data);return;}if(event.type==='auftrag')showOrder(event.data);if(event.type==='vorschlag'&&event.data.status==='vorschlag')proposal=event.data;if(event.type==='kamera'){showOrder(event.data);await camera();if(!currentChat())return;await capture();photoReady=true;}}
      if(!currentChat())return;
      await refresh();if(!currentChat())return;status('Antwort erhalten.');
      if(proposal){showActions();if(spoken||$('read-aloud').checked)await prepareReadback(proposal);else status('Vorschlag ist bereit. Bitte vollständig prüfen und bestätigen.');}
      else if(photoReady){const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;if(await say(`Foto für Auftrag ${photoOrder.id}, Kennzeichen ${photoOrder.kennzeichen}. Prüfe Bild und Zuordnung. Sage: ${phrase}. Oder Abbrechen.`))pending={type:'photo',phrase};}
      else if(spoken||$('read-aloud').checked)await say(r.text);
      else state('idle','Antwort erhalten.');
    }catch(error){
      // A stopped request may still finish or fail on the server. Its UI is obsolete.
      if(currentChat()||error.playbackGeneration!==undefined)throw error;
    }finally{if(busyGeneration===generation){busy=false;busyGeneration=null;}}
  }
  function voiceAvailability(){$('voice-mode').disabled=$('assistant').dataset.ready!=='true'||!(pending?voiceAvailable:realtimeAvailable);}
  function voiceButtons(active){$('voice-mode').hidden=active;voiceAvailability();$('voice-stop').hidden=!active;$('record').disabled=active||$('assistant').dataset.ready!=='true';}
  const voiceOptions={
    onState:(name,text)=>{state(name,text);voiceButtons(voice.active);},
    onError:reportError,
    onSegment:async blob=>{
      const generation=voice.generation;
      try{
        const form=new FormData();form.append('audio',blob,blob.type.includes('mp4')?'sprache.mp4':'sprache.webm');
        const r=await api('/audio',form);if(!voice.active||generation!==voice.generation)return;
        await chat(r.text,true);
        if(voice.active&&generation===voice.generation&&!pending){voice.stop();stopPlayback();if(realtimeAvailable)await realtime.start(current?.id);}
      }catch(error){if(voice.active&&generation===voice.generation&&!stalePlaybackError(error))throw error;}
    }
  };
  const realtimeOptions={api,audio:$('speech'),
    onState:(name,text,detail)=>{state(name,text);if(detail?.phase)voicePhase(detail.phase);voiceButtons(realtime.active||voice.active);if(!realtime.active||name==='speaking')$('audio-resume').hidden=true;},
    onRemoteStream:stream=>{if(stream)meter('useStream',stream);else meter('clear');},
    onPlaybackBlocked:()=>{$('audio-resume').hidden=false;},
    onError:reportError,onText:log,
    onEvent:async event=>{
      if(event.type==='auftrag'){showOrder(event.data);return;}
      if(event.type==='materialfoto'){stopVoice();window.AssistantMaterialPhoto?.open();return;}if(event.type==='unterlage'){stopVoice();if(event.data)showOrder(event.data);else clearOrder();window.AssistantWorkflow?.openUpload(event.data);return;}
      // Existing deterministic readback and exact confirmation stay outside model control.
      realtime.stop();await refresh();
      if(event.type==='vorschlag'){
        if(!await prepareReadback(event.data))return;
        status('Prüfmodus: genannte Bestätigung sprechen oder Abbrechen.');
        await voice.start();
      }else if(event.type==='kamera'){
        showOrder(event.data);await camera();await capture();
        if(!voiceAvailable){status('Foto aufgenommen. Bitte das Bild im Menü prüfen und dort ausdrücklich speichern.');return;}
        const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;
        if(!await say(`Foto für Auftrag ${photoOrder.id}, Kennzeichen ${photoOrder.kennzeichen}. Prüfe Bild und Zuordnung. Sage: ${phrase}. Oder Abbrechen.`))return;
        pending={type:'photo',phrase};await voice.start();
      }
    }
  };
  const audioComponents=[
    {key:'realtime',name:'Echtzeitgespräch',global:'AssistantRealtime',file:'assistent-realtime.js',methods:['start','stop','resumePlayback'],options:realtimeOptions,install:value=>{realtime=value;realtimeAvailable=true;}},
    {key:'voice',name:'Sprachbestätigung',global:'AssistantVoiceMode',file:'assistent-voice.js',methods:['start','stop'],options:voiceOptions,install:value=>{voice=value;voiceAvailable=true;}},
    {key:'meter',name:'Mundbewegung',global:'OutputAudioMeter',file:'assistent-audio-meter.js',methods:['unlock','clear','useBlob','useStream','suspend'],options:{window,audio:$('speech'),onLevel:audioLevel},install:value=>{outputMeter=value;}}
  ];
  function initializeAudio(component){
    try{
      const Constructor=window[component.global];
      if(typeof Constructor!=='function')throw new Error('Missing audio component');
      const instance=new Constructor(component.options);
      if(component.methods.some(method=>typeof instance[method]!=='function'))throw new Error('Incomplete audio component');
      component.install(instance);audioFailures.delete(component.key);
    }catch{audioFailures.add(component.key);}
  }
  function audioHelp(retried=false){
    const box=$('audio-components-help'),message=$('audio-components-message'),retry=$('audio-components-retry');
    if(box)box.hidden=!audioFailures.size;
    if(message){const names=audioComponents.filter(component=>audioFailures.has(component.key)).map(component=>component.name).join(', ');message.textContent=audioFailures.size?`${names} konnte nicht geladen werden. Texteingabe und Menü bleiben verfügbar. ${retried?'Der Nachladeversuch ist beendet. Bitte später die Seite neu öffnen; ungespeicherte Angaben vorher sichern.':'Du kannst die fehlenden Sprachfunktionen einmal erneut laden. Es wird dabei kein Gespräch gestartet.'}`:'';}
    if(retry){retry.hidden=!audioFailures.size;retry.disabled=audioRetryUsed;}
    // Recovery must not hide Stop or unlock recording during an existing readback.
    voiceAvailability();
    document.querySelectorAll('[data-voice-confirmation]').forEach(button=>{button.disabled=!voiceAvailable||$('assistant').dataset.ready!=='true';});
  }
  function loadAudioScript(component){
    return new Promise(resolve=>{
      const script=document.createElement('script');let settled=false;
      const finish=ok=>{if(settled)return;settled=true;clearTimeout(timer);script.onload=null;script.onerror=null;if(!ok)script.remove();resolve(ok);};
      const timer=setTimeout(()=>finish(false),15000);
      // Fixed local assets only; no supplier/model/user-controlled script URL.
      script.src='/static/'+component.file+'?audio-retry='+Date.now();script.async=true;
      script.onload=()=>finish(true);script.onerror=()=>finish(false);
      document.head.append(script);
    });
  }
  audioComponents.forEach(initializeAudio);
  voiceButtons(false);
  audioHelp();
  if($('audio-components-retry'))$('audio-components-retry').onclick=safe(async()=>{
    if(audioRetryUsed)return;audioRetryUsed=true;audioHelp();
    $('audio-components-message').textContent='Fehlende Sprachfunktionen werden einmal nachgeladen. Deine Eingaben bleiben erhalten.';
    await Promise.all(audioComponents.filter(component=>audioFailures.has(component.key)).map(async component=>{
      // Do not redeclare an already evaluated script after a constructor error.
      if(typeof window[component.global]!=='function'&&!await loadAudioScript(component))return;
      initializeAudio(component);
    }));
    audioHelp(true);
    if(!audioFailures.size)status('Sprachfunktionen geladen. Du kannst das Gespräch selbst starten.');
  });
  function stopRecording(){++recordingGeneration;micPending=false;cancelMicrophone?.();cancelMicrophone=null;clearTimeout(recordingTimer);recordingTimer=null;if(recorder){recorder.onstop=null;if(recorder.state==='recording')recorder.stop();recorder=null;}micStream?.getTracks().forEach(t=>t.stop());micStream=null;$('record').textContent='Mikrofon starten';}
  function stopVoice(){stopPlayback();stopRecording();voicePhase('idle');busy=false;busyGeneration=null;realtime.stop();voice.stop();$('speech').pause();$('audio-resume').hidden=true;voiceButtons(false);state('idle','Sprachmodus beendet.');}
  $('audio-resume').onclick=safe(async()=>{unlockOutput();$('audio-resume').disabled=true;try{const retry=resumeOutput;if(await (retry?retry():realtime.resumePlayback()))$('audio-resume').hidden=true;}finally{$('audio-resume').disabled=false;}});
  $('voice-mode').onclick=safe(async()=>{unlockOutput();if(busy)throw new Error('Bitte Antwort abwarten.');if(micPending||recorder?.state==='recording')throw new Error('Einzelaufnahme zuerst beenden.');stopPlayback();if(pending)await voice.start();else await realtime.start(current?.id);});
  $('voice-stop').onclick=()=>{stopVoice();stopCamera();};
  $('write-message').onclick=()=>{stopVoice();const menu=$('assistant-menu');if(!menu.open)menu.showModal();menu.querySelector('details').open=true;$('chat').scrollIntoView({block:'center'});$('chat').elements.text.focus();};
  $('chat').onsubmit=safe(async()=>{const input=$('chat').elements.text,text=input.value.trim();if(!text)return;unlockOutput();stopVoice();input.value='';await chat(text);});
  $('order-form').onsubmit=safe(async()=>{stopVoice();pending=null;showOrder(await api('/auftrag/'+Number($('order-form').elements.id.value)));});
  if($('status-change'))$('status-change').onsubmit=safe(async()=>{
    if(!statusEnabled)throw new Error('Statusänderungen sind für diesen Zugang nicht freigegeben.');
    stopVoice();pending=null;const button=$('status-change').querySelector('button');button.disabled=true;
    try{await api('/vorschlag',{art:'status',auftrag_id:needOrder(),aktion:$('status-change').elements.aktion.value});await refresh();status('Statusänderung vorbereitet. Bitte den Vorschlag prüfen und bestätigen.');$('actions-section').scrollIntoView({block:'center'});}finally{button.disabled=false;}
  });
  $('profile').onsubmit=safe(async()=>{stopVoice();const data=Object.fromEntries(new FormData($('profile')));await api('/profil',data);$('assistant').dataset.avatar=data.avatar;$('avatar-name').textContent=data.name;status('Persönlichkeit gespeichert.');});
  $('note').onsubmit=safe(async()=>{const item=await api('/vorschlag',{auftrag_id:needOrder(),art:'notiz',text:$('note').elements.text.value});await refresh();status('Notiz zur Prüfung vorbereitet.');});
  $('purchase').onsubmit=safe(async()=>{const data=Object.fromEntries(new FormData($('purchase')));data.menge=Number(data.menge);await api('/vorschlag',{...data,auftrag_id:needOrder()});await refresh();status('Entwurf vorbereitet. E-Mail kann unter Vorschläge geöffnet werden. Keine Bestellung.');});
  $('purchase-kind').onchange=()=>{document.querySelectorAll('[data-price]').forEach(input=>{input.required=$('purchase-kind').value==='einkauf';});};
  $('camera').onclick=safe(camera);$('capture').onclick=safe(capture);$('camera-stop').onclick=stopCamera;
  $('photo-file').onchange=safe(()=>{needOrder();const file=$('photo-file').files[0];if(file)preview(file);});
  $('photo-save').onclick=safe(async()=>status(await savePhoto()));
  $('clear').onclick=safe(async()=>{stopVoice();pending=null;await api('/dialog/leeren',{});$('conversation').replaceChildren();status('Dialog gelöscht. Aktionsprotokoll bleibt erhalten.');});
  $('record').onclick=safe(async()=>{
    unlockOutput();
    if(micPending){stopVoice();return;}
    if(recorder?.state==='recording'){recorder.stop();return;}
    if(busy)throw new Error('Bitte laufende Antwort abwarten.');
    if(!window.isSecureContext||!navigator.mediaDevices||!window.MediaRecorder)throw new Error('Sprachaufnahme benötigt HTTPS und MediaRecorder.');
    stopVoice();
    const generation=playbackGeneration,recordRun=++recordingGeneration;
    micPending=true;$('record').textContent='Mikrofonzugriff abbrechen';$('voice-mode').hidden=true;$('voice-stop').hidden=false;voicePhase('microphone');state('connecting','Mikrofon wird geöffnet. Bitte im Browser erlauben.');
    let received,cancelRequest,pendingTimer;
    const cancelled=new Promise(resolve=>{cancelRequest=()=>resolve(null);cancelMicrophone=cancelRequest;});
    const deadline=new Promise((resolve,reject)=>{pendingTimer=setTimeout(()=>reject(new Error('Mikrofon antwortet nicht.')),60000);});
    try{
      const media=navigator.mediaDevices.getUserMedia({audio:true}).then(result=>{if(recordRun!==recordingGeneration||document.hidden){result.getTracks().forEach(t=>t.stop());return null;}micStream=result;return result;});
      received=await Promise.race([media,cancelled,deadline]);
    }catch(error){if(recordRun!==recordingGeneration)return;stopRecording();voiceButtons(false);const message=error.name==='NotAllowedError'?'Mikrofonzugriff nicht erlaubt. Bitte die Freigabe im Browser prüfen.':'Mikrofon konnte nicht geöffnet werden. Bitte Mikrofon und Browserfreigabe prüfen.';const failure=new Error(message);failure.phase='microphone';throw failure;}
    finally{clearTimeout(pendingTimer);if(cancelMicrophone===cancelRequest)cancelMicrophone=null;}
    if(recordRun!==recordingGeneration||!received)return;
    if(document.hidden){stopRecording();return;}
    micPending=false;micStream=received;voicePhase('recording');
    const mime=['audio/webm;codecs=opus','audio/mp4'].find(t=>MediaRecorder.isTypeSupported(t));
    let recording;
    try{recording=recorder=new MediaRecorder(received,mime?{mimeType:mime}:{});}catch(error){stopRecording();voiceButtons(false);throw error;}
    const chunks=[];const timer=recordingTimer=setTimeout(()=>{if(recordRun===recordingGeneration&&recording.state==='recording')recording.stop();},60000);
    recording.ondataavailable=e=>{if(e.data.size)chunks.push(e.data);};
    recording.onstop=async()=>{
      clearTimeout(timer);received.getTracks().forEach(t=>t.stop());if(micStream===received)micStream=null;if(recorder===recording)recorder=null;
      if(recordRun!==recordingGeneration||generation!==playbackGeneration||document.hidden)return;
      $('record').textContent='Mikrofon starten';voiceButtons(false);
      try{
        state('thinking','Sprache wird erkannt.');const form=new FormData();
        form.append('audio',new Blob(chunks,{type:recording.mimeType}),recording.mimeType.includes('mp4')?'sprache.mp4':'sprache.webm');
        const r=await api('/audio',form);
        if(generation!==playbackGeneration||document.hidden)return;
        await chat(r.text);
      }catch(e){if(e.playbackGeneration!==undefined||(generation===playbackGeneration&&!document.hidden))reportError(e);}
    };
    try{recording.start();}catch(error){clearTimeout(timer);stopRecording();voiceButtons(false);throw error;}$('record').textContent='Aufnahme stoppen';state('listening','Mikrofon aktiv. Zum Senden stoppen.');
  });
  function stopDevices(){const failure=$('avatar').dataset.state==='error'?$('avatar-status').textContent:null;stopVoice();meter('suspend');pending=null;stopCamera();if(failure)state('error',failure);}
  document.addEventListener('visibilitychange',()=>{if(document.hidden)stopDevices();});window.addEventListener('pagehide',stopDevices);
  if($('assistant').dataset.ready!=='true'){$('record').disabled=true;$('voice-mode').disabled=true;$('chat').querySelector('button').disabled=true;$('vision').disabled=true;state('idle','Avatar bereit · Sprachzugang noch einrichten.');}
  async function loadSource(){
    const source=await api('/quelle');
    if(!['lesestand','live'].includes(source.modus))return;
    $('source-panel').hidden=false;
    $('source-status').textContent=source.modus==='live'?`${source.auftraege.length} aktuelle Aufträge geladen. ${source.native?'Direkt aus diesem Cockpit':'Direkte Cockpit-API'}; weitere Aufträge per Sprachsuche erreichbar.`:`${source.auftraege.length} aktive Aufträge · Stand ${new Date(source.stand).toLocaleString('de-DE')}. Manuell übernommener Lesestand, keine automatische Aktualisierung. Nach einer Stunde neu übernehmen.`;
    $('assistant').dataset.source='cockpit';
    for(const item of source.auftraege){const option=document.createElement('option');option.value=item.id;option.textContent=`${item.id} · ${item.fahrzeug} · ${item.kennzeichen||'ohne Kennzeichen'}`;$('source-orders').append(option);}
    $('source-orders').onchange=safe(async()=>{if(!$('source-orders').value)return;stopVoice();pending=null;showOrder(await api('/auftrag/'+Number($('source-orders').value)));});
    if(readOnly||source.readonly||source.modus==='lesestand'){
    for(const id of ['note','purchase'])$(id).closest('section').hidden=true;
    for(const id of ['camera','capture','camera-stop','photo-file','photo-save','vision'])$(id).disabled=true;
    if(!statusEnabled&&!purchaseEnabled&&!documentsEnabled&&!offersEnabled&&!personalEnabled)$('actions').textContent='Lesemodus: Speichern, Fotozuordnung und Bestellungen sind gesperrt.';
    }
  }
  function showActions(){const menu=$('assistant-menu');if(!menu.open)menu.showModal();menu.querySelector('details').open=true;$('actions-section').hidden=false;$('actions-section').scrollIntoView({block:'start'});}
  window.AssistantMaterialPhotoHost={api,status,stopVoice,selectMaterial:selection=>{stopVoice();log('KI','Artikel gewählt: '+(selection.produkt_name||'Produkt')+'. Menge und Dringlichkeit bitte noch nennen.');status('Artikel gewählt. Gespräch starten oder Nachricht schreiben, um Menge und Dringlichkeit zu nennen.');}};
  $('materialfoto-open')?.addEventListener('click',()=>stopVoice());
  window.AssistantWorkflowHost={api,safe,status,stopVoice:()=>{stopVoice();pending=null;},getOrder:()=>current,
    proposed:async()=>{pending=null;await refresh();showActions();status('Vorschlag vorbereitet. Bitte prüfen und ausdrücklich bestätigen.');}};
  safe(async()=>{await refresh();await loadSource();})();
})();
