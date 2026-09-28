'use strict';
(() => {
  const $ = id => document.getElementById(id), base = '/werkstatt/assistent';
  const readOnly = $('assistant').dataset.readOnly === 'true';
  if(readOnly)document.querySelectorAll('[data-write-section]').forEach(el=>{el.hidden=true;el.querySelectorAll('input,button,textarea,select').forEach(field=>field.disabled=true);});
  const token = document.querySelector('meta[name="csrf-token"]').content;
  let current=null, photoOrder=null, photo=null, stream=null, recorder=null, micStream=null;
  let busy=false, audioUrl=null, photoUrl=null, pending=null, cancelPlayback=null;
  let playbackGeneration=0, busyGeneration=null;
  // Observe output only. Playback and microphone ownership stay with their controllers.
  const outputMeter=window.OutputAudioMeter?new window.OutputAudioMeter({window,audio:$('speech'),onLevel:level=>window.AssistantAvatar?.instance?.animation.setAudioLevel(level)}):null;
  const unlockOutput=()=>{outputMeter?.unlock().catch(()=>{});};
  const normalized = text => text.toLocaleLowerCase('de-DE').replace(/[^\p{L}\p{N}\s]/gu,'').trim();
  const status = text => {$('status').textContent=text;};
  const state = (name,text) => {$('avatar').dataset.state=name;$('avatar-status').textContent=text;if(name!=='speaking')window.AssistantAvatar?.instance?.animation.setAudioLevel(0);};
  async function api(path,data,raw=false,signal) {
    const options={headers:{'X-CSRF-Token':token},signal};
    if(data!==undefined){options.method='POST';if(data instanceof FormData)options.body=data;else{options.headers['Content-Type']='application/json';options.body=JSON.stringify(data);}}
    const response=await fetch(base+path,options);
    if(!response.ok){let message='Anfrage fehlgeschlagen. Bitte Anmeldung und Verbindung prüfen.';try{message=(await response.json()).error||message;}catch{}throw new Error(message);}
    return raw?response.blob():response.json();
  }
  const stalePlaybackError = error => error.playbackGeneration!==undefined&&error.playbackGeneration!==playbackGeneration;
  function reportError(error){if(stalePlaybackError(error))return;if(error.phase)$('assistant').dataset.voicePhase=error.phase;status(error.message);state('error',error.message);}
  function safe(fn){return async event=>{event?.preventDefault();try{await fn(event);}catch(e){reportError(e);}};}
  function log(who,text){const p=document.createElement('p');p.textContent=who+': '+text;$('conversation').append(p);}
  function showOrder(data){current=data;$('order-form').elements.id.value=data.id;$('active-order').textContent=`Auftrag ${data.id} · ${data.kennzeichen||'ohne Kennzeichen'}`;const pre=document.createElement('pre');pre.textContent=`Auftrag ${data.id} · ${data.kennzeichen||'ohne Kennzeichen'} · ${data.fahrzeug}\nAngebotsstatus: ${data.angebot_status}\nVersicherungsfreigabe: ${data.versicherung_freigabe_status}\nAngebotstext: ${data.werkstatt_angebot_text||'nicht hinterlegt'}\nBeschreibung (keine Freigabe): ${data.beschreibung||'–'}\nTeile-Aktenstand: ${JSON.stringify(data.teile,null,2)}\n${data.hinweis}`;$('order').replaceChildren(pre);if(data.quelle){pre.textContent=`Auftrag ${data.id} · ${data.fahrzeug} · ${data.kennzeichen||'ohne Kennzeichen'}\nStatus im Cockpit: ${data.status}\nArbeiten / Beschreibung:\n${data.beschreibung||'Keine Angaben in der Übersicht.'}\nTermine: ${data.termine||('Annahme: '+(data.annahme_datum||'offen')+' · Fertig: '+(data.fertig_datum||'offen')+' · Rückgabe: '+(data.abholtermin||'offen'))}\n${data.modus==='lesestand'?'Freigaben und Teilebestand: im Lesestand nicht erhoben.':'Angebotsstatus: '+(data.angebot_status||'unbekannt')+' · Versicherungsfreigabe: '+(data.versicherung_freigabe_status||'unbekannt')+'\nDokumente: '+(data.dokumente||[]).map(d=>d.id+' · '+d.original_name).join(', ')}`;const note=document.createElement('p');note.textContent=(data.modus==='live'?'Cockpit, abgerufen: ':'Cockpit-Lesestand: ')+new Date(data.stand).toLocaleString('de-DE')+' · '+(data.modus==='live'?'Direkter Portalabruf':data.detail_gelesen?'Beschreibung aus Auftragsdetails':'Listenübersicht, möglicherweise gekürzt');const link=document.createElement('a');link.href=data.quelle;link.target='_blank';link.rel='noopener';link.textContent='Originalauftrag im Cockpit öffnen';$('order').prepend(note);if($('assistant').dataset.admin==='true')$('order').prepend(link);if(data.uebersicht){const summary=document.createElement('p');summary.textContent='Cockpit-Kurzfassung: '+data.uebersicht;$('order').append(summary);}}}
  document.addEventListener('assistant-open-order',safe(async event=>{showOrder(await api('/auftrag/'+Number(event.detail.id)));$('order').scrollIntoView({behavior:'smooth'});}));
  function needOrder(){if(!current)throw new Error('Zuerst den richtigen Auftrag aufrufen.');return current.id;}
  function stopCamera(){stream?.getTracks().forEach(t=>t.stop());stream=null;$('video').srcObject=null;$('video').hidden=true;$('capture').disabled=true;}
  async function camera(){needOrder();if(!window.isSecureContext||!navigator.mediaDevices)throw new Error('Kamera benötigt HTTPS und Browserfreigabe.');stopCamera();const result=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}},audio:false});if(document.hidden){result.getTracks().forEach(t=>t.stop());return;}stream=result;$('video').srcObject=stream;$('video').hidden=false;await $('video').play();$('capture').disabled=false;}
  function preview(blob){photo=blob;photoOrder={...current};if(photoUrl)URL.revokeObjectURL(photoUrl);photoUrl=URL.createObjectURL(blob);$('photo-preview').src=photoUrl;$('photo-preview').hidden=false;$('photo-save').disabled=false;status(`Aufnahme prüfen: Auftrag ${photoOrder.id} · ${photoOrder.kennzeichen}.`);}
  async function capture(){needOrder();const video=$('video');if(!stream||!video.videoWidth)throw new Error('Kamerabild noch nicht bereit. Sage gleich erneut: Foto aufnehmen.');const canvas=document.createElement('canvas');const scale=Math.min(1,1600/video.videoWidth);canvas.width=video.videoWidth*scale;canvas.height=video.videoHeight*scale;canvas.getContext('2d').drawImage(video,0,0,canvas.width,canvas.height);const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/jpeg',0.88));if(!blob)throw new Error('Aufnahme fehlgeschlagen.');preview(blob);}
  async function savePhoto(){if(!photo||!photoOrder)throw new Error('Foto fehlt.');if(needOrder()!==photoOrder.id)throw new Error('Auftrag wurde gewechselt. Ursprünglichen Auftrag wieder aufrufen oder neues Foto aufnehmen.');$('photo-save').disabled=true;try{const form=new FormData();form.append('foto',photo,'aufnahme.jpg');form.append('analyse',$('vision').checked?'1':'0');const r=await api('/foto/'+photoOrder.id,form);photo=null;pending=null;$('photo-preview').hidden=true;stopCamera();await refresh();return r.hinweis+(r.sichtung?' KI-Sichtung: '+r.sichtung:'');}catch(e){$('photo-save').disabled=false;throw e;}}
  function stopPlayback(){
    ++playbackGeneration;
    const cancel=cancelPlayback;cancelPlayback=null;cancel?.();
    outputMeter?.clear();
  }
  function say(text){
    stopPlayback();
    const generation=playbackGeneration, abort=new AbortController(), audio=$('speech');
    const current=()=>generation===playbackGeneration&&!abort.signal.aborted;
    let timer=null, settled=false, url=null;
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
      const fail=message=>{if(!current())return;state('error',message);done(new Error(message));};
      const listen=(name,handler)=>{
        const guarded=()=>{if(ownsAudio())handler();};
        listeners.push([name,guarded]);audio.addEventListener(name,guarded);
      };
      api('/sprechen',{text},true,abort.signal).then(blob=>{
        if(!current())return;
        if(document.hidden){stopPlayback();return;}
        if(audioUrl)URL.revokeObjectURL(audioUrl);
        url=audioUrl=URL.createObjectURL(blob);audio.srcObject=null;audio.src=url;audio.hidden=false;
        outputMeter?.useBlob(blob,url);
        // Keep these listeners for manual replay, until stop or the next output.
        listen('playing',()=>state('speaking','Ich spreche. Das Mikrofon nimmt gerade nicht auf.'));
        listen('pause',()=>state('idle',audio.ended?'Bereit für deine nächste Nachricht.':'Sprachausgabe pausiert.'));
        listen('waiting',()=>state('thinking','Sprachausgabe lädt.'));
        listen('ended',()=>{state('idle','Bereit für deine nächste Nachricht.');done();});
        listen('error',()=>fail('Audio konnte nicht abgespielt werden.'));
        timer=setTimeout(()=>{if(ownsAudio()){audio.pause();fail('Sprachausgabe unterbrochen. Bitte erneut starten.');}},90000);
        audio.play().catch(()=>{if(ownsAudio())fail('iPhone blockiert die Wiedergabe. Audioplayer antippen oder Gespräch erneut starten.');});
      }).catch(error=>{if(current()){state('error',error.message);done(error);}});
    });
  }
  async function prepareReadback(item){
    pending=null;
    const requestedGeneration=playbackGeneration;
    const challenge=await api('/vorlesen/'+item.id,{});
    if(requestedGeneration!==playbackGeneration)return false;
    log('Bestätigung',challenge.text);
    const playback=say(challenge.text), generation=playbackGeneration;
    if(!await playback||generation!==playbackGeneration)return false;
    pending={type:'action',...challenge};
    return true;
  }
  async function refresh(){
    const items=await api('/aktionen');$('actions').replaceChildren();
    for(const item of items){if(item.art==='foto')continue;const div=document.createElement('div');div.className='action';const pre=document.createElement('pre');let details=item.daten.text;
      if(item.art!=='notiz'){details=`${item.daten.lieferant} · ${item.daten.teilenummer}\n${item.daten.menge} × ${item.daten.bezeichnung}\n`;
        details+=item.art==='anfrage'?'Preis und Verfügbarkeit werden angefragt. Keine Bestellung.':`Stückpreis: ${(item.daten.stueckpreis_brutto_cent/100).toFixed(2)} € · Versand: ${(item.daten.versand_brutto_cent/100).toFixed(2)} € · Nebenkosten: ${(item.daten.nebenkosten_brutto_cent/100).toFixed(2)} €\nGesamt brutto: ${(item.daten.gesamt_cent/100).toFixed(2)} €\n${item.daten.preisquelle}\nKein Bestellversand.`;}
      pre.textContent=`Auftrag ${item.auftrag_id} · ${item.art} · ${item.status}\n${details}`;div.append(pre);
      if(item.status==='vorschlag'){
        const button=document.createElement('button');button.textContent=item.art==='notiz'?'Geprüfte Notiz speichern':'Geprüften Entwurf intern freigeben';
        button.onclick=safe(async()=>{button.disabled=true;try{const r=await api('/bestaetigen/'+item.id,{});pending=null;status(r.hinweis);await refresh();}finally{button.disabled=false;}});div.append(button);
        const read=document.createElement('button');read.className='secondary';read.textContent='Vorlesen & per Sprache bestätigen';read.disabled=$('assistant').dataset.ready!=='true';read.onclick=safe(async()=>{unlockOutput();stopVoice();if(await prepareReadback(item))status('Vorgelesen. Gespräch starten und die genannte Bestätigung sprechen.');});div.append(read);
      }
      if(item.art!=='notiz'){const a=document.createElement('a');a.href=base+'/email/'+item.id;a.textContent='E-Mail-Anfrage als Entwurf öffnen';a.className='email-download';div.append(a);}
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
        else {const r=await api('/sprache-bestaetigen',{nonce:pending.nonce,text});message=r.hinweis;pending=null;await refresh();}
        if(!currentChat())return;
        log('KI',message);status(message);await say(message);return;
      }
      if(clean==='foto aufnehmen'&&stream){await capture();if(!currentChat())return;const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;if(await say(`Aufnahme für Auftrag ${photoOrder.id}, ${photoOrder.kennzeichen}. Bitte Bild und Zuordnung prüfen. Sage: ${phrase}. Oder Abbrechen.`))pending={type:'photo',phrase};return;}
      const r=await api('/dialog',{text,auftrag_id:current?.id});
      if(!currentChat()||(spoken&&!voice.active))return;
      log('KI',r.text);
      let proposal=null,photoReady=false;
      for(const event of r.events){if(!currentChat())return;if(event.type==='auftrag')showOrder(event.data);if(event.type==='vorschlag'&&event.data.status==='vorschlag')proposal=event.data;if(event.type==='kamera'){showOrder(event.data);await camera();if(!currentChat())return;await capture();photoReady=true;}}
      if(!currentChat())return;
      await refresh();if(!currentChat())return;status('Antwort erhalten.');
      if(proposal){await prepareReadback(proposal);}
      else if(photoReady){const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;if(await say(`Foto für Auftrag ${photoOrder.id}, Kennzeichen ${photoOrder.kennzeichen}. Prüfe Bild und Zuordnung. Sage: ${phrase}. Oder Abbrechen.`))pending={type:'photo',phrase};}
      else if(spoken||$('read-aloud').checked)await say(r.text);
      else state('idle','Antwort erhalten.');
    }catch(error){
      // A stopped request may still finish or fail on the server. Its UI is obsolete.
      if(currentChat()||error.playbackGeneration!==undefined)throw error;
    }finally{if(busyGeneration===generation){busy=false;busyGeneration=null;}}
  }
  function voiceButtons(active){$('voice-mode').hidden=active;$('voice-stop').hidden=!active;$('record').disabled=active||$('assistant').dataset.ready!=='true';}
  const voice=new window.AssistantVoiceMode({
    onState:(name,text)=>{state(name,text);voiceButtons(voice.active);},
    onError:reportError,
    onSegment:async blob=>{
      const generation=voice.generation;
      try{
        const form=new FormData();form.append('audio',blob,blob.type.includes('mp4')?'sprache.mp4':'sprache.webm');
        const r=await api('/audio',form);if(!voice.active||generation!==voice.generation)return;
        $('chat').elements.text.value=r.text;await chat(r.text,true);
        if(voice.active&&generation===voice.generation&&!pending){voice.stop();stopPlayback();await realtime.start(current?.id);}
      }catch(error){if(voice.active&&generation===voice.generation&&!stalePlaybackError(error))throw error;}
    }
  });
  const realtime=new window.AssistantRealtime({api,audio:$('speech'),
    onState:(name,text,detail)=>{state(name,text);if(detail?.phase)$('assistant').dataset.voicePhase=detail.phase;voiceButtons(realtime.active||voice.active);if(!realtime.active||name==='speaking')$('audio-resume').hidden=true;},
    onRemoteStream:stream=>{if(stream)outputMeter?.useStream(stream);else outputMeter?.clear();},
    onPlaybackBlocked:()=>{$('audio-resume').hidden=false;},
    onError:reportError,onText:log,
    onEvent:async event=>{
      if(event.type==='auftrag'){showOrder(event.data);return;}
      // Existing deterministic readback and exact confirmation stay outside model control.
      realtime.stop();await refresh();
      if(event.type==='vorschlag'){
        if(!await prepareReadback(event.data))return;
        status('Prüfmodus: genannte Bestätigung sprechen oder Abbrechen.');
        await voice.start();
      }else if(event.type==='kamera'){
        showOrder(event.data);await camera();await capture();
        const phrase=`Foto für Auftrag ${photoOrder.id} speichern`;
        if(!await say(`Foto für Auftrag ${photoOrder.id}, Kennzeichen ${photoOrder.kennzeichen}. Prüfe Bild und Zuordnung. Sage: ${phrase}. Oder Abbrechen.`))return;
        pending={type:'photo',phrase};await voice.start();
      }
    }
  });
  function stopVoice(){stopPlayback();busy=false;busyGeneration=null;realtime.stop();voice.stop();$('speech').pause();$('audio-resume').hidden=true;}
  $('audio-resume').onclick=safe(async()=>{unlockOutput();$('audio-resume').disabled=true;try{if(await realtime.resumePlayback())$('audio-resume').hidden=true;}finally{$('audio-resume').disabled=false;}});
  $('voice-mode').onclick=safe(async()=>{unlockOutput();if(busy)throw new Error('Bitte Antwort abwarten.');if(recorder?.state==='recording')throw new Error('Einzelaufnahme zuerst beenden.');stopPlayback();if(pending)await voice.start();else await realtime.start(current?.id);});
  $('voice-stop').onclick=()=>{stopVoice();stopCamera();};
  $('chat').onsubmit=safe(async()=>{unlockOutput();stopVoice();await chat($('chat').elements.text.value);$('chat').reset();});
  $('order-form').onsubmit=safe(async()=>{stopVoice();pending=null;showOrder(await api('/auftrag/'+Number($('order-form').elements.id.value)));});
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
    if(recorder?.state==='recording'){recorder.stop();return;}
    if(busy)throw new Error('Bitte laufende Antwort abwarten.');
    if(!window.isSecureContext||!navigator.mediaDevices||!window.MediaRecorder)throw new Error('Sprachaufnahme benötigt HTTPS und MediaRecorder.');
    $('speech').pause();micStream=await navigator.mediaDevices.getUserMedia({audio:true});
    const mime=['audio/webm;codecs=opus','audio/mp4'].find(t=>MediaRecorder.isTypeSupported(t));recorder=new MediaRecorder(micStream,mime?{mimeType:mime}:{});
    const generation=playbackGeneration;
    const chunks=[];const timer=setTimeout(()=>{if(recorder?.state==='recording')recorder.stop();},60000);
    recorder.ondataavailable=e=>{if(e.data.size)chunks.push(e.data);};
    recorder.onstop=async()=>{
      clearTimeout(timer);micStream?.getTracks().forEach(t=>t.stop());micStream=null;$('record').textContent='Mikrofon starten';
      if(generation!==playbackGeneration||document.hidden)return;
      try{
        state('thinking','Sprache wird erkannt.');const form=new FormData();
        form.append('audio',new Blob(chunks,{type:recorder.mimeType}),recorder.mimeType.includes('mp4')?'sprache.mp4':'sprache.webm');
        const r=await api('/audio',form);
        if(generation!==playbackGeneration||document.hidden)return;
        $('chat').elements.text.value=r.text;await chat(r.text);
      }catch(e){if(e.playbackGeneration!==undefined||(generation===playbackGeneration&&!document.hidden))reportError(e);}
    };
    recorder.start();$('record').textContent='Aufnahme stoppen';state('listening','Mikrofon aktiv. Zum Senden stoppen.');
  });
  function stopDevices(){const failure=$('avatar').dataset.state==='error'?$('avatar-status').textContent:null;stopVoice();outputMeter?.suspend();pending=null;stopCamera();if(recorder?.state==='recording'){recorder.onstop=null;recorder.stop();}micStream?.getTracks().forEach(t=>t.stop());micStream=null;$('record').textContent='Mikrofon starten';if(failure)state('error',failure);}
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
    $('actions').textContent='Lesemodus: Speichern, Fotozuordnung und Bestellungen sind gesperrt.';
    }
    $('conversation').replaceChildren();current=null;
  }
  safe(async()=>{await refresh();await loadSource();})();
})();
