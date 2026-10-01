'use strict';
// Bidirectional WebRTC: microphone stays live during model audio.
window.AssistantRealtime = class {
  constructor({api, audio, onState, onError, onText, onEvent, onPlaybackBlocked, onRemoteStream}) {
    Object.assign(this,{api,audio,onState,onError,onText,onEvent,onPlaybackBlocked,onRemoteStream});
    this.active=false;this.generation=0;this.phase='idle';this.session=null;
  }
  current(session) {return !!session&&this.active&&this.session===session&&session.generation===this.generation;}
  state(session,name,text) {
    if(this.current(session))this.onState(name,text,{phase:session.phase,playbackBlocked:!!session.playbackBlocked});
  }
  setPhase(session,phase,text,delay,timeoutMessage) {
    if(!this.current(session))return;
    clearTimeout(session.phaseTimer);session.phase=phase;this.phase=phase;
    this.state(session,'thinking',text);
    session.phaseTimer=setTimeout(()=>{
      if(this.current(session))this.fail(new Error(timeoutMessage),session);
    },delay);
  }
  async waitFor(promise,session) {
    const result=await Promise.race([promise,session.stopped]);
    if(!this.current(session)||result===session.cancelled)throw new Error('Gespräch beendet.');
    return result;
  }
  send(event,session=this.session) {
    if(this.current(session)&&session.channel?.readyState==='open')session.channel.send(JSON.stringify(event));
  }
  notifyRemoteStream(stream) {
    // Optional visualisation must never take down the voice connection.
    try{this.onRemoteStream?.(stream);}catch(_){}
  }
  async start(order) {
    if(this.active)return;
    if(!window.isSecureContext)throw new Error('Das Gespräch benötigt HTTPS. Bitte die sichere Cockpit-Adresse öffnen.');
    if(!navigator.mediaDevices?.getUserMedia||!window.RTCPeerConnection)throw new Error('Dieser Browser unterstützt das Mikrofon oder Echtzeitgespräche nicht. Bitte Safari oder Chrome verwenden.');
    this.order=order;this.active=true;
    const session={generation:++this.generation,abort:new AbortController(),cancelled:{},cleanups:[],refreshing:false,playbackBlocked:false};
    session.stopped=new Promise(resolve=>{session.resolveStop=resolve;});
    this.session=session;this.abort=session.abort;this.refreshing=false;this.playbackBlocked=false;
    this.setPhase(session,'microphone','Mikrofon wird geöffnet. Bitte den Mikrofonzugriff im Browser erlauben.',60000,
      'Der Browser hat das Mikrofon nach 60 Sekunden noch nicht bereitgestellt. Bitte Mikrofonfreigabe und angeschlossenes Mikrofon prüfen und erneut starten.');
    try {
      // Stop settles start even while a permission prompt stays pending.
      // A permission granted later must still release the obsolete stream.
      const microphone=navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true}}).then(stream=>{
        if(!this.current(session))stream.getTracks().forEach(track=>track.stop());
        // Own the resource before the next await continuation: Stop may run
        // after permission resolves but before start() resumes below.
        else session.stream=stream;
        return stream;
      });
      const stream=await this.waitFor(microphone,session);
      this.stream=session.stream=stream;
      this.setPhase(session,'connection','Mikrofon ist bereit. Die Sprachverbindung wird vorbereitet.',25000,
        'Der Browser konnte die Sprachverbindung nicht vorbereiten. Bitte erneut starten oder den Browser neu öffnen.');
      const pc=this.pc=session.pc=new RTCPeerConnection();
      this.audio.autoplay=true;this.audio.playsInline=true;this.audio.srcObject=null;this.audio.removeAttribute('src');this.audio.muted=false;
      pc.ontrack=event=>{
        if(!this.current(session)||(event.track?.kind&&event.track.kind!=='audio'))return;
        const remote=event.streams?.[0]||(event.track?new MediaStream([event.track]):null);
        if(!remote){this.fail(new Error('Die Sprachverbindung hat keine Audiospur geliefert. Bitte erneut starten.'),session);return;}
        session.remoteStream=remote;this.audio.srcObject=remote;this.notifyRemoteStream(remote);
        void this.playRemote(session);
      };
      stream.getTracks().forEach(track=>{
        pc.addTrack(track,stream);
        const ended=()=>{if(this.current(session))this.fail(new Error('Das Mikrofon wurde getrennt oder die Freigabe beendet. Bitte erneut starten.'),session);};
        track.addEventListener?.('ended',ended);
        session.cleanups.push(()=>track.removeEventListener?.('ended',ended));
      });
      pc.onconnectionstatechange=()=>{
        if(!this.current(session))return;
        if(['failed','closed'].includes(pc.connectionState)){
          this.fail(new Error('Sprachverbindung unterbrochen. Bitte Internetverbindung prüfen und erneut starten.'),session);
        }else if(pc.connectionState==='disconnected'&&session.phase==='connected'){
          // A short network loss can recover without replacing the connection.
          // Repeated disconnected events must not extend the deadline.
          if(session.disconnectTimer!=null)return;
          this.state(session,'thinking','Die Verbindung ist kurz unterbrochen. Ich warte auf die Wiederverbindung.');
          session.disconnectTimer=setTimeout(()=>{
            if(this.current(session)&&pc.connectionState!=='connected')this.fail(new Error('Die Sprachverbindung konnte nicht wiederhergestellt werden. Bitte Internetverbindung prüfen und erneut starten.'),session);
          },5000);
        }else if(pc.connectionState==='connected'&&session.disconnectTimer!=null){
          clearTimeout(session.disconnectTimer);session.disconnectTimer=null;
          const speaking=session.outputActive&&!session.playbackBlocked&&!this.audio.muted;
          this.state(session,speaking?'speaking':'listening',session.playbackBlocked?'Verbindung wiederhergestellt. Bitte „Ton einschalten“ antippen.':speaking?'Verbindung wiederhergestellt. Du kannst mich jederzeit unterbrechen.':'Verbindung wiederhergestellt. Ich höre zu.');
        }
      };
      const channel=this.channel=session.channel=pc.createDataChannel('oai-events');
      channel.onopen=()=>{
        if(!this.current(session)||session.phase==='connected')return;
        clearTimeout(session.phaseTimer);session.phase='connected';this.phase='connected';
        this.state(session,'listening',session.playbackBlocked?'Ich höre zu. Bitte „Ton einschalten“ antippen, um meine Antwort zu hören.':'Ich höre zu. Du kannst mich beim Sprechen unterbrechen.');
        session.refreshTimer=setInterval(()=>{if(this.current(session))void this.refresh(session);},15000);
        session.limit=setTimeout(()=>{
          if(!this.current(session))return;
          this.stop();this.onState('idle','Gespräch nach 15 Minuten beendet. Bei Bedarf neu starten.',{phase:'idle'});
        },900000);
      };
      channel.onmessage=message=>{
        if(!this.current(session))return;
        try {
          const event=JSON.parse(message.data);
          this.event(event,session).catch(error=>{if(this.current(session))this.fail(error,session);});
        }catch(_){this.fail(new Error('Der Sprachdienst hat eine ungültige Nachricht gesendet. Bitte erneut starten.'),session);}
      };
      channel.onclose=()=>{if(this.current(session))this.fail(new Error('Sprachkanal geschlossen. Bitte neu starten.'),session);};
      channel.onerror=()=>{if(this.current(session))this.fail(new Error('Der Sprachkanal konnte keine Daten übertragen. Bitte erneut starten.'),session);};
      const offer=await this.waitFor(pc.createOffer(),session);
      await this.waitFor(pc.setLocalDescription(offer),session);
      this.setPhase(session,'server','Mikrofon ist bereit. Der KI-Sprachdienst wird verbunden und die Aufträge werden geladen.',80000,
        'Der Sprachdienst hat nicht rechtzeitig geantwortet. Mikrofonfreigabe ist vorhanden; bitte die Serververbindung prüfen und erneut starten.');
      const access=await this.waitFor(this.api('/realtime/start',{sdp:offer.sdp,auftrag_id:order||null,transport:'browser'},false,session.abort.signal),session);
      if(typeof access?.client_secret!=='string'||!access.client_secret||access.client_secret.length>4096||
          !Number.isFinite(access.expires_at)||access.expires_at*1000<=Date.now()){
        throw new Error('Der kurzlebige Sprachzugang ist ungültig oder bereits abgelaufen. Bitte das Gespräch erneut starten.');
      }
      // Keep the credential only in this start call. No persistent session field,
      // DOM, storage or logging; the fixed destination cannot be supplied by API data.
      this.state(session,'thinking','Sprachzugang ist bereit. Der Browser verbindet sich direkt mit dem KI-Sprachdienst.');
      let response;
      try{
        response=await this.waitFor(fetch('https://api.openai.com/v1/realtime/calls',{
          method:'POST',body:offer.sdp,headers:{'Content-Type':'application/sdp',Authorization:'Bearer '+access.client_secret},
          signal:session.abort.signal,credentials:'omit',redirect:'error',cache:'no-store',referrerPolicy:'no-referrer'
        }),session);
      }catch(_){throw new Error('Die direkte Sprachverbindung konnte nicht aufgebaut werden. Bitte Netzwerk oder Browser prüfen und erneut starten.');}
      if(!response.ok){
        const message=response.status===401
          ?'Der kurzlebige Sprachzugang wurde abgelehnt. Bitte das Gespräch erneut starten.'
          :response.status===403
            ?'Der Sprachdienst hat die Verbindung nicht freigegeben. Bitte die Werkstattleitung den KI-Zugang prüfen lassen.'
            :response.status===429
              ?'Der Sprachdienst ist gerade ausgelastet. Bitte kurz warten und das Gespräch erneut starten.'
              :'Der Sprachdienst konnte die direkte Verbindung nicht herstellen. Bitte später erneut starten.';
        // Never read provider error bodies or expose request/credential details.
        throw new Error(message);
      }
      let answerSdp;
      try{answerSdp=await this.waitFor(response.text(),session);}
      catch(_){throw new Error('Die Antwort für die Sprachverbindung konnte nicht gelesen werden. Bitte erneut starten.');}
      if(typeof answerSdp!=='string'||!answerSdp.startsWith('v=0')||answerSdp.length>128000)throw new Error('Der Sprachdienst hat keine gültige Verbindungsantwort geliefert. Bitte erneut starten.');
      this.setPhase(session,'connection','Der Sprachdienst ist bereit. Die Audioverbindung wird aufgebaut.',25000,
        'Der Sprachdienst hat geantwortet, aber die Audioverbindung kam nicht zustande. Bitte Netzwerk oder Browser prüfen und erneut starten.');
      await this.waitFor(pc.setRemoteDescription({type:'answer',sdp:answerSdp}),session);
    }catch(error){
      if(!this.current(session))return;
      if(session.phase==='microphone'){
        const messages={
          NotAllowedError:'Der Mikrofonzugriff wurde nicht erlaubt. Bitte das Mikrofon in den Website-Einstellungen freigeben und erneut starten.',
          PermissionDeniedError:'Der Mikrofonzugriff wurde nicht erlaubt. Bitte das Mikrofon in den Website-Einstellungen freigeben und erneut starten.',
          NotFoundError:'Kein Mikrofon gefunden. Bitte ein Mikrofon anschließen oder ein anderes Gerät verwenden.',
          NotReadableError:'Das Mikrofon lässt sich nicht öffnen. Bitte andere Mikrofon-Apps schließen und die Geräteeinstellungen prüfen.',
          AbortError:'Der Mikrofonstart wurde vom Gerät abgebrochen. Bitte erneut starten.',
          SecurityError:'Der Browser blockiert das Mikrofon für diese Seite. Bitte die Website-Berechtigungen prüfen.'
        };
        error=new Error(messages[error.name]||'Das Mikrofon konnte nicht gestartet werden. Bitte Mikrofon und Browserfreigabe prüfen.');
      }
      this.fail(error,session);
    }
  }
  async playRemote(session) {
    if(!this.current(session)||!session.remoteStream||this.audio.srcObject!==session.remoteStream)return false;
    const stream=session.remoteStream;
    const attempt=session.playAttempt=(session.playAttempt||0)+1;
    const ownsPlayback=()=>this.current(session)&&session.playAttempt===attempt&&this.audio.srcObject===stream;
    try {
      // Called synchronously from resumePlayback on a user gesture.
      await this.waitFor(this.audio.play(),session);
      if(!ownsPlayback())return false;
      const wasBlocked=session.playbackBlocked;
      this.playbackBlocked=session.playbackBlocked=false;
      if(wasBlocked)this.state(session,session.outputActive?'speaking':'listening',session.outputActive?'Du kannst mich jederzeit unterbrechen.':'Ton eingeschaltet. Ich höre zu.');
      return true;
    }catch(error){
      if(!ownsPlayback())return false;
      if(error.name==='NotAllowedError'){
        this.playbackBlocked=session.playbackBlocked=true;
        this.state(session,session.phase==='connected'?'listening':'thinking','Die Tonwiedergabe ist gesperrt. Bitte „Ton einschalten“ antippen.');
        try{this.onPlaybackBlocked?.(error,{generation:session.generation,retry:()=>this.playRemote(session)});}catch(_){}
      }else this.fail(new Error('Audio konnte nicht abgespielt werden. Bitte Gespräch erneut starten.'),session);
      return false;
    }
  }
  resumePlayback() {return this.playRemote(this.session);}
  async refresh(session=this.session) {
    if(!this.current(session)||session.refreshing)return;
    this.refreshing=session.refreshing=true;
    try {
      const context=await this.waitFor(this.api('/realtime/kontext'+(this.order?'?auftrag_id='+encodeURIComponent(this.order):''),undefined,false,session.abort.signal),session);
      this.send({type:'session.update',session:{type:'realtime',instructions:context.instructions}},session);
    }catch(error){if(this.current(session))this.fail(new Error('Auftragskontext oder Zugriffsrecht nicht mehr verfügbar. Gespräch beendet.'),session);}
    finally{session.refreshing=false;if(this.current(session))this.refreshing=false;}
  }
  async event(event,session=this.session) {
    if(!this.current(session))return;
    try {
      switch(event.type){
        case 'response.created':
          // A late terminal event from an interrupted response must not end
          // the newer response in the same WebRTC session.
          session.responseId=event.response?.id||null;break;
        case 'response.done': {
          const response=event.response;
          if(!response||(session.responseId&&response.id&&response.id!==session.responseId))break;
          // Generation completion is not playback completion. Keep successful
          // audio/tool follow-ups and normal VAD cancellations on their paths.
          if(!['failed','incomplete'].includes(response.status))break;
          let message;
          if(response.status==='incomplete'){
            message=response.status_details?.reason==='max_output_tokens'
              ?'Die Sprachantwort wurde wegen ihrer Länge abgeschnitten. Bitte Gespräch neu starten und die Frage in kürzeren Schritten stellen.'
              :'Die Sprachantwort wurde nicht vollständig erzeugt. Bitte Gespräch neu starten oder die Nachricht schreiben.';
          }else{
            const code=response.status_details?.error?.code;
            message=code==='rate_limit_exceeded'
              ?'Der Sprachdienst ist gerade ausgelastet. Bitte kurz warten und das Gespräch erneut starten.'
              :['insufficient_quota','billing_hard_limit_reached'].includes(code)
                ?'Der Sprachdienst kann derzeit keine Antwort erzeugen. Bitte die Werkstattleitung den KI-Zugang prüfen lassen.'
                :'Der Sprachdienst konnte keine Antwort erzeugen. Bitte Gespräch neu starten oder die Nachricht schreiben.';
          }
          // Never display provider text: it may contain request or account data.
          this.fail(new Error(message),session);break;
        }
        case 'input_audio_buffer.speech_started':
          // Server VAD also cancels/truncates pending output; mute locally now.
          this.audio.muted=true;session.outputActive=false;
          this.state(session,'listening','Ich höre zu.');break;
        case 'input_audio_buffer.speech_stopped':
          this.state(session,'thinking','Antwort kommt.');break;
        case 'output_audio_buffer.started':
          this.audio.muted=false;session.outputActive=true;
          this.state(session,session.playbackBlocked?'listening':'speaking',session.playbackBlocked?'Bitte „Ton einschalten“ antippen, um meine Antwort zu hören.':'Du kannst mich jederzeit unterbrechen.');break;
        case 'output_audio_buffer.stopped':
          session.outputActive=false;this.state(session,'listening','Ich höre zu.');break;
        case 'conversation.item.input_audio_transcription.completed':
          this.onText('Du',event.transcript);
          if(/^(gespräch beenden|sprachmodus beenden|stop)[.!?]?$/i.test(event.transcript.trim()))this.stop();
          break;
        case 'response.output_audio_transcript.done':this.onText('KI',event.transcript);break;
        case 'response.function_call_arguments.done': {
          let result;
          try{result=await this.waitFor(this.api('/realtime/werkzeug',{name:event.name,arguments:JSON.parse(event.arguments)},false,session.abort.signal),session);}
          catch(error){if(!this.current(session))return;result={result:{error:error.message}};}
          if(!this.current(session))return;
          if(result.event?.type==='auftrag')this.order=result.event.data.id;
          if(result.event)await this.waitFor(this.onEvent(result.event),session);
          if(!this.current(session))return;
          this.send({type:'conversation.item.create',item:{type:'function_call_output',call_id:event.call_id,output:JSON.stringify(result.result)}},session);
          this.send({type:'response.create'},session);break;
        }
        case 'error':throw new Error('Echtzeitdienst meldet einen Fehler. Bitte Gespräch neu starten oder Einzelaufnahme verwenden.');
      }
    }catch(error){if(this.current(session))throw error;}
  }
  fail(error,session=this.session){
    if(!this.current(session))return;
    error.phase=session.phase;this.stop();this.onError(error);
  }
  stop(){
    const session=this.session;
    this.active=false;++this.generation;this.session=null;this.phase='idle';this.refreshing=false;this.playbackBlocked=false;
    if(session){
      clearTimeout(session.phaseTimer);clearTimeout(session.disconnectTimer);clearTimeout(session.limit);clearInterval(session.refreshTimer);
      session.resolveStop(session.cancelled);session.abort.abort();
      session.cleanups.forEach(cleanup=>cleanup());
      if(session.channel){session.channel.onopen=null;session.channel.onmessage=null;session.channel.onclose=null;session.channel.onerror=null;session.channel.close();}
      if(session.pc){session.pc.ontrack=null;session.pc.onconnectionstatechange=null;session.pc.close();}
      session.stream?.getTracks().forEach(track=>track.stop());
    }
    this.stream=null;this.pc=null;this.channel=null;this.abort=null;
    this.audio.pause();this.audio.srcObject=null;this.audio.muted=false;this.notifyRemoteStream(null);
    this.onState('idle','Gespräch beendet.',{phase:'idle',playbackBlocked:false});
  }
};
