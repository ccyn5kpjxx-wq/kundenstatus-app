'use strict';
// Bidirectional WebRTC: microphone stays live during model audio.
window.AssistantRealtime = class {
  constructor({api, audio, onState, onError, onText, onEvent}) {
    Object.assign(this,{api,audio,onState,onError,onText,onEvent});
    this.active=false;this.generation=0;
  }
  send(event) {if(this.channel?.readyState==='open')this.channel.send(JSON.stringify(event));}
  async start(order) {
    if(this.active)return;
    if(!window.isSecureContext||!navigator.mediaDevices||!window.RTCPeerConnection)throw new Error('Echtzeitgespräch benötigt HTTPS und WebRTC.');
    this.order=order;this.active=true;const generation=++this.generation;
    this.onState('thinking','Sprachverbindung und Aufträge werden geladen.');
    this.abort=new AbortController();
    this.timeout=setTimeout(()=>this.fail(new Error('Sprachverbindung nicht erreichbar. Erneut starten.')),25000);
    try {
      const stream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true}});
      if(!this.active||generation!==this.generation){stream.getTracks().forEach(t=>t.stop());return;}
      this.stream=stream;
      const pc=this.pc=new RTCPeerConnection();
      this.audio.autoplay=true;this.audio.srcObject=null;this.audio.removeAttribute('src');
      pc.ontrack=event=>{if(!this.active)return;this.audio.srcObject=event.streams[0];this.audio.play().catch(()=>this.fail(new Error('Audiowiedergabe blockiert. Gespräch erneut per Knopfdruck starten.')));};
      stream.getTracks().forEach(track=>pc.addTrack(track,stream));
      pc.onconnectionstatechange=()=>{if(['failed','disconnected','closed'].includes(pc.connectionState)&&this.active)this.fail(new Error('Sprachverbindung unterbrochen. Bitte erneut starten.'));};
      const channel=this.channel=pc.createDataChannel('oai-events');
      channel.onopen=()=>{
        if(!this.active)return;
        clearTimeout(this.timeout);
        this.onState('listening','Ich höre zu. Du kannst mich beim Sprechen unterbrechen.');
        this.refreshTimer=setInterval(()=>this.refresh(),15000);
        this.limit=setTimeout(()=>{this.stop();this.onState('idle','Gespräch nach 15 Minuten beendet. Bei Bedarf neu starten.');},900000);
      };
      channel.onmessage=event=>{if(this.active)this.event(JSON.parse(event.data)).catch(error=>this.fail(error));};
      channel.onclose=()=>{if(this.active)this.fail(new Error('Sprachkanal geschlossen. Bitte neu starten.'));};
      const offer=await pc.createOffer();await pc.setLocalDescription(offer);
      const answer=await this.api('/realtime/start',{sdp:offer.sdp,auftrag_id:order||null},false,this.abort.signal);
      if(!this.active||generation!==this.generation)return;
      await pc.setRemoteDescription({type:'answer',sdp:answer.sdp});
    }catch(error){if(this.active&&generation===this.generation)this.fail(error);}
  }
  async refresh() {
    if(!this.active||this.refreshing)return;
    this.refreshing=true;
    try {
      const context=await this.api('/realtime/kontext'+(this.order?'?auftrag_id='+encodeURIComponent(this.order):''),undefined,false,this.abort.signal);
      if(this.active)this.send({type:'session.update',session:{type:'realtime',instructions:context.instructions}});
    }catch(error){if(this.active)this.fail(new Error('Auftragskontext oder Zugriffsrecht nicht mehr verfügbar. Gespräch beendet.'));}
    finally{this.refreshing=false;}
  }
  async event(event) {
    switch(event.type){
      case 'input_audio_buffer.speech_started':
        // Server VAD also cancels/truncates pending output; mute the local buffer immediately.
        this.audio.muted=true;
        this.onState('listening','Ich höre zu.');break;
      case 'input_audio_buffer.speech_stopped':
        this.onState('thinking','Antwort kommt.');break;
      case 'output_audio_buffer.started':
        this.audio.muted=false;
        this.onState('speaking','Du kannst mich jederzeit unterbrechen.');break;
      case 'output_audio_buffer.stopped':
        this.onState('listening','Ich höre zu.');break;
      case 'conversation.item.input_audio_transcription.completed':
        this.onText('Du',event.transcript);
        if(/^(gespräch beenden|sprachmodus beenden|stop)[.!?]?$/i.test(event.transcript.trim()))this.stop();
        break;
      case 'response.output_audio_transcript.done':this.onText('KI',event.transcript);break;
      case 'response.function_call_arguments.done': {
        let result;
        try{result=await this.api('/realtime/werkzeug',{name:event.name,arguments:JSON.parse(event.arguments)},false,this.abort.signal);}
        catch(error){if(!this.active)return;result={result:{error:error.message}};}
        if(!this.active)return;
        if(result.event?.type==='auftrag')this.order=result.event.data.id;
        if(result.event)await this.onEvent(result.event);
        if(!this.active)return;
        this.send({type:'conversation.item.create',item:{type:'function_call_output',call_id:event.call_id,output:JSON.stringify(result.result)}});
        this.send({type:'response.create'});break;
      }
      case 'error':throw new Error('Echtzeitdienst meldet einen Fehler. Bitte Gespräch neu starten oder Einzelaufnahme verwenden.');
    }
  }
  fail(error){this.stop();this.onError(error);}
  stop(){
    this.active=false;++this.generation;clearTimeout(this.timeout);clearTimeout(this.limit);clearInterval(this.refreshTimer);
    this.abort?.abort();this.channel?.close();this.pc?.close();
    this.stream?.getTracks().forEach(t=>t.stop());this.stream=null;this.pc=null;this.channel=null;
    this.audio.pause();this.audio.srcObject=null;this.audio.muted=false;
    this.onState('idle','Gespräch beendet.');
  }
};
