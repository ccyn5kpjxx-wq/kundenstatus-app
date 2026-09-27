'use strict';
// Active foreground mode: record one utterance, wait for answer, then listen again.
class AssistantVoiceMode {
  constructor({onSegment, onState, onError}) {
    Object.assign(this, {onSegment, onState, onError});
    this.active = false;
    this.generation = 0;
  }
  async start() {
    if (this.active) return;
    if (!window.isSecureContext || !navigator.mediaDevices || !window.MediaRecorder) {
      throw new Error('Sprachmodus benötigt HTTPS, Mikrofonfreigabe und einen Browser mit MediaRecorder.');
    }
    const generation = ++this.generation;
    this.active = true;
    try {
      this.context = new (window.AudioContext || window.webkitAudioContext)();
      await this.context.resume();
      const stream = await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true}});
      if (!this.active || generation !== this.generation) {stream.getTracks().forEach(t=>t.stop()); return;}
      this.stream = stream;
      this.analyser = this.context.createAnalyser();
      this.analyser.fftSize = 2048;
      this.context.createMediaStreamSource(stream).connect(this.analyser);
      this.limitTimer = setTimeout(()=>this.fail(new Error('Sprachmodus nach 15 Minuten beendet. Bei Bedarf neu starten.')),15*60*1000);
      this.listen(generation);
    } catch(error) {this.stop(); throw error;}
  }
  listen(generation) {
    if (!this.active || generation !== this.generation || document.hidden) return;
    const mime = ['audio/webm;codecs=opus','audio/mp4'].find(type=>MediaRecorder.isTypeSupported(type));
    const recorder = new MediaRecorder(this.stream, mime ? {mimeType:mime} : {});
    this.recorder = recorder;
    const chunks = [], samples = new Float32Array(this.analyser.fftSize);
    let voiced = 0, lastLoud = 0;
    const started = performance.now();
    recorder.ondataavailable = event=>{if(event.data.size) chunks.push(event.data);};
    recorder.onerror = ()=>this.fail(new Error('Mikrofonaufnahme abgebrochen. Bitte erneut starten.'));
    recorder.onstop = async()=>{
      cancelAnimationFrame(this.frame);
      clearTimeout(this.segmentTimer);
      if (!this.active || generation !== this.generation) return;
      if (voiced < 4) {this.fail(new Error('Keine Sprache erkannt. Sprachmodus beendet.')); return;}
      this.onState('thinking','Ich verarbeite deine Nachricht.');
      try {
        await this.onSegment(new Blob(chunks,{type:recorder.mimeType}));
        if (this.active && generation === this.generation) {
          this.resumeTimer = setTimeout(()=>this.listen(generation),500);
        }
      } catch(error) {if(this.active)this.fail(error);}
    };
    recorder.start();
    this.onState('listening','Ich höre zu. Sprich und mache danach eine kurze Pause.');
    this.segmentTimer = setTimeout(()=>{if(recorder.state==='recording')recorder.stop();},45000);
    const sample = ()=>{
      if (!this.active || recorder.state !== 'recording') return;
      this.analyser.getFloatTimeDomainData(samples);
      const level = Math.sqrt(samples.reduce((sum,x)=>sum+x*x,0)/samples.length);
      const now = performance.now();
      if(level > 0.025) {voiced++;lastLoud=now;}
      if(voiced>=4 && now-lastLoud>1400 && now-started>700) {recorder.stop();return;}
      if(!voiced && now-started>20000) {this.fail(new Error('Sprachmodus wegen Stille beendet.'));return;}
      this.frame=requestAnimationFrame(sample);
    };
    sample();
  }
  fail(error) {this.stop();this.onError(error);}
  stop() {
    this.active=false;this.generation++;
    cancelAnimationFrame(this.frame);
    clearTimeout(this.resumeTimer);clearTimeout(this.limitTimer);clearTimeout(this.segmentTimer);
    if(this.recorder?.state==='recording'){this.recorder.onstop=null;this.recorder.stop();}
    this.stream?.getTracks().forEach(track=>track.stop());this.stream=null;
    if(this.context && this.context.state!=='closed') this.context.close().catch(()=>{});
    this.onState('idle','Sprachmodus beendet.');
  }
}
window.AssistantVoiceMode = AssistantVoiceMode;
