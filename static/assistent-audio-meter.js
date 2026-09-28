/* Local output-only measurement. Never routes playback or opens a microphone. */
(function (host) {
  'use strict';
  const SAMPLE_SECONDS = 0.02;
  const SILENCE_FLOOR = 0.006;
  const CLOSE_AFTER_MS = 60;

  class OutputAudioMeter {
    constructor({window, audio, onLevel}) {
      this.window = window;
      this.document = window.document;
      this.audio = audio;
      this.onLevel = onLevel;
      this.context = null;
      this.source = null;
      this.generation = 0;
      this.unlockGeneration = 0;
      this.frameId = null;
      this.frameGeneration = 0;
      this.destroyed = false;
      this.suspended = false;
      this.buffering = false;
      this.level = 0;
      this.lastSent = null;
      this.lastTime = null;
      this.silentMs = 0;
      this.listeners = [];
      this.sync = this.sync.bind(this);
      const on = (target, type, handler) => {
        target?.addEventListener(type, handler);
        this.listeners.push(() => target?.removeEventListener(type, handler));
      };
      on(audio, 'playing', () => { this.buffering = false; this.sync(); });
      for (const type of ['waiting', 'stalled', 'error', 'emptied']) {
        on(audio, type, () => { this.buffering = true; this.sync(); });
      }
      for (const type of ['pause', 'ended', 'volumechange', 'timeupdate', 'seeked']) on(audio, type, this.sync);
      on(this.document, 'visibilitychange', this.sync);
      on(window, 'pagehide', () => this.suspend());
      this.emit(0);
    }

    emit(value) {
      value = Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
      // Positive samples also act as a heartbeat for the renderer's stall guard.
      if (value === 0 && this.lastSent === 0) return;
      this.lastSent = value;
      // A visual consumer must never interrupt the independent audio player.
      try { this.onLevel(value); } catch (_) {}
    }

    reset() {
      ++this.frameGeneration;
      if (this.frameId !== null) this.window.cancelAnimationFrame(this.frameId);
      this.frameId = null;
      this.level = 0;
      this.lastTime = null;
      this.silentMs = 0;
      this.emit(0);
    }

    async unlock() {
      if (this.destroyed) return false;
      this.suspended = false;
      const attempt = ++this.unlockGeneration;
      try {
        if (!this.context) {
          const Context = this.window.AudioContext || this.window.webkitAudioContext;
          if (!Context || !this.window.requestAnimationFrame) return false;
          this.context = new Context();
          this.context.addEventListener?.('statechange', this.sync);
        }
        if (this.context.state !== 'running') await this.context.resume();
        if (this.destroyed || this.suspended || attempt !== this.unlockGeneration) return false;
        this.bindStream();
        this.decodeBlob();
        this.sync();
        return this.context.state === 'running';
      } catch (_) {
        this.reset();
        return false;
      }
    }

    useStream(stream) {
      this.clear();
      if (this.destroyed || !stream || typeof stream.getAudioTracks !== 'function') return;
      this.source = {kind: 'stream', stream};
      this.buffering = false;
      this.bindStream();
      this.sync();
    }

    bindStream() {
      const source = this.source;
      if (!this.context || source?.kind !== 'stream' || source.node || source.failed) return;
      try {
        if (!source.stream.getAudioTracks().some(track => track.readyState !== 'ended')) return;
        source.analyser = this.context.createAnalyser();
        source.analyser.fftSize = 1024;
        source.samples = new Float32Array(source.analyser.fftSize);
        source.node = this.context.createMediaStreamSource(source.stream);
        source.node.connect(source.analyser);
        // No destination connection: the existing audio element owns playback.
      } catch (_) {
        this.disconnect(source);
        source.node = null;
        source.analyser = null;
        source.failed = true;
      }
    }

    useBlob(blob, sourceUrl) {
      this.clear();
      if (this.destroyed || typeof blob?.arrayBuffer !== 'function' || typeof sourceUrl !== 'string' || !sourceUrl) return;
      // TTS output is bounded; an unexpectedly large response stays unanimated.
      if (blob.size > 20 * 1024 * 1024) return;
      this.source = {kind: 'blob', blob, url: sourceUrl, envelope: null, decoding: false};
      this.buffering = false;
      this.decodeBlob();
      this.sync();
    }

    async decodeBlob() {
      const source = this.source;
      const context = this.context;
      const generation = this.generation;
      if (!context || source?.kind !== 'blob' || source.decoding || source.envelope || source.failed) return;
      source.decoding = true;
      const current = () => !this.destroyed && this.source === source && this.generation === generation;
      try {
        const bytes = await source.blob.arrayBuffer();
        if (!current()) return;
        // Callback form also supports Safari versions predating promise decode.
        const decoded = await new Promise((resolve, reject) => {
          const pending = context.decodeAudioData(bytes, resolve, reject);
          pending?.then?.(resolve, reject);
        });
        if (!current()) return;
        if (!Number.isFinite(decoded.sampleRate) || decoded.sampleRate <= 0 || decoded.duration > 120 || decoded.numberOfChannels < 1 || decoded.numberOfChannels > 8) throw new Error('unsupported audio');
        const stride = Math.max(1, Math.round(decoded.sampleRate * SAMPLE_SECONDS));
        const envelope = new Float32Array(Math.ceil(decoded.length / stride));
        const channels = Array.from({length: decoded.numberOfChannels}, (_, index) => decoded.getChannelData(index));
        for (let frame = 0; frame < envelope.length; frame++) {
          const start = frame * stride, end = Math.min(decoded.length, start + stride);
          let sum = 0;
          for (const channel of channels) {
            for (let sample = start; sample < end; sample++) sum += channel[sample] * channel[sample];
          }
          envelope[frame] = Math.min(1, Math.sqrt(sum / ((end - start) * channels.length)));
        }
        source.envelope = envelope;
        source.step = stride / decoded.sampleRate;
        source.blob = null;
      } catch (_) {
        if (current()) { source.failed = true; source.blob = null; }
      } finally {
        if (current()) { source.decoding = false; this.sync(); }
      }
    }

    canSample() {
      const source = this.source, audio = this.audio;
      if (this.destroyed || this.suspended || this.document?.hidden || this.context?.state !== 'running' || !source || source.failed) return false;
      if (audio.paused || audio.ended || audio.muted || audio.volume <= 0 || this.buffering) return false;
      if (typeof audio.readyState === 'number' && audio.readyState < 2) return false;
      if (source.kind === 'blob') return Boolean(source.envelope && !audio.srcObject && audio.src === source.url);
      try {
        return Boolean(source.node && source.analyser && audio.srcObject === source.stream &&
          source.stream.getAudioTracks().some(track => track.readyState !== 'ended'));
      } catch (_) { return false; }
    }

    sync() {
      if (!this.canSample()) { this.reset(); return; }
      if (this.frameId === null) this.schedule();
    }

    schedule() {
      const generation = this.generation;
      const frameGeneration = this.frameGeneration;
      this.frameId = this.window.requestAnimationFrame(time => {
        if (generation !== this.generation || frameGeneration !== this.frameGeneration || this.destroyed) return;
        this.frameId = null;
        if (!this.canSample()) { this.reset(); return; }
        let raw = 0;
        try {
          const source = this.source;
          if (source.kind === 'blob') {
            const position = Math.floor(this.audio.currentTime / source.step);
            raw = source.envelope[position] || 0;
          } else {
            source.analyser.getFloatTimeDomainData(source.samples);
            let sum = 0;
            for (const value of source.samples) sum += value * value;
            raw = Math.sqrt(sum / source.samples.length);
          }
          raw *= Math.min(1, Math.max(0, this.audio.volume));
          if (!Number.isFinite(raw)) raw = 0;
          raw = Math.min(1, Math.max(0, raw));
        } catch (_) { this.reset(); return; }
        const delta = this.lastTime === null ? 1000 / 60 : Math.max(0, Math.min(100, time - this.lastTime));
        this.lastTime = time;
        if (raw <= SILENCE_FLOOR) { raw = 0; this.silentMs += delta; }
        else this.silentMs = 0;
        const tau = raw > this.level ? 20 : 35;
        this.level += (raw - this.level) * (1 - Math.exp(-delta / tau));
        if (this.silentMs >= CLOSE_AFTER_MS) this.level = 0;
        this.emit(this.level);
        if (generation === this.generation && frameGeneration === this.frameGeneration && this.canSample()) this.schedule();
      });
    }

    clear() {
      ++this.generation;
      const source = this.source;
      this.source = null;
      this.reset();
      this.disconnect(source);
      // Remote tracks and the audio element are owned by the speech transport.
    }

    disconnect(source) {
      for (const node of [source?.node, source?.analyser]) {
        try { node?.disconnect(); } catch (_) {}
      }
    }

    suspend() {
      this.suspended = true;
      ++this.unlockGeneration;
      this.reset();
      try { this.context?.suspend()?.catch?.(() => {}); } catch (_) {}
    }

    destroy() {
      if (this.destroyed) return;
      this.destroyed = true;
      ++this.unlockGeneration;
      this.clear();
      this.listeners.forEach(remove => remove());
      this.listeners = [];
      this.context?.removeEventListener?.('statechange', this.sync);
      try { this.context?.close()?.catch?.(() => {}); } catch (_) {}
      this.context = null;
    }
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = {OutputAudioMeter};
  else host.OutputAudioMeter = OutputAudioMeter;
})(typeof window === 'undefined' ? globalThis : window);
