/* Decorative, state-driven portrait animation; no audio capture or phoneme analysis. */
(function (host) {
  'use strict';
  const CHARACTERS = Object.freeze({chris: 'Chris', mila: 'Mila', robot: 'Roboter'});
  const knownCharacter = value => Object.prototype.hasOwnProperty.call(CHARACTERS, value);

  class AvatarAnimationController {
    constructor({avatar, portraits, document, mediaQuery, MutationObserver,
                 setTimeout, clearTimeout, random = Math.random}) {
      this.avatar = avatar;
      this.portraits = Array.from(portraits || []);
      this.document = document;
      this.mediaQuery = mediaQuery;
      this.schedule = setTimeout;
      this.cancel = clearTimeout;
      this.random = random;
      this.timer = null;
      this.generation = 0;
      this.destroyed = false;
      this.suspended = false;
      this.refresh = this.refresh.bind(this);
      this.observer = MutationObserver ? new MutationObserver(this.refresh) : null;
      this.observer?.observe(avatar, {attributes: true, attributeFilter: ['data-state']});
      document.addEventListener('visibilitychange', this.refresh);
      if (mediaQuery?.addEventListener) mediaQuery.addEventListener('change', this.refresh);
      else mediaQuery?.addListener?.(this.refresh);
      this.refresh();
    }

    frame(value) {
      this.portraits.forEach(portrait => { portrait.dataset.frame = value; });
    }

    active() {
      return !this.destroyed && !this.suspended && !this.document.hidden && !this.mediaQuery?.matches;
    }

    later(delay, generation, callback) {
      this.timer = this.schedule(() => {
        if (generation !== this.generation) return;
        this.timer = null;
        if (!this.active()) return;
        callback();
      }, delay);
    }

    refresh() {
      const generation = ++this.generation;
      if (this.timer !== null) this.cancel(this.timer);
      this.timer = null;
      this.frame('rest');
      if (!this.active() || !this.portraits.length) return;
      const state = this.avatar.dataset.state;
      if (state === 'speaking') {
        const frames = ['a', 'o', 'a', 'rest'];
        let index = 0;
        const speak = () => {
          if (this.avatar.dataset.state !== 'speaking') return this.refresh();
          this.frame(frames[index++ % frames.length]);
          this.later(170, generation, speak);
        };
        speak();
      } else if (state === 'idle' || state === 'listening') {
        const waitForBlink = () => this.later(3200 + Math.floor(this.random() * 2200), generation, () => {
          if (!['idle', 'listening'].includes(this.avatar.dataset.state)) return this.refresh();
          this.frame('blink');
          this.later(130, generation, () => {
            this.frame('rest');
            waitForBlink();
          });
        });
        waitForBlink();
      }
    }

    suspend() { this.suspended = true; this.refresh(); }
    resume() { this.suspended = false; this.refresh(); }

    destroy() {
      this.destroyed = true;
      this.refresh();
      this.observer?.disconnect();
      this.document.removeEventListener('visibilitychange', this.refresh);
      if (this.mediaQuery?.removeEventListener) this.mediaQuery.removeEventListener('change', this.refresh);
      else this.mediaQuery?.removeListener?.(this.refresh);
    }
  }

  function initAssistantAvatar(document, window) {
    const assistant = document.getElementById('assistant');
    const avatar = document.getElementById('avatar');
    if (!assistant || !avatar) return null;
    const animation = new AvatarAnimationController({
      avatar, portraits: avatar.querySelectorAll('.avatar-portrait'), document,
      mediaQuery: window.matchMedia?.('(prefers-reduced-motion: reduce)'),
      MutationObserver: window.MutationObserver,
      setTimeout: window.setTimeout.bind(window), clearTimeout: window.clearTimeout.bind(window)
    });
    const listeners = [];
    const on = (target, event, callback) => {
      if (!target) return;
      target.addEventListener(event, callback);
      listeners.push(() => target.removeEventListener(event, callback));
    };
    on(window, 'pagehide', () => animation.suspend());
    on(window, 'pageshow', () => animation.resume());
    const picker = document.getElementById('avatar-picker');
    const choose = document.getElementById('avatar-choose');
    const close = document.getElementById('avatar-picker-close');
    const status = document.getElementById('avatar-picker-status');
    const profileCharacter = document.getElementById('profile-character');
    const label = document.getElementById('avatar-choice-label');
    const options = Array.from(picker?.querySelectorAll('[data-character-option]') || []);
    let saving = false;
    let disposed = false;
    let requestAbort = null;
    let requestTimeout = null;
    const selected = () => knownCharacter(assistant.dataset.character) ? assistant.dataset.character : 'chris';
    const syncSelection = () => {
      const value = selected();
      options.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.characterOption === value)));
      if (profileCharacter) profileCharacter.value = value;
      if (label) label.textContent = CHARACTERS[value];
    };
    const setSaving = (busy, pending = '') => {
      saving = busy;
      options.forEach(button => {
        button.disabled = busy || !knownCharacter(button.dataset.characterOption);
        button.dataset.pending = String(busy && button.dataset.characterOption === pending);
      });
      if (close) close.disabled = busy;
      if (choose) choose.disabled = busy;
      picker?.setAttribute('aria-busy', String(busy));
    };
    const closePicker = () => {
      if (saving) return;
      picker?.close();
      choose?.focus();
    };
    on(choose, 'click', () => {
      if (saving || !picker) return;
      syncSelection();
      if (status) status.textContent = '';
      if (!picker.open) picker.showModal();
    });
    on(close, 'click', closePicker);
    on(picker, 'cancel', event => {
      if (saving) event.preventDefault();
    });
    on(picker, 'close', () => choose?.focus());

    async function saveCharacter(character) {
      if (saving || disposed || !knownCharacter(character)) return;
      if (character === selected()) { closePicker(); return; }
      const token = document.querySelector('meta[name="csrf-token"]')?.content;
      if (!token) {
        if (status) status.textContent = 'Deine Anmeldung konnte nicht geprüft werden. Bitte die Seite neu öffnen.';
        return;
      }
      setSaving(true, character);
      if (status) status.textContent = `${CHARACTERS[character]} wird gespeichert …`;
      let saved = false;
      try {
        requestAbort = new window.AbortController();
        requestTimeout = window.setTimeout(() => requestAbort?.abort(), 15000);
        const response = await window.fetch('/werkstatt/assistent/avatar', {
          method: 'POST', credentials: 'same-origin', signal: requestAbort.signal,
          headers: {'Content-Type': 'application/json', 'X-CSRF-Token': token},
          body: JSON.stringify({character})
        });
        if (!response.ok) throw new Error('save failed');
        const result = await response.json();
        if (result.ok !== true || result.character !== character) throw new Error('unexpected response');
        if (disposed) return;
        assistant.dataset.character = result.character;
        syncSelection();
        if (status) status.textContent = `${CHARACTERS[character]} ist ausgewählt.`;
        saved = true;
      } catch (_) {
        if (!disposed && status) status.textContent = 'Die Figur konnte nicht gespeichert werden. Deine bisherige Auswahl bleibt erhalten. Bitte erneut versuchen.';
      } finally {
        if (requestTimeout !== null) window.clearTimeout(requestTimeout);
        requestTimeout = null;
        requestAbort = null;
        if (!disposed) {
          setSaving(false);
          if (saved) closePicker();
        }
      }
    }
    options.forEach(button => on(button, 'click', () => saveCharacter(button.dataset.characterOption)));
    syncSelection();
    setSaving(false);
    return {
      animation,
      destroy() {
        disposed = true;
        requestAbort?.abort();
        if (requestTimeout !== null) window.clearTimeout(requestTimeout);
        animation.destroy();
        listeners.forEach(remove => remove());
      }
    };
  }

  const api = {AvatarAnimationController, initAssistantAvatar};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else {
    host.AssistantAvatar = api;
    const start = () => initAssistantAvatar(host.document, host);
    if (host.document.readyState === 'loading') host.document.addEventListener('DOMContentLoaded', start, {once: true});
    else start();
  }
})(typeof window === 'undefined' ? globalThis : window);
