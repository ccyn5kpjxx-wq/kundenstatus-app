(() => {
  document.querySelectorAll('[data-current-deck]').forEach(deck => {
    const stack = deck.querySelector('.current-stack');
    const cards = [...deck.querySelectorAll('[data-current-card]')];
    const controls = deck.querySelector('[data-deck-controls]');
    const previous = deck.querySelector('[data-deck-prev]');
    const next = deck.querySelector('[data-deck-next]');
    const status = deck.querySelector('[data-deck-status]');
    const choices = [...deck.querySelectorAll('[data-deck-index]')];
    if (!stack || cards.length < 2 || !controls || !previous || !next || !status) return;
    const visibleLayers = Math.min(cards.length, 5);
    stack.style.setProperty('--card-count', String(visibleLayers));

    let current = 0;
    const show = requested => {
      const index = ((requested % cards.length) + cards.length) % cards.length;
      if (index !== current && cards[current].contains(document.activeElement)) next.focus();
      current = index;
      cards.forEach((card, number) => {
        const active = number === current;
        const position = (number - current + cards.length) % cards.length;
        card.style.setProperty('--card-position', String(Math.min(position, visibleLayers - 1)));
        card.classList.toggle('is-current', active);
        card.setAttribute('aria-hidden', String(!active));
        card.inert = !active;
      });
      choices.forEach(button => button.setAttribute('aria-pressed', String(Number(button.dataset.deckIndex) === current)));
      status.textContent = `${current + 1} von ${cards.length} · ${cards[current].dataset.cardTitle}`;
    };
    previous.addEventListener('click', () => show(current - 1));
    next.addEventListener('click', () => show(current + 1));
    choices.forEach(button => button.addEventListener('click', () => show(Number(button.dataset.deckIndex))));
    deck.addEventListener('keydown', event => {
      if (event.altKey || event.ctrlKey || event.metaKey) return;
      const destinations = { ArrowLeft: current - 1, ArrowRight: current + 1, Home: 0, End: cards.length - 1 };
      if (!(event.key in destinations)) return;
      event.preventDefault();
      show(destinations[event.key]);
    });

    let touchStart = null;
    stack.addEventListener('touchstart', event => {
      touchStart = event.touches.length === 1 ? { x: event.touches[0].clientX, y: event.touches[0].clientY } : null;
    }, { passive: true });
    stack.addEventListener('touchend', event => {
      if (!touchStart || event.changedTouches.length !== 1) return;
      const dx = event.changedTouches[0].clientX - touchStart.x;
      const dy = event.changedTouches[0].clientY - touchStart.y;
      touchStart = null;
      if (Math.abs(dx) > 60 && Math.abs(dx) > Math.abs(dy) * 1.5) show(current + (dx < 0 ? 1 : -1));
    }, { passive: true });
    stack.addEventListener('touchcancel', () => { touchStart = null; }, { passive: true });

    show(0);
    stack.classList.add('is-ready');
    controls.hidden = false;
  });
})();
