'use strict';

document.querySelectorAll('.bb-copy').forEach((button) => {
  button.addEventListener('click', async () => {
    const status = button.closest('.bb-transfer-readonly').querySelector('[data-copy-status]');
    status.hidden = false;
    status.textContent = '';
    button.disabled = true;
    try {
      if (!window.isSecureContext || !navigator.clipboard || !navigator.clipboard.writeText) {
        throw new Error('Clipboard unavailable');
      }
      await navigator.clipboard.writeText(button.dataset.copyValue);
      status.textContent = `${button.dataset.copyLabel} kopiert.`;
    } catch {
      status.textContent = 'Kopieren nicht möglich. Bitte den angezeigten Wert markieren und manuell kopieren.';
    } finally {
      button.disabled = false;
    }
  });
});

document.querySelectorAll('.bb-chef-review').forEach((form) => {
  const kind = form.querySelector('[name="typ"]');
  const category = form.querySelector('[name="kategorie"]');
  const paymentLabel = form.querySelector('[data-payment-confirm]');
  const payment = paymentLabel.querySelector('input');
  const confirmation = form.querySelector('[data-review-confirm]');
  const button = form.querySelector('[data-review-button]');
  const transfer = form.querySelector('[data-transfer-fields]');
  kind.required = true;
  category.required = true;

  const update = () => {
    const specialPayment = ['bescheid', 'zahlungsauftrag'].includes(kind.value);
    if (transfer) {
      transfer.hidden = false;
      transfer.querySelectorAll('input, select').forEach((field) => {
        field.disabled = false;
        field.required = false;
      });
      const orderMode = transfer.querySelector('[data-order-mode]');
      if (orderMode) {
        orderMode.hidden = kind.value !== 'zahlungsauftrag';
        orderMode.querySelector('select').disabled = kind.value !== 'zahlungsauftrag';
      }
    }
    paymentLabel.hidden = !specialPayment;
    payment.disabled = !specialPayment;
    payment.required = kind.value === 'zahlungsauftrag';
    if (!specialPayment) payment.checked = false;
    const payingNotice = kind.value === 'bescheid' && payment.checked;
    const paying = kind.value === 'rechnung' || kind.value === 'zahlungsauftrag' || payingNotice;
    confirmation.textContent = paying
      ? 'Original, Betrag und Zuordnung geprüft. Das Büro darf die Überweisung ausführen.'
      : 'Original, Angaben und Zuordnung geprüft. Diesen Beleg als geprüft kennzeichnen.';
    button.textContent = paying ? 'Zahlung freigeben' : 'Prüfung bestätigen';
  };

  kind.addEventListener('change', () => {
    payment.checked = false;
    form.querySelector('[name="confirmed"]').checked = false;
    update();
  });
  payment.addEventListener('change', update);
  update();
});
