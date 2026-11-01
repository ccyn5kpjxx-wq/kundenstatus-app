'use strict';

document.querySelectorAll('.bb-chef-review').forEach((form) => {
  const kind = form.querySelector('[name="typ"]');
  const category = form.querySelector('[name="kategorie"]');
  const paymentLabel = form.querySelector('[data-payment-confirm]');
  const payment = paymentLabel.querySelector('input');
  const confirmation = form.querySelector('[data-review-confirm]');
  const button = form.querySelector('[data-review-button]');
  kind.required = true;
  category.required = true;

  const update = () => {
    const specialPayment = ['bescheid', 'zahlungsauftrag'].includes(kind.value);
    paymentLabel.hidden = !specialPayment;
    payment.disabled = !specialPayment;
    payment.required = kind.value === 'zahlungsauftrag';
    if (!specialPayment) payment.checked = false;
    const payingNotice = kind.value === 'bescheid' && payment.checked;
    confirmation.textContent = kind.value === 'rechnung' || payingNotice
      ? 'Original, Belegart, Betrag und Zuordnung geprüft. Zahlung durch das Büro freigeben.'
      : 'Original, Belegart, Angaben und Zuordnung geprüft. Diesen Beleg als geprüft kennzeichnen.';
    button.textContent = kind.value === 'rechnung' ? 'Rechnung zur Zahlung freigeben'
      : payingNotice ? 'Bescheid zur Zahlung freigeben'
      : kind.value === 'zahlungsauftrag' ? 'Zahlungsauftrag freigeben' : 'Prüfung bestätigen';
  };

  kind.addEventListener('change', () => {
    payment.checked = false;
    form.querySelector('[name="confirmed"]').checked = false;
    update();
  });
  payment.addEventListener('change', update);
  update();
});
