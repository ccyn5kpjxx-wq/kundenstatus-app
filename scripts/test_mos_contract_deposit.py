"""Regression checks for authorized deposits and older charged deposits."""

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import sys
import unittest

from PIL import Image, ImageDraw
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_public_contract import render_pdf, snapshot, snapshot_hash


def quote(*, authorized):
    rental = 14700
    result = {
        'accepted_terms': True,
        'vehicle_name': 'Hyundai KONA', 'vehicle_id': 3,
        'vehicle_plate': 'TEST-K 3', 'vehicle_vin': 'TESTVIN',
        'start_slot': '2026-10-01T09:00:00+02:00',
        'end_slot': '2026-10-04T09:00:00+02:00',
        'days': 3, 'daily_cents': 4900, 'rental_cents': rental,
        'deposit_cents': 50000,
        'deposit_charged_cents': 0 if authorized else 50000,
        'amount_cents': rental if authorized else rental + 50000,
        'deductible_cents': 100000, 'included_km': 450,
        'extra_km_cents': 25, 'rules_version': 'test-v1',
        'terms_text': 'Vollgetankt zurückgeben.', 'test_only': True,
    }
    if authorized:
        result.update(deposit_authorized_cents=50000,
                      deposit_method='card_authorization_at_booking')
    return result


def signature():
    image = Image.new('RGBA', (700, 180), (255, 255, 255, 0))
    ImageDraw.Draw(image).line([(40, 110), (90, 40), (130, 120), (220, 55)],
                               fill=(20, 30, 40, 255), width=5)
    output = BytesIO()
    image.save(output, 'PNG')
    return output.getvalue()


class DepositContractTests(unittest.TestCase):
    def contract(self, authorized):
        return snapshot({'quote': quote(authorized=authorized),
                         'customer': {'name': 'Testkunde', 'email': 'test@example.invalid'}})

    def pdf_text(self, contract):
        pdf = render_pdf(contract, datetime.now(timezone.utc).isoformat(), signature(),
                         snapshot_hash(contract), 'test-signature-hash', 'TEST-BOOKING')
        return '\n'.join(page.extract_text() or '' for page in PdfReader(BytesIO(pdf)).pages)

    def test_new_quote_signs_only_rent_as_payable_and_deposit_as_authorization(self):
        contract = self.contract(True)
        self.assertEqual(contract['amount_cents'], 14700)
        self.assertEqual(contract['deposit_charged_cents'], 0)
        self.assertEqual(contract['deposit_authorized_cents'], 50000)
        self.assertEqual(contract['deposit_method'], 'card_authorization_at_booking')
        text = self.pdf_text(contract)
        self.assertIn('Kaution auf Kreditkarte reserviert', text)
        self.assertIn('keine Abbuchung', text)
        self.assertIn('Zahlbetrag (nur Miete)', text)
        self.assertNotIn('Kaution im Zahlbetrag', text)

    def test_historic_charged_quote_keeps_original_snapshot_shape_and_pdf(self):
        contract = self.contract(False)
        self.assertNotIn('deposit_authorized_cents', contract)
        self.assertNotIn('deposit_method', contract)
        self.assertEqual(contract['amount_cents'], 64700)
        self.assertEqual(contract['deposit_charged_cents'], 50000)
        text = self.pdf_text(contract)
        self.assertIn('Kaution im Zahlbetrag', text)
        self.assertNotIn('Kaution auf Kreditkarte reserviert', text)

    def test_new_contract_rejects_kaution_hidden_in_payable_amount(self):
        wrong = quote(authorized=True)
        wrong['amount_cents'] += 50000
        with self.assertRaisesRegex(ValueError, 'Zahlbetrag und Kreditkartenreservierung'):
            snapshot({'quote': wrong,
                      'customer': {'name': 'Testkunde', 'email': 'test@example.invalid'}})


if __name__ == '__main__':
    unittest.main()
