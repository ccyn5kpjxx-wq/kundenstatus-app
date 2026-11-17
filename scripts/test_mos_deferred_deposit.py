"""Paid advance bookings remain reserved until a later on-session card hold."""
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from PIL import Image, ImageDraw
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from run_mos_public_test import build_test_app
from mietwagen_checkout import (SharedCheckout, init_schema, deferred_deposit_quote,
                                deposit_authorization_opens, deposit_authorization_due,
                                _checkout_disclosure)
from mos_booking.gateway import OfflineGateway, StripeTestGateway
from mos_booking.production import StripeLiveGateway, RefundLedger, init_refund_schema
from mos_public_contract import (init_schema as init_contract_schema, presign_quote,
                                 snapshot, signed_payload, render_pdf)
from mos_handover import init_schema as init_handover_schema


class DeferredDepositTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='mos-deferred-')
        cls.portal = build_test_app(cls.temp.name, origin='http://localhost')
        db = cls.portal.get_db()
        try:
            init_schema(db)
            init_refund_schema(db)
            init_contract_schema(db)
            init_handover_schema(db)
            db.commit()
        finally:
            db.close()

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.portal.app.config.update(TESTING=True, MOS_SHARED_CHECKOUT_ENABLED=True)
        self.portal.app.config['MOS_PUBLIC_BOOKING'].update(enabled=True, mode='offline')
        self.clock = datetime.now(timezone.utc).timestamp()
        self.gateway = OfflineGateway(Path(self.temp.name) / (uuid.uuid4().hex + '.provider.sqlite3'),
                                      'http://localhost', 'whsec_synthetic', clock=lambda: self.clock)
        self.service = SharedCheckout(self.portal, self.gateway, 'http://localhost/status')
        db = self.portal.get_db()
        try:
            self.vehicle_id = db.execute('''INSERT INTO mietfahrzeuge
                (kennzeichen,bezeichnung,erstellt_am,geaendert_am) VALUES (?,?,?,?)''',
                ('TEST-DEFERRED', 'Synthetischer Vorausbuchungswagen',
                 self.portal.now_str(), self.portal.now_str())).lastrowid
            db.commit()
        finally:
            db.close()
        self.customer = {'name': 'Synthetischer Testkunde', 'email': 'test@example.invalid', 'telefon': ''}
        image = Image.new('RGBA', (700, 180), (255, 255, 255, 0))
        ImageDraw.Draw(image).line([(30, 110), (80, 40), (130, 120), (220, 55)], fill=(20, 30, 40, 255), width=5)
        output = BytesIO()
        image.save(output, 'PNG')
        self.signature = 'data:image/png;base64,' + b64encode(output.getvalue()).decode('ascii')

    def hold(self, *, days_ahead=250, test_only=True, deferred=True):
        start = (datetime.now(timezone.utc) + timedelta(days=days_ahead)).replace(second=0, microsecond=0)
        end = start + timedelta(days=1)
        quote = {'amount_cents': 3900, 'rental_cents': 3900, 'deposit_charged_cents': 0,
                 'deposit_authorized_cents': 50000, 'deposit_cents': 50000,
                 'deposit_method': 'card_authorization_at_booking', 'currency': 'eur',
                 'rules_version': 'synthetic-deferred-v1', 'vehicle_id': self.vehicle_id,
                 'vehicle_name': 'Synthetischer Vorausbuchungswagen', 'vehicle_plate': 'TEST-DEFERRED',
                 'vehicle_vin': '', 'start_slot': start.isoformat(), 'end_slot': end.isoformat(),
                 'days': 1, 'daily_cents': 3900, 'deductible_cents': 100000,
                 'included_km': 150, 'extra_km_cents': 25, 'terms_text': 'Nur synthetischer Test.',
                 'test_only': test_only, 'cancellation_policy': 'free_48h_then_10pct_rent'}
        if deferred:
            quote['deposit_timing'] = 'before_pickup'
        signed = presign_quote(quote, self.customer, self.signature)
        return self.service.reserve(uuid.uuid4().hex, self.vehicle_id, start.date().isoformat(),
                                    end.date().isoformat(), self.customer, signed)

    def pay(self, hold):
        session = self.service.create_checkout(hold['id'])
        self.gateway.pay(session['id'])
        body, signature = self.gateway.signed_event(session['id'])
        rental_id = self.service.handle_signed_event(body, signature, 'whsec_synthetic')
        return self.service.read(hold['id']), rental_id

    def during_authorization_window(self, hold):
        quote = json.loads(hold['payload'])['quote']
        moment = datetime.fromisoformat(quote['start_slot']) - timedelta(hours=6)
        self.clock = moment.timestamp()

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

        return patch('mietwagen_checkout.datetime', FrozenDateTime)

    def rental_status(self, hold):
        db = self.portal.get_db()
        try:
            return db.execute('SELECT status FROM mietvorgaenge WHERE id=?',
                              (hold['mietvorgang_id'],)).fetchone()['status']
        finally:
            db.close()

    def test_payment_months_in_advance_confirms_exactly_one_rental_without_deposit(self):
        hold = self.hold()
        with patch.object(self.gateway, 'create_deposit_intent', wraps=self.gateway.create_deposit_intent) as create:
            paid, rental_id = self.pay(hold)
            self.assertEqual(paid['status'], 'confirmed')
            self.assertTrue(rental_id)
            create.assert_not_called()
            self.assertIsNone(self.service._deposit_record(hold['id']))
            body, signature = self.gateway.signed_event(paid['session_id'])
            self.assertEqual(self.service.handle_signed_event(body, signature, 'whsec_synthetic'), rental_id)
        with self.assertRaisesRegex(ValueError, 'belegt'):
            self.hold()

    def test_no_authorization_before_payment_or_before_twelve_hour_window(self):
        hold = self.hold()
        with self.during_authorization_window(hold), self.assertRaises(ValueError):
            self.service.prepare_deposit(hold['id'])
        self.clock = datetime.now(timezone.utc).timestamp()
        paid, _ = self.pay(hold)
        with patch.object(self.gateway, 'create_deposit_intent', wraps=self.gateway.create_deposit_intent) as create:
            with self.assertRaisesRegex(ValueError, 'zwölf Stunden'):
                self.service.prepare_deposit(paid['id'])
            create.assert_not_called()

    def test_paid_booking_gets_one_later_authorization_even_when_new_bookings_are_paused(self):
        paid, _ = self.pay(self.hold())
        db = self.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_holds SET expires_at=1 WHERE id=?', (paid['id'],))
            db.commit()
        finally:
            db.close()
        paid = self.service.read(paid['id'])
        self.portal.app.config['MOS_SHARED_CHECKOUT_ENABLED'] = False
        with self.during_authorization_window(paid):
            first = self.service.prepare_deposit(paid['id'])
            self.assertEqual(self.service.prepare_deposit(paid['id'])['id'], first['id'])
            self.gateway.authorize_deposit_intent(first['id'], valid_for_seconds=7 * 86400)
            result = self.service.reconcile_deposit(paid['id'])
        self.assertTrue(result['ready'])
        self.assertEqual(self.service._deposit_record(paid['id'])['status'], 'authorized')
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
        self.assertEqual(self.service.read(paid['id'])['expires_at'], paid['expires_at'])
        with self.assertRaises(ValueError):
            self.hold()

    def test_canceled_paid_booking_cannot_authorize_a_card(self):
        paid, _ = self.pay(self.hold())
        RefundLedger(self.service).cancel(paid['id'])
        with self.during_authorization_window(paid), self.assertRaisesRegex(ValueError, 'beendete'):
            self.service.prepare_deposit(paid['id'])
        self.assertIsNone(self.service._deposit_record(paid['id']))
        self.assertEqual(self.rental_status(paid), 'storniert')

    def test_cancellation_during_card_creation_keeps_provider_id_for_review(self):
        paid, _ = self.pay(self.hold())
        original = self.gateway.create_deposit_intent
        created = []

        def create_then_cancel(params, key):
            intent = original(params, key)
            created.append(intent['id'])
            RefundLedger(self.service).cancel(paid['id'])
            return intent

        with self.during_authorization_window(paid), patch.object(self.gateway, 'create_deposit_intent', side_effect=create_then_cancel):
            with self.assertRaisesRegex(ValueError, 'manuelle Prüfung'):
                self.service.prepare_deposit(paid['id'])
        record = self.service._deposit_record(paid['id'])
        self.assertEqual(record['intent_id'], created[0])
        self.assertEqual(record['status'], 'review')
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
        self.assertEqual(self.rental_status(paid), 'storniert')

    def test_debit_and_short_authorizations_release_only_the_card_not_the_paid_rental(self):
        for funding, seconds in [('debit', 7 * 86400), ('credit', 3600)]:
            with self.subTest(funding=funding):
                # Separate inventory avoids intentional overlap protection.
                self.setUp()
                paid, _ = self.pay(self.hold())
                with self.during_authorization_window(paid):
                    intent = self.service.prepare_deposit(paid['id'])
                    self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=seconds, funding=funding)
                    with self.assertRaisesRegex(ValueError, 'bezahlte Buchung bleibt bestehen'):
                        self.service.reconcile_deposit(paid['id'])
                self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
                self.assertEqual(self.rental_status(paid), 'aktiv')
                self.assertEqual(self.service._deposit_record(paid['id'])['status'], 'released')

    def test_expired_authorization_never_cancels_a_paid_rental(self):
        paid, _ = self.pay(self.hold())
        with self.during_authorization_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7 * 86400)
            self.service.reconcile_deposit(paid['id'])
            self.clock += 8 * 86400
            with self.assertRaisesRegex(ValueError, 'Admin-Prüfung'):
                self.service.reconcile_deposit(paid['id'])
        self.service.cancel_or_reconcile(paid['id'], cancel=True)
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
        self.assertEqual(self.rental_status(paid), 'aktiv')

    def test_unpaid_deferred_checkout_expiry_releases_inventory_without_deposit_row(self):
        hold = self.hold()
        session = self.service.create_checkout(hold['id'])
        self.gateway.expire(session['id'])
        self.service.cancel_or_reconcile(hold['id'])
        self.assertEqual(self.service.read(hold['id'])['status'], 'released')
        self.assertIsNone(self.service._deposit_record(hold['id']))

    def test_unpaid_expired_webhook_releases_inventory_without_deposit_row(self):
        hold = self.hold()
        session = self.service.create_checkout(hold['id'])
        self.gateway.expire(session['id'])
        body, signature = self.gateway.signed_event(session['id'], kind='checkout.session.expired')
        self.service.handle_signed_event(body, signature, 'whsec_synthetic')
        self.assertEqual(self.service.read(hold['id'])['status'], 'released')

    def test_legacy_quote_still_requires_deposit_before_rent_checkout(self):
        hold = self.hold(deferred=False)
        with self.assertRaisesRegex(ValueError, 'Kartenautorisierung fehlt'):
            self.service.create_checkout(hold['id'])
        self.assertFalse(deferred_deposit_quote(json.loads(hold['payload'])['quote']))

    def test_unpaid_deferred_booking_after_pickup_is_released_without_a_payment(self):
        hold = self.hold()
        pickup = datetime.fromisoformat(json.loads(hold['payload'])['quote']['start_slot'])

        class AfterPickup(datetime):
            @classmethod
            def now(cls, tz=None):
                moment = pickup + timedelta(minutes=1)
                return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

        with patch('mietwagen_checkout.datetime', AfterPickup), self.assertRaisesRegex(ValueError, 'Abholtermin ist verstrichen'):
            self.service.create_checkout(hold['id'])
        self.assertEqual(self.service.read(hold['id'])['status'], 'released')
        self.assertIsNone(self.service.read(hold['id'])['session_id'])

    def test_timing_is_frozen_in_contract_and_described_without_claiming_current_authorization(self):
        hold = self.hold()
        payload = json.loads(hold['payload'])
        contract, digest, signature = signed_payload(payload)
        self.assertEqual(contract['deposit_timing'], 'before_pickup')
        pdf = render_pdf(contract, payload['quote']['signed_at'], signature, digest,
                         payload['quote']['signature_record_hash'], 'SYNTHETIC')
        text = '\n'.join(page.extract_text() or '' for page in PdfReader(BytesIO(pdf)).pages)
        self.assertIn('Kaution vor Abholung online reservieren', text)
        self.assertIn('zwölf Stunden vor Abholung', text)
        self.assertNotIn('Kaution auf Kreditkarte reserviert', text)
        _, disclosure = _checkout_disclosure(payload['quote'], test_mode=False)
        self.assertIn('frühestens 12 Stunden vorher', disclosure)
        self.assertNotIn('bereits', disclosure)
        self.assertLessEqual(len(disclosure), 500)
        payload['quote'].pop('deposit_timing')
        with self.assertRaisesRegex(ValueError, 'unterschriebene Vertrag'):
            signed_payload(payload)

    def test_window_has_exact_twelve_hour_boundary_and_keeps_utc_offsets(self):
        quote = {'start_slot': '2027-03-29T08:00:00+02:00', 'end_slot': '2027-03-30T08:00:00+02:00'}
        opens = deposit_authorization_opens(quote)
        self.assertEqual(opens.isoformat(), '2027-03-28T18:00:00+00:00')
        self.assertFalse(deposit_authorization_due(quote, opens - timedelta(microseconds=1)))
        self.assertTrue(deposit_authorization_due(quote, opens))
        self.assertFalse(deposit_authorization_due(quote, datetime.fromisoformat(quote['end_slot'])))

    def test_paused_live_gateway_has_narrow_existing_paid_deposit_entry_point(self):
        paid, _ = self.pay(self.hold(test_only=False))
        live_hold = {**paid, 'session_id': 'cs_live_synthetic'}
        gateway = object.__new__(StripeLiveGateway)
        gateway.settlement_only = True
        params = {'amount': 50000, 'currency': 'eur', 'capture_method': 'manual',
                  'payment_method_types': ['card'],
                  'metadata': {'hold_id': paid['id'], 'quote_hash': paid['fingerprint']}}
        key = 'mos-deposit-' + paid['id']
        with self.during_authorization_window(paid), patch.object(StripeTestGateway, 'create_deposit_intent', return_value={'id': 'pi_synthetic'}) as create:
            self.assertEqual(gateway.create_existing_deposit_intent(params, key, live_hold)['id'], 'pi_synthetic')
            create.assert_called_once_with(params, key)
            with self.assertRaises(ValueError):
                gateway.create_existing_deposit_intent(params, key, {**live_hold, 'status': 'pending'})
            with self.assertRaises(ValueError):
                gateway.create_existing_deposit_intent({**params, 'metadata': {}}, key, live_hold)
            with self.assertRaises(ValueError):
                gateway.create_deposit_intent(params, key)
        with patch.object(StripeTestGateway, 'create_deposit_intent') as create:
            with self.assertRaises(ValueError):
                gateway.create_existing_deposit_intent(params, key, live_hold)
            create.assert_not_called()


if __name__ == '__main__':
    unittest.main()
