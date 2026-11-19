"""Real temporary journals and wiring; synthetic SDK/outbox, no provider IO."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import re
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_buyer import ShopBuyer
from shop_consumer import ConsumerError, DOCS, create_consumer_blueprint, _freeze_pack
from shop_complete_wheels import NOTICE
from shop_production_payments import PaymentProfileError, prepare_production_payments
from shop_runtime import register_shop_runtime, FLAGS
from shop_storage import ShopStorage
from shop_stripe_gateway import StripeShopGateway
from reifen_rechtstexte import ANBIETER
from test_shop_order_store import quote, rehash, OIL

ORIGIN = 'https://shop.example.test'
ADDRESS = {'recipient': 'SYNTHETIC Buyer', 'street': 'Teststraße 12', 'postcode': '74821', 'city': 'Mosbach', 'country': 'DE'}
CONTEXT = {'contact': {'name': 'SYNTHETIC Buyer', 'email': 'synthetic@example.invalid'}, 'billing': ADDRESS, 'delivery': ADDRESS}


def profile():
    return {'mode': 'live', **{key: True for key in ('checkout_enabled', 'supplier_live_enabled',
        'customer_mail_enabled', 'withdrawals_enabled', 'delivery_terms_reviewed', 'single_worker_confirmed')}}


def pack():
    return {'version': 'SYNTHETIC-REVIEWED-1', 'status': 'approved', 'approved': True,
            'scope': 'mixed_retail', 'texts': {key: 'SYNTHETIC text ' + key for key in DOCS},
            'attachments': [{'filename': 'SYNTHETIC.txt', 'mimetype': 'text/plain', 'content': b'SYNTHETIC annex'}]}


def reviewed_complete_pack():
    value = pack()
    value['version'] = 'SYNTHETIC-REVIEWED-rad-review-v1'
    value['texts']['komplettraeder_zahlung'] = NOTICE
    value['texts']['bestellhinweis'] = 'SYNTHETIC reviewed complete order notice'
    return value


def reviewed_complete_profile(value):
    # Keep this independent of NOTICE: the deployed payment profile pins the reviewed wording.
    return {**profile(), 'complete_wheel_review_approved': True,
        'complete_wheel_notice_sha256': '07371bb8c23d41f8d71ca719e80ed326d5c9b44afb9d6e31de1324a7b4efb024',
        'complete_wheel_frozen_pack_hash': sha256(_freeze_pack(value).encode('utf-8')).hexdigest()}


class FakeOutbox:
    def __init__(self):
        self.sent, self.results = [], {}

    def send(self, token, message, config, *, retry_not_sent):
        assert retry_not_sent is False
        self.sent.append((token, message, config))
        value = {'token': token, 'state': 'sent', 'accepted': [str(message['To'])]}
        self.results[token] = value
        return value

    def status(self, token):
        return self.results.get(token)


class ProductionPaymentChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = ShopStorage(Path(self.temp.name)).provision(create=True)
        self.now = datetime.now(timezone.utc)
        self.store = self.storage.order_store()
        self.buyer = ShopBuyer(self.storage.buyer_backend(), public_origin=ORIGIN, mode='live', enabled=True)
        self.quotes = SimpleNamespace(mode='live', quote=Mock(return_value=SimpleNamespace(snapshot=lambda: {'synthetic': True})))
        self.supplier = SimpleNamespace(fresh_inquiry=Mock(side_effect=AssertionError('No supplier inquiry')))
        self.adapters = {'storage': self.storage, 'store': self.store, 'buyer': self.buyer,
                         'quotes': self.quotes, 'supplier_quotes': self.supplier}
        self.portal = SimpleNamespace(app=Flask('synthetic-payment-wiring', root_path=str(ROOT)),
            get_werkstatt_smtp_config=Mock(return_value={'smtp_configured': True, 'from_address': 'info@auto-lackierzentrum.de'}),
            get_werkstatt_imap_config=Mock(side_effect=AssertionError('No IMAP')))
        self.workshop_orders = {}
        self.portal.erstelle_shop_montageauftrag = Mock(side_effect=lambda order, snapshot, context:
            self.workshop_orders.setdefault(order['id'], 800 + len(self.workshop_orders)))
        self.portal.shop_montageauftraege = lambda ids: {key: self.workshop_orders[key]
                                                        for key in ids if key in self.workshop_orders}
        self.stripe_config = {'SHOP_STRIPE_MODE': 'live', 'SHOP_STRIPE_SECRET_KEY': 'sk_live_SYNTHETIC_ONLY',
            'SHOP_STRIPE_WEBHOOK_SECRET': 'whsec_SYNTHETIC_ONLY', 'SHOP_STRIPE_PUBLIC_ORIGIN': ORIGIN}
        self.supplier_config = {'TYRESYSTEM_API_MODE': 'live', 'TYRESYSTEM_API_USERNAME': 'SYNTHETIC', 'TYRESYSTEM_API_PASSWORD': 'SYNTHETIC'}
        self.client = Mock()
        self.outbox = FakeOutbox()
        self.gateway_constructor = patch('shop_production_payments.StripeShopGateway',
            side_effect=lambda config: StripeShopGateway(config, client=self.client, sdk=Mock()))
        self.construct_gateway = self.gateway_constructor.start()
        self.addCleanup(self.gateway_constructor.stop)
        self.outbox_constructor = patch('shop_production_payments.MailOutbox', return_value=self.outbox)
        self.construct_outbox = self.outbox_constructor.start()
        self.addCleanup(self.outbox_constructor.stop)

    def prepare(self, **changes):
        args = {'pack': pack(), 'supplier_config': self.supplier_config, 'stripe_config': self.stripe_config}
        args.update(changes)
        return prepare_production_payments(self.portal, self.adapters, profile(), **args)

    def count(self, table):
        with closing(self.storage.connect()) as db:
            return db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]

    def save_quote(self):
        value = quote(mode='live', context=CONTEXT)
        value['created_at'] = (self.now - timedelta(minutes=1)).isoformat()
        value['expires_at'] = (self.now + timedelta(minutes=14)).isoformat()
        value = rehash(value)
        self.store.save_quote(value, expected_quote_hash=value['quote_hash'], customer_id='customer-a', mode='live', delivery_context=CONTEXT)
        return value

    def bound_order(self, dependencies, reference):
        self.now = datetime.now(timezone.utc)
        snapshot = self.save_quote()
        consumer = dependencies['consumer_service']
        consumer.accept_quote(snapshot['quote_hash'], customer_id='customer-a', pack_hash=consumer.pack_hash,
                              terms_accepted=True, consumer=True)
        order = dependencies['order_store'].create_order(snapshot['quote_hash'], customer_id='customer-a',
            mode='live', idempotency_key='SYNTHETIC-' + reference)
        attempt = self.store.claim_payment(order['id'], customer_id='customer-a', mode='live')
        session, intent = 'cs_live_' + reference, 'pi_' + reference
        self.store.bind_payment(order['id'], attempt['id'], customer_id='customer-a', mode='live',
                                session_id=session, intent_id=intent, currency='eur',
                                amount_gross_cent=order['amount_gross_cent'])
        def record(kind):
            return self.store.apply_verified_event({'event_id': 'evt_' + reference + '_' + kind,
                'order_id': order['id'], 'attempt_id': attempt['id'], 'mode': 'live', 'currency': 'eur',
                'amount_gross_cent': order['amount_gross_cent'], 'session_id': session, 'intent_id': intent,
                'kind': kind, 'occurred_at': self.now.isoformat()}, verified=True)
        return order['id'], record

    def test_workshop_order_precedes_confirmation_and_survives_duplicate_paid_events(self):
        deps, _ = self.prepare()
        identifier, record = self.bound_order(deps, 'checkout')
        workflow = self.adapters['workflow']
        record('authorized')
        workflow.consider_paid(identifier)
        self.assertEqual(self.workshop_orders, {})
        workflow.consider(identifier)
        def capture(order_id, **kwargs):
            record('paid')
            return {'order_id': order_id, 'mode': 'live', 'state': 'captured',
                    'payment_confirmed': True, 'authorization_released': False}
        with patch.object(workflow.fulfillment, 'run', side_effect=capture) as run:
            self.assertEqual(workflow.run_once()['state'], 'fulfilled')
            workflow.consider_paid(identifier)
            workflow.run_once()
            run.assert_called_once()
        self.assertEqual(self.workshop_orders, {identifier: 800})
        self.assertEqual(deps['workshop_order_view'](identifier, customer_id='customer-a')['number'], 800)
        with closing(self.storage.connect()) as db:
            messages = db.execute("SELECT body FROM shop_order_receipts_v2 WHERE kind='order_confirmed'").fetchall()
        self.assertEqual(len(messages), 1)
        self.assertIn('800', messages[0][0])
        self.assertEqual(len(self.outbox.sent), 2)

    def test_paid_repair_restores_missed_projection_without_sending_old_receipts_or_reordering(self):
        deps, _ = self.prepare()
        workflow = self.adapters['workflow']
        paid, capture = self.bound_order(deps, 'paid')
        handled, historical_capture = self.bound_order(deps, 'handled')
        _, reject = self.bound_order(deps, 'cancelled')
        capture('paid')
        historical_capture('paid')
        reject('cancelled')
        with closing(self.storage.connect()) as db:
            db.execute('INSERT INTO shop_workshop_handled_v1 VALUES (?,?)', (handled, self.now.isoformat()))
            db.commit()
        with patch.object(workflow, 'workshop_order_create', side_effect=RuntimeError('temporary portal failure')):
            workflow.consider_paid(paid)
        self.assertEqual(self.workshop_orders, {})
        with patch.object(workflow.fulfillment, 'run') as supplier:
            for _ in range(2):
                self.assertEqual(workflow.run_once()['state'], 'idle')
            supplier.assert_not_called()
        self.assertEqual(self.workshop_orders, {paid: 800})
        self.assertEqual(self.outbox.sent, [])
        self.assertEqual(self.count('shop_order_receipts_v2'), 0)

    def test_unarmed_profiles_never_touch_adapters_or_construct_transports(self):
        for key in ('checkout_enabled', 'supplier_live_enabled', 'customer_mail_enabled', 'withdrawals_enabled',
                    'delivery_terms_reviewed', 'single_worker_confirmed'):
            for invalid in (False, 'true', 1, None):
                supplied = profile()
                supplied[key] = invalid
                with self.subTest(key=key, invalid=invalid), self.assertRaises(PaymentProfileError):
                    prepare_production_payments(self.portal, {}, supplied, pack=pack(),
                        supplier_config=self.supplier_config, stripe_config=self.stripe_config)
        for mode in ('test', None):
            supplied = profile()
            supplied['mode'] = mode
            with self.assertRaises(PaymentProfileError):
                prepare_production_payments(self.portal, {}, supplied, pack=pack(),
                    supplier_config=self.supplier_config, stripe_config=self.stripe_config)
        self.construct_gateway.assert_not_called()
        self.construct_outbox.assert_not_called()
        self.portal.get_werkstatt_smtp_config.assert_not_called()

    def test_joint_checkout_requires_matching_base_and_amended_terms(self):
        from shop_combined_cart_terms import PACK_VERSION, _amend, checkout_terms
        base = reviewed_complete_pack()
        old_hash = reviewed_complete_profile(base)['complete_wheel_frozen_pack_hash']
        self.assertEqual(checkout_terms(base, old_hash), (base, False))
        old_text = base['texts']['bestellhinweis']
        new_hash = sha256(_freeze_pack(_amend(base)).encode()).hexdigest()
        with patch('shop_combined_cart_terms.BASE_HASH', old_hash), patch('shop_combined_cart_terms.PACK_HASH', new_hash):
            self.assertEqual(checkout_terms(base, 'unrecognised'), (base, False))
            updated, active = checkout_terms(base, old_hash)
            self.assertTrue(active)
            self.assertEqual(updated['version'], PACK_VERSION)
            self.assertEqual(checkout_terms(updated, new_hash), (updated, True))
            self.assertEqual(base['texts']['bestellhinweis'], old_text)
            changed = deepcopy(base)
            changed['texts']['datenschutz'] += ' NEW REVISION'
            self.assertEqual(checkout_terms(changed, old_hash), (changed, False))

    def test_complete_wheel_review_requires_explicit_profile_and_wires_all_fences(self):
        self.adapters['complete_wheel_review_requested'] = True
        for invalid in (None, False, 'true', 1):
            supplied = {**profile(), 'complete_wheel_review_approved': invalid}
            with self.assertRaisesRegex(PaymentProfileError, 'complete_wheel_review_not_approved'):
                prepare_production_payments(self.portal, self.adapters, supplied, pack=pack(),
                    supplier_config=self.supplier_config, stripe_config=self.stripe_config)
        self.assertEqual(self.client.mock_calls, [])
        candidate = reviewed_complete_pack()
        supplied = reviewed_complete_profile(candidate)
        original = deepcopy(candidate)
        dependencies, _ = prepare_production_payments(self.portal, self.adapters, supplied, pack=candidate,
            supplier_config=self.supplier_config, stripe_config=self.stripe_config)
        review = self.adapters['order_review']
        self.assertTrue(review.readiness()['ready'])
        self.assertIs(self.adapters['fulfillment'].review_service, review)
        self.assertIs(self.adapters['workflow'].review_service, review)
        self.assertIs(self.adapters['checkout'].checkout.review_service, review)
        terms = dependencies['consumer_service'].terms()
        self.assertTrue(self.quotes.manual_mixed_orders)
        self.assertIn('komplettraeder_zahlung', terms['texts'])
        self.assertIn('reserviert', terms['texts']['komplettraeder_zahlung'])
        self.assertIn('rad-review-v1', terms['version'])
        self.assertEqual(terms['version'], candidate['version'])
        self.assertEqual(terms['before_order_notice'], candidate['texts']['bestellhinweis'])
        self.assertEqual(terms['pack_hash'], supplied['complete_wheel_frozen_pack_hash'])
        self.assertEqual(candidate, original, 'Activation must validate, never rewrite, approved legal text')
        self.assertEqual(self.portal.app.config['SHOP_REVIEW_ADMIN_ORIGIN'], ORIGIN)
        self.assertEqual(self.client.mock_calls, [])
        self.assertEqual(self.outbox.sent, [])

    def test_complete_wheel_terms_need_exact_reviewed_document_and_frozen_pack_pins_before_io(self):
        self.adapters['complete_wheel_review_requested'] = True
        candidate = reviewed_complete_pack()
        valid = reviewed_complete_profile(candidate)
        cases = [(pack(), valid), (candidate, {**valid, 'complete_wheel_frozen_pack_hash': None}),
            (candidate, {**valid, 'complete_wheel_frozen_pack_hash': 'f'*64}),
            (candidate, {**valid, 'complete_wheel_notice_sha256': None}),
            (candidate, {**valid, 'complete_wheel_notice_sha256': 'f'*64})]
        for field in ('agb', 'lieferung', 'bestellhinweis', 'komplettraeder_zahlung'):
            altered = deepcopy(candidate)
            altered['texts'][field] += ' SYNTHETIC changed'
            cases.append((altered, valid))
        altered = deepcopy(candidate)
        altered['attachments'][0]['content'] += b' changed'
        cases.append((altered, valid))
        for value, supplied in cases:
            with self.subTest(pack=value['version'], profile=supplied), self.assertRaisesRegex(
                    PaymentProfileError, 'complete_wheel_terms_not_approved'):
                prepare_production_payments(self.portal, self.adapters, supplied, pack=value,
                    supplier_config=self.supplier_config, stripe_config=self.stripe_config)
        self.construct_gateway.assert_not_called()
        self.construct_outbox.assert_not_called()
        self.portal.get_werkstatt_smtp_config.assert_not_called()
        self.assertNotIn('order_review', self.adapters)

    def test_missing_or_draft_legal_pack_never_arms_checkout(self):
        bad = [None, {}, {**pack(), 'approved': False}, {**pack(), 'scope': 'tyres_only'},
               {**pack(), 'version': 'entwurf:synthetic'}, {**pack(), 'texts': {'agb': 'incomplete'}}]
        for candidate in bad:
            with self.subTest(pack=candidate), self.assertRaises(PaymentProfileError):
                self.prepare(pack=candidate)
        self.construct_gateway.assert_not_called()
        self.assertNotIn('checkout', self.adapters)
        self.assertEqual(self.count('shop_orders_v2'), 0)
        self.assertEqual(self.outbox.sent, [])

    def test_storage_and_real_sender_are_required_before_any_provider_constructor(self):
        self.storage.ready = False
        with self.assertRaises(PaymentProfileError):
            self.prepare()
        self.storage.ready = True
        for smtp in ({'smtp_configured': False, 'from_address': 'info@auto-lackierzentrum.de'},
                     {'smtp_configured': True, 'from_address': 'wrong@example.invalid'}):
            self.portal.get_werkstatt_smtp_config.return_value = smtp
            with self.assertRaises(PaymentProfileError):
                self.prepare()
        self.construct_gateway.assert_not_called()
        self.assertEqual(self.client.mock_calls, [])

    def test_test_mode_buyer_cannot_arm_a_live_payment_workflow(self):
        self.adapters['buyer'] = ShopBuyer(self.storage.buyer_backend(), public_origin=ORIGIN, mode='test', enabled=True)
        with self.assertRaises(PaymentProfileError):
            self.prepare()
        self.assertNotIn('checkout', self.adapters)
        self.assertEqual(self.client.mock_calls, [])

    def test_mismatched_stripe_return_origin_cannot_arm_checkout(self):
        supplied = {**self.stripe_config, 'SHOP_STRIPE_PUBLIC_ORIGIN': 'https://other.example.test'}
        with self.assertRaises(PaymentProfileError):
            self.prepare(stripe_config=supplied)
        self.assertNotIn('checkout', self.adapters)
        self.assertEqual(self.client.mock_calls, [])

    def test_missing_capture_adapter_cannot_publish_payment_readiness(self):
        self.client.v1.payment_intents.capture = None
        with self.assertRaises(PaymentProfileError):
            self.prepare()
        self.assertNotIn('checkout', self.adapters)
        self.assertEqual(self.client.mock_calls, [])

    def test_foreign_legal_seller_cannot_arm_gaertner_checkout(self):
        supplied = pack()
        supplied['seller'] = {**ANBIETER, 'firma': 'SYNTHETIC OTHER SELLER', 'strasse': 'SYNTHETIC other return address'}
        with self.assertRaises(PaymentProfileError):
            self.prepare(pack=supplied)
        self.assertNotIn('checkout', self.adapters)
        self.assertEqual(self.client.mock_calls, [])

    def test_complete_factory_builds_real_journals_but_never_starts_or_calls_providers(self):
        dependencies, start = self.prepare()
        self.assertTrue(dependencies['checkout_service'].readiness()['ready'])
        self.assertTrue(dependencies['consumer_service'].readiness()['checkout_ready'])
        self.assertTrue(self.adapters['purchasing_enabled'])
        self.assertIsNone(self.adapters['workflow']._thread)
        self.assertEqual(start.__self__, self.adapters['workflow'])
        self.assertEqual(self.client.mock_calls, [])
        self.supplier.fresh_inquiry.assert_not_called()
        self.portal.get_werkstatt_imap_config.assert_not_called()
        self.assertEqual(self.outbox.sent, [])
        for table in ('shop_payment_actions_v2', 'shop_fulfillment_v2', 'shop_order_receipts_v2',
                      'shop_order_workflow_v2', 'shop_consumer_acceptances_v2', 'shop_withdrawals_v2'):
            self.assertEqual(self.count(table), 0)

    def test_legal_guard_precedes_real_order_write_and_is_owner_bound(self):
        dependencies, _ = self.prepare()
        snapshot = self.save_quote()
        guarded, consumer = dependencies['order_store'], dependencies['consumer_service']
        with self.assertRaises(ConsumerError):
            guarded.create_order(snapshot['quote_hash'], customer_id='customer-a', mode='live', idempotency_key='SYNTHETIC-order')
        self.assertEqual(self.count('shop_orders_v2'), 0)
        consumer.accept_quote(snapshot['quote_hash'], customer_id='customer-a', pack_hash=consumer.pack_hash, terms_accepted=True, consumer=True)
        with self.assertRaises(ConsumerError):
            guarded.create_order(snapshot['quote_hash'], customer_id='customer-b', mode='live', idempotency_key='SYNTHETIC-other')
        order = guarded.create_order(snapshot['quote_hash'], customer_id='customer-a', mode='live', idempotency_key='SYNTHETIC-order')
        self.assertEqual(self.count('shop_orders_v2'), 1)
        frozen = consumer.contract_pack(order['id'], customer_id='customer-a', mode='live')
        self.assertEqual(set(frozen['texts']), set(DOCS))
        self.assertEqual(frozen['attachments'][0]['content'], b'SYNTHETIC annex')
        self.assertEqual(self.client.mock_calls, [])

    def test_real_checkout_route_preserves_accepted_montage_request_across_retry(self):
        dependencies, _ = self.prepare()
        snapshot = self.save_quote()
        calendar = Mock(return_value=['10:00'])
        self.portal.freie_reifen_slots = calendar
        app = self.portal.app
        app.config.update({flag: True for flag in FLAGS}, SHOP_PUBLIC_ORIGIN=ORIGIN,
                          SHOP_ORDER_MODE='live', SHOP_CONSUMER_ENABLED=True)
        register_shop_runtime(self.portal, catalogue=object(), quote_service=self.quotes,
            authorize_quote=lambda req: 'customer-a', quote_context=lambda req, owner: {},
            quote_sink=lambda *args, **kwargs: None, **dependencies)
        client = app.test_client()
        wish = {'date': '2026-11-24', 'time': '10:00'}
        body = {'quote_hash': snapshot['quote_hash'], 'idempotency_key': 'SYNTHETIC-montage-order',
                'appointment_preference': wish}
        def submit():
            return client.post('/shop/api/v2/orders', base_url=ORIGIN, headers={'Origin': ORIGIN}, json=body)
        self.assertNotEqual(submit().status_code, 200)
        calendar.assert_not_called()
        self.assertEqual(self.count('shop_orders_v2'), 0)
        consumer = dependencies['consumer_service']
        consumer.accept_quote(snapshot['quote_hash'], customer_id='customer-a', pack_hash=consumer.pack_hash,
                              terms_accepted=True, consumer=True)
        first = submit()
        self.assertEqual(first.status_code, 200, first.json)
        order = first.json['order']
        self.assertEqual(order['appointment_preference'], {**wish, 'status': 'requested'})
        self.assertEqual(order['amount_gross_cent'], snapshot['total_gross_cent'])
        calendar.return_value = []
        self.assertEqual(submit().json['order'], order)
        calendar.assert_called_once_with(wish['date'])
        self.assertEqual(self.count('shop_orders_v2'), 1)
        self.assertEqual(self.count('shop_order_appointment_requests_v2'), 1)
        self.assertEqual(self.count('shop_payment_attempts_v2'), 0)
        self.assertEqual(self.client.mock_calls, [])
        self.assertEqual(self.outbox.sent, [])

    def test_actual_revalidation_address_and_authorization_callback_signatures(self):
        self.prepare()
        snapshot = quote(mode='live', context=CONTEXT)
        saga = self.adapters['fulfillment']
        result = saga.revalidate(snapshot, deepcopy(CONTEXT), destinations={'customer'})
        self.assertEqual(result, {'synthetic': True})
        args, kwargs = self.quotes.quote.call_args
        self.assertEqual(args[0]['lines'], [snapshot['request'][1]])
        self.assertEqual(args[0]['lines'][0]['id'], OIL)
        self.assertEqual(kwargs, {'customer_id': 'customer-a', 'vehicle_ref': snapshot['vehicle_ref'], 'delivery_context': CONTEXT})
        with self.assertRaisesRegex(PaymentProfileError, 'delivery_address_unconfirmed'):
            saga.delivery_address('customer', CONTEXT)
        customer_context = {**CONTEXT, 'delivery': {**ADDRESS, 'salutation': 'Frau'}}
        customer = saga.delivery_address('customer', customer_context)
        self.assertEqual(customer['salutation'], 'Frau')
        workshop = saga.delivery_address('workshop', CONTEXT)
        self.assertEqual(customer['name'], ADDRESS['recipient'])
        self.assertEqual(customer['zipcode'], ADDRESS['postcode'])
        self.assertEqual(workshop['zipcode'], '74821')
        self.assertIs(type(customer['neutraldelivery']), int)
        self.assertEqual(customer['neutraldelivery'], 1)
        transport = saga.transport
        # No durable supplier claim exists, so the real circular callback denies.
        self.assertIs(transport._authorize({'configured_mode': 'live', 'reference': 'SYNTHETIC'}), False)
        frozen = self.save_quote()
        order = self.store.create_order(frozen['quote_hash'], customer_id='customer-a', mode='live',
                                       idempotency_key='synthetic-circular-proof')
        identifier = order['id']
        attempt = self.store.claim_payment(identifier, customer_id='customer-a', mode='live')
        self.store.bind_payment(identifier, attempt['id'], customer_id='customer-a', mode='live',
            session_id='cs_live_SYNTHETIC', intent_id='pi_SYNTHETIC', currency='eur',
            amount_gross_cent=order['amount_gross_cent'])
        self.store.apply_verified_event({'event_id': 'evt_SYNTHETIC_AUTH', 'order_id': identifier,
            'attempt_id': attempt['id'], 'mode': 'live', 'currency': 'eur',
            'amount_gross_cent': order['amount_gross_cent'], 'session_id': 'cs_live_SYNTHETIC',
            'intent_id': 'pi_SYNTHETIC', 'kind': 'authorized', 'occurred_at': self.now.isoformat()}, verified=True)
        with patch.object(saga.journal, 'proof', return_value={'synthetic': True}) as resolve:
            self.assertEqual(saga.payment.proof_resolver(identifier, operation='capture'), {'synthetic': True})
            resolve.assert_called_once_with(identifier, operation='capture', mode='live')
        self.assertEqual(self.client.mock_calls, [])

    def test_idle_worker_pulse_delivers_a_real_synthetic_withdrawal_once(self):
        dependencies, _ = self.prepare()
        consumer = dependencies['consumer_service']
        app = Flask('synthetic-withdrawal-pulse')
        app.config['SHOP_CONSUMER_ENABLED'] = True
        app.register_blueprint(create_consumer_blueprint(consumer))
        client = app.test_client()
        opened = client.get('/shop/widerruf/', base_url=ORIGIN)
        self.assertEqual(opened.status_code, 200)
        csrf = re.search(r'name="csrf" value="([A-Za-z0-9_-]+)"', opened.text).group(1)
        reviewed = client.post('/shop/widerruf/pruefen', base_url=ORIGIN, headers={'Origin': ORIGIN}, data={
            'csrf': csrf, 'name': 'SYNTHETIC Buyer', 'email': 'synthetic@example.invalid',
            'contract_reference': 'SYNTHETIC EXTERNAL CONTRACT', 'scope': 'SYNTHETIC one article'})
        self.assertEqual(reviewed.status_code, 200)
        confirm = re.search(r'name="confirm" value="([A-Za-z0-9_-]+)"', reviewed.text).group(1)
        response = client.post('/shop/widerruf/bestaetigen', base_url=ORIGIN, headers={'Origin': ORIGIN}, data={'csrf': csrf, 'confirm': confirm})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.outbox.sent, [])
        self.assertEqual(self.adapters['workflow'].run_once(), {'processed': False, 'state': 'idle'})
        self.assertEqual(len(self.outbox.sent), 1)
        self.assertEqual(str(self.outbox.sent[0][1]['To']), 'synthetic@example.invalid')
        self.adapters['workflow'].run_once()
        self.assertEqual(len(self.outbox.sent), 1)
        self.assertEqual(self.client.mock_calls, [])
        self.supplier.fresh_inquiry.assert_not_called()

    def test_runtime_missing_legal_dependency_cannot_start_checkout_worker(self):
        app = self.portal.app
        app.config.update({key: True for key in FLAGS}, SHOP_BUYER_ENABLED=True, SHOP_FRONTEND_ENABLED=True, SHOP_CONSUMER_ENABLED=True)
        starter = Mock(side_effect=AssertionError('No worker before legal readiness'))
        app.extensions['shop_production'] = {'start_workflow': starter}
        result = register_shop_runtime(self.portal, catalogue=Mock(), quote_service=self.quotes,
            quote_sink=Mock(), order_store=self.store, buyer_service=self.buyer, checkout_service=Mock())
        self.assertFalse(result['enabled']['SHOP_CONSUMER_ENABLED'])
        self.assertFalse(result['enabled']['SHOP_CHECKOUT_ENABLED'])
        starter.assert_not_called()
        self.assertEqual(app.test_client().get('/shop/api/v2/consumer/terms').status_code, 404)

    def test_runtime_starts_only_the_deferred_worker_after_all_routes_are_registered(self):
        dependencies, _ = self.prepare()
        app = self.portal.app
        app.config.update({key: True for key in FLAGS}, SHOP_BUYER_ENABLED=True, SHOP_FRONTEND_ENABLED=True,
                          SHOP_CONSUMER_ENABLED=True, SHOP_ORDER_MODE='live', SHOP_PUBLIC_ORIGIN=ORIGIN)
        def deferred_start():
            routes = {rule.rule for rule in app.url_map.iter_rules()}
            self.assertTrue({'/shop/api/v2/consumer/terms', '/shop/api/v2/orders', '/shop/widerruf/', '/shop/warenkorb/'} <= routes)
            self.assertTrue(app.extensions['shop_v2']['enabled']['SHOP_CHECKOUT_ENABLED'])
            return True
        starter = Mock(side_effect=deferred_start)
        self.adapters['start_workflow'] = starter
        app.extensions['shop_production'] = self.adapters
        state = register_shop_runtime(self.portal, catalogue=Mock(), quote_service=self.quotes,
            quote_sink=Mock(), buyer_service=self.buyer, **dependencies)
        self.assertTrue(state['enabled']['SHOP_CONSUMER_ENABLED'])
        self.assertTrue(state['enabled']['SHOP_CHECKOUT_ENABLED'])
        starter.assert_called_once_with()
        self.assertIsNone(self.adapters['workflow']._thread)
        self.assertEqual(app.test_client().get('/shop/api/v2/consumer/terms', base_url=ORIGIN).status_code, 200)
        self.assertEqual(self.count('shop_orders_v2'), 0)
        self.assertEqual(self.client.mock_calls, [])
        self.assertEqual(self.outbox.sent, [])


if __name__ == '__main__':
    unittest.main()
