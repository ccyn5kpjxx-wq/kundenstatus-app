"""A new card intent is allowed only after a verified zero-charge release."""
from datetime import datetime, timezone
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
import test_mos_deferred_deposit as deferred_fixture
from test_postgres_contract_insert_adapter import FakeConnection
from mos_booking.gateway import StripeTestGateway
from mos_booking.production import RefundLedger, StripeLiveGateway


class DepositRetryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deferred_fixture.DeferredDepositTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        deferred_fixture.DeferredDepositTests.tearDownClass()

    def setUp(self):
        self.fixture = deferred_fixture.DeferredDepositTests()
        self.fixture.setUp()
        self.service = self.fixture.service
        self.portal = self.fixture.portal
        self.gateway = self.fixture.gateway

    def released(self, *, test_only=True):
        paid, _ = self.fixture.pay(self.fixture.hold(test_only=test_only))
        with self.fixture.during_authorization_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7 * 86400, funding='debit')
            with self.assertRaisesRegex(ValueError, 'bezahlte Buchung bleibt bestehen'):
                self.service.reconcile_deposit(paid['id'])
        self.assertEqual(self.service._deposit_record(paid['id'])['status'], 'released')
        return paid, intent['id']

    def attempts(self, hold_id):
        db = self.portal.get_db()
        try:
            return [dict(row) for row in db.execute('''SELECT * FROM miet_checkout_deposit_attempts
                WHERE hold_id=? ORDER BY generation''', (hold_id,)).fetchall()]
        finally:
            db.close()

    def test_correct_card_uses_new_generation_and_keeps_old_release_evidence(self):
        paid, old_id = self.released()
        with self.fixture.during_authorization_window(paid), \
                patch.object(self.gateway, 'create', side_effect=AssertionError('No second rent checkout')), \
                patch.object(self.gateway, 'capture_deposit_intent', side_effect=AssertionError('Never capture a deposit')):
            before = self.attempts(paid['id'])
            self.assertTrue(self.service.can_retry_deposit(paid['id']))
            self.assertEqual(self.attempts(paid['id']), before)
            new = self.service.retry_deposit(paid['id'])
            self.assertNotEqual(new['id'], old_id)
            self.gateway.authorize_deposit_intent(new['id'], valid_for_seconds=7 * 86400, funding='credit')
            self.assertTrue(self.service.reconcile_deposit(paid['id'])['ready'])
        rows = self.attempts(paid['id'])
        self.assertEqual([row['generation'] for row in rows], [1, 2])
        self.assertEqual(rows[0]['intent_id'], old_id)
        self.assertEqual((rows[0]['status'], rows[0]['provider_status'], rows[0]['amount_received']),
                         ('released', 'canceled', 0))
        self.assertTrue(rows[0]['released_verified_at'])
        self.assertEqual((rows[1]['intent_id'], rows[1]['status']), (new['id'], 'authorized'))
        self.assertEqual(self.gateway.retrieve_deposit_intent(old_id)['status'], 'canceled')
        self.assertEqual(self.service.read(paid['id'])['payment_intent'], paid['payment_intent'])
        self.assertEqual(self.service.read(paid['id'])['mietvorgang_id'], paid['mietvorgang_id'])
        self.assertEqual(self.fixture.rental_status(paid), 'aktiv')
        with closing(sqlite3.connect(self.gateway.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM demo_sessions').fetchone()[0], 1)

    def test_replayed_retry_reuses_current_generation_before_and_after_card_confirmation(self):
        paid, _ = self.released()
        with self.fixture.during_authorization_window(paid):
            first = self.service.retry_deposit(paid['id'])
            self.assertEqual(self.service.retry_deposit(paid['id'])['id'], first['id'])
            self.gateway.authorize_deposit_intent(first['id'], valid_for_seconds=7 * 86400)
            self.service.reconcile_deposit(paid['id'])
            self.assertEqual(self.service.retry_deposit(paid['id'])['id'], first['id'])
        self.assertEqual(len(self.attempts(paid['id'])), 2)

    def test_two_failed_cards_keep_distinct_creation_and_release_keys(self):
        paid, _ = self.released()
        with self.fixture.during_authorization_window(paid):
            second = self.service.retry_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(second['id'], valid_for_seconds=3600)
            with self.assertRaises(ValueError):
                self.service.reconcile_deposit(paid['id'])
            third = self.service.retry_deposit(paid['id'])
        rows = self.attempts(paid['id'])
        self.assertEqual([row['generation'] for row in rows], [1, 2, 3])
        self.assertEqual([row['status'] for row in rows], ['released', 'released', 'awaiting_card'])
        self.assertEqual(rows[2]['intent_id'], third['id'])
        with closing(sqlite3.connect(self.gateway.path)) as db:
            creation_keys = [row[0] for row in db.execute('SELECT key FROM demo_deposit_intents')]
            release_keys = [row[0] for row in db.execute("SELECT key FROM demo_deposit_operations WHERE action='cancel'")]
        self.assertEqual(len(set(creation_keys)), 3)
        self.assertEqual(len(set(release_keys)), 2)
        self.assertTrue(any(key.endswith('-g2') for key in release_keys))

    def test_unknown_provider_outcome_never_resets_the_released_pointer(self):
        paid, old_id = self.released()
        before = self.attempts(paid['id'])
        with self.fixture.during_authorization_window(paid), \
                patch.object(self.gateway, 'retrieve_deposit_intent', side_effect=OSError('synthetic timeout')), \
                patch.object(self.gateway, 'create_deposit_intent') as create:
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaises(OSError):
                self.service.retry_deposit(paid['id'])
            create.assert_not_called()
        self.assertEqual(self.service._deposit_record(paid['id'])['intent_id'], old_id)
        self.assertEqual(self.attempts(paid['id']), before)

    def test_local_release_alone_is_insufficient_when_fresh_provider_still_has_a_hold(self):
        paid, old_id = self.released()
        canceled = self.gateway.retrieve_deposit_intent(old_id)
        unconfirmed = {**canceled, 'status': 'requires_payment_method'}
        with self.fixture.during_authorization_window(paid), \
                patch.object(self.gateway, 'retrieve_deposit_intent', return_value=unconfirmed), \
                patch.object(self.gateway, 'create_deposit_intent') as create:
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaisesRegex(ValueError, 'sicher freigegeben'):
                self.service.retry_deposit(paid['id'])
            create.assert_not_called()
        self.assertEqual(len(self.attempts(paid['id'])), 1)

    def test_releasing_and_review_states_never_call_provider_or_start_another_generation(self):
        paid, _ = self.released()
        for status in ('releasing', 'review'):
            db = self.portal.get_db()
            try:
                db.execute('UPDATE miet_checkout_deposit_auths SET status=? WHERE hold_id=?', (status, paid['id']))
                db.commit()
            finally:
                db.close()
            with self.subTest(status=status), self.fixture.during_authorization_window(paid), \
                    patch.object(self.gateway, 'retrieve_deposit_intent') as retrieve, \
                    patch.object(self.gateway, 'create_deposit_intent') as create:
                self.assertFalse(self.service.can_retry_deposit(paid['id']))
                with self.assertRaises(ValueError):
                    self.service.retry_deposit(paid['id'])
                retrieve.assert_not_called()
                create.assert_not_called()

    def test_canceled_or_early_booking_cannot_start_a_new_attempt(self):
        paid, _ = self.released()
        with patch.object(self.gateway, 'retrieve_deposit_intent') as retrieve:
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaisesRegex(ValueError, 'zwölf Stunden'):
                self.service.retry_deposit(paid['id'])
            retrieve.assert_not_called()
        RefundLedger(self.service).cancel(paid['id'])
        with self.fixture.during_authorization_window(paid), patch.object(self.gateway, 'retrieve_deposit_intent') as retrieve:
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaisesRegex(ValueError, 'beendete'):
                self.service.retry_deposit(paid['id'])
            retrieve.assert_not_called()

    def test_handed_over_booking_cannot_start_a_new_attempt(self):
        paid, _ = self.released()
        db = self.portal.get_db()
        try:
            db.execute('INSERT INTO miet_checkout_handovers VALUES (?,?,?,?,?,?,1,1,1,1)',
                       (paid['id'],paid['mietvorgang_id'],datetime.now(timezone.utc).isoformat(),
                        'Synthetischer Mitarbeiter',1234,'synthetic-protocol'))
            db.commit()
        finally:
            db.close()
        with self.fixture.during_authorization_window(paid), patch.object(self.gateway, 'retrieve_deposit_intent') as retrieve:
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaisesRegex(ValueError, 'bereits übergebene'):
                self.service.retry_deposit(paid['id'])
            retrieve.assert_not_called()

    def test_racing_retry_adopts_the_single_durable_next_generation(self):
        paid, old_id = self.released()
        retrieve = self.gateway.retrieve_deposit_intent
        nested = []

        def racing_retrieve(intent_id):
            result = retrieve(intent_id)
            if intent_id == old_id and not nested:
                nested.append(None)
                nested[0] = self.service.retry_deposit(paid['id'])
            return result

        with self.fixture.during_authorization_window(paid), patch.object(self.gateway, 'retrieve_deposit_intent', side_effect=racing_retrieve):
            result = self.service.retry_deposit(paid['id'])
        self.assertEqual(result['id'], nested[0]['id'])
        self.assertEqual(len(self.attempts(paid['id'])), 2)

    def test_uncertain_new_attempt_replays_the_same_key_without_losing_old_ids(self):
        paid, old_id = self.released()
        create = self.gateway.create_deposit_intent
        created = []

        def lose_reply(params, key):
            created.append(create(params, key))
            raise OSError('Synthetic lost provider reply')

        with self.fixture.during_authorization_window(paid):
            with patch.object(self.gateway, 'create_deposit_intent', side_effect=lose_reply):
                with self.assertRaises(OSError):
                    self.service.retry_deposit(paid['id'])
            self.assertIsNone(self.service._deposit_record(paid['id'])['intent_id'])
            result = self.service.retry_deposit(paid['id'])
        self.assertEqual(result['id'], created[0]['id'])
        self.assertEqual(self.attempts(paid['id'])[0]['intent_id'], old_id)
        self.assertEqual(len(self.attempts(paid['id'])), 2)

    def test_expired_provider_hold_is_synced_without_canceling_or_releasing_the_rental(self):
        paid, _ = self.fixture.pay(self.fixture.hold())
        with self.fixture.during_authorization_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7 * 86400)
            self.service.reconcile_deposit(paid['id'])
            self.fixture.clock += 8 * 86400
        expired_at = datetime.fromtimestamp(self.fixture.clock, timezone.utc)

        class AfterExpiry(datetime):
            @classmethod
            def now(cls, tz=None):
                return expired_at.astimezone(tz) if tz else expired_at.replace(tzinfo=None)

        with patch('mietwagen_checkout.datetime', AfterExpiry):
            with patch.object(self.gateway, 'cancel_deposit_intent', side_effect=AssertionError('Already expired at provider')):
                self.assertFalse(self.service.reconcile_deposit(paid['id'])['ready'])
            self.assertFalse(self.service.can_retry_deposit(paid['id']))
            with self.assertRaisesRegex(ValueError, 'Rückgabetermin'):
                self.service.retry_deposit(paid['id'])
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
        self.assertEqual(self.fixture.rental_status(paid), 'aktiv')

    def test_provider_canceled_early_inside_window_can_be_corrected(self):
        paid, _ = self.fixture.pay(self.fixture.hold())
        with self.fixture.during_authorization_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7 * 86400)
            self.service.reconcile_deposit(paid['id'])
            self.gateway.cancel_deposit_intent(intent['id'], 'synthetic-provider-early-cancel')
            self.assertFalse(self.service.reconcile_deposit(paid['id'])['ready'])
            self.assertTrue(self.service.can_retry_deposit(paid['id']))
            corrected = self.service.retry_deposit(paid['id'])
        self.assertNotEqual(corrected['id'], intent['id'])
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')

    def test_paused_gateway_requires_server_journal_evidence_for_generation_key(self):
        paid, _ = self.released(test_only=False)
        with self.fixture.during_authorization_window(paid), patch.object(self.gateway, 'create_deposit_intent', side_effect=OSError('Before create')):
            with self.assertRaises(OSError):
                self.service.retry_deposit(paid['id'])
        attempt = self.attempts(paid['id'])[-1]
        live_hold = {**paid, 'session_id': 'cs_live_synthetic'}
        gateway = object.__new__(StripeLiveGateway)
        gateway.settlement_only = True
        params = {'amount': 50000, 'currency': 'eur', 'capture_method': 'manual',
                  'payment_method_types': ['card'],
                  'metadata': {'hold_id': paid['id'], 'quote_hash': paid['fingerprint']}}
        key = 'mos-deposit-' + paid['id'] + '-g2'
        with self.fixture.during_authorization_window(paid), patch.object(StripeTestGateway, 'create_deposit_intent', return_value={'id': 'pi_synthetic'}) as create:
            self.assertEqual(gateway.create_existing_deposit_intent(params, key, live_hold, attempt=attempt)['id'], 'pi_synthetic')
            create.assert_called_once_with(params, key)
            for evidence in (None, {**attempt, 'status': 'review'}, {**attempt, 'generation': 3}):
                with self.subTest(evidence=evidence), self.assertRaises(ValueError):
                    gateway.create_existing_deposit_intent(params, key, live_hold, attempt=evidence)

    def test_journal_inserts_pass_the_real_postgres_adapter_without_returning_id(self):
        statements = []
        original = self.portal.get_db

        def traced_connection():
            db = original()
            db.set_trace_callback(statements.append)
            return db

        with patch.object(self.portal, 'get_db', side_effect=traced_connection):
            paid, _ = self.released()
            with self.fixture.during_authorization_window(paid):
                self.service.retry_deposit(paid['id'])
        inserts = [statement for statement in statements
                   if statement.lstrip().upper().startswith('INSERT INTO MIET_CHECKOUT_DEPOSIT_ATTEMPTS')]
        self.assertEqual(len(inserts), 2)
        for sql in inserts:
            fake = FakeConnection()
            self.portal.PostgresConnection(fake).execute(sql)
            self.assertIn('RETURNING generation', fake.statements[0][0])
            self.assertNotIn('RETURNING id', fake.statements[0][0])


if __name__ == '__main__':
    unittest.main()
