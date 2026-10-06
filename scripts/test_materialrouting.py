"""Signed HTTP routing with real intake/dialog and isolated vehicle-side effects.

Load the actual webhook functions without starting the production app or workers.
All phones, messages, databases and transports below are synthetic.
"""
import ast
from copy import deepcopy
import hashlib
import hmac
import json
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch

from flask import abort, jsonify, request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_materialdialog as dialog_fixtures


class MaterialRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        names = {'normalize_whatsapp_number', 'whatsapp_number_key', 'whatsapp_inbound_text',
                 'handle_whatsapp_inbound_message', 'process_whatsapp_webhook', 'whatsapp_webhook'}
        nodes = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in names]
        if {node.name for node in nodes} != names:
            raise AssertionError('Actual WhatsApp routing functions missing')
        cls.webhook_code = compile(ast.Module(body=nodes, type_ignores=[]), '<actual-whatsapp-routing>', 'exec')

    def setUp(self):
        self.f = dialog_fixtures.DialogTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p, self.channel = self.f.p, self.f.channel
        self.p.WHATSAPP_PHONE_NUMBER_ID = '123456'
        self.p.app.config.update(TESTING=True, MATERIAL_WHATSAPP_SHARE_PORTAL_NUMBER=True,
                                 MATERIAL_WHATSAPP_REPLIES_ENABLED=False)
        self.car_messages, self.car_mail_notices, self.car_records = [], [], set()
        self.employee, self.workshop, self.unknown = '491701111111', '491709999998', '491709999999'
        self.ns = {'app': self.p.app, 'request': request, 'abort': abort, 'jsonify': jsonify,
            'json': json, 'hashlib': hashlib, 'hmac': hmac, 're': re,
            'WHATSAPP_PHONE_NUMBER_ID': self.p.WHATSAPP_PHONE_NUMBER_ID,
            'WHATSAPP_APP_SECRET': self.p.WHATSAPP_APP_SECRET,
            'WHATSAPP_VERIFY_TOKEN': 'synthetic-verify', 'material_channel': self.channel,
            'clean_text': lambda value: value.strip() if isinstance(value, str) else '',
            'whatsapp_bridge_enabled': lambda: True,
            # Include the employee in the old vehicle allowlist: the new routing
            # must still reserve that sender before the vehicle handler runs.
            'whatsapp_workshop_number_keys': lambda: {self.workshop, self.employee},
            'whatsapp_message_exists': lambda provider_id: provider_id in self.car_records,
            'resolve_whatsapp_reply_auftrag_id': lambda *args: (77, ''),
            'get_auftrag': lambda order_id: {'id': 77, 'kunde_name': 'Synthetic'} if order_id == 77 else None,
            'add_chat_nachricht': self.add_vehicle_chat,
            'add_benachrichtigung': lambda *args: None,
            'sende_autohaus_benachrichtigung_mail': lambda *args: self.car_mail_notices.append(args),
            'record_whatsapp_message': lambda *args, **kwargs: self.car_records.add(kwargs['provider_message_id'])}
        exec(self.webhook_code, self.ns)
        self.client = self.p.app.test_client()

    def add_vehicle_chat(self, order_id, author, text):
        self.car_messages.append((order_id, author, text))
        return len(self.car_messages)

    def text(self, key, sender=None, body='Auftrag 77 ist fertig'):
        return {'id': 'wamid.' + key, 'from': sender or self.employee,
                'timestamp': str(int(self.f.f.time)), 'type': 'text', 'text': {'body': body}}

    def envelope(self, *messages, receiver='123456'):
        payload = self.f.f.envelope(phone_id=receiver)
        payload['entry'][0]['changes'][0]['value']['messages'] = list(messages)
        return payload

    def post(self, payload, *, signature_secret=None):
        raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        signature = 'sha256=' + hmac.new((signature_secret or self.p.WHATSAPP_APP_SECRET).encode(), raw, hashlib.sha256).hexdigest()
        return self.client.post('/webhooks/whatsapp', data=raw,
            headers={'Content-Type': 'application/json', 'X-Hub-Signature-256': signature})

    def assert_vehicle_untouched(self):
        self.assertEqual(self.car_messages, [])
        self.assertEqual(self.car_mail_notices, [])
        self.assertEqual(self.car_records, set())

    def test_mixed_signed_webhook_routes_each_item_once_and_replay_is_idempotent(self):
        payload = self.envelope(deepcopy(self.f.f.message),
            self.text('material-text', body='Bitte einen Karton Klebeband bestellen, dringend'),
            self.text('workshop-text', self.workshop), self.text('unknown-text', self.unknown))
        response = self.post(payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['material']['accepted'], 2)
        self.assertEqual(response.json['processed'], 1)
        self.assertEqual(self.car_records, {'wamid.workshop-text'})
        self.assertEqual(len(self.car_messages), 1)
        self.assertEqual(len(self.car_mail_notices), 1)
        self.assertEqual(len(self.channel.status()), 1)
        self.assertEqual(len(self.f.f.sql('SELECT * FROM einkauf_material_texte')), 1)
        replay = self.post(payload)
        self.assertEqual(replay.json['material']['duplicates'], 2)
        self.assertEqual(replay.json['processed'], 0)
        self.assertEqual(len(self.car_messages), 1)
        self.assertEqual(self.f.f.transport.calls, [])
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_shared_receiver_keeps_material_sender_reserved_while_channel_paused(self):
        self.p.app.config['MATERIAL_WHATSAPP_ENABLED'] = False
        with patch.object(self.channel, 'ingest_webhook', side_effect=AssertionError('Paused intake')):
            response = self.post(self.envelope(self.text('employee-paused'), self.text('workshop-paused', self.workshop)))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['processed'], 1)
        self.assertEqual(self.car_records, {'wamid.workshop-paused'})
        self.assertEqual(self.channel.status(), [])
        self.assertEqual(self.f.f.sql('SELECT * FROM einkauf_material_texte'), [])

    def test_revoked_inactive_or_rights_changed_employee_never_falls_into_vehicle_chat(self):
        changes = (
            ('revoked', 'UPDATE einkauf_material_absender SET active=0,revision=revision+1 WHERE id=1'),
            ('inactive', 'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'),
            ('rights', 'UPDATE assistent_rechte SET einkaufen=0,version=version+1 WHERE mitarbeiter_id=1'))
        for key, statement in changes:
            with self.subTest(key=key):
                self.f.f.sql('UPDATE einkauf_material_absender SET active=1 WHERE id=1')
                self.f.f.sql('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
                self.f.f.sql('UPDATE assistent_rechte SET einkaufen=1 WHERE mitarbeiter_id=1')
                self.f.f.sql(statement)
                response = self.post(self.envelope(self.text(key)))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json['material']['accepted'], 0)
                self.assertEqual(response.json['processed'], 0)
                self.assert_vehicle_untouched()

    def test_unknown_sender_still_requires_existing_vehicle_allowlist(self):
        response = self.post(self.envelope(self.text('unknown', self.unknown)))
        self.assertEqual(response.json['material']['accepted'], 0)
        self.assertEqual(response.json['processed'], 0)
        self.assert_vehicle_untouched()

    def test_shared_number_requires_explicit_boolean_opt_in(self):
        for flag in (False, 'true', 1):
            with self.subTest(flag=flag):
                self.p.app.config['MATERIAL_WHATSAPP_SHARE_PORTAL_NUMBER'] = flag
                response = self.post(self.envelope(self.text('not-shared-' + str(flag), self.workshop)))
                self.assertEqual(response.json['processed'], 0)
                self.assert_vehicle_untouched()

    def test_other_material_receiver_remains_exclusive_and_wrong_receiver_is_ignored(self):
        self.p.app.config['MATERIAL_WHATSAPP_PHONE_IDS'] = '123456,654321'
        for receiver in ('654321', '999999', None, 123456, []):
            with self.subTest(receiver=receiver):
                response = self.post(self.envelope(self.text('wrong-receiver', self.workshop), receiver=receiver))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json['processed'], 0)
                self.assert_vehicle_untouched()

    def test_group_messages_are_rejected_by_both_channels_even_during_pause(self):
        for paused in (False, True):
            for location, field, value in (('message', 'group_id', 'synthetic-group'),
                    ('message', 'recipient_type', 'group'), ('context', 'group_id', 'synthetic-group'),
                    ('value', 'group_id', 'synthetic-group'), ('value', 'recipient_type', 'group')):
                with self.subTest(paused=paused, location=location, field=field):
                    self.p.app.config['MATERIAL_WHATSAPP_ENABLED'] = not paused
                    payload = self.envelope(self.text('group-' + field, self.workshop),
                                            self.text('employee-group-' + field))
                    bucket = payload['entry'][0]['changes'][0]['value']
                    if location == 'value':
                        bucket[field] = value
                    else:
                        for message in bucket['messages']:
                            target = message if location == 'message' else message.setdefault('context', {})
                            target[field] = value
                    response = self.post(payload)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json['processed'], 0)
                    self.assertEqual(response.json['material'].get('accepted', 0), 0)
                    self.assert_vehicle_untouched()

    def test_invalid_signature_or_missing_secret_never_reaches_intake_or_vehicle(self):
        payload = self.envelope(self.text('unsigned-car', self.workshop), self.text('unsigned-material'))
        with patch.object(self.channel, 'ingest_webhook') as intake:
            self.assertEqual(self.post(payload, signature_secret='wrong').status_code, 403)
            self.assertEqual(self.client.post('/webhooks/whatsapp', json=payload).status_code, 403)
            self.ns['WHATSAPP_APP_SECRET'] = ''
            self.assertEqual(self.post(payload).status_code, 403)
            intake.assert_not_called()
        self.assert_vehicle_untouched()

    def test_ingest_errors_do_not_fall_back_to_vehicle_chat(self):
        payload = self.envelope(self.text('blocked-car', self.workshop))
        for error, code in ((ValueError('synthetic invalid'), 400), (PermissionError('synthetic denied'), 403)):
            with self.subTest(code=code), patch.object(self.channel, 'ingest_webhook', side_effect=error), \
                    patch.object(self.channel, 'vehicle_routing_exclusions') as routing:
                self.assertEqual(self.post(payload).status_code, code)
                routing.assert_not_called()
                self.assert_vehicle_untouched()

    def test_routing_is_read_after_ingest_so_new_reservation_cannot_fall_through(self):
        original = self.channel.ingest_webhook
        def ingest_then_reserve(raw, signature):
            result = original(raw, signature)
            self.channel.verify_sender(2, '+' + self.workshop, 'Synthetic newly verified sender', confirmed=True)
            return result
        with patch.object(self.channel, 'ingest_webhook', side_effect=ingest_then_reserve):
            response = self.post(self.envelope(self.text('newly-reserved', self.workshop)))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['processed'], 0)
        self.assert_vehicle_untouched()

    def test_routing_database_error_never_falls_back_to_vehicle_chat(self):
        with patch.object(self.channel, 'vehicle_routing_exclusions', side_effect=RuntimeError('synthetic unavailable')):
            with self.assertRaises(RuntimeError):
                self.post(self.envelope(self.text('routing-failure', self.workshop)))
        self.assert_vehicle_untouched()

    def test_missing_channel_preserves_configured_exclusive_receiver(self):
        self.ns['material_channel'] = None
        response = self.post(self.envelope(self.text('missing-service', self.workshop)))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['processed'], 0)
        self.assert_vehicle_untouched()


if __name__ == '__main__':
    unittest.main(verbosity=2)
