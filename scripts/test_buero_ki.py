"""Offline transport checks for the configured document AI; never imports app."""
from pathlib import Path
import json
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_buero_ki import OfficeDocumentAI, OfficeAIUnavailable, register_office_document_ai, OFFICIAL_URL, MAX_RESPONSE_BYTES


SCHEMA = {'type': 'object', 'properties': {}, 'required': [], 'additionalProperties': False}


def response(data=None, *, reason='stop', status=200, refusal=None, tools=None):
    value = {'choices': [{'finish_reason': reason, 'message': {
        'content': json.dumps({} if data is None else data), 'refusal': refusal, 'tool_calls': tools}}]}
    result = Mock(status_code=status)
    result.iter_content.return_value = [json.dumps(value).encode()]
    return result


class OfficeAITransportTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config['OFFICE_AI_ANALYSIS_ENABLED'] = True
        self.http = Mock()
        self.reply = response({'fields': {}, 'warnings': []})
        self.http.post.return_value = self.reply
        self.p = SimpleNamespace(app=self.app, get_ai_config=Mock(return_value={'openai_ready': True, 'openai_model': 'configured-model'}),
            get_openai_api_key=Mock(return_value='synthetic-secret-not-real'), get_requests=Mock(return_value=self.http),
            build_openai_visual_inputs=Mock(return_value=[]), OPENAI_API_URL=OFFICIAL_URL)
        self.service = OfficeDocumentAI(self.p)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.photo = Path(self.temp.name) / 'synthetic.png'
        self.photo.write_bytes(b'synthetic-transport-only')

    def extract(self, **kw):
        return self.service.extract(text='Rechnung\nGesamtbetrag 119,00 EUR', schema=SCHEMA, **kw)

    def test_disabled_registration_and_missing_account_are_inert(self):
        register_office_document_ai(self.p)
        self.http.post.assert_not_called()
        self.p.get_ai_config.assert_not_called()
        self.app.config['OFFICE_AI_ANALYSIS_ENABLED'] = False
        self.assertTrue(self.extract()['unavailable'])
        self.p.get_ai_config.assert_not_called()
        self.app.config['OFFICE_AI_ANALYSIS_ENABLED'] = True
        self.p.get_ai_config.return_value = {'openai_ready': False}
        self.assertTrue(self.extract()['unavailable'])
        self.http.post.assert_not_called()

    def test_one_existing_account_request_has_strict_schema_and_no_tools(self):
        out = self.extract()
        self.assertFalse(out['visual_evidence'])
        self.assertEqual(out['data'], {'fields': {}, 'warnings': []})
        args, kw = self.http.post.call_args
        self.assertEqual(args, (OFFICIAL_URL,))
        self.assertFalse(kw['allow_redirects'])
        self.assertTrue(kw['stream'])
        self.assertLessEqual(kw['timeout'][1], 20)
        payload = kw['json']
        self.assertEqual(payload['model'], 'configured-model')
        self.assertFalse(payload['store'])
        self.assertNotIn('tools', payload)
        self.assertTrue(payload['response_format']['json_schema']['strict'])
        self.reply.close.assert_called_once()

    def test_photo_with_no_ocr_has_trusted_visual_source(self):
        self.p.build_openai_visual_inputs.return_value = [{'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,c3ludGhldGlj'}}]
        out = self.service.extract(text='', schema=SCHEMA, path=self.photo, filename=self.photo.name)
        self.assertTrue(out['visual_evidence'])
        self.assertEqual(len(self.http.post.call_args.kwargs['json']['messages'][1]['content']), 2)

    def test_model_cannot_set_trusted_visual_marker(self):
        self.http.post.return_value = response({'fields': {}, 'warnings': [], 'visual_evidence': True})
        self.assertFalse(self.extract()['visual_evidence'])

    def test_real_analysis_chain_accepts_photo_fields_as_reviewable_proposals(self):
        from werkstatt_buero_analyse import FIELDS, analyse_document
        fields = {name: {'value': '', 'evidence': '', 'confidence': 'unsicher'} for name in FIELDS}
        fields['lieferant'] = {'value': 'Muster GmbH', 'evidence': 'Aussteller: Muster GmbH', 'confidence': 'hoch'}
        fields['betrag_brutto'] = {'value': '119,00', 'evidence': 'Gesamtbetrag 119,00 EUR', 'confidence': 'hoch'}
        self.http.post.return_value = response({'fields': fields, 'warnings': []})
        self.p.build_openai_visual_inputs.return_value = [{'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,c3ludGhldGlj'}}]
        result = analyse_document('', ai_hook=self.service.extract, ai_enabled=True, path=self.photo, filename=self.photo.name)
        self.assertEqual(result['fields']['lieferant'], 'Muster GmbH')
        self.assertEqual(result['fields']['betrag_brutto'], '119.00')
        self.assertEqual(result['field_sources']['betrag_brutto']['source'], 'ai_visual')
        self.assertFalse(result['identity_verified'])

    def test_payment_schema_and_prompt_allow_only_evidenced_transfer_fields_without_tools(self):
        from werkstatt_buero_analyse import AI_FIELDS_SCHEMA, PAYMENT_FIELDS
        self.service.extract(text='SYNTHETIC invoice only',schema=AI_FIELDS_SCHEMA)
        payload=self.http.post.call_args.kwargs['json']
        properties=payload['response_format']['json_schema']['schema']['properties']['fields']
        self.assertTrue(set(PAYMENT_FIELDS)<=set(properties['required']))
        self.assertTrue(set(PAYMENT_FIELDS)<=set(properties['properties']))
        prompt=payload['messages'][0]['content']
        for instruction in ('Kontoinhaber','Verwendungszweck','Keine BIC','Lastschrift','alle vier Überweisungsfelder leer'):
            self.assertIn(instruction,prompt)
        self.assertNotIn('Keine Kontodaten',prompt)
        self.assertNotIn('tools',payload)
        self.http.post.assert_called_once()

    def test_real_adapter_payment_photo_chain_keeps_data_as_unapproved_source_proposals(self):
        from werkstatt_buero_analyse import FIELDS, PAYMENT_FIELDS, analyse_document
        bban='0'*18
        iban='DE'+f'{98-int(bban+"131400")%97:02d}'+bban
        values={'zahlungsempfaenger':('Synthetische Transfer GmbH','Kontoinhaber: Synthetische Transfer GmbH'),
                'iban':(iban,'IBAN: '+iban),'bic':('ZZZZDE00','BIC: ZZZZDE00'),
                'verwendungszweck':('TEST-RE-001','Verwendungszweck: TEST-RE-001')}
        fields={name:{'value':'','evidence':'','confidence':'unsicher'} for name in FIELDS}
        fields.update({name:{'value':value,'evidence':quote,'confidence':'hoch'} for name,(value,quote) in values.items()})
        self.http.post.return_value=response({'fields':fields,'warnings':[]})
        self.p.build_openai_visual_inputs.return_value=[{'type':'image_url','image_url':{'url':'data:image/png;base64,c3ludGhldGlj'}}]
        result=analyse_document('',ai_hook=self.service.extract,ai_enabled=True,path=self.photo,filename=self.photo.name)
        self.assertEqual(set(result['fields']),set(PAYMENT_FIELDS))
        self.assertEqual(result['source'],'ai')
        self.assertFalse(result['identity_verified'])
        self.http.post.assert_called_once()

    def test_remote_image_and_custom_provider_never_receive_document_or_key(self):
        self.p.build_openai_visual_inputs.return_value = [{'type': 'image_url', 'image_url': {'url': 'https://example.invalid/private.png'}}]
        with self.assertRaises(OfficeAIUnavailable):
            self.extract(path=self.photo)
        self.http.post.assert_not_called()
        self.p.OPENAI_API_URL = 'https://example.invalid/api'
        with self.assertRaises(OfficeAIUnavailable):
            self.extract()
        self.http.post.assert_not_called()

    def test_unfinished_refused_and_tool_responses_are_rejected(self):
        for kw in ({'reason': 'length'}, {'refusal': 'cannot analyse'}, {'tools': [{'id': 'not-executed'}]}, {'status': 429}):
            with self.subTest(kw=kw):
                result = response(**kw)
                self.http.post.return_value = result
                self.http.post.reset_mock()
                with self.assertRaises(OfficeAIUnavailable): self.extract()
                self.http.post.assert_called_once()
                result.close.assert_called_once()

    def test_transport_error_is_generic_and_never_retried(self):
        self.http.post.side_effect = RuntimeError('synthetic-secret-not-real private-provider-detail')
        with self.assertRaises(OfficeAIUnavailable) as caught:
            self.extract()
        self.assertNotIn('synthetic-secret', str(caught.exception))
        self.assertNotIn('private-provider', str(caught.exception))
        self.http.post.assert_called_once()

    def test_response_bytes_and_time_are_bounded_and_closed(self):
        self.reply.iter_content.return_value = [b'x' * (MAX_RESPONSE_BYTES + 1)]
        with self.assertRaises(OfficeAIUnavailable): self.extract()
        self.reply.close.assert_called_once()
        self.reply.close.reset_mock()
        self.reply.iter_content.return_value = [b'{}']
        with patch('werkstatt_buero_ki.time.monotonic', side_effect=[0, 1, 31]):
            with self.assertRaises(OfficeAIUnavailable): self.extract()
        self.reply.close.assert_called_once()

    def test_empty_input_without_image_never_calls_provider(self):
        self.assertTrue(self.service.extract(text='', schema=SCHEMA)['unavailable'])
        self.http.post.assert_not_called()

    def test_invalid_json_and_multiple_results_are_rejected(self):
        for data in (b'not-json', b'{"choices":[]}', json.dumps({'choices': [{}, {}]}).encode()):
            with self.subTest(data=data):
                self.reply.iter_content.return_value = [data]
                with self.assertRaises(OfficeAIUnavailable): self.extract()


if __name__ == '__main__':
    unittest.main()
