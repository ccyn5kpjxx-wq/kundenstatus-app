"""Offline personal form -> original -> guarded material order regressions."""
import copy
import ast
from datetime import datetime, timezone
from contextlib import nullcontext, contextmanager
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from jinja2 import DictLoader
from PIL import Image
from werkzeug.security import generate_password_hash, check_password_hash
from flask import abort, jsonify, request, session, redirect, url_for

import test_materialdialog as dialog_fixtures
import test_materialbestellung_e2e as e2e_fixtures
from werkstatt_materialbestellung import (register_material_order_portal, PORTAL_SOURCE,
                                         MAX_PHOTO_BYTES, MAX_TOTAL_BYTES, SubmissionConflict)
from werkstatt_bestellplan import BERLIN


def uid():
    return str(uuid.uuid4())


def picture(color='blue', format='PNG'):
    buffer = io.BytesIO()
    Image.new('RGB', (12, 12), color).save(buffer, format)
    return buffer.getvalue()


class PortalTests(unittest.TestCase):
    def setUp(self):
        self.f = dialog_fixtures.DialogTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p = self.f.p
        self.p.portal_originals_operation_lock = nullcontext
        self.p.app.config.update(TESTING=True, SECRET_KEY='synthetic-only', ASSISTANT_NATIVE_COCKPIT=True,
                                 MAX_CONTENT_LENGTH=25 * 1024 * 1024)
        self.p.app.jinja_loader = DictLoader({'materialbestellung.html': '{{ auth }} {{ can_order }} {{ who.actor if who else "" }} {{ csrf_token }}'})
        self.f.f.sql("ALTER TABLE assistent_rechte ADD COLUMN passwort_hash TEXT NOT NULL DEFAULT 'synthetic'")
        self.f.f.sql('''CREATE TABLE assistent_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT,auftrag_id INTEGER,aktion TEXT,details TEXT,zeit TEXT)''')
        self.portal = register_material_order_portal(self.p)
        # Execute the real nested login function against this isolated DB. No
        # app import, live config, external connection or worker is involved.
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'werkstatt_assistent.py').read_text(encoding='utf-8'))
        register = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'register_assistant')
        login = copy.deepcopy(next(node for node in register.body if isinstance(node, ast.FunctionDef) and node.name == 'login'))
        login.decorator_list = []
        namespace = dict(p=self.p, db_scope=self.portal.db, abort=abort, jsonify=jsonify, request=request,
                         session=session, redirect=redirect, url_for=url_for, check_password_hash=check_password_hash,
                         secrets=__import__('secrets'))
        self.p.login_rate_limit_status = lambda *_: (False, None)
        self.p.record_failed_login = self.p.clear_login_attempts = lambda *_: None
        exec(compile(ast.fix_missing_locations(ast.Module(body=[login], type_ignores=[])), 'actual-login', 'exec'), namespace)
        self.p.app.add_url_rule('/werkstatt/assistent/login', endpoint='assistent.login', view_func=namespace['login'], methods=['POST'])
        self.p.app.add_url_rule('/werkstatt/assistent', endpoint='assistent.page', view_func=lambda: 'assistant')
        self.f.f.sql('UPDATE assistent_rechte SET passwort_hash=?', (generate_password_hash('synthetic-password-123'),))
        self.client = self.p.app.test_client()
        self.login()

    def login(self, mid=1, version=1, admin=False):
        with self.client.session_transaction() as session:
            session.clear()
            session['assistent_mid'] = mid
            session['assistent_version'] = version
            session['csrf_token'] = 'synthetic-csrf'
            if admin:
                session['admin'] = True

    def who(self, mid=1):
        with self.p.app.test_request_context():
            from flask import session
            session['assistent_mid'] = mid
            session['assistent_version'] = 1
            return self.portal.identity()

    def payload(self, rows=None, request_id=None):
        rows = rows or [(uid(), 1, False, picture())]
        data = {'request_id': request_id or uid(),
                'positionen': json.dumps([{'id': row[0], 'menge': row[1], 'dringend': row[2]} for row in rows]),
                'csrf_token': 'synthetic-csrf'}
        for client_id, _, _, raw in rows:
            data['foto_' + client_id] = (io.BytesIO(raw), 'test.png')
        return data

    def submit(self, rows=None, request_id=None):
        return self.client.post('/werkstatt/materialbestellung/anforderungen',
                                data=self.payload(rows, request_id))

    def sql(self, query, values=()):
        return self.f.f.sql(query, values)

    def source(self, draft_id):
        return self.sql('''SELECT n.* FROM einkauf_material_nachrichten n
            JOIN einkauf_material_dialoge d ON d.message_id=n.id WHERE d.id=?''', (draft_id,))[0]

    def analyze(self, view):
        self.p.material_dialog.analyze(view['id'])
        return self.p.material_dialog.status(view['id'])

    def answer(self, view, answer, request_id=None):
        return self.client.post('/werkstatt/materialbestellung/anforderungen/' + str(view['id']) + '/antwort',
            json={'revision': view['revision'], 'antwort': answer, 'request_id': request_id or uid()},
            headers={'X-CSRF-Token': 'synthetic-csrf'})

    def get(self, query=''):
        return self.client.get('/werkstatt/materialbestellung/anforderungen' + query,
                               headers={'X-CSRF-Token': 'synthetic-csrf'})

    def test_page_is_public_but_shows_only_personal_access(self):
        self.assertIn('True True mitarbeiter:1', self.client.get('/werkstatt/materialbestellung').text)
        with self.client.session_transaction() as session:
            session.clear()
            session['admin'] = True
        response = self.client.get('/werkstatt/materialbestellung')
        self.assertEqual(response.status_code, 200)
        self.assertIn('False False', response.text)
        self.assertNotIn('admin', response.text)

    def test_own_session_wins_when_admin_session_also_exists(self):
        self.login(mid=2, admin=True)
        view = self.submit().json['anforderungen'][0]
        self.assertEqual(self.source(view['id'])['employee_id'], 2)
        self.assertEqual(self.sql('SELECT actor FROM assistent_audit')[0]['actor'], 'mitarbeiter:2')

    def test_anonymous_admin_stale_and_nonbuyer_rejected_before_upload_read(self):
        for session_values in ({}, {'admin': True}, {'assistent_mid': 1, 'assistent_version': 2},
                               {'assistent_mid': True, 'assistent_version': 1}):
            with self.client.session_transaction() as session:
                session.clear()
                session.update(session_values)
            result = self.submit()
            self.assertEqual(result.status_code, 401)
            self.assertFalse(result.json['accepted'])
        self.login()
        self.sql('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.submit().status_code, 403)
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])

    def test_csrf_is_required_even_in_isolated_blueprint(self):
        data = self.payload()
        data['csrf_token'] = 'wrong'
        self.assertEqual(self.client.post('/werkstatt/materialbestellung/anforderungen', data=data).status_code, 403)
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])

    def test_defaults_are_photo_one_piece_regular_with_real_material_source(self):
        result = self.submit()
        self.assertEqual(result.status_code, 200, result.text)
        view = result.json['anforderungen'][0]
        self.assertEqual((view['quantity'], view['unit'], view['urgent']), ('1', 'Stück', False))
        self.assertEqual(view['request_id'], result.json['request_id'])
        source = self.source(view['id'])
        self.assertEqual(source['phone_number_id'], PORTAL_SOURCE)
        self.assertEqual(source['sender_id'], 0)
        draft = self.p.material_dialog.status(view['id'])
        self.assertTrue(draft['fields']['order_requested']['value'])
        self.assertEqual(draft['fields']['quantity']['proof']['id'], source['id'])
        self.assertEqual(self.p.workshop_orders.calls, [])
        self.assertEqual(self.f.vision_calls, [])
        intake = self.p.workshop_intake.detail(source['intake_id'])
        self.assertEqual(intake['lines'][0]['quantity'], '1')
        self.assertEqual(intake['original_author'], 'Testperson Eins')
        self.assertEqual(intake['created_by'], 'mitarbeiter:1')

    def test_ten_photos_have_distinct_immutable_quantity_bindings(self):
        rows = [(uid(), i + 1, bool(i % 2), picture('blue' if i % 2 else 'red')) for i in range(10)]
        response = self.submit(rows)
        self.assertEqual(response.status_code, 200, response.text)
        views = response.json['anforderungen']
        self.assertEqual(len({view['id'] for view in views}), 10)
        self.assertEqual({view['client_id']: (view['quantity'], view['urgent']) for view in views},
                         {row[0]: (str(row[1]), row[2]) for row in rows})
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_eingang_dateien')), 10)
        self.assertEqual(len(self.sql('SELECT id FROM assistent_audit')), 1)

    def test_invalid_later_photo_or_late_service_failure_rolls_back_entire_batch(self):
        result = self.submit([(uid(), 1, False, picture()), (uid(), 2, False, b'not an image')])
        self.assertEqual(result.status_code, 400)
        self.assertFalse(result.json['accepted'])
        for table in ('einkauf_eingang', 'einkauf_eingang_dateien', 'assistent_materialfotos', 'einkauf_material_nachrichten', 'einkauf_material_dialoge'):
            self.assertEqual(self.sql('SELECT id FROM ' + table), [])
        original = self.p.material_dialog._ensure
        calls = []
        def fail_second(db, source, text=None):
            calls.append(True)
            if len(calls) == 2:
                raise ValueError('synthetic failure')
            return original(db, source, text)
        with patch('werkstatt_materialdialog.MaterialDialog._ensure', side_effect=fail_second):
            result = self.submit([(uid(), 1, False, picture()), (uid(), 2, False, picture('red'))])
        self.assertEqual(result.status_code, 400)
        for table in ('einkauf_eingang', 'einkauf_eingang_dateien', 'assistent_materialfotos', 'einkauf_material_nachrichten', 'einkauf_material_dialoge', 'assistent_audit'):
            self.assertEqual(self.sql('SELECT id FROM ' + table), [])

    def test_replay_preserves_batch_ids_and_originals_without_second_audit(self):
        request_id = uid()
        rows = [(uid(), 2, True, picture()), (uid(), 5, False, picture('red'))]
        first = self.submit(rows, request_id).json
        replay = self.submit(list(reversed(rows)), request_id)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(first, replay.json)
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_material_nachrichten')), 2)
        self.assertEqual(len(self.sql('SELECT id FROM assistent_audit')), 1)

    def test_changed_payload_same_request_id_conflicts_even_for_added_removed_item(self):
        request_id, a, b = uid(), uid(), uid()
        rows = [(a, 1, False, picture()), (b, 2, True, picture('red'))]
        self.assertEqual(self.submit(rows, request_id).status_code, 200)
        variants = [[(a, 9, False, picture()), rows[1]], [(a, 1, True, picture()), rows[1]],
                    [(a, 1, False, picture('green')), rows[1]], rows[:1], rows + [(uid(), 1, False, picture())],
                    [(uid(), 1, False, picture()), rows[1]]]
        for changed in variants:
            result = self.submit(changed, request_id)
            self.assertEqual(result.status_code, 409, result.text)
            self.assertFalse(result.json['accepted'])
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_material_nachrichten')), 2)

    def test_quantity_urgency_ids_and_file_binding_are_strict(self):
        for quantity in (0, -1, True, '1', 1.2, 1000):
            self.assertEqual(self.submit([(uid(), quantity, False, picture())]).status_code, 400)
        for urgent in (None, 1, 'false'):
            self.assertEqual(self.submit([(uid(), 1, urgent, picture())]).status_code, 400)
        same = uid()
        self.assertEqual(self.submit([(same, 1, False, picture()), (same, 2, True, picture())]).status_code, 400)
        self.assertEqual(self.submit(request_id='../../invalid').status_code, 400)
        rows = [(uid(), 1, False, picture()) for _ in range(11)]
        self.assertEqual(self.submit(rows).status_code, 400)
        payload = self.payload()
        payload['foto_foreign'] = (io.BytesIO(picture()), 'foreign.png')
        self.assertEqual(self.client.post('/werkstatt/materialbestellung/anforderungen', data=payload).status_code, 400)
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])

    def test_photo_and_total_size_limits_precede_any_write(self):
        with patch('werkstatt_materialbestellung.MAX_PHOTO_BYTES', 32):
            self.assertEqual(self.submit().status_code, 400)
        with patch('werkstatt_materialbestellung.MAX_TOTAL_BYTES', len(picture())):
            self.assertEqual(self.submit([(uid(), 1, False, picture()), (uid(), 2, False, picture())]).status_code, 400)
        self.assertEqual(self.sql('SELECT id FROM einkauf_eingang'), [])

    def test_actual_original_bytes_preserved_for_png_jpeg_and_webp(self):
        for format in ('PNG', 'JPEG', 'WEBP'):
            raw = picture(format=format)
            view = self.submit([(uid(), 1, False, raw)]).json['anforderungen'][0]
            source = self.source(view['id'])
            restored, mime, _ = self.p.workshop_intake.original(source['intake_id'], source['file_id'])
            self.assertEqual(restored, raw)
            self.assertEqual(mime, {'PNG': 'image/png', 'JPEG': 'image/jpeg', 'WEBP': 'image/webp'}[format])

    def test_rights_revoked_during_image_validation_prevent_whole_commit(self):
        from werkstatt_materialbestellung import _image
        def revoke(raw):
            self.sql('UPDATE assistent_rechte SET version=2,einkaufen=0 WHERE mitarbeiter_id=1')
            return _image(raw)
        with patch('werkstatt_materialbestellung._image', side_effect=revoke):
            result = self.submit()
        self.assertEqual(result.status_code, 403)
        self.assertEqual(self.sql('SELECT id FROM einkauf_eingang'), [])

    def test_status_is_personal_and_does_not_leak_other_employee(self):
        one = self.submit().json['anforderungen'][0]
        self.login(2)
        self.assertEqual(self.get().json['anforderungen'], [])
        two = self.submit().json['anforderungen'][0]
        self.login(1)
        self.assertEqual([row['id'] for row in self.get().json['anforderungen']], [one['id']])
        self.assertEqual(self.answer(two, 'ja').status_code, 403)

    def test_portal_analysis_independent_of_meta_pause_and_no_meta_question_send(self):
        view = self.submit().json['anforderungen'][0]
        self.p.app.config['MATERIAL_WHATSAPP_ENABLED'] = False
        self.p.WHATSAPP_ACCESS_TOKEN = ''
        result = self.p.material_channel.worker_tick()
        self.assertEqual(result['id'], view['id'])
        self.assertEqual(len(self.f.vision_calls), 1)
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED'] = True
        self.assertIsNone(self.p.material_dialog.send_question())
        self.assertEqual(self.f.f.transport.calls, [])

    def test_worker_rechecks_revocation_and_stops_dispatch(self):
        view = self.submit().json['anforderungen'][0]
        self.sql('UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.portal.process_next()['state'], 'review')
        self.assertEqual(self.f.vision_calls, [])
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_bound_clarification_and_replay_keep_one_draft(self):
        view = self.submit().json['anforderungen'][0]
        analyzed = self.analyze(view)
        request_id = uid()
        result = self.answer(analyzed, '2', request_id)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json['anforderungen'][0]['quantity'], '2')
        replay = self.answer(analyzed, '2', request_id)
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_material_dialoge')), 1)
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_material_texte')), 1)
        self.assertEqual(self.answer(analyzed, '3', request_id).status_code, 409)
        self.assertEqual(self.answer(analyzed, '3').status_code, 409)

    def test_answer_rejects_foreign_metadata_and_embedded_other_draft(self):
        view = self.submit().json['anforderungen'][0]
        self.assertEqual(self.answer(view, 'M-999 R1: ja').status_code, 400)
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_texte'), [])

    def test_duplicate_requires_personal_bound_yes_and_no_cancels_only_extra(self):
        first = self.analyze(self.submit().json['anforderungen'][0])
        self.f.review(first, unit='Stück')
        second = self.analyze(self.submit().json['anforderungen'][0])
        self.assertIn('possible_duplicate', second['missing_fields'])
        result = self.answer(second, 'nein')
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json['anforderungen'][0]['state'], 'cancelled')
        third = self.analyze(self.submit().json['anforderungen'][0])
        self.assertIn('possible_duplicate', third['missing_fields'])
        result = self.answer(third, 'ja')
        self.assertEqual(result.status_code, 200, result.text)
        updated = self.p.material_dialog.status(third['id'])
        if updated['state'] != 'approved':
            updated = self.f.review(updated, unit='Stück')
        approved = self.p.material_dialog.approved_order(third['id'], updated['revision'])
        self.assertEqual(approved['actor'], 'mitarbeiter:1')

    def test_after_durable_handoff_all_changes_are_rejected(self):
        view = self.analyze(self.submit().json['anforderungen'][0])
        view = self.f.review(view, unit='Stück')
        self.p.workshop_orders.submit_material_request(view['id'], view['revision'])
        view = self.p.material_dialog.status(view['id'])
        self.assertEqual(self.answer(view, 'abbrechen').status_code, 409)
        self.assertEqual(self.answer(view, '2').status_code, 409)

    def test_portal_source_spoofed_person_or_sender_cannot_pass_guard(self):
        view = self.submit().json['anforderungen'][0]
        source = self.source(view['id'])
        for fields in ({'employee_id': 2}, {'sender_id': self.f.f.sender['id']}, {'forwarded': 1}, {'sender_revision': 7}):
            altered = dict(source, **fields)
            with self.portal.db() as db:
                with self.assertRaises(PermissionError):
                    self.p.material_channel._active(db, altered)

    def test_registration_never_starts_worker_or_grants_access(self):
        before = self.sql('SELECT * FROM assistent_rechte')
        self.assertIs(register_material_order_portal(self.p), self.portal)
        self.assertEqual(self.sql('SELECT * FROM assistent_rechte'), before)
        self.assertIsNone(self.p.material_channel.worker_thread)
        self.assertEqual(len(self.sql('SELECT id FROM einkauf_material_absender')), 1)

    def test_successful_login_rotates_personal_page_binding_and_whitelists_return(self):
        stale_payload = self.payload()
        login = self.client.post('/werkstatt/assistent/login', data={
            'mitarbeiter_id': 2, 'password': 'synthetic-password-123', 'next': '/werkstatt/materialbestellung'})
        self.assertEqual(login.location, '/werkstatt/materialbestellung')
        with self.client.session_transaction() as session:
            self.assertEqual(session['assistent_mid'], 2)
            self.assertNotEqual(session['csrf_token'], 'synthetic-csrf')
        stale = self.client.post('/werkstatt/materialbestellung/anforderungen', data=stale_payload,
                                 headers={'X-CSRF-Token': 'synthetic-csrf'})
        self.assertEqual(stale.status_code, 403)
        self.assertFalse(stale.json['accepted'])
        self.assertTrue(stale.json['reload_required'])
        self.assertEqual(self.get().status_code, 403)
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])
        login = self.client.post('/werkstatt/assistent/login', data={
            'mitarbeiter_id': 1, 'password': 'synthetic-password-123', 'next': 'https://evil.example'})
        self.assertEqual(login.location, '/werkstatt/assistent')

    def test_exact_batch_lookup_preserves_reference_and_is_not_history_limited(self):
        request_id = uid()
        response = self.submit(request_id=request_id)
        original = response.json['anforderungen'][0]
        # 100 unrelated rows need not contain real originals to prove the
        # SQL limit behavior. The exact query excludes them before rendering.
        source = self.source(original['id'])
        with self.portal.db() as db:
            for i in range(101):
                ref = 'portal.1.' + uid() + '.' + uid()
                cursor = db.execute('''INSERT INTO einkauf_material_nachrichten
                    (phone_number_id,wamid,canonical_hash,sender_id,sender_revision,employee_id,employee_name,
                    rights_version,media_id,mime,expected_sha256,source_at,received_at,state,updated_at)
                    VALUES(?,?,?,0,1,1,'Testperson Eins',1,'','image/png',?,? ,?,'ready',?) RETURNING id''',
                    (PORTAL_SOURCE, ref, source['canonical_hash'], source['expected_sha256'], source['source_at'], self.portal.clock(), self.portal.clock())).fetchone()
                db.execute('INSERT INTO einkauf_material_dialoge(message_id,analysis_state,created_at,updated_at) VALUES(?,\'failed\',?,?)',
                           (cursor['id'], self.portal.clock(), self.portal.clock()))
        result = self.get('?request_id=' + request_id)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json['request_id'], request_id)
        self.assertEqual(result.json['anforderungen'], [original])
        self.assertEqual(self.get('?request_id=' + uid()).json['anforderungen'], [])
        self.assertEqual(self.get('?request_id=invalid').status_code, 400)

    def test_personal_questions_do_not_require_whatsapp_quote(self):
        view = self.submit().json['anforderungen'][0]
        self.f.hits = []
        self.p.assistant_material_photos.vision = lambda *_: {'art': 'unklar'}
        self.analyze(view)
        question = self.get().json['anforderungen'][0]['questions'][0]['body']
        self.assertNotIn('zitiert', question)
        self.assertFalse(question.startswith('M-'))
        original = self.p.material_dialog.status(view['id'])['questions'][0]['body']
        self.assertIn('zitiert', original)

    def test_cancel_and_rights_change_during_vision_do_not_resurrect_order(self):
        view = self.submit().json['anforderungen'][0]
        def cancel_during_vision(*args):
            current = self.p.material_dialog.status(view['id'])
            self.assertEqual(self.answer(current, 'abbrechen').status_code, 200)
            return {'art': 'produkt', 'produkt': 'Test-Klebeband', 'breite': '50 mm', 'farbe': 'grün'}
        self.p.assistant_material_photos.vision = cancel_during_vision
        self.p.material_dialog.analyze(view['id'])
        self.assertEqual(self.p.material_dialog.status(view['id'])['state'], 'cancelled')
        self.assertEqual(self.p.workshop_orders.calls, [])
        view = self.submit().json['anforderungen'][0]
        def revoke_during_vision(*args):
            self.sql('UPDATE assistent_rechte SET version=2,einkaufen=0 WHERE mitarbeiter_id=1')
            return {'art': 'produkt', 'produkt': 'Test-Klebeband', 'breite': '50 mm', 'farbe': 'grün'}
        self.p.assistant_material_photos.vision = revoke_during_vision
        self.p.material_dialog.analyze(view['id'])
        current = self.p.material_dialog.status(view['id'])
        self.assertEqual(current['analysis_state'], 'failed')
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_count_correction_during_vision_keeps_new_quantity_and_bound_proof(self):
        view = self.submit().json['anforderungen'][0]
        def correct_during_vision(*args):
            current = self.p.material_dialog.status(view['id'])
            self.assertEqual(self.answer(current, '3').status_code, 200)
            return {'art': 'produkt', 'produkt': 'Test-Klebeband', 'breite': '50 mm', 'farbe': 'grün'}
        self.p.assistant_material_photos.vision = correct_during_vision
        updated = self.p.material_dialog.analyze(view['id'])
        self.assertEqual(updated['fields']['quantity']['value'], '3')
        self.assertEqual(updated['fields']['quantity']['proof']['kind'], 'text')
        self.assertEqual(updated['fields']['quantity']['proof']['employee_id'], 1)
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_50mb_route_limit_precedes_global_csrf_form_parser_without_global_change(self):
        parsed = []
        def global_csrf_parser():
            if request.method == 'POST':
                request.form.get('csrf_token')
                parsed.append(True)
        self.p.app.before_request_funcs.setdefault(None, []).append(global_csrf_parser)
        # The validated PNG may contain trailing original bytes. Four 7 MB
        # originals exceed the normal 25 MB body cap but remain within this
        # personal endpoint's independent 8 MB/file and 50 MB/batch bounds.
        raw = picture() + b'\0' * (7 * 1024 * 1024)
        result = self.submit([(uid(), 1, False, raw) for _ in range(4)])
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(len(result.json['anforderungen']), 4)
        self.assertTrue(parsed)
        self.assertEqual(self.p.app.config['MAX_CONTENT_LENGTH'], 25 * 1024 * 1024)
        result.request.environ['wsgi.input'].close()
        result.close()

    def test_overall_body_limit_rejects_before_any_batch_mutation(self):
        from werkstatt_materialbestellung import MAX_BODY_BYTES
        result = self.client.post('/werkstatt/materialbestellung/anforderungen',
            headers={'X-CSRF-Token': 'synthetic-csrf'},
            environ_overrides={'CONTENT_LENGTH': str(MAX_BODY_BYTES + 1)})
        self.assertEqual(result.status_code, 413)
        self.assertFalse(result.json['accepted'])
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])

    def test_submit_answer_and_worker_keep_originals_lock_for_each_mutation(self):
        depth = [0]
        @contextmanager
        def operation_lock():
            depth[0] += 1
            try:
                yield
            finally:
                depth[0] -= 1
        original_db = self.p.get_db
        class GuardedConnection:
            def __init__(connection_self):
                connection_self.connection = original_db()
            def execute(connection_self, query, args=()):
                if query.lstrip().split(' ', 1)[0].upper() in {'INSERT', 'UPDATE', 'DELETE'}:
                    self.assertGreater(depth[0], 0, query)
                return connection_self.connection.execute(query, args)
            def __getattr__(connection_self, key):
                return getattr(connection_self.connection, key)
        self.p.portal_originals_operation_lock = operation_lock
        self.p.get_db = GuardedConnection
        view = self.submit().json['anforderungen'][0]
        self.assertEqual(depth[0], 0)
        self.portal.process_next()
        current = self.p.material_dialog.status(view['id'])
        self.assertEqual(self.answer(current, '2').status_code, 200)
        self.assertEqual(depth[0], 0)

    def test_lock_failure_is_not_swallowed_or_replaced_with_unprotected_writes(self):
        @contextmanager
        def failed_lock():
            raise OSError('synthetic lock failure')
            yield
        self.p.portal_originals_operation_lock = failed_lock
        with self.assertRaises(OSError):
            self.submit()
        self.assertEqual(self.sql('SELECT id FROM einkauf_material_nachrichten'), [])
        with self.assertRaises(OSError):
            self.portal.process_next()


class PortalRealOrderTests(unittest.TestCase):
    def setUp(self):
        self.e = e2e_fixtures.MaterialPurchaseEndToEndTests('runTest')
        self.e.setUp()
        self.addCleanup(self.e.doCleanups)
        self.p, self.f = self.e.p, self.e.f
        self.p.portal_originals_operation_lock = nullcontext
        self.p.app.config['SECRET_KEY'] = 'synthetic-only'
        self.f.f.sql("ALTER TABLE assistent_rechte ADD COLUMN passwort_hash TEXT NOT NULL DEFAULT 'synthetic'")
        self.f.f.sql('''CREATE TABLE IF NOT EXISTS assistent_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT,auftrag_id INTEGER,aktion TEXT,details TEXT,zeit TEXT)''')
        self.portal = register_material_order_portal(self.p)
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as session:
            session['assistent_mid'] = 1
            session['assistent_version'] = 1
            session['csrf_token'] = 'synthetic-csrf'

    def demand(self, urgent=False, *, price=1000, extras=0):
        request_id, client_id = uid(), uid()
        result = self.client.post('/werkstatt/materialbestellung/anforderungen', data={
            'request_id': request_id, 'positionen': json.dumps([{'id': client_id, 'menge': 1, 'dringend': urgent}]),
            'foto_' + client_id: (io.BytesIO(picture()), 'test.png'), 'csrf_token': 'synthetic-csrf'})
        self.assertEqual(result.status_code, 200, result.text)
        view = result.json['anforderungen'][0]
        self.p.material_dialog.analyze(view['id'])
        view = self.p.material_dialog.status(view['id'])
        return self.f.review(view, supplier_id=self.e.contact, unit='Stück', unit_price_cents=price,
                             shipping_cents=300, extra_costs_cents=extras)

    def test_regular_form_request_runs_monday14_real_dispatch_once(self):
        view = self.demand()
        self.assertEqual(self.portal.process_next()['state'], 'queued')
        self.assertEqual(self.e.smtp.data_calls, 0)
        self.f.f.time = datetime(2026, 10, 12, 13, 59, tzinfo=BERLIN).timestamp()
        self.e.manager.tick(worker=True)
        self.assertEqual(self.e.smtp.data_calls, 0)
        self.f.f.time = datetime(2026, 10, 12, 14, 0, tzinfo=BERLIN).timestamp()
        self.e.manager.tick(worker=True)
        self.assertEqual(self.e.smtp.data_calls, 1)
        listing = self.client.get('/werkstatt/materialbestellung/anforderungen',
                                  headers={'X-CSRF-Token': 'synthetic-csrf'}).json['anforderungen']
        self.assertEqual(listing[0]['label'], 'Bestellt')
        self.assertEqual(listing[0]['dispatch_state'], 'sent')
        self.e.manager.tick(worker=True)
        self.assertEqual(self.e.smtp.data_calls, 1)
        row = self.p.material_dialog.status(view['id'])
        status = self.e.manager.dispatch.status(row['dispatch_id'])
        with self.portal.db() as db:
            actor = db.execute('SELECT actor_id FROM assistent_bestellanforderungen WHERE request_id=?',
                               ('material:' + str(view['id']),)).fetchone()
        self.assertEqual(actor['actor_id'], 'mitarbeiter:1')
        self.assertEqual(status['order']['quantity'], '1')

    def test_urgent_form_request_uses_real_dispatch_guard_and_brutto_cap(self):
        self.demand(True)
        self.assertEqual(self.portal.process_next()['state'], 'sent')
        self.assertEqual(self.e.smtp.data_calls, 1)
        view = self.demand(True, price=24600, extras=101)
        self.assertIn('budget', view['missing_fields'])
        self.assertIsNone(self.portal.process_next())
        self.assertEqual(self.e.smtp.data_calls, 1)

    def test_revoked_personal_rights_before_monday_block_actual_delivery(self):
        self.demand()
        self.assertEqual(self.portal.process_next()['state'], 'queued')
        self.f.f.sql('UPDATE assistent_rechte SET einkaufen=0,version=version+1 WHERE mitarbeiter_id=1')
        self.f.f.time = datetime(2026, 10, 12, 14, 0, tzinfo=BERLIN).timestamp()
        self.e.manager.tick(worker=True)
        self.assertEqual(self.e.smtp.data_calls, 0)

    def test_real_handoff_and_actual_dispatch_keep_shared_lock_held(self):
        view = self.demand()
        depth = [0]
        @contextmanager
        def operation_lock():
            depth[0] += 1
            try:
                yield
            finally:
                depth[0] -= 1
        self.p.portal_originals_operation_lock = operation_lock
        real_enqueue, real_due = self.e.manager.dispatch.enqueue, self.e.manager.dispatch.dispatch_due
        def checked_enqueue(*args, **kwargs):
            self.assertGreater(depth[0], 0)
            return real_enqueue(*args, **kwargs)
        def checked_due(*args, **kwargs):
            self.assertGreater(depth[0], 0)
            return real_due(*args, **kwargs)
        with patch.object(self.e.manager.dispatch, 'enqueue', side_effect=checked_enqueue), \
             patch.object(self.e.manager.dispatch, 'dispatch_due', side_effect=checked_due):
            self.e.manager.submit_material_request(view['id'], view['revision'])
            self.f.f.time = datetime(2026, 10, 12, 14, 0, tzinfo=BERLIN).timestamp()
            self.e.manager.tick(worker=True)
        self.assertEqual(self.e.smtp.data_calls, 1)
        self.assertEqual(depth[0], 0)


if __name__ == '__main__':
    unittest.main()
