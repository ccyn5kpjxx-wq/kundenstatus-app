"""CHECK24 parser contract with synthetic RFC822 messages; no app or network."""
from email.message import EmailMessage
from contextlib import contextmanager
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check24_mail import parse_check24_mail


BODY = """Guten Tag,
unser Kunde hat einen Montagetermin bei Ihnen gebucht.
Voraussichtliche Zustellung: Do. 29. Oktober
Kunde
Erika Beispiel
+49 170 0000000
kundenalias@example.invalid
Gebuchter Termin
Fr. 30. Oktober um 08:00 - 08:30 Uhr
Beispielwerkstatt
Teststraße 1, 12345 Beispielstadt
Gebuchte Leistungen
Reifenmontage für 2 Reifen
Gesamtpreis
45,00 €
Gesamtpreis (Netto)
37,82 €
Provision
6,43 €
Bestellnummer
TEST9001
2x Beispielreifen 185/60 R15 88 V, Ganzjahresreifen
"""


def synthetic_mail(body=BODY, *, sender="montagepartner@check24.de", date="Mon, 26 Oct 2026 11:29:00 +0100", subject="Neue CHECK24 Terminbuchung (Bestellnummer: TEST9001)", html=False, auth=""):
    message = EmailMessage()
    message["From"] = sender
    message["To"] = "werkstatt@example.invalid"
    if date:
        message["Date"] = date
    message["Subject"] = subject
    message["Message-ID"] = "<synthetic-booking@example.invalid>"
    if auth:
        message["Authentication-Results"] = auth
    message.set_content(body, subtype="html" if html else "plain")
    return message.as_bytes()


class Check24MailParserTest(unittest.TestCase):
    def test_service_booking_without_tire_delivery_is_supported(self):
        body = BODY.replace('Montagetermin', 'Servicetermin').replace(
            'Voraussichtliche Zustellung: Do. 29. Oktober\n', '').replace(
            '2x Beispielreifen 185/60 R15 88 V, Ganzjahresreifen\n', '').replace(
            'für 2 Reifen', 'für 4 Reifen').replace('45,00 €', '90,00 €')
        for html in (False, True):
            with self.subTest(html=html):
                content = ''.join('<p>' + line + '</p>' for line in body.splitlines()) if html else body
                result = parse_check24_mail(synthetic_mail(content, html=html))
                self.assertTrue(result['valid'], result['errors'])
                self.assertEqual(result['kind'], 'new')
                self.assertEqual(result['total_gross'], '90.00')
                self.assertEqual(result['service'], 'Reifenmontage für 4 Reifen')
                self.assertEqual(result['tires'], [])
                self.assertEqual(result['delivery_date'], '')

    def test_service_changes_cancellations_and_unknown_services_stay_in_review(self):
        body = BODY.replace('Montagetermin', 'Servicetermin')
        for suffix in ('Termin storniert.', 'Termin verschoben.', 'Terminänderung.', 'Termin abgesagt.'):
            self.assertFalse(parse_check24_mail(synthetic_mail(body + '\n' + suffix))['valid'])
        self.assertFalse(parse_check24_mail(synthetic_mail(body.replace('Reifenmontage für 2 Reifen', 'Bremsenprüfung')))['valid'])
        self.assertFalse(parse_check24_mail(synthetic_mail(body, subject='Fwd: Neue CHECK24 Terminbuchung (Bestellnummer: TEST9001)'))['valid'])

    def test_observed_new_template_is_structural_review_only(self):
        result = parse_check24_mail(synthetic_mail())
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["provider_id"], "TEST9001")
        self.assertEqual(result["start_at"], "2026-10-30T08:00:00+01:00")
        self.assertEqual(result["end_at"], "2026-10-30T08:30:00+01:00")
        self.assertEqual(result["delivery_date"], "2026-10-29")
        self.assertEqual(result["customer_phone"], "+491700000000")
        self.assertEqual(result["customer_email"], "kundenalias@example.invalid")
        self.assertEqual(result["total_gross"], "45.00")
        self.assertTrue(result["requires_review"])
        self.assertFalse(result["authentication"]["trusted"])

    def test_html_contacts_and_table_cells_without_loading_urls(self):
        body = "<html><head><style>hidden</style></head><body>" + "".join(
            "<div>" + line + "</div>" for line in BODY.splitlines()) + "</body></html>"
        body = body.replace("+49 170 0000000", '<a href="tel:+491700000000">Anrufen</a>')
        body = body.replace("kundenalias@example.invalid", '<a href="mailto:kundenalias@example.invalid?subject=Termin">E-Mail an Kunden senden</a>')
        body += '<img src="https://example.invalid/tracker"><script>fetch("bad")</script>'
        result = parse_check24_mail(synthetic_mail(body, html=True))
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["customer_name"], "Erika Beispiel")
        self.assertEqual(result["customer_email"], "kundenalias@example.invalid")

    def test_plain_contact_link_and_divider_are_not_customer_name(self):
        body = BODY.replace("kundenalias@example.invalid", "E-Mail an Kunden senden: mailto:kundenalias@example.invalid?subject=Infos zum Termin\n__________________________________")
        result = parse_check24_mail(synthetic_mail(body))
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["customer_name"], "Erika Beispiel")
        self.assertEqual(result["customer_email"], "kundenalias@example.invalid")

    def test_html_preferred_when_plain_alternative_omits_delivery(self):
        from email.parser import BytesParser
        from email import policy
        raw = synthetic_mail(BODY.replace("Voraussichtliche Zustellung: Do. 29. Oktober\n", ""))
        message = BytesParser(policy=policy.default).parsebytes(raw)
        message.add_alternative("".join("<p>" + line + "</p>" for line in BODY.splitlines()), subtype="html")
        result = parse_check24_mail(message.as_bytes())
        self.assertTrue(result["valid"], result["errors"])
        self.assertEqual(result["delivery_date"], "2026-10-29")

    def test_forged_authentication_results_never_create_trust(self):
        result = parse_check24_mail(synthetic_mail(auth="attacker.invalid; dkim=pass header.d=check24.de; dmarc=pass"))
        self.assertTrue(result["valid"])
        self.assertFalse(result["authentication"]["trusted"])
        self.assertIn("attacker.invalid", result["authentication"]["authentication_results"][0])

    def test_foreign_sender_and_forwarded_subject_rejected(self):
        for changes in ({"sender": "montagepartner@check24.de.attacker.invalid"},
                        {"subject": "Fwd: Neue CHECK24 Terminbuchung (Bestellnummer: TEST9001)"}):
            self.assertFalse(parse_check24_mail(synthetic_mail(**changes))["valid"])

    def test_yearless_without_mail_date_and_weekday_mismatch_rejected(self):
        self.assertFalse(parse_check24_mail(synthetic_mail(date=""))["valid"])
        self.assertFalse(parse_check24_mail(synthetic_mail(BODY.replace("Fr. 30.", "Do. 30.")))["valid"])
        self.assertFalse(parse_check24_mail(synthetic_mail(BODY.replace("Fr. 30. Oktober", "30. Mai")))["valid"])
        self.assertFalse(parse_check24_mail(synthetic_mail(BODY.replace("Fr. 30. Oktober", "31. Februar 2027")))["valid"])

    def test_new_year_inference_and_explicit_year(self):
        body = BODY.replace("Do. 29. Oktober", "Do. 31. Dezember").replace("Fr. 30. Oktober", "Sa. 2. Januar")
        result = parse_check24_mail(synthetic_mail(body, date="Wed, 30 Dec 2026 11:29:00 +0100"))
        self.assertTrue(result["valid"], result["errors"])
        self.assertTrue(result["start_at"].startswith("2027-01-02"))
        explicit = BODY.replace("29. Oktober", "29. Oktober 2026").replace("30. Oktober", "30. Oktober 2026")
        self.assertTrue(parse_check24_mail(synthetic_mail(explicit, date=""))["valid"])

    def test_dst_ambiguous_and_nonexistent_time_rejected(self):
        for day in ("So. 25. Oktober 2026", "So. 29. März 2026"):
            body = BODY.replace("Voraussichtliche Zustellung: Do. 29. Oktober\n", "")
            body = body.replace("Fr. 30. Oktober um 08:00 - 08:30", day + " um 02:00 - 02:30")
            self.assertFalse(parse_check24_mail(synthetic_mail(body))["valid"])

    def test_overnight_zero_and_multiple_intervals_rejected(self):
        for clock in ("23:30 - 00:30", "08:00 - 08:00", "08:30 - 08:00", "24:00 - 24:30"):
            self.assertFalse(parse_check24_mail(synthetic_mail(BODY.replace("08:00 - 08:30", clock)))["valid"])
        body = BODY.replace("Gebuchte Leistungen", "Sa. 31. Oktober um 09:00 - 09:30 Uhr\nGebuchte Leistungen")
        self.assertFalse(parse_check24_mail(synthetic_mail(body))["valid"])

    def test_mismatched_id_missing_customer_and_late_delivery_rejected(self):
        for old, new in (("\nTEST9001\n", "\nTEST9002\n"), ("Erika Beispiel\n", ""),
                         ("Do. 29. Oktober", "Sa. 31. Oktober")):
            self.assertFalse(parse_check24_mail(synthetic_mail(BODY.replace(old, new)))["valid"])

    def test_changes_and_cancellations_are_never_new(self):
        for state in ("storniert", "verschoben", "geändert"):
            result = parse_check24_mail(synthetic_mail(BODY + "\nDer Montagetermin wurde " + state + "."))
            self.assertEqual(result["kind"], "unknown")
            self.assertFalse(result["valid"])
            self.assertEqual(result["provider_id"], "TEST9001")
        for text in ("Der Kunde hat diesen Termin storniert.",
                     "Ihr Termin wurde auf Samstag geändert.",
                     "Bitte beachten Sie die Terminänderung."):
            result = parse_check24_mail(synthetic_mail(BODY + "\n" + text))
            self.assertEqual(result["kind"], "unknown")
            self.assertFalse(result["valid"])

    def test_repeated_sections_and_interval_outside_booking_rejected(self):
        for heading in ("Kunde", "Gebuchter Termin", "Gebuchte Leistungen"):
            result = parse_check24_mail(synthetic_mail(BODY + "\n" + heading + "\n"))
            self.assertFalse(result["valid"])
        second = "\nGebuchter Termin\nSa. 31. Oktober um 10:00 - 10:30 Uhr\nGebuchte Leistungen\nReifenmontage für 2 Reifen\n"
        self.assertFalse(parse_check24_mail(synthetic_mail(BODY + second))["valid"])
        self.assertFalse(parse_check24_mail(synthetic_mail(BODY + "\nSa. 31. Oktober um 10:00 - 10:30 Uhr"))["valid"])

    def test_cancellation_in_plain_alternative_is_not_hidden_by_html(self):
        from email.parser import BytesParser
        from email import policy
        message = BytesParser(policy=policy.default).parsebytes(synthetic_mail(BODY + "\nDer Kunde hat diesen Termin storniert."))
        message.add_alternative("".join("<p>" + line + "</p>" for line in BODY.splitlines()), subtype="html")
        self.assertFalse(parse_check24_mail(message.as_bytes())["valid"])

    def test_duplicate_header_and_malformed_mime_rejected(self):
        raw = synthetic_mail().replace(b"From:", b"From: attacker@example.invalid\nFrom:", 1)
        self.assertFalse(parse_check24_mail(raw)["valid"])
        self.assertFalse(parse_check24_mail(b"Content-Type: multipart/mixed; boundary=x\n\ninvalid")["valid"])

    def test_limit_and_no_empty_messages(self):
        for value in (b"", "text", b"X" * (5 * 1024 * 1024 + 1)):
            with self.assertRaises(ValueError):
                parse_check24_mail(value)


class Check24IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import smoke_test as isolated
        cls.p = isolated.portal
        cls.p.app.config['TESTING'] = True
        cls.service = cls.p.check24_bookings

    def setUp(self):
        from unittest.mock import patch
        from datetime import datetime
        from zoneinfo import ZoneInfo
        self.fixed = datetime(2026, 10, 26, 12, tzinfo=ZoneInfo('Europe/Berlin'))
        self.clock = patch('check24_buchungen.datetime', wraps=datetime)
        clock = self.clock.start()
        clock.now.return_value = self.fixed
        self.addCleanup(self.clock.stop)
        self.service._worker_stop.clear()
        self.service._worker = None
        self.service._worker_pid = None
        db = self.p.get_db()
        db.execute('DELETE FROM check24_worker_state')
        db.execute('INSERT INTO check24_worker_state(id) VALUES(1)')
        for table in ('check24_bookings', 'check24_events', 'check24_receipts', 'reifen_termin_slots'):
            db.execute('DELETE FROM ' + table)
        db.execute("DELETE FROM status_log WHERE auftrag_id IN (SELECT id FROM auftraege WHERE quelle='check24')")
        db.execute("DELETE FROM auftraege WHERE quelle='check24'")
        db.commit()
        db.close()

    def count_orders(self):
        db = self.p.get_db()
        try:
            return db.execute("SELECT COUNT(*) FROM auftraege WHERE quelle='check24'").fetchone()[0]
        finally:
            db.close()

    @contextmanager
    def fake_sync_mailbox(self, *, count=1, on_raw=None, raw_bytes=None):
        from unittest.mock import patch
        calls = []
        cfg = {'configured': True, 'ssl': True, 'host': 'imap.example.invalid',
               'user': 'werkstatt@example.invalid', 'folder': 'INBOX'}
        class FakeClient:
            def uid(self, *args):
                calls.append(args)
                return 'OK', [' '.join(str(i) for i in range(1, count+1)).encode()]
        class FakeMailbox:
            def __init__(self, config): pass
            @contextmanager
            def connect(self): yield FakeClient()
            def select(self, client, folder, readonly):
                if readonly is not True: raise AssertionError('Writable mailbox')
                return '991'
            def raw(self, client, uid):
                calls.append(('FETCH', uid))
                if on_raw: on_raw(cfg, uid)
                return raw_bytes if raw_bytes is not None else synthetic_mail(), ''
        with patch('check24_buchungen.Mailbox', FakeMailbox), patch.object(self.p, 'get_werkstatt_imap_config', return_value=cfg):
            yield cfg, calls

    def test_sync_claim_is_shared_and_expired_owner_cannot_release_successor(self):
        from unittest.mock import patch
        from check24_buchungen import Check24SyncStopped
        with self.fake_sync_mailbox() as (cfg, calls), patch.object(self.service, '_clock', return_value=1000) as clock:
            first = self.service._claim_sync(cfg, False)
            self.assertIsNotNone(first)
            self.assertIsNone(self.service._claim_sync(cfg, False))
            clock.return_value = 1301
            second = self.service._claim_sync(cfg, False)
            self.assertNotEqual(first['token'], second['token'])
            self.service._finish_sync(first, 0)
            db = self.p.get_db()
            try:
                self.assertEqual(db.execute('SELECT lease_token FROM check24_worker_state').fetchone()[0], second['token'])
                with self.assertRaises(Check24SyncStopped):
                    self.service._guard_sync(db, first)
                db.rollback()
            finally:
                db.close()
            self.assertEqual(calls, [])

    def test_parallel_manual_scans_open_only_one_mailbox_fetch(self):
        from concurrent.futures import ThreadPoolExecutor
        from check24_buchungen import Check24SyncBusy
        import threading
        entered, release = threading.Event(), threading.Event()
        def hold(cfg, uid):
            entered.set()
            if not release.wait(5): raise AssertionError('Test release timeout')
        with self.fake_sync_mailbox(on_raw=hold) as (cfg, calls), ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.service.sync)
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaises(Check24SyncBusy):
                    self.service.sync(auto_apply=True)
            finally:
                release.set()
            self.assertEqual(pending.result(timeout=5), 1)
            self.assertEqual([call for call in calls if call[0]=='FETCH'], [('FETCH','1')])

    def test_expired_lease_after_fetch_fences_both_message_and_error_receipt(self):
        from check24_buchungen import Check24SyncStopped
        for malformed in (False, True):
            with self.subTest(malformed=malformed):
                def replace_lease(cfg, uid):
                    db = self.p.get_db()
                    db.execute("UPDATE check24_worker_state SET lease_token='successor',lease_until=9999999999")
                    db.commit(); db.close()
                    if malformed: raise ValueError('synthetic malformed mail')
                db = self.p.get_db()
                db.execute("UPDATE check24_worker_state SET lease_token='',lease_until=0")
                db.commit(); db.close()
                with self.fake_sync_mailbox(on_raw=replace_lease), self.assertRaises(Check24SyncStopped):
                    self.service.sync(auto_apply=True)
                db = self.p.get_db()
                try:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_events').fetchone()[0], 0)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_receipts').fetchone()[0], 0)
                    self.assertEqual(db.execute('SELECT lease_token FROM check24_worker_state').fetchone()[0], 'successor')
                finally:
                    db.close()

    def test_sync_batch_and_wall_budget_bound_new_fetches(self):
        from unittest.mock import patch
        with self.fake_sync_mailbox(count=55) as (cfg, calls):
            self.assertEqual(self.service.sync(), 50)
            self.assertEqual(len([c for c in calls if c[0]=='FETCH']), 50)
            self.assertEqual(self.service.sync(), 5)
        db = self.p.get_db()
        db.execute('DELETE FROM check24_receipts'); db.commit(); db.close()
        with self.fake_sync_mailbox(count=3) as (cfg, calls), patch.object(self.service, '_monotonic', side_effect=[0,0,61]):
            self.assertEqual(self.service.sync(), 1)
            self.assertEqual([c for c in calls if c[0]=='FETCH'], [('FETCH','1')])

    def test_old_unknown_mail_is_reparsed_once_from_mailbox_without_duplicate(self):
        import json
        from unittest.mock import patch
        from check24_mail import PARSER_VERSION
        service_mail = synthetic_mail(BODY.replace('Montagetermin', 'Servicetermin'))
        with self.fake_sync_mailbox(raw_bytes=service_mail) as (cfg, calls), patch('check24_buchungen.ionos_verified', return_value=True):
            self.service.sync(auto_apply=False)
            db = self.p.get_db()
            row = db.execute('SELECT id,payload FROM check24_events').fetchone()
            old = json.loads(row['payload'])
            old.update(kind='unknown', valid=False, errors=['Alte Vorlage'], parser_version=1)
            db.execute('UPDATE check24_events SET payload=?,note=? WHERE id=?',
                       (json.dumps(old), 'Diese Nachricht muss manuell geklärt werden.', row['id']))
            db.commit(); db.close()
            self.assertEqual(self.service.sync(auto_apply=True), 1)
            self.assertEqual(self.count_orders(), 1)
            self.assertEqual(self.service.rows()[0]['id'], row['id'])
            self.assertEqual(self.service.rows()[0]['data']['parser_version'], PARSER_VERSION)
            self.assertEqual(self.service.rows()[0]['state'], 'applied')
            self.assertEqual(self.service.sync(auto_apply=True), 0)
            self.assertEqual(self.count_orders(), 1)
            self.assertEqual(len([c for c in calls if c[0]=='FETCH']), 2)

    def test_reifenportal_surfaces_pending_check24_review(self):
        self.service.ingest(synthetic_mail(BODY + '\nTermin storniert.'), auto_apply=True)
        client = self.p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
        response = client.get('/admin/reifenportal')
        self.assertEqual(response.status_code, 200)
        self.assertIn('data-check24-review-alert', response.text)
        self.assertIn('CHECK24: 1 Nachricht(en)', response.text)
        self.assertIn('CHECK24-Buchungen prüfen', response.text)
        self.assertEqual(self.count_orders(), 0)

    def test_old_unknown_reparse_does_not_upgrade_unverified_mail(self):
        import json
        from unittest.mock import patch
        with self.fake_sync_mailbox() as (cfg, calls):
            self.service.sync(auto_apply=False)
            db = self.p.get_db()
            row = db.execute('SELECT id,payload FROM check24_events').fetchone()
            old = json.loads(row['payload'])
            old.update(kind='unknown', valid=False, errors=['Alte Vorlage'])
            old.pop('parser_version')
            db.execute('UPDATE check24_events SET payload=?,verified=1 WHERE id=?', (json.dumps(old), row['id']))
            db.commit(); db.close()
            with patch('check24_buchungen.ionos_verified', return_value=False):
                self.assertEqual(self.service.sync(auto_apply=True), 1)
            self.assertEqual(self.count_orders(), 0)
            self.assertFalse(self.service.rows()[0]['verified'])
            self.assertTrue(self.service.rows()[0]['data']['valid'])
            self.assertEqual(self.service.sync(auto_apply=True), 0)

    def test_resolved_old_unknown_is_not_reopened(self):
        import json
        with self.fake_sync_mailbox() as (cfg, calls):
            self.service.sync(auto_apply=False)
            db = self.p.get_db()
            row = db.execute('SELECT id,payload FROM check24_events').fetchone()
            old = json.loads(row['payload'])
            old.update(kind='unknown', valid=False, errors=['Alte Vorlage'], parser_version=1)
            db.execute("UPDATE check24_events SET payload=?,state='resolved',note='Geprüft' WHERE id=?", (json.dumps(old), row['id']))
            db.commit(); db.close()
            self.assertEqual(self.service.sync(auto_apply=True), 0)
            self.assertEqual(self.count_orders(), 0)
            self.assertEqual(self.service.rows()[0]['note'], 'Geprüft')

    def test_config_change_or_worker_stop_after_fetch_prevents_publication(self):
        from unittest.mock import patch
        from check24_buchungen import Check24SyncStopped
        def config_changed(cfg, uid): cfg['user'] = 'different@example.invalid'
        with self.fake_sync_mailbox(on_raw=config_changed), self.assertRaises(Check24SyncStopped):
            self.service.sync(auto_apply=True)
        db = self.p.get_db()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_receipts').fetchone()[0], 0)
        db.execute('UPDATE check24_worker_state SET next_poll_at=0'); db.commit(); db.close()
        with patch.dict(self.p.app.config, CHECK24_WORKER_ENABLED=True), self.fake_sync_mailbox(on_raw=lambda cfg, uid:self.service._worker_stop.set()):
            with self.assertRaises(Check24SyncStopped):
                self.service.sync(auto_apply=True, worker=True)
        self.assertEqual(self.count_orders(), 0)
        self.assertEqual(self.service.status()['last_error_code'], 'stopped')

    def test_worker_backoff_and_safe_status_without_success_claim(self):
        from unittest.mock import patch
        def failure(cfg, uid): raise OSError('SECRET private@example.invalid')
        with patch.dict(self.p.app.config, CHECK24_WORKER_ENABLED=True), self.fake_sync_mailbox(on_raw=failure) as (cfg,calls), patch.object(self.service, '_clock', return_value=1000) as clock:
            with self.assertRaises(OSError): self.service.sync(worker=True)
            status = self.service.status()
            self.assertEqual(status['last_error_code'], 'sync_failed')
            self.assertEqual(status['last_success_at'], '')
            self.assertEqual(status['failures'], 1)
            self.assertNotIn('SECRET', str(status))
            self.assertNotIn('lease_token', status)
            self.assertFalse(status['worker_alive'])
            self.assertEqual(self.service.sync(worker=True), 0)
            clock.return_value = 1061
            with self.assertRaises(OSError): self.service.sync(worker=True)
            self.assertEqual(self.service.status()['failures'], 2)
            db = self.p.get_db()
            self.assertEqual(db.execute('SELECT next_poll_at FROM check24_worker_state').fetchone()[0], 1181)
            db.close()

    def test_worker_default_off_testing_public_and_duplicate_start_guards(self):
        from unittest.mock import patch
        from check24_buchungen import start_check24_worker
        threads = []
        class FakeThread:
            def __init__(self, **kwargs): self.kwargs=kwargs; self.started=False; threads.append(self)
            def start(self): self.started=True
            def is_alive(self): return self.started
        with patch.dict(self.p.app.config, CHECK24_WORKER_ENABLED=False, TESTING=False), patch.object(self.p, 'PUBLIC_SITE_ONLY', False), patch('check24_buchungen.threading.Thread', FakeThread):
            self.assertFalse(start_check24_worker(self.service))
            self.p.app.config['CHECK24_WORKER_ENABLED']=True
            self.p.app.config['TESTING']=True
            self.assertFalse(start_check24_worker(self.service))
            self.p.app.config['TESTING']=False
            self.p.PUBLIC_SITE_ONLY=True
            self.assertFalse(start_check24_worker(self.service))
            self.p.PUBLIC_SITE_ONLY=False
            self.assertTrue(start_check24_worker(self.service))
            self.assertFalse(start_check24_worker(self.service))
            self.assertEqual(len(threads), 1)
            self.assertTrue(threads[0].kwargs['daemon'])
            self.assertTrue(self.service.status()['worker_alive'])
            self.service._worker_stop.set()
            self.assertFalse(self.service.status()['worker_alive'])

    def test_upload_never_auto_applies_and_manual_is_idempotent(self):
        event = self.service.ingest(synthetic_mail(auth='kundenserver.de; dkim=pass'), auto_apply=True)
        self.assertEqual(self.count_orders(), 0)
        first = self.service.apply(event)
        self.assertEqual(first, self.service.apply(event))
        self.assertEqual(event, self.service.ingest(synthetic_mail(auth='kundenserver.de; dkim=pass')))
        self.assertEqual(self.count_orders(), 1)
        order = self.p.get_auftrag(first)
        self.assertEqual(order['leistungsart'], 'reifenservice')
        self.assertEqual(order['status'], 2)
        self.assertEqual(order['annahme_uhrzeit'], '08:00')
        self.assertIn('08:00–08:30', order['beschreibung'])
        self.assertFalse(order.get('lexware_invoice_id'))
        days = self.p.kalender_daten([order])
        self.assertTrue(any(any(e['auftrag']['id'] == first for e in day['events']) for day in days))
        labels = str([e.get('termine') for day in days for e in day['events'] if e['auftrag']['id'] == first])
        self.assertIn('Reifenservice', labels)
        self.assertIn('08:00–08:30', labels)
        client = self.p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
        board = client.get('/werkstatt/tafel')
        self.assertEqual(board.status_code, 200)
        self.assertIn('CHECK24', board.text)
        self.assertIn('Reifenservice', board.text)

    def test_semantic_duplicate_new_headers_do_not_make_another_order(self):
        first = self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        second = self.service.ingest(synthetic_mail().replace(b'<synthetic-booking@example.invalid>', b'<resend@example.invalid>'), verified=True, auto_apply=True)
        self.assertNotEqual(first, second)
        self.assertEqual(self.count_orders(), 1)
        self.assertEqual(self.service.rows()[0]['state'], 'duplicate')

    def test_changed_or_cancelled_mail_keeps_original_slot(self):
        self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        self.service.ingest(synthetic_mail(BODY.replace('08:00 - 08:30', '10:00 - 10:30')), verified=True, auto_apply=True)
        self.service.ingest(synthetic_mail(BODY + '\nDer Kunde hat diesen Termin storniert.'), verified=True, auto_apply=True)
        self.assertEqual(self.count_orders(), 1)
        self.assertEqual(self.service.review_count(), 2)
        db = self.p.get_db()
        self.assertTrue(self.service.native_slot_blocked(db, '2026-10-30', '08:00'))
        self.assertFalse(self.service.native_slot_blocked(db, '2026-10-30', '10:00'))
        db.close()

    def test_half_open_intervals_and_native_slot_block(self):
        from datetime import datetime
        self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        db = self.p.get_db()
        at = lambda s: datetime.fromisoformat('2026-10-30T' + s + ':00+01:00')
        self.assertFalse(self.service.overlaps(db, at('08:30'), at('09:00')))
        for start, end in (('08:00','08:30'), ('08:29','09:00'), ('07:30','09:00')):
            self.assertTrue(self.service.overlaps(db, at(start), at(end)))
        self.assertTrue(self.service.native_slot_blocked(db, '2026-10-30', '08:00'))
        self.assertFalse(self.service.native_slot_blocked(db, '2026-10-30', '09:00'))
        db.close()
        # Distinct provider bookings that just touch are allowed.
        raw = synthetic_mail(BODY.replace('TEST9001','TEST9002').replace('08:00 - 08:30','08:30 - 09:00'), subject='Neue CHECK24 Terminbuchung (Bestellnummer: TEST9002)')
        self.service.ingest(raw, verified=True, auto_apply=True)
        self.assertEqual(self.count_orders(), 2)

    def test_native_reservation_conflict_is_review_not_overwritten(self):
        db = self.p.get_db()
        # Synthetic existing website lead, no customer/provider side effects.
        db.execute("INSERT INTO leads(kunde_name,status,erstellt_am,geaendert_am) VALUES('Test','neu','','')")
        lead = db.execute('SELECT MAX(id) FROM leads').fetchone()[0]
        db.execute("INSERT INTO reifen_termin_slots(datum,uhrzeit,lead_id,erstellt_am) VALUES('2026-10-30','08:00',?,'')", (lead,))
        db.commit()
        db.close()
        self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        self.assertEqual(self.count_orders(), 0)
        self.assertIn('überschneidung', self.service.rows()[0]['note'])

    def test_concurrent_duplicate_imports_create_one_order(self):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as pool:
            ids = list(pool.map(lambda _: self.service.ingest(synthetic_mail(), verified=True, auto_apply=True), range(2)))
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(self.count_orders(), 1)

    def test_admin_csrf_review_gate_and_escaped_mail_content(self):
        from io import BytesIO
        client = self.p.app.test_client()
        self.assertEqual(client.get('/admin/check24').status_code, 302)
        with client.session_transaction() as session:
            session['werkstatt_tafel'] = True
        self.assertEqual(client.get('/admin/check24').status_code, 302)
        with client.session_transaction() as session:
            session['admin'] = True
        page = client.get('/admin/check24')
        self.assertEqual(page.status_code, 200)
        with client.session_transaction() as session:
            csrf = session[self.p.CSRF_FIELD_NAME]
        self.assertNotEqual(client.post('/admin/check24/import', data={'mail': (BytesIO(synthetic_mail()),'test.eml')}).status_code, 302)
        event = self.service.ingest(synthetic_mail())
        self.assertEqual(client.post(f'/admin/check24/{event}/uebernehmen', data={self.p.CSRF_FIELD_NAME: csrf}).status_code, 400)
        self.assertEqual(self.count_orders(), 0)
        result = client.post(f'/admin/check24/{event}/uebernehmen', data={self.p.CSRF_FIELD_NAME: csrf, 'reviewed':'1'})
        self.assertEqual(result.status_code, 302)
        self.assertEqual(self.count_orders(), 1)

    def test_mailbox_receipt_dedupes_repeated_uids_and_same_raw(self):
        self.service.ingest(synthetic_mail(), 'account/3/1')
        self.service.ingest(synthetic_mail(), 'account/3/2')
        db = self.p.get_db()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_receipts').fetchone()[0], 2)
        db.close()
        self.assertEqual(len(self.service.rows()), 1)

    def test_receiver_authentication_boundary(self):
        from check24_buchungen import ionos_verified
        cfg = {'host':'imap.ionos.de','ssl':True,'user':'werkstatt@example.invalid'}
        headers = (b'Authentication-Results: kundenserver.de; dkim=pass header.i=@check24.de header.s=check24; spf=pass smtp.mailfrom=montagepartner@check24.de; dmarc=pass header.from=check24.de\n'
                   b'Received: from smtp.example.invalid by mx.kundenserver.de (test) with ESMTPS; Mon, 26 Oct 2026 11:29:01 +0100\n')
        good = headers + synthetic_mail()
        self.assertTrue(ionos_verified(good, cfg))
        for bad in (good.replace(b'dmarc=pass', b'dmarc=fail'), good.replace(b'kundenserver.de;',b'evil.invalid;'), good.replace(b'by mx.kundenserver.de (',b'by mx.kundenserver.de.evil.invalid ('), good.replace(b'header.i=@check24.de',b'header.i=@check24.de.evil.invalid'), good.replace(b'dkim=pass',b'dkim=fail; dkim=pass')):
            self.assertFalse(ionos_verified(bad, cfg))
        self.assertFalse(ionos_verified(good, dict(cfg, ssl=False)))
        self.assertFalse(ionos_verified(good, dict(cfg, user='other@example.invalid')))

    def test_verified_storno_releases_time_but_never_started_or_invoiced_order(self):
        event = self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        row = self.service.rows()[0]
        db = self.p.get_db()
        db.execute('UPDATE auftraege SET status=3 WHERE id=?', (row['order_id'],))
        db.commit()
        db.close()
        with self.assertRaises(ValueError):
            self.service.cancel(event, 'Geprüft')
        db = self.p.get_db()
        db.execute("UPDATE auftraege SET status=2,rechnung_nummer='TEST-RE' WHERE id=?", (row['order_id'],))
        db.commit()
        db.close()
        with self.assertRaises(ValueError):
            self.service.cancel(event, 'Geprüft')
        db = self.p.get_db()
        db.execute("UPDATE auftraege SET rechnung_nummer='' WHERE id=?", (row['order_id'],))
        db.commit()
        db.close()
        self.service.cancel(event, 'Storno beim Anbieter geprüft')
        db = self.p.get_db()
        self.assertFalse(self.service.native_slot_blocked(db, '2026-10-30', '08:00'))
        self.assertEqual(db.execute('SELECT archiviert FROM auftraege WHERE id=?', (row['order_id'],)).fetchone()[0], 1)
        db.close()
        self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        self.assertEqual(self.count_orders(), 1)
        self.assertEqual(self.service.rows()[0]['state'], 'resolved')

    def test_scanner_reads_seen_and_unseen_without_writes_and_records_failures(self):
        from contextlib import contextmanager
        from unittest.mock import patch
        calls = []
        class Client:
            def uid(self, *args):
                calls.append(args)
                return 'OK', [b'1 2 3']
        class FakeMailbox:
            def __init__(self, config): pass
            @contextmanager
            def connect(self): yield Client()
            def select(self, client, folder, readonly):
                self_readonly = readonly
                calls.append(('SELECT', folder, self_readonly))
                return '123'
            def raw(self, client, uid):
                calls.append(('PEEK', uid))
                if uid == '1': raise ValueError('oversize')
                return synthetic_mail(), ''
        cfg = {'configured':True, 'ssl':True, 'host':'imap.example.invalid', 'user':'werkstatt@example.invalid','folder':'INBOX'}
        with patch('check24_buchungen.Mailbox', FakeMailbox), patch.object(self.p, 'get_werkstatt_imap_config', return_value=cfg):
            self.assertEqual(self.service.sync(auto_apply=True), 3)
            self.assertEqual(self.service.sync(auto_apply=True), 0)
        self.assertEqual(self.count_orders(), 0)  # Unknown receiver is never trusted.
        self.assertEqual(self.service.review_count(), 2)
        self.assertIn(('SELECT','INBOX',True), calls)
        self.assertEqual([c for c in calls if c[0]=='PEEK'], [('PEEK','1'),('PEEK','2'),('PEEK','3')])
        self.assertTrue(all('UNSEEN' not in c and 'STORE' not in c and 'EXPUNGE' not in c for c in calls))

    def test_reschedule_updates_order_and_slot_atomically_and_checks_conflicts(self):
        import json
        event = self.service.ingest(synthetic_mail(), verified=True, auto_apply=True)
        order_id = self.service.rows()[0]['order_id']
        raw = synthetic_mail(BODY.replace('TEST9001','TEST9002').replace('08:00 - 08:30','09:00 - 09:30'), subject='Neue CHECK24 Terminbuchung (Bestellnummer: TEST9002)')
        self.service.ingest(raw, verified=True, auto_apply=True)
        with self.assertRaises(ValueError):
            self.service.reschedule(event, '2026-10-30', '08:30', '09:15', 'Abgeglichen')
        self.service.reschedule(event, '2026-10-30', '10:00', '10:30', 'Abgeglichen')
        order = self.p.get_auftrag(order_id)
        self.assertEqual(order['annahme_uhrzeit'], '10:00')
        self.assertIn('10:00–10:30', order['beschreibung'])
        self.assertIn('T10:00:', json.loads(order['schaden_aufnahme_json'])['check24_start'])
        db = self.p.get_db()
        self.assertFalse(self.service.native_slot_blocked(db, '2026-10-30', '08:00'))
        self.assertTrue(self.service.native_slot_blocked(db, '2026-10-30', '10:00'))
        db.close()
        client = self.p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
        client.get('/admin/check24')
        with client.session_transaction() as session:
            csrf = session[self.p.CSRF_FIELD_NAME]
        result = client.post(f'/admin/auftrag/{order_id}', data={self.p.CSRF_FIELD_NAME: csrf,'annahme_datum':'31.10.2026','annahme_uhrzeit':'12:00'})
        self.assertEqual(result.status_code, 302)
        self.assertTrue(result.location.endswith('/admin/check24'))
        self.assertEqual(self.p.get_auftrag(order_id)['annahme_uhrzeit'], '10:00')

    def test_old_reviews_remain_visible_and_history_is_paginated(self):
        event = self.service.ingest(synthetic_mail())
        db = self.p.get_db()
        data = db.execute('SELECT payload FROM check24_events WHERE id=?', (event,)).fetchone()[0]
        for i in range(60):
            db.execute("INSERT INTO check24_events(raw_hash,source_key,payload,state,created_at) VALUES(?,?,?,'resolved','')", (str(i),'test',data))
        db.commit()
        db.close()
        self.assertEqual(self.service.rows()[0]['id'], event)
        self.assertEqual(len(self.service.rows(1)), 50)
        self.assertEqual(len(self.service.rows(2)), 11)

    def test_real_postgres_adapter_handles_natural_keys_through_full_import(self):
        """Exercise production SQL conversion, not a copied RETURNING heuristic.

        SQLite is only the disposable relational backend for the driver's SQL
        subset; this catches missing-column RETURNING and result-consumption
        errors without a PostgreSQL service or any production connections.
        """
        from contextlib import contextmanager
        from types import SimpleNamespace
        from unittest.mock import patch
        from check24_buchungen import Check24Bookings

        original_get_db = self.p.get_db
        statements = []

        class CursorBridge:
            def __init__(self, connection):
                self.cursor = connection.cursor()
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.cursor.close()
            def execute(self, sql, params):
                statements.append(sql)
                sql = sql.replace('%s', '?').replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
                self.cursor.execute(sql, params)
                self.rowcount = self.cursor.rowcount
                self.description = [SimpleNamespace(name=item[0]) for item in self.cursor.description] if self.cursor.description else None
            def fetchall(self):
                return [tuple(row) for row in self.cursor.fetchall()]

        def adapter_connection():
            raw = original_get_db()
            return self.p.PostgresConnection(SimpleNamespace(
                cursor=lambda: CursorBridge(raw), commit=raw.commit,
                rollback=raw.rollback, close=raw.close))

        class FakeClient:
            def uid(self, *args):
                return 'OK', [b'1 2']

        class FakeMailbox:
            def __init__(self, config):
                pass
            @contextmanager
            def connect(self):
                yield FakeClient()
            def select(self, client, folder, readonly):
                return '8'
            def raw(self, client, uid):
                if uid == '1':
                    raise ValueError('synthetic invalid mail')
                return synthetic_mail(), ''

        cfg = {'configured': True, 'ssl': True, 'host': 'imap.example.invalid',
               'user': 'werkstatt@example.invalid', 'folder': 'INBOX'}
        with patch.object(self.p, 'get_db', side_effect=adapter_connection), \
             patch.object(self.p, 'get_werkstatt_imap_config', return_value=cfg), \
             patch('check24_buchungen.Mailbox', FakeMailbox):
            service = Check24Bookings(self.p)  # Includes natural-key boot lock.
            service.init_schema()  # ON CONFLICT must also work with no returned row.
            event = service.ingest(synthetic_mail(), 'adapter/8/1', verified=True, auto_apply=True)
            self.assertEqual(service.ingest(synthetic_mail(), 'adapter/8/1'), event)
            self.assertEqual(self.count_orders(), 1)
            self.assertEqual(service.rows()[0]['state'], 'applied')
            self.assertEqual(service.sync(), 2)  # Error receipt and success receipt.
            self.assertEqual(service.sync(), 0)  # last_sync upsert, no duplicate jobs.
            db = self.p.get_db()
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_bookings').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_receipts').fetchone()[0], 3)
                self.assertTrue(db.execute("SELECT value FROM check24_sync_state WHERE key='last_sync'").fetchone()[0])
                self.assertEqual(service.review_count(), 1)
            finally:
                db.close()
        self.assertTrue(any('RETURNING provider_id' in sql for sql in statements))


class Check24CLITest(unittest.TestCase):
    def cli(self, args, env, loader, sleeper=lambda seconds: None):
        from io import StringIO
        from contextlib import redirect_stderr, redirect_stdout
        from scripts.check24_sync import main
        output = StringIO()
        with redirect_stdout(output), redirect_stderr(output):
            try:
                code = main(args, environment=env, service_loader=loader, sleeper=sleeper)
            except SystemExit as exc:
                code = exc.code
        return code, output.getvalue()

    def test_default_and_missing_enable_never_load_application(self):
        from unittest.mock import Mock
        loader = Mock(side_effect=AssertionError('No app import allowed'))
        for args, env, expected in (([], {}, 0), (['--loop'], {'CHECK24_SYNC_ENABLED':'1'}, 0),
                                    (['--run'], {}, 2), (['--run'], {'CHECK24_SYNC_ENABLED':'true'}, 2)):
            self.assertEqual(self.cli(args, env, loader)[0], expected)
        loader.assert_not_called()

    def test_enabled_once_and_cli_process_worker_suppression(self):
        from unittest.mock import Mock
        from scripts.check24_sync import CLI_ENVIRONMENT
        service = Mock()
        service.sync.return_value = 2
        code, output = self.cli(['--run','--once'], {'CHECK24_SYNC_ENABLED':'1'}, lambda: service)
        self.assertEqual(code, 0)
        service.sync.assert_called_once_with(auto_apply=True)
        self.assertIn('2 neue Nachricht', output)
        self.assertEqual(CLI_ENVIRONMENT['CHECK24_WORKER_ENABLED'], '0')
        self.assertEqual(CLI_ENVIRONMENT['PUBLIC_SITE_ONLY'], '1')

    def test_loop_validation_failure_and_error_output_remain_safe(self):
        from unittest.mock import Mock
        loader = Mock()
        self.assertEqual(self.cli(['--run','--loop','--interval','59'], {'CHECK24_SYNC_ENABLED':'1'}, loader)[0], 2)
        loader.assert_not_called()
        service = Mock()
        service.sync.side_effect = [0, RuntimeError('SECRET someone@example.invalid')]
        pauses = []
        code, output = self.cli(['--run','--loop','--interval','60'], {'CHECK24_SYNC_ENABLED':'1'}, lambda: service, pauses.append)
        self.assertEqual(code, 1)
        self.assertEqual(pauses, [60])
        self.assertNotIn('SECRET', output)
        self.assertNotIn('someone@', output)


if __name__ == "__main__":
    unittest.main()
