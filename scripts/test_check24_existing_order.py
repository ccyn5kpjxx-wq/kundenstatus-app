"""Link a shop-created order to an authenticated CHECK24 original in the isolated portal."""
import hashlib
import json
import unittest
from unittest.mock import patch

import test_check24_buchungen as fixtures


class ExistingOrderTest(unittest.TestCase):
    setUpClass = classmethod(fixtures.Check24IntegrationTest.setUpClass.__func__)
    setUp = fixtures.Check24IntegrationTest.setUp
    fake_sync_mailbox = fixtures.Check24IntegrationTest.fake_sync_mailbox

    def manual(self, reference='CHECK24-Bestellung TEST9001.', name='Erika Beispiel', number=''):
        order = self.p.create_auftrag('intern', leistungsart='reifenservice', kunde_name=name,
            fahrzeug='Reifenservice', beschreibung=reference, kontakt_telefon='0170 0000000', auftragsnummer=number,
            annahme_datum='30.10.2026', schaden_aufnahme_json='{"notiz":"Alte Räder mitgeben"}')
        db = self.p.get_db()
        db.execute("UPDATE auftraege SET annahme_uhrzeit='08:00' WHERE id=?", (order,))
        db.commit()
        db.close()
        self.addCleanup(self.remove_manual, order)
        return order

    def remove_manual(self, order):
        db = self.p.get_db()
        db.execute('DELETE FROM status_log WHERE auftrag_id=?', (order,))
        db.execute('DELETE FROM auftraege WHERE id=?', (order,))
        db.commit()
        db.close()

    def old_resolved_receipt(self):
        raw = fixtures.synthetic_mail()
        account = hashlib.sha256(b'imap.example.invalid/werkstatt@example.invalid/INBOX').hexdigest()
        event = self.service.ingest(raw, account + '/991/1', verified=True)
        old = fixtures.parse_check24_mail(raw)
        old.update(valid=False, parser_version=1, service='')
        db = self.p.get_db()
        db.execute("UPDATE check24_events SET payload=?,state='resolved',note='Portalabgleich dokumentiert.' WHERE id=?",
                   (json.dumps(old), event))
        db.commit()
        db.close()
        return event

    def test_verified_resolved_original_and_manual_order_are_joined_once(self):
        order = self.manual()
        event = self.old_resolved_receipt()
        client = self.p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
        page = client.get('/admin/check24')
        self.assertIn(f'Vorhandenen Reifenauftrag {order} verknüpfen', page.get_data(as_text=True))
        with client.session_transaction() as session:
            csrf = session[self.p.CSRF_FIELD_NAME]
        path = f'/admin/check24/{event}/verknuepfen'
        form = {'order_id': order, self.p.CSRF_FIELD_NAME: csrf}
        self.assertEqual(client.post(path, data=form).status_code, 400)
        with self.fake_sync_mailbox() as (_, calls), patch('check24_buchungen.ionos_verified', return_value=True):
            form['reviewed'] = '1'
            self.assertEqual(client.post(path, data=form).status_code, 302)
            self.assertEqual(self.service.link_existing(event, order), order)
            self.assertEqual(calls.count(('FETCH', '1')), 1)
        db = self.p.get_db()
        try:
            row = db.execute('SELECT * FROM check24_bookings').fetchone()
            self.assertEqual((row['order_id'], row['active'], row['start_at'][11:16], row['end_at'][11:16]),
                             (order, 1, '08:00', '08:30'))
            self.assertTrue(self.service.native_slot_blocked(db, '2026-10-30', '08:00'))
            linked = dict(db.execute('SELECT * FROM auftraege WHERE id=?', (order,)).fetchone())
            self.assertEqual(linked['beschreibung'], 'CHECK24-Bestellung TEST9001.')
            self.assertEqual(linked['quelle'], 'check24')
            self.assertEqual(json.loads(linked['schaden_aufnahme_json'])['notiz'], 'Alte Räder mitgeben')
            self.assertEqual(self.p.check24_rechnungsbasis(linked)['brutto'], '45.00')
            self.assertEqual(self.p.check24_rechnungsbasis(linked)['leistung'], 'Reifenmontage für 2 Reifen')
        finally:
            db.close()
        card = self.service.rows()[0]
        self.assertIn('Portalabgleich dokumentiert.', card['note'])
        self.assertEqual(card['state'], 'applied')
        self.service.ingest(fixtures.synthetic_mail().replace(b'<synthetic-booking@', b'<resend@'),
                            verified=True, auto_apply=True)
        self.assertEqual(self.service.rows()[0]['state'], 'duplicate')

    def test_manual_reference_blocks_import_and_requires_unambiguous_identity(self):
        first = self.manual(name='Falscher Name')
        event = self.service.ingest(fixtures.synthetic_mail(), verified=True, auto_apply=True)
        self.assertEqual(self.service.rows()[0]['state'], 'review')
        with self.assertRaisesRegex(ValueError, 'Kundenname'):
            self.service.link_existing(event, first)
        self.manual(reference='CHECK24 TEST9001 · Montage')
        with self.assertRaisesRegex(ValueError, 'eindeutigen'):
            self.service.link_existing(event, first)
        with self.assertRaisesRegex(ValueError, 'manuellen Reifenauftrag'):
            self.service.apply(event)
        self.assertIsNone(self.service.rows()[0]['order_id'])

    def test_distinct_reference_does_not_block_new_booking(self):
        self.manual(reference='CHECK24 TEST90011')
        self.service.ingest(fixtures.synthetic_mail(), verified=True, auto_apply=True)
        self.assertEqual(self.service.rows()[0]['state'], 'applied')

    def test_refuses_untrusted_original_and_hash_changes(self):
        order = self.manual()
        raw = fixtures.synthetic_mail()
        event = self.old_resolved_receipt()
        for payload, trust in ((raw + b'\n', True), (raw, False)):
            with self.subTest(trust=trust), self.fake_sync_mailbox(raw_bytes=payload), \
                    patch('check24_buchungen.ionos_verified', return_value=trust):
                with self.assertRaisesRegex(ValueError, 'Originalmail'):
                    self.service.link_existing(event, order)
        self.assertIsNone(self.service.rows()[0]['order_id'])
        uploaded = self.service.ingest(raw.replace(b'<synthetic-booking@', b'<upload@'))
        with self.assertRaisesRegex(ValueError, 'verifizierte'):
            self.service.link_existing(uploaded, order)

    def test_busy_slot_rolls_back_manual_order_and_event(self):
        order = self.manual()
        event = self.service.ingest(fixtures.synthetic_mail(), verified=True)
        other = fixtures.synthetic_mail(fixtures.BODY.replace('TEST9001', 'TEST9999'),
            subject='Neue CHECK24 Terminbuchung (Bestellnummer: TEST9999)')
        self.service.ingest(other, verified=True, auto_apply=True)
        with self.assertRaisesRegex(ValueError, 'Zeit ist bereits belegt'):
            self.service.link_existing(event, order)
        db = self.p.get_db()
        try:
            self.assertEqual(db.execute('SELECT quelle FROM auftraege WHERE id=?', (order,)).fetchone()[0], 'intern')
            self.assertEqual(db.execute('SELECT state FROM check24_events WHERE id=?', (event,)).fetchone()[0], 'review')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM check24_bookings').fetchone()[0], 1)
        finally:
            db.close()

    def test_saved_invoice_draft_gross_total_internal_number_and_revision(self):
        order = self.manual(number='WERK-104')
        event = self.service.ingest(fixtures.synthetic_mail(fixtures.BODY.replace('45,00 €', '106,00 €')),
                                    verified=True, auto_apply=True)
        items = [{'preis_id': 'montage_13_16', 'menge': 4}]
        valid = self.p.berechne_reifenservice(items + [{'preis_id': 'entsorgung', 'menge': 4}])
        different = self.p.berechne_reifenservice(items)
        db = self.p.get_db()
        try:
            for calculation in (different, valid):
                stored = json.dumps({'kalkulation': calculation, 'kunde': {'name': 'Erika Beispiel'}})
                db.execute('UPDATE auftraege SET reifen_rechnung_json=? WHERE id=?', (stored, order))
                db.commit()
                if calculation == different:
                    with self.assertRaisesRegex(ValueError, 'CHECK24-Bruttobetrag'):
                        self.service.link_existing(event, order)
            before = self.p.reifen_rechnungsentwurf(self.p.get_auftrag(order))
            self.service.link_existing(event, order)
            linked = self.p.get_auftrag(order)
            self.assertEqual(linked['auftragsnummer'], 'WERK-104')
            self.assertEqual(linked['reifen_rechnung_json'], stored)
            after = self.p.reifen_rechnungsentwurf(linked)
            self.assertEqual(after['kalkulation'], valid)
            self.assertEqual(after['auswahl'], {})
            self.assertEqual(after['check24_basis']['brutto'], '106.00')
            self.assertNotEqual(after['revision'], before['revision'])
            client = self.p.app.test_client()
            with client.session_transaction() as session:
                session['admin'] = True
            path = f'/admin/auftrag/{order}/reifen-rechnung'
            self.assertEqual(client.get(path).status_code, 200)
            with client.session_transaction() as session:
                csrf = session[self.p.CSRF_FIELD_NAME]
            reply = client.post(path, data={self.p.CSRF_FIELD_NAME: csrf, 'revision': before['revision']})
            self.assertEqual(reply.status_code, 400)
            self.assertIn('zwischenzeitlich geändert', reply.get_data(as_text=True))
        finally:
            db.close()

    def test_missing_gross_total_preserves_manual_order(self):
        order = self.manual()
        event = self.service.ingest(fixtures.synthetic_mail(fixtures.BODY.replace('45,00 €', '')), verified=True)
        with self.assertRaisesRegex(ValueError, 'fehlt ein eindeutiger CHECK24-Bruttobetrag'):
            self.service.link_existing(event, order)
        self.assertIsNone(self.service.rows()[0]['order_id'])

    def test_cancellation_by_reference_and_inactive_ledger_remain_final(self):
        order = self.manual()
        event = self.service.ingest(fixtures.synthetic_mail(), verified=True)
        cancelled = fixtures.BODY + '\nDer Kunde hat diesen Termin storniert.'
        self.service.ingest(fixtures.synthetic_mail(cancelled.replace('TEST9001', 'TEST9999'),
            subject='CHECK24 Storno (Bestellnummer: TEST9999)'), verified=True)
        self.service.link_existing(event, order)
        self.service.cancel(event, 'Im Anbieterportal bestätigt.')
        with self.assertRaises(ValueError):
            self.service.link_existing(event, order)
        db = self.p.get_db()
        try:
            self.assertEqual(db.execute('SELECT active FROM check24_bookings WHERE order_id=?',
                                        (order,)).fetchone()[0], 0)
        finally:
            db.close()

    def test_unlinked_order_with_recorded_cancellation_cannot_be_revived(self):
        order = self.manual()
        event = self.service.ingest(fixtures.synthetic_mail(), verified=True)
        self.service.ingest(fixtures.synthetic_mail(fixtures.BODY + '\nDer Kunde hat diesen Termin storniert.',
            subject='CHECK24 Storno (Bestellnummer: TEST9001)'), verified=True)
        with self.assertRaisesRegex(ValueError, 'Stornovermerk'):
            self.service.link_existing(event, order)

    def test_resolved_note_requires_positive_cancellation_of_this_reference(self):
        order = self.manual()
        event = self.service.ingest(fixtures.synthetic_mail(), verified=True)
        db = self.p.get_db()
        try:
            for note, blocked in [('CHECK24 TEST9001 wurde storniert.', True),
                                  ('Storno für Bestellung TEST9001.', True),
                                  ('Storno für Bestellung TEST9999. CHECK24 TEST9001 ist nicht storniert.', False)]:
                db.execute("UPDATE check24_events SET state='resolved',note=? WHERE id=?", (note, event))
                db.commit()
                if blocked:
                    with self.assertRaisesRegex(ValueError, 'Stornovermerk'):
                        self.service.link_existing(event, order)
                else:
                    self.assertEqual(self.service.link_existing(event, order), order)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
