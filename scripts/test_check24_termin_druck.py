"""A changed CHECK24 appointment needs a newly issued, up-to-date workshop PDF."""
from contextlib import closing
import json
import re
import unittest
from unittest.mock import patch

import fitz
import test_check24_buchungen as fixtures
from smoke_test import csrf_data, portal
import werkstatt_druckpaket


class Check24AppointmentPrintTest(unittest.TestCase):
    setUpClass = classmethod(fixtures.Check24IntegrationTest.setUpClass.__func__)

    def setUp(self):
        fixtures.Check24IntegrationTest.setUp(self)
        self.client = portal.app.test_client()
        with self.client.session_transaction() as session:
            session['admin'] = True
        self.client.get('/login')

    def book(self, reference='TEST9001', interval='08:00 - 08:30'):
        body = fixtures.BODY.replace('TEST9001', reference).replace('08:00 - 08:30', interval)
        mail = fixtures.synthetic_mail(body, subject=f'Neue CHECK24 Terminbuchung (Bestellnummer: {reference})')
        event = self.service.ingest(mail, verified=True)
        return event, self.service.apply(event)

    def reschedule(self, event, day, begin, finish, note='Geprüfter Montagetermin'):
        form = dict(day=day, begin=begin, finish=finish, note=note, reviewed='1')
        response = self.client.post(f'/admin/check24/{event}/verschieben', data=csrf_data(self.client, form))
        self.assertEqual(response.status_code, 302)

    def receipt(self, order):
        with closing(portal.get_db()) as db:
            return db.execute('SELECT reifen_auftrag_gedruckt_am FROM auftraege WHERE id=?', (order,)).fetchone()[0]

    def print_order(self, order):
        response = self.client.post(f'/admin/auftrag/{order}/ausdrucken', data=csrf_data(self.client))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'application/pdf')
        self.assertTrue(self.receipt(order))
        with fitz.open(stream=response.data, filetype='pdf') as pdf:
            return pdf[0].get_text()

    def assert_pending(self, order):
        self.assertEqual(self.receipt(order), '')
        status = self.client.get('/admin/reifenportal/druckoffen', query_string={'id': order})
        self.assertEqual(status.get_json(), [order])

    def test_each_change_to_the_reserved_interval_requires_a_new_print(self):
        event, order = self.book()
        self.print_order(order)
        for day, begin, finish in (
            ('2026-10-30', '08:10', '08:30'),  # Only the start changed.
            ('2026-10-30', '08:10', '08:55'),  # Only the end changed.
            ('2026-10-31', '08:10', '08:55'),  # Only the day changed.
        ):
            with self.subTest(day=day, begin=begin, finish=finish):
                self.reschedule(event, day, begin, finish)
                self.assert_pending(order)
                preview = self.client.get(f'/admin/auftrag/{order}/ausdrucken')
                self.assertEqual(preview.mimetype, 'application/pdf')
                self.assert_pending(order)
                printed = self.print_order(order)
                self.assertIn(begin, printed)
                self.assertIn(finish, printed)
                receipt = self.receipt(order)
                self.reschedule(event, day, begin, finish, note='Erneut im Portal verglichen')
                self.assertEqual(self.receipt(order), receipt)

    def test_conflicting_change_preserves_printed_booking(self):
        event, order = self.book()
        self.book('TEST9002', '11:30 - 12:30')
        self.print_order(order)
        receipt = self.receipt(order)
        self.reschedule(event, '2026-10-30', '11:40', '12:00')
        with closing(portal.get_db()) as db:
            booking = db.execute('SELECT start_at, end_at FROM check24_bookings WHERE order_id=?', (order,)).fetchone()
        self.assertEqual((booking['start_at'][11:16], booking['end_at'][11:16]), ('08:00', '08:30'))
        self.assertEqual(self.receipt(order), receipt)

    def test_archive_or_invoice_in_progress_protects_slot_and_print_receipt(self):
        event, order = self.book()
        self.print_order(order)
        receipt = self.receipt(order)
        for archived, invoice_status in ((1, ''), (0, 'in_bearbeitung')):
            with self.subTest(archived=archived, invoice_status=invoice_status):
                with closing(portal.get_db()) as db:
                    db.execute('UPDATE auftraege SET archiviert=?, rechnung_erstellung_status=? WHERE id=?',
                               (archived, invoice_status, order))
                    db.commit()
                with self.assertRaisesRegex(ValueError, 'zuerst im Büro klären'):
                    self.service.reschedule(event, '2026-10-31', '11:30', '12:30', 'Termin geprüft')
                self.assertEqual(self.receipt(order), receipt)
                self.assertEqual(portal.get_auftrag(order)['annahme_uhrzeit'], '08:00')

    def test_interval_requires_minute_precision(self):
        for begin, finish in (('11:30:15', '12:30'), ('11:30', '18:00:59'), ('11:30+01:00', '12:30')):
            with self.subTest(begin=begin, finish=finish), self.assertRaises(ValueError):
                self.service.reschedule_interval('2026-10-31', begin, finish)

    def test_earlier_arrival_reopens_print_and_survives_confirmation_of_same_slot(self):
        event, order = self.book()
        self.print_order(order)
        path = f'/admin/check24/auftrag/{order}/anlieferung'
        form = dict(day='2026-10-30', at='07:45', note='Fahrzeug wird früher abgegeben')
        self.assertEqual(self.client.post(path, data=csrf_data(self.client, form)).status_code, 302)
        self.assert_pending(order)
        self.assertIn('07:45', self.print_order(order))
        receipt = self.receipt(order)
        self.reschedule(event, '2026-10-30', '08:00', '08:30')
        self.assertEqual(portal.get_auftrag(order)['annahme_uhrzeit'], '07:45')
        form['note'] = 'Abgabe bestätigt'
        self.assertEqual(self.client.post(path, data=csrf_data(self.client, form)).status_code, 302)
        self.assertEqual(self.receipt(order), receipt)

    def test_obsolete_pdf_cannot_acknowledge_new_appointment_even_if_original_time_restored(self):
        event, order = self.book()
        self.print_order(order)
        original_cover = werkstatt_druckpaket._cover

        for restore_previous in (False, True):
            with self.subTest(restore_previous=restore_previous):
                def change_after_cover(*args, **kwargs):
                    old_cover = original_cover(*args, **kwargs)
                    if restore_previous:
                        self.service.reschedule(event, '2026-10-30', '08:00', '08:30', 'Zwischenzeitlich verschoben')
                    self.service.reschedule(event, '2026-10-31', '10:00', '11:00', 'Parallele Umbuchung')
                    return old_cover

                with patch.object(werkstatt_druckpaket, '_cover', change_after_cover):
                    response = self.client.post(f'/admin/auftrag/{order}/ausdrucken', data=csrf_data(self.client))
                self.assertEqual(response.status_code, 409)
                self.assert_pending(order)
                self.assertIn('10:00', self.print_order(order))

    def edit_order(self, order, form):
        response = self.client.post(f'/admin/auftrag/{order}', data=csrf_data(self.client, form))
        self.assertEqual(response.status_code, 302)

    def test_manual_tyre_order_work_times_and_partial_office_forms(self):
        order = portal.create_auftrag('intern', leistungsart='reifenservice', beschreibung='Räder umstecken')
        self.edit_order(order, dict(start_datum='2026-10-30', start_uhrzeit='09:15',
                                    fertig_datum='2026-10-30', fertig_uhrzeit='10:45'))
        printed = self.print_order(order)
        for at in ('09:15', '10:45'):
            self.assertIn(at, printed)
        details = self.client.get(f'/admin/auftrag/{order}').get_data(as_text=True)
        inputs = re.findall(r'<input\b[^>]*>', details)
        for field, at in (('start_uhrzeit', '09:15'), ('fertig_uhrzeit', '10:45')):
            matching = [tag for tag in inputs if f'name="{field}"' in tag]
            self.assertTrue(any('type="time"' in tag and f'value="{at}"' in tag
                                and not re.search(r'\b(disabled|readonly)\b', tag) for tag in matching), field)

        revision = portal.get_auftrag(order)['reifen_termin_revision']
        self.edit_order(order, {'notiz_intern': 'Termin geprüft'})
        self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], revision)
        self.assertTrue(self.receipt(order))
        # Legacy clients send dates but no time inputs. A partial client may send just a time.
        changes = [
            ({'start_datum': '2026-10-31', 'fertig_datum': '2026-10-31'},
             ('31.10.2026', '09:15', '31.10.2026', '10:45')),
            ({'start_uhrzeit': '09:30'}, ('31.10.2026', '09:30', '31.10.2026', '10:45')),
            ({'fertig_uhrzeit': '11:00'}, ('31.10.2026', '09:30', '31.10.2026', '11:00')),
            ({'start_datum': ''}, ('', '', '31.10.2026', '11:00')),
            ({'fertig_datum': '', 'fertig_uhrzeit': '11:00'}, ('', '', '', '')),
        ]
        for form, expected in changes:
            with self.subTest(form=form):
                self.edit_order(order, form)
                saved = portal.get_auftrag(order)
                self.assertEqual(tuple(saved[k] or '' for k in
                                       ('start_datum', 'start_uhrzeit', 'fertig_datum', 'fertig_uhrzeit')), expected)
                revision += 1
                self.assertEqual(saved['reifen_termin_revision'], revision)
                self.assert_pending(order)
                self.print_order(order)
                # The freshly opened form shows empty time fields after clearing their date.
                repeated = {k: saved[k] if k.endswith('_uhrzeit') else v for k, v in form.items()}
                self.edit_order(order, repeated)
                self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], revision)
                self.assertTrue(self.receipt(order))

    def test_invalid_work_time_rejects_the_whole_office_edit(self):
        order = portal.create_auftrag('intern', leistungsart='reifenservice', beschreibung='Montage',
                                      start_datum='30.10.2026', notiz_intern='Freigegebener Plan')
        self.print_order(order)
        original = portal.get_auftrag(order)
        for form in ({'start_uhrzeit': '25:30'}, {'fertig_uhrzeit': '09:00'},
                     {'start_uhrzeit': '09:30', 'fertig_datum': '2026-10-30', 'fertig_uhrzeit': '13:84'}):
            with self.subTest(form=form):
                form = dict(form, notiz_intern='Darf bei Fehler nicht gespeichert werden')
                response = self.client.post(f'/admin/auftrag/{order}', data=csrf_data(self.client, form))
                self.assertEqual(response.status_code, 302)
                with self.client.session_transaction() as session:
                    self.assertEqual(session['_flashes'][-1][0], 'warning')
                saved = portal.get_auftrag(order)
                for field in ('start_datum', 'start_uhrzeit', 'fertig_datum', 'fertig_uhrzeit', 'notiz_intern',
                              'reifen_termin_revision', 'reifen_auftrag_gedruckt_am'):
                    self.assertEqual(saved[field], original[field], field)

    def test_start_date_matching_printed_arrival_leaves_receipt_valid(self):
        order = portal.create_auftrag('intern', leistungsart='reifenservice')
        form = dict(annahme_datum='2026-10-30', annahme_uhrzeit='09:15')
        self.edit_order(order, form)
        original_pdf = self.print_order(order)
        revision = portal.get_auftrag(order)['reifen_termin_revision']
        receipt = self.receipt(order)
        self.edit_order(order, dict(form, start_datum='2026-10-30'))
        saved = portal.get_auftrag(order)
        self.assertEqual(saved['start_datum'], '30.10.2026')
        self.assertEqual(saved['reifen_termin_revision'], revision)
        self.assertEqual(self.receipt(order), receipt)
        self.assertEqual(self.print_order(order), original_pdf)

    def test_check24_slot_has_priority_over_internal_work_times_but_not_handover_times(self):
        _, order = self.book()

        def remove_edit_notices():
            with closing(portal.get_db()) as db:
                db.execute('DELETE FROM benachrichtigungen WHERE auftrag_id=?', (order,))
                db.commit()

        self.addCleanup(remove_edit_notices)
        original = portal.get_auftrag(order)
        form = dict(annahme_datum='2026-10-30', annahme_uhrzeit='08:00', auftragsnummer=original['auftragsnummer'],
                    beschreibung=original['beschreibung'], start_datum='2026-10-31', start_uhrzeit='11:15',
                    fertig_datum='2026-10-31', fertig_uhrzeit='12:45')
        self.print_order(order)
        receipt = self.receipt(order)
        self.edit_order(order, form)
        saved = portal.get_auftrag(order)
        self.assertEqual((saved['start_uhrzeit'], saved['fertig_uhrzeit']), ('11:15', '12:45'))
        self.assertEqual(saved['reifen_termin_revision'], original['reifen_termin_revision'])
        self.assertEqual(self.receipt(order), receipt)
        printed = self.print_order(order)
        for at in ('08:00', '08:30'):
            self.assertIn(at, printed)
        for internal in ('11:15', '12:45'):
            self.assertNotIn(internal, printed)
        self.edit_order(order, dict(form, abholtermin='2026-10-31', abhol_uhrzeit='15:40'))
        self.assert_pending(order)
        self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], original['reifen_termin_revision'] + 1)
        self.assertIn('15:40', self.print_order(order))

    def test_manual_check24_reference_and_internal_order_number(self):
        form = dict(kunde_name='Montage Test', fahrzeug='Testfahrzeug', beschreibung='CHECK24-Bestellung PRN1234',
                    auftragsnummer='WERK-123', leistungsart='reifenservice', annahme_datum='2026-10-30')
        order = portal.create_auftrag('intern', **form)
        self.edit_order(order, form)
        self.print_order(order)
        for field, value in (('annahme_datum', '2026-10-31'), ('annahme_uhrzeit', '11:30'),
                             ('start_datum', '2026-11-01'), ('fertig_datum', '2026-11-02'),
                             ('abholtermin', '2026-11-02'), ('abhol_uhrzeit', '12:30')):
            with self.subTest(field=field):
                form[field] = value
                self.edit_order(order, form)
                self.assert_pending(order)
                self.print_order(order)
                receipt = self.receipt(order)
                form['notiz_intern'] = f'{field} kontrolliert'
                self.edit_order(order, form)
                self.assertEqual(self.receipt(order), receipt)
        portal.apply_delay_to_order(order, abholtermin='2026-11-03')
        self.assert_pending(order)
        self.print_order(order)
        receipt = self.receipt(order)
        portal.apply_delay_to_order(order, abholtermin='03.11.2026')
        self.assertEqual(self.receipt(order), receipt)

    def test_partial_legacy_check24_interval_uses_office_clock_for_unconfirmed_end(self):
        intake = {'check24_bestellnummer': 'TEST543', 'check24_start': '2026-10-30T11:30:00+01:00'}
        order = portal.create_auftrag('intern', leistungsart='reifenservice', start_datum='30.10.2026',
                                      fertig_datum='30.10.2026', schaden_aufnahme_json=json.dumps(intake))
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET start_uhrzeit='10.30 Uhr', fertig_uhrzeit='12.30 Uhr' WHERE id=?",
                       (order,))
            db.commit()
        printed = self.print_order(order)
        self.assertIn('30.10.2026 · 11:30', printed)
        self.assertNotIn('10:30', printed)
        self.assertIn('Planende', printed)
        self.assertIn('30.10.2026 · 12:30', printed)
        revision = portal.get_auftrag(order)['reifen_termin_revision']
        receipt = self.receipt(order)
        self.edit_order(order, {'start_uhrzeit': '10:45'})
        saved = portal.get_auftrag(order)
        self.assertEqual(saved['start_uhrzeit'], '10:45')
        self.assertEqual(saved['reifen_termin_revision'], revision)
        self.assertEqual(self.receipt(order), receipt)
        self.edit_order(order, {'fertig_uhrzeit': '13:00'})
        self.assert_pending(order)
        self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], revision + 1)
        updated = self.print_order(order)
        self.assertIn('30.10.2026 · 11:30', updated)
        self.assertIn('30.10.2026 · 13:00', updated)
        self.assertNotIn('12:30', updated)

    def test_end_time_edit_in_confirmed_manual_summary_requires_a_new_print(self):
        summary = 'CHECK24 TEST544 · 30.10.2026, 11:30–12:30 Uhr · Reifenmontage'
        order = portal.create_auftrag('intern', leistungsart='reifenservice', annahme_datum='30.10.2026',
                                      beschreibung='CHECK24-Bestellung TEST544. Montage von vier Reifen.',
                                      analyse=summary)
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET annahme_uhrzeit='11.30 Uhr' WHERE id=?", (order,))
            db.commit()
        original = self.print_order(order)
        self.assertIn('Gebuchtes Ende', original)
        self.assertIn('30.10.2026 · 12:30', original)
        revision = portal.get_auftrag(order)['reifen_termin_revision']
        changed = {'analyse_text': summary.replace('12:30', '12:45')}
        self.edit_order(order, changed)
        self.assert_pending(order)
        self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], revision + 1)
        updated = self.print_order(order)
        self.assertIn('30.10.2026 · 11:30', updated)
        self.assertIn('30.10.2026 · 12:45', updated)
        self.assertNotIn('12:30', updated)
        receipt = self.receipt(order)
        self.edit_order(order, changed)
        self.assertEqual(portal.get_auftrag(order)['reifen_termin_revision'], revision + 1)
        self.assertEqual(self.receipt(order), receipt)

    def test_other_order_types_keep_their_state(self):
        form = dict(kunde_name='Mechanik Test', fahrzeug='Testfahrzeug', beschreibung='CHECK24-Bestellung PRN2222',
                    auftragsnummer='WERK-222', leistungsart='mechanik', annahme_datum='2026-10-30')
        order = portal.create_auftrag('intern', **form)
        with closing(portal.get_db()) as db:
            db.execute('UPDATE auftraege SET reifen_auftrag_gedruckt_am=NULL WHERE id=?', (order,))
            db.commit()
        form['annahme_datum'] = '2026-10-31'
        self.edit_order(order, form)
        self.assertIsNone(self.receipt(order))


if __name__ == '__main__':
    unittest.main()
