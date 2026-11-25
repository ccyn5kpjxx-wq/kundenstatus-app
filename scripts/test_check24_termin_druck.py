"""A changed CHECK24 appointment needs a newly issued, up-to-date workshop PDF."""
from contextlib import closing
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

    def test_manual_check24_reference_and_internal_order_number(self):
        form = dict(kunde_name='Montage Test', fahrzeug='Testfahrzeug', beschreibung='CHECK24-Bestellung PRN1234',
                    auftragsnummer='WERK-123', leistungsart='reifenservice', annahme_datum='2026-10-30')
        order = portal.create_auftrag('intern', **form)
        self.edit_order(order, form)
        self.print_order(order)
        for field, value in (('annahme_datum', '2026-10-31'), ('annahme_uhrzeit', '11:30'),
                             ('start_datum', '2026-10-31'), ('fertig_datum', '2026-11-02'),
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
