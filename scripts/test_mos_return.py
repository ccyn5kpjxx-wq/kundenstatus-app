"""Offline checks for actual MOS return and review-only charges."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import sqlite3
import sys
import unittest
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mos_return import (init_schema, record, existing, late_review_cents,
                        release_review_deadline, record_clearance,
                        existing_clearance, deposit_release_eligible,
                        vehicle_blocked, record_vehicle_readiness,
                        existing_vehicle_readiness)


BERLIN = ZoneInfo('Europe/Berlin')


class ReturnTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.start = datetime(2026, 10, 5, 8, tzinfo=BERLIN)
        self.end = self.start + timedelta(days=3)
        self.handed = self.start + timedelta(minutes=5)
        self.returned = self.end + timedelta(minutes=31)
        self.now = self.returned + timedelta(minutes=5)
        quote = {'test_only': False, 'start_slot': self.start.isoformat(),
                 'end_slot': self.end.isoformat(), 'daily_cents': 4900,
                 'included_km': 450, 'extra_km_cents': 25}
        self.db.execute('''CREATE TABLE miet_checkout_holds
            (id TEXT PRIMARY KEY,mietvorgang_id INTEGER,mietfahrzeug_id INTEGER,
             status TEXT,payload TEXT)''')
        self.db.execute('''CREATE TABLE miet_checkout_handovers
            (hold_id TEXT PRIMARY KEY,mietvorgang_id INTEGER,handed_at TEXT,
             odometer_km INTEGER)''')
        self.db.execute('''CREATE TABLE mietvorgaenge
            (id INTEGER PRIMARY KEY,mietfahrzeug_id INTEGER,status TEXT,
             rueckgabe_datum TEXT,geaendert_am TEXT)''')
        init_schema(self.db)
        self.db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?)',
                        ('h1', 7, 3, 'confirmed', json.dumps({'quote': quote})))
        self.db.execute('INSERT INTO miet_checkout_handovers VALUES (?,?,?,?)',
                        ('h1', 7, self.handed.isoformat(), 12000))
        self.db.execute("INSERT INTO mietvorgaenge VALUES (7,3,'aktiv','',NULL)")

    def returned_record(self, **overrides):
        arguments = dict(returned_at=self.returned, operator_name='Test Person',
                         odometer_km=12010, fuel_full=True,
                         condition_recorded=True, damage_free=True,
                         charges_resolved=True, no_objection=True,
                         protocol_ref='RETURN-1', note='Zeitanteil erlassen', now=self.now)
        arguments.update(overrides)
        return record(self.db, 'h1', **arguments)

    def test_thirty_minute_grace_then_pro_rata_using_actual_rate(self):
        self.assertEqual(late_review_cents(self.end,self.end+timedelta(minutes=30),4900),0)
        self.assertEqual(late_review_cents(self.end,self.end+timedelta(minutes=31),4900),3)
        self.assertEqual(late_review_cents(self.end,self.end+timedelta(hours=1),4900),102)
        self.assertEqual(late_review_cents(self.end,self.end+timedelta(hours=1),5900),123)
        # Elapsed time, not wall-clock time, controls the grace across DST.
        dst = datetime(2026,10,25,1,45,tzinfo=BERLIN)
        self.assertEqual(late_review_cents(dst,dst.astimezone(timezone.utc)+timedelta(minutes=31),3900),3)

    def test_internal_deposit_reminder_counts_saturday_but_skips_bw_holiday(self):
        holidays=lambda year:{date(2026,10,3)} if year==2026 else set()
        friday=datetime(2026,10,2,20,tzinfo=BERLIN)
        self.assertEqual(release_review_deadline(friday,holidays).date(),date(2026,10,5))
        self.assertEqual(release_review_deadline(friday,lambda _:set()).date(),date(2026,10,3))

    def test_return_is_immutable_and_does_not_charge_card(self):
        with self.assertRaisesRegex(ValueError,'erledigen'):
            self.returned_record(odometer_km=12451,charges_resolved=False)
        row = self.returned_record()
        self.assertEqual(row['late_review_cents'],3)
        self.assertEqual(row['extra_km_review_cents'],0)
        self.assertEqual(row['no_objection'],1)
        self.assertFalse(vehicle_blocked(self.db,3))
        self.assertEqual(self.db.execute('SELECT status,rueckgabe_datum FROM mietvorgaenge WHERE id=7').fetchone()['status'],
                         'zurueck')
        again = self.returned_record(operator_name='Another Person',odometer_km=13000)
        self.assertEqual(again['operator_name'],'Test Person')
        self.assertEqual(existing(self.db,'h1')['odometer_km'],12010)

    def test_more_kilometers_are_separate_review_without_capture(self):
        row=self.returned_record(odometer_km=12451,no_objection=False,note='Mehrkilometer prüfen')
        self.assertEqual(row['extra_km_review_cents'],25)
        self.assertEqual(row['no_objection'],0)
        self.assertTrue(vehicle_blocked(self.db,3))
        self.assertFalse(deposit_release_eligible(self.db,'h1',7))
        arguments=dict(operator_name='Test Person',evidence_ref='INVOICE-1',
                       note='Mehrkilometer separat abgerechnet; Kunde informiert',
                       fuel_resolved=True,damage_resolved=True,time_km_resolved=True,
                       now=self.now+timedelta(minutes=1))
        with self.assertRaisesRegex(ValueError,'vollständig klären'):
            record_clearance(self.db,'h1',**{**arguments,'time_km_resolved':False})
        cleared=record_clearance(self.db,'h1',**arguments)
        self.assertEqual(cleared['evidence_ref'],'INVOICE-1')
        self.assertTrue(deposit_release_eligible(self.db,'h1',7))
        self.assertTrue(vehicle_blocked(self.db,3),
                        'Financial clearance must not release physical inventory')
        self.assertEqual(existing(self.db,'h1')['no_objection'],0)
        self.assertEqual(record_clearance(self.db,'h1',**{**arguments,'note':'Changed'})['note'],
                         arguments['note'])
        self.assertEqual(existing_clearance(self.db,'h1')['operator_name'],'Test Person')

    def test_disputed_vehicle_needs_separate_immutable_workshop_release(self):
        self.returned_record(fuel_full=False,damage_free=False,no_objection=False,
                             note='Tank nicht voll und neuer Schaden')
        self.assertTrue(vehicle_blocked(self.db,3))
        arguments=dict(operator_name='Workshop Operator',evidence_ref='WORKSHOP-1',
                       note='Nachbetankt, Schaden repariert, gereinigt und Probefahrt',
                       fuel_ready=True,damage_ready=True,cleaned=True,safe_to_rent=True,
                       now=self.now+timedelta(minutes=2))
        with self.assertRaisesRegex(ValueError,'Tank, Schäden'):
            record_vehicle_readiness(self.db,'h1',**{**arguments,'damage_ready':False})
        self.assertTrue(vehicle_blocked(self.db,3))
        released=record_vehicle_readiness(self.db,'h1',**arguments)
        self.assertEqual(released['evidence_ref'],'WORKSHOP-1')
        self.assertFalse(vehicle_blocked(self.db,3))
        self.assertFalse(deposit_release_eligible(self.db,'h1',7),
                         'Physical readiness must not release deposit')
        repeated=record_vehicle_readiness(self.db,'h1',**{**arguments,'note':'Changed'})
        self.assertEqual(repeated['note'],arguments['note'])
        self.assertEqual(existing_vehicle_readiness(self.db,'h1')['operator_name'],
                         'Workshop Operator')

    def test_unobjected_return_cannot_acquire_a_spurious_release(self):
        self.returned_record()
        with self.assertRaisesRegex(ValueError,'beanstandet'):
            record_vehicle_readiness(self.db,'h1',operator_name='Workshop Operator',
                evidence_ref='WORKSHOP-1',note='Fahrzeug fahrbereit',fuel_ready=True,
                damage_ready=True,cleaned=True,safe_to_rent=True,
                now=self.now+timedelta(minutes=2))

    def test_unresolved_fuel_or_damage_cannot_be_marked_clear(self):
        with self.assertRaisesRegex(ValueError,'Tank'):
            self.returned_record(fuel_full=False)
        with self.assertRaisesRegex(ValueError,'Tank'):
            self.returned_record(damage_free=False)
        row=self.returned_record(fuel_full=False,no_objection=False,
                                 note='Tank nicht voll; Beleg und Nachfüllmenge später prüfen')
        self.assertEqual(row['fuel_full'],0)
        self.assertEqual(row['no_objection'],0)

    def test_rejects_return_before_handover_and_mileage_rollback(self):
        with self.assertRaisesRegex(ValueError,'Schlüsselübergabe'):
            self.returned_record(returned_at=self.handed-timedelta(minutes=1))
        with self.assertRaisesRegex(ValueError,'Schlüsselübergabe'):
            self.returned_record(odometer_km=11999)
        self.assertIsNone(existing(self.db,'h1'))


if __name__ == '__main__':
    unittest.main()
