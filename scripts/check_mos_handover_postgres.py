"""Optional isolated PostgreSQL handover acceptance with fake SMTP/Stripe data.

Creates a fresh database only on the dedicated local 127.0.0.1:55439 cluster.
No real message, Stripe request or operational database is used.
"""

import argparse
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from check_mos_contract_delivery_postgres import FakeSMTP
from run_mos_stripe_postgres_staging import (
    clean_environment, database_url, fresh_test_database,
    isolated_process_environment, load_cluster_config,
)


def check(cfg, name, directory):
    url = database_url(cfg, name)
    with isolated_process_environment(clean_environment(directory, url)):
        import app as portal
        from mietwagen_checkout import SharedCheckout
        from mos_booking.production import RefundLedger, init_refund_schema
        from mos_public_contract import init_schema as init_contract_schema
        from mos_contract_delivery import deliver_one, enqueue
        from mos_handover import record
        from mos_return import (init_schema as init_return_schema,
                                record as record_return, record_clearance,
                                deposit_release_eligible, vehicle_blocked,
                                record_vehicle_readiness)

        if not portal.USE_POSTGRES or portal.DATABASE_URL != url:
            raise RuntimeError('Only the fresh synthetic PostgreSQL database is allowed.')
        now = datetime.now(timezone.utc)
        start, end = now - timedelta(minutes=5), now + timedelta(days=1)
        until = (end + timedelta(days=2)).isoformat()
        hold_id = 'handover-test-' + name.rsplit('_', 1)[1]
        quote = {'test_only': False, 'start_slot': start.isoformat(),
                 'end_slot': end.isoformat(), 'amount_cents': 3900,
                 'daily_cents': 3900, 'included_km': 150,
                 'extra_km_cents': 25,
                 'deposit_method': 'card_authorization_at_booking',
                 'deposit_authorized_cents': 50000}
        contract = {'test_only': False, 'customer_email': 'synthetic@example.test'}
        canonical = json.dumps(contract, ensure_ascii=False, sort_keys=True,
                               separators=(',', ':'))
        pdf = b'%PDF-1.4 synthetic handover acceptance\n%%EOF'
        pdf_hash = sha256(pdf).hexdigest()
        db = portal.get_db()
        try:
            init_refund_schema(db)
            init_contract_schema(db)
            init_return_schema(db)
            vehicle_id = db.execute('''INSERT INTO mietfahrzeuge
                (kennzeichen,erstellt_am,geaendert_am) VALUES (?,?,?) RETURNING id''',
                ('TEST-HANDOVER', portal.now_str(), portal.now_str())).fetchone()['id']
            rental_id = db.execute('''INSERT INTO mietvorgaenge
                (mietfahrzeug_id,start_datum,end_datum,status,erstellt_am,geaendert_am)
                VALUES (?,?,?,'aktiv',?,?) RETURNING id''',
                (vehicle_id, start.date().isoformat(), end.date().isoformat(),
                 portal.now_str(), portal.now_str())).fetchone()['id']
            db.execute('''INSERT INTO miet_checkout_holds
                (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,
                 status,expires_at,payment_intent,mietvorgang_id,grund)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (hold_id, hold_id, vehicle_id, start.date().isoformat(),
                 end.date().isoformat(), json.dumps({'quote': quote}), 'synthetic-digest',
                 'confirmed', 1, 'pi_synthetic_payment', rental_id, ''))
            db.execute('''INSERT INTO miet_checkout_deposit_auths
                (hold_id,intent_id,status,created_at,capture_before)
                VALUES (?,?,'authorized',?,?) RETURNING hold_id''',
                (hold_id, 'pi_synthetic_deposit', 1, until))
            db.execute('''INSERT INTO miet_checkout_contracts
                (hold_id,contract_json,contract_sha256,signer_name,signed_at,
                 signature_png_base64,pdf_base64,pdf_sha256)
                VALUES (?,?,?,?,?,?,?,?) RETURNING hold_id''',
                (hold_id, canonical, sha256(canonical.encode()).hexdigest(),
                 'Synthetic Customer', (now-timedelta(hours=1)).isoformat(),
                 'synthetic-signature', b64encode(pdf).decode(), pdf_hash))
            hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',
                                   (hold_id,)).fetchone())
            if not enqueue(db, hold, contract, pdf_hash):
                raise AssertionError('Synthetic contract was not queued.')
            db.commit()
        finally:
            db.close()

        mail = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                'smtp_user': 'synthetic@example.test', '_smtp_password': 'synthetic',
                'from_address': 'synthetic@example.test', 'display_name': 'Synthetic'}
        FakeSMTP.calls = 0
        with patch('mos_contract_delivery.smtplib.SMTP_SSL', FakeSMTP):
            if deliver_one(portal, hold_id, mail, live=True, enabled=True) != 'sent':
                raise AssertionError('Fake SMTP did not accept the synthetic contract.')
        if FakeSMTP.calls != 1:
            raise AssertionError('Fake SMTP accepted more than one message.')

        service = SharedCheckout.__new__(SharedCheckout)
        service.p = portal
        service.gateway = SimpleNamespace(livemode=True)
        intent = {'id': 'pi_synthetic_deposit', 'livemode': True,
                  'amount': 50000, 'currency': 'eur',
                  'metadata': {'hold_id': hold_id, 'quote_hash': 'synthetic-digest'},
                  'status': 'requires_capture', 'amount_capturable': 50000,
                  'capture_before': until, 'card_funding': 'credit'}
        with service.locked(vehicle_id) as (db, _):
            first = record(db, service, hold_id, intent, operator_name='Synthetic Operator',
                           odometer_km=12345, protocol_ref='SYNTHETIC-1',
                           receipt_confirmed=True, license_checked=True,
                           fuel_full=True, condition_recorded=True)
        with service.locked(vehicle_id) as (db, _):
            second = record(db, service, hold_id, intent, operator_name='Another Operator',
                            odometer_km=22222, protocol_ref='SYNTHETIC-2',
                            receipt_confirmed=True, license_checked=True,
                            fuel_full=True, condition_recorded=True)
        if first['operator_name'] != second['operator_name'] or first['odometer_km'] != 12345:
            raise AssertionError('Handover audit was not idempotent.')
        try:
            RefundLedger(service).cancel(hold_id, admin=True, no_show=True)
        except ValueError as exc:
            if 'Schlüsselübergabe' not in str(exc):
                raise
        else:
            raise AssertionError('No-show cancellation bypassed the handed-out key audit.')
        try:
            portal.mietvorgang_zuruecknehmen(rental_id)
        except ValueError as exc:
            if 'MOS-Admin' not in str(exc):
                raise
        else:
            raise AssertionError('Generic date-only return bypassed the MOS return audit.')
        return_at = end + timedelta(minutes=31)
        with service.locked(vehicle_id) as (db, _):
            returned = record_return(db, hold_id, returned_at=return_at,
                operator_name='Synthetic Operator', odometer_km=12496,
                fuel_full=True, condition_recorded=True, damage_free=True,
                charges_resolved=False, no_objection=False,
                protocol_ref='SYNTHETIC-RETURN', note='Extra kilometers for manual review',
                now=return_at+timedelta(minutes=1))
        with service.locked(vehicle_id) as (db, _):
            repeated = record_return(db, hold_id, returned_at=return_at,
                operator_name='Another Operator', odometer_km=12999,
                fuel_full=False, condition_recorded=False, damage_free=False,
                charges_resolved=False, no_objection=False,
                protocol_ref='IGNORED', note='Ignored', now=return_at+timedelta(minutes=2))
        if (returned['odometer_km'] != repeated['odometer_km']
                or returned['late_review_cents'] != 3
                or returned['extra_km_review_cents'] != 25
                or returned['no_objection'] != 0):
            raise AssertionError('PostgreSQL MOS return audit is not immutable and correctly calculated.')
        db=portal.get_db()
        try:
            if not vehicle_blocked(db,vehicle_id):
                raise AssertionError('Disputed PostgreSQL return did not block availability.')
        finally:db.close()
        with service.locked(vehicle_id) as (db, _):
            cleared = record_clearance(db, hold_id, operator_name='Synthetic Operator',
                evidence_ref='SYNTHETIC-INVOICE',
                note='Zeit und Kilometer getrennt geklärt; Kunde informiert',
                fuel_resolved=True, damage_resolved=True, time_km_resolved=True,
                now=return_at+timedelta(minutes=2))
            if not deposit_release_eligible(db, hold_id, rental_id):
                raise AssertionError('Resolved PostgreSQL return should permit separate card release.')
            if not vehicle_blocked(db,vehicle_id):
                raise AssertionError('Financial clearance released physical inventory.')
            readiness=record_vehicle_readiness(db,hold_id,
                operator_name='Workshop Operator',evidence_ref='SYNTHETIC-WORKSHOP',
                note='Tank, Schäden, Reinigung und Fahrbereitschaft geprüft',
                fuel_ready=True,damage_ready=True,cleaned=True,safe_to_rent=True,
                now=return_at+timedelta(minutes=3))
            if vehicle_blocked(db,vehicle_id):
                raise AssertionError('Documented workshop release did not unblock inventory.')
        if cleared['evidence_ref'] != 'SYNTHETIC-INVOICE':
            raise AssertionError('PostgreSQL return clearance record is missing.')
        if readiness['evidence_ref'] != 'SYNTHETIC-WORKSHOP':
            raise AssertionError('PostgreSQL workshop release record is missing.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-file', required=True)
    args = parser.parse_args()
    cfg = load_cluster_config(args.connection_file)
    directory = Path(tempfile.mkdtemp(prefix='mos-handover-pg-'))
    with isolated_process_environment(clean_environment(directory, '')):
        name = fresh_test_database(cfg)
    check(cfg, name, directory)
    print('PASS: isolated PostgreSQL adapter, fake SMTP and handover audit; no external calls.')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('MOS PostgreSQL handover acceptance failed: ' + type(exc).__name__ + ': ' + str(exc),
              file=sys.stderr)
        raise SystemExit(1)
