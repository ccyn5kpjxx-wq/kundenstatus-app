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

        if not portal.USE_POSTGRES or portal.DATABASE_URL != url:
            raise RuntimeError('Only the fresh synthetic PostgreSQL database is allowed.')
        now = datetime.now(timezone.utc)
        start, end = now - timedelta(minutes=5), now + timedelta(days=1)
        until = (end + timedelta(days=2)).isoformat()
        hold_id = 'handover-test-' + name.rsplit('_', 1)[1]
        quote = {'test_only': False, 'start_slot': start.isoformat(),
                 'end_slot': end.isoformat(), 'amount_cents': 3900,
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
