"""Optional retention acceptance on a fresh, synthetic local PostgreSQL DB only.

The launcher accepts only the dedicated MOS loopback test cluster and creates
a new random database. It never opens a business database or contacts Stripe.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from mos_signature_retention import RetentionPolicy, block_hold, cleanup, init_schema
from run_mos_signature_retention import PostgresDb
from run_mos_stripe_postgres_staging import database_url, fresh_test_database, load_cluster_config


def check(cfg, name):
    import psycopg
    from psycopg.rows import dict_row

    db = PostgresDb(psycopg.connect(database_url(cfg, name), row_factory=dict_row))
    try:
        for statement in (
            '''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,status TEXT,payload TEXT,
               expires_at BIGINT,session_id TEXT,payment_intent TEXT,mietvorgang_id INTEGER)''',
            'CREATE TABLE miet_checkout_contracts (hold_id TEXT)',
            'CREATE TABLE miet_checkout_contract_delivery (hold_id TEXT)',
            'CREATE TABLE miet_checkout_order_receipts (hold_id TEXT)',
            'CREATE TABLE miet_checkout_deposit_auths (hold_id TEXT)',
            'CREATE TABLE miet_checkout_creation_attempts (hold_id TEXT)',
            'CREATE TABLE miet_checkout_events (hold_id TEXT)',
            'CREATE TABLE miet_checkout_refunds (hold_id TEXT)',
            'CREATE TABLE miet_checkout_cancellations (id TEXT)',
            'CREATE TABLE miet_checkout_handovers (hold_id TEXT)',
        ):
            db.execute(statement)
        init_schema(db)
        init_schema(db)  # Migration is idempotent.
        quote = {'quote': {'signature_png_base64': 'synthetic-signature',
                           'signed_at': '2020-01-01T00:00:00+00:00',
                           'deposit_method': 'card_authorization_at_booking',
                           'slot_policy': 'db_open_slots_v1', 'test_only': False,
                           'signed_contract_hash': 'a' * 64,
                           'signature_record_hash': 'b' * 64}}
        payload = json.dumps(quote)
        for ident, session in (('isolated', None), ('provider', 'cs_test_synthetic'),
                               ('blocked', None), ('handed', None), ('receipted', None)):
            db.execute('''INSERT INTO miet_checkout_holds
                (id,status,payload,expires_at,session_id,payment_intent,mietvorgang_id)
                VALUES (?,'released',?,?,?,?,NULL) RETURNING id''',
                (ident, payload, 1577836800, session, None)).fetchone()
        db.commit()
        db.execute('INSERT INTO miet_checkout_handovers (hold_id) VALUES (?)', ('handed',))
        db.execute('INSERT INTO miet_checkout_order_receipts (hold_id) VALUES (?)', ('receipted',))
        db.commit()
        block_hold(db, 'blocked', 'Synthetic legal hold', postgres=True,
                   now=datetime(2021, 1, 1, tzinfo=timezone.utc))
        policy = RetentionPolicy(30, 'synthetic-test-approval')
        first = cleanup(db, policy, apply=True, postgres=True,
                        now=datetime(2021, 1, 1, tzinfo=timezone.utc))
        if (first['observed'], first['deleted']) != (1, 0):
            raise AssertionError('First PostgreSQL pass must only observe the isolated hold.')
        second = cleanup(db, policy, apply=True, postgres=True,
                         now=datetime(2021, 2, 1, tzinfo=timezone.utc))
        if (second['observed'], second['deleted']) != (0, 1):
            raise AssertionError('Second PostgreSQL pass did not purge exactly one hold.')
        remaining = {row['id'] for row in db.execute('SELECT id FROM miet_checkout_holds').fetchall()}
        if remaining != {'provider', 'blocked', 'handed', 'receipted'}:
            raise AssertionError('Provider-linked or legally blocked hold was changed.')
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--connection-file', required=True)
    args = parser.parse_args()
    cfg = load_cluster_config(args.connection_file)
    name = fresh_test_database(cfg)
    check(cfg, name)
    print('PASS: isolated PostgreSQL retention observation, purge and exclusions; synthetic data only.')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('MOS PostgreSQL retention acceptance failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
