"""Optional isolated PostgreSQL adapter acceptance with synthetic data and fake SMTP.

Creates a new database only on the dedicated loopback MOS test cluster. It never
opens an existing business database, contacts Stripe, or sends a real message.
"""

import argparse
from base64 import b64encode
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from run_mos_stripe_postgres_staging import (
    clean_environment, database_url, fresh_test_database,
    isolated_process_environment, load_cluster_config,
)


class FakeSMTP:
    calls = 0

    def __init__(self, *args, **kwargs):
        pass

    def login(self, *args):
        pass

    def mail(self, *args):
        return 250, b'ok'

    def rcpt(self, *args):
        return 250, b'ok'

    def data(self, raw):
        if b'MOS-Mietvertrag-' not in raw:
            raise AssertionError('Synthetic contract PDF attachment missing.')
        type(self).calls += 1
        return 250, b'ok'

    def quit(self):
        pass


def check(cfg, name, directory):
    url = database_url(cfg, name)
    with isolated_process_environment(clean_environment(directory, url)):
        import app as portal
        from mietwagen_checkout import init_schema as init_checkout_schema
        from mos_public_contract import init_schema as init_contract_schema
        from mos_contract_delivery import deliver_one, enqueue, unfinished_live_ids
        if not portal.USE_POSTGRES or portal.DATABASE_URL != url:
            raise RuntimeError('Isolated test database was not loaded.')
        hold_id = 'delivery-test-' + name.rsplit('_', 1)[1]
        test_id = 'delivery-test-only-' + name.rsplit('_', 1)[1]
        contract = {'customer_email': 'synthetic@example.test', 'test_only': False}
        canonical = json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        pdf = b'%PDF-1.4 synthetic contract acceptance\n%%EOF'
        pdf_hash = sha256(pdf).hexdigest()
        now = datetime.now(timezone.utc).isoformat()
        db = portal.get_db()
        try:
            init_checkout_schema(db)
            init_contract_schema(db)
            db.execute('CREATE TABLE IF NOT EXISTS miet_checkout_cancellations (id TEXT PRIMARY KEY)')
            for id_, test_only in ((hold_id, False), (test_id, True)):
                db.execute('''INSERT INTO miet_checkout_holds
                    (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,
                     status,expires_at,payment_intent,mietvorgang_id,grund)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id''',
                    (id_, id_, 1, '2026-09-24', '2026-09-25',
                     json.dumps({'quote': {'test_only': test_only}}), 'synthetic',
                     'confirmed', 1, 'pi_synthetic_' + id_, 1 if not test_only else 2, ''))
            db.execute('''INSERT INTO miet_checkout_contracts
                (hold_id,contract_json,contract_sha256,signer_name,signed_at,
                 signature_png_base64,pdf_base64,pdf_sha256)
                VALUES (?,?,?,?,?,?,?,?) RETURNING hold_id''',
                (hold_id, canonical, sha256(canonical.encode('utf-8')).hexdigest(),
                 'Synthetic Customer', now, 'synthetic-signature',
                 b64encode(pdf).decode('ascii'), pdf_hash))
            hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            if not enqueue(db, hold, contract, pdf_hash):
                raise AssertionError('Live synthetic contract was not queued.')
            db.commit()
        finally:
            db.close()
        if unfinished_live_ids(portal):
            raise AssertionError('Explicit test-only hold was not excluded from backfill.')
        mail_cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                    'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                    'smtp_user': 'synthetic@example.test', '_smtp_password': 'synthetic',
                    'from_address': 'synthetic@example.test', 'display_name': 'Synthetic'}
        with patch('mos_contract_delivery.smtplib.SMTP_SSL', FakeSMTP):
            if deliver_one(portal, hold_id, mail_cfg, live=True, enabled=True) != 'sent':
                raise AssertionError('First synthetic delivery was not accepted.')
            if deliver_one(portal, hold_id, mail_cfg, live=True, enabled=True) != 'sent':
                raise AssertionError('Repeated delivery did not remain idempotent.')
        if FakeSMTP.calls != 1:
            raise AssertionError('Duplicate SMTP DATA submission in adapter test.')
        db = portal.get_db()
        try:
            state = db.execute('SELECT status,attempts FROM miet_checkout_contract_delivery WHERE hold_id=?',
                               (hold_id,)).fetchone()
            if (state['status'], state['attempts']) != ('sent', 1):
                raise AssertionError('Synthetic delivery state was not persisted.')
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-file', required=True)
    args = parser.parse_args()
    cfg = load_cluster_config(args.connection_file)
    directory = Path(tempfile.mkdtemp(prefix='mos-delivery-pg-'))
    with isolated_process_environment(clean_environment(directory, '')):
        name = fresh_test_database(cfg)
    check(cfg, name, directory)
    print('PASS: isolated PostgreSQL adapter and fake SMTP delivery; no real e-mail.')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('MOS PostgreSQL delivery acceptance failed: ' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
