"""Explicit MOS retention migration, dry-run and policy-gated cleanup.

No database is selected implicitly. Actual deletion is disabled unless a
documented policy file, --apply and a separate runtime switch are present.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_signature_retention import POLICY_SCOPE, RetentionPolicy, block_hold, cleanup, init_schema


class PostgresDb:
    """Small qmark adapter; psycopg owns transaction and row dictionaries."""

    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, params=()):
        return self.connection.execute(sql.replace('?', '%s'), params)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


def load_policy(path):
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict) or data.get('scope') != POLICY_SCOPE:
        raise ValueError('Retention policy scope is missing or unsupported.')
    approved_at = data.get('approved_at')
    try:
        stamp = datetime.fromisoformat(approved_at.replace('Z', '+00:00'))
    except (AttributeError, ValueError) as exc:
        raise ValueError('Policy approval time must be ISO-8601.') from exc
    if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
        raise ValueError('Policy approval time must be timezone-aware and past.')
    policy = RetentionPolicy(days=data.get('retention_days'), approval_ref=data.get('approval_ref', ''))
    if not policy.approval_ref.strip():
        raise ValueError('Documented policy approval reference is required.')
    return policy


def connect(args, *, write):
    if args.sqlite_db:
        path = args.sqlite_db.resolve(strict=True)
        if not path.is_file():
            raise ValueError('SQLite target must be an existing file.')
        db = sqlite3.connect(path.as_uri() + ('?mode=rw' if write else '?mode=ro'), uri=True)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        if not write:
            db.execute('PRAGMA query_only=ON')
        return db, False
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', args.postgres_url_env):
        raise ValueError('PostgreSQL environment variable name is invalid.')
    url = os.environ.get(args.postgres_url_env, '')
    if not url.startswith(('postgresql://', 'postgres://')):
        raise ValueError('PostgreSQL URL is missing from the selected environment variable.')
    try:
        import psycopg
        from psycopg.rows import dict_row
        connection = psycopg.connect(url, row_factory=dict_row)
    except Exception:
        raise RuntimeError('PostgreSQL connection failed; credentials were not printed.') from None
    db = PostgresDb(connection)
    if not write:
        db.execute('SET TRANSACTION READ ONLY')
    return db, True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--sqlite-db', type=Path, help='Explicit existing SQLite target')
    target.add_argument('--postgres-url-env', help='Name of environment variable containing PostgreSQL URL')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--migrate', action='store_true', help='Create only the two retention metadata tables')
    action.add_argument('--apply', action='store_true', help='Policy-gated cleanup; dry-run is the default')
    action.add_argument('--block-file', type=Path, help='Record a case-specific legal hold from a local JSON file')
    parser.add_argument('--policy-file', type=Path, help='Documented, reviewed policy JSON outside Git')
    args = parser.parse_args(argv)
    if args.apply and (not args.policy_file or os.environ.get('MOS_SIGNATURE_RETENTION_APPLY_ENABLED') != '1'):
        parser.error('Apply requires a policy file and separate runtime enablement.')
    policy = load_policy(args.policy_file) if args.policy_file else RetentionPolicy()
    db, postgres = connect(args, write=args.migrate or args.apply or bool(args.block_file))
    try:
        if args.migrate:
            init_schema(db)
            db.commit()
            result = {'schema_migrated': True, 'automatic_deletion': False}
        elif args.block_file:
            legal_hold = json.loads(args.block_file.read_text(encoding='utf-8'))
            if (not isinstance(legal_hold, dict) or not isinstance(legal_hold.get('hold_id'), str)
                    or not legal_hold['hold_id'] or not isinstance(legal_hold.get('reason'), str)):
                raise ValueError('Legal hold file requires a hold_id and reason.')
            block_hold(db, legal_hold['hold_id'], legal_hold['reason'], postgres=postgres)
            result = {'legal_hold_recorded': True, 'automatic_deletion': False}
        else:
            result = cleanup(db, policy, apply=args.apply, postgres=postgres)
    finally:
        db.close()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
