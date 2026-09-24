"""Read-only MOS signature inventory for an explicitly selected SQLite copy.

This tool does not connect to Render, use Stripe, update rows or delete data.
Run it against an access-controlled database copy after choosing a policy.
"""

import argparse
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_signature_retention import parse_cutoff, retention_report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sqlite-copy', required=True, type=Path,
                        help='Existing SQLite database copy to inspect read-only')
    parser.add_argument('--signed-before-utc',
                        help='Optional explicitly approved UTC cutoff, e.g. 2026-09-01T00:00:00Z')
    args = parser.parse_args(argv)
    path = args.sqlite_copy.resolve(strict=True)
    if not path.is_file():
        parser.error('The SQLite copy must be an existing file.')
    cutoff = parse_cutoff(args.signed_before_utc)
    uri = path.as_uri() + '?mode=ro'
    db = sqlite3.connect(uri, uri=True)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        report = retention_report(db, cutoff)
    finally:
        db.close()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
