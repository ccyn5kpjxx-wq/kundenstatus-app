"""Preserve one-time employee invitations across destructive backup imports.

The caller holds ``portal_originals_operation_lock`` from this check through the
import. Invitation issuance and redemption must hold that same lock. Registering
an invitation table in BACKUP_TABLES alone cannot prevent an older snapshot from
reviving a consumed or superseded token and its previous password/auth version.
This guard reads the actual source, independently of the backup-table catalogue.
"""
from pathlib import Path
import sqlite3


INVITATION_TABLE = 'assistent_einladungen'
_ERROR = ('Datenimport gesperrt: Die Sicherung enthält vorhandene persönliche '
          'Einladungen oder deren Zugangszustand nicht unverändert. '
          'Bitte eine aktuelle Sicherung verwenden.')


def ensure_employee_invitation_state_for_import(p, *, export=None, imported_db=None, target=None):
    """Read-only fence for JSON, SQLite-row and direct SQLite-file restoration.

    A SQLite file is authoritative when both it and backup.json are present.
    Exact invitation rows, their rights/password/auth rows and employee identity
    must survive. Legacy imports remain possible before any invitation exists.
    An existing connection belongs to the caller and is never committed/closed.
    """
    own_target = target is None
    if own_target:
        target = p.get_db()
    source = None
    try:
        if not p.get_table_columns(target, INVITATION_TABLE):
            return
        invitations = [dict(row) for row in target.execute(
            'SELECT * FROM assistent_einladungen ORDER BY mitarbeiter_id').fetchall()]
        if not invitations:
            return

        if imported_db is not None:
            source = sqlite3.connect(Path(imported_db).resolve().as_uri() + '?mode=ro', uri=True)
            source.row_factory = sqlite3.Row
            source_tables = {row['name'] for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

            def incoming(table):
                return [dict(row) for row in source.execute('SELECT * FROM ' + table).fetchall()] if table in source_tables else []
        else:
            tables = (export or {}).get('tables')
            if not isinstance(tables, dict):
                raise ValueError(_ERROR)

            def incoming(table):
                return tables.get(table, [])

        rows_by_table = {}
        def require_exact(table, key, expected):
            if table not in rows_by_table:
                rows = incoming(table)
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ValueError(_ERROR)
                rows_by_table[table] = rows
            matches = [row for row in rows_by_table[table] if row.get(key) == expected[key]]
            if len(matches) != 1:
                raise ValueError(_ERROR)
            restored = matches[0]
            if any(column not in restored or restored[column] != value for column, value in expected.items()):
                raise ValueError(_ERROR)

        for invitation in invitations:
            require_exact(INVITATION_TABLE, 'mitarbeiter_id', invitation)
            mid = invitation['mitarbeiter_id']
            rights = target.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=?', (mid,)).fetchone()
            employee = target.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
            if not rights or not employee:
                raise ValueError(_ERROR)
            require_exact('assistent_rechte', 'mitarbeiter_id', dict(rights))
            require_exact('mitarbeiter', 'id', dict(employee))
    except (sqlite3.Error, OSError, TypeError, KeyError, AttributeError):
        # Never include token/password hashes or any imported private data.
        raise ValueError(_ERROR) from None
    finally:
        if source is not None:
            source.close()
        if own_target:
            target.close()
