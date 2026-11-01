"""Preserve office originals, approvals, grants and reconciliation on restore."""
import json
from pathlib import Path
import re
import sqlite3

from werkstatt_buero_zugang import BACKUP_TABLES as ACCESS_TABLES
from werkstatt_buero_bereiche import BACKUP_TABLES as SECTION_TABLES
from werkstatt_buero_belege import TABLES as DOCUMENT_TABLES
from werkstatt_buero_lexware import TABLES as LEXWARE_TABLES
from werkstatt_buero_posteingang import BACKUP_TABLES as INBOX_TABLES
from werkstatt_buero_lohn import BACKUP_TABLES as PAYROLL_TABLES, ensure_office_payroll_for_import
from werkstatt_buero_zugang import ensure_office_access_for_import

TABLES = (*ACCESS_TABLES, *SECTION_TABLES, *DOCUMENT_TABLES, *LEXWARE_TABLES, *INBOX_TABLES, *PAYROLL_TABLES)
_ACTOR_FIELDS = ('actor', 'created_by', 'decided_by', 'approved_by', 'geaendert_von')


def _canonical(rows):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError('Ungültige Büro-Sicherungsdaten.')
    return sorted(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':')) for row in rows)


def _employee_ids(rows):
    """Preserve both direct employee references and historical workflow actors."""
    mids = set()
    for row in rows:
        if row.get('mitarbeiter_id') is not None:
            mid = row['mitarbeiter_id']
            if type(mid) is not int or mid < 1:
                raise ValueError('Ungültige Mitarbeiterzuordnung.')
            mids.add(mid)
        for field in _ACTOR_FIELDS:
            actor = row.get(field)
            if isinstance(actor, str) and actor.startswith('mitarbeiter:'):
                match = re.fullmatch(r'mitarbeiter:([1-9][0-9]*)', actor)
                if not match:
                    raise ValueError('Ungültiger Büroakteur.')
                mids.add(int(match[1]))
    return mids


def _require_employee_state(db, read_rows, mids):
    for table, key, columns in (('mitarbeiter', 'id', ('id', 'name', 'aktiv')),
                                ('assistent_rechte', 'mitarbeiter_id', None)):
        incoming = read_rows(table)
        # Validate even the rows outside the protected set before filtering.
        _canonical(incoming)
        if any(type(row.get(key)) is not int or row[key] < 1 for row in incoming):
            # The importer can coerce numeric strings later. They must not
            # disguise a currently absent historical ID during this check.
            raise ValueError('Ungültige Mitarbeiterzuordnung in der Sicherung.')
        for mid in sorted(mids):
            current = db.execute('SELECT ' + (','.join(columns) if columns else '*') +
                                 ' FROM ' + table + ' WHERE ' + key + '=?', (mid,)).fetchone()
            matches = [row for row in incoming if row.get(key) == mid]
            if current is None:
                # A deleted login must remain absent instead of being revived.
                if matches:
                    raise ValueError('Eine frühere Mitarbeiterzuordnung würde wiederhergestellt.')
                continue
            if len(matches) != 1 or type(matches[0].get(key)) is not int:
                raise ValueError('Eine Mitarbeiterzuordnung würde verändert.')
            candidate = matches[0]
            if columns:
                if any(column not in candidate for column in columns):
                    raise ValueError('Eine Mitarbeiterzuordnung fehlt.')
                candidate = {column: candidate[column] for column in columns}
            if _canonical([dict(current)]) != _canonical([candidate]):
                raise ValueError('Eine Mitarbeiterzuordnung oder ihr Zugangszustand würde verändert.')


def ensure_office_state_for_import(p, *, export=None, imported_db=None, target=None, **_):
    """An old/partial backup must never revive grants or remove approvals/files.

    Office binary originals remain inline in backup JSON; they are deliberately
    not registered as externalized binary fields. SQLite imports are opened RO.
    """
    own, source = target is None, None
    db = p.get_db() if own else target
    try:
        with p.portal_originals_operation_lock():
            if imported_db is not None:
                source = sqlite3.connect(Path(imported_db).resolve().as_uri() + '?mode=ro', uri=True)
                source.row_factory = sqlite3.Row
            def read_rows(table):
                if source is not None:
                    exists = source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                    return [dict(row) for row in source.execute('SELECT * FROM ' + table).fetchall()] if exists else []
                return (export or {}).get('tables', {}).get(table, [])

            mids = set()
            for table in TABLES:
                current = [dict(row) for row in db.execute('SELECT * FROM ' + table).fetchall()]
                incoming = read_rows(table)
                if _canonical(current) != _canonical(incoming):
                    raise ValueError('Büro-Daten stimmen nicht mit der aktuellen Sicherung überein.')
                mids.update(_employee_ids(current))
            if mids:
                _require_employee_state(db, read_rows, mids)
            ensure_office_access_for_import(p, export=export, imported_db=imported_db, target=db)
            ensure_office_payroll_for_import(p, export=export, imported_db=imported_db, target=db)
    except (TypeError, KeyError, ValueError, sqlite3.Error):
        raise ValueError('Import gestoppt: Bürozugänge, Originalbelege, Freigaben oder Prüfungsnachweise würden verändert. Eine aktuelle vollständige Sicherung verwenden.') from None
    finally:
        if source is not None:
            source.close()
        if own:
            db.close()
