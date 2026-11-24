"""Manually recorded vehicle occupancy, using inclusive Berlin calendar dates."""
from datetime import date, datetime, timezone
import json
from zoneinfo import ZoneInfo

from mietwagen_checkout import locked_vehicle
from mos_contract_delivery import confirmed_order


SCHEMA_FEATURE = 'mietkalender_sperren_v1'
SOURCES = {'telefon': 'Telefonbuchung', 'vor_ort': 'Vor-Ort-Buchung', 'sonstiges': 'Sonstiger Grund'}


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS mietkalender_sperren (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vehicle_id INTEGER NOT NULL REFERENCES mietfahrzeuge(id),
        start_date TEXT NOT NULL, end_date TEXT NOT NULL,
        source TEXT NOT NULL, note TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, created_by TEXT NOT NULL,
        released_at TEXT, released_by TEXT,
        CHECK (start_date <= end_date)
    )''')
    db.execute('''CREATE INDEX IF NOT EXISTS idx_mietkalender_sperren_belegung
        ON mietkalender_sperren (vehicle_id, start_date, end_date) WHERE released_at IS NULL''')


def overlaps(db, vehicle_id, start, end):
    return db.execute('''SELECT 1 FROM mietkalender_sperren
        WHERE vehicle_id=? AND released_at IS NULL AND start_date<=? AND end_date>=? LIMIT 1''',
        (vehicle_id, (end or date.max).isoformat(), start.isoformat())).fetchone() is not None


def create(portal, vehicle_id, start, end, source, note='', actor=''):
    first, last = portal.validiere_mietzeitraum(start, end)
    if source not in SOURCES:
        raise ValueError('Bitte den Buchungsweg auswählen.')
    note = (note or '').strip()
    if len(note) > 500:
        raise ValueError('Die Notiz darf höchstens 500 Zeichen enthalten.')
    try:
        vehicle_id = int(vehicle_id)
    except (TypeError, ValueError) as error:
        raise ValueError('Bitte ein Fahrzeug auswählen.') from error
    with locked_vehicle(portal, vehicle_id) as (db, vehicle):
        if not vehicle['aktiv'] or portal.normalize_mietfahrzeug_status(vehicle['status']) in {'bald', 'wartung', 'inaktiv'}:
            raise ValueError('Dieses Fahrzeug ist aktuell nicht zur Vermietung freigegeben.')
        if not portal.mietfahrzeug_zeitraum_frei_db(db, vehicle_id, first, last):
            raise ValueError('Das Fahrzeug ist in diesem Zeitraum bereits belegt oder gesperrt.')
        result = db.execute('''INSERT INTO mietkalender_sperren
            (vehicle_id, start_date, end_date, source, note, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (vehicle_id, first.isoformat(), last.isoformat(), source, note,
             datetime.now(timezone.utc).isoformat(), actor or 'Werkstatt'))
        return result.lastrowid


def release(portal, block_id, actor=''):
    db = portal.get_db()
    try:
        result = db.execute('''UPDATE mietkalender_sperren SET released_at=?, released_by=?
            WHERE id=? AND released_at IS NULL''',
            (datetime.now(timezone.utc).isoformat(), actor or 'Werkstatt', block_id))
        db.commit()
        return result.rowcount == 1
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def list_active(portal, start=None, end=None):
    sql = '''SELECT b.id, b.vehicle_id, b.start_date, b.end_date, b.source, b.note,
                    f.bezeichnung AS vehicle_name, f.kennzeichen
             FROM mietkalender_sperren b JOIN mietfahrzeuge f ON f.id=b.vehicle_id
             WHERE b.released_at IS NULL'''
    params = []
    if start is not None:
        sql += ' AND b.end_date>=?'
        params.append(portal.parse_date(start).isoformat())
    if end is not None:
        sql += ' AND b.start_date<=?'
        params.append(portal.parse_date(end).isoformat())
    db = portal.get_db()
    try:
        return [dict(row) for row in db.execute(sql + ' ORDER BY b.start_date, f.bezeichnung, b.id', params).fetchall()]
    finally:
        db.close()


def vermietungsuebersicht(portal, heute):
    """One row per open rental or current/future all-day block."""
    db = portal.get_db()
    try:
        rentals = db.execute("""SELECT v.id, v.mietfahrzeug_id, v.kunde_name, v.start_datum, v.end_datum,
                f.bezeichnung, f.kennzeichen, h.id AS hold_id, h.payload, h.status AS hold_status,
                h.payment_intent, uebergabe.handed_at
            FROM mietvorgaenge v JOIN mietfahrzeuge f ON f.id=v.mietfahrzeug_id
            LEFT JOIN miet_checkout_holds h ON h.mietvorgang_id=v.id
            LEFT JOIN miet_checkout_handovers uebergabe ON uebergabe.hold_id=h.id
            WHERE COALESCE(v.status,'') NOT IN ('zurueck','storniert')
              AND COALESCE(v.rueckgabe_datum,'')=''""").fetchall()
    finally:
        db.close()

    def local_time(value):
        if not value:
            return None
        try:
            stamp = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return None
        return stamp.astimezone(ZoneInfo('Europe/Berlin')) if stamp.tzinfo is not None else None

    agenda = []
    for rental in rentals:
        start, end = (portal.parse_date(rental[k]) for k in ('start_datum', 'end_datum'))
        pickup = dropoff = None
        if rental['hold_id']:
            try:
                quote = json.loads(rental['payload'])['quote']
            except (TypeError, ValueError, KeyError):
                continue
            if (not isinstance(quote, dict) or quote.get('test_only') is not False
                    or not confirmed_order(rental['hold_status'], rental['payment_intent'], quote)):
                continue
            pickup, dropoff = (local_time(quote.get(k)) for k in ('start_slot', 'end_slot'))
        if not start:
            continue
        agenda.append({
            'start': start, 'pickup': pickup, 'end': end, 'dropoff': dropoff,
            'vehicle': rental['bezeichnung'], 'plate': rental['kennzeichen'], 'customer': rental['kunde_name'],
            'source': 'Website-Buchung' if rental['hold_id'] else 'Mietvorgang',
            'vehicle_id': rental['mietfahrzeug_id'], 'hold_id': rental['hold_id'],
            'handed_at': local_time(rental['handed_at']), 'overdue': end is not None and end < heute,
        })
    for block in list_active(portal, start=heute):
        agenda.append({
            'start': date.fromisoformat(block['start_date']), 'end': date.fromisoformat(block['end_date']),
            'vehicle': block['vehicle_name'], 'plate': block['kennzeichen'],
            'source': SOURCES.get(block['source'], 'Fahrzeugsperre'), 'block_id': block['id'], 'note': block['note'],
        })
    return sorted(agenda, key=lambda row: (row['start'], row['pickup'].time() if row.get('pickup') else datetime.min.time(),
                                           row['plate'] or ''))
