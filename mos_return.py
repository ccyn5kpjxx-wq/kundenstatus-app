"""Audited physical return of a live MOS direct rental.

The caller holds the vehicle lock. Amounts are review aids only: this module
never charges a card, captures a deposit, or creates a customer invoice.
"""

from datetime import datetime, timedelta, timezone
import json
from zoneinfo import ZoneInfo


BERLIN = ZoneInfo('Europe/Berlin')


def init_vehicle_schema(db):
    """Core availability tables, created even when MOS booking is disabled."""
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_vehicle_blocks (
        hold_id TEXT PRIMARY KEY, mietfahrzeug_id INTEGER NOT NULL,
        blocked_at TEXT NOT NULL, reason TEXT NOT NULL)''')
    db.execute('''CREATE INDEX IF NOT EXISTS idx_miet_checkout_vehicle_blocks_vehicle
        ON miet_checkout_vehicle_blocks (mietfahrzeug_id)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_vehicle_readiness (
        hold_id TEXT PRIMARY KEY, released_at TEXT NOT NULL,
        operator_name TEXT NOT NULL, evidence_ref TEXT NOT NULL,
        note TEXT NOT NULL, fuel_ready INTEGER NOT NULL,
        damage_ready INTEGER NOT NULL, cleaned INTEGER NOT NULL,
        safe_to_rent INTEGER NOT NULL)''')


def vehicle_blocked(db, vehicle_id):
    return bool(db.execute('''SELECT 1 FROM miet_checkout_vehicle_blocks b
        WHERE b.mietfahrzeug_id=?
          AND NOT EXISTS (SELECT 1 FROM miet_checkout_vehicle_readiness ready
                          WHERE ready.hold_id=b.hold_id) LIMIT 1''',
                           (int(vehicle_id),)).fetchone())


def init_schema(db):
    init_vehicle_schema(db)
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_returns (
        hold_id TEXT PRIMARY KEY, mietvorgang_id INTEGER NOT NULL UNIQUE,
        returned_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
        operator_name TEXT NOT NULL, odometer_km INTEGER NOT NULL,
        fuel_full INTEGER NOT NULL, condition_recorded INTEGER NOT NULL,
        damage_free INTEGER NOT NULL, charges_resolved INTEGER NOT NULL,
        no_objection INTEGER NOT NULL, protocol_ref TEXT NOT NULL,
        note TEXT NOT NULL, late_review_cents BIGINT NOT NULL,
        extra_km_review_cents BIGINT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_return_clearances (
        hold_id TEXT PRIMARY KEY, cleared_at TEXT NOT NULL,
        operator_name TEXT NOT NULL, evidence_ref TEXT NOT NULL,
        note TEXT NOT NULL, fuel_resolved INTEGER NOT NULL,
        damage_resolved INTEGER NOT NULL, time_km_resolved INTEGER NOT NULL)''')


def existing(db, hold_id):
    row = db.execute('SELECT * FROM miet_checkout_returns WHERE hold_id=?',
                     (hold_id,)).fetchone()
    return dict(row) if row else None


def existing_clearance(db, hold_id):
    row = db.execute('SELECT * FROM miet_checkout_return_clearances WHERE hold_id=?',
                     (hold_id,)).fetchone()
    return dict(row) if row else None


def existing_vehicle_readiness(db, hold_id):
    row = db.execute('SELECT * FROM miet_checkout_vehicle_readiness WHERE hold_id=?',
                     (hold_id,)).fetchone()
    return dict(row) if row else None


def deposit_release_eligible(db, hold_id, rental_id):
    """A handed-out live rental needs a return and resolved audit before release."""
    returned = existing(db, hold_id)
    if not returned or returned['mietvorgang_id'] != rental_id:
        return False
    rental = db.execute('SELECT status FROM mietvorgaenge WHERE id=?',
                        (rental_id,)).fetchone()
    if not rental or rental['status'] != 'zurueck':
        return False
    return bool(returned['no_objection'] or existing_clearance(db, hold_id))


def late_review_cents(scheduled_return, actual_return, daily_cents):
    """30 free minutes, then actual seconds beyond grace at day price/24h."""
    if (scheduled_return.tzinfo is None or actual_return.tzinfo is None
            or type(daily_cents) is not int or daily_cents <= 0):
        raise ValueError('Zeit oder vereinbarter Tagessatz für Rückgabe fehlt.')
    extra_seconds = max(0, round((actual_return.astimezone(timezone.utc) -
                                  scheduled_return.astimezone(timezone.utc) -
                                  timedelta(minutes=30)).total_seconds()))
    return (daily_cents * extra_seconds + 43200) // 86400


def release_review_deadline(returned_at, holidays_for_year):
    """Conservative internal reminder by the second Mon–Sat business day.

    Count the return date if it is a business day, even if the return was late.
    This intentionally warns earlier than a contract deadline might require.
    """
    if not isinstance(returned_at, datetime) or returned_at.tzinfo is None:
        raise ValueError('Tatsächliche Rückgabezeit mit Zeitzone fehlt.')
    day = returned_at.astimezone(BERLIN).date()
    counted = 1 if day.weekday() < 6 and day not in holidays_for_year(day.year) else 0
    while counted < 2:
        day += timedelta(days=1)
        if day.weekday() < 6 and day not in holidays_for_year(day.year):
            counted += 1
    return datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=BERLIN)


def record(db, hold_id, *, returned_at, operator_name, odometer_km,
           fuel_full, condition_recorded, damage_free, charges_resolved,
           no_objection, protocol_ref,
           note='', now=None):
    """Persist the first factual return; later corrections need manual review."""
    prior = existing(db, hold_id)
    if prior:
        return prior
    now = now or datetime.now(timezone.utc)
    if (not isinstance(returned_at, datetime) or returned_at.tzinfo is None
            or now.tzinfo is None or returned_at > now):
        raise ValueError('Tatsächliche Rückgabezeit mit Zeitzone angeben; keine Zukunftszeit.')
    operator_name = operator_name.strip() if isinstance(operator_name, str) else ''
    protocol_ref = protocol_ref.strip() if isinstance(protocol_ref, str) else ''
    note = note.strip() if isinstance(note, str) else ''
    if (not 2 <= len(operator_name) <= 100 or not 3 <= len(protocol_ref) <= 100
            or type(odometer_km) is not int or not 0 <= odometer_km <= 2_000_000
            or type(fuel_full) is not bool or type(condition_recorded) is not bool
            or type(damage_free) is not bool or type(charges_resolved) is not bool
            or type(no_objection) is not bool or not condition_recorded
            or len(note) > 1000 or (not no_objection and len(note) < 3)
            or (no_objection and not (fuel_full and damage_free))):
        raise ValueError('Rückgabezeit, Kilometer, Tank, Zustand und Prüfvermerk vollständig dokumentieren.')
    hold = db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone()
    if not hold or hold['status'] != 'confirmed' or not hold['mietvorgang_id']:
        raise ValueError('Nur bestätigte MOS-Direktmieten können zurückgenommen werden.')
    quote = json.loads(hold['payload'])['quote']
    if quote.get('test_only') is not False:
        raise ValueError('Rückgabeprotokoll nur für echte MOS-Direktmieten.')
    handover = db.execute('SELECT * FROM miet_checkout_handovers WHERE hold_id=?',
                          (hold_id,)).fetchone()
    rental = db.execute('SELECT * FROM mietvorgaenge WHERE id=?',
                        (hold['mietvorgang_id'],)).fetchone()
    if (not handover or handover['mietvorgang_id'] != hold['mietvorgang_id']
            or not rental or rental['mietfahrzeug_id'] != hold['mietfahrzeug_id']
            or rental['status'] != 'aktiv' or rental['rueckgabe_datum']
            or returned_at < datetime.fromisoformat(handover['handed_at'])
            or odometer_km < handover['odometer_km']):
        raise ValueError('Schlüsselübergabe, aktive Miete und Rückgabe-Kilometerstand prüfen.')
    try:
        end = datetime.fromisoformat(quote['end_slot'])
        daily_cents = quote['daily_cents']
        included_km = quote['included_km']
        extra_km_cents = quote['extra_km_cents']
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('Vereinbarter Rückgabetermin oder Tarif fehlt im Vertrag.') from exc
    if (type(included_km) is not int or included_km < 0
            or type(extra_km_cents) is not int or extra_km_cents < 0):
        raise ValueError('Kilometertarif fehlt im Vertrag.')
    late = late_review_cents(end, returned_at, daily_cents)
    excess_km = max(0, odometer_km - handover['odometer_km'] - included_km)
    extra_km = excess_km * extra_km_cents
    if no_objection and (late or extra_km) and (not charges_resolved or len(note) < 3):
        raise ValueError('Zeit- oder Kilometerüberschreitung vor unbeanstandeter Freigabe belegen und erledigen.')
    recorded_at = now.astimezone(timezone.utc).isoformat()
    db.execute('''INSERT INTO miet_checkout_returns
        (hold_id,mietvorgang_id,returned_at,recorded_at,operator_name,
         odometer_km,fuel_full,condition_recorded,damage_free,charges_resolved,
         no_objection,protocol_ref,
         note,late_review_cents,extra_km_review_cents)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING hold_id''',
        (hold_id, hold['mietvorgang_id'], returned_at.astimezone(timezone.utc).isoformat(),
         recorded_at, operator_name, odometer_km, int(fuel_full), int(condition_recorded),
         int(damage_free), int(charges_resolved), int(no_objection), protocol_ref,
         note, late, extra_km)).fetchone()
    if not no_objection:
        db.execute('''INSERT INTO miet_checkout_vehicle_blocks
            (hold_id,mietfahrzeug_id,blocked_at,reason)
            VALUES (?,?,?,'return_disputed') RETURNING hold_id''',
            (hold_id, hold['mietfahrzeug_id'], recorded_at)).fetchone()
    db.execute("UPDATE mietvorgaenge SET status='zurueck',rueckgabe_datum=?,geaendert_am=? WHERE id=?",
               (returned_at.astimezone(BERLIN).strftime('%d.%m.%Y'),
                now.astimezone(BERLIN).strftime('%d.%m.%Y %H:%M'), hold['mietvorgang_id']))
    return existing(db, hold_id)


def record_vehicle_readiness(db, hold_id, *, operator_name, evidence_ref, note,
                             fuel_ready, damage_ready, cleaned, safe_to_rent,
                             now=None):
    """Unlock physical availability independently of financial clearance."""
    prior = existing_vehicle_readiness(db, hold_id)
    if prior:
        return prior
    returned = existing(db, hold_id)
    block = db.execute('SELECT * FROM miet_checkout_vehicle_blocks WHERE hold_id=?',
                       (hold_id,)).fetchone()
    hold = db.execute('SELECT status,mietfahrzeug_id,mietvorgang_id,payload FROM miet_checkout_holds WHERE id=?',
                      (hold_id,)).fetchone()
    if (not returned or returned['no_objection'] or not block or not hold
            or hold['status']!='confirmed' or hold['mietvorgang_id']!=returned['mietvorgang_id']
            or hold['mietfahrzeug_id']!=block['mietfahrzeug_id']
            or json.loads(hold['payload'])['quote'].get('test_only') is not False
            or not db.execute("SELECT id FROM mietvorgaenge WHERE id=? AND status='zurueck'",
                              (returned['mietvorgang_id'],)).fetchone()):
        raise ValueError('Nur ein beanstandet zurückgegebenes echtes MOS-Fahrzeug kann betrieblich freigegeben werden.')
    operator_name = operator_name.strip() if isinstance(operator_name,str) else ''
    evidence_ref = evidence_ref.strip() if isinstance(evidence_ref,str) else ''
    note = note.strip() if isinstance(note,str) else ''
    if (not 2<=len(operator_name)<=100 or not 3<=len(evidence_ref)<=100
            or not 5<=len(note)<=1000
            or not all((fuel_ready is True,damage_ready is True,
                        cleaned is True,safe_to_rent is True))):
        raise ValueError('Tank, Schäden, Aufbereitung und Fahrbereitschaft mit Beleg prüfen.')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now < datetime.fromisoformat(returned['recorded_at']):
        raise ValueError('Betriebsfreigabezeit muss nach Rückgabeprotokoll liegen.')
    db.execute('''INSERT INTO miet_checkout_vehicle_readiness
        (hold_id,released_at,operator_name,evidence_ref,note,
         fuel_ready,damage_ready,cleaned,safe_to_rent)
        VALUES (?,?,?,?,?,1,1,1,1) RETURNING hold_id''',
        (hold_id,now.astimezone(timezone.utc).isoformat(),operator_name,evidence_ref,note)).fetchone()
    return existing_vehicle_readiness(db,hold_id)


def record_clearance(db, hold_id, *, operator_name, evidence_ref, note,
                     fuel_resolved, damage_resolved, time_km_resolved, now=None):
    """Append a separate immutable decision; never rewrite the factual return."""
    prior = existing_clearance(db, hold_id)
    if prior:
        return prior
    returned = existing(db, hold_id)
    if not returned or returned['no_objection']:
        raise ValueError('Nur eine beanstandete dokumentierte Rückgabe kann nachträglich geklärt werden.')
    hold = db.execute('SELECT status,mietvorgang_id,payload FROM miet_checkout_holds WHERE id=?',
                      (hold_id,)).fetchone()
    if (not hold or hold['status'] != 'confirmed'
            or hold['mietvorgang_id'] != returned['mietvorgang_id']
            or json.loads(hold['payload'])['quote'].get('test_only') is not False
            or not db.execute("SELECT id FROM mietvorgaenge WHERE id=? AND status='zurueck'",
                              (returned['mietvorgang_id'],)).fetchone()):
        raise ValueError('Bestätigte echte MOS-Miete muss als zurückgegeben dokumentiert sein.')
    operator_name = operator_name.strip() if isinstance(operator_name, str) else ''
    evidence_ref = evidence_ref.strip() if isinstance(evidence_ref, str) else ''
    note = note.strip() if isinstance(note, str) else ''
    if (not 2 <= len(operator_name) <= 100 or not 3 <= len(evidence_ref) <= 100
            or not 5 <= len(note) <= 1000
            or not all((fuel_resolved is True, damage_resolved is True,
                        time_km_resolved is True))):
        raise ValueError('Tank, Schaden, Zeit und Kilometer vollständig klären und Beleg dokumentieren.')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now < datetime.fromisoformat(returned['recorded_at']):
        raise ValueError('Klärungszeit muss nach dem Rückgabeprotokoll liegen.')
    db.execute('''INSERT INTO miet_checkout_return_clearances
        (hold_id,cleared_at,operator_name,evidence_ref,note,
         fuel_resolved,damage_resolved,time_km_resolved)
        VALUES (?,?,?,?,?,1,1,1) RETURNING hold_id''',
        (hold_id,now.astimezone(timezone.utc).isoformat(),operator_name,
         evidence_ref,note)).fetchone()
    return existing_clearance(db, hold_id)
