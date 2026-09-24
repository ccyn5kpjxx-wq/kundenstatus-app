"""Operator record for the physical handover of a paid MOS direct rental.

The vehicle row lock is held by the caller while ``record`` runs.  This is a
separate audit record: a paid rental is not evidence that keys were handed out.
"""

from datetime import datetime, timedelta, timezone
import json

from mos_contract_delivery import _verified_row


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_handovers (
        hold_id TEXT PRIMARY KEY, mietvorgang_id INTEGER NOT NULL UNIQUE,
        handed_at TEXT NOT NULL, operator_name TEXT NOT NULL,
        odometer_km INTEGER NOT NULL, protocol_ref TEXT NOT NULL,
        customer_receipt_confirmed INTEGER NOT NULL,
        license_checked INTEGER NOT NULL, fuel_full INTEGER NOT NULL,
        condition_recorded INTEGER NOT NULL)''')


def existing(db, hold_id):
    row = db.execute('SELECT * FROM miet_checkout_handovers WHERE hold_id=?', (hold_id,)).fetchone()
    return dict(row) if row else None


def record(db, service, hold_id, provider_intent, *, operator_name, odometer_km,
           protocol_ref, receipt_confirmed, license_checked, fuel_full,
           condition_recorded, now=None):
    """Fail closed, then persist once; caller must hold the vehicle DB lock.

    ``provider_intent`` is the freshly retrieved Stripe intent, validated again
    against the locked hold and persisted deposit. No provider request is made
    while holding the transaction open.
    """
    prior = existing(db, hold_id)
    if prior:
        return prior
    if (not all((receipt_confirmed, license_checked, fuel_full, condition_recorded))
            or not isinstance(operator_name, str) or not 2 <= len(operator_name.strip()) <= 100
            or not isinstance(protocol_ref, str) or not 3 <= len(protocol_ref.strip()) <= 100
            or type(odometer_km) is not int or not 0 <= odometer_km <= 2_000_000):
        raise ValueError('Übergabe: Empfang, Führerschein, Volltank, Zustand und Protokolldaten bestätigen.')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Übergabezeit braucht eine Zeitzone.')
    now = now.astimezone(timezone.utc)

    row, contract, _pdf = _verified_row(db, hold_id)
    hold = db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone()
    if not hold or row['cancellation_id'] or row['status'] != 'sent' or not row['accepted_at']:
        raise ValueError('Übergabe gesperrt: Vertragskopie oder Buchungsstatus muss geprüft werden.')
    quote = json.loads(hold['payload'])['quote']
    if (quote.get('test_only') is not False or contract.get('test_only') is not False
            or quote.get('deposit_method') != 'card_authorization_at_booking'
            or quote.get('deposit_authorized_cents') != 50000):
        raise ValueError('Übergabe nur für echte MOS-Direktbuchungen mit 500-EUR-Kartenreservierung.')
    try:
        start = datetime.fromisoformat(quote['start_slot'])
        end = datetime.fromisoformat(quote['end_slot'])
        accepted = datetime.fromisoformat(row['accepted_at'])
        signed = datetime.fromisoformat(row['signed_at'])
        if any(value.tzinfo is None for value in (start, end, accepted, signed)):
            raise ValueError('Zeitzone fehlt.')
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('Vertrags- oder Übergabezeit muss geprüft werden.') from exc
    if not (signed <= accepted <= now and start <= now < end):
        raise ValueError('Vertragskopie muss vor der Übergabe angenommen sein; Mietzeitraum prüfen.')

    rental = db.execute('''SELECT mietfahrzeug_id,status,rueckgabe_datum FROM mietvorgaenge
        WHERE id=?''', (hold['mietvorgang_id'],)).fetchone()
    deposit = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?',
                         (hold_id,)).fetchone()
    if (not rental or rental['mietfahrzeug_id'] != hold['mietfahrzeug_id']
            or rental['status'] != 'aktiv' or rental['rueckgabe_datum']
            or not deposit or deposit['status'] != 'authorized'
            or not deposit['capture_before'] or not provider_intent
            or provider_intent.get('livemode') is not True
            or provider_intent.get('capture_before') != deposit['capture_before']
            or provider_intent.get('status') != 'requires_capture'):
        raise ValueError('Übergabe gesperrt: Mietvorgang oder Kartenreservierung muss geprüft werden.')
    if not service._validated_deposit(dict(hold), dict(deposit), provider_intent,
                                      enforce_coverage=True):
        raise ValueError('500-EUR-Kartenreservierung ist nicht mehr gültig.')
    until = datetime.fromisoformat(deposit['capture_before'].replace('Z', '+00:00'))
    if until.tzinfo is None or until <= now or until <= end + timedelta(hours=24):
        raise ValueError('500-EUR-Kartenreservierung endet vor der geplanten Rückgabe.')

    db.execute('''INSERT INTO miet_checkout_handovers
        (hold_id,mietvorgang_id,handed_at,operator_name,odometer_km,protocol_ref,
         customer_receipt_confirmed,license_checked,fuel_full,condition_recorded)
        VALUES (?,?,?,?,?,?,1,1,1,1) RETURNING hold_id''',
        (hold_id, hold['mietvorgang_id'], now.isoformat(), operator_name.strip(),
         odometer_km, protocol_ref.strip()))
    return existing(db, hold_id)
