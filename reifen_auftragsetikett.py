"""Print data for tyre-service and shop orders, read from their original bookings."""

import json
import re
from datetime import datetime, timedelta

from auftragsarten import AUFTRAGSARTEN, aufnahme_daten, auftrag_leistungsart
from werkzeug.exceptions import HTTPException
from werkstatt_auftrag_ausdruck import _date, _time


_SIZE = re.compile(
    r"(?<!\d)\d{3}\s*/\s*\d{2,3}\s*(?:Z?R|B|D|-)\s*\d{2}(?:[C]|\.\d)?"
    r"(?:\s+\d{2,3}(?:/\d{2,3})?\s*[A-Z]\b)?(?:\s+(?:XL|RF)\b)?", re.I)
_MANUAL_BOOKING = re.compile(
    r"^CHECK24\s+([A-Z0-9]{4,20})\s*·\s*(\d{2}\.\d{2}\.\d{4}),\s*"
    r"(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})\s*Uhr(?:\s*·|\s*$)")
_DATE_IN_TEXT = re.compile(r"\b(?:\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})(?!\d)")
_TIME_IN_TEXT = re.compile(r"\b\d{1,2}:\d{2}\b")


def _slot(day, at):
    day = _date(day)
    return f"{day} · {_time(at) or 'Zeit offen'}" if day else "Offen"


def _timestamp(value):
    if not isinstance(value, str) or len(value.strip()) <= 10:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _source_time(value):
    return value.strftime("%d.%m.%Y · %H:%M") if value else "Offen"


def _manual_booking_window(order):
    # Older office orders kept the CHECK24 reference and interval in a fixed
    # heading. Only use it for printing when the order fields corroborate it.
    summary = (order.get("analyse_text") or "").strip()
    match = _MANUAL_BOOKING.match(summary)
    if not match:
        return None
    ref, day, begin, end = match.groups()
    description = (order.get("beschreibung") or "").strip()
    if not description.startswith(f"CHECK24-Bestellung {ref}."):
        return None
    if _DATE_IN_TEXT.search(summary[match.end():]) or _TIME_IN_TEXT.search(summary[match.end():]):
        return None
    if (any(_date(other_day) != day for other_day in _DATE_IN_TEXT.findall(description))
            or _TIME_IN_TEXT.findall(description) not in ([], [begin, end])):
        return None
    if not _date(day) or not _time(begin) or not _time(end) or end <= begin:
        return None
    if (_date(order["annahme_datum"]), _time(order["annahme_uhrzeit"])) != (day, begin):
        return None
    for key, expected, normalize in (("start_datum", day, _date), ("start_uhrzeit", begin, _time),
                                     ("fertig_datum", day, _date), ("fertig_uhrzeit", end, _time)):
        if order[key] and normalize(order[key]) != expected:
            return None
    return dict(start_label="Arbeitsbeginn", start=_slot(day, begin),
                end_label="Gebuchtes Ende", end=_slot(day, end))


def _workshop_times(order):
    if order["quelle"] == "intern":
        window = _manual_booking_window(order)
        if window:
            return window
    start_day = _date(order["start_datum"])
    start_at = _time(order["start_uhrzeit"])
    times = dict(start_label="Arbeitsbeginn", start=_slot(start_day, start_at),
                 end_label="Planende", end=_slot(order["fertig_datum"], order["fertig_uhrzeit"]))
    arrival_day = _date(order["annahme_datum"])
    arrival_at = _time(order["annahme_uhrzeit"])
    if (order["quelle"] != "shop" and arrival_day and arrival_at
            and (not start_day or (start_day == arrival_day and not start_at))):
        times.update(start_label="Anlieferung", start=_slot(arrival_day, arrival_at))
    return times


def _check24_times(times, booking, intake, order):
    if booking:
        if not booking["active"]:
            return dict(start_label="Arbeitsbeginn", start="Storniert", end_label="Planende", end="–")
        return dict(start_label="Arbeitsbeginn", start=_source_time(_timestamp(booking["start_at"])),
                    end_label="Gebuchtes Ende", end=_source_time(_timestamp(booking["end_at"])))
    start = _timestamp(intake.get("check24_start"))
    end = _timestamp(intake.get("check24_ende"))
    effective = times.copy()
    if start:
        effective.update(start_label="Arbeitsbeginn", start=_source_time(start))
    if end:
        effective.update(end_label="Gebuchtes Ende", end=_source_time(end))
    elif start:
        planned_day, planned_at = _date(order["fertig_datum"]), _time(order["fertig_uhrzeit"])
        if planned_day == start.strftime("%d.%m.%Y") and planned_at > start.strftime("%H:%M"):
            effective.update(end_label="Planende", end=_slot(planned_day, planned_at))
        else:
            effective.update(end_label="Gebuchtes Ende", end="Offen")
    return effective


def _reserved_hour(slot):
    # The native tyre calendar reserves the entire hour, including its planning buffer.
    start = _timestamp(f"{slot['datum']}T{slot['uhrzeit']}")
    return dict(start_label="Arbeitsbeginn", start=_source_time(start), end_label="Planende (Zeitblock)",
                end=_source_time(start + timedelta(hours=1) if start else None))


def _shop_services(reader, order_id):
    from shop_order_receipts import LABELS
    from shop_order_store import workshop_mounting_services, workshop_service_codes
    try:
        with reader.read() as (db, _):
            row = db.execute("SELECT snapshot_json FROM shop_orders_v2 WHERE id=?", (order_id,)).fetchone()
        snapshot = json.loads(row["snapshot_json"]) if row else {}
        codes = workshop_service_codes(snapshot)
        mounting = workshop_mounting_services(snapshot) if codes else {}
        labels = [label for selected in mounting.values() for label in selected]
        booked = set().union(*codes.values())
        if "mounting" in booked and not labels:
            labels.append(LABELS["mounting"])
        labels.extend(label for code, label in LABELS.items() if code != "mounting" and code in booked)
        return list(dict.fromkeys(labels))
    except (HTTPException, TypeError, ValueError, KeyError):
        return []


def _dimensions(text):
    return list(dict.fromkeys(match.group().strip() for match in _SIZE.finditer(text or "")))


def _position(title="", size="", delivery=""):
    return dict(title=title, size=size or "Nicht angegeben", delivery=_date(delivery) or "Termin offen")


def tafel_etikett_sichtbar(portal, order):
    return (not order.get("archiviert") and not order.get("angebotsphase")
            and portal.werkstatt_tafel_auftrag_sichtbar(order)
            and (order.get("quelle") == "shop" or auftrag_leistungsart(order) == "reifenservice"))


def reifenetikett(portal, order_id, *, tafel=False, nur_arbeitsdaten=False):
    db = portal.get_db()
    try:
        row = db.execute("""SELECT id, quelle, leistungsart, kunde_name, kennzeichen, annahme_datum, annahme_uhrzeit,
            start_datum, start_uhrzeit, fertig_datum, fertig_uhrzeit, schaden_aufnahme_json,
            analyse_text, beschreibung, archiviert, angebotsphase,
            status, versicherung_id, versicherung_freigabe_status, schaden_eigenauftrag
            FROM auftraege WHERE id=?""",
                         (order_id,)).fetchone()
        if row is None:
            return None
        order = dict(row)
        if tafel and not tafel_etikett_sichtbar(portal, order):
            return None
        intake = aufnahme_daten(order)
        is_shop = order["quelle"] == "shop"
        if not is_shop and auftrag_leistungsart(order) != "reifenservice":
            return None

        times = _workshop_times(order)
        selected = intake.get("reifen_leistungen")
        services = [portal.WEBSITE_REIFEN_LEISTUNGEN[k] for k in selected
                    if isinstance(k, str) and k in portal.WEBSITE_REIFEN_LEISTUNGEN] if isinstance(selected, list) else []
        reference = ""
        positions = []
        if is_shop:
            link = db.execute("SELECT shop_order_id FROM shop_montageauftraege WHERE auftrag_id=?",
                              (order_id,)).fetchone()
            if not link:
                return None
            shop_id = link["shop_order_id"]
            booked = db.execute("SELECT datum,uhrzeit FROM shop_reifen_termin_slots WHERE shop_order_id=?",
                                (shop_id,)).fetchone()
            wish = intake.get("shop_montagewunsch")
            if booked:
                times = _reserved_hour(booked)
            elif times["start"] == "Offen" and isinstance(wish, dict):
                times.update(start_label="Wunschtermin", start=_slot(wish.get("date"), wish.get("time")))
        elif order["quelle"] == "check24" or intake.get("check24_bestellnummer"):
            booking = db.execute("""SELECT b.active,b.start_at,b.end_at,b.provider_id,e.payload
                FROM check24_bookings b JOIN check24_events e ON e.id=b.event_id WHERE b.order_id=?""",
                                 (order_id,)).fetchone()
            if booking:
                reference = "CHECK24 " + booking["provider_id"]
                data = json.loads(booking["payload"])
                if data.get("service"):
                    services = [data["service"]]
                if not nur_arbeitsdaten:
                    for tyre in data.get("tires") or ():
                        positions.append(_position(tyre, ", ".join(_dimensions(tyre)), data.get("delivery_date")))
                    if not positions:
                        positions.append(_position(delivery=data.get("delivery_date")))
            else:
                reference = ("CHECK24 " + str(intake.get("check24_bestellnummer") or "")).strip()
            times = _check24_times(times, booking, intake, order)
            disposal = intake.get("check24_entsorgung_menge")
            if type(disposal) is int and disposal > 0:
                services.append(f"{disposal} Altreifen entsorgen")
        elif intake.get("reifen_buchung_id"):
            booked = db.execute("""SELECT s.datum,s.uhrzeit FROM reifen_termin_slots s
                JOIN leads l ON l.id=s.lead_id WHERE l.auftrag_id=? AND l.status!='verloren'""",
                                (order_id,)).fetchone()
            if booked:
                times = _reserved_hour(booked)
        if intake.get("reifen_storniert_am"):
            times.update(start_label="Arbeitsbeginn", start="Storniert", end="–")
    finally:
        db.close()

    if is_shop:
        from shop_admin import shop_admin_reader
        try:
            reader = shop_admin_reader()
            services = _shop_services(reader, shop_id)
        except (HTTPException, KeyError):
            if not nur_arbeitsdaten:
                raise
            services = []
    services = list(dict.fromkeys(services))
    fallback = AUFTRAGSARTEN.get(auftrag_leistungsart(order), {}).get("label") if is_shop else "Reifenservice"
    times.update(service=" · ".join(services) or fallback or "Reifenservice", service_items=services)
    if nur_arbeitsdaten:
        return times
    if is_shop:
        detail = reader.detail(shop_id)
        for line in detail["lines"]:
            title = f"{line['quantity']} × {line['name']}"
            size = line["tyre_size"] or ", ".join(_dimensions(line["name"]))
            if line["category"] != "tyre" and not size:
                size = "–"
            positions.append(_position(title, size, line["supplier"]["eta_date"]))
    if not positions:
        positions = [_position(size=size) for size in _dimensions(order["beschreibung"])] or [_position()]
    return dict(number=order_id, customer=order["kunde_name"] or "Nicht angegeben",
                plate=order["kennzeichen"] or "Nicht angegeben", reference=reference,
                **times, tafel=tafel,
                positions=positions)
