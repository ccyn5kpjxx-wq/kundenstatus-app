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


def _slot(day, at):
    day = _date(day)
    return f"{day} · {_time(at) or 'Zeit offen'}" if day else "Offen"


def _timestamp(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _source_time(value):
    return value.strftime("%d.%m.%Y · %H:%M") if value else "Offen"


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
        row = db.execute("""SELECT id, quelle, leistungsart, kunde_name, kennzeichen, start_datum,
            start_uhrzeit, fertig_datum, fertig_uhrzeit, schaden_aufnahme_json, beschreibung, archiviert, angebotsphase,
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

        times = dict(start_label="Arbeitsbeginn", start=_slot(order["start_datum"], order["start_uhrzeit"]),
                     end_label="Planende", end=_slot(order["fertig_datum"], order["fertig_uhrzeit"]))
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
                if booking["active"]:
                    times.update(start=_source_time(_timestamp(booking["start_at"])), end_label="Gebuchtes Ende",
                                 end=_source_time(_timestamp(booking["end_at"])))
                else:
                    times.update(start="Storniert", end="–")
            else:
                reference = ("CHECK24 " + str(intake.get("check24_bestellnummer") or "")).strip()
                times.update(start=_source_time(_timestamp(intake.get("check24_start"))), end_label="Gebuchtes Ende",
                             end=_source_time(_timestamp(intake.get("check24_ende"))))
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
