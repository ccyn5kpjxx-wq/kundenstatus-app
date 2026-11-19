"""Print data for tyre-service and shop orders, read from their original bookings."""

import json
import re
from datetime import datetime

from auftragsarten import aufnahme_daten, auftrag_leistungsart
from werkstatt_auftrag_ausdruck import _date, _time


_SIZE = re.compile(
    r"(?<!\d)\d{3}\s*/\s*\d{2,3}\s*(?:Z?R|B|D|-)\s*\d{2}(?:[C]|\.\d)?"
    r"(?:\s+\d{2,3}(?:/\d{2,3})?\s*[A-Z]\b)?(?:\s+(?:XL|RF)\b)?", re.I)


def _slot(day, at):
    day = _date(day)
    return " · ".join(filter(None, (day, _time(at)))) if day else ""


def _dimensions(text):
    return list(dict.fromkeys(match.group().strip() for match in _SIZE.finditer(text or "")))


def _position(title="", size="", delivery=""):
    return dict(title=title, size=size or "Nicht angegeben", delivery=_date(delivery) or "Termin offen")


def reifenetikett(portal, order_id):
    db = portal.get_db()
    try:
        row = db.execute("""SELECT id, quelle, leistungsart, kunde_name, kennzeichen, annahme_datum,
            annahme_uhrzeit, schaden_aufnahme_json, beschreibung FROM auftraege WHERE id=?""",
                         (order_id,)).fetchone()
        if row is None:
            return None
        order = dict(row)
        intake = aufnahme_daten(order)
        is_shop = order["quelle"] == "shop"
        if not is_shop and auftrag_leistungsart(order) != "reifenservice":
            return None

        appointment = _slot(order["annahme_datum"], order["annahme_uhrzeit"])
        appointment_label = "Reifenservice"
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
            appointment = _slot(booked["datum"], booked["uhrzeit"]) if booked else ""
            wish = intake.get("shop_montagewunsch")
            if not booked and isinstance(wish, dict):
                appointment_label = "Reifenservice · Wunschtermin"
                appointment = _slot(wish.get("date"), wish.get("time"))
        elif order["quelle"] == "check24" or intake.get("check24_bestellnummer"):
            booking = db.execute("""SELECT b.active,b.start_at,b.end_at,b.provider_id,e.payload
                FROM check24_bookings b JOIN check24_events e ON e.id=b.event_id WHERE b.order_id=?""",
                                 (order_id,)).fetchone()
            if booking:
                reference = "CHECK24 " + booking["provider_id"]
                data = json.loads(booking["payload"])
                for tyre in data.get("tires") or ():
                    positions.append(_position(tyre, ", ".join(_dimensions(tyre)), data.get("delivery_date")))
                if not positions:
                    positions.append(_position(delivery=data.get("delivery_date")))
                if booking["active"]:
                    start = datetime.fromisoformat(booking["start_at"])
                    end = datetime.fromisoformat(booking["end_at"])
                    appointment = start.strftime("%d.%m.%Y · %H:%M") + "–" + end.strftime("%H:%M")
                else:
                    appointment = "Storniert"
            else:
                reference = ("CHECK24 " + str(intake.get("check24_bestellnummer") or "")).strip()
                start = intake.get("check24_start") or ""
                appointment = _slot(start[:10], start[11:16])
        elif intake.get("reifen_buchung_id"):
            booked = db.execute("""SELECT s.datum,s.uhrzeit FROM reifen_termin_slots s
                JOIN leads l ON l.id=s.lead_id WHERE l.auftrag_id=? AND l.status!='verloren'""",
                                (order_id,)).fetchone()
            appointment = _slot(booked["datum"], booked["uhrzeit"]) if booked else ""
        if intake.get("reifen_storniert_am"):
            appointment = "Storniert"
    finally:
        db.close()

    if is_shop:
        from shop_admin import shop_admin_reader
        detail = shop_admin_reader().detail(shop_id)
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
                appointment_label=appointment_label, appointment=appointment or "Kein Termin eingetragen",
                positions=positions)
