"""Pure, source-preserving daily briefing from safe workshop order dictionaries.

Portal status: 1 angelegt, 2 eingeplant, 3 in Arbeit, 4 fertig,
5 zurückgegeben. Annahme/Abholung are transport dates, not finish deadlines.
No database, document analysis, network access or writes occur here.
"""

from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
import re
from zoneinfo import ZoneInfo


BERLIN = ZoneInfo("Europe/Berlin")
SPOKEN_ITEM_LIMIT = 5
CATEGORIES = (
    "ueberfaellig", "heute_faellig", "abholung_durch_werkstatt_heute",
    "anlieferung_heute", "rueckbringung_heute", "kundenabholung_heute",
    "transport_ungeklaert",
)


def _date(value):
    if isinstance(value, datetime):
        return value.astimezone(BERLIN).date() if value.tzinfo is not None else value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), pattern).date()
            except ValueError:
                continue
    return None


def _time(value):
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)(?: Uhr)?", value.strip())
    return f"{int(match[1]):02d}:{match[2]}" if match else None


def _positive_id(value):
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,18}", value.strip()):
        return int(value) if int(value) > 0 else None
    return None


def _text(value, limit=120):
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())[:limit]


def _spoken(event):
    subject = f"Auftrag {event['auftrag_id']}"
    if event["fahrzeug"]:
        subject += f", {event['fahrzeug']}"
    partner = f" bei {event['autohaus']}" if event["autohaus"] else ""
    clock = f"um {event['uhrzeit']} Uhr" if event["uhrzeit"] else "Uhrzeit unbekannt"
    kind = event["art"]
    if kind == "ueberfaellig":
        due = date.fromisoformat(event["datum"]).strftime("%d.%m.%Y")
        return f"{subject}: Fertigtermin {due}, {clock}, noch nicht als fertig markiert."
    if kind == "heute_faellig":
        deadline = f"bis {event['uhrzeit']} Uhr" if event["uhrzeit"] else "Uhrzeit unbekannt"
        return f"{subject}: Fertigstellung heute geplant, {deadline}."
    if kind == "abholung_durch_werkstatt_heute":
        return f"{subject}: heute von euch{partner} abzuholen, {clock}."
    if kind == "anlieferung_heute":
        return f"{subject}: wird heute vom Kunden angeliefert, {clock}."
    if kind == "rueckbringung_heute":
        return f"{subject}: heute von euch zurückzubringen, {clock}."
    if kind == "kundenabholung_heute":
        return f"{subject}: Abholung durch den Kunden heute geplant, {clock}."
    direction = "Annahme" if event["datum_feld"] == "annahme_datum" else "Abholung oder Rückgabe"
    return f"{subject}: {direction} heute geplant, Transportart ungeklärt, {clock}."


def build_briefing(orders, today=None):
    """Return all relevant events and a short German spoken overview.

    today accepts a date, German/ISO date string or an aware datetime. If absent,
    today is determined in Europe/Berlin. Naive datetime arguments are rejected.
    Order dates accept the portal's DD.MM.YYYY and YYYY-MM-DD formats.

    Unfinished (status 1..3) orders contribute due/overdue completion and incoming
    events. Completed (4) orders can still contribute outgoing transport events.
    Returned (5) and archived orders are omitted. Invalid/duplicate internal IDs,
    unknown status/archive flags, invalid dates/times and unknown transport are
    reported in datenhinweise. Missing times stay None. Missing transport is not
    silently treated as customer transport. Separate events are retained even
    when the same order has completion and return planned on the same day.

    Only fertig_uhrzeit is evidence for the completion hour. Descriptions,
    document extracts, permissions, releases and quantities are not interpreted.
    Source links are derived from the stable internal order ID. This overview
    records planning/status data and does not certify work or transport release.
    """
    if today is None:
        target = datetime.now(BERLIN).date()
    else:
        if isinstance(today, datetime) and (today.tzinfo is None or today.utcoffset() is None):
            raise ValueError("today als datetime braucht eine Zeitzone.")
        target = _date(today)
        if target is None:
            raise ValueError("today muss ein gültiges Datum sein.")
    orders = list(orders)
    ids = [_positive_id(row.get("id")) if isinstance(row, Mapping) else None for row in orders]
    duplicates = {key for key, count in Counter(ids).items() if key is not None and count > 1}
    events = []
    warnings = []

    def warn(index, oid, field, reason):
        warnings.append({"index": index, "auftrag_id": oid, "feld": field, "hinweis": reason})

    def event_for(row, oid, kind, day, date_field, time_field):
        return {
            "auftrag_id": oid, "art": kind, "fahrzeug": _text(row.get("fahrzeug")),
            "kennzeichen": _text(row.get("kennzeichen"), 30),
            "autohaus": _text(row.get("autohaus") or row.get("autohaus_name")),
            "datum": day.isoformat(), "uhrzeit": _time(row.get(time_field)),
            "uhrzeit_status": "bekannt" if _time(row.get(time_field)) else "unbekannt",
            "datum_feld": date_field, "uhrzeit_feld": time_field,
            "status": int(row["status"]), "transport_art": _text(row.get("transport_art")),
            "quelle": f"/admin/auftrag/{oid}",
        }

    for index, row in enumerate(orders):
        oid = ids[index]
        if not isinstance(row, Mapping) or oid is None or oid in duplicates:
            warn(index, oid, "id", "Interne Auftrags-ID fehlt, ist ungültig oder mehrfach vorhanden.")
            continue
        archive = row.get("archiviert", 0)
        if archive in (1, "1", True):
            continue
        if archive not in (0, "0", False, None, ""):
            warn(index, oid, "archiviert", "Archivstatus unbekannt; Auftrag nicht eingeordnet.")
            continue
        status = row.get("status")
        if type(status) is bool or str(status) not in ("1", "2", "3", "4", "5"):
            warn(index, oid, "status", "Bearbeitungsstatus unbekannt; Auftrag nicht eingeordnet.")
            continue
        status = int(status)
        if status == 5:
            continue
        unfinished = status < 4
        dates = {}
        for field in ("fertig_datum", "annahme_datum", "abholtermin"):
            value = row.get(field)
            dates[field] = _date(value)
            if value and dates[field] is None:
                warn(index, oid, field, "Termin ist ungültig; Datum im Cockpit prüfen.")
        for field in ("fertig_uhrzeit", "annahme_uhrzeit", "abhol_uhrzeit"):
            if row.get(field) and _time(row[field]) is None:
                warn(index, oid, field, "Uhrzeit ist ungültig und wird als unbekannt behandelt.")
        finish = dates["fertig_datum"]
        if unfinished and finish and finish <= target:
            kind = "heute_faellig" if finish == target else "ueberfaellig"
            events.append(event_for(row, oid, kind, finish, "fertig_datum", "fertig_uhrzeit"))
        transport = row.get("transport_art")
        for field, time_field, enabled, standard, service in (
            ("annahme_datum", "annahme_uhrzeit", unfinished, "anlieferung_heute", "abholung_durch_werkstatt_heute"),
            ("abholtermin", "abhol_uhrzeit", True, "kundenabholung_heute", "rueckbringung_heute"),
        ):
            if not enabled or dates[field] != target:
                continue
            kind = standard if transport == "standard" else service if transport == "hol_und_bring" else "transport_ungeklaert"
            if kind == "transport_ungeklaert":
                warn(index, oid, "transport_art", "Wer das Fahrzeug bringt oder holt, ist nicht eindeutig hinterlegt.")
            events.append(event_for(row, oid, kind, target, field, time_field))

    events.sort(key=lambda item: (CATEGORIES.index(item["art"]), item["datum"],
                                  item["uhrzeit"] or "99:99", item["auftrag_id"], item["datum_feld"]))
    categories = {kind: [event for event in events if event["art"] == kind] for kind in CATEGORIES}
    spoken = " ".join(_spoken(event) for event in events[:SPOKEN_ITEM_LIMIT])
    if not spoken:
        spoken = "Keine heutigen Termine oder offenen überfälligen Fertigstellungen in den auswertbaren Aufträgen."
    remainder = len(events) - SPOKEN_ITEM_LIMIT
    if remainder > 0:
        spoken += f" Dazu kommen {remainder} weitere Termine in der Übersicht."
    if warnings:
        affected = len({warning["index"] for warning in warnings})
        spoken += f" Bei {affected} Aufträgen müssen Planungsdaten geprüft werden."
    return {
        "datum": target.isoformat(), "zeitzone": "Europe/Berlin", "ereignisse": events,
        "kategorien": categories, "anzahl_ereignisse": len(events),
        "anzahl_auftraege": len({event["auftrag_id"] for event in events}),
        "datenhinweise": warnings, "speech_text": "Heute wichtig: " + spoken,
        "hinweis": "Planungs- und Statusdaten aus dem Cockpit. Keine Bestätigung einer Arbeits- oder Transportfreigabe; Dokumente wurden hier nicht ausgewertet.",
    }
