"""Read-only, admin-only printable workshop sheet using the stable internal ID.

Register with ``register_auftrag_ausdruck(portal)`` after the portal is created.
The view deliberately bypasses get_auftrag(): that existing helper can trigger
document analysis and writes. No private customer/contact fields, amounts,
customer status tokens or raw document/OCR texts are selected.
"""

from datetime import datetime
import re
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, make_response, render_template


_FIELDS = (
    "id", "fahrzeug", "kennzeichen", "auftragsnummer", "beschreibung", "status",
    "produktion_schritt", "annahme_datum", "annahme_uhrzeit", "start_datum",
    "fertig_datum", "fertig_uhrzeit", "abholtermin", "abhol_uhrzeit",
    "transport_art", "archiviert", "farbcode", "farbton", "farbton_2",
)
_PRIVATE_LINE = re.compile(r"^\s*(?:Kunden?(?:name|adresse)?|Name|Anschrift|Adresse|E-?Mail|Telefon|Mobil|IBAN|BIC|Bankverbindung)\s*:", re.I)
_EMAIL = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(r"(?<!\w)(?:\+49|0049|0)[\d /()\-]{6,}\d(?!\w)")
_AMOUNT = re.compile(r"(?:\b\d[\d., ]*\s*(?:EUR\b|€|netto\b|brutto\b)|(?:EUR\b|€)\s*\d[\d., ]*|\b(?:Preis|Kosten|Betrag|Netto|Brutto)(?:\s+insgesamt)?\s*:?\s*\d[\d., ]*)", re.I)


def _clean(value):
    return str(value or "").strip()


def _work_text(value):
    """Remove common contact/money additions from otherwise useful free text."""
    lines = []
    for line in _clean(value).splitlines():
        if _PRIVATE_LINE.search(line):
            continue
        line = _EMAIL.sub("[Kontaktangabe ausgeblendet]", line)
        line = _PHONE.sub("[Kontaktangabe ausgeblendet]", line)
        line = _AMOUNT.sub("[Preisangabe ausgeblendet]", line)
        lines.append(line)
    return "\n".join(lines).strip()


def _date(value):
    raw = _clean(value)
    for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, pattern).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return ""


def _time(value):
    raw = _clean(value).lower().replace("uhr", "").replace(".", ":").replace(",", ":").strip()
    match = re.fullmatch(r"(\d{1,2})(?::?(\d{2}))?", raw)
    if not match:
        return ""
    hour, minute = int(match.group(1)), int(match.group(2) or 0)
    return f"{hour:02d}:{minute:02d}" if hour < 24 and minute < 60 else ""


def _event(label, day, time=""):
    parsed_day = _date(day)
    # An orphaned time is not a complete appointment.
    return {"label": label, "datum": parsed_day, "uhrzeit": _time(time) if parsed_day else ""}


def workshop_sheet(portal, order_id):
    """Return the explicit printable field subset, or None; never hydrate/write."""
    db = portal.get_db()
    try:
        row = db.execute("SELECT " + ", ".join(_FIELDS) + " FROM auftraege WHERE id=?", (order_id,)).fetchone()
    finally:
        db.close()
    if row is None:
        return None
    order = dict(row)
    try:
        status_id = int(order.get("status") or 0)
    except (TypeError, ValueError):
        status_id = 0
    status = getattr(portal, "STATUSLISTE", {}).get(status_id, {}).get("label", "Status offen")
    stage = next((label for key, label, _ in getattr(portal, "PRODUKTION_SCHRITTE", ())
                  if key == order.get("produktion_schritt")), "") if status_id == 3 else ""
    transport = _clean(order.get("transport_art"))
    if transport == "hol_und_bring":
        arrival, departure, transport_label = "Abholung durch uns", "Rückbringung", "Hol- und Bringservice"
    elif transport == "standard":
        arrival, departure, transport_label = "Kunde bringt", "Kunde holt", "Kunde bringt und holt"
    else:
        arrival, departure, transport_label = "Annahme", "Rückgabe", "Transportart noch offen"
    return {
        "nummer": int(order["id"]),
        "fahrzeug": _clean(order.get("fahrzeug")) or "Fahrzeug noch offen",
        "kennzeichen": _clean(order.get("kennzeichen")) or "Kennzeichen noch offen",
        "externe_referenz": _clean(order.get("auftragsnummer")),
        "arbeit": _work_text(order.get("beschreibung")),
        "lackdaten": [
            {"label": label, "wert": _clean(order.get(field))}
            for field, label in (("farbcode", "Farbcode"), ("farbton", "Farbton"), ("farbton_2", "Zweiter Farbton"))
            if _clean(order.get(field))
        ],
        "status": status, "produktion": stage,
        "archiviert": bool(order.get("archiviert")),
        "fertig": _event("Geplante Fertigstellung", order.get("fertig_datum"), order.get("fertig_uhrzeit")),
        "termine": [
            _event(arrival, order.get("annahme_datum"), order.get("annahme_uhrzeit")),
            _event("Geplanter Arbeitsbeginn", order.get("start_datum")),
            _event(departure, order.get("abholtermin"), order.get("abhol_uhrzeit")),
        ],
        "transport": transport_label,
        "stand": datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y, %H:%M"),
    }


def register_auftrag_ausdruck(portal):
    """Install GET /admin/auftrag/<id>/werkstattzettel, protected like admin."""
    if "auftrag_ausdruck" in portal.app.blueprints:
        return portal.app.blueprints["auftrag_ausdruck"]
    blueprint = Blueprint("auftrag_ausdruck", __name__)

    @blueprint.get("/admin/auftrag/<int:auftrag_id>/werkstattzettel")
    @portal.admin_required
    def werkstattzettel(auftrag_id):
        sheet = workshop_sheet(portal, auftrag_id)
        if sheet is None:
            abort(404)
        response = make_response(render_template("werkstattzettel.html", zettel=sheet))
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    portal.app.register_blueprint(blueprint)
    return blueprint
