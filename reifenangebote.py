"""Pure validation and presentation for tire quotations, without provider IO."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
import unicodedata
from zoneinfo import ZoneInfo


REIFEN_SAISONS = {"sommer": "Sommerreifen", "winter": "Winterreifen", "ganzjahr": "Ganzjahresreifen"}
REIFEN_QUALITAETEN = {"alle": "Alle Marken", "preisbewusst": "Preisbewusst", "mittelklasse": "Mittelklasse",
                      "markenqualitaet": "Markenqualität", "premium": "Premium"}


def parse_reifenangebot(form, *, has_photo=False):
    """Accept a readable size or a photo; vehicle suitability still needs review."""
    data = {key: str(form.get("reifen_" + key) or "").strip()
            for key in ("saison", "qualitaet", "anzahl", "groesse_vorne", "groesse_hinten", "montage")}
    errors = []
    # This is unverified customer prose, never a selected article or a price.
    # Keep it separate from the customer's free-text message and do not consume
    # any client-side price fields when creating the quotation request.
    produktwunsch = form.get("reifen_produktwunsch", "")
    if not isinstance(produktwunsch, str) or len(produktwunsch) > 400 or any(
            unicodedata.category(char).startswith("C") for char in produktwunsch):
        errors.append("Der Reifen-Produktwunsch darf höchstens 400 Zeichen ohne Steuerzeichen enthalten.")
    elif produktwunsch.strip():
        data["produktwunsch"] = produktwunsch.strip()
    if data["saison"] not in REIFEN_SAISONS:
        errors.append("Bitte Sommer-, Winter- oder Ganzjahresreifen auswählen.")
    if data["qualitaet"] not in REIFEN_QUALITAETEN:
        errors.append("Bitte die gewünschte Reifenqualität auswählen.")
    if not re.fullmatch(r"[1-8]", data["anzahl"]):
        errors.append("Bitte eine Stückzahl von 1 bis 8 Reifen auswählen.")
    else:
        data["anzahl"] = int(data["anzahl"])
    if data["montage"] not in {"ja", "nein"}:
        errors.append("Bitte auswählen, ob Sie eine Montage wünschen.")
    data["montage"] = data["montage"] == "ja"
    for key in ("groesse_vorne", "groesse_hinten"):
        value = data[key].upper()
        # A full size can include load/speed, XL and runflat markings. Other
        # formats can be submitted as a photo instead of guessed by the system.
        if value and (len(value) > 60 or not re.fullmatch(
                r"[0-9]{3}\s*/\s*[0-9]{2,3}\s*(?:Z?R)\s*[0-9]{2}(?:[ .0-9A-Z()/+-]{0,35})", value)):
            errors.append("Bitte eine Reifengröße wie 205/55 R16 91V eintragen oder ein Foto hochladen.")
        data[key] = value
    if not data["groesse_vorne"] and not data["groesse_hinten"] and not has_photo:
        errors.append("Bitte die Reifengröße angeben oder ein Foto der Reifenbeschriftung bzw. des Fahrzeugscheins hochladen.")
    return data, list(dict.fromkeys(errors))


def reifenangebot_summary(data):
    return " · ".join((REIFEN_SAISONS.get(data.get("saison"), "Reifen"),
        REIFEN_QUALITAETEN.get(data.get("qualitaet"), ""), f"{data.get('anzahl', '')} Stück",
        "Größe: " + (data.get("groesse_vorne") or "nach Foto prüfen"),
        *(('Hinterachse: ' + data['groesse_hinten'],) if data.get('groesse_hinten') else ()),
        "mit Montage" if data.get("montage") else "ohne Montage",
        *(("Kundenwunsch (ungeprüft): " + data["produktwunsch"],) if data.get("produktwunsch") else ())))


def reifenangebot_deadline(now=None):
    now = now or datetime.now(timezone.utc)
    return (now.astimezone(timezone.utc) + timedelta(hours=24)).isoformat(timespec="seconds")


def reifenangebot_frist(value, *, erledigt=False, now=None):
    try:
        deadline = datetime.fromisoformat(value)
        if deadline.tzinfo is None:
            raise ValueError("timezone missing")
    except (ValueError, TypeError):
        return {"reifenangebot_frist_label": "", "reifenangebot_ueberfaellig": False,
                "reifenangebot_bald_faellig": False, "reifenangebot_frist_sort": float("inf")}
    remaining = (deadline - (now or datetime.now(timezone.utc))).total_seconds()
    return {"reifenangebot_frist_label": deadline.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y · %H:%M Uhr"),
            "reifenangebot_ueberfaellig": not erledigt and remaining <= 0,
            "reifenangebot_bald_faellig": not erledigt and 0 < remaining <= 4 * 3600,
            "reifenangebot_frist_sort": float("inf") if erledigt else deadline.timestamp()}


def reifenangebot_mail(lead):
    """Only the workshop-reviewed offer text and price become a quotation."""
    return (f"Guten Tag {lead.get('kunde_name', '')},\n\n"
            "hier ist Ihr persönliches Reifenangebot:\n\n"
            f"{lead.get('angebot_text', '')}\n\n"
            f"Gesamtpreis: {lead.get('angebot_preis', '')}\n"
            + (f"\nHinweise: {lead['angebot_notiz']}\n" if lead.get('angebot_notiz') else "")
            + "\nEin Montagetermin ist damit noch nicht reserviert. Bei Rückfragen oder zur Annahme antworten Sie bitte auf diese E-Mail.\n"
            + (f"Ihr Angebot und Ihre Unterlagen: {lead['kunden_status_url']}\n" if lead.get('kunden_status_url') else ""))


def reifenangebot_preis(value):
    """A single positive gross EUR total, never an unreviewed free-text amount."""
    text = str(value or "").strip()
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]{3})*(?:,[0-9]{1,2})?|[0-9]+\.[0-9]{1,2})\s*(?:€|EUR)?\s*(?:brutto)?", text, re.I)
    if not match:
        raise ValueError("Bitte einen eindeutigen Gesamtpreis in Euro brutto eintragen, z. B. 450,00 € brutto.")
    number = match[1]
    if "," in number or re.fullmatch(r"[0-9]+(?:\.[0-9]{3})+", number):
        number = number.replace(".", "").replace(",", ".")
    try:
        amount = Decimal(number)
        if not Decimal("0") < amount <= Decimal("100000"):
            raise ValueError("Bitte einen positiven, geprüften Gesamtpreis bis 100.000 € brutto eintragen.")
    except InvalidOperation as exc:
        raise ValueError("Bitte einen gültigen Gesamtpreis eintragen.") from exc
    return f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " € brutto"
