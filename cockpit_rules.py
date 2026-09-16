"""Pure rules for explicit prices, document audiences and invoice preflight."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import re
from pathlib import PurePath

AUDIENCE_FIELDS = {"kunde": "kunde_sichtbar", "partner": "partner_sichtbar", "versicherung": "versicherung_sichtbar"}
CUSTOMER_SOURCES = {"kunde", "kunde_portal", "website", "website_formular"}


def workshop_completion_photo(document):
    """Only the workshop board's explicitly designated completion photos."""
    return (document.get("quelle") == "werkstatt"
            and document.get("kategorie") == "fertigbild"
            and str(document.get("mime_type") or "").startswith("image/")
            and PurePath(str(document.get("original_name") or "")).suffix.lower()
            in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", ".heif"})


def document_visible(document, audience, *, lead=False):
    """Legacy internal documents fail closed; own uploads retain their audience."""
    document = dict(document or {})
    if audience not in AUDIENCE_FIELDS:
        return False
    source = str(document.get("quelle") or "").strip()
    if lead:
        value = document.get("kunden_sichtbar")
        return audience == "kunde" and (bool(value) if value is not None else source in CUSTOMER_SOURCES | {"werkstatt_angebot"})
    if document.get("sichtbarkeit_geprueft"):
        return bool(document.get(AUDIENCE_FIELDS[audience]))
    if audience == "kunde":
        return bool(document.get("kunde_sichtbar")) or source in CUSTOMER_SOURCES
    # Older board uploads only recorded the customer flag. Restore their intended
    # partner visibility, but never override an explicit review/revocation above.
    if audience == "partner" and document.get("kunde_sichtbar") and workshop_completion_photo(document):
        return True
    return source == {"partner": "autohaus", "versicherung": "versicherung"}.get(audience)


def decimal_input(value, *, max_value=10000000):
    text = str(value or "").strip().replace(" ", "")
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d{1,2})?", text):
        raise ValueError("Bitte einen positiven Betrag mit höchstens zwei Nachkommastellen eingeben.")
    try:
        amount = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("Ungültiger Betrag.") from exc
    if not amount.is_finite() or not 0 <= amount <= max_value:
        raise ValueError("Der Betrag liegt außerhalb des erlaubten Bereichs.")
    return amount.quantize(Decimal(".01"), rounding=ROUND_HALF_UP)


def price_record(amount, tax_rate, source="", document_id=None):
    net = decimal_input(amount)
    if net is None:
        return None
    rate = decimal_input(tax_rate, max_value=100)
    if rate is None:
        raise ValueError("Bitte den Mehrwertsteuersatz angeben.")
    tax = (net * rate / 100).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
    return {"netto": str(net), "mwst_satz": str(rate), "mwst": str(tax), "brutto": str(net + tax),
            "quelle": str(source or "").strip()[:300], "datei_id": document_id}


def price_state(value):
    try:
        result = json.loads(value or "{}")
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def has_invoice(order):
    return bool(order.get("lexware_invoice_id") or str(order.get("rechnung_nummer") or "").strip()
                or order.get("rechnung_status") in {"geschrieben", "lexware_entwurf"})


def exact_contact(contacts, customer):
    """Never silently substitute another customer or ambiguous namesake."""
    name = str(customer.get("name") or "").strip().casefold()
    matches = []
    for contact in contacts:
        company = str((contact.get("company") or {}).get("name") or "").strip().casefold()
        person = contact.get("person") or {}
        fullname = f"{person.get('firstName') or ''} {person.get('lastName') or ''}".strip().casefold()
        if name and name in {company, fullname}:
            matches.append(contact)
    if len(matches) == 1:
        contact = matches[0]
        addresses = (contact.get("addresses") or {}).get("billing") or []
        expected = {"street": customer.get("strasse"), "zip": customer.get("plz"), "city": customer.get("ort")}
        supplied = {k: v for k, v in expected.items() if str(v or '').strip()}
        if supplied and (not addresses or not any(
            all(str(address.get(k) or "").strip().casefold() == str(v).strip().casefold() for k, v in supplied.items())
            for address in addresses
        )):
            raise ValueError("Der Lexware-Kontakt hat eine abweichende Rechnungsanschrift. Bitte zuerst den Empfänger in Lexware klären.")
        email = str(customer.get('email') or '').strip().casefold()
        emails = {str(e).strip().casefold() for values in (contact.get('emailAddresses') or {}).values() for e in (values or [])}
        if email and emails and email not in emails:
            raise ValueError('Die E-Mail-Adresse weicht vom Lexware-Kontakt ab. Bitte zuerst den Rechnungsempfänger klären.')
        if not all(expected.values()) and not (email and email in emails):
            raise ValueError('Der Name allein reicht für die Zuordnung nicht. Bitte Rechnungsanschrift oder E-Mail im Lexware-Kontakt vervollständigen.')
        return contact
    if contacts:
        raise ValueError("Der Rechnungsempfänger ist in Lexware nicht eindeutig zugeordnet. Bitte den Kontakt prüfen; es wurde kein Entwurf erstellt.")
    return None
