"""Pure, conservative extraction of CHECK24 booking mail for human review.

No network, database, attachment execution or trust decision happens here.  In
particular, Authentication-Results supplied in an uploaded EML are only evidence,
not proof of the sender.  Even a structurally valid result requires review.
"""
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
import hashlib
from html.parser import HTMLParser
import re
from urllib.parse import unquote
from zoneinfo import ZoneInfo


MAX_MAIL_BYTES = 5 * 1024 * 1024
BERLIN = ZoneInfo("Europe/Berlin")
MONTHS = {name: index for index, name in enumerate((
    "januar", "februar", "märz", "april", "mai", "juni", "juli", "august",
    "september", "oktober", "november", "dezember"), 1)}
WEEKDAYS = {"mo": 0, "di": 1, "mi": 2, "do": 3, "fr": 4, "sa": 5, "so": 6}
DATE_PATTERN = (
    r"(?:(?P<weekday>Mo|Di|Mi|Do|Fr|Sa|So)\.\s*)?"
    r"(?P<day>\d{1,2})\.\s*(?P<month>" + "|".join(MONTHS) + r")"
    r"(?:\s+(?P<year>20\d{2}))?"
)
APPOINTMENT = re.compile(DATE_PATTERN +
    r"\s+um\s+(?P<start>\d{1,2}:\d{2})\s*[-–—]\s*"
    r"(?P<end>\d{1,2}:\d{2})\s*Uhr\b", re.I)
SUBJECT = re.compile(
    r"Neue CHECK24 Terminbuchung\s*\(Bestellnummer:\s*([A-Z0-9]{4,20})\)", re.I)
EMAIL = re.compile(r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+", re.I)


def _clean(value):
    return re.sub(r"[\t\r \xa0]+", " ", str(value or "")).strip()


class _MailText(HTMLParser):
    """Read visible text and contact links; never load linked content."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self.hidden += 1
        if self.hidden:
            return
        if tag in ("br", "p", "div", "tr", "td", "th", "li", "h1", "h2", "h3"):
            self.parts.append("\n")
        if tag == "a":
            href = dict(attrs).get("href", "")
            if href.lower().startswith(("mailto:", "tel:")):
                value = unquote(href.split(":", 1)[1].split("?", 1)[0])
                # Make contact values available in their original section too.
                if not any(c in value for c in "\r\n\x00"):
                    self.parts.append("\n" + value + "\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self.hidden = max(0, self.hidden - 1)
        if not self.hidden and tag in ("p", "div", "tr", "td", "th", "li", "h1", "h2", "h3"):
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _body(message, preference=("html", "plain")):
    # The observed plain-text alternative omits the delivery-date panel.
    # Parse HTML as inert text; no URL, script, image or attachment is executed.
    part = message.get_body(preferencelist=preference)
    if not part or part.get_content_maintype() != "text":
        return ""
    content = part.get_content()
    if part.get_content_subtype() == "html":
        parser = _MailText()
        parser.feed(content)
        content = "".join(parser.parts)
    return "\n".join(_clean(line) for line in content.splitlines() if _clean(line))


def _section(text, heading, end):
    match = re.search(r"(?im)^" + heading + r"\s*:?\s*$", text)
    if not match:
        return ""
    tail = text[match.end():]
    stop = re.search(r"(?im)^\s*(?:" + end + r")\b", tail)
    return tail[:stop.start()].strip() if stop else tail.strip()


def _date_from_match(match, anchor, warnings):
    day = int(match["day"])
    month = MONTHS[match["month"].lower()]
    year = match["year"]
    if year:
        result = date(int(year), month, day)
    else:
        if anchor is None:
            raise ValueError("Termin ohne Jahr und ohne eindeutiges Maildatum.")
        candidates = []
        for candidate_year in (anchor.year, anchor.year + 1):
            try:
                candidate = date(candidate_year, month, day)
            except ValueError:
                continue
            if 0 <= (candidate - anchor).days <= 180:
                candidates.append(candidate)
        if len(candidates) != 1:
            raise ValueError("Jahr des Termins ist nicht eindeutig aus dem Maildatum ableitbar.")
        result = candidates[0]
        warning = "Jahr aus dem Maildatum abgeleitet; bei der Übernahme prüfen."
        if warning not in warnings:
            warnings.append(warning)
    if match["weekday"] and result.weekday() != WEEKDAYS[match["weekday"].lower()]:
        raise ValueError("Wochentag und Datum widersprechen sich.")
    return result


def _local_datetime(day, clock):
    naive = datetime.combine(day, time.fromisoformat(clock.zfill(5)))
    candidates = []
    for fold in (0, 1):
        aware = naive.replace(tzinfo=BERLIN, fold=fold)
        if aware.astimezone(timezone.utc).astimezone(BERLIN).replace(tzinfo=None) == naive:
            if not any(item.utcoffset() == aware.utcoffset() for item in candidates):
                candidates.append(aware)
    if len(candidates) != 1:
        raise ValueError("Termin liegt in einer ungültigen oder mehrdeutigen Zeitumstellung.")
    return candidates[0]


def _one(values, label, errors):
    values = list(dict.fromkeys(value for value in values if value))
    if len(values) > 1:
        errors.append(label + " ist nicht eindeutig.")
        return ""
    return values[0] if values else ""


def parse_check24_mail(raw_bytes):
    """Return JSON-safe extracted evidence; ``valid`` never means trusted.

    start_at/end_at are ISO8601 strings with Europe/Berlin's date-specific UTC
    offset, or empty strings.  total_gross is an exact decimal string.  Invalid
    input produces errors and cannot be applied; oversize/non-byte input raises
    ValueError before MIME parsing.  Only the observed new-booking template is
    supported; changes, cancellations and other templates remain unknown/review.
    """
    if not isinstance(raw_bytes, (bytes, bytearray)) or not raw_bytes:
        raise ValueError("Bitte eine vollständige EML-Datei auswählen.")
    raw_bytes = bytes(raw_bytes)
    if len(raw_bytes) > MAX_MAIL_BYTES:
        raise ValueError("Die EML-Datei darf höchstens 5 MB groß sein.")
    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
    errors = []
    warnings = ["Absender und Authentifizierungsheader sind nicht verifiziert. Originalmail vor Übernahme prüfen."]
    result = {
        "provider": "check24", "provider_id": "", "kind": "unknown",
        "subject": _clean(message.get("Subject")), "sender": "",
        "message_id": _clean(message.get("Message-ID")),
        "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "start_at": "", "end_at": "", "delivery_date": "",
        "customer_name": "", "customer_phone": "", "customer_email": "",
        "service": "", "tires": [], "total_gross": "",
        "valid": False, "requires_review": True, "errors": errors, "warnings": warnings,
        "authentication": {
            "trusted": False,
            "authentication_results": [str(v) for v in message.get_all("Authentication-Results", [])],
            "dkim_signatures": [str(v) for v in message.get_all("DKIM-Signature", [])],
            "received": [str(v) for v in message.get_all("Received", [])],
        },
    }
    for header in ("From", "Subject", "Date", "Message-ID"):
        if len(message.get_all(header, [])) > 1:
            errors.append("Mehrfacher " + header + "-Header.")
    senders = getaddresses([str(value) for value in message.get_all("From", [])])
    if len(senders) == 1:
        result["sender"] = senders[0][1].lower()
    if result["sender"] != "montagepartner@check24.de":
        errors.append("Die Mail stammt nicht vom erwarteten CHECK24-Absender.")
    if not result["message_id"]:
        warnings.append("Message-ID fehlt; Dubletten zusätzlich über Originaldatei und Bestellnummer prüfen.")
    try:
        text = _body(message)
        plain_alternative = _body(message, ("plain",))
    except (ValueError, LookupError, UnicodeError, KeyError):
        text = ""
        plain_alternative = ""
        errors.append("Mailinhalt kann nicht eindeutig gelesen werden.")
    if any(part.defects for part in message.walk()):
        errors.append("Die MIME-Struktur der EML-Datei ist fehlerhaft.")

    subject_match = SUBJECT.fullmatch(result["subject"])
    subject_id = re.search(r"\(Bestellnummer:\s*([A-Z0-9]{4,20})\)$", result["subject"], re.I)
    if subject_id:
        result["provider_id"] = subject_id[1].upper()
    body_new = re.search(r"unser Kunde hat einen Montagetermin bei Ihnen gebucht\.", text, re.I)
    # Only new bookings are supported.  Do not let an old quoted new-booking
    # paragraph turn a cancellation or reschedule into a second appointment.
    changed = re.search(
        r"\b(?:storn\w*|abgesagt\w*|absag\w*|verschob\w*|verschieb\w*|"
        r"geändert\w*|änderung\w*|terminänder\w*|umbuch\w*|cancel\w*)\b",
        text + "\n" + plain_alternative, re.I)
    if subject_match and body_new and not changed:
        result["kind"] = "new"
    else:
        errors.append("Unbekannte Buchungsvorlage oder Terminänderung/Storno: manuell prüfen.")
    ids = [value.upper() for value in re.findall(r"Bestellnummer\s*:?\s*([A-Z0-9]{4,20})\b", text, re.I)]
    if ids and set(ids) != {result["provider_id"]}:
        errors.append("Bestellnummer im Betreff und Mailinhalt widerspricht sich.")
    for heading in ("Kunde", "Gebuchter Termin", "Gebuchte Leistungen"):
        if len(re.findall(r"(?im)^" + heading + r"[ \t]*:?[ \t]*$", text)) != 1:
            errors.append("Abschnitt '" + heading + "' fehlt oder ist mehrfach vorhanden.")

    anchor = None
    try:
        mail_date = parsedate_to_datetime(str(message.get("Date", "")))
        if mail_date.tzinfo is not None:
            anchor = mail_date.astimezone(BERLIN).date()
    except (ValueError, TypeError, OverflowError):
        pass
    appointment_text = _section(text, r"Gebuchter Termin", r"Gebuchte Leistungen|Bestellnummer")
    matches = list(APPOINTMENT.finditer(appointment_text))
    if len(matches) != 1 or len(list(APPOINTMENT.finditer(text))) != 1:
        errors.append("Gebuchtes Datum und Zeitintervall fehlen oder sind nicht eindeutig.")
    else:
        try:
            day = _date_from_match(matches[0], anchor, warnings)
            start = _local_datetime(day, matches[0]["start"])
            end = _local_datetime(day, matches[0]["end"])
            if end <= start or end - start > timedelta(hours=12):
                raise ValueError("Ungültiges Zeitintervall; Ende muss am selben Tag nach dem Beginn liegen.")
            result["start_at"], result["end_at"] = start.isoformat(), end.isoformat()
        except ValueError as exc:
            errors.append(str(exc))
    delivery_matches = list(re.finditer(r"Voraussichtliche Zustellung\s*:?\s*" + DATE_PATTERN, text, re.I))
    if len(delivery_matches) > 1:
        warnings.append("Mehrere Lieferdaten: Lieferung manuell prüfen.")
    elif delivery_matches:
        try:
            delivery = _date_from_match(delivery_matches[0], anchor, warnings)
            result["delivery_date"] = delivery.isoformat()
            if result["start_at"] and delivery > date.fromisoformat(result["start_at"][:10]):
                errors.append("Voraussichtliche Reifenlieferung liegt nach dem Montagetermin.")
        except ValueError as exc:
            errors.append("Lieferdatum: " + str(exc))

    customer = _section(text, r"Kunde", r"Gebuchter Termin|Gebuchte Leistungen|Bestellnummer")
    lines = customer.splitlines()
    names = [line for line in lines if not EMAIL.search(line) and
             not re.search(r"\d", line) and not re.fullmatch(r"[-_=* ]+", line) and
             line.lower().rstrip(":") not in (
                 "telefon", "e-mail", "anrufen", "e-mail schreiben", "e-mail an kunden senden")]
    result["customer_name"] = _one(names, "Kundenname", errors)
    phones = [re.sub(r"[ ()/.-]", "", value) for value in re.findall(r"(?<!\w)(?:\+|00|0)[\d ()/.-]{6,}\d", customer)]
    result["customer_phone"] = _one(phones, "Kundentelefon", errors)
    result["customer_email"] = _one(EMAIL.findall(customer), "Kunden-E-Mail", errors)
    if not result["customer_name"]:
        errors.append("Kundenname fehlt oder ist nicht eindeutig.")
    if not (result["customer_phone"] or result["customer_email"]):
        errors.append("Telefonnummer oder E-Mail des Kunden fehlt.")
    services = re.findall(r"(?im)^Reifenmontage\s+für\s+\d{1,2}\s+Reifen\s*$", text)
    result["service"] = _one(services, "Gebuchte Leistung", errors)
    if not result["service"]:
        errors.append("Gebuchte Reifenmontage konnte nicht eindeutig erkannt werden.")
    result["tires"] = list(dict.fromkeys(re.findall(r"(?im)^\d{1,2}\s*[x×]\s*[^\n]+", text)))
    gross = re.findall(r"(?im)^Gesamtpreis\s*:?\s*(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})\s*€", text)
    amount = _one(gross, "Gesamtpreis", errors)
    if amount:
        result["total_gross"] = str(Decimal(amount.replace(".", "").replace(",", ".")))
    else:
        warnings.append("Gesamtpreis nicht eindeutig erkannt; keine Preisübernahme.")
    result["valid"] = not errors and result["kind"] == "new"
    return result
