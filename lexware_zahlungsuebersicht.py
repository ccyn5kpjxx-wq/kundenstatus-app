"""Validated, source-backed Lexware EÜR month snapshots.

The public payments API covers voucher-linked payments only. An EÜR export is
an independent authoritative source for Lexware's dashboard by payment date.
No totals are inferred from invoice dates or hard-coded for a customer.
"""

import calendar
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from io import BytesIO
import json
import re
from zoneinfo import ZoneInfo


SETTING_KEY = "LEXWARE_EUER_MONATSSTAENDE_V1"
MONATE = ("", "Januar", "Februar", "März", "April", "Mai", "Juni", "Juli",
          "August", "September", "Oktober", "November", "Dezember")


def _identitaet(value):
    value = value.casefold().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    value = value.replace("&", "und").replace("+", "und")
    return re.sub(r"[^a-z0-9]", "", value)


def _berichtszeilen(text):
    """Accept the readable print report or an unmodified accessible DOM export."""
    result = []
    for line in text.replace("\u00ad", "").splitlines():
        line = line.strip()
        if re.match(r"- /url:", line):
            continue
        quoted = re.match(r'- (?:heading|link) "(.*?)"(?: \[level=\d+\])?:?$', line)
        if quoted:
            line = quoted.group(1)
        else:
            scalar = re.match(r"^- (?:generic|text):\s*(.*)$", line)
            if scalar:
                line = scalar.group(1)
                # Accessible DOM exports quote some scalars, including negative
                # amounts. Decode that serialization without changing the source.
                if line.startswith('"'):
                    try:
                        decoded = json.loads(line)
                    except json.JSONDecodeError:
                        pass
                    else:
                        if isinstance(decoded, str):
                            line = decoded
        result.append(line.strip())
    return result


def _berichtsbetrag(text, label):
    number = r"-?(?:\d{1,3}(?:\.\d{3})*|\d+),\d{2}"
    matches = re.findall(re.escape(label) + r"\s*:?\s*(" + number + r")(?!\d)", text)
    values = {int(Decimal(value.replace(".", "").replace(",", ".")) * 100) for value in matches}
    if len(values) != 1:
        raise ValueError(f"{label}: kein eindeutiger Betrag im Lexware-Bericht.")
    return values.pop()


def monatsstand_aus_bericht(original, filename, expected_company, *, now=None):
    """Parse totals and dates from source labels; never accept entered totals."""
    if not isinstance(original, bytes) or not original or len(original) > 12 * 1024 * 1024:
        raise ValueError("Der Lexware-Bericht fehlt oder ist größer als 12 MB.")
    suffix = filename.rsplit(".", 1)[-1].lower() if isinstance(filename, str) else ""
    if suffix == "pdf":
        if not original.startswith(b"%PDF-"):
            raise ValueError("Die Datei ist kein PDF-Bericht.")
        try:
            from pypdf import PdfReader
            reader = PdfReader(BytesIO(original), strict=True)
            if reader.is_encrypted or not 1 <= len(reader.pages) <= 50:
                raise ValueError("Der PDF-Bericht ist verschlüsselt oder zu umfangreich.")
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            raise ValueError("Der PDF-Bericht konnte nicht zuverlässig als Text gelesen werden.") from exc
    elif suffix == "txt":
        try:
            text = original.decode("utf-8-sig", errors="strict")
        except UnicodeError as exc:
            raise ValueError("Der Textbericht muss UTF-8 enthalten.") from exc
    else:
        raise ValueError("Bitte den Lexware-Druckbericht als PDF oder unveränderten Textbericht wählen.")
    lines = _berichtszeilen(text)
    text = "\n".join(lines)
    headings = re.findall(r"Einnahmen[-–]Überschussrechnung\s+für\s+den\s+Monat\s+(\w+)\s+(20\d{2})", text)
    if not headings or len(set(headings)) != 1:
        raise ValueError("Kein eindeutiger monatlicher Lexware-EÜR-Bericht erkannt.")
    month_name, year_text = headings[0]
    if month_name not in MONATE:
        raise ValueError("Der Auswertungsmonat konnte nicht gelesen werden.")
    year, month = int(year_text), MONATE.index(month_name)
    if "Berechnung nach Zahldatum" not in text or "Alle Angaben in Euro" not in text:
        raise ValueError("Der Bericht muss nach Zahldatum und in Euro berechnet sein.")
    if re.search(r"Berechnung nach (?:Beleg|Leistungs)datum", text):
        raise ValueError("Der Bericht verwendet nicht ausschließlich das Zahldatum.")
    heading_index = next((i for i, line in enumerate(lines) if "Einnahmen-Überschussrechnung" in line), len(lines))
    header = lines[:heading_index]
    company = _identitaet(expected_company)
    if not company or not any(_identitaet(line.split(",", 1)[0]) == company for line in header):
        raise ValueError("Der Bericht gehört nicht zum hinterlegten Betrieb.")
    dates = re.findall(r"(?<!\d)(\d{2}\.\d{2}\.20\d{2}\s+\d{2}:\d{2})(?!\d)", "\n".join(header))
    if len(set(dates)) != 1:
        raise ValueError("Kein eindeutiger Erstellungszeitpunkt im Berichtskopf erkannt.")
    observed = datetime.strptime(dates[0], "%d.%m.%Y %H:%M").replace(tzinfo=ZoneInfo("Europe/Berlin"))
    now = now or datetime.now(ZoneInfo("Europe/Berlin"))
    if now.tzinfo is None or observed > now + timedelta(minutes=5):
        raise ValueError("Der Erstellungszeitpunkt liegt in der Zukunft.")
    first = date(year, month, 1)
    if observed.date() < first:
        raise ValueError("Der Auswertungsmonat liegt nach dem Erstellungsdatum.")
    cutoff = min(observed.date(), date(year, month, calendar.monthrange(year, month)[1]))
    income = _berichtsbetrag(text, "Betriebseinnahmen gesamt")
    expense = _berichtsbetrag(text, "Betriebsausgaben gesamt")
    difference = _berichtsbetrag(text, "Differenz")
    if income - expense != difference:
        raise ValueError("Die Summen und die Differenz im Bericht stimmen nicht überein.")
    return validiere_monatsstand({
        "basis": "lexware_euer_zahldatum", "currency": "EUR", "jahr": year, "monat": month,
        "stichtag": cutoff.isoformat(), "quellenzeitpunkt": observed.isoformat(),
        "quelldatei": filename, "quelle_sha256": hashlib.sha256(original).hexdigest(),
        "einnahmen_cent": income, "ausgaben_cent": expense,
    }, original)


def _cent(value):
    if isinstance(value, bool) or not isinstance(value, int) or abs(value) > 10**15:
        raise ValueError("Lexware-Beträge müssen ganze Centbeträge im zulässigen Bereich sein.")
    return value


def _timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Der Quellenzeitpunkt fehlt.")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Der Quellenzeitpunkt benötigt eine Zeitzone.")
    return parsed


def validiere_monatsstand(payload, original=None):
    """Validate a parsed export and, on import, bind it to its original bytes.

    The caller must extract these values from an authentic Lexware EÜR export.
    This validator deliberately does not pretend to parse arbitrary PDFs/CSVs.
    """
    if not isinstance(payload, dict):
        raise ValueError("Ungültiger Lexware-Monatsstand.")
    if payload.get("basis") != "lexware_euer_zahldatum" or payload.get("currency") != "EUR":
        raise ValueError("Erforderlich ist eine Lexware-EÜR in EUR nach Zahldatum.")
    year, month = payload.get("jahr"), payload.get("monat")
    if (isinstance(year, bool) or isinstance(month, bool) or not isinstance(year, int)
        or not isinstance(month, int) or not 2000 <= year <= 2100 or not 1 <= month <= 12):
        raise ValueError("Ungültiger Auswertungsmonat.")
    cutoff = date.fromisoformat(payload.get("stichtag", ""))
    if (cutoff.year, cutoff.month) != (year, month):
        raise ValueError("Der Stichtag gehört nicht zum Auswertungsmonat.")
    observed = _timestamp(payload.get("quellenzeitpunkt"))
    if observed.date() < cutoff:
        raise ValueError("Der Quellenzeitpunkt liegt vor dem Stichtag.")
    filename = payload.get("quelldatei")
    if not isinstance(filename, str) or not filename.strip() or len(filename) > 255:
        raise ValueError("Der Name der Originaldatei fehlt.")
    digest = payload.get("quelle_sha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("Die Prüfsumme der Originaldatei fehlt.")
    if original is not None:
        if not isinstance(original, bytes) or not original or hashlib.sha256(original).hexdigest() != digest:
            raise ValueError("Originaldatei und Prüfsumme stimmen nicht überein.")
    return {
        "basis": "lexware_euer_zahldatum", "currency": "EUR",
        "jahr": year, "monat": month, "stichtag": cutoff.isoformat(),
        "quellenzeitpunkt": observed.isoformat(), "quelldatei": filename.strip(),
        "quelle_sha256": digest,
        "einnahmen_cent": _cent(payload.get("einnahmen_cent")),
        "ausgaben_cent": _cent(payload.get("ausgaben_cent")),
    }


def monatsstaende_lesen(payload):
    """A corrupt or mixed legacy record must never replace valid API data."""
    if not isinstance(payload, dict) or payload.get("version") != 1:
        return {}
    valid = {}
    for record in payload.get("monate", []) if isinstance(payload.get("monate"), list) else []:
        try:
            record = validiere_monatsstand(record)
        except (ValueError, TypeError, OverflowError):
            continue
        key = f'{record["jahr"]:04d}-{record["monat"]:02d}'
        if (key not in valid
            or _timestamp(record["quellenzeitpunkt"]) > _timestamp(valid[key]["quellenzeitpunkt"])):
            valid[key] = record
    return valid


def monatsstand_ersetzen(payload, neuer_stand, original):
    """Return updated settings without changing earlier months or source files."""
    record = validiere_monatsstand(neuer_stand, original)
    records = monatsstaende_lesen(payload)
    key = f'{record["jahr"]:04d}-{record["monat"]:02d}'
    if key in records and _timestamp(record["quellenzeitpunkt"]) < _timestamp(records[key]["quellenzeitpunkt"]):
        raise ValueError("Ein älterer Export darf einen neueren Lexware-Stand nicht ersetzen.")
    records[key] = record
    return {"version": 1, "monate": [records[key] for key in sorted(records)]}


def _euro(cent):
    value = f"{Decimal(cent) / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{value} €"


def _uebersicht_mit_staenden(uebersicht, records, active, api_unvollstaendig):
    months = {(row["jahr"], row["monat"]): {**row, "quellen_label": "Belegzahlungen",
              "hat_daten": True, "unvollstaendig": api_unvollstaendig}
              for row in uebersicht}
    for snapshot in records.values():
        ym = (snapshot["jahr"], snapshot["monat"])
        existing = months.get(ym, {})
        income, expense = snapshot["einnahmen_cent"], snapshot["ausgaben_cent"]
        months[ym] = {**existing, "jahr": ym[0], "monat": ym[1],
            "label": f"{MONATE[ym[1]]} {ym[0]}",
            "einnahmen_label": _euro(income), "ausgaben_label": _euro(expense),
            "differenz_label": _euro(income - expense), "differenz_positiv": income >= expense,
            "investitionen_label": "", "ist_aktiv": ym == active,
            "vormonat_geschlagen": False,
            "quellen_label": "Lexware EÜR · Stand " + date.fromisoformat(snapshot["stichtag"]).strftime("%d.%m.%Y"),
            "hat_daten": True, "unvollstaendig": False,
        }
    return [months[ym] for ym in sorted(months, reverse=True)]


def monatsstand_anwenden(zahlen, records):
    """Prefer an authentic EÜR snapshot, keeping API totals clearly separate."""
    result = dict(zahlen)
    result["lexware_euer_snapshot"] = False
    result["datenbasis_label"] = "Belegzugeordnete Lexware-Zahlungen"
    result["uebersicht"] = _uebersicht_mit_staenden(result.get("uebersicht", []), records,
        (zahlen["jahr"], zahlen["monat"]), bool(zahlen.get("zahlungsabgleich_unvollstaendig")))
    key = f'{zahlen["jahr"]:04d}-{zahlen["monat"]:02d}'
    record = records.get(key)
    if not record:
        return result
    received, paid = record["einnahmen_cent"], record["ausgaben_cent"]
    cutoff = date.fromisoformat(record["stichtag"])
    observed = _timestamp(record["quellenzeitpunkt"])
    result.update({
        "lexware_euer_snapshot": True,
        "datenbasis_label": "Lexware EÜR · Nach Zahldatum",
        "snapshot_stichtag": cutoff.strftime("%d.%m.%Y"),
        "snapshot_quellenzeitpunkt": observed.strftime("%d.%m.%Y %H:%M %Z"),
        "snapshot_quelldatei": record["quelldatei"],
        "snapshot_sha256": record["quelle_sha256"],
        "snapshot_veraltet": bool(zahlen.get("ist_aktueller_monat")
            and cutoff < datetime.strptime(zahlen["heute_label"], "%d.%m.%Y").date()),
        "as_of": cutoff.day, "hat_daten": True,
        "last_sync": observed.strftime("%d.%m.%Y %H:%M"),
        "zahlungsabgleich_unvollstaendig": False, "zahlungsdaten_luecken": 0,
        "einnahmen_bisher_label": _euro(received), "ausgaben_bisher_label": _euro(paid),
        "einnahmen_monat_total_label": _euro(received), "ausgaben_monat_total_label": _euro(paid),
        "differenz_bisher_label": _euro(received - paid), "differenz_positiv": received >= paid,
        # A monthly export does not provide a matching daily prior-month curve.
        "ziel": 0, "ziel_gesetzt": False, "ziel_label": "—", "ampel": "neutral",
        "ziel_erklaerung": "Monatsziel und Tagesvergleich werden nur mit gleicher Auswertungsbasis und passendem Vergleichszeitraum angezeigt.",
        "hochrechnung_label": "—", "tendenz_pct": None,
        "hat_investitionen": False, "top_ausgaben": [], "alle_ausgaben": [],
    })
    previous_day = cutoff.replace(day=1) - timedelta(days=1)
    previous = records.get(previous_day.strftime("%Y-%m"))
    if previous and previous["stichtag"] == previous_day.isoformat() and previous["einnahmen_cent"] > 0:
        goal_cent = ((previous["einnahmen_cent"] + 99_999) // 100_000) * 100_000
        result.update({
            "ziel": goal_cent / 100, "ziel_label": _euro(goal_cent), "ziel_gesetzt": True,
            "vormonat_einnahmen_total_label": _euro(previous["einnahmen_cent"]),
            "ziel_erklaerung": f"Vollständiger Lexware-EÜR-Vormonat: {_euro(previous['einnahmen_cent'])}, auf den nächsten Tausender aufgerundet. Ohne Tagestrend-Aufschlag.",
            "fortschritt_pct": round(Decimal(received) / goal_cent * 100),
            "fortschritt_capped": max(0, min(round(Decimal(received) / goal_cent * 100), 100)),
        })
    if (result.get("ist_aktueller_monat") and not result["snapshot_veraltet"]
        and observed.date() == cutoff and cutoff.day > 0):
        projection = int((Decimal(received) * calendar.monthrange(cutoff.year, cutoff.month)[1]
                          / cutoff.day).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        result["hochrechnung_label"] = _euro(projection)
    else:
        result["hochrechnung_label"] = "—"
    return result
