"""Explicit workshop service types; descriptions and OCR never classify orders."""
import json


AUFTRAGSARTEN = {}
for key, label, gruppe, bg, farbe in (
    ("lackierung", "Lackierauftrag", "Karosserie & Lack", "#ede4f3", "#593d72"),
    ("reifenservice", "Reifenservice", "Reifen & Räder", "#dff2ed", "#12685f"),
    ("mechanik", "Mechanik & Wartung", "Mechanik", "#e1edf8", "#24547b"),
    ("bremsen", "Bremsarbeiten", "Mechanik", "#fff0dd", "#804712"),
    ("oelwechsel", "Ölwechsel", "Mechanik", "#fff0dd", "#804712"),
    ("inspektion", "Inspektion & Wartung", "Mechanik", "#e1edf8", "#24547b"),
    ("motor_antrieb", "Motor & Antrieb", "Mechanik", "#e1edf8", "#24547b"),
    ("fahrwerk_lenkung", "Fahrwerk & Lenkung", "Mechanik", "#e1edf8", "#24547b"),
    ("achsvermessung", "Achsvermessung", "Mechanik", "#e1edf8", "#24547b"),
    ("auspuff", "Auspuff & Abgasanlage", "Mechanik", "#e1edf8", "#24547b"),
    ("elektrik_batterie", "Elektrik & Batterie", "Mechanik", "#e1edf8", "#24547b"),
    ("klima", "Klimaservice", "Mechanik", "#e1edf8", "#24547b"),
    ("autoglas", "Autoglas", "Glas", "#dff0f4", "#235968"),
    ("dellen_smart_repair", "Dellen & Smart Repair", "Karosserie & Lack", "#ede4f3", "#593d72"),
    ("fahrzeugpflege", "Fahrzeugaufbereitung", "Pflege", "#e8efdc", "#465e24"),
    ("felgenservice", "Felgenarbeiten", "Karosserie & Lack", "#ede4f3", "#593d72"),
    ("fahrzeugcheck", "Fahrzeugcheck", "Prüfung", "#e1edf8", "#24547b"),
    ("hu_au", "HU/AU (Prüfpartner)", "Prüfung", "#e1edf8", "#24547b"),
    ("sonstiges", "Sonstige Arbeiten", "Werkstatt", "#ececea", "#4b514e"),
):
    AUFTRAGSARTEN[key] = {"label": label, "kategorie": gruppe, "bg": bg, "farbe": farbe}

OFFENE_AUFTRAGSART = {"label": "Werkstattauftrag · Art prüfen", "kategorie": "Werkstatt", "bg": "#ececea", "farbe": "#4b514e"}


def normalize_leistungsart(value):
    return value if isinstance(value, str) and value in AUFTRAGSARTEN else ""


def aufnahme_daten(auftrag):
    data = (auftrag or {}).get("schaden_aufnahme")
    if isinstance(data, dict):
        return data
    try:
        data = json.loads((auftrag or {}).get("schaden_aufnahme_json") or "{}")
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def hat_reifenbuchung(auftrag):
    # The original intake marker is immutable through the service selector.
    return aufnahme_daten(auftrag).get("anliegen") == "reifenservice"


def leistungsart_aus_anfrage(daten):
    if not isinstance(daten, dict):
        return ""
    anliegen = daten.get("anliegen")
    if anliegen == "reifenservice":
        return "reifenservice"
    if anliegen == "mechanikanfrage":
        kategorien = daten.get("mechanik_kategorien", [])
        if not isinstance(kategorien, list):
            return "mechanik"
        keys = {key for key in kategorien if isinstance(key, str) and key in AUFTRAGSARTEN and AUFTRAGSARTEN[key]["kategorie"] == "Mechanik"}
        if "sonstiges" in kategorien or len(keys) != 1:
            return "mechanik"
        return next(iter(keys))
    return {
        "reparaturanfrage": "lackierung", "lackanfrage": "lackierung", "farbton": "lackierung",
        "glasschaden": "autoglas", "fahrzeugpflege": "fahrzeugpflege",
        "felgenservice": "felgenservice", "dellenreparatur": "dellen_smart_repair",
        "leasingrueckgabe": "fahrzeugcheck", "kaufberatung": "fahrzeugcheck", "fahrzeugcheck": "fahrzeugcheck",
    }.get(anliegen, "")


def auftrag_leistungsart(auftrag):
    if hat_reifenbuchung(auftrag):
        return "reifenservice"
    return normalize_leistungsart((auftrag or {}).get("leistungsart")) or leistungsart_aus_anfrage(aufnahme_daten(auftrag))


def verwendet_lackierablauf(auftrag):
    key = auftrag_leistungsart(auftrag)
    # Unclassified historical records retain their existing process/statistics.
    # New classified mechanical, glass and care jobs never use paint stages.
    return not key or key in {"lackierung", "dellen_smart_repair", "felgenservice"}
