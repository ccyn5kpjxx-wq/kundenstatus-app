"""Read-only tire search over a server-injected, explicitly confirmed provider.

There is deliberately no vendor URL, HTTP client, credential, default product or
default purchase price here. The production provider is None until a real feed
is authorized and wired separately. Provider dictionaries are never serialized;
customer responses are built field by field from a selling-price whitelist.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re

from reifenangebote import REIFEN_SAISONS, REIFEN_QUALITAETEN
from reifenpreis_kalkulation import berechne_reifenpreis
from reifenservice import berechne_reifenservice


MAX_PREISALTER = timedelta(minutes=15)
MAX_PRODUKTE = 12
_FELDER = {"groesse", "saison", "qualitaet", "anzahl", "montage"}
# Nominal km/h, not alphabetic order. Conservative search: no automatic winter
# exceptions or special >300 km/h approvals. Source: Michelin's published table:
# https://www.michelin.de/auto/tipps/grundlagen-reifen/lastindex-geschwindigkeitsindex
_TEMPO = {"L": 120, "M": 130, "N": 140, "Q": 160, "R": 170, "S": 180,
          "T": 190, "U": 200, "H": 210, "V": 240, "W": 270, "Y": 300}


class ReifenSucheEingabefehler(ValueError):
    pass


def _text(value, *, maximum=200):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError("Textfeld fehlt oder ist zu lang.")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Ungültiges Textfeld.")
    return value.strip()


def _integer(value, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError("Ungültiges Zahlenfeld.")
    return value


def parse_reifen_suche(args):
    if not isinstance(args, Mapping) or set(args) - _FELDER:
        raise ReifenSucheEingabefehler("Bitte nur Reifengröße, Saison, Qualität, Stückzahl und Montage auswählen.")
    if hasattr(args, "getlist") and any(len(args.getlist(key)) != 1 for key in args):
        raise ReifenSucheEingabefehler("Jedes Suchfeld darf nur einmal vorkommen.")
    try:
        size = _text(args.get("groesse"), maximum=40).upper()
        match = re.fullmatch(r"([0-9]{3})\s*/\s*([0-9]{2})\s*R\s*([0-9]{2})(?:\s*([0-9]{2,3})?\s*([A-Z])?)?", size)
        if not match:
            raise ValueError("size")
        width, ratio, rim = (int(match[i]) for i in (1, 2, 3))
        _integer(width, 100, 500)
        _integer(ratio, 10, 95)
        _integer(rim, 10, 30)
        load = int(match[4]) if match[4] else None
        speed = match[5] or None
        if load is not None:
            _integer(load, 60, 150)
        if speed is not None and speed not in _TEMPO:
            raise ValueError("speed")
        season, quality = args.get("saison"), args.get("qualitaet")
        if season not in REIFEN_SAISONS or quality not in REIFEN_QUALITAETEN:
            raise ValueError("selection")
        quantity = args.get("anzahl")
        if not isinstance(quantity, str) or not re.fullmatch(r"[1-8]", quantity):
            raise ValueError("quantity")
        mounting = args.get("montage")
        if mounting not in {"ja", "nein"}:
            raise ValueError("mounting")
        return {"breite": width, "querschnitt": ratio, "zoll": rim,
                "lastindex": load, "geschwindigkeitsindex": speed,
                "saison": season, "qualitaet": quality, "anzahl": int(quantity),
                "montage": mounting == "ja"}
    except (TypeError, ValueError) as exc:
        raise ReifenSucheEingabefehler(
            "Bitte eine Größe wie 205/55 R16 91V, Saison, Qualität, 1 bis 8 Reifen und den Montagewunsch angeben.") from exc


def _zeit(value):
    text = _text(value, maximum=40)
    dt = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("Zeitzone fehlt.")
    return dt.astimezone(timezone.utc)


def _montage(query):
    if not query["montage"]:
        return None
    rim = query["zoll"]
    if query["anzahl"] > 4 or rim < 13 or rim > 20:
        raise ValueError("Für diesen Montageumfang ist ein individuelles Angebot erforderlich.")
    price_id = "montage_13_16" if rim <= 16 else "montage_17_18" if rim <= 18 else "montage_19_20"
    return berechne_reifenservice([{"preis_id": price_id, "menge": query["anzahl"]}])


def _produkt(item, query, mounting, now):
    if not isinstance(item, Mapping):
        raise ValueError("Kein bestätigtes Angebot.")
    for key in ("breite", "querschnitt", "zoll"):
        if _integer(item.get(key), 1, 500) != query[key]:
            raise ValueError("Andere Reifengröße.")
    load = _integer(item.get("lastindex"), 60, 150)
    speed = item.get("geschwindigkeitsindex")
    if not isinstance(speed, str) or speed not in _TEMPO:
        raise ValueError("Geschwindigkeitsindex fehlt.")
    if query["lastindex"] is not None and load < query["lastindex"]:
        raise ValueError("Lastindex zu niedrig.")
    if query["geschwindigkeitsindex"] and _TEMPO[speed] < _TEMPO[query["geschwindigkeitsindex"]]:
        raise ValueError("Geschwindigkeitsindex zu niedrig.")
    if item.get("saison") != query["saison"] or item.get("qualitaet") != query["qualitaet"]:
        raise ValueError("Andere Saison oder Qualitätsklasse.")
    stock = _integer(item.get("bestand"), 0, 1000000)
    if stock < query["anzahl"] or item.get("waehrung") != "EUR" or item.get("preis_bestaetigt") is not True:
        raise ValueError("Keine ausreichende bestätigte Verfügbarkeit.")
    if query["montage"] and (item.get("montage_standard_bestaetigt") is not True
                             or item.get("runflat") is True):
        raise ValueError("Die Ausführung ist nicht für den Standardmontagepreis bestätigt.")
    retrieved, valid_until = _zeit(item.get("abgerufen_am")), _zeit(item.get("gueltig_bis"))
    if retrieved < now - MAX_PREISALTER or retrieved > now + timedelta(seconds=30) or valid_until <= now or valid_until <= retrieved:
        raise ValueError("Preis ist nicht aktuell.")
    sku = _text(item.get("artikel_id"), maximum=100)
    if not re.fullmatch(r"[A-Za-z0-9_.:/-]+", sku):
        raise ValueError("Ungültige Artikel-ID.")
    name = _text(item.get("bezeichnung"), maximum=200)
    delivery = _text(item.get("lieferzeit"), maximum=120)
    ean = item.get("ean", "")
    if not isinstance(ean, str) or (ean and not re.fullmatch(r"[0-9]{13}", ean)):
        raise ValueError("Ungültige EAN.")
    calculation = berechne_reifenpreis(einkauf_netto=item.get("netto_einkauf"),
        menge=query["anzahl"], bezugskosten_netto=item.get("fracht_netto"),
        preise_bestaetigt=True, montage_kalkulation=mounting)
    return {"artikel_id": sku, "ean": ean, "bezeichnung": name,
        "groesse": f"{query['breite']}/{query['querschnitt']} R{query['zoll']} {load}{speed}",
        "saison": query["saison"], "qualitaet": query["qualitaet"],
        "bestand": stock, "lieferzeit": delivery,
        "preisstand": retrieved.isoformat(),
        "gueltig_bis": min(valid_until, retrieved + MAX_PREISALTER).isoformat(),
        "preis": {"einzel_brutto": calculation["einzelpreis_brutto_reifen"],
                  "gesamt_reifen_brutto": calculation["gesamt_brutto_reifen"],
                  "versand_brutto": calculation["versand_brutto"],
                  "montage_brutto": calculation["montage_brutto"],
                  "gesamt_brutto": calculation["gesamt_brutto"], "waehrung": "EUR"}}


def suche_reifenangebote(args, provider=None, *, now=None):
    """Return (public payload, HTTP status); calls a server-side callable only.

    Provider contract: provider(query_dict) -> {"angebote": [item, ...]}.
    Every item requires dimension/index/season/curated quality, artikel_id,
    bezeichnung, EUR netto_einkauf, actual order-level fracht_netto, integer
    bestand, preis_bestaetigt=True, timezone-aware abgerufen_am/gueltig_bis and
    lieferzeit. ean is optional. For requested mounting, the server adapter must
    explicitly set montage_standard_bestaetigt=True after checking the actual
    tire construction against workshop pricing; runflat=True cannot qualify.
    No ratings are translated into quality classes.
    Missing, mismatched, stale or insufficient offers are discarded. No ordering.
    """
    query = parse_reifen_suche(args)
    unavailable = {"status": "nicht_verfuegbar", "verfuegbar": False, "produkte": [],
                   "nachricht": "Onlinepreise sind derzeit nicht verfügbar. Bitte senden Sie uns eine Reifenanfrage."}
    if provider is None:
        return unavailable, 200
    if not callable(provider):
        return unavailable, 503
    try:
        mounting = _montage(query)
    except ValueError:
        return {**unavailable, "nachricht": "Für diesen Montageumfang erstellen wir Ihnen gerne ein individuelles Angebot."}, 200
    try:
        raw = provider(dict(query))
        if not isinstance(raw, Mapping) or not isinstance(raw.get("angebote"), list) or len(raw["angebote"]) > 100:
            raise ValueError("Ungültige Anbieterantwort.")
    except Exception:
        # A provider error may contain credentials or purchase prices. Neither
        # its text nor raw structures may reach the customer response.
        return unavailable, 503
    current = now or datetime.now(timezone.utc)
    products, seen = [], set()
    for item in raw["angebote"]:
        try:
            product = _produkt(item, query, mounting, current)
            if product["artikel_id"] not in seen:
                seen.add(product["artikel_id"])
                products.append(product)
        except (ValueError, TypeError, ArithmeticError, OverflowError):
            continue
    products.sort(key=lambda product: Decimal(product["preis"]["gesamt_brutto"]))
    return {"status": "ok" if products else "keine_treffer", "verfuegbar": bool(products),
            "produkte": products[:MAX_PRODUKTE],
            "nachricht": "" if products else "Für diese Auswahl sind gerade keine bestätigten Onlinepreise verfügbar."}, 200
