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
import unicodedata
from urllib.parse import urlsplit

from reifenangebote import REIFEN_SAISONS, REIFEN_QUALITAETEN
from reifenpreis_kalkulation import berechne_reifenpreis
from reifenservice import berechne_reifenservice
from reifen_bilder import public_image_metadata
from tyresystem_provider import TyreSystemCatalogError, valid_manufacturer
from tyresystem_feed import MAX_ROWS


MAX_PREISALTER = timedelta(minutes=15)
MAX_PRODUKTE = 12
# Workshop decision, 02.11.2026: the existing EUR 5 surcharge applies at 40 or below.
NIEDERQUERSCHNITT_BIS = 40
_FELDER = {"groesse", "saison", "qualitaet", "anzahl", "montage", "hersteller", "sortierung", "seite"}
# Nominal km/h, not alphabetic order. Conservative search: no automatic winter
# exceptions or special >300 km/h approvals. Source: Michelin's published table:
# https://www.michelin.de/auto/tipps/grundlagen-reifen/lastindex-geschwindigkeitsindex
_TEMPO = {"L": 120, "M": 130, "N": 140, "P": 150, "Q": 160, "R": 170, "S": 180,
          "T": 190, "U": 200, "H": 210, "V": 240, "W": 270, "Y": 300}


class ReifenSucheEingabefehler(ValueError):
    def __init__(self, message, *, code="suchfelder_ungueltig", feld=None):
        super().__init__(message)
        allowed = {"suchfelder_ungueltig", "artikelauswahl_ungueltig"} | {key + "_ungueltig" for key in _FELDER}
        self.code = code if code in allowed else "suchfelder_ungueltig"
        self.feld = feld if feld in _FELDER else None


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
        match = re.fullmatch(
            r"([0-9]{3})(?:\s*/\s*|\s+)([0-9]{2})(?:\s*Z?R\s*|\s+)([0-9]{2})"
            r"(?:\s*([0-9]{2,3})?\s*([A-Z])?)?", size)
        if not match:
            # Exactly seven digits unambiguously identify the three dimensions.
            # Never infer or rewrite a missing load/speed requirement.
            match = re.fullmatch(r"([0-9]{3})([0-9]{2})([0-9]{2})", size)
        if not match:
            raise ValueError("size")
        width, ratio, rim = (int(match[i]) for i in (1, 2, 3))
        _integer(width, 100, 500)
        _integer(ratio, 10, 95)
        _integer(rim, 10, 30)
        load = int(match[4]) if match.lastindex >= 4 and match[4] else None
        speed = match[5] if match.lastindex >= 5 and match[5] else None
        if load is not None:
            _integer(load, 60, 150)
        if speed is not None and speed not in _TEMPO:
            raise ValueError("speed")
    except (TypeError, ValueError) as exc:
        raise ReifenSucheEingabefehler(
            "Bitte eine Reifengröße wie 225/40 R18 92V oder 225 40 18 eingeben. "
            "Last- und Geschwindigkeitsindex unverändert übernehmen.", code="groesse_ungueltig", feld="groesse") from exc
    season, quality = args.get("saison"), args.get("qualitaet")
    if not isinstance(season, str) or season not in REIFEN_SAISONS:
        raise ReifenSucheEingabefehler("Bitte Sommer-, Winter- oder Ganzjahresreifen auswählen.",
                                      code="saison_ungueltig", feld="saison")
    if not isinstance(quality, str) or quality not in REIFEN_QUALITAETEN:
        raise ReifenSucheEingabefehler("Bitte eine Marken- und Preiswahl treffen, zum Beispiel Alle Marken.",
                                      code="qualitaet_ungueltig", feld="qualitaet")
    quantity = args.get("anzahl")
    if not isinstance(quantity, str) or not re.fullmatch(r"[1-8]", quantity):
        raise ReifenSucheEingabefehler("Bitte eine Anzahl von 1 bis 8 Reifen auswählen.",
                                      code="anzahl_ungueltig", feld="anzahl")
    mounting = args.get("montage")
    if not isinstance(mounting, str) or mounting not in {"ja", "nein"}:
        raise ReifenSucheEingabefehler("Bitte Abholung oder Montage auswählen.",
                                      code="montage_ungueltig", feld="montage")
    result = {"breite": width, "querschnitt": ratio, "zoll": rim,
            "lastindex": load, "geschwindigkeitsindex": speed,
            "saison": season, "qualitaet": quality, "anzahl": int(quantity),
            "montage": mounting == "ja"}
    manufacturer = args.get("hersteller", "")
    if (not isinstance(manufacturer, str) or len(manufacturer) > 100
            or any(unicodedata.category(char).startswith("C") for char in manufacturer)
            or (manufacturer.strip() and not valid_manufacturer(manufacturer.strip()))):
        raise ReifenSucheEingabefehler("Bitte einen Hersteller aus der Auswahl wählen.",
                                      code="hersteller_ungueltig", feld="hersteller")
    if manufacturer.strip():
        result["hersteller"] = manufacturer.strip()
    sort = args.get("sortierung", "preis_auf")
    if sort not in ("preis_auf", "preis_ab", "name"):
        raise ReifenSucheEingabefehler("Bitte eine gültige Sortierung auswählen.",
                                      code="sortierung_ungueltig", feld="sortierung")
    if sort != "preis_auf":
        result["sortierung"] = sort
    page = args.get("seite", "1")
    if not isinstance(page, str) or not re.fullmatch(r"[1-9][0-9]{0,4}", page) or int(page) > 10000:
        raise ReifenSucheEingabefehler("Bitte eine gültige Ergebnisseite auswählen.",
                                      code="seite_ungueltig", feld="seite")
    if page != "1":
        result["seite"] = int(page)
    return result


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
    services = [{"preis_id": price_id, "menge": query["anzahl"]}]
    if query["querschnitt"] <= NIEDERQUERSCHNITT_BIS:
        services.append({"preis_id": "runflat", "menge": query["anzahl"]})
    return berechne_reifenservice(services)


def _reifenlabel(item):
    """A bounded public label summary; no provider URL is fetched or embedded."""
    value = item.get("eprel_url")
    if (not isinstance(value, str) or not value or len(value) > 300
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)):
        return None
    try:
        link = urlsplit(value)
        if (link.scheme != "https" or link.hostname != "eprel.ec.europa.eu"
                or link.username or link.password or link.port not in (None, 443)
                or link.query or link.fragment
                or not re.fullmatch(r"/(?:qr/[0-9]{1,12}|screen/product/tyres/[0-9]{1,12})/?", link.path)):
            return None
        fuel, wet, noise_class = (item.get(k) for k in
            ("label_kraftstoff", "label_nass", "label_geraeuschklasse"))
        noise = item.get("label_geraeusch_db")
        if isinstance(noise, str) and re.fullmatch(r"[0-9]{2,3}", noise):
            noise = int(noise)
        if (fuel not in ("A", "B", "C", "D", "E") or wet not in ("A", "B", "C", "D", "E")
                or noise_class not in ("A", "B", "C") or type(noise) is not int or not 40 <= noise <= 130):
            return None
    except ValueError:
        return None
    return {"kraftstoffeffizienz": fuel, "nasshaftung": wet,
            "rollgeraeusch_db": noise, "rollgeraeuschklasse": noise_class,
            "eprel_url": value}


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
    quality = item.get("qualitaet")
    if quality is not None and quality not in ("preisbewusst", "mittelklasse", "markenqualitaet", "premium"):
        raise ValueError("Unbestätigte Qualitätsklasse.")
    if (item.get("saison") != query["saison"]
            or (query["qualitaet"] != "alle" and quality != query["qualitaet"])):
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
    manufacturer = item.get("hersteller")
    if manufacturer is not None and not valid_manufacturer(manufacturer):
        raise ValueError("Hersteller ist nicht prüfbar.")
    if query.get("hersteller") is not None and manufacturer != query["hersteller"]:
        raise ValueError("Anderer Hersteller.")
    delivery = _text(item.get("lieferzeit"), maximum=120)
    ean = item.get("ean", "")
    if ean is None:
        ean = ""
    if not isinstance(ean, str) or (ean and not re.fullmatch(r"(?:[0-9]{8}|[0-9]{12,14})", ean)):
        raise ValueError("Ungültige EAN.")
    calculation = berechne_reifenpreis(einkauf_netto=item.get("netto_einkauf"),
        menge=query["anzahl"], bezugskosten_netto=item.get("fracht_netto"),
        preise_bestaetigt=True, montage_kalkulation=mounting)
    product = {"artikel_id": sku, "ean": ean, "bezeichnung": name,
        "groesse": f"{query['breite']}/{query['querschnitt']} R{query['zoll']} {load}{speed}",
        "saison": query["saison"], "qualitaet": quality,
        "bestand": stock, "lieferzeit": delivery,
        "preisstand": retrieved.isoformat(),
        "gueltig_bis": min(valid_until, retrieved + MAX_PREISALTER).isoformat(),
        "preis": {"einzel_brutto": calculation["einzelpreis_brutto_reifen"],
                  "gesamt_reifen_brutto": calculation["gesamt_brutto_reifen"],
                  "versand_brutto": calculation["versand_brutto"],
                  "montage_brutto": calculation["montage_brutto"],
                  "gesamt_brutto": calculation["gesamt_brutto"], "waehrung": "EUR"}}
    label = _reifenlabel(item)
    if manufacturer is not None:
        product["hersteller"] = manufacturer
    if label is not None:
        product["eu_label"] = label
    try:
        picture = public_image_metadata(item.get("artikel_bild"), sku)
        if picture is not None:
            product["bild"] = picture
    except Exception:
        pass
    return product


def suche_reifenangebote(args, provider=None, *, now=None, artikel_id=None):
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
    Internal selection revalidation may pass artikel_id. An optional provider
    lookup(query, artikel_id) avoids search ranking limits; legacy callables
    remain supported by selecting the exact SKU before the public result cap.
    This selector is never accepted as a public search query parameter.
    """
    query = parse_reifen_suche(args)
    if artikel_id is not None and (not isinstance(artikel_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,100}", artikel_id)):
        raise ReifenSucheEingabefehler("Ungültige Artikelauswahl.", code="artikelauswahl_ungueltig")
    unavailable = {"status": "nicht_verfuegbar", "verfuegbar": False, "produkte": [],
                   "code": "katalog_nicht_verfuegbar",
                   "nachricht": "Die Reifenpreise können gerade nicht geladen werden. Bitte versuchen Sie es später erneut."}
    if provider is None:
        return {**unavailable, "code": "katalog_nicht_bereit",
                "nachricht": "Der Reifenkatalog wird noch vorbereitet. Aktuell sind keine Onlinepreise verfügbar."}, 200
    if not callable(provider):
        return unavailable, 503
    try:
        mounting = _montage(query)
    except ValueError:
        return {**unavailable, "code": "montage_nicht_online",
                "nachricht": "Für diesen Montageumfang ist kein Onlinepreis verfügbar. Bitte wählen Sie Abholung oder fragen Sie die Montage persönlich an."}, 200
    try:
        lookup = getattr(provider, "lookup", None) if artikel_id is not None else None
        raw = lookup(dict(query), artikel_id) if callable(lookup) else provider(dict(query))
    except TyreSystemCatalogError as exc:
        if exc.code == "cache_stale":
            return {**unavailable, "code": "katalog_veraltet",
                    "nachricht": "Die gespeicherten Reifenpreise sind abgelaufen. Bitte versuchen Sie es später erneut."}, 503
        if exc.code == "cache_missing":
            return {**unavailable, "code": "katalog_nicht_bereit",
                    "nachricht": "Der Reifenkatalog wurde noch nicht bereitgestellt. Aktuell sind keine Onlinepreise verfügbar."}, 503
        # Only this explicit allowlist is public; never copy provider messages.
        return unavailable, 503
    except Exception:
        # A provider error may contain credentials or purchase prices. Neither
        # its text nor raw structures may reach the customer response.
        return unavailable, 503
    if not isinstance(raw, Mapping) or not isinstance(raw.get("angebote"), list) or len(raw["angebote"]) > 100:
        return {**unavailable, "code": "angebote_nicht_pruefbar",
                "nachricht": "Die Reifenangebote konnten nicht vollständig geprüft werden. Bitte versuchen Sie es später erneut."}, 503
    pagination = {}
    paginated = raw.get("paginiert", False)
    if (type(paginated) is not bool or (paginated and ("gesamt" not in raw or len(raw["angebote"]) > MAX_PRODUKTE))
            or ("gesamt" in raw and (type(raw["gesamt"]) is not int or not len(raw["angebote"]) <= raw["gesamt"] <= MAX_ROWS))):
        return {**unavailable, "code": "angebote_nicht_pruefbar"}, 503
    if artikel_id is None and "gesamt" in raw:
        pagination = {"treffer_gesamt": raw["gesamt"], "seite": query.get("seite", 1),
                      "seiten": (raw["gesamt"] + MAX_PRODUKTE - 1) // MAX_PRODUKTE}
    current = now or datetime.now(timezone.utc)
    products, seen = [], set()
    for item in raw["angebote"]:
        if artikel_id is not None and (not isinstance(item, Mapping) or item.get("artikel_id") != artikel_id):
            continue
        try:
            product = _produkt(item, query, mounting, current)
            if product["artikel_id"] not in seen:
                seen.add(product["artikel_id"])
                products.append(product)
        except (ValueError, TypeError, ArithmeticError, OverflowError):
            continue
    sort = query.get("sortierung", "preis_auf")
    if sort == "name":
        products.sort(key=lambda product: product["bezeichnung"].casefold())
    else:
        products.sort(key=lambda product: Decimal(product["preis"]["gesamt_brutto"]), reverse=sort == "preis_ab")
    if not products and raw["angebote"]:
        return {**unavailable, "code": "angebote_nicht_pruefbar",
                "nachricht": "Für die übermittelten Angebote sind Preis oder Verfügbarkeit nicht ausreichend bestätigt. Bitte versuchen Sie es später erneut."}, 200
    if not paginated and artikel_id is None:
        start = (query.get("seite", 1) - 1) * MAX_PRODUKTE
        products = products[start:start + MAX_PRODUKTE]
    return {"status": "ok" if products else "keine_treffer", "code": "ok" if products else "keine_treffer", "verfuegbar": bool(products),
            **pagination, "produkte": products[:MAX_PRODUKTE],
            "nachricht": "" if products else "Für diese Auswahl sind gerade keine bestätigten Onlinepreise verfügbar."}, 200


def suche_reifenkatalog_auswahl(provider=None, *, now=None):
    """Read-only, freshly validated public facets; no provider structure escapes."""
    unavailable = {"status": "nicht_verfuegbar", "code": "katalog_nicht_verfuegbar",
                   "dimensionen": [], "hersteller": [],
                   "nachricht": "Die verfügbaren Reifengrößen können gerade nicht geladen werden."}
    try:
        facets = getattr(provider, "facets", None)
        if not callable(facets):
            return {**unavailable, "code": "katalog_nicht_bereit"}, 200
        raw = facets()
    except TyreSystemCatalogError as exc:
        codes = {"cache_stale": "katalog_veraltet", "cache_missing": "katalog_nicht_bereit"}
        return {**unavailable, "code": codes.get(exc.code, "katalog_nicht_verfuegbar")}, 503
    except Exception:
        return unavailable, 503
    try:
        if (not isinstance(raw, Mapping) or not isinstance(raw.get("dimensionen"), list)
                or len(raw["dimensionen"]) > MAX_ROWS or not isinstance(raw.get("hersteller"), list)
                or len(raw["hersteller"]) > MAX_ROWS):
            raise ValueError()
        retrieved, valid_until = _zeit(raw.get("preisstand")), _zeit(raw.get("gueltig_bis"))
        current = now or datetime.now(timezone.utc)
        if retrieved <= current - MAX_PREISALTER or valid_until <= current:
            return {**unavailable, "code": "katalog_veraltet"}, 503
        if retrieved > current + timedelta(seconds=30) or valid_until <= retrieved:
            raise ValueError()
        dimensions, seen = [], set()
        for item in raw["dimensionen"]:
            if not isinstance(item, Mapping):
                raise ValueError()
            key = (_integer(item.get("breite"), 100, 500), _integer(item.get("querschnitt"), 10, 95),
                   _integer(item.get("zoll"), 10, 30))
            seasons = item.get("saisons")
            if (key in seen or not isinstance(seasons, list) or not 1 <= len(seasons) <= 3
                    or any(not isinstance(season, str) or season not in REIFEN_SAISONS for season in seasons)
                    or len(set(seasons)) != len(seasons)):
                raise ValueError()
            seen.add(key)
            dimensions.append({"breite": key[0], "querschnitt": key[1], "zoll": key[2], "saisons": sorted(seasons)})
        if any(not valid_manufacturer(value) for value in raw["hersteller"]) or len(set(raw["hersteller"])) != len(raw["hersteller"]):
            raise ValueError()
        dimensions.sort(key=lambda item: (item["breite"], item["querschnitt"], item["zoll"]))
        manufacturers = sorted(raw["hersteller"], key=lambda value: (value.casefold(), value))
        return {"status": "ok", "code": "ok", "dimensionen": dimensions, "hersteller": manufacturers,
                "preisstand": retrieved.isoformat(), "gueltig_bis": min(valid_until, retrieved + MAX_PREISALTER).isoformat()}, 200
    except (ValueError, TypeError, ArithmeticError, OverflowError):
        return unavailable, 503
