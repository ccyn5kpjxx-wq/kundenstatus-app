"""Typed, read-only wheel-product selection; no Flask, database or transport.

Initial selections require a valid 15-minute token. ``refresh_owned`` is only
for an already owned server-side draft: the caller must authenticate ownership
and pass its stored query/SKU, never arbitrary browser fields. Neither method
reserves stock, approves fitment, enables a purchase or adds a configured set.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from types import MappingProxyType
import unicodedata

from itsdangerous import TimestampSigner, URLSafeTimedSerializer

from felgen_bilder import public_felgen_image_metadata
from felgen_katalog import FelgenCatalogProvider, FelgenKatalogError
from reifen_suche import parse_reifen_suche, suche_reifenangebote


TOKEN_MAX_AGE = 900
FELGEN_TOKEN_PREFIX = "felgen."
FELGEN_TOKEN_SALT = "raeder-felgen-auswahl-v1"
_TIRE_SALT = "reifen-auswahl-v1"
_TIRE_QUERY = frozenset({"groesse", "saison", "qualitaet", "anzahl", "montage"})
_SKU = re.compile(r"[1-9][0-9]{0,17}")
_TIRE_SKU = re.compile(r"[A-Za-z0-9_.:/-]{1,100}")
_MONEY = re.compile(r"(?:0|[1-9][0-9]{0,5})\.[0-9]{2}")
_ERRORS = {
    "auswahl_ungueltig": "Die Artikelauswahl ist ungültig oder abgelaufen. Bitte neu auswählen.",
    "produktart_ungueltig": "Diese Produktart ist nicht für die Auswahl eingerichtet.",
    "komplettrad_nicht_konfiguriert": "Ein bestätigtes Komplettrad-Set ist noch nicht verfügbar.",
    "artikel_nicht_verfuegbar": "Dieser Artikel ist für die gewählte Menge nicht mehr verfügbar.",
    "katalog_nicht_bereit": "Der Produktkatalog ist noch nicht bereit.",
    "katalog_veraltet": "Die Produktdaten müssen aktualisiert werden. Bitte später erneut versuchen.",
    "katalog_nicht_verfuegbar": "Der Produktkatalog ist gerade nicht verfügbar.",
}


class RaederAuswahlNichtVerfuegbar(ValueError):
    def __init__(self, code="auswahl_ungueltig"):
        self.code = code if code in _ERRORS else "auswahl_ungueltig"
        super().__init__(_ERRORS[self.code])


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError()
    return value.astimezone(timezone.utc)


def _freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    if value is None or type(value) in (str, int, bool):
        return value
    raise ValueError()


def _mutable(value):
    if isinstance(value, Mapping):
        return {key: _mutable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_mutable(item) for item in value]
    return value


def _text(value, maximum):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum or value != value.strip()
            or any(unicodedata.category(char).startswith("C") for char in value)):
        raise ValueError()
    return value


def _quantity(value):
    if type(value) is not int or not 1 <= value <= 8:
        raise ValueError()
    return value


class _ClockSigner(TimestampSigner):
    def __init__(self, *args, clock, **kwargs):
        super().__init__(*args, **kwargs)
        self.clock = clock

    def get_timestamp(self):
        return int(_utc(self.clock()).timestamp())


def _serializer(secret, salt, clock):
    return URLSafeTimedSerializer(secret, salt=salt, signer=_ClockSigner,
                                 signer_kwargs={"clock": clock})


def erstelle_felgen_auswahl_token(secret, artikel_id, anzahl, *, clock=None):
    """Mint a purchase-selection token, distinct from read-only rim-page tokens."""
    try:
        if not isinstance(artikel_id, str) or not _SKU.fullmatch(artikel_id):
            raise ValueError()
        quantity = _quantity(anzahl)
        timer = clock or (lambda: datetime.now(timezone.utc))
        return FELGEN_TOKEN_PREFIX + _serializer(secret, FELGEN_TOKEN_SALT, timer).dumps(
            {"produktart": "felgen", "artikel_id": artikel_id, "anzahl": quantity})
    except Exception:
        raise RaederAuswahlNichtVerfuegbar() from None


@dataclass(frozen=True, slots=True)
class RaederAuswahl:
    produktart: str
    artikel_id: str
    anzahl: int
    montage: bool | None
    query: Mapping
    produkt: Mapping

    @property
    def core(self):
        value = {"produktart": self.produktart, "artikel_id": self.artikel_id, "anzahl": self.anzahl}
        if self.montage is not None:
            value["montage"] = self.montage
        return MappingProxyType(value)

    def mutable_query(self):
        return _mutable(self.query)

    def mutable_product(self):
        return _mutable(self.produkt)

    def as_dict(self):
        return {**dict(self.core), "query": self.mutable_query(), "produkt": self.mutable_product()}


class RaederAuswahlResolver:
    def __init__(self, config, secret, *, clock=None):
        if not isinstance(config, Mapping) or not isinstance(secret, (str, bytes)) or not secret:
            raise RaederAuswahlNichtVerfuegbar("katalog_nicht_bereit")
        self.config, self._secret = config, secret
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return "<RaederAuswahlResolver private_configuration=redacted>"

    def from_token(self, token, *, produktart="reifen"):
        self._category(produktart)
        try:
            if not isinstance(token, str) or not 1 <= len(token) <= 2000:
                raise ValueError()
            if produktart == "reifen":
                if token.startswith(FELGEN_TOKEN_PREFIX):
                    raise ValueError()
                selected = _serializer(self._secret, _TIRE_SALT, self._clock).loads(token, max_age=TOKEN_MAX_AGE)
                if not isinstance(selected, dict) or set(selected) != {"query", "artikel_id"}:
                    raise ValueError()
                query, sku = selected["query"], selected["artikel_id"]
                if not isinstance(query, Mapping) or set(query) != _TIRE_QUERY:
                    raise ValueError()
            else:
                if not token.startswith(FELGEN_TOKEN_PREFIX):
                    raise ValueError()
                selected = _serializer(self._secret, FELGEN_TOKEN_SALT, self._clock).loads(
                    token[len(FELGEN_TOKEN_PREFIX):], max_age=TOKEN_MAX_AGE)
                if (not isinstance(selected, dict) or set(selected) != {"produktart", "artikel_id", "anzahl"}
                        or selected["produktart"] != "felgen"):
                    raise ValueError()
                query, sku = {"produktart": "felgen", "anzahl": _quantity(selected["anzahl"])}, selected["artikel_id"]
        except Exception:
            raise RaederAuswahlNichtVerfuegbar() from None
        return self.refresh_owned(query, sku)

    @staticmethod
    def _category(category):
        if category == "komplettraeder":
            raise RaederAuswahlNichtVerfuegbar("komplettrad_nicht_konfiguriert")
        if not isinstance(category, str) or category not in {"reifen", "felgen"}:
            raise RaederAuswahlNichtVerfuegbar("produktart_ungueltig")

    def refresh_owned(self, query, artikel_id):
        """Refresh only authenticated, server-owned stored selections; no token TTL bypass route."""
        if not isinstance(query, Mapping):
            raise RaederAuswahlNichtVerfuegbar()
        category = query.get("produktart", "reifen")
        self._category(category)
        try:
            now = _utc(self._clock())
            if category == "reifen":
                if set(query) != _TIRE_QUERY or not isinstance(artikel_id, str) or not _TIRE_SKU.fullmatch(artikel_id):
                    raise ValueError()
                parsed = parse_reifen_suche(query)
                quantity, mounting = parsed["anzahl"], parsed["montage"]
                payload, status = suche_reifenangebote(dict(query), self.config.get("REIFENANGEBOT_PROVIDER"),
                                                     now=now, artikel_id=artikel_id)
                if status != 200 or not payload.get("produkte"):
                    code = payload.get("code")
                    raise RaederAuswahlNichtVerfuegbar(code if code in _ERRORS else "artikel_nicht_verfuegbar")
                product = next((item for item in payload["produkte"] if item["artikel_id"] == artikel_id), None)
                if product is None:
                    raise RaederAuswahlNichtVerfuegbar("artikel_nicht_verfuegbar")
            else:
                if set(query) != {"produktart", "anzahl"} or not isinstance(artikel_id, str) or not _SKU.fullmatch(artikel_id):
                    raise ValueError()
                quantity, mounting = _quantity(query["anzahl"]), None
                provider = self.config.get("FELGENANGEBOT_PROVIDER")
                if not isinstance(provider, FelgenCatalogProvider):
                    raise RaederAuswahlNichtVerfuegbar("katalog_nicht_bereit")
                product = self._rim_product(provider.lookup(artikel_id, quantity), artikel_id, quantity, now)
            return RaederAuswahl(category, artikel_id, quantity, mounting, _freeze(query), _freeze(product))
        except RaederAuswahlNichtVerfuegbar:
            raise
        except FelgenKatalogError as error:
            code = {"configuration": "katalog_nicht_bereit", "cache_stale": "katalog_veraltet"}.get(error.code,
                    "katalog_nicht_verfuegbar")
            raise RaederAuswahlNichtVerfuegbar(code) from None
        except (ValueError, TypeError, KeyError):
            raise RaederAuswahlNichtVerfuegbar() from None
        except Exception:
            raise RaederAuswahlNichtVerfuegbar("katalog_nicht_verfuegbar") from None

    @staticmethod
    def _rim_product(raw, sku, quantity, now):
        if raw is None:
            raise RaederAuswahlNichtVerfuegbar("artikel_nicht_verfuegbar")
        if (not isinstance(raw, Mapping) or raw.get("artikel_id") != sku
                or type(raw.get("anzahl")) is not int or raw["anzahl"] != quantity
                or type(raw.get("bestand")) is not int or not quantity <= raw["bestand"] <= 1000000
                or raw.get("material") not in {"alu", "stahl"} or raw.get("kennungspassung") != "nicht_geprueft"):
            raise ValueError()
        product = {"artikel_id": sku, "anzahl": quantity, "bestand": raw["bestand"],
                   "material": raw["material"], "kennungspassung": "nicht_geprueft"}
        for key, maximum in (("hersteller", 100), ("profil", 200), ("bezeichnung", 301)):
            product[key] = _text(raw.get(key), maximum)
        mpn, ean = raw.get("hersteller_artikelnummer"), raw.get("ean")
        if mpn is not None:
            _text(mpn, 100)
        if ean is not None and (not isinstance(ean, str) or not re.fullmatch(r"(?:[0-9]{8}|[0-9]{12,14})", ean)):
            raise ValueError()
        product.update(hersteller_artikelnummer=mpn, ean=ean)
        times = {}
        for key in ("preisstand", "gueltig_bis"):
            times[key] = _utc(datetime.fromisoformat(_text(raw.get(key), 40).replace("Z", "+00:00")))
            product[key] = times[key].isoformat()
        if (not now - timedelta(minutes=15) < times["preisstand"] <= now + timedelta(seconds=30)
                or not now < times["gueltig_bis"] <= times["preisstand"] + timedelta(minutes=15)):
            raise RaederAuswahlNichtVerfuegbar("katalog_veraltet")
        price = raw.get("preis")
        if not isinstance(price, Mapping) or price.get("waehrung") != "EUR":
            raise ValueError()
        checked = {}
        for key in ("einzel_brutto", "gesamt_felgen_brutto", "versand_brutto", "gesamt_brutto"):
            value = price.get(key)
            if not isinstance(value, str) or not _MONEY.fullmatch(value) or Decimal(value) > 100000:
                raise ValueError()
            checked[key] = value
        if (Decimal(checked["einzel_brutto"]) <= 0
                or Decimal(checked["einzel_brutto"]) * quantity != Decimal(checked["gesamt_felgen_brutto"])
                or Decimal(checked["gesamt_felgen_brutto"]) + Decimal(checked["versand_brutto"]) != Decimal(checked["gesamt_brutto"])):
            raise ValueError()
        product["preis"] = {**checked, "waehrung": "EUR"}
        image = public_felgen_image_metadata(raw.get("bild"), sku)
        if image is not None:
            product["bild"] = image
        return product

    @staticmethod
    def quote_identity(auswahl):
        """JSON-safe identity/price binding, excluding receipt, stock and display extras."""
        if not isinstance(auswahl, RaederAuswahl):
            raise RaederAuswahlNichtVerfuegbar()
        product = auswahl.mutable_product()
        keys = ("artikel_id", "bezeichnung", "ean", "hersteller", "profil", "hersteller_artikelnummer",
                "groesse", "saison", "qualitaet", "material", "kennungspassung")
        return {"core": dict(auswahl.core), "query": auswahl.mutable_query(),
                "produkt": {**{key: product[key] for key in keys if key in product}, "preis": product["preis"]}}
