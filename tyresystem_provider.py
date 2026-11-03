"""Private TYREsystem catalogue cache and explicitly released search adapter.

No network, dotenv, application import, reservation or order operation. The
callable returns INTERNAL search inputs (including EK); reifen_suche owns the
public selling-price whitelist. Reading a cache never changes its receipt time.
"""
from collections.abc import Mapping
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
import threading
import unicodedata
from types import MappingProxyType

from tyresystem_feed import FeedArticle, FeedSnapshot, HEADERS, MAX_ROWS, parse_feed


MAX_CACHE_BYTES = 64 * 1024 * 1024
MAX_AGE = timedelta(minutes=15)
QUALITY_CLASSES = frozenset({"preisbewusst", "mittelklasse", "markenqualitaet", "premium"})
_SPEED = {"L": 120, "M": 130, "N": 140, "P": 150, "Q": 160, "R": 170,
          "S": 180, "T": 190, "U": 200, "H": 210, "V": 240, "W": 270, "Y": 300}
_FIELDS = ("artikel_id", "ean", "hersteller", "profil", "hersteller_artikelnummer",
           "breite", "querschnitt", "zoll", "bauart", "lastindex", "speedindex", "saison",
           "gemeldeter_bestand", "einkaufspreis", "dot_hinweis", "demo", "da", "eprel_url",
           "label_kraftstoff", "label_nass", "label_geraeusch_db", "label_geraeuschklasse")
_STRINGS = {"artikel_id", "hersteller", "profil", "zoll", "bauart", "speedindex", "saison", "einkaufspreis"}
_INTS = {"breite", "querschnitt", "lastindex", "gemeldeter_bestand"}
_OPTIONAL = set(_FIELDS) - _STRINGS - _INTS - {"demo", "da"}
_SOURCES = {"price": "https://www.tyresystem.de/agb",
            "shipping": "https://www.tyresystem.de/lieferbedingungen",
            "delivery": "https://www.tyresystem.de/leistungen"}
_DELIVERY_TEXT = "Standardlieferung 1–4 Werktage; keine Reservierung."
PAGE_SIZE = 12


def valid_manufacturer(value):
    return (isinstance(value, str) and 1 <= len(value) <= 100 and value == value.strip()
            and not any(unicodedata.category(char).startswith("C") or char in "<>" for char in value))
_ERRORS = {"configuration": "Der private Reifenkatalog ist nicht gültig eingerichtet.",
           "cache_missing": "Der Reifenkatalog wurde noch nicht aktualisiert.",
           "cache_invalid": "Der Reifenkatalog ist nicht vollständig prüfbar.",
           "cache_stale": "Der Reifenkatalog ist nicht aktuell.",
           "query": "Die Katalogsuche ist nicht gültig.",
           "write": "Der Reifenkatalog konnte nicht sicher aktualisiert werden."}


class TyreSystemCatalogError(ValueError):
    def __init__(self, code):
        self.code = code if code in _ERRORS else "cache_invalid"
        super().__init__(_ERRORS[self.code])


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError()
    return value.astimezone(timezone.utc)


def _canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError()
        value[key] = item
    return value


def _record(article):
    if not isinstance(article, FeedArticle):
        raise ValueError()
    result = {key: getattr(article, key) for key in _FIELDS}
    for key in ("zoll", "einkaufspreis"):
        if not isinstance(result[key], Decimal) or not result[key].is_finite():
            raise ValueError()
        result[key] = format(result[key], "f")
    return result


def _decode(payload):
    """Validate normalized cache records through the existing CSV parser too."""
    if (not isinstance(payload, dict) or set(payload) != {"schema", "received_at", "source_sha256", "articles"}
            or type(payload["schema"]) is not int or payload["schema"] != 1
            or not isinstance(payload["received_at"], str) or len(payload["received_at"]) > 40
            or not isinstance(payload["source_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", payload["source_sha256"])
            or not isinstance(payload["articles"], list) or not 1 <= len(payload["articles"]) <= MAX_ROWS):
        raise ValueError()
    received = _utc(datetime.fromisoformat(payload["received_at"]))
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";", lineterminator="\n")
    writer.writerow(HEADERS)
    for item in payload["articles"]:
        if (not isinstance(item, dict) or set(item) != set(_FIELDS)
                or any(type(item[k]) is not int for k in _INTS)
                or any(type(item[k]) is not bool for k in ("demo", "da"))
                or any(not isinstance(item[k], str) for k in _STRINGS)
                or any(item[k] is not None and not isinstance(item[k], str) for k in _OPTIONAL)
                or any(isinstance(v, str) and len(v) > 2048 for v in item.values())):
            raise ValueError()
        row = dict.fromkeys(HEADERS, "")
        row.update(Artikelnummer=item["artikel_id"], ean=item["ean"] or "",
            Lagerbestand=str(item["gemeldeter_bestand"]), Einkaufspreis=item["einkaufspreis"].replace(".", ","),
            Hersteller=item["hersteller"], Profil=item["profil"], Herstellerartikelnummer=item["hersteller_artikelnummer"] or "",
            Reifenbreite=str(item["breite"]), Reifenquerschnitt=str(item["querschnitt"]),
            Reifendurchmesser=item["zoll"], Loadindex=str(item["lastindex"]), Speedindex=item["speedindex"],
            Saison={"sommer": "s", "winter": "w", "ganzjahr": "g"}.get(item["saison"], ""),
            Fahrzeugtyp="PKW", Bauart=item["bauart"], Artikeltyp="Reifen", Artikeluntertyp="PKW",
            Dot=item["dot_hinweis"] or "", Demo="Demo" if item["demo"] else "", Da="Da" if item["da"] else "",
            **{"EPREL-URL": item["eprel_url"] or "",
               "LabelKraftstoffeffizienz": item["label_kraftstoff"] or "",
               "LabelNasshaftung": item["label_nass"] or "",
               "LabelRollgeraeusch": item["label_geraeusch_db"] or "",
               "LabelRollgeraeuschklasse": item["label_geraeuschklasse"] or ""})
        writer.writerow(row[key] for key in HEADERS)
    parsed = parse_feed(output.getvalue().encode("utf-8"), abgerufen_am=received)
    expected = sorted(payload["articles"], key=lambda item: int(item["artikel_id"]))
    if [_record(article) for article in parsed.artikel] != expected:
        raise ValueError()  # No partial, silently normalized or duplicate cache.
    snapshot = FeedSnapshot(parsed.artikel, received, payload["source_sha256"], parsed.statistik, parsed.fehlerkategorien)
    return IndexedCatalog.build(snapshot)


@dataclass(frozen=True, slots=True)
class IndexedCatalog:
    snapshot: FeedSnapshot
    dimensions: Mapping

    @classmethod
    def build(cls, snapshot):
        index = {}
        for article in snapshot.artikel:
            if article.zoll == article.zoll.to_integral_value():
                key = (article.breite, article.querschnitt, int(article.zoll), article.saison)
                index.setdefault(key, []).append(article)
        return cls(snapshot, MappingProxyType({key: tuple(value) for key, value in index.items()}))


class TyreSystemCatalogCache:
    """One bounded checksummed JSON file; no credentials, pickle or public path."""
    def __init__(self, directory, *, private_root, clock=None):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._memory = None
        self._signature = None
        try:
            for value in (directory, private_root):
                text = str(value).replace("\\", "/")
                if text.startswith("//") or "://" in text or not Path(value).is_absolute():
                    raise ValueError()
            root, folder = Path(private_root).resolve(), Path(directory).resolve()
            if folder == root or not folder.is_relative_to(root):
                raise ValueError()
            if any(part.lower() in {"static", "public", "templates", "outputs", "wwwroot"} for part in folder.parts):
                raise ValueError()
            repository = Path(__file__).resolve().parent
            if folder.is_relative_to(repository) and not folder.is_relative_to(repository / "data"):
                raise ValueError()
            self.directory, self.private_root, self.path = folder, root, folder / "catalog.json"
        except Exception:
            raise TyreSystemCatalogError("configuration") from None

    def __repr__(self):
        return "<TyreSystemCatalogCache private_path=redacted>"

    def _path_guard(self):
        if (self.path.is_symlink() or self.directory.resolve() != self.directory
                or self.private_root.resolve() != self.private_root
                or not self.path.resolve().is_relative_to(self.private_root)):
            raise TyreSystemCatalogError("configuration")

    @staticmethod
    def _stat(stat):
        return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    def load(self):
        with self._lock:
            try:
                self._path_guard()
                signature = self._stat(self.path.stat())
                if signature == self._signature and self._memory is not None:
                    return self._memory
                with self.path.open("rb") as stream:
                    signature = self._stat(os.fstat(stream.fileno()))
                    if signature[1] > MAX_CACHE_BYTES:
                        raise ValueError()
                    raw = stream.read(MAX_CACHE_BYTES + 1)
                if len(raw) > MAX_CACHE_BYTES:
                    raise ValueError()
                envelope = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
                if not isinstance(envelope, dict) or set(envelope) != {"payload", "sha256"}:
                    raise ValueError()
                if hashlib.sha256(_canonical(envelope["payload"])).hexdigest() != envelope["sha256"]:
                    raise ValueError()
                catalog = _decode(envelope["payload"])
                if catalog.snapshot.abgerufen_am > _utc(self._clock()) + timedelta(seconds=30):
                    raise ValueError()
                self._memory, self._signature = catalog, signature
                return catalog
            except Exception as exc:
                self._memory = self._signature = None
                code = "cache_missing" if isinstance(exc, FileNotFoundError) else "cache_invalid"
                raise TyreSystemCatalogError(code) from None

    def replace(self, snapshot):
        """Publish locally only after full validation; failures preserve old bytes/time."""
        temporary, lock_owned = None, False
        lock_path = self.directory / ".catalog-write.lock"
        with self._lock:
            try:
                self._path_guard()
                self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                self._path_guard()
                descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(descriptor)
                lock_owned = True
                if not isinstance(snapshot, FeedSnapshot):
                    raise ValueError()
                received, now = _utc(snapshot.abgerufen_am), _utc(self._clock())
                if not now - MAX_AGE < received <= now + timedelta(seconds=30):
                    raise ValueError()
                payload = {"schema": 1, "received_at": received.isoformat(), "source_sha256": snapshot.sha256,
                           "articles": [_record(article) for article in snapshot.artikel]}
                catalog = _decode(payload)
                raw = _canonical({"payload": payload, "sha256": hashlib.sha256(_canonical(payload)).hexdigest()})
                if len(raw) > MAX_CACHE_BYTES:
                    raise ValueError()
                if self.path.exists():
                    try:
                        previous = self.load()
                    except TyreSystemCatalogError:
                        previous = None
                    if previous and previous.snapshot.abgerufen_am > received:
                        raise ValueError()  # A slower refresh cannot roll back newer data.
                descriptor, temporary = tempfile.mkstemp(prefix=".catalog-", suffix=".tmp", dir=self.directory)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path)
                temporary = None
                self._memory, self._signature = catalog, None
            except Exception:
                raise TyreSystemCatalogError("write") from None
            finally:
                if temporary is not None:
                    try:
                        os.unlink(temporary)
                    except OSError:
                        pass
                if lock_owned:
                    try:
                        os.unlink(lock_path)
                    except OSError:
                        pass


def _policy(value):
    keys = {"currency", "price_basis", "single_tyre_shipping_net", "free_shipping_from",
            "delivery_text", "verified_on", "sources"}
    try:
        if (not isinstance(value, Mapping) or set(value) != keys or value["currency"] != "EUR"
                or value["price_basis"] != "net_unit" or value["single_tyre_shipping_net"] != "6.00"
                or type(value["free_shipping_from"]) is not int or value["free_shipping_from"] != 2
                or value["delivery_text"] != _DELIVERY_TEXT
                or value["sources"] != _SOURCES
                or date.fromisoformat(value["verified_on"]) != date(2026, 11, 2)):
            return None
        return MappingProxyType(dict(value))
    except Exception:
        return None


def _metadata(values):
    result = {}
    if not isinstance(values, Mapping) or len(values) > MAX_ROWS:
        return MappingProxyType(result)
    allowed = {"bestaetigt", "nachweis", "qualitaet", "runflat", "montage_standard_bestaetigt", "xl", "schneeflocke"}
    for sku, item in values.items():
        if (not isinstance(sku, str) or not re.fullmatch(r"[1-9][0-9]{0,17}", sku)
                or not isinstance(item, Mapping) or set(item) - allowed or item.get("bestaetigt") is not True
                or not isinstance(item.get("nachweis"), str) or not 1 <= len(item["nachweis"]) <= 200 or not item["nachweis"].strip()
                or any(ord(char) < 32 for char in item["nachweis"])
                or (item.get("qualitaet") is not None and (not isinstance(item["qualitaet"], str) or item["qualitaet"] not in QUALITY_CLASSES))
                or any(key in item and type(item[key]) is not bool
                       for key in ("runflat", "montage_standard_bestaetigt", "xl", "schneeflocke"))):
            continue
        result[sku] = MappingProxyType(dict(item))
    return MappingProxyType(result)


class TyreSystemCatalogProvider:
    def __init__(self, cache, *, publication_approved=False, publication_reference="",
                 policy=None, article_metadata=None, clock=None, private_view_authorized=None, image_resolver=None):
        self.cache = cache
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._approved = (publication_approved is True and isinstance(publication_reference, str)
                          and 1 <= len(publication_reference) <= 200
                          and bool(publication_reference.strip())
                          and not any(ord(char) < 32 for char in publication_reference))
        self._policy = _policy(policy)
        self._metadata = _metadata(article_metadata)
        self._private_view_authorized = private_view_authorized if callable(private_view_authorized) else None
        self._image_resolver = image_resolver if callable(image_resolver) else None

    def __repr__(self):
        return "<TyreSystemCatalogProvider private_catalog=redacted>"

    @property
    def publication_ready(self):
        """Configuration readiness only; does not inspect or refresh the cache."""
        return self._approved and self._policy is not None

    def _read_authorized(self):
        if self._policy is None:
            return False
        if self._approved:
            return True
        try:
            return self._private_view_authorized is not None and self._private_view_authorized() is True
        except Exception:
            return False

    def _current_catalog(self):
        catalog = self.cache.load()
        now, received = _utc(self._clock()), catalog.snapshot.abgerufen_am
        if not now - MAX_AGE < received <= now + timedelta(seconds=30):
            raise TyreSystemCatalogError("cache_stale")
        return catalog

    def facets(self):
        """Current supported dimensions/brands only, without any purchase data."""
        if not self._read_authorized():
            raise TyreSystemCatalogError("configuration")
        catalog = self._current_catalog()
        dimensions, manufacturers = {}, set()
        for (width, ratio, rim, season), articles in catalog.dimensions.items():
            if not (100 <= width <= 500 and 10 <= ratio <= 95 and 10 <= rim <= 30):
                continue
            for article in articles:
                if (article.sonderware or article.gemeldeter_bestand < 1
                        or not valid_manufacturer(article.hersteller)
                        or len(article.hersteller + " " + article.profil) > 200):
                    continue
                dimensions.setdefault((width, ratio, rim), set()).add(season)
                manufacturers.add(article.hersteller)
        received = catalog.snapshot.abgerufen_am
        return {"dimensionen": [{"breite": width, "querschnitt": ratio, "zoll": rim,
                                  "saisons": sorted(seasons)}
                                 for (width, ratio, rim), seasons in sorted(dimensions.items())],
                "hersteller": sorted(manufacturers, key=lambda value: (value.casefold(), value)),
                "preisstand": received.isoformat(), "gueltig_bis": (received + MAX_AGE).isoformat()}

    def __call__(self, query):
        return self._offers(query)

    def lookup(self, query, artikel_id):
        """Revalidate one exact SKU with the same gates, independent of rank."""
        if not isinstance(artikel_id, str) or not re.fullmatch(r"[1-9][0-9]{0,17}", artikel_id):
            raise TyreSystemCatalogError("query")
        return self._offers(query, artikel_id=artikel_id)

    def _offers(self, query, *, artikel_id=None):
        if not self._read_authorized():
            return {"angebote": []}
        try:
            required = {"breite", "querschnitt", "zoll", "lastindex", "geschwindigkeitsindex",
                        "saison", "qualitaet", "anzahl", "montage"}
            if (not isinstance(query, Mapping) or not required <= set(query)
                    or set(query) - required - {"hersteller", "sortierung", "seite"}
                    or any(type(query[k]) is not int for k in ("breite", "querschnitt", "zoll", "anzahl"))
                    or not 100 <= query["breite"] <= 500 or not 10 <= query["querschnitt"] <= 95
                    or not 10 <= query["zoll"] <= 30 or not 1 <= query["anzahl"] <= 8
                    or type(query["montage"]) is not bool
                    or query["saison"] not in {"sommer", "winter", "ganzjahr"}
                    or query["qualitaet"] not in QUALITY_CLASSES | {"alle"}
                    or ("hersteller" in query and not valid_manufacturer(query["hersteller"]))
                    or query.get("sortierung", "preis_auf") not in {"preis_auf", "preis_ab", "name"}
                    or type(query.get("seite", 1)) is not int or not 1 <= query.get("seite", 1) <= 10000
                    or (query["lastindex"] is not None and (type(query["lastindex"]) is not int or not 60 <= query["lastindex"] <= 150))
                    or (query["geschwindigkeitsindex"] is not None and query["geschwindigkeitsindex"] not in _SPEED)):
                raise ValueError()
        except Exception:
            raise TyreSystemCatalogError("query") from None
        catalog = self._current_catalog()
        received = catalog.snapshot.abgerufen_am
        candidates = []
        for article in catalog.dimensions.get((query["breite"], query["querschnitt"], query["zoll"], query["saison"]), ()):
            if artikel_id is not None and article.artikel_id != artikel_id:
                continue
            meta = self._metadata.get(article.artikel_id, {})
            if (article.sonderware or article.gemeldeter_bestand < query["anzahl"]
                    or not valid_manufacturer(article.hersteller)
                    or (query.get("hersteller") is not None and article.hersteller != query["hersteller"])
                    or (query["lastindex"] is not None and article.lastindex < query["lastindex"])
                    or (query["geschwindigkeitsindex"] is not None and _SPEED[article.speedindex] < _SPEED[query["geschwindigkeitsindex"]])
                    or (query["qualitaet"] != "alle" and meta.get("qualitaet") != query["qualitaet"])
                    or (query["montage"] and (meta.get("montage_standard_bestaetigt") is not True or meta.get("runflat") is not False))
                    or len(article.hersteller + " " + article.profil) > 200):
                continue
            candidates.append((article, meta))
        if artikel_id is None:
            sort = query.get("sortierung", "preis_auf")
            if sort == "name":
                candidates.sort(key=lambda pair: ((pair[0].hersteller + " " + pair[0].profil).casefold(), int(pair[0].artikel_id)))
            else:
                candidates.sort(key=lambda pair: (pair[0].einkaufspreis * (-1 if sort == "preis_ab" else 1), int(pair[0].artikel_id)))
            total = len(candidates)
            start = (query.get("seite", 1) - 1) * PAGE_SIZE
            candidates = candidates[start:start + PAGE_SIZE]
        shipping = "0.00" if query["anzahl"] >= self._policy["free_shipping_from"] else self._policy["single_tyre_shipping_net"]
        offers = []
        for article, meta in candidates:
            item = {"artikel_id": article.artikel_id, "ean": article.ean, "hersteller": article.hersteller,
                    "bezeichnung": article.hersteller + " " + article.profil,
                    "breite": article.breite, "querschnitt": article.querschnitt, "zoll": int(article.zoll),
                    "lastindex": article.lastindex, "geschwindigkeitsindex": article.speedindex,
                    "saison": article.saison, "qualitaet": meta.get("qualitaet"),
                    "netto_einkauf": format(article.einkaufspreis, "f"), "fracht_netto": shipping,
                    "waehrung": "EUR", "bestand": article.gemeldeter_bestand, "preis_bestaetigt": True,
                    "abgerufen_am": received.isoformat(), "gueltig_bis": (received + MAX_AGE).isoformat(),
                    "lieferzeit": self._policy["delivery_text"],
                    "montage_standard_bestaetigt": meta.get("montage_standard_bestaetigt") is True and meta.get("runflat") is False,
                    "runflat": meta.get("runflat"), "xl": meta.get("xl"), "schneeflocke": meta.get("schneeflocke")}
            item.update({key: getattr(article, key) for key in ("eprel_url", "label_kraftstoff", "label_nass", "label_geraeusch_db", "label_geraeuschklasse")})
            if self._image_resolver is not None:
                try:
                    item["artikel_bild"] = self._image_resolver(article)
                except Exception:
                    pass
            offers.append(item)
        return {"angebote": offers, "gesamt": total, "paginiert": True} if artikel_id is None else {"angebote": offers}
