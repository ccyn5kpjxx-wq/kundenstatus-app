"""Private physical-rim catalogue from the authorized 28-column export.

No network, dotenv, application import, vehicle-fitment inference or orders.
Blank tire dimensions are never interpreted as rim geometry. Prices and stock
remain private until an explicit commercial policy and read authorization exist.
"""
from collections import Counter
from collections.abc import Mapping
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
import threading
from types import MappingProxyType
import unicodedata

from reifenpreis_kalkulation import berechne_reifenpreis
from tyresystem_feed import HEADERS, MAX_BYTES, MAX_ROWS, MAX_FIELD_CHARS


MAX_AGE = timedelta(minutes=15)
PAGE_SIZE = 12
# Schema 2 certifies that source Demo/Da flags were checked before caching.
CACHE_SCHEMA = 2
_SKU = re.compile(r"[1-9][0-9]{0,17}")
_SOURCES = {"price": "https://www.tyresystem.de/agb", "shipping": "https://www.tyresystem.de/lieferbedingungen"}
_RECORD_FIELDS = {"artikel_id", "ean", "hersteller_artikelnummer", "hersteller", "profil", "material", "bestand", "einkaufspreis"}
_MESSAGES = {"feed_invalid": "Der Felgenexport ist nicht vollständig prüfbar.",
             "configuration": "Der Felgenkatalog ist noch nicht freigegeben oder vollständig eingerichtet.",
             "cache_missing": "Der Felgenkatalog wurde noch nicht bereitgestellt.",
             "cache_invalid": "Der Felgenkatalog ist nicht vollständig prüfbar.",
             "cache_stale": "Die Felgenpreise sind nicht mehr aktuell.",
             "write": "Der Felgenkatalog konnte nicht sicher aktualisiert werden."}


class FelgenKatalogError(ValueError):
    def __init__(self, code):
        self.code = code if code in _MESSAGES else "cache_invalid"
        super().__init__(_MESSAGES[self.code])


class FelgenEingabefehler(ValueError):
    def __init__(self, field):
        self.feld = field if field in {"suche", "hersteller", "material", "anzahl", "sortierung", "seite"} else None
        self.code = self.feld + "_ungueltig" if self.feld else "suchfelder_ungueltig"
        super().__init__("Bitte die Felgensucheingaben prüfen.")


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError()
    return value.astimezone(timezone.utc)


def _text(value, maximum, *, optional=False):
    if not isinstance(value, str) or len(value) > maximum or any(unicodedata.category(c).startswith("C") for c in value):
        raise ValueError()
    value = value.strip()
    if not value and not optional:
        raise ValueError()
    return value


def _number(value, maximum=1000000):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,7}", value) or int(value) > maximum:
        raise ValueError()
    return int(value)


@dataclass(frozen=True, slots=True, repr=False)
class FelgenArtikel:
    artikel_id: str
    ean: str | None
    hersteller_artikelnummer: str | None
    hersteller: str
    profil: str
    material: str
    gemeldeter_bestand: int
    einkaufspreis: Decimal
    abgerufen_am: datetime

    def __repr__(self):
        return f"<FelgenArtikel artikel_id={self.artikel_id} private_price=redacted>"


@dataclass(frozen=True, slots=True, repr=False)
class FelgenSnapshot:
    artikel: tuple[FelgenArtikel, ...]
    abgerufen_am: datetime
    sha256: str
    statistik: Mapping

    def __repr__(self):
        return f"<FelgenSnapshot articles={len(self.artikel)} private_prices=redacted>"


def _rim(row, received):
    if (row["Artikeltyp"] != "Felge" or row["Artikeluntertyp"] not in {"Alu", "Stahl"}
            or row["Fahrzeugtyp"] not in {"", "PKW"} or row["Demo"] or row["Da"]):
        return None
    if not _SKU.fullmatch(row["Artikelnummer"]):
        raise ValueError()
    ean = row["ean"]
    if ean and not re.fullmatch(r"(?:[0-9]{8}|[0-9]{12,14})", ean):
        raise ValueError()
    price = row["Einkaufspreis"]
    if not re.fullmatch(r"[0-9]{1,7},[0-9]{2,4}", price):
        raise ValueError()
    price = Decimal(price.replace(",", "."))
    if not Decimal("0.0001") <= price <= Decimal("1000000"):
        raise ValueError()
    manufacturer, profile = _text(row["Hersteller"], 100), _text(row["Profil"], 200)
    if len(manufacturer + " " + profile) > 200:
        raise ValueError()
    mpn = _text(row["Herstellerartikelnummer"], 100, optional=True)
    return FelgenArtikel(row["Artikelnummer"], ean or None, mpn or None, manufacturer, profile,
                         {"Alu": "alu", "Stahl": "stahl"}[row["Artikeluntertyp"]],
                         _number(row["Lagerbestand"]), price, received)


def parse_felgen_feed(raw, *, abgerufen_am):
    """Parse one complete export, quarantining conflicting IDs across all types."""
    try:
        received = _utc(abgerufen_am)
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_BYTES:
            raise ValueError()
        reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), delimiter=";", strict=True)
        if tuple(next(reader)) != HEADERS:
            raise ValueError()
        seen, blocked, articles, stats = {}, set(), {}, Counter()
        for row in reader:
            if not row:
                continue
            stats["rows_read"] += 1
            if stats["rows_read"] > MAX_ROWS:
                raise ValueError()
            sku = row[0] if row and _SKU.fullmatch(row[0]) else None
            if sku:
                fingerprint = hashlib.sha256(json.dumps(row, ensure_ascii=False).encode()).digest()
                if sku in blocked:
                    continue
                if sku in seen:
                    if seen[sku] == fingerprint:
                        stats["duplicate_rows"] += 1
                    else:
                        stats["conflicting_ids"] += 1
                        blocked.add(sku)
                        articles.pop(sku, None)
                    continue
                seen[sku] = fingerprint
            if len(row) != len(HEADERS) or any(len(value) > MAX_FIELD_CHARS for value in row):
                stats["invalid_rows"] += 1
                continue
            try:
                article = _rim(dict(zip(HEADERS, row)), received)
                if article is None:
                    stats["ignored_categories"] += 1
                else:
                    articles[article.artikel_id] = article
            except (ValueError, TypeError, ArithmeticError):
                stats["invalid_rows"] += 1
        ordered = tuple(articles[key] for key in sorted(articles, key=int))
        stats["accepted_rims"] = len(ordered)
        return FelgenSnapshot(ordered, received, hashlib.sha256(raw).hexdigest(), MappingProxyType(dict(stats)))
    except Exception:
        raise FelgenKatalogError("feed_invalid") from None


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


def _payload(snapshot):
    if (not isinstance(snapshot, FelgenSnapshot) or not isinstance(snapshot.artikel, tuple)
            or any(not isinstance(a, FelgenArtikel) or a.abgerufen_am != snapshot.abgerufen_am for a in snapshot.artikel)):
        raise ValueError()
    return {"schema": CACHE_SCHEMA, "received_at": _utc(snapshot.abgerufen_am).isoformat(), "source_sha256": snapshot.sha256,
            "articles": [{"artikel_id": a.artikel_id, "ean": a.ean, "hersteller_artikelnummer": a.hersteller_artikelnummer,
                "hersteller": a.hersteller, "profil": a.profil, "material": a.material, "bestand": a.gemeldeter_bestand,
                "einkaufspreis": format(a.einkaufspreis, "f")} for a in snapshot.artikel]}


def _decode(payload):
    if (not isinstance(payload, dict) or set(payload) != {"schema", "received_at", "source_sha256", "articles"}
            or type(payload["schema"]) is not int or payload["schema"] != CACHE_SCHEMA
            or not isinstance(payload["received_at"], str) or len(payload["received_at"]) > 40
            or not isinstance(payload["source_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", payload["source_sha256"])
            or not isinstance(payload["articles"], list) or len(payload["articles"]) > MAX_ROWS):
        raise ValueError()
    received = _utc(datetime.fromisoformat(payload["received_at"]))
    articles, seen = [], set()
    for item in payload["articles"]:
        if (not isinstance(item, dict) or set(item) != _RECORD_FIELDS or not isinstance(item["artikel_id"], str)
                or not _SKU.fullmatch(item["artikel_id"]) or item["artikel_id"] in seen
                or item["material"] not in ("alu", "stahl") or type(item["bestand"]) is not int
                or not 0 <= item["bestand"] <= 1000000 or not isinstance(item["einkaufspreis"], str)
                or not re.fullmatch(r"[0-9]{1,7}\.[0-9]{2,4}", item["einkaufspreis"])
                or (item["ean"] is not None and not isinstance(item["ean"], str))
                or (item["hersteller_artikelnummer"] is not None and not isinstance(item["hersteller_artikelnummer"], str))):
            raise ValueError()
        row = dict.fromkeys(HEADERS, "")
        row.update(Artikelnummer=item["artikel_id"], Artikeltyp="Felge", Artikeluntertyp={"alu": "Alu", "stahl": "Stahl"}[item["material"]],
            Lagerbestand=str(item["bestand"]), Einkaufspreis=item["einkaufspreis"].replace(".", ","),
            ean=item["ean"] or "", Herstellerartikelnummer=item["hersteller_artikelnummer"] or "", Hersteller=item["hersteller"], Profil=item["profil"])
        article = _rim(row, received)
        if (article is None or article.hersteller != item["hersteller"] or article.profil != item["profil"]
                or article.ean != item["ean"] or article.hersteller_artikelnummer != item["hersteller_artikelnummer"]):
            raise ValueError()
        seen.add(article.artikel_id)
        articles.append(article)
    return FelgenSnapshot(tuple(sorted(articles, key=lambda a: int(a.artikel_id))), received, payload["source_sha256"],
                          MappingProxyType({"accepted_rims": len(articles)}))


class FelgenCatalogCache:
    def __init__(self, directory, *, private_root, clock=None):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._memory = self._signature = None
        try:
            for value in (directory, private_root):
                text = str(value).replace("\\", "/")
                if text.startswith("//") or "://" in text or not Path(value).is_absolute():
                    raise ValueError()
            self.private_root, self.directory = Path(private_root).resolve(), Path(directory).resolve()
            if self.directory == self.private_root or not self.directory.is_relative_to(self.private_root):
                raise ValueError()
            if any(part.lower() in {"static", "public", "templates", "outputs", "wwwroot"} for part in self.directory.parts):
                raise ValueError()
            repository = Path(__file__).resolve().parent
            if self.directory.is_relative_to(repository) and not self.directory.is_relative_to(repository / "data"):
                raise ValueError()
            self.path = self.directory / "felgen-catalog.json"
        except Exception:
            raise FelgenKatalogError("configuration") from None

    def __repr__(self):
        return "<FelgenCatalogCache private_path=redacted>"

    def _guard(self):
        if (self.directory.resolve() != self.directory or self.private_root.resolve() != self.private_root
                or self.path.is_symlink() or self.path.resolve().parent != self.directory):
            raise ValueError()

    def load(self):
        with self._lock:
            try:
                self._guard()
                info = self.path.stat()
                signature = (info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                if signature == self._signature and self._memory is not None:
                    return self._memory
                with self.path.open("rb") as stream:
                    raw = stream.read(MAX_BYTES + 1)
                if len(raw) > MAX_BYTES:
                    raise ValueError()
                value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
                if (not isinstance(value, dict) or set(value) != {"payload", "sha256"}
                        or hashlib.sha256(_canonical(value["payload"])).hexdigest() != value["sha256"]):
                    raise ValueError()
                snapshot = _decode(value["payload"])
                if snapshot.abgerufen_am > _utc(self._clock()) + timedelta(seconds=30):
                    raise ValueError()
                self._memory, self._signature = snapshot, signature
                return snapshot
            except Exception as exc:
                self._memory = self._signature = None
                raise FelgenKatalogError("cache_missing" if isinstance(exc, FileNotFoundError) else "cache_invalid") from None

    def replace(self, snapshot):
        temporary, acquired, lock_path = None, False, self.directory / ".felgen-write.lock"
        with self._lock:
            try:
                self._guard()
                payload = _payload(snapshot)
                verified = _decode(payload)
                if not _utc(self._clock()) - MAX_AGE < verified.abgerufen_am <= _utc(self._clock()) + timedelta(seconds=30):
                    raise ValueError()
                self.directory.mkdir(parents=True, exist_ok=True)
                descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                acquired = True
                os.close(descriptor)
                try:
                    previous = self.load()
                except FelgenKatalogError:
                    previous = None
                if previous is not None and verified.abgerufen_am < previous.abgerufen_am:
                    raise ValueError()
                raw = _canonical({"payload": payload, "sha256": hashlib.sha256(_canonical(payload)).hexdigest()})
                if len(raw) > MAX_BYTES:
                    raise ValueError()
                descriptor, temporary = tempfile.mkstemp(prefix=".felgen-", suffix=".tmp", dir=self.directory)
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path)
                temporary = None
                self._memory, self._signature = verified, None
            except Exception:
                raise FelgenKatalogError("write") from None
            finally:
                if temporary is not None:
                    try:
                        Path(temporary).unlink()
                    except OSError:
                        pass
                if acquired:
                    try:
                        lock_path.unlink()
                    except OSError:
                        pass


def parse_felgen_suche(args):
    fields = {"suche", "hersteller", "material", "anzahl", "sortierung", "seite"}
    if not isinstance(args, Mapping) or set(args) - fields or (hasattr(args, "getlist") and any(len(args.getlist(key)) != 1 for key in args)):
        raise FelgenEingabefehler(None)
    result = {}
    for key, maximum in (("suche", 120), ("hersteller", 100)):
        try:
            result[key] = _text(args.get(key, ""), maximum, optional=True)
            if "<" in result[key] or ">" in result[key]:
                raise ValueError()
        except (ValueError, TypeError):
            raise FelgenEingabefehler(key) from None
    result["material"] = args.get("material", "alle")
    result["sortierung"] = args.get("sortierung", "preis_auf")
    for key, allowed in (("material", ("alle", "alu", "stahl")), ("sortierung", ("preis_auf", "preis_ab", "name"))):
        if result[key] not in allowed:
            raise FelgenEingabefehler(key)
    for key, default, maximum in (("anzahl", "4", 8), ("seite", "1", 10000)):
        value = args.get(key, default)
        if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,4}", value) or int(value) > maximum:
            raise FelgenEingabefehler(key)
        result[key] = int(value)
    return result


def _policy(value):
    try:
        if (not isinstance(value, Mapping) or set(value) != {"currency", "price_basis", "single_item_shipping_net", "free_shipping_from", "verified_on", "sources"}
                or value["currency"] != "EUR" or value["price_basis"] != "net_unit"
                or value["single_item_shipping_net"] != "6.00" or type(value["free_shipping_from"]) is not int
                or value["free_shipping_from"] != 2 or value["verified_on"] != "2026-11-03" or value["sources"] != _SOURCES):
            return None
        return MappingProxyType(dict(value))
    except Exception:
        return None


class FelgenCatalogProvider:
    def __init__(self, cache, *, policy=None, publication_approved=False, publication_reference="", private_view_authorized=None, clock=None):
        self.cache, self._policy = cache, _policy(policy)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._authorize = private_view_authorized if callable(private_view_authorized) else None
        try:
            reference = _text(publication_reference, 200)
        except (ValueError, TypeError):
            reference = ""
        self._public = publication_approved is True and bool(reference)

    def __repr__(self):
        return "<FelgenCatalogProvider private_prices=redacted>"

    @property
    def publication_ready(self):
        return self._public and self._policy is not None

    def _snapshot(self):
        permitted = self._public
        if not permitted and self._authorize is not None:
            try:
                permitted = self._authorize() is True
            except Exception:
                pass
        if not permitted or self._policy is None:
            raise FelgenKatalogError("configuration")
        try:
            snapshot = self.cache.load()
            if not isinstance(snapshot, FelgenSnapshot):
                raise FelgenKatalogError("cache_invalid")
            if not _utc(self._clock()) - MAX_AGE < snapshot.abgerufen_am <= _utc(self._clock()) + timedelta(seconds=30):
                raise FelgenKatalogError("cache_stale")
            return snapshot
        except FelgenKatalogError:
            raise
        except Exception:
            raise FelgenKatalogError("cache_invalid") from None

    def _calculation(self, article, quantity):
        if article.gemeldeter_bestand < quantity:
            return None
        try:
            return berechne_reifenpreis(einkauf_netto=article.einkaufspreis, menge=quantity,
                bezugskosten_netto="0.00" if quantity >= 2 else "6.00", preise_bestaetigt=True)
        except (ValueError, TypeError, ArithmeticError):
            return None

    def _product(self, article, quantity):
        calculation = self._calculation(article, quantity)
        if calculation is None:
            return None
        return {"artikel_id": article.artikel_id, "ean": article.ean, "hersteller_artikelnummer": article.hersteller_artikelnummer,
                "hersteller": article.hersteller, "profil": article.profil, "bezeichnung": article.hersteller + " " + article.profil,
                "material": article.material, "bestand": article.gemeldeter_bestand, "anzahl": quantity,
                "preisstand": article.abgerufen_am.isoformat(), "gueltig_bis": (article.abgerufen_am + MAX_AGE).isoformat(),
                "kennungspassung": "nicht_geprueft", "preis": {"einzel_brutto": calculation["einzelpreis_brutto_reifen"],
                    "gesamt_felgen_brutto": calculation["gesamt_brutto_reifen"], "versand_brutto": calculation["versand_brutto"],
                    "gesamt_brutto": calculation["gesamt_brutto"], "waehrung": "EUR"}}

    def search(self, args):
        query = parse_felgen_suche(args)
        snapshot = self._snapshot()
        needle = query["suche"].casefold()
        exact = {a.artikel_id for a in snapshot.artikel if needle and needle in
                 {a.artikel_id.casefold(), (a.ean or "").casefold(), (a.hersteller_artikelnummer or "").casefold()}}
        candidates, totals = [], {}
        for article in snapshot.artikel:
            if (article.gemeldeter_bestand < query["anzahl"]
                    or query["hersteller"] and article.hersteller != query["hersteller"]
                    or query["material"] != "alle" and article.material != query["material"]):
                continue
            if exact and article.artikel_id not in exact:
                continue
            if needle and not exact:
                haystack = (article.hersteller + " " + article.profil).casefold()
                if not all(part in haystack for part in needle.split()):
                    continue
            # Validate each distinct price with the authoritative calculator,
            # keeping only a scalar key rather than 60k public dictionaries.
            if article.einkaufspreis not in totals:
                calculation = self._calculation(article, query["anzahl"])
                totals[article.einkaufspreis] = Decimal(calculation["gesamt_brutto"]) if calculation is not None else None
            if totals[article.einkaufspreis] is not None:
                candidates.append(article)
        sort = query["sortierung"]
        if sort == "name":
            candidates.sort(key=lambda a: ((a.hersteller + " " + a.profil).casefold(), int(a.artikel_id)))
        else:
            candidates.sort(key=lambda a: (totals[a.einkaufspreis] * (-1 if sort == "preis_ab" else 1), int(a.artikel_id)))
        total, page = len(candidates), query["seite"]
        products = [self._product(article, query["anzahl"]) for article in candidates[(page-1)*PAGE_SIZE:page*PAGE_SIZE]]
        return {"status": "ok" if products else "keine_treffer", "code": "ok" if products else "keine_treffer",
                "verfuegbar": bool(products), "produkte": products, "treffer_gesamt": total, "seite": page,
                "seiten": (total+PAGE_SIZE-1)//PAGE_SIZE}

    __call__ = search

    def lookup(self, artikel_id, anzahl=4):
        if not isinstance(artikel_id, str) or not _SKU.fullmatch(artikel_id):
            raise FelgenEingabefehler(None)
        if type(anzahl) is not int or not 1 <= anzahl <= 8:
            raise FelgenEingabefehler("anzahl")
        snapshot = self._snapshot()
        article = next((a for a in snapshot.artikel if a.artikel_id == artikel_id), None)
        return self._product(article, anzahl) if article is not None else None

    def facets(self):
        snapshot = self._snapshot()
        articles = [a for a in snapshot.artikel if a.gemeldeter_bestand >= 1]
        return {"hersteller": sorted({a.hersteller for a in articles}, key=lambda s: (s.casefold(), s)),
                "materialien": sorted({a.material for a in articles}), "artikel_gesamt": len(articles),
                "preisstand": snapshot.abgerufen_am.isoformat(), "gueltig_bis": (snapshot.abgerufen_am+MAX_AGE).isoformat()}
