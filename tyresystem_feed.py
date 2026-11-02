"""Local, internal-only reader for the observed TYREsystem CSV export.

No network, application wiring, price release, stock reservation or order API.
``Einkaufspreis`` is preserved as Decimal; tax, currency and shipping are not
inferred. ``abgerufen_am`` is a caller-supplied local receipt time, not a supplier
generation timestamp or price-validity promise. Real exports contain private EK.
"""
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import csv
import hashlib
import io
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit


HEADERS = (
    "Artikelnummer", "ean", "Lagerbestand", "Einkaufspreis", "PlusFakt GuID",
    "Hersteller", "Profil", "Herstellerartikelnummer", "Reifenbreite",
    "Reifenquerschnitt", "Reifendurchmesser", "Loadindex", "Speedindex", "Saison",
    "Fahrzeugtyp", "Bauart", "LabelKraftstoffeffizienz", "LabelNasshaftung",
    "LabelRollgeraeusch", "LabelRollgeraeuschklasse", "LabelFahrzeugklasse", "Dot",
    "Demo", "Da", "Artikeltyp", "Artikeluntertyp", "EU Direktive", "EPREL-URL",
)
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 300_000
MAX_FIELD_CHARS = 2048
_SPEED_KMH = {"L": 120, "M": 130, "N": 140, "P": 150, "Q": 160,
              "R": 170, "S": 180, "T": 190, "U": 200, "H": 210,
              "V": 240, "W": 270, "Y": 300}
_SEASONS = {"s": "sommer", "w": "winter", "g": "ganzjahr"}
_CONTROL = re.compile(r"[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]")


class FeedError(ValueError):
    """A safe structural error; never includes raw rows, paths or URLs."""


class _InvalidRow(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class FeedArticle:
    artikel_id: str
    ean: str | None
    hersteller: str
    profil: str
    hersteller_artikelnummer: str | None
    breite: int
    querschnitt: int
    zoll: Decimal
    bauart: str
    lastindex: int
    speedindex: str
    saison: str
    gemeldeter_bestand: int
    einkaufspreis: Decimal
    dot_hinweis: str | None
    demo: bool
    da: bool
    eprel_url: str | None
    label_kraftstoff: str | None
    label_nass: str | None
    label_geraeusch_db: str | None
    label_geraeuschklasse: str | None
    xl_bestaetigt: None = field(default=None, init=False)
    runflat_bestaetigt: None = field(default=None, init=False)
    schneeflocke_bestaetigt: None = field(default=None, init=False)
    fracht_netto: None = field(default=None, init=False)
    waehrung_bestaetigt: None = field(default=None, init=False)
    steuerbasis_bestaetigt: None = field(default=None, init=False)
    lieferzusage: None = field(default=None, init=False)
    public_ready: bool = field(default=False, init=False)

    @property
    def sonderware(self):
        return bool(self.dot_hinweis or self.demo or self.da)

    def internal_dict(self):
        """Contains confidential purchase data; never serialize to public routes."""
        return {name: (format(value, "f") if isinstance(value, Decimal) else value)
                for name in self.__dataclass_fields__
                for value in (getattr(self, name),)}


@dataclass(frozen=True, slots=True)
class FeedSnapshot:
    artikel: tuple[FeedArticle, ...]
    abgerufen_am: datetime
    sha256: str
    statistik: Mapping[str, int]
    fehlerkategorien: Mapping[str, int]
    erzeugt_am_lieferant: None = field(default=None, init=False)
    public_ready: bool = field(default=False, init=False)

    def query(self, *, breite, querschnitt, zoll, lastindex, speedindex,
              saison, menge=4, limit=12, sonderware=False):
        """Internal candidates only, sorted by observed EK then article ID.

        LI/SI are minimum requirements, dimensions and season exact. No fitting
        approval follows from these filters. Reported stock is not a hold.
        Known DOT/Demo/Da products are excluded by default; blank DOT does not
        prove age or a current DOT. Unconfirmed profile flags stay unconfirmed.
        """
        width = _query_int(breite, 100, 455)
        aspect = _query_int(querschnitt, 15, 100)
        load = _query_int(lastindex, 30, 150)
        amount = _query_int(menge, 1, 1000)
        count = _query_int(limit, 1, 100)
        try:
            diameter = _decimal_plain(zoll, "query", "10", "30")
        except _InvalidRow:
            raise FeedError("Ungültige Suchparameter.") from None
        if (not isinstance(speedindex, str) or speedindex not in _SPEED_KMH
                or saison not in _SEASONS.values() or type(sonderware) is not bool):
            raise FeedError("Ungültige Suchparameter.")
        matches = (a for a in self.artikel
                   if a.breite == width and a.querschnitt == aspect
                   and a.zoll == diameter and a.lastindex >= load
                   and _SPEED_KMH[a.speedindex] >= _SPEED_KMH[speedindex]
                   and a.saison == saison and a.gemeldeter_bestand >= amount
                   and (sonderware or not a.sonderware))
        return tuple(sorted(matches, key=lambda a: (a.einkaufspreis, int(a.artikel_id)))[:count])


def _text(value, *, required=False):
    if not isinstance(value, str) or len(value) > MAX_FIELD_CHARS or _CONTROL.search(value):
        raise _InvalidRow("text")
    result = value.strip()
    if required and not result:
        raise _InvalidRow("text")
    return result


def _integer(value, name, minimum, maximum):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,18}", value):
        raise _InvalidRow(name)
    number = int(value)
    if not minimum <= number <= maximum:
        raise _InvalidRow(name)
    return number


def _decimal_plain(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise _InvalidRow(name)
    text = str(value)
    if not re.fullmatch(r"[0-9]{1,8}(?:\.[0-9]{1,4})?", text):
        raise _InvalidRow(name)
    try:
        number = Decimal(text)
    except InvalidOperation:
        raise _InvalidRow(name) from None
    if not Decimal(minimum) <= number <= Decimal(maximum):
        raise _InvalidRow(name)
    return number


def _query_int(value, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise FeedError("Ungültige Suchparameter.")
    try:
        return _integer(str(value), "query", minimum, maximum)
    except _InvalidRow:
        raise FeedError("Ungültige Suchparameter.") from None


def _eprel(value):
    if not value:
        return None
    # Metadata only; this module never follows even an accepted URL.
    try:
        url = urlsplit(value)
        if (url.scheme == "https" and url.hostname == "eprel.ec.europa.eu"
                and not url.username and not url.password and url.port in (None, 443)
                and not url.query and not url.fragment and url.path.startswith("/")):
            return value
    except ValueError:
        pass
    return None


def _article(row):
    article_id = row["Artikelnummer"]
    _integer(article_id, "artikel_id", 1, 10**18 - 1)
    ean = row["ean"] or None
    if ean and not re.fullmatch(r"(?:[0-9]{8}|[0-9]{12,14})", ean):
        raise _InvalidRow("ean")
    if row["Bauart"] != "R":
        raise _InvalidRow("bauart")
    # Do not guess semantics for compound (Z), dual-load or inch-size formats.
    width = _integer(row["Reifenbreite"], "abmessungen", 100, 455)
    aspect = _integer(row["Reifenquerschnitt"], "abmessungen", 15, 100)
    diameter = _decimal_plain(row["Reifendurchmesser"], "abmessungen", "10", "30")
    load = _integer(row["Loadindex"], "lastindex", 30, 150)
    speed = row["Speedindex"]
    season = _SEASONS.get(row["Saison"])
    if speed not in _SPEED_KMH:
        raise _InvalidRow("speedindex")
    if season is None:
        raise _InvalidRow("saison")
    stock = _integer(row["Lagerbestand"], "bestand", 0, 1_000_000)
    price = row["Einkaufspreis"]
    if not re.fullmatch(r"[0-9]{1,8},[0-9]{2,4}", price):
        raise _InvalidRow("preis")
    price = _decimal_plain(price.replace(",", "."), "preis", "0.0001", "1000000")
    if row["Demo"] not in ("", "Demo") or row["Da"] not in ("", "Da"):
        raise _InvalidRow("sonderware")
    dot = row["Dot"] or None
    if dot and not re.fullmatch(r"DOT[0-9]{2,4}", dot):
        raise _InvalidRow("sonderware")
    return FeedArticle(
        artikel_id=article_id, ean=ean, hersteller=_text(row["Hersteller"], required=True),
        profil=_text(row["Profil"], required=True),
        hersteller_artikelnummer=row["Herstellerartikelnummer"] or None,
        breite=width, querschnitt=aspect, zoll=diameter, bauart="R", lastindex=load,
        speedindex=speed, saison=season, gemeldeter_bestand=stock,
        einkaufspreis=price, dot_hinweis=dot, demo=bool(row["Demo"]), da=bool(row["Da"]),
        eprel_url=_eprel(row["EPREL-URL"]),
        label_kraftstoff=row["LabelKraftstoffeffizienz"] or None,
        label_nass=row["LabelNasshaftung"] or None,
        label_geraeusch_db=row["LabelRollgeraeusch"] or None,
        label_geraeuschklasse=row["LabelRollgeraeuschklasse"] or None,
    )


def parse_feed(data, *, abgerufen_am):
    """Parse bounded UTF-8 CSV bytes with the exact observed 28-column header.

    Bad individual PKW rows are skipped and counted by fixed, non-sensitive
    categories. Conflicting repeated IDs quarantine all rows for that ID;
    identical repeats are counted once and never overwrite the first record.
    Structural CSV/header/encoding/resource errors reject the entire snapshot.
    """
    if (not isinstance(data, bytes) or len(data) > MAX_BYTES
            or not isinstance(abgerufen_am, datetime) or abgerufen_am.tzinfo is None
            or abgerufen_am.utcoffset() is None):
        raise FeedError("Ungültige Feeddatei oder fehlende Abrufzeit.")
    try:
        text = data.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError:
        raise FeedError("Feeddatei ist kein gültiges UTF-8.") from None
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=";", strict=True)
    stats = Counter(rows_read=0, ignored_non_pkw=0, invalid_rows=0, duplicate_rows=0,
                    conflicting_ids=0)
    errors = Counter()
    articles = {}
    seen = {}
    conflicted = set()
    try:
        if tuple(next(reader, ())) != HEADERS:
            raise FeedError("Unerwarteter CSV-Spaltenaufbau.")
        for cells in reader:
            if not cells:
                continue
            stats["rows_read"] += 1
            if stats["rows_read"] > MAX_ROWS:
                raise FeedError("Feeddatei überschreitet die Zeilengrenze.")
            if len(cells) != len(HEADERS):
                stats["invalid_rows"] += 1
                errors["spaltenzahl"] += 1
                continue
            try:
                identity = cells[0].strip()
                if not re.fullmatch(r"[1-9][0-9]{0,17}", identity):
                    raise _InvalidRow("artikel_id")
                # Compare the entire normalized row, not only fields retained
                # by this initial adapter, across product types too. Keep only
                # a fixed-size fingerprint rather than a second private row.
                fingerprint = hashlib.sha256(repr(tuple(c.strip() for c in cells)).encode("utf-8")).digest()
                if identity in seen:
                    stats["duplicate_rows"] += 1
                    if seen[identity] != fingerprint and identity not in conflicted:
                        conflicted.add(identity)
                        stats["conflicting_ids"] += 1
                        articles.pop(identity, None)
                    continue
                seen[identity] = fingerprint
                row = dict(zip(HEADERS, (_text(c) for c in cells)))
                if row["Artikeltyp"] != "Reifen" or row["Fahrzeugtyp"] != "PKW" or row["Artikeluntertyp"] != "PKW":
                    stats["ignored_non_pkw"] += 1
                    continue
                article = _article(row)
            except _InvalidRow as exc:
                stats["invalid_rows"] += 1
                errors[str(exc)] += 1
                continue
            if identity not in conflicted:
                articles[identity] = article
    except csv.Error:
        raise FeedError("Ungültige CSV-Struktur oder zu großes CSV-Feld.") from None
    stats["articles"] = len(articles)
    return FeedSnapshot(
        artikel=tuple(articles[key] for key in sorted(articles, key=int)),
        abgerufen_am=abgerufen_am.astimezone(timezone.utc),
        sha256=hashlib.sha256(data).hexdigest(),
        statistik=MappingProxyType(dict(stats)), fehlerkategorien=MappingProxyType(dict(errors)),
    )


def read_feed(path, *, abgerufen_am):
    """Read a local file only, bounded before decoding. No URL support."""
    if not isinstance(path, (str, Path)):
        raise FeedError("Lokale Feeddatei erforderlich.")
    path_text = str(path)
    scheme = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", path_text)
    if (path_text.replace("\\", "/").startswith("//") or "://" in path_text
            or (scheme and not re.match(r"^[A-Za-z]:[\\/]", path_text))):
        raise FeedError("Lokale Feeddatei erforderlich.")
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(MAX_BYTES + 1)
    except (OSError, ValueError):
        raise FeedError("Feeddatei konnte nicht gelesen werden.") from None
    return parse_feed(data, abgerufen_am=abgerufen_am)


def load_feed(path_or_bytes, *, abgerufen_am):
    """Convenience entry point for a local path or already received bytes."""
    if isinstance(path_or_bytes, bytes):
        return parse_feed(path_or_bytes, abgerufen_am=abgerufen_am)
    return read_feed(path_or_bytes, abgerufen_am=abgerufen_am)
