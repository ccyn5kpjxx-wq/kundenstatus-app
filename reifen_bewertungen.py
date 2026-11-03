"""Explicitly sourced ADAC test grades; no customer stars, network or price IO.

The private store reads its bounded ledger only at construction. Every lookup
must match the current validated feed article exactly and remain authorized.
Historical test years are preserved; price freshness is enforced elsewhere.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import stat
import unicodedata
from urllib.parse import urlsplit

from tyresystem_feed import FeedArticle

MAX_LEDGER_BYTES = 4 * 1024 * 1024
MAX_RATINGS = 20000
PUBLIC_FIELDS = frozenset({"quelle", "note", "urteil", "jahr", "testgroesse", "quelle_url"})
_IDENTITY = ("hersteller", "profil", "breite", "querschnitt", "lastindex", "speedindex", "saison")
_SPEED = frozenset("LMNPQRSTUHVWY")
_SEASON_PATH = {"winter": "winterreifen", "sommer": "sommerreifen", "ganzjahr": "ganzjahresreifen"}


def _text(value, maximum):
    return (isinstance(value, str) and 1 <= len(value) <= maximum and value == value.strip()
            and not any(unicodedata.category(char).startswith("C") or char in "<>" for char in value))


def verdict(note):
    value = Decimal(note)
    return next(label for ceiling, label in (("1.5", "sehr gut"), ("2.5", "gut"),
        ("3.5", "befriedigend"), ("4.5", "ausreichend"), ("5.5", "mangelhaft")) if value <= Decimal(ceiling))


def public_test_rating(value, *, groesse=None, saison=None, current_year=None):
    """Return six scalar public fields, or None; never repair/guess a rating."""
    try:
        year = datetime.now(timezone.utc).year if current_year is None else current_year
        if (not isinstance(value, Mapping) or set(value) != PUBLIC_FIELDS or value["quelle"] != "ADAC"
                or not isinstance(value["note"], str) or not re.fullmatch(r"[0-5]\.[0-9]", value["note"])
                or not Decimal("0.6") <= Decimal(value["note"]) <= Decimal("5.5")
                or value["urteil"] != verdict(value["note"])
                or type(value["jahr"]) is not int or not 2000 <= value["jahr"] <= year
                or not _text(value["testgroesse"], 40) or not _text(value["quelle_url"], 500)):
            return None
        size = re.fullmatch(r"([1-5][0-9]{2})/([1-9][0-9]) R([1-3][0-9]) ([0-9]{2,3})([LMNPQRSTUHVWY])", value["testgroesse"])
        if not size or (groesse is not None and value["testgroesse"] != groesse):
            return None
        parsed = urlsplit(value["quelle_url"])
        path = re.fullmatch(r"/rund-ums-fahrzeug/ausstattung-technik-zubehoer/reifen/reifentest/"
            r"(winterreifen|sommerreifen|ganzjahresreifen)/([0-9]{3}-[0-9]{2}-r[0-9]{2})/"
            r"[a-z0-9-]+-id-[0-9]{1,8}/", parsed.path)
        if (parsed.scheme != "https" or parsed.netloc != "www.adac.de" or parsed.query or parsed.fragment
                or not path or path[2] != f"{size[1]}-{size[2]}-r{size[3]}"
                or (saison is not None and path[1] != _SEASON_PATH.get(saison))):
            return None
        return {key: value[key] for key in ("quelle", "note", "urteil", "jahr", "testgroesse", "quelle_url")}
    except Exception:
        return None


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


class ReifenBewertungsStore:
    def __init__(self, path, *, private_view_authorized=None, publication_approved=False,
                 publication_reference="", clock=None):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._authorize = private_view_authorized if callable(private_view_authorized) else None
        self._public = publication_approved is True and _text(publication_reference, 200)
        self._ratings = {}
        try:
            candidate = Path(path)
            resolved = candidate.resolve()
            if (not candidate.is_absolute() or str(candidate).replace("\\", "/").startswith("//")
                    or candidate.is_symlink() or candidate != resolved
                    or any(part.lower() in {"static", "public", "templates", "outputs", "wwwroot"} for part in resolved.parts)):
                raise ValueError()
            repository = Path(__file__).resolve().parent
            if resolved.is_relative_to(repository) and not resolved.is_relative_to(repository / "data"):
                raise ValueError()
            before = candidate.stat(follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= MAX_LEDGER_BYTES:
                raise ValueError()
            with candidate.open("rb") as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino, opened.st_size) != (before.st_dev, before.st_ino, before.st_size):
                    raise ValueError()
                raw = stream.read(MAX_LEDGER_BYTES + 1)
            if len(raw) > MAX_LEDGER_BYTES or candidate.is_symlink() or candidate.resolve() != resolved:
                raise ValueError()
            ledger = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
            if (not isinstance(ledger, dict) or set(ledger) != {"schema", "ratings"}
                    or type(ledger["schema"]) is not int or ledger["schema"] != 1
                    or not isinstance(ledger["ratings"], list) or len(ledger["ratings"]) > MAX_RATINGS):
                raise ValueError()
            records, ambiguous = {}, set()
            allowed = PUBLIC_FIELDS | set(_IDENTITY) | {"artikel_id", "zoll", "vergleichsgruppe", "nachweis"}
            for row in ledger["ratings"]:
                if (not isinstance(row, dict) or set(row) != allowed
                        or not isinstance(row["artikel_id"], str) or not re.fullmatch(r"[1-9][0-9]{0,17}", row["artikel_id"])
                        or not _text(row["hersteller"], 100) or not _text(row["profil"], 200)
                        or any(type(row[key]) is not int for key in ("breite", "querschnitt", "lastindex"))
                        or not 100 <= row["breite"] <= 500 or not 10 <= row["querschnitt"] <= 95
                        or not 60 <= row["lastindex"] <= 150 or row["speedindex"] not in _SPEED
                        or row["saison"] not in _SEASON_PATH or not isinstance(row["zoll"], str)
                        or not re.fullmatch(r"[1-3][0-9]", row["zoll"])
                        or not _text(row["vergleichsgruppe"], 100) or not _text(row["nachweis"], 200)):
                    continue
                expected_size = f"{row['breite']}/{row['querschnitt']} R{row['zoll']} {row['lastindex']}{row['speedindex']}"
                description = public_test_rating({key: row[key] for key in PUBLIC_FIELDS},
                    groesse=expected_size, saison=row["saison"], current_year=self._clock().year)
                if description is None:
                    continue
                sku = row["artikel_id"]
                if sku in records or sku in ambiguous:
                    records.pop(sku, None)
                    ambiguous.add(sku)
                else:
                    records[sku] = (tuple(row[key] for key in _IDENTITY), Decimal(row["zoll"]), description)
            self._ratings = records
        except Exception:
            self._ratings = {}

    def __repr__(self):
        return "<ReifenBewertungsStore private_ledger=redacted>"

    def describe(self, article):
        try:
            authorized = self._public or (self._authorize is not None and self._authorize() is True)
            if not authorized or not isinstance(article, FeedArticle):
                return None
            record = self._ratings.get(article.artikel_id)
            if record is None or tuple(getattr(article, key) for key in _IDENTITY) != record[0] or article.zoll != record[1]:
                return None
            return dict(record[2])
        except Exception:
            return None
