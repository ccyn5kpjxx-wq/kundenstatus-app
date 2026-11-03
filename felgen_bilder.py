"""Explicit local rim-only images bound to an exact current catalogue article.

No network, source image URL, image transformation or vehicle-fitment inference.
Only the constructor reads the bounded private manifest. Descriptions use its
immutable index; original asset bytes are checked on an explicit local read.
"""
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
from types import MappingProxyType
import unicodedata

from felgen_katalog import FelgenArtikel, FelgenSnapshot
from reifen_bilder import MAX_IMAGE_BYTES, validate_webp_bytes
from tyresystem_feed import MAX_ROWS


MAX_MANIFEST_BYTES = 32 * 1024 * 1024
_SKU = re.compile(r"[1-9][0-9]{0,17}")
_SHA = re.compile(r"[0-9a-f]{64}")
_EAN = re.compile(r"(?:[0-9]{8}|[0-9]{12,14})")
_URL = re.compile(r"/api/felgenkatalog/bild/([1-9][0-9]{0,17})/([0-9a-f]{64})\.webp")


def _text(value, maximum):
    return (isinstance(value, str) and 1 <= len(value) <= maximum and value == value.strip()
            and not any(unicodedata.category(char).startswith("C") for char in value))


def public_felgen_image_metadata(value, artikel_id):
    """Whitelist one article-bound local image, never supplier fields or URLs."""
    if not isinstance(value, Mapping) or set(value) != {"url", "alt", "bezug", "motiv"}:
        return None
    url = value.get("url")
    if (not isinstance(url, str) or not _text(value.get("alt"), 250)
            or value.get("bezug") != "artikel" or value.get("motiv") != "felge"):
        return None
    match = _URL.fullmatch(url)
    if not match or match[1] != artikel_id:
        return None
    return {"url": url, "alt": value["alt"], "bezug": "artikel", "motiv": "felge"}


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


def _identity(article):
    return (article.hersteller, article.profil, article.hersteller_artikelnummer, article.ean)


class FelgenBildStore:
    def __init__(self, root, *, catalog_source, private_view_authorized=None,
                 publication_approved=False, publication_reference=""):
        self._source = catalog_source if callable(catalog_source) else None
        self._authorize = private_view_authorized if callable(private_view_authorized) else None
        self._public = publication_approved is True and _text(publication_reference, 200)
        self._root_input = self._root = None
        self._entries = MappingProxyType({})
        self._index_catalog, self._index = None, {}
        self._lock = threading.Lock()
        try:
            text, candidate = str(root).replace("\\", "/"), Path(root)
            if text.startswith("//") or "://" in text or not candidate.is_absolute() or candidate.is_symlink():
                raise ValueError()
            resolved = candidate.resolve()
            if any(part.lower() in {"static", "public", "templates", "outputs", "wwwroot"} for part in resolved.parts):
                raise ValueError()
            repository = Path(__file__).resolve().parent
            if resolved.is_relative_to(repository) and not resolved.is_relative_to(repository / "data"):
                raise ValueError()
            self._root_input, self._root = candidate, resolved
            manifest = json.loads(self._read_file("manifest.json", MAX_MANIFEST_BYTES).decode("utf-8"), object_pairs_hook=_unique)
            if (not isinstance(manifest, dict) or set(manifest) != {"schema", "articles"}
                    or type(manifest["schema"]) is not int or manifest["schema"] != 1
                    or not isinstance(manifest["articles"], list) or len(manifest["articles"]) > MAX_ROWS):
                raise ValueError()
            entries, seen, blocked = {}, set(), set()
            fields = {"artikel_id", "hersteller", "profil", "hersteller_artikelnummer", "ean", "sha256", "bezug", "motiv", "nachweis"}
            for item in manifest["articles"]:
                if not isinstance(item, dict) or not isinstance(item.get("artikel_id"), str) or not _SKU.fullmatch(item["artikel_id"]):
                    continue
                sku = item["artikel_id"]
                if sku in seen:
                    entries.pop(sku, None)
                    blocked.add(sku)
                    continue
                seen.add(sku)
                if (sku in blocked or set(item) != fields or not _text(item["hersteller"], 100)
                        or not _text(item["profil"], 200) or not isinstance(item["sha256"], str)
                        or not _SHA.fullmatch(item["sha256"]) or item["bezug"] != "artikel" or item["motiv"] != "felge"
                        or not _text(item["nachweis"], 200) or "://" in item["nachweis"] or "?" in item["nachweis"]
                        or (item["hersteller_artikelnummer"] is not None and not _text(item["hersteller_artikelnummer"], 100))
                        or (item["ean"] is not None and (not isinstance(item["ean"], str) or not _EAN.fullmatch(item["ean"])))):
                    continue
                entries[sku] = ((item["hersteller"], item["profil"], item["hersteller_artikelnummer"], item["ean"]), item["sha256"])
            self._entries = MappingProxyType(entries)
        except Exception:
            # Optional images never make a valid rim catalogue unavailable.
            self._entries = MappingProxyType({})

    def __repr__(self):
        return "<FelgenBildStore private_assets=redacted>"

    def _authorized(self):
        if self._public:
            return True
        try:
            return self._authorize is not None and self._authorize() is True
        except Exception:
            return False

    def _read_file(self, name, maximum):
        if (self._root is None or self._root_input.is_symlink() or self._root_input.resolve() != self._root):
            raise ValueError()
        path = self._root / name
        if path.is_symlink() or path.resolve() != path or path.parent != self._root:
            raise ValueError()
        before = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= maximum:
            raise ValueError()
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if ((opened.st_dev, opened.st_ino, opened.st_size) != (before.st_dev, before.st_ino, before.st_size)
                    or path.is_symlink() or path.resolve() != path or self._root_input.resolve() != self._root):
                raise ValueError()
            value = stream.read(maximum + 1)
        if len(value) > maximum:
            raise ValueError()
        return value

    def _article(self, sku):
        if self._source is None:
            return None
        catalog = self._source()
        if not isinstance(catalog, FelgenSnapshot):
            return None
        with self._lock:
            if catalog is not self._index_catalog:
                self._index = {a.artikel_id: a for a in catalog.artikel if isinstance(a, FelgenArtikel)}
                self._index_catalog = catalog
            return self._index.get(sku)

    def _match(self, sku):
        article, entry = self._article(sku), self._entries.get(sku)
        if article is None or entry is None or _identity(article) != entry[0]:
            return None
        return article, entry[1]

    def describe(self, article):
        try:
            if not self._authorized() or not isinstance(article, FelgenArtikel) or not _SKU.fullmatch(article.artikel_id):
                return None
            matched = self._match(article.artikel_id)
            if matched is None or _identity(matched[0]) != _identity(article):
                return None
            return public_felgen_image_metadata({"url": f"/api/felgenkatalog/bild/{article.artikel_id}/{matched[1]}.webp",
                "alt": f"Felge ohne Reifen: {article.hersteller} {article.profil}", "bezug": "artikel", "motiv": "felge"}, article.artikel_id)
        except Exception:
            return None

    def read(self, sku, sha):
        try:
            if (not self._authorized() or not isinstance(sku, str) or not _SKU.fullmatch(sku)
                    or not isinstance(sha, str) or not _SHA.fullmatch(sha)):
                return None
            matched = self._match(sku)
            if matched is None or matched[1] != sha:
                return None
            data = self._read_file(sha + ".webp", MAX_IMAGE_BYTES)
            if hashlib.sha256(data).hexdigest() != sha:
                return None
            validate_webp_bytes(data)
            return data
        except Exception:
            return None


Store = FelgenBildStore
