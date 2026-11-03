"""Confirmed local tire-profile images. No network or source URLs are accepted.

Image bytes are inspected, never transformed. Invalid/missing optional images
fail closed without affecting tire prices or construction/quality metadata.
"""
from collections.abc import Mapping
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import threading
import unicodedata
import warnings

from PIL import Image

from tyresystem_feed import FeedArticle
from tyresystem_provider import IndexedCatalog


MAX_IMAGE_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_PROFILES = 20000
_SKU = re.compile(r"[1-9][0-9]{0,17}")
_SHA = re.compile(r"[0-9a-f]{64}")
_URL = re.compile(r"/api/reifenkatalog/bild/([1-9][0-9]{0,17})/([0-9a-f]{64})\.webp")


def _text(value, maximum):
    return (isinstance(value, str) and 1 <= len(value) <= maximum and value == value.strip()
            and not any(unicodedata.category(char).startswith("C") for char in value))


def validate_webp_bytes(value):
    """Return verified dimensions for one bounded WEBP frame; preserve all bytes."""
    try:
        if (not isinstance(value, bytes) or not 12 <= len(value) <= MAX_IMAGE_BYTES
                or value[:4] != b"RIFF" or value[8:12] != b"WEBP"
                or int.from_bytes(value[4:8], "little") + 8 != len(value)):
            raise ValueError()
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(value)) as picture:
                width, height = picture.size
                if (picture.format != "WEBP" or getattr(picture, "n_frames", 1) != 1
                        or not 32 <= width <= 2048 or not 32 <= height <= 2048):
                    raise ValueError()
                picture.verify()
            # WEBP verify() alone does not guarantee that pixel decoding succeeds.
            with Image.open(io.BytesIO(value)) as picture:
                picture.load()
                if picture.format != "WEBP" or picture.size != (width, height) or getattr(picture, "n_frames", 1) != 1:
                    raise ValueError()
        return width, height
    except Exception:
        raise ValueError("Das Reifenbild ist nicht vollständig prüfbar.") from None


def public_image_metadata(value, artikel_id):
    """Only an explicitly confirmed tire-only motif belongs in this catalogue."""
    if not isinstance(value, Mapping) or set(value) != {"url", "alt", "bezug", "motiv"}:
        return None
    url, alt = value.get("url"), value.get("alt")
    if (not isinstance(url, str) or not _text(alt, 250) or value.get("bezug") != "profil"
            or value.get("motiv") != "reifen"):
        return None
    match = _URL.fullmatch(url)
    if not match or match[1] != artikel_id:
        return None
    return {"url": url, "alt": alt, "bezug": "profil", "motiv": "reifen"}


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


class ReifenBildStore:
    def __init__(self, root, *, catalog_source, private_view_authorized=None,
                 publication_approved=False, publication_reference=""):
        self._source = catalog_source if callable(catalog_source) else None
        self._authorize = private_view_authorized if callable(private_view_authorized) else None
        self._public = publication_approved is True and _text(publication_reference, 200)
        self._root = self._root_input = None
        self._profiles = {}
        self._index_catalog = None
        self._index = {}
        self._lock = threading.Lock()
        try:
            text = str(root).replace("\\", "/")
            candidate = Path(root)
            if text.startswith("//") or "://" in text or not candidate.is_absolute() or candidate.is_symlink():
                raise ValueError()
            resolved = candidate.resolve()
            if any(part.lower() in {"static", "public", "templates", "outputs", "wwwroot"} for part in resolved.parts):
                raise ValueError()
            repository = Path(__file__).resolve().parent
            if resolved.is_relative_to(repository) and not resolved.is_relative_to(repository / "data"):
                raise ValueError()
            self._root_input, self._root = candidate, resolved
            raw = self._read_file("manifest.json", MAX_MANIFEST_BYTES)
            manifest = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
            if (not isinstance(manifest, dict) or set(manifest) != {"schema", "profiles"}
                    or type(manifest["schema"]) is not int or manifest["schema"] != 1
                    or not isinstance(manifest["profiles"], list) or len(manifest["profiles"]) > MAX_PROFILES):
                raise ValueError()
            profiles, ambiguous = {}, set()
            for item in manifest["profiles"]:
                if (not isinstance(item, dict) or set(item) != {"hersteller", "profil", "sha256", "bezug", "nachweis", "motiv"}
                        or not _text(item["hersteller"], 100) or not _text(item["profil"], 200)
                        or item["bezug"] != "profil" or item["motiv"] != "reifen" or not isinstance(item["sha256"], str)
                        or not _SHA.fullmatch(item["sha256"]) or not _text(item["nachweis"], 200)
                        or "://" in item["nachweis"] or "?" in item["nachweis"]):
                    continue
                key = (item["hersteller"], item["profil"])
                if key in profiles or key in ambiguous:
                    profiles.pop(key, None)
                    ambiguous.add(key)
                else:
                    profiles[key] = item["sha256"]
            self._profiles = profiles
        except Exception:
            # Optional image failures must not make the catalogue unavailable.
            self._profiles = {}

    def __repr__(self):
        return "<ReifenBildStore private_assets=redacted>"

    def _authorized(self):
        if self._public:
            return True
        try:
            return self._authorize is not None and self._authorize() is True
        except Exception:
            return False

    def _read_file(self, name, maximum):
        if (self._root is None or self._root_input.is_symlink()
                or self._root_input.resolve() != self._root):
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
        if not isinstance(catalog, IndexedCatalog):
            return None
        with self._lock:
            if catalog is not self._index_catalog:
                self._index = {article.artikel_id: article for article in catalog.snapshot.artikel if isinstance(article, FeedArticle)}
                self._index_catalog = catalog
            return self._index.get(sku)

    def describe(self, article):
        try:
            if not self._authorized() or not isinstance(article, FeedArticle) or not _SKU.fullmatch(article.artikel_id):
                return None
            current = self._article(article.artikel_id)
            if current is None or (current.hersteller, current.profil) != (article.hersteller, article.profil):
                return None
            digest = self._profiles.get((article.hersteller, article.profil))
            if digest is None:
                return None
            return public_image_metadata({"url": f"/api/reifenkatalog/bild/{article.artikel_id}/{digest}.webp",
                "alt": f"Profilabbildung {article.hersteller} {article.profil}", "bezug": "profil", "motiv": "reifen"}, article.artikel_id)
        except Exception:
            return None

    def read(self, sku, sha):
        try:
            if (not self._authorized() or not isinstance(sku, str) or not _SKU.fullmatch(sku)
                    or not isinstance(sha, str) or not _SHA.fullmatch(sha)):
                return None
            article = self._article(sku)
            if article is None or self._profiles.get((article.hersteller, article.profil)) != sha:
                return None
            value = self._read_file(sha + ".webp", MAX_IMAGE_BYTES)
            if hashlib.sha256(value).hexdigest() != sha:
                return None
            validate_webp_bytes(value)
            return value
        except Exception:
            return None


Store = ReifenBildStore
