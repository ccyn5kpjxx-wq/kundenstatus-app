"""Synthetic exact-variant rim-image contracts; no app, real assets or network."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from PIL import Image
from felgen_bilder import FelgenBildStore, public_felgen_image_metadata
from felgen_katalog import FelgenCatalogProvider, parse_felgen_feed
from scripts.test_felgen_katalog import NOW, POLICY, raw_feed
from scripts.test_reifen_bilder import synthetic_webp


class RimImagesTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="rim-images-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.assets = self.root / "private-rim-images"
        self.assets.mkdir()
        self.image = synthetic_webp()
        self.sha = hashlib.sha256(self.image).hexdigest()
        self.path = self.assets / (self.sha + ".webp")
        self.path.write_bytes(self.image)
        self.catalog = parse_felgen_feed(raw_feed(), abgerufen_am=NOW)
        article = self.catalog.artikel[0]
        self.entry = {"artikel_id": article.artikel_id, "hersteller": article.hersteller, "profil": article.profil,
                      "hersteller_artikelnummer": article.hersteller_artikelnummer, "ean": article.ean,
                      "sha256": self.sha, "bezug": "artikel", "motiv": "felge",
                      "nachweis": "Synthetic article 1, visually approved rim without tire"}
        self.manifest([self.entry])

    def manifest(self, articles):
        (self.assets / "manifest.json").write_text(json.dumps({"schema": 1, "articles": articles}), encoding="utf-8")

    def store(self, **settings):
        return FelgenBildStore(self.assets, catalog_source=lambda: self.catalog, **settings)

    def approved(self):
        return self.store(private_view_authorized=lambda: True)

    def test_correct_article_binding_returns_original_bytes_without_reading_assets_during_describe(self):
        store = self.approved()
        with patch.object(store, "_read_file", side_effect=AssertionError("No image IO during search")) as read:
            picture = store.describe(self.catalog.artikel[0])
            self.assertEqual(picture, {"url": f"/api/felgenkatalog/bild/1/{self.sha}.webp",
                "alt": "Felge ohne Reifen: Synthetic Wheel Model", "bezug": "artikel", "motiv": "felge"})
            read.assert_not_called()
        self.assertEqual(store.read("1", self.sha), self.image)
        self.assertEqual(hashlib.sha256(self.image).hexdigest(), self.sha)
        self.assertNotIn("nachweis", str(picture))
        self.assertNotIn("PRIVATE", repr(store))

    def test_default_deny_callback_requires_true_and_public_approval_needs_reference(self):
        for callback in (None, True, lambda: 1, lambda: "true", Mock(side_effect=RuntimeError("PRIVATE"))):
            store = self.store(private_view_authorized=callback)
            with patch.object(store, "_article") as source:
                self.assertIsNone(store.describe(self.catalog.artikel[0]))
                self.assertIsNone(store.read("1", self.sha))
                source.assert_not_called()
        self.assertIsNone(self.store(publication_approved=True).read("1", self.sha))
        self.assertEqual(self.store(publication_approved=True, publication_reference="synthetic explicit rights").read("1", self.sha), self.image)

    def test_sku_manufacturer_full_profile_mpn_and_ean_cannot_transfer_between_variants(self):
        store = self.approved()
        original = self.catalog.artikel[0]
        for fields in ({"artikel_id": "2"}, {"hersteller": "Other"}, {"profil": "Wheel Model black"},
                       {"hersteller_artikelnummer": "MPN-OTHER"}, {"hersteller_artikelnummer": None},
                       {"ean": "9999999999999"}, {"ean": None}):
            with self.subTest(fields=fields):
                changed = replace(original, **fields)
                self.catalog = replace(self.catalog, artikel=(changed,))
                self.assertIsNone(store.describe(changed))
                self.assertIsNone(store.read(changed.artikel_id, self.sha))
        self.catalog = replace(self.catalog, artikel=(original,))
        self.assertEqual(store.read("1", self.sha), self.image)
        self.assertIsNone(store.describe(replace(original, profil="Wheel Model silver")))

    def test_nullable_identifiers_only_match_when_both_catalogue_and_manifest_lack_them(self):
        original = self.catalog.artikel[0]
        self.catalog = replace(self.catalog, artikel=(replace(original, ean=None, hersteller_artikelnummer=None),))
        self.assertIsNone(self.approved().read("1", self.sha))
        self.manifest([{**self.entry, "ean": None, "hersteller_artikelnummer": None}])
        self.assertEqual(self.approved().read("1", self.sha), self.image)

    def test_manifest_requires_positive_rim_motif_and_unique_exact_article(self):
        invalid = [{key: value for key, value in self.entry.items() if key != "motiv"}]
        invalid.extend({**self.entry, "motiv": motif} for motif in (None, "reifen", "komplettrad", "rad", True))
        invalid.extend(({**self.entry, "bezug": "profil"}, {**self.entry, "nachweis": "https://source.invalid/?token=PRIVATE"},
                        {**self.entry, "image_url": "https://source.invalid"}))
        for entry in invalid:
            with self.subTest(motif=entry.get("motiv")):
                self.manifest([entry])
                store = self.approved()
                self.assertIsNone(store.describe(self.catalog.artikel[0]))
                self.assertIsNone(store.read("1", self.sha))
        for entries in ([self.entry, self.entry], [{**self.entry, "motiv": "reifen"}, self.entry]):
            self.manifest(entries)
            self.assertIsNone(self.approved().read("1", self.sha))

    def test_hash_paths_corrupt_files_and_nonwebp_payloads_are_not_served(self):
        store = self.approved()
        for sku, sha in (("../1", self.sha), ("01", self.sha), ("1", "0"*64), ("1", self.sha+"?token=PRIVATE")):
            self.assertIsNone(store.read(sku, sha))
        self.path.write_bytes(synthetic_webp((160, 160)))
        self.assertIsNone(store.read("1", self.sha))
        png = io.BytesIO()
        Image.new("RGB", (150, 150), "white").save(png, "PNG")
        other = png.getvalue()
        digest = hashlib.sha256(other).hexdigest()
        (self.assets / (digest + ".webp")).write_bytes(other)
        self.manifest([{**self.entry, "sha256": digest}])
        self.assertIsNone(self.approved().read("1", digest))
        self.path.unlink()
        self.assertIsNone(store.read("1", self.sha))

    def test_private_roots_manifest_bounds_and_symlinks_are_checked(self):
        for root in ("relative", "//server/share", self.root / "public"):
            store = FelgenBildStore(root, catalog_source=lambda: self.catalog, private_view_authorized=lambda: True)
            self.assertIsNone(store.read("1", self.sha))
        with patch("felgen_bilder.MAX_MANIFEST_BYTES", 2):
            self.assertIsNone(self.approved().read("1", self.sha))
        original = Path.is_symlink
        with patch.object(Path, "is_symlink", lambda path: path == self.path or original(path)):
            self.assertIsNone(self.approved().read("1", self.sha))
        (self.assets / "manifest.json").write_text("not json PRIVATE", encoding="utf-8")
        self.assertIsNone(self.approved().read("1", self.sha))

    def test_public_metadata_is_strict_article_local_rim_only_and_prices_survive_bad_images(self):
        picture = self.approved().describe(self.catalog.artikel[0])
        self.assertEqual(public_felgen_image_metadata(picture, "1"), picture)
        bad = [{key: value for key, value in picture.items() if key != "motiv"}]
        bad.extend(({**picture, "url": "https://foreign.invalid/?key=PRIVATE"},
                    {**picture, "url": picture["url"].replace("/1/", "/2/")},
                    {**picture, "url": picture["url"]+"?token=PRIVATE"}, {**picture, "motiv": "reifen"},
                    {**picture, "bezug": "profil"}, {**picture, "proof": "PRIVATE"}))
        class Cache:
            def load(_):
                return self.catalog
        provider = FelgenCatalogProvider(Cache(), policy=deepcopy(POLICY), private_view_authorized=lambda: True,
                                         clock=lambda: NOW, image_resolver=self.approved().describe)
        product = provider.lookup("1")
        self.assertEqual(product["bild"], picture)
        expected_price = product["preis"]
        self.assertEqual(expected_price["gesamt_brutto"], "523.60")
        for metadata in bad:
            self.assertIsNone(public_felgen_image_metadata(metadata, "1"))
            provider._image_resolver = lambda _, value=metadata: value
            product = provider.search({})["produkte"][0]
            self.assertEqual(product["preis"], expected_price)
            self.assertEqual(product["kennungspassung"], "nicht_geprueft")
            self.assertNotIn("bild", product)
            self.assertNotIn("PRIVATE", str(product))
        provider._image_resolver = Mock(side_effect=RuntimeError("PRIVATE"))
        self.assertEqual(provider.lookup("1")["preis"], expected_price)


if __name__ == "__main__":
    unittest.main(verbosity=2)
