"""Synthetic local image contracts; never loads real assets, keys or providers."""
import ast
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask, abort, make_response, request
from PIL import Image
from reifen_bilder import ReifenBildStore, public_image_metadata, validate_webp_bytes
from reifen_suche import suche_reifenangebote
from scripts.test_tyresystem_provider import NOW, POLICY, QUERY, feed
from tyresystem_provider import IndexedCatalog, TyreSystemCatalogProvider


def synthetic_webp(size=(150, 150), *, animated=False):
    first = Image.new("RGB", size, "gray")
    output = io.BytesIO()
    options = {"save_all": True, "append_images": [Image.new("RGB", size, "white")], "duration": 100} if animated else {}
    first.save(output, "WEBP", lossless=True, **options)
    return output.getvalue()


class _ImageFixture:
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="tire-images-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.assets = self.root / "private-images"
        self.assets.mkdir()
        self.image = synthetic_webp()
        self.digest = hashlib.sha256(self.image).hexdigest()
        self.path = self.assets / (self.digest + ".webp")
        self.path.write_bytes(self.image)
        self.catalog = IndexedCatalog.build(feed())
        self.entry = {"hersteller": "Synthetic", "profil": "Winter", "sha256": self.digest,
                      "bezug": "profil", "motiv": "reifen", "nachweis": "Synthetic article 1, verified profile fixture"}
        self.manifest([self.entry])

    def manifest(self, profiles):
        (self.assets / "manifest.json").write_text(json.dumps({"schema": 1, "profiles": profiles}), encoding="utf-8")

    def store(self, **options):
        return ReifenBildStore(self.assets, catalog_source=lambda: self.catalog, **options)

    def approved(self):
        return self.store(private_view_authorized=lambda: True)


class TireImagesTest(_ImageFixture, unittest.TestCase):
    def test_verifier_decodes_without_changing_image_bytes(self):
        original = self.image
        self.assertEqual(validate_webp_bytes(self.image), (150, 150))
        self.assertEqual(self.image, original)
        self.assertEqual(hashlib.sha256(self.image).hexdigest(), self.digest)
        png = io.BytesIO()
        Image.new("RGB", (150, 150), "black").save(png, "PNG")
        corrupted = bytearray(self.image[:-10])
        corrupted[4:8] = (len(corrupted)-8).to_bytes(4, "little")
        for value in (png.getvalue(), bytes(corrupted), synthetic_webp((31, 150)), synthetic_webp((2049, 32)),
                      synthetic_webp(animated=True), self.image + b"x", b"x" * (2*1024*1024+1)):
            with self.subTest(length=len(value)), self.assertRaises(ValueError):
                validate_webp_bytes(value)

    def test_default_deny_requires_real_boolean_callback_or_separate_public_rights(self):
        for callback in (None, True, lambda: 1, lambda: "true", Mock(side_effect=RuntimeError("PRIVATE"))):
            store = self.store(private_view_authorized=callback)
            with patch.object(store, "_article") as lookup:
                self.assertIsNone(store.describe(self.catalog.snapshot.artikel[0]))
                self.assertIsNone(store.read("1", self.digest))
                lookup.assert_not_called()
        self.assertIsNone(self.store(publication_approved=True).read("1", self.digest))
        self.assertEqual(self.store(publication_approved=True, publication_reference="synthetic image rights").read("1", self.digest), self.image)

    def test_exact_current_profile_binding_and_original_file_bytes(self):
        store = self.approved()
        description = store.describe(self.catalog.snapshot.artikel[0])
        self.assertEqual(description, {"url": f"/api/reifenkatalog/bild/1/{self.digest}.webp",
                                      "alt": "Profilabbildung Synthetic Winter", "bezug": "profil", "motiv": "reifen"})
        self.assertNotIn("nachweis", str(description))
        self.assertEqual(store.read("1", self.digest), self.image)
        self.catalog = IndexedCatalog.build(feed({"Hersteller": "Other"}))
        self.assertIsNone(store.read("1", self.digest))
        self.assertIsNone(store.describe(feed().artikel[0]))
        self.catalog = IndexedCatalog.build(feed({"Profil": "Winter plus"}))
        self.assertIsNone(store.read("1", self.digest))

    def test_unknown_sku_hash_mismatch_and_untrusted_path_values_are_denied(self):
        store = self.approved()
        for sku, digest in (("2", self.digest), ("1", "0"*64), ("../1", self.digest), ("01", self.digest),
                            ("1", self.digest + "?key=PRIVATE"), ("1", "../manifest"), (True, self.digest)):
            with self.subTest(sku=sku):
                self.assertIsNone(store.read(sku, digest))
        self.path.write_bytes(synthetic_webp((160, 160)))
        self.assertIsNone(store.read("1", self.digest))
        self.path.unlink()
        self.assertIsNone(store.read("1", self.digest))

    def test_invalid_or_ambiguous_manifest_entries_never_infer_a_profile(self):
        for rows in ([{**self.entry, "profil": "WINTER"}], [{**self.entry, "nachweis": ""}],
                     [{**self.entry, "nachweis": "https://private.invalid/?key=PRIVATE"}],
                     [self.entry, self.entry], [{**self.entry, "url": "https://private.invalid"}]):
            self.manifest(rows)
            self.assertIsNone(self.approved().read("1", self.digest))
        (self.assets / "manifest.json").write_text("not json PRIVATE", encoding="utf-8")
        self.assertIsNone(self.approved().read("1", self.digest))
        (self.assets / "manifest.json").unlink()
        self.assertIsNone(self.approved().read("1", self.digest))

    def test_public_roots_and_asset_symlinks_are_not_read(self):
        for root in ("relative", "//server/share", self.root / "static"):
            with self.subTest(root=str(root)):
                store = ReifenBildStore(root, catalog_source=lambda: self.catalog, private_view_authorized=lambda: True)
                self.assertIsNone(store.read("1", self.digest))
        self.path.unlink()
        outside = self.root / "outside.webp"
        outside.write_bytes(self.image)
        try:
            self.path.symlink_to(outside)
        except OSError:
            # Windows may deny symlink creation; exercise the path guard directly.
            original = Path.is_symlink
            with patch.object(Path, "is_symlink", lambda path: path == self.path or original(path)):
                self.assertIsNone(self.approved().read("1", self.digest))
        else:
            self.assertIsNone(self.approved().read("1", self.digest))

    def test_optional_metadata_rejects_external_or_wrong_article_urls(self):
        good = {"url": f"/api/reifenkatalog/bild/1/{self.digest}.webp", "alt": "Synthetic profile", "bezug": "profil", "motiv": "reifen"}
        self.assertEqual(public_image_metadata(good, "1"), good)
        for bad in ({**good, "url": "https://foreign.invalid/?key=PRIVATE"},
                    {**good, "url": f"/api/reifenkatalog/bild/2/{self.digest}.webp"},
                    {**good, "url": good["url"] + "?token=PRIVATE"}, {**good, "alt": "bad\nalt"},
                    {**good, "bezug": "exact-article"}, {**good, "proof": "PRIVATE"}):
            self.assertIsNone(public_image_metadata(bad, "1"))

    def test_resolver_failures_do_not_change_price_or_construction_metadata(self):
        class Cache:
            def load(_):
                return self.catalog
        query = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}
        provider = TyreSystemCatalogProvider(Cache(), policy=deepcopy(POLICY), private_view_authorized=lambda: True,
            clock=lambda: NOW, image_resolver=self.approved().describe)
        body, status = suche_reifenangebote(query, provider, now=NOW)
        self.assertEqual(status, 200)
        self.assertEqual(body["produkte"][0]["bild"]["bezug"], "profil")
        expected = body["produkte"][0]["preis"]
        self.assertEqual(expected["gesamt_brutto"], "487.28")
        self.assertIsNone(body["produkte"][0]["qualitaet"])
        self.assertNotIn("nachweis", str(body))
        for resolver in (Mock(side_effect=RuntimeError("PRIVATE-MARKER")),
                         lambda _: {"url": "https://private.invalid", "alt": "bad", "bezug": "profil", "motiv": "reifen"}):
            provider._image_resolver = resolver
            body, status = suche_reifenangebote(query, provider, now=NOW)
            self.assertEqual(status, 200)
            self.assertEqual(body["produkte"][0]["preis"], expected)
            self.assertNotIn("bild", body["produkte"][0])
            self.assertNotIn("PRIVATE", str(body))

    def test_manifest_requires_positive_tire_only_motif_for_description_and_read(self):
        invalid_entries = [{key: value for key, value in self.entry.items() if key != "motiv"}]
        invalid_entries.extend({**self.entry, "motiv": motif} for motif in
                               (None, "felge", "komplettrad", "reifen_mit_felge", "Reifen", "", True, ["reifen"]))
        for entry in invalid_entries:
            with self.subTest(motif=entry.get("motiv")):
                self.manifest([entry])
                store = self.approved()
                self.assertIsNone(store.describe(self.catalog.snapshot.artikel[0]))
                self.assertIsNone(store.read("1", self.digest))
        self.manifest([self.entry])
        store = self.approved()
        self.assertEqual(store.describe(self.catalog.snapshot.artikel[0])["motiv"], "reifen")
        self.assertEqual(store.read("1", self.digest), self.image)

    def test_missing_or_other_public_motifs_omit_only_image_and_keep_tire_price(self):
        class Cache:
            def load(_):
                return self.catalog
        image = self.approved().describe(self.catalog.snapshot.artikel[0])
        self.assertEqual(set(image), {"url", "alt", "bezug", "motiv"})
        query = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}
        provider = TyreSystemCatalogProvider(Cache(), policy=deepcopy(POLICY), private_view_authorized=lambda: True,
            clock=lambda: NOW, image_resolver=lambda _: image)
        good, status = suche_reifenangebote(query, provider, now=NOW)
        self.assertEqual((status, good["produkte"][0]["bild"]["motiv"]), (200, "reifen"))
        expected = good["produkte"][0]["preis"]
        invalid = [{key: value for key, value in image.items() if key != "motiv"}]
        invalid.extend({**image, "motiv": motif} for motif in (None, "felge", "komplettrad", "reifen_mit_felge", True))
        for metadata in invalid:
            with self.subTest(motif=metadata.get("motiv")):
                provider._image_resolver = lambda _, value=metadata: value
                body, status = suche_reifenangebote(query, provider, now=NOW)
                self.assertEqual(status, 200)
                self.assertEqual(len(body["produkte"]), 1)
                self.assertEqual(body["produkte"][0]["preis"], expected)
                self.assertIsNone(body["produkte"][0]["qualitaet"])
                self.assertNotIn("bild", body["produkte"][0])


class ImageRouteTest(_ImageFixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / "app.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        route = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "reifenkatalog_bild")
        cls.app = Flask("synthetic-image-route")
        cls.app.testing = True
        env = {"app": cls.app, "abort": abort, "make_response": make_response, "request": request}
        exec(compile(ast.Module(body=[route], type_ignores=[]), str(source), "exec"), env)

    def test_actual_image_get_is_private_bounded_and_has_no_remote_redirect(self):
        self.app.config["REIFEN_BILD_STORE"] = self.approved()
        client = self.app.test_client()
        path = f"/api/reifenkatalog/bild/1/{self.digest}.webp"
        response = client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, self.image)
        self.assertEqual(response.content_type, "image/webp")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["Cache-Control"], "private, max-age=300")
        self.assertNotIn("Location", response.headers)
        self.assertEqual(client.get(path + "?url=PRIVATE").status_code, 404)
        self.assertEqual(client.get(path.replace("/1/", "/2/")).status_code, 404)
        self.assertEqual(client.post(path).status_code, 405)
        self.app.config["REIFEN_BILD_STORE"] = self.store()
        self.assertEqual(client.get(path).status_code, 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
