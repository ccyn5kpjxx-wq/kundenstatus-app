"""Real rim blueprint/templates and relevant portal hooks, entirely offline.

No application import, dotenv, real database, private feed or background worker.
Only synthetic in-memory snapshots are used; network and worker starts are denied.
"""
import ast
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask, abort, redirect, request, session, url_for
from itsdangerous import URLSafeTimedSerializer
from felgen_bilder import FelgenBildStore
from felgen_katalog import FelgenCatalogProvider, parse_felgen_feed
from felgen_shop import register_felgen_shop
from portal_static_assets import asset_version
from scripts.test_felgen_katalog import POLICY, raw_feed
from scripts.test_reifen_bilder import synthetic_webp


class RimShopRoutesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = str(ROOT / "app.py")
        wanted = {"restrict_public_site_service", "protect_csrf", "add_csrf_fields", "csrf_field", "get_csrf_token"}
        cls.functions = [node for node in ast.parse(Path(cls.source).read_text(encoding="utf-8-sig")).body
                         if isinstance(node, ast.FunctionDef) and node.name in wanted]
        if {node.name for node in cls.functions} != wanted:
            raise AssertionError("Relevant real portal hooks must all be present")

    def setUp(self):
        for name in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo", "threading.Thread.start"):
            blocker = patch(name, side_effect=AssertionError("External operation forbidden in offline rim tests"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.app = Flask("offline-rim-shop", template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.testing = True
        self.app.secret_key = "synthetic-offline-rim-route-secret"
        self.no_write = Mock(side_effect=AssertionError("Database, owner or order adapter must never run"))
        self.limit = Mock(return_value=(False, 0))
        self.attempt = Mock()
        self.public = False
        self.env = {"__name__": __name__, "app": self.app, "request": request, "session": session,
                    "abort": abort, "redirect": redirect, "urlsplit": urlsplit, "sys": sys, "re": re,
                    "secrets": secrets, "hmac": hmac, "CSRF_FIELD_NAME": "csrf_token",
                    "is_public_site_request": lambda: self.public, "PUBLIC_SITE_ONLY": False, "PORTAL_BASE_URL": "",
                    "progress_csrf_exempt": lambda _: False, "csrf_recovery_response": lambda: None,
                    "get_db": self.no_write}
        exec(compile(ast.Module(body=self.functions, type_ignores=[]), self.source, "exec"), self.env)
        self.portal = SimpleNamespace(app=self.app, public_form_rate_limit_status=self.limit,
            record_public_form_attempt=self.attempt, REIFEN_SUCHE_RATE_LIMIT_MAX=60,
            REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS=60, get_db=self.no_write)
        for endpoint, route in (("oeffentliche_homepage", "/homepage"), ("oeffentlicher_reifenshop", "/reifen"),
                                ("impressum_seite", "/impressum"), ("datenschutz_seite", "/datenschutz")):
            self.app.add_url_rule(route, endpoint, lambda: "offline placeholder")
        self.app.add_url_rule("/other-form", "other_form", lambda: "<form method='post'></form>", methods=["GET", "POST"])
        self.app.add_url_rule("/internal", "internal", lambda: "private route")
        self.app.extensions["reifen_kauf"] = self.no_write
        self.app.extensions["reifen_bestellung"] = self.no_write
        self.app.config.update(REIFEN_KAUF_MODE="off", REIFEN_KAUF_ENABLED=False,
                               REIFEN_KAUF_SUPPLIER=self.no_write, REIFEN_KAUF_SMS=self.no_write,
                               REIFEN_KAUF_GATEWAY=self.no_write)
        def static_asset_url(filename):
            version = asset_version(self.app.static_folder, filename)
            return url_for("static", filename=filename, **({"v": version} if version else {}))
        self.app.jinja_env.globals.update(static_asset_url=static_asset_url,
            werkstatt_oeffentlich={"name": "Offline Testwerkstatt", "adresse": "Teststraße 1", "ort": "Testort",
                                  "telefon": "00000", "telefon_url": "tel:00000"})
        register_felgen_shop(self.portal)
        self.now = datetime.now(timezone.utc)
        self.received = self.now - timedelta(minutes=2)
        self.snapshot = parse_felgen_feed(raw_feed(), abgerufen_am=self.received)
        self.cache = SimpleNamespace(load=Mock(side_effect=lambda: self.snapshot))
        self.provider = FelgenCatalogProvider(self.cache, policy=deepcopy(POLICY),
                                             private_view_authorized=lambda: True, clock=lambda: self.now)
        self.app.config["FELGENANGEBOT_PROVIDER"] = self.provider
        self.serializer = URLSafeTimedSerializer(self.app.secret_key, salt="felgen-auswahl-v1")
        self.token = self.serializer.dumps({"artikel_id": "1", "anzahl": 4})
        self.path = "/felgen/auswahl/" + self.token
        self.client = self.app.test_client()

    def assert_private(self, response, *, cookie_absent=True):
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")
        self.assertEqual(response.headers["X-Robots-Tag"], "noindex, nofollow")
        self.assertNotIn("Set-Cookie", response.headers)
        if cookie_absent:
            self.assertIsNone(self.client.get_cookie("session"))
        self.assertEqual(self.no_write.mock_calls, [])
        for marker in ("PRIVATE-MARKER", "private.invalid", "SECRET-PROVIDER", "einkaufspreis", "netto_einkauf"):
            self.assertNotIn(marker.encode(), response.get_data())

    def install_images(self, *, authorized=True, motif="felge"):
        temporary = tempfile.TemporaryDirectory(prefix="offline-rim-route-images-")
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name) / "private-rim-images"
        directory.mkdir()
        image = synthetic_webp()
        digest = hashlib.sha256(image).hexdigest()
        (directory / (digest + ".webp")).write_bytes(image)
        article = self.snapshot.artikel[0]
        manifest = {"schema": 1, "articles": [{"artikel_id": article.artikel_id,
            "hersteller": article.hersteller, "profil": article.profil,
            "hersteller_artikelnummer": article.hersteller_artikelnummer, "ean": article.ean,
            "sha256": digest, "bezug": "artikel", "motiv": motif,
            "nachweis": "Synthetic article, approved rim-only fixture"}]}
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        store = FelgenBildStore(directory, catalog_source=lambda: self.snapshot,
                               private_view_authorized=(lambda: True) if authorized else None)
        self.app.config["FELGEN_BILD_STORE"] = store
        self.provider = FelgenCatalogProvider(self.cache, policy=deepcopy(POLICY),
            private_view_authorized=lambda: True, clock=lambda: self.now, image_resolver=store.describe)
        self.app.config["FELGENANGEBOT_PROVIDER"] = self.provider
        return store, f"/api/felgenkatalog/bild/{article.artikel_id}/{digest}.webp", image

    def test_verified_image_route_returns_original_webp_without_cookie_form_or_order(self):
        store, path, original = self.install_images()
        self.public = True
        with patch.object(store, "read", wraps=store.read) as read:
            response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(), original)
        self.assertEqual(response.headers["Content-Type"], "image/webp")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assert_private(response)
        self.assertNotIn(b"<form", response.get_data())
        self.assertNotIn(b"csrf_token", response.get_data())
        read.assert_called_once_with("1", hashlib.sha256(original).hexdigest())
        self.cache.load.assert_not_called()
        self.limit.assert_not_called()
        self.attempt.assert_not_called()

    def test_image_route_denies_unapproved_missing_wrong_identity_digest_and_query(self):
        store, path, _ = self.install_images(authorized=False)
        self.public = True
        denied = self.client.get(path)
        self.assertEqual(denied.status_code, 404)
        self.assert_private(denied)
        self.app.config.pop("FELGEN_BILD_STORE")
        missing = self.client.get(path)
        self.assertEqual(missing.status_code, 404)
        self.assert_private(missing)
        fake = SimpleNamespace(read=Mock(side_effect=AssertionError("Untrusted image store")))
        self.app.config["FELGEN_BILD_STORE"] = fake
        wrong_store = self.client.get(path)
        self.assertEqual(wrong_store.status_code, 404)
        self.assert_private(wrong_store)
        fake.read.assert_not_called()
        store, path, _ = self.install_images()
        for invalid in (path.replace("/1/", "/2/"), path.replace("/1/", "/01/"),
                        path.rsplit("/", 1)[0]+"/"+"0"*64+".webp"):
            response = self.client.get(invalid)
            self.assertEqual(response.status_code, 404)
            self.assert_private(response)
        with patch.object(store, "read", wraps=store.read) as read:
            response = self.client.get(path+"?token=PRIVATE-MARKER")
            self.assertEqual(response.status_code, 400)
            self.assert_private(response)
            read.assert_not_called()
        article = self.snapshot.artikel[0]
        self.snapshot = replace(self.snapshot, artikel=(replace(article, profil=article.profil+" another finish"),))
        response = self.client.get(path)
        self.assertEqual(response.status_code, 404)
        self.assert_private(response)
        self.cache.load.assert_not_called()

    def test_real_selection_and_search_show_only_valid_article_image_without_changing_price(self):
        original_price = self.client.get("/api/felgenangebote/suche").json["produkte"][0]["preis"]
        _, path, _ = self.install_images()
        search = self.client.get("/api/felgenangebote/suche")
        self.assertEqual(search.status_code, 200)
        self.assert_private(search)
        product = search.json["produkte"][0]
        self.assertEqual(product["preis"], original_price)
        self.assertEqual(product["bild"], {"url": path, "alt": "Felge ohne Reifen: Synthetic Wheel Model",
                                          "bezug": "artikel", "motiv": "felge"})
        selected = self.client.get(self.path)
        self.assertEqual(selected.status_code, 200)
        self.assert_private(selected)
        self.assertIn(f'src="{path}"', selected.text)
        self.assertIn('alt="Felge ohne Reifen: Synthetic Wheel Model"', selected.text)
        self.assertIn("523,60 €", selected.text)
        self.assertIn("Fahrzeugpassung noch nicht geprüft", selected.text)
        self.assertNotRegex(selected.text, r"<(?:form|input|select)\b")
        self.assertNotIn('name="csrf_token"', selected.text)
        for settings in ({"motif": "reifen"}, {"motif": "komplettrad"}, {"authorized": False}):
            with self.subTest(settings=settings):
                self.install_images(**settings)
                product = self.client.get("/api/felgenangebote/suche").json["produkte"][0]
                self.assertEqual(product["preis"], original_price)
                self.assertNotIn("bild", product)
                selected = self.client.get(self.path)
                self.assertEqual(selected.status_code, 200)
                self.assert_private(selected)
                self.assertNotIn("/api/felgenkatalog/bild/", selected.text)
                self.assertIn("523,60 €", selected.text)

    def test_real_category_templates_have_correct_inputs_and_no_fake_fitment_or_set_checkout(self):
        catalog = self.client.get("/felgen?anzeigen=1")
        self.assertEqual(catalog.status_code, 200)
        self.assert_private(catalog)
        html = catalog.get_data(as_text=True)
        self.assertIn("data-rim-search", html)
        for name in ("suche", "hersteller", "material", "anzahl", "sortierung"):
            self.assertIn(f'name="{name}"', html)
        self.assertRegex(html, r"/static/homepage-felgen-suche\.js\?v=[a-f0-9]{20}")
        self.assertIn('method="get"', html)
        self.assertNotIn('name="csrf_token"', html)
        self.assertIn("Fahrzeugzuordnung, Maße und Gutachten", html)
        sets = self.client.get("/komplettraeder")
        self.assertEqual(sets.status_code, 200)
        self.assert_private(sets)
        self.assertIn("Komplettrad-Konfigurator ist noch nicht verfügbar", sets.text)
        self.assertIn("keine passenden Kompletträder anzeigen oder bestellen", sets.text)
        self.assertNotIn("<form", sets.text)
        self.assertNotRegex(sets.text, r"\d+[,.]\d{2}\s*€")
        self.cache.load.assert_not_called()

    def test_search_and_facets_publish_only_whitelisted_real_product_fields_and_signed_quantity(self):
        facets = self.client.get("/api/felgenkatalog/auswahl")
        self.assertEqual(facets.status_code, 200)
        self.assert_private(facets)
        self.assertEqual(set(facets.json), {"status", "hersteller", "materialien", "artikel_gesamt", "preisstand", "gueltig_bis"})
        self.assertEqual(facets.json["materialien"], ["alu"])
        response = self.client.get("/api/felgenangebote/suche?anzahl=1")
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        product = response.json["produkte"][0]
        self.assertEqual(set(product), {"artikel_id", "ean", "hersteller_artikelnummer", "hersteller", "profil", "bezeichnung",
            "material", "bestand", "anzahl", "preisstand", "gueltig_bis", "kennungspassung", "preis", "auswahl_url"})
        self.assertEqual(product["preis"], {"einzel_brutto": "130.90", "gesamt_felgen_brutto": "130.90",
            "versand_brutto": "7.14", "gesamt_brutto": "138.04", "waehrung": "EUR"})
        self.assertEqual(product["kennungspassung"], "nicht_geprueft")
        token = urlsplit(product["auswahl_url"]).path.rsplit("/", 1)[1]
        self.assertEqual(self.serializer.loads(token, max_age=900), {"artikel_id": "1", "anzahl": 1})
        self.assertEqual(urlsplit(product["auswahl_url"]).query, "")

    def test_actual_selection_template_is_read_only_and_backlink_preserves_signed_sku_quantity(self):
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        html = response.text
        self.assertIn("Synthetic Wheel Model", html)
        self.assertIn("523,60 €", html)
        self.assertIn("Ausgewählt · 4 Felgen", html)
        self.assertIn("Fahrzeugpassung noch nicht geprüft", html)
        self.assertIn("keine Bestellung", html)
        self.assertIn(self.received.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M"), html)
        self.assertNotRegex(html, r"<(?:form|input|select)\b")
        self.assertNotIn("Zahlung", html)
        back = re.search(r'href="([^"]+)"[^>]*>Felge oder Stückzahl ändern', html).group(1).replace("&amp;", "&")
        self.assertEqual(urlsplit(back).path, "/felgen")
        self.assertEqual(parse_qs(urlsplit(back).query), {"suche": ["1"], "anzahl": ["4"], "anzeigen": ["1"]})
        for forbidden in ("owner_hash", "kunde", "telefon", "reservierung", "payment_id"):
            self.assertNotIn(forbidden, html)

    def test_selection_rechecks_current_price_and_exact_sku_outside_search_page(self):
        self.assertEqual(self.client.get(self.path).status_code, 200)
        rows = [{"Artikelnummer": str(i), "Einkaufspreis": "99,00"} for i in range(2, 40)]
        self.snapshot = parse_felgen_feed(raw_feed(*rows, {"Artikelnummer": "1", "Einkaufspreis": "110,00"}), abgerufen_am=self.received)
        search = self.client.get("/api/felgenangebote/suche")
        self.assertNotIn("1", [product["artikel_id"] for product in search.json["produkte"]])
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        self.assertIn("575,96 €", response.text)
        self.assertIn("Artikel 1", response.text)
        self.assertNotIn("523,60 €", response.text)

    def test_tamper_ttl_cross_category_and_malformed_signed_payload_are_generic_conflicts(self):
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(time.time())-901):
            expired = self.serializer.dumps({"artikel_id": "1", "anzahl": 4})
        cross = URLSafeTimedSerializer(self.app.secret_key, salt="reifen-auswahl-v1").dumps({"artikel_id": "1", "anzahl": 4})
        payload, signature = self.token.rsplit(".", 1)
        tampered = payload + "." + ("A" if signature[0] != "A" else "B") + signature[1:]
        tokens = [expired, cross, tampered, "X"*2001]
        for selected in ({"artikel_id": "1", "anzahl": True}, {"artikel_id": "1", "anzahl": 0},
                         {"artikel_id": "1", "anzahl": 9}, {"artikel_id": "1", "anzahl": "4"},
                         {"artikel_id": "1", "anzahl": 4, "preis": "0.01"}, {"artikel_id": "PRIVATE-MARKER", "anzahl": 4}):
            tokens.append(self.serializer.dumps(selected))
        for token in tokens:
            with self.subTest(token_length=len(token)):
                response = self.client.get("/felgen/auswahl/" + token)
                self.assertEqual(response.status_code, 409)
                self.assert_private(response)
        self.cache.load.assert_not_called()

    def test_selection_refuses_query_price_quantity_and_category_overrides(self):
        for suffix in ("?preis=0.01", "?anzahl=1", "?artikel_id=PRIVATE-MARKER", "?kategorie=reifen", "?anzeigen=1"):
            response = self.client.get(self.path + suffix)
            self.assertEqual(response.status_code, 400)
            self.assert_private(response)
        self.cache.load.assert_not_called()
        invalid = self.client.get("/api/felgenangebote/suche?preis=0.01")
        self.assertEqual(invalid.status_code, 400)
        self.assert_private(invalid)
        duplicate = self.client.get("/api/felgenangebote/suche?anzahl=4&anzahl=1")
        self.assertEqual(duplicate.status_code, 400)
        field = self.client.get("/api/felgenangebote/suche?material=all")
        self.assertEqual(field.status_code, 400)
        self.assertEqual(field.json["feld"], "material")
        self.cache.load.assert_not_called()

    def test_public_catalogue_is_default_denied_before_cache_read(self):
        self.public = True
        self.app.config["FELGENANGEBOT_PROVIDER"] = FelgenCatalogProvider(self.cache, policy=deepcopy(POLICY), clock=lambda: self.now)
        for path in ("/api/felgenkatalog/auswahl", "/api/felgenangebote/suche"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json["code"], "configuration")
            self.assert_private(response)
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 409)
        self.assert_private(response)
        self.cache.load.assert_not_called()
        self.assertEqual(self.client.get("/internal").status_code, 404)

    def test_missing_faulty_stale_stock_and_non_rim_sources_cannot_produce_a_selection(self):
        for provider in (None, lambda _: {"private": "SECRET-PROVIDER"}):
            self.app.config["FELGENANGEBOT_PROVIDER"] = provider
            response = self.client.get(self.path)
            self.assertEqual(response.status_code, 409)
            self.assert_private(response)
        self.app.config["FELGENANGEBOT_PROVIDER"] = self.provider
        self.cache.load.side_effect = RuntimeError("PRIVATE-MARKER https://private.invalid/SECRET-PROVIDER")
        for path, status in ((self.path, 409), ("/api/felgenangebote/suche", 503), ("/api/felgenkatalog/auswahl", 503)):
            response = self.client.get(path)
            self.assertEqual(response.status_code, status)
            self.assert_private(response)
        self.cache.load.side_effect = lambda: self.snapshot
        for row, received in (({"Lagerbestand": "3"}, self.received), ({}, self.now-timedelta(minutes=16)),
                              ({"Artikeltyp": "Reifen", "Artikeluntertyp": "PKW"}, self.received)):
            self.snapshot = parse_felgen_feed(raw_feed(row), abgerufen_am=received)
            response = self.client.get(self.path)
            self.assertEqual(response.status_code, 409)
            self.assert_private(response)

    def test_rate_limit_uses_retry_after_without_cache_or_adapter_calls(self):
        self.limit.return_value = (True, 11)
        for path in (self.path, "/api/felgenangebote/suche", "/api/felgenkatalog/auswahl"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers["Retry-After"], "11")
            self.assert_private(response)
        self.cache.load.assert_not_called()
        self.attempt.assert_not_called()

    def test_get_does_not_change_existing_session_and_existing_post_forms_keep_csrf(self):
        _, image_path, _ = self.install_images()
        self.client.get("/other-form")
        with self.client.session_transaction() as values:
            values["unrelated"] = "synthetic-session-marker"
            before = dict(values)
            csrf = values["csrf_token"]
        for path in ("/felgen", "/komplettraeder", self.path, "/api/felgenangebote/suche", "/api/felgenkatalog/auswahl", image_path):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assert_private(response, cookie_absent=False)
            with self.client.session_transaction() as values:
                self.assertEqual(dict(values), before)
        self.assertEqual(self.client.post("/other-form").status_code, 400)
        self.assertEqual(self.client.post("/other-form", data={"csrf_token": csrf}).status_code, 200)
        self.cache.load.reset_mock()
        for rule in self.app.url_map.iter_rules():
            if rule.endpoint.startswith("felgen_shop."):
                self.assertEqual(rule.methods - {"HEAD", "OPTIONS"}, {"GET"})
                path = image_path if rule.endpoint == "felgen_shop.bild" else rule.rule.replace("<token>", self.token)
                response = self.client.post(path, data={"csrf_token": csrf})
                self.assertEqual(response.status_code, 405)
        self.cache.load.assert_not_called()
        self.assertEqual(self.no_write.mock_calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
