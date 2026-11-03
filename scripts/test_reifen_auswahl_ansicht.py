"""Actual selection route/CSRF hooks with signed tokens and synthetic providers.

Only the relevant app.py functions are compiled into a disposable Flask app.
No portal import, real database, dotenv, provider network or customer data.
"""
import ast
from datetime import datetime, timedelta, timezone
import hmac
from pathlib import Path
import re
import secrets
import sys
import time
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask, abort, jsonify, make_response, request, session, url_for
from reifen_bestellung import erstelle_auswahl_token
from reifen_suche import suche_reifenangebote, ReifenSucheEingabefehler
from scripts.test_tyresystem_provider import POLICY, feed
from tyresystem_provider import IndexedCatalog, TyreSystemCatalogProvider


ARGS = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}


class SelectionViewTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / "app.py"
        names = {"reifen_auswahl", "reifenangebote_suche", "restrict_public_site_service",
                 "get_csrf_token", "csrf_field", "add_csrf_fields", "protect_csrf"}
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        cls.functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        cls.source = str(source)

    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.app = Flask("synthetic-selection-view")
        self.app.testing = True
        self.app.secret_key = "synthetic-selection-secret"
        self.render = Mock(return_value="<html><body>Reifenauswahl</body></html>")
        self.no_write = Mock(side_effect=AssertionError("Database/adapters must not be called"))
        self.env = {"__name__": __name__, "app": self.app, "abort": abort, "request": request,
                    "jsonify": jsonify, "make_response": make_response, "url_for": url_for,
                    "session": session, "re": re, "secrets": secrets, "sys": sys, "hmac": hmac,
                    "datetime": datetime, "timezone": timezone, "ZoneInfo": ZoneInfo,
                    "render_template": self.render, "suche_reifenangebote": suche_reifenangebote,
                    "erstelle_auswahl_token": erstelle_auswahl_token, "ReifenSucheEingabefehler": ReifenSucheEingabefehler,
                    "public_form_rate_limit_status": Mock(return_value=(False, 0)),
                    "record_public_form_attempt": Mock(), "REIFEN_SUCHE_RATE_LIMIT_MAX": 60,
                    "REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS": 60, "CSRF_FIELD_NAME": "csrf_token",
                    "progress_csrf_exempt": lambda _: False, "csrf_recovery_response": lambda: None,
                    "is_public_site_request": lambda: False, "PUBLIC_SITE_ONLY": False,
                    "get_db": self.no_write}
        exec(compile(ast.Module(body=self.functions, type_ignores=[]), self.source, "exec"), self.env)
        self.app.add_url_rule("/reifen", "oeffentlicher_reifenshop", lambda: "shop")
        self.app.add_url_rule("/reifen/bestellen/<token>/kauf", "reifen_kauf.start", lambda token: "checkout")
        self.app.add_url_rule("/other-form", "other_form", lambda: "<form method='post'></form>", methods=["GET", "POST"])
        self.app.extensions["reifen_kauf"] = self.no_write
        self.app.extensions["reifen_bestellung"] = self.no_write
        self.app.config.update(REIFEN_KAUF_MODE="off", REIFEN_KAUF_ENABLED=False,
                               REIFEN_KAUF_SUPPLIER=self.no_write, REIFEN_KAUF_SMS=self.no_write,
                               REIFEN_KAUF_GATEWAY=self.no_write)
        self.received = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.catalog = IndexedCatalog.build(feed({"Artikelnummer": "1", "Einkaufspreis": "100,00"}, received=self.received))
        class Cache:
            def load(_):
                return self.catalog
        self.provider = TyreSystemCatalogProvider(Cache(), policy=POLICY, private_view_authorized=lambda: True)
        self.app.config["REIFENANGEBOT_PROVIDER"] = self.provider
        self.token = erstelle_auswahl_token(self.app.secret_key, ARGS, "1")
        self.path = f"/reifen/bestellen/{self.token}/auswahl"
        self.client = self.app.test_client()

    def assert_private(self, response):
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.assertEqual(response.headers["X-Robots-Tag"], "noindex, nofollow")
        self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")
        self.assertNotIn("Set-Cookie", response.headers)
        self.assertIsNone(self.client.get_cookie("session"))
        self.no_write.assert_not_called()

    def test_signed_real_sku_is_read_only_revalidated_and_has_receipt_based_berlin_times(self):
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        self.assertNotIn("<form", response.text)
        args, context = self.render.call_args
        self.assertEqual(args, ("reifen_auswahl.html",))
        self.assertEqual(context["query"], ARGS)
        self.assertEqual(context["produkt"]["artikel_id"], "1")
        self.assertEqual(context["produkt"]["preis"]["gesamt_brutto"], "523.60")
        self.assertIs(context["sale_ready"], False)
        self.assertNotIn("checkout_url", context)
        self.assertEqual(context["pruefen_url"], self.path)
        self.assertEqual(context["preisstand_text"], self.received.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M"))
        self.assertEqual(context["gueltig_bis_text"], (self.received+timedelta(minutes=15)).astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M"))
        back = urlsplit(context["zurueck_url"])
        self.assertEqual(back.path, "/reifen")
        self.assertEqual(parse_qs(back.query), {key: [value] for key, value in {**ARGS, "anzeigen": "1"}.items()})
        for forbidden in ("netto_einkauf", "fracht_netto", "owner_hash", "kunde", "telefon", "reservierung"):
            self.assertNotIn(forbidden, str(context))
        # A back link must also preserve non-default signed service filters.
        mounted_args = {**ARGS, "qualitaet": "premium", "montage": "ja"}
        self.app.config["REIFENANGEBOT_PROVIDER"] = TyreSystemCatalogProvider(self.provider.cache,
            policy=POLICY, private_view_authorized=lambda: True,
            article_metadata={"1": {"bestaetigt": True, "nachweis": "synthetic workshop fixture",
                "qualitaet": "premium", "runflat": False, "montage_standard_bestaetigt": True}})
        mounted_token = erstelle_auswahl_token(self.app.secret_key, mounted_args, "1")
        response = self.client.get(f"/reifen/bestellen/{mounted_token}/auswahl")
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        mounted = self.render.call_args.kwargs
        self.assertEqual(mounted["query"], mounted_args)
        self.assertEqual(mounted["produkt"]["preis"]["montage_brutto"], "120.00")
        self.assertEqual(parse_qs(urlsplit(mounted["zurueck_url"]).query),
                         {key: [value] for key, value in {**mounted_args, "anzeigen": "1"}.items()})

    def test_price_refresh_and_rank_changes_still_revalidate_exact_sku(self):
        rows = [{"Artikelnummer": str(i), "Einkaufspreis": "99,00"} for i in range(2, 124)]
        self.catalog = IndexedCatalog.build(feed(*rows, {"Artikelnummer": "1", "Einkaufspreis": "110,00"}, received=self.received))
        generic, _ = suche_reifenangebote(ARGS, self.provider)
        self.assertNotIn("1", [p["artikel_id"] for p in generic["produkte"]])
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)
        product = self.render.call_args.kwargs["produkt"]
        self.assertEqual((product["artikel_id"], product["preis"]["gesamt_brutto"]), ("1", "575.96"))

    def test_expired_or_tampered_token_never_reaches_supplier_or_exposes_input(self):
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(time.time())-901):
            expired = erstelle_auswahl_token(self.app.secret_key, ARGS, "1")
        for token in (expired, "PRIVATE-INPUT-" + self.token, "X"*2001):
            with self.subTest(length=len(token)):
                response = self.client.get(f"/reifen/bestellen/{token}/auswahl")
                self.assertEqual(response.status_code, 409)
                self.assert_private(response)
                self.assertNotIn("PRIVATE-INPUT", response.text)
        self.render.assert_not_called()

    def test_stale_stock_and_provider_failure_are_generic_conflicts(self):
        def fail(_):
            raise RuntimeError("PRIVATE-MARKER https://private.invalid/?token=SECRET")
        for item, received, provider in (({"Lagerbestand": "3"}, self.received, self.provider),
                                         ({}, datetime.now(timezone.utc)-timedelta(minutes=16), self.provider),
                                         ({}, self.received, fail), ({}, self.received, None)):
            self.catalog = IndexedCatalog.build(feed({"Artikelnummer": "1", **item}, received=received))
            self.app.config["REIFENANGEBOT_PROVIDER"] = provider
            response = self.client.get(self.path)
            self.assertEqual(response.status_code, 409)
            self.assert_private(response)
            for marker in ("PRIVATE-MARKER", "private.invalid", "SECRET", "netto_einkauf"):
                self.assertNotIn(marker, response.text)
        self.render.assert_not_called()

    def test_query_override_and_rate_limit_are_safe_and_write_nothing(self):
        response = self.client.get(self.path + "?artikel_id=PRIVATE&anzahl=1&preis=0.01")
        self.assertEqual(response.status_code, 400)
        self.assert_private(response)
        self.assertNotIn("PRIVATE", response.text)
        with patch.dict(self.env, {"public_form_rate_limit_status": lambda *a, **kw: (True, 11)}):
            response = self.client.get(self.path)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["Retry-After"], "11")
        self.assert_private(response)
        self.render.assert_not_called()

    def test_existing_post_forms_still_receive_and_require_csrf(self):
        response = self.client.get("/other-form")
        self.assertIn('name="csrf_token"', response.text)
        self.assertIn("Set-Cookie", response.headers)
        self.assertEqual(self.client.post("/other-form").status_code, 400)
        with self.client.session_transaction() as values:
            csrf = values["csrf_token"]
        self.assertEqual(self.client.post("/other-form", data={"csrf_token": csrf}).status_code, 200)
        with self.client.session_transaction() as values:
            before = dict(values)
        response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Set-Cookie", response.headers)
        with self.client.session_transaction() as values:
            self.assertEqual(dict(values), before)

    def test_off_mode_search_link_is_selectable_live_and_test_links_stay_unchanged(self):
        for mode, ending in (("off", "/auswahl"), ("test", "/kauf"), ("live", "/kauf")):
            self.app.config["REIFEN_KAUF_MODE"] = mode
            response = self.client.get("/api/reifenangebote/suche", query_string=ARGS)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json["produkte"][0]["bestell_url"].endswith(ending))
            self.assertNotIn("Set-Cookie", response.headers)
        with patch.dict(self.env, {"is_public_site_request": lambda: True}):
            response = self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assert_private(response)


if __name__ == "__main__":
    unittest.main(verbosity=2)
