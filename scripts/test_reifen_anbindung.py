"""Offline integration-wiring checks; no application DB, provider IO or workers."""
from copy import deepcopy
from contextlib import closing
from email.message import EmailMessage
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask, abort, session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import reifen_anbindung as wiring


PHONE = "+4915100000011"
REFERENCE = "synthetic-order-000001"


class Service:
    def __init__(self):
        self.ready = True
        self.value = {"id":REFERENCE,"mode":"test","telefon":PHONE,
                      "query_json":json.dumps({"produktart":"reifen"}), "produkt_json":"{}"}
        self.references = []
        self.category_checks = []

    def readiness(self):
        return {"ready":self.ready}

    def readiness_for(self, category):
        self.category_checks.append(category)
        return {"ready":self.ready, "produktart":category}

    def category(self, query, product):
        kind = query["produktart"]
        if kind not in {"reifen", "felgen"}:
            raise ValueError("unconfigured category")
        return kind

    def row(self, reference):
        if reference != REFERENCE or session.get("synthetic_owner") != "owned":
            abort(404)
        self.references.append(reference)
        return deepcopy(self.value)


class ReifenAnbindungTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reifen-config-offline-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "wiring.sqlite3"
        self.app = Flask(__name__)
        self.app.secret_key = "synthetic-tests-only"
        self.app.testing = True
        for path,endpoint in (("/code","reifen_kauf.telefon_code"),("/check","reifen_kauf.telefon_pruefen"),("/other","other")):
            self.app.add_url_rule(path,endpoint,lambda: "unused",methods=["GET","POST"])
        self.service = Service()
        self.app.extensions["reifen_kauf"] = self.service
        self.portal = SimpleNamespace(app=self.app,get_db=self.db)
        with closing(self.db()) as db, db:
            db.execute("CREATE TABLE reifen_kaeufe(id TEXT,state TEXT,erstellt_am TEXT)")
        self.addCleanup(patch.stopall)
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            patch(target,side_effect=AssertionError("Real network forbidden")).start()
        patch("threading.Thread.start",side_effect=AssertionError("Real workers forbidden")).start()

    def db(self):
        db=sqlite3.connect(self.path)
        db.row_factory=sqlite3.Row
        return db

    def configure_sms(self, **change):
        captured={}
        def factory(db, *, environment, authorize):
            captured.update(db=db,environment=environment,authorize=authorize)
            return SimpleNamespace(name="synthetic-sms")
        env={"REIFEN_KAUF_MODE":"test","REIFEN_TWILIO_ACCOUNT_SID":"AC"+"a"*32,
            "REIFEN_TWILIO_AUTH_TOKEN":"synthetic-unusable-secret","REIFEN_TWILIO_VERIFY_SERVICE_SID":"VA"+"b"*32,
            "REIFEN_SMS_TESTNUMMERN":PHONE,**change}
        with patch("reifen_sms.TwilioVerifySMS.from_env",side_effect=factory):
            wiring.configure_integrations(self.portal,environment=env)
        return captured

    def authorized(self, callback, path, operation, *, method="POST", owned=True, reference=REFERENCE):
        with self.app.test_request_context(path,method=method):
            if owned:
                session["synthetic_owner"]="owned"
            return callback(operation,reference)

    def smtp_env(self, **changes):
        return {"REIFEN_KAUF_MODE":"test","REIFEN_KAUF_MAIL_ENABLED":"1",
            "REIFEN_SMTP_HOST":"smtp.synthetic.invalid","REIFEN_SMTP_USER":"synthetic-tire-user",
            "REIFEN_SMTP_PASSWORD":"synthetic-tire-password","REIFEN_SMTP_FROM":"tires@synthetic.invalid",
            "REIFEN_MAIL_TESTEMPFAENGER":"allowed@synthetic.invalid",**changes}

    def message(self, to="allowed@synthetic.invalid"):
        message=EmailMessage()
        message["To"]=to
        message.set_content("Synthetic isolated message")
        return message

    def test_default_off_does_not_reuse_other_business_credentials(self):
        env={"STRIPE_SECRET_KEY":"sk_live_unrelated","MOS_STRIPE_SECRET_KEY":"sk_live_rental",
            "TWILIO_AUTH_TOKEN":"unrelated","TWILIO_API_KEY_SID":"SK"+"d"*32,
            "TWILIO_API_KEY_SECRET":"synthetic-unrelated-secret",
            "SMTP_HOST":"smtp.other.invalid","SMTP_USER":"other",
            "SMTP_PASSWORD":"other", "MOS_SMTP_PASSWORD":"rental", "SCHADEN_SMTP_PASSWORD":"insurance"}
        with patch("reifen_sms.TwilioVerifySMS.from_env") as sms,patch("reifen_zahlung.StripeReifenGateway") as gateway:
            self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
        self.assertEqual(self.app.config["REIFEN_KAUF_MODE"],"off")
        self.assertIs(self.app.config["REIFEN_KAUF_ENABLED"],False)
        self.assertIs(self.app.config["REIFEN_KAUF_MAIL_ENABLED"],False)
        self.assertEqual(self.app.config["REIFEN_SMS_MAX_STARTS_PRO_TAG"],0)
        for key in ("REIFEN_KAUF_SMS","REIFEN_KAUF_GATEWAY","REIFEN_KAUF_SUPPLIER","REIFEN_KAUF_MAILER"):
            self.assertNotIn(key,self.app.config)
        sms.assert_not_called()
        gateway.assert_not_called()

    def test_sms_authorization_uses_the_owned_product_category(self):
        callback = self.configure_sms()["authorize"]
        self.service.value["query_json"] = json.dumps({"produktart":"felgen"})
        self.assertTrue(self.authorized(callback,"/code","start"))
        self.assertEqual(self.service.category_checks, ["felgen"])
        with patch.object(self.service, "readiness_for", return_value={"ready":True,"produktart":"reifen"}):
            self.assertFalse(self.authorized(callback,"/code","start"))
        with patch.object(self.service, "readiness_for", return_value={"ready":1,"produktart":"felgen"}):
            self.assertFalse(self.authorized(callback,"/code","start"))
        self.service.value["query_json"] = "invalid-json"
        self.assertFalse(self.authorized(callback,"/code","start"))

    def catalog_environment(self):
        root=Path(self.temp.name)
        policy={"currency":"EUR","price_basis":"net_unit","single_tyre_shipping_net":"6.00",
                "free_shipping_from":2,"delivery_text":"Standardlieferung 1–4 Werktage; keine Reservierung.",
                "verified_on":"2026-11-02","sources":{"price":"https://www.tyresystem.de/agb",
                "shipping":"https://www.tyresystem.de/lieferbedingungen","delivery":"https://www.tyresystem.de/leistungen"}}
        path=root/"policy.json"
        path.write_text(json.dumps(policy),encoding="utf-8")
        return {"TYRESYSTEM_PRIVATE_ROOT":str(root),"TYRESYSTEM_CATALOG_DIR":str(root/"catalog"),
                "TYRESYSTEM_CATALOG_POLICY_FILE":str(path),"TYRESYSTEM_CATALOG_PUBLICATION_APPROVED":"1",
                "TYRESYSTEM_CATALOG_PUBLICATION_REFERENCE":"synthetic-release-only"}

    def test_catalog_wiring_is_explicit_without_cache_network_database_or_worker_io(self):
        from tyresystem_provider import TyreSystemCatalogProvider
        env=self.catalog_environment()
        with patch("tyresystem_provider.TyreSystemCatalogCache.load") as load,patch.object(self.portal,"get_db") as db:
            self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
        self.assertIsInstance(self.app.config["REIFENANGEBOT_PROVIDER"],TyreSystemCatalogProvider)
        self.assertTrue(self.app.config["REIFENANGEBOT_PROVIDER"].publication_ready)
        load.assert_not_called()
        db.assert_not_called()
        self.assertFalse((Path(self.temp.name)/"catalog").exists())
        self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])

    def test_catalog_revocation_or_invalid_reconfiguration_removes_old_provider(self):
        env=self.catalog_environment()
        for change in ({"TYRESYSTEM_CATALOG_PUBLICATION_APPROVED":"0"},
                       {"TYRESYSTEM_CATALOG_PUBLICATION_REFERENCE":""},
                       {"TYRESYSTEM_CATALOG_POLICY_FILE":str(Path(self.temp.name)/"missing.json")},None):
            with self.subTest(change=change):
                self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
                self.assertIn("REIFENANGEBOT_PROVIDER",self.app.config)
                current={} if change is None else {**env,**change}
                errors=wiring.configure_integrations(self.portal,environment=current)
                self.assertNotIn("REIFENANGEBOT_PROVIDER",self.app.config)
                self.assertNotIn("reifen_tyresystem_catalog_provider",self.app.extensions)
                self.assertEqual(bool(errors),change is not None and "TYRESYSTEM_CATALOG_PUBLICATION_APPROVED" not in change)

    def test_unapproved_catalog_never_reads_config_and_absent_settings_preserve_injected_provider(self):
        injected=Mock()
        self.app.config["REIFENANGEBOT_PROVIDER"]=injected
        wiring.configure_integrations(self.portal,environment={})
        self.assertIs(self.app.config["REIFENANGEBOT_PROVIDER"],injected)
        with patch("reifen_anbindung._private_catalog_json") as read:
            wiring.configure_integrations(self.portal,environment={"TYRESYSTEM_CATALOG_DIR":"not-a-private-path"})
        read.assert_not_called()
        self.assertNotIn("REIFENANGEBOT_PROVIDER",self.app.config)

    def rim_environment(self):
        from scripts.test_felgen_katalog import POLICY
        root = Path(self.temp.name) / "rim-private"
        root.mkdir(exist_ok=True)
        policy = root / "rim-policy.json"
        policy.write_text(json.dumps(deepcopy(POLICY)), encoding="utf-8")
        return {"FELGEN_PRIVATE_ROOT":str(root), "FELGEN_CATALOG_DIR":str(root / "catalog"),
                "FELGEN_CATALOG_POLICY_FILE":str(policy), "FELGEN_CATALOG_PUBLICATION_APPROVED":"1",
                "FELGEN_CATALOG_PUBLICATION_REFERENCE":"synthetic-rim-data-release"}

    def test_rim_wiring_installs_only_public_read_provider_without_cache_database_network_or_worker_io(self):
        from felgen_katalog import FelgenCatalogProvider
        env = self.rim_environment()
        with patch("felgen_katalog.FelgenCatalogCache.load", side_effect=AssertionError("No cache read at configure")) as load, \
                patch("felgen_katalog.FelgenCatalogCache.replace", side_effect=AssertionError("No cache write")) as replace, \
                patch.object(self.portal,"get_db",side_effect=AssertionError("No database")) as db:
            self.assertEqual(wiring.configure_integrations(self.portal, environment=env), [])
        provider = self.app.config["FELGENANGEBOT_PROVIDER"]
        self.assertIsInstance(provider, FelgenCatalogProvider)
        self.assertTrue(provider.publication_ready)
        self.assertIsNone(provider._authorize)
        self.assertIsNone(provider._image_resolver)
        self.assertIs(self.app.extensions["felgen_tyresystem_catalog_provider"], provider)
        self.assertFalse(Path(env["FELGEN_CATALOG_DIR"]).exists())
        load.assert_not_called()
        replace.assert_not_called()
        db.assert_not_called()
        self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])
        self.assertNotIn("REIFEN_KAUF_SUPPLIER", self.app.config)
        self.assertNotIn("FELGEN_BILD_STORE", self.app.config)

    def test_rim_revocation_removed_settings_and_invalid_reconfiguration_remove_managed_provider(self):
        env = self.rim_environment()
        for change in ({"FELGEN_CATALOG_PUBLICATION_APPROVED":"0"},
                       {"FELGEN_CATALOG_PUBLICATION_REFERENCE":""},
                       {"FELGEN_CATALOG_POLICY_FILE":str(Path(self.temp.name)/"PRIVATE-missing.json")}, None):
            with self.subTest(change=change):
                self.assertEqual(wiring.configure_integrations(self.portal,environment=env), [])
                self.assertIn("FELGENANGEBOT_PROVIDER", self.app.config)
                errors = wiring.configure_integrations(self.portal, environment={} if change is None else {**env, **change})
                self.assertNotIn("FELGENANGEBOT_PROVIDER", self.app.config)
                self.assertNotIn("felgen_tyresystem_catalog_provider", self.app.extensions)
                self.assertEqual(bool(errors), change is not None and "FELGEN_CATALOG_PUBLICATION_APPROVED" not in change)
                self.assertNotIn("PRIVATE", str(errors))

    def test_rim_unapproved_or_missing_reference_is_denied_before_private_config_or_cache_construction(self):
        injected = object()
        self.app.config["FELGENANGEBOT_PROVIDER"] = injected
        self.assertEqual(wiring.configure_felgen_catalog(self.portal, {}), [])
        self.assertIs(self.app.config["FELGENANGEBOT_PROVIDER"], injected)
        env = self.rim_environment()
        with patch("reifen_anbindung._private_catalog_json") as read, patch("felgen_katalog.FelgenCatalogCache") as cache:
            for changes in ({"FELGEN_CATALOG_PUBLICATION_APPROVED":"0"},
                            {"FELGEN_CATALOG_PUBLICATION_REFERENCE":""},
                            {"FELGEN_CATALOG_PUBLICATION_REFERENCE":" "},
                            {"FELGEN_CATALOG_PUBLICATION_REFERENCE":"x"*201},
                            {"FELGEN_CATALOG_PUBLICATION_REFERENCE":"bad\u200bref"}):
                self.app.config["FELGENANGEBOT_PROVIDER"] = injected
                wiring.configure_felgen_catalog(self.portal, {**env, **changes})
                self.assertNotIn("FELGENANGEBOT_PROVIDER", self.app.config)
            read.assert_not_called()
            cache.assert_not_called()

    def test_rim_private_policy_and_cache_paths_are_bounded_local_and_fail_closed(self):
        env = self.rim_environment()
        root = Path(env["FELGEN_PRIVATE_ROOT"])
        public = root / "static"
        public.mkdir()
        (public / "policy.json").write_text("{}", encoding="utf-8")
        for change in ({"FELGEN_CATALOG_DIR":str(root)},
                       {"FELGEN_CATALOG_DIR":str(Path(self.temp.name)/"outside")},
                       {"FELGEN_CATALOG_DIR":str(public/"catalog")},
                       {"FELGEN_CATALOG_POLICY_FILE":str(public/"policy.json")},
                       {"FELGEN_CATALOG_POLICY_FILE":str(Path(self.temp.name)/"outside-policy.json")},
                       {"FELGEN_CATALOG_POLICY_FILE":"relative-policy.json"},
                       {"FELGEN_CATALOG_POLICY_FILE":"https://PRIVATE.invalid/policy.json"}):
            with self.subTest(change=change):
                self.assertEqual(wiring.configure_felgen_catalog(self.portal, {**env, **change}),
                                 ["Felgenkatalog-Konfiguration oder Datenfreigabe unvollständig"])
                self.assertNotIn("FELGENANGEBOT_PROVIDER", self.app.config)
        with patch("reifen_anbindung.Path.resolve") as resolve:
            self.assertTrue(wiring.configure_felgen_catalog(self.portal, {**env, "FELGEN_PRIVATE_ROOT":"//PRIVATE/share"}))
        resolve.assert_not_called()
        path = Path(env["FELGEN_CATALOG_POLICY_FILE"])
        for raw in ('{"currency":"EUR","currency":"PRIVATE"}', 'x'*16385, '['*2000+'0'+']'*2000):
            path.write_text(raw, encoding="utf-8")
            errors = wiring.configure_felgen_catalog(self.portal, env)
            self.assertEqual(errors, ["Felgenkatalog-Konfiguration oder Datenfreigabe unvollständig"])
            self.assertNotIn("FELGENANGEBOT_PROVIDER", self.app.config)
        self.rim_environment()
        original = Path.is_symlink
        with patch.object(Path, "is_symlink", lambda value: value == path or original(value)):
            self.assertTrue(wiring.configure_felgen_catalog(self.portal, env))

    def test_tire_and_rim_catalog_configuration_are_independent_and_require_exact_own_policy(self):
        env = {**self.catalog_environment(), **self.rim_environment()}
        self.assertEqual(wiring.configure_integrations(self.portal, environment=env), [])
        tire = self.app.config["REIFENANGEBOT_PROVIDER"]
        self.assertTrue(self.app.config["FELGENANGEBOT_PROVIDER"].publication_ready)
        self.assertEqual(wiring.configure_felgen_catalog(self.portal, {**env, "FELGEN_CATALOG_PUBLICATION_APPROVED":"0"}), [])
        self.assertIs(self.app.config["REIFENANGEBOT_PROVIDER"], tire)
        self.assertEqual(wiring.configure_felgen_catalog(self.portal, env), [])
        rim = self.app.config["FELGENANGEBOT_PROVIDER"]
        self.assertEqual(wiring.configure_catalog(self.portal, {**env, "TYRESYSTEM_CATALOG_PUBLICATION_APPROVED":"0"}), [])
        self.assertIs(self.app.config["FELGENANGEBOT_PROVIDER"], rim)
        Path(env["FELGEN_CATALOG_POLICY_FILE"]).write_text(Path(env["TYRESYSTEM_CATALOG_POLICY_FILE"]).read_text(encoding="utf-8"), encoding="utf-8")
        self.assertTrue(wiring.configure_felgen_catalog(self.portal, env))
        self.assertNotIn("FELGENANGEBOT_PROVIDER", self.app.config)

    def test_private_catalog_config_rejects_duplicate_keys_public_path_and_oversize(self):
        root=Path(self.temp.name)
        path=root/"policy.json"
        for raw in (b'{"a":1,"a":2}',b'[]',b'x'*17):
            path.write_bytes(raw)
            with self.assertRaises(ValueError):
                wiring._private_catalog_json(str(path),str(root),max_bytes=16)
        public=root/"static"
        public.mkdir()
        path=public/"policy.json"
        path.write_text("{}",encoding="utf-8")
        with self.assertRaises(ValueError):
            wiring._private_catalog_json(str(path),str(root),max_bytes=16)

    def test_catalog_unc_is_rejected_before_resolution_and_deep_json_fails_closed(self):
        with patch("reifen_anbindung.Path.resolve") as resolve:
            with self.assertRaises(ValueError):
                wiring._private_catalog_json("//synthetic.invalid/private/policy.json",self.temp.name,max_bytes=16)
        resolve.assert_not_called()
        env=self.catalog_environment()
        Path(env["TYRESYSTEM_CATALOG_POLICY_FILE"]).write_text("["*2000+"0"+"]"*2000,encoding="utf-8")
        errors=wiring.configure_integrations(self.portal,environment=env)
        self.assertEqual(errors,["Reifenkatalog-Konfiguration oder Datenfreigabe unvollständig"])
        self.assertNotIn("REIFENANGEBOT_PROVIDER",self.app.config)

    def test_sms_key_configuration_constructs_without_database_or_provider_io(self):
        from reifen_sms import TwilioVerifySMS
        env={"REIFEN_KAUF_MODE":"test","REIFEN_TWILIO_ACCOUNT_SID":"AC"+"a"*32,
            "REIFEN_TWILIO_API_KEY_SID":"SK"+"d"*32,
            "REIFEN_TWILIO_API_KEY_SECRET":"synthetic-unusable-key-secret",
            "REIFEN_TWILIO_VERIFY_SERVICE_SID":"VA"+"b"*32,
            "TWILIO_AUTH_TOKEN":"unrelated-token-must-not-be-inherited"}
        with patch.object(self.portal,"get_db") as db,patch("reifen_sms.requests.Session") as http:
            self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
            self.assertIsInstance(self.app.config["REIFEN_KAUF_SMS"],TwilioVerifySMS)
            self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])
            self.assertFalse(self.app.config["REIFEN_SMS_KOSTEN_FREIGEGEBEN"])
            self.assertEqual(self.app.config["REIFEN_SMS_MAX_STARTS_PRO_TAG"],0)
            db.assert_not_called()
            http.assert_not_called()

    def test_sms_invalid_or_removed_credentials_never_retain_previous_adapter(self):
        for invalid in ({"REIFEN_TWILIO_API_KEY_SID":"SK"+"d"*32},
                        {"REIFEN_TWILIO_API_KEY_SECRET":"synthetic-secret-only"},
                        {"REIFEN_TWILIO_ACCOUNT_SID":"AC"+"a"*32,
                         "REIFEN_TWILIO_VERIFY_SERVICE_SID":"VA"+"b"*32,
                         "REIFEN_TWILIO_AUTH_TOKEN":"synthetic-token",
                         "REIFEN_TWILIO_API_KEY_SID":"SK"+"d"*32,
                         "REIFEN_TWILIO_API_KEY_SECRET":"synthetic-key-secret"}, {}):
            with self.subTest(fields=list(invalid)):
                self.configure_sms()
                self.assertIn("REIFEN_KAUF_SMS",self.app.config)
                with patch.object(self.portal,"get_db") as db,patch("reifen_sms.requests.Session") as http:
                    errors=wiring.configure_integrations(self.portal,environment=invalid)
                self.assertEqual(errors,["SMS-Zugang unvollständig"] if invalid else [])
                self.assertNotIn("REIFEN_KAUF_SMS",self.app.config)
                db.assert_not_called()
                http.assert_not_called()

    def test_sms_callback_requires_exact_operation_route_post_and_current_owner(self):
        captured=self.configure_sms()
        callback=captured["authorize"]
        self.assertFalse(callback("start",REFERENCE))
        for path,operation in (("/code","start"),("/check","check")):
            self.assertTrue(self.authorized(callback,path,operation))
            self.assertFalse(self.authorized(callback,path,operation,method="GET"))
        for path,operation in (("/check","start"),("/code","check"),("/other","start"),("/code","wrong")):
            self.assertFalse(self.authorized(callback,path,operation))
        for options in ({"owned":False},{"reference":"someone-elses-order"}):
            with self.assertRaises(Exception) as denied:
                self.authorized(callback,"/code","start",**options)
            self.assertEqual(denied.exception.code,404)

    def test_sms_test_numbers_modes_and_checkout_readiness_are_required(self):
        callback=self.configure_sms()["authorize"]
        self.service.value["telefon"]="+4915100000022"
        self.assertFalse(self.authorized(callback,"/code","start"))
        self.service.value["telefon"]=PHONE
        self.service.ready=False
        self.assertFalse(self.authorized(callback,"/code","start"))
        self.service.ready=True
        self.service.value["mode"]="live"
        self.assertFalse(self.authorized(callback,"/code","start"))
        self.app.config["REIFEN_KAUF_MODE"]="live"
        self.assertTrue(self.authorized(callback,"/code","start"))

    def test_stripe_has_only_explicit_tire_settings_and_current_mode(self):
        env={"REIFEN_KAUF_MODE":"test","REIFEN_STRIPE_SECRET_KEY":"sk_test_unusable",
            "REIFEN_STRIPE_WEBHOOK_SECRET":"whsec_unusable","REIFEN_STRIPE_MODE":"live",
            "REIFEN_STRIPE_PUBLIC_ORIGIN":"https://tires.synthetic.invalid",
            "REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS":"tires.synthetic.invalid, other.synthetic.invalid",
            "STRIPE_SECRET_KEY":"sk_live_ignored","MOS_STRIPE_SECRET_KEY":"sk_live_ignored_rental"}
        with patch("reifen_zahlung.StripeReifenGateway") as gateway:
            wiring.configure_integrations(self.portal,environment=env)
        settings=gateway.call_args.args[0]
        self.assertEqual(settings["REIFEN_STRIPE_MODE"],"test")
        self.assertEqual(settings["REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS"],["tires.synthetic.invalid","other.synthetic.invalid"])
        self.assertTrue(all(key.startswith("REIFEN_STRIPE_") for key in settings))
        self.assertNotIn("sk_live_ignored",str(settings))
        self.assertNotIn("REIFEN_KAUF_SUPPLIER",self.app.config)

    def test_incomplete_access_is_redacted_and_never_inherits_a_fallback(self):
        env=self.smtp_env(REIFEN_SMTP_PORT="-1",REIFEN_TWILIO_AUTH_TOKEN="synthetic-secret",
            REIFEN_STRIPE_SECRET_KEY="sk_test_synthetic",REIFEN_STRIPE_WEBHOOK_SECRET="whsec_synthetic")
        with patch("reifen_sms.TwilioVerifySMS.from_env",side_effect=ValueError("synthetic-secret")),patch("reifen_zahlung.StripeReifenGateway",side_effect=ValueError("sk_test_synthetic")):
            errors=wiring.configure_integrations(self.portal,environment=env)
        self.assertEqual(len(errors),3)
        self.assertNotIn("synthetic-secret",str(errors))
        self.assertNotIn("sk_test_synthetic",str(errors))
        self.assertFalse(self.app.config["REIFEN_KAUF_MAIL_ENABLED"])

    def test_mail_uses_own_tls_settings_and_explicit_test_recipient_allowlist(self):
        env=self.smtp_env(SMTP_HOST="smtp.unrelated.invalid",MOS_SMTP_PASSWORD="unrelated-rental-secret")
        self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
        mailer=self.app.config["REIFEN_KAUF_MAILER"]
        with patch("mos_contract_delivery._smtp_send",return_value="sent") as smtp:
            self.assertEqual(mailer(self.message("blocked@synthetic.invalid")),"not_sent")
            smtp.assert_not_called()
            self.assertEqual(mailer(self.message("Test <ALLOWED@SYNTHETIC.INVALID>")),"sent")
            settings=smtp.call_args.args[1]
            self.assertEqual(settings,{"smtp_host":"smtp.synthetic.invalid","smtp_port":465,"smtp_ssl":True,
                "smtp_tls":False,"smtp_user":"synthetic-tire-user","_smtp_password":"synthetic-tire-password",
                "from_address":"tires@synthetic.invalid"})
            self.app.config["REIFEN_KAUF_MAIL_ENABLED"]=False
            self.assertEqual(mailer(self.message()),"not_sent")
            self.assertEqual(smtp.call_count,1)

    def test_mail_live_mode_and_starttls_are_explicit(self):
        wiring.configure_integrations(self.portal,environment=self.smtp_env(REIFEN_KAUF_MODE="live",REIFEN_SMTP_TLS_MODE="starttls",REIFEN_SMTP_PORT="587"))
        with patch("mos_contract_delivery._smtp_send",return_value="sent") as smtp:
            self.assertEqual(self.app.config["REIFEN_KAUF_MAILER"](self.message("real-customer@synthetic.invalid")),"sent")
            settings=smtp.call_args.args[1]
            self.assertTrue(settings["smtp_tls"])
            self.assertFalse(settings["smtp_ssl"])
            self.assertEqual(settings["smtp_port"],587)

    def test_mail_requested_without_own_credentials_is_disabled(self):
        errors=wiring.configure_integrations(self.portal,environment={"REIFEN_KAUF_MAIL_ENABLED":"1","SMTP_HOST":"smtp.unrelated.invalid","SMTP_USER":"other","SMTP_PASSWORD":"other"})
        self.assertEqual(errors,["Vertragsmail-Zugang fehlt"])
        self.assertFalse(self.app.config["REIFEN_VERTRAG_MAIL_FREIGABE"]())
        self.assertNotIn("REIFEN_KAUF_MAILER",self.app.config)

    def test_canonical_terms_file_is_immutable_and_tamper_fails_closed(self):
        value={key:"Synthetic legal "+key for key in ("version","agb","widerruf","widerrufsformular","datenschutz","lieferung")}
        canonical=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"))
        value["sha256"]=hashlib.sha256(canonical.encode()).hexdigest()
        path=Path(self.temp.name)/"terms.json"
        path.write_text(json.dumps(value),encoding="utf-8")
        env={"REIFEN_TERMS_FILE":str(path),"REIFEN_TERMS_APPROVED_SHA256":value["sha256"]}
        self.assertEqual(wiring.configure_integrations(self.portal,environment=env),[])
        provider=self.app.config["REIFEN_KAUF_RECHTSTEXTE"]
        self.assertEqual(provider(),value)
        one=provider()
        one["agb"]="changed externally"
        self.assertEqual(provider(),value)
        value["agb"]="changed on disk without reapproval"
        path.write_text(json.dumps(value),encoding="utf-8")
        self.assertEqual(wiring.configure_integrations(self.portal,environment=env),["Bedingungsdatei ungültig"])
        self.assertEqual(self.app.config["REIFEN_TERMS_APPROVED_SHA256"],"")

    def test_terms_reject_non_json_incomplete_and_oversize_files(self):
        path=Path(self.temp.name)/"invalid-terms.json"
        for raw in (b"not json",b"[]",b"{}",b"x"*300001):
            path.write_bytes(raw)
            with self.assertRaises(ValueError):
                wiring.read_terms(path)

    def auto_environment(self):
        policy = Path(self.temp.name) / "auto-policy.json"
        policy.write_text(json.dumps({"schema":1}), encoding="utf-8")
        return {"REIFEN_BESCHAFFUNG_VERFAHREN":"tyresystem_auto_v1", "REIFEN_KAUF_MODE":"test",
                "TYRESYSTEM_PRIVATE_ROOT":self.temp.name, "TYRESYSTEM_AUTO_POLICY_FILE":str(policy),
                "TYRESYSTEM_API_USERNAME":"synthetic-api-user", "TYRESYSTEM_API_PASSWORD":"synthetic-api-secret",
                "TYRESYSTEM_API_MODE":"test"}

    def test_auto_configuration_only_constructs_dedicated_clients_without_io_or_activation(self):
        from tyresystem_api import TyreSystemInquiryClient
        from tyresystem_order import DurableTyreSystemOrders
        with patch.object(self.portal, "get_db") as db:
            self.assertEqual(wiring.configure_integrations(self.portal, environment=self.auto_environment()), [])
        db.assert_not_called()
        self.assertIsInstance(self.app.config["REIFEN_TYRESYSTEM_INQUIRY"], TyreSystemInquiryClient)
        self.assertIsInstance(self.app.config["REIFEN_TYRESYSTEM_ORDERS"], DurableTyreSystemOrders)
        self.assertEqual(self.app.config["REIFEN_TYRESYSTEM_AUTO_POLICY"], {"schema":1})
        self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])
        self.assertNotIn("REIFEN_KAUF_SUPPLIER", self.app.config)
        self.assertEqual(self.app.config["REIFEN_TYRESYSTEM_INQUIRY"].account_fingerprint,
                         self.app.config["REIFEN_TYRESYSTEM_ORDERS"].account_fingerprint)

    def test_auto_removal_incomplete_or_invalid_configuration_revokes_own_clients(self):
        env = self.auto_environment()
        keys = ("REIFEN_TYRESYSTEM_INQUIRY", "REIFEN_TYRESYSTEM_ORDERS", "REIFEN_TYRESYSTEM_AUTO_POLICY")
        for changes in (None, {"TYRESYSTEM_API_PASSWORD":""}, {"TYRESYSTEM_AUTO_POLICY_FILE":str(Path(self.temp.name)/"missing.json")},
                        {"REIFEN_BESCHAFFUNG_VERFAHREN":"unknown"}):
            with self.subTest(changes=changes):
                self.assertEqual(wiring.configure_integrations(self.portal, environment=env), [])
                current = {} if changes is None else {**env, **changes}
                errors = wiring.configure_integrations(self.portal, environment=current)
                self.assertEqual(bool(errors), changes is not None)
                for key in keys:
                    self.assertNotIn(key, self.app.config)
        self.assertEqual(wiring.configure_integrations(self.portal, environment={"TYRESYSTEM_API_MODE":"test"}), [])

    def test_auto_policy_path_stays_private_and_unknown_flow_never_enables_orders(self):
        env = self.auto_environment()
        self.app.config["REIFEN_KAUF_ENABLED"] = True
        for value in ("unknown", [], True):
            with self.subTest(value=value):
                self.assertTrue(wiring.configure_integrations(self.portal, environment={**env,
                    "REIFEN_KAUF_ENABLED":"1", "REIFEN_BESCHAFFUNG_VERFAHREN":value}))
                self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])
        for bad_path in ("relative.json", str(Path(self.temp.name).parent / "outside.json"), "https://synthetic.invalid/policy"):
            with self.subTest(path=bad_path):
                self.assertTrue(wiring.configure_integrations(self.portal, environment={**env,"TYRESYSTEM_AUTO_POLICY_FILE":bad_path}))
                self.assertNotIn("REIFEN_TYRESYSTEM_ORDERS", self.app.config)

    def test_order_transport_requires_current_core_authorization_even_after_configured(self):
        from tyresystem_order import TyreSystemOrderError, freeze_order_request
        self.assertEqual(wiring.configure_integrations(self.portal, environment=self.auto_environment()), [])
        transport = self.app.config["REIFEN_TYRESYSTEM_ORDERS"].client
        request = freeze_order_request({"produktart":"reifen", "artikel_id":"123", "ean":"4019238594195",
            "anzahl":4,"einkauf_netto":"42.28","waehrung":"EUR","reference":"synthetic-order-reference",
            "delivery_address":{"neutraldelivery":0,"salutation":"Firma","name":"Synthetic Workshop",
                "street":"Teststrasse 1","zipcode":"74821","city":"Mosbach","countrycode":"DE"}})
        with self.assertRaises(TyreSystemOrderError):
            transport.check_authorized(request)
        self.service.auto_authorize_order = Mock(return_value=1)
        with self.assertRaises(TyreSystemOrderError):
            transport.check_authorized(request)
        self.service.auto_authorize_order.return_value = True
        transport.check_authorized(request)
        envelope = self.service.auto_authorize_order.call_args.args[0]
        self.assertEqual(envelope["reference"], request.reference)
        self.assertEqual(envelope["account_fingerprint"], transport.account_fingerprint)
        self.app.extensions.pop("reifen_kauf")
        with self.assertRaises(TyreSystemOrderError):
            transport.check_authorized(request)

    def test_pending_worker_is_bounded_and_reconciles_without_browser_request(self):
        with closing(self.db()) as db, db:
            db.executemany("INSERT INTO reifen_kaeufe VALUES(?,?,?)",[(str(i),"zahlung_offen" if i%2 else "zahlung_bestaetigt",f"2026-01-{i+1:02d}") for i in range(25)])
            db.execute("INSERT INTO reifen_kaeufe VALUES('draft','entwurf','2025-01-01')")
            db.execute("INSERT INTO reifen_kaeufe VALUES('review','pruefung_noetig','2025-01-01')")
            db.execute("INSERT INTO reifen_kaeufe VALUES('final','bestaetigt','2025-01-01')")
        reconciled=[]
        def reconcile(identifier):
            reconciled.append(identifier)
            if identifier=="1":
                raise RuntimeError("Synthetic unknown result")
        self.service.reconcile=reconcile
        mailer=Mock()
        self.app.config["REIFEN_KAUF_MAILER"]=mailer
        with patch("reifen_vertrag.deliver_pending") as deliver:
            wiring.run_pending(self.portal)
        self.assertEqual(reconciled,[str(i) for i in range(20)])
        deliver.assert_called_once_with(self.portal,mailer,limit=10)
        mailer.assert_not_called()

    def test_worker_default_off_and_enabled_singleton_never_depends_on_return_url(self):
        with patch.dict("os.environ",{},clear=True),patch("reifen_anbindung.threading.Thread") as thread:
            self.assertIsNone(wiring.start_worker(self.portal))
            thread.assert_not_called()
        stop=Mock()
        stop.wait.side_effect=[False,True]
        with patch.dict("os.environ",{"REIFEN_KAUF_WORKER_ENABLED":"1"},clear=True),patch("reifen_anbindung.threading.Event",return_value=stop),patch("reifen_anbindung.threading.Thread") as thread,patch("reifen_anbindung.run_pending") as pending:
            self.assertIs(wiring.start_worker(self.portal),stop)
            thread.assert_called_once()
            self.assertTrue(thread.call_args.kwargs["daemon"])
            self.assertIs(self.app.extensions["reifen_kauf_worker_thread"],thread.return_value)
            self.assertIs(self.app.extensions["reifen_kauf_worker"],stop)
            thread.return_value.start.assert_called_once()
            thread.call_args.kwargs["target"]()
            pending.assert_called_once_with(self.portal)
            self.assertEqual(stop.wait.call_args.args,(30,))
            self.assertIs(wiring.start_worker(self.portal),stop)
            thread.assert_called_once()

    def test_pending_worker_includes_safe_auto_recovery_with_fair_bounded_deduplication(self):
        with closing(self.db()) as db, db:
            db.executemany("INSERT INTO reifen_kaeufe VALUES(?,?,?)",
                           [(str(i),"zahlung_offen",f"2026-01-{i+1:02d}") for i in range(25)])
        self.service.auto_pending_ids = Mock(return_value=["auto-acknowledged", "0", "auto-capture-unknown"])
        self.service.reconcile = Mock()
        wiring.run_pending(self.portal)
        identifiers = [call.args[0] for call in self.service.reconcile.call_args_list]
        self.assertEqual(len(identifiers), 20)
        self.assertEqual(len(set(identifiers)), 20)
        self.assertIn("auto-acknowledged", identifiers)
        self.assertIn("auto-capture-unknown", identifiers)
        self.assertIn("1", identifiers)
        self.assertEqual(identifiers.count("0"), 1)
        self.service.auto_pending_ids.assert_called_once_with(limit=20)


if __name__=="__main__":
    unittest.main(verbosity=2)
