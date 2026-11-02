"""Public tire search against a fake provider, with disposable data only.

Checks real customer-visible totals, fitment/freshness gates, disclosure limits
and the contact fallback. Network connections and worker startup are blocked
before application import; no supplier order or real email can be sent.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html.parser import HTMLParser
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parent))
with patch("threading.Thread.start"):
    import smoke_test as isolated

portal = isolated.portal
ENDPOINT = "/api/reifenangebote/suche"
QUERY = {"groesse": "205/55 R16 91V", "saison": "sommer", "qualitaet": "premium",
         "anzahl": "4", "montage": "nein"}


class HiddenInputs(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.values = {}
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "input" and attrs.get("name"):
            self.values[attrs["name"]] = attrs.get("value", "")


def article(**changes):
    now = datetime.now(timezone.utc)
    item = {
        "artikel_id": "SYNTHETIC-TIRE-42", "ean": "4000000000042",
        "bezeichnung": "Musterreifen Alpha", "breite": 205, "querschnitt": 55,
        "zoll": 16, "lastindex": 91, "geschwindigkeitsindex": "V",
        "saison": "sommer", "qualitaet": "premium", "netto_einkauf": "100.00",
        "fracht_netto": "10.00", "waehrung": "EUR", "bestand": 8,
        "preis_bestaetigt": True, "abgerufen_am": now.isoformat(),
        "gueltig_bis": (now + timedelta(hours=1)).isoformat(), "lieferzeit": "2–3 Werktage",
        "montage_standard_bestaetigt": True,
    }
    item.update(changes)
    return item


class ReifenSucheTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        self.config_patch = patch.dict(portal.app.config, {"REIFENANGEBOT_PROVIDER": None})
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        for name in ("lead_notice", "notify_lead_workshop", "schedule_change_backup",
                     "sende_website_lead_benachrichtigung"):
            provider = patch.object(portal, name)
            provider.start()
            self.addCleanup(provider.stop)
        self.clear_rate()

    def clear_rate(self):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()

    def provider(self, items=None, **response_changes):
        result = {"angebote": [article()] if items is None else items}
        result.update(response_changes)
        hook = Mock(return_value=result)
        portal.app.config["REIFENANGEBOT_PROVIDER"] = hook
        return hook

    def search(self, changes=None, *, query=None, **kwargs):
        params = dict(QUERY)
        params.update(changes or {})
        return self.client.get(ENDPOINT, query_string=query if query is not None else params, **kwargs)

    def count(self, table):
        db = portal.get_db()
        try:
            return db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
        finally:
            db.close()

    def contact_payload(self, **changes):
        response = self.client.get("/anfrage?anliegen=reifenangebot")
        self.assertEqual(response.status_code, 200)
        inputs = HiddenInputs(response.text).values
        payload = {
            portal.CSRF_FIELD_NAME: inputs[portal.CSRF_FIELD_NAME],
            "reifenangebot_token": inputs["reifenangebot_token"], "anliegen": "reifenangebot",
            "name": "Fallback Testkunde", "email": "fallback@example.invalid", "telefon": "",
            "reifen_saison": "sommer", "reifen_qualitaet": "premium", "reifen_anzahl": "4",
            "reifen_groesse_vorne": "205/55 R16 91V", "reifen_montage": "nein",
        }
        payload.update(changes)
        return payload

    def latest_lead(self):
        db = portal.get_db()
        try:
            lead_id = db.execute("SELECT MAX(id) FROM leads").fetchone()[0]
        finally:
            db.close()
        return portal.get_lead(lead_id)

    def assert_no_offer(self, response, *, statuses=(200,)):
        self.assertIn(response.status_code, statuses)
        self.assertTrue(response.is_json)
        self.assertEqual(response.json.get("produkte"), [])
        self.assertFalse(response.json.get("verfuegbar"))

    def price(self, response):
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["status"], "ok")
        self.assertTrue(response.json["verfuegbar"])
        self.assertEqual(len(response.json["produkte"]), 1)
        return response.json["produkte"][0]["preis"]

    def test_default_without_supplier_access_returns_no_placeholder_price(self):
        response = self.search()
        self.assert_no_offer(response)
        self.assertEqual(response.json["status"], "nicht_verfuegbar")
        self.assertTrue(response.json.get("nachricht"))
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_public_get_needs_no_admin_or_credentials_and_provider_receives_only_normalized_query(self):
        hook = self.provider()
        self.price(self.search())
        hook.assert_called_once_with({
            "breite": 205, "querschnitt": 55, "zoll": 16, "lastindex": 91,
            "geschwindigkeitsindex": "V", "saison": "sommer", "qualitaet": "premium",
            "anzahl": 4, "montage": False,
        })
        with self.client.session_transaction() as session:
            self.assertFalse(session.get("admin"))

    def test_ten_percent_net_markup_then_vat_and_one_shipping_charge(self):
        self.provider()
        price = self.price(self.search())
        self.assertEqual(Decimal(price["einzel_brutto"]), Decimal("130.90"))
        self.assertEqual(Decimal(price["gesamt_reifen_brutto"]), Decimal("523.60"))
        self.assertEqual(Decimal(price["versand_brutto"]), Decimal("11.90"))
        self.assertEqual(Decimal(price["montage_brutto"]), Decimal("0.00"))
        self.assertEqual(Decimal(price["gesamt_brutto"]), Decimal("535.50"))
        self.assertEqual(price["waehrung"], "EUR")
        self.assertEqual(Decimal(price["gesamt_brutto"]),
                         Decimal(price["gesamt_reifen_brutto"]) + Decimal(price["versand_brutto"]) + Decimal(price["montage_brutto"]))

    def test_shipping_does_not_multiply_with_quantity(self):
        self.provider()
        one = self.price(self.search({"anzahl": "1"}))
        eight = self.price(self.search({"anzahl": "8"}))
        self.assertEqual(Decimal(one["versand_brutto"]), Decimal("11.90"))
        self.assertEqual(one["versand_brutto"], eight["versand_brutto"])
        self.assertEqual(Decimal(eight["gesamt_reifen_brutto"]), Decimal("1047.20"))

    def test_optional_mounting_uses_approved_gross_catalogue_once(self):
        self.provider()
        price = self.price(self.search({"montage": "ja"}))
        self.assertEqual(Decimal(price["montage_brutto"]), Decimal("90.00"))
        self.assertEqual(Decimal(price["gesamt_brutto"]), Decimal("625.50"))

    def test_low_profile_surcharge_at_40_and_below_only_with_mounting(self):
        for ratio, mounting, expected in ((35,"ja","120.00"),(40,"ja","120.00"),
                                           (45,"ja","100.00"),(40,"nein","0.00")):
            with self.subTest(ratio=ratio,mounting=mounting):
                self.clear_rate()
                self.provider([article(breite=225,querschnitt=ratio,zoll=18,lastindex=92,runflat=False)])
                price=self.price(self.search({"groesse":f"225/{ratio} R18 92V","montage":mounting}))
                self.assertEqual(price["montage_brutto"],expected)

    def test_all_brands_does_not_invent_quality_and_class_filter_still_applies(self):
        self.provider([article(qualitaet=None,ean=None)])
        response=self.search({"qualitaet":"alle","groesse":"205/55 R16"})
        self.price(response)
        self.assertIsNone(response.json["produkte"][0]["qualitaet"])
        self.assertEqual(response.json["produkte"][0]["ean"],"")
        self.assert_no_offer(self.search())
        self.provider([article(qualitaet="alle")])
        self.assert_no_offer(self.search({"qualitaet":"alle"}))

    def test_optional_eu_label_has_only_validated_eprel_metadata(self):
        labels={"eprel_url":"https://eprel.ec.europa.eu/qr/123456", "label_kraftstoff":"B",
                "label_nass":"A","label_geraeusch_db":"71","label_geraeuschklasse":"B"}
        self.provider([article(**labels)])
        response=self.search()
        self.price(response)
        self.assertEqual(response.json["produkte"][0]["eu_label"],{
            "eprel_url":labels["eprel_url"],"kraftstoffeffizienz":"B","nasshaftung":"A",
            "rollgeraeusch_db":71,"rollgeraeuschklasse":"B"})
        for invalid in ({"eprel_url":"https://eprel.ec.europa.eu.evil.invalid/qr/123"},
                        {"eprel_url":"https://user:private@eprel.ec.europa.eu/qr/123"},
                        {"eprel_url":"https://eprel.ec.europa.eu/qr/123?secret=private"},
                        {"eprel_url":"https://eprel.ec.europa.eu:444/qr/123"},
                        {"eprel_url":"https://eprel.ec.europa.eu/qr/123#secret"},
                        {"eprel_url":"javascript:alert(1)"}, {"label_kraftstoff":"F"},
                        {"label_geraeusch_db":True}, {"label_geraeusch_db":"131"}):
            with self.subTest(invalid=invalid):
                self.clear_rate()
                self.provider([article(**{**labels,**invalid})])
                response=self.search()
                self.price(response)
                self.assertNotIn("eu_label",response.json["produkte"][0])

    def test_dedicated_shop_has_search_without_contact_form_or_enabled_purchase(self):
        with patch.dict(portal.app.config,{"REIFEN_KAUF_MODE":"off","REIFEN_KAUF_ENABLED":False}):
            response=self.client.get("/reifen")
        self.assertEqual(response.status_code,200)
        self.assertIn('data-standalone-shop',response.text)
        self.assertIn('data-order-enabled="false"',response.text)
        self.assertIn('name="reifen_qualitaet"',response.text)
        self.assertNotIn('name="email"',response.text)
        self.assertNotIn('name="telefon"',response.text)
        self.assertIn('Onlinebestellungen starten in Kürze',response.text)
        self.assertEqual(response.headers["Cache-Control"],"no-store")

    def test_shop_and_read_only_search_are_available_on_public_homepage_host(self):
        self.provider()
        with patch.object(portal,"PUBLIC_HOSTS",{"auto-lackierzentrum.de"}):
            response=self.client.get("/reifen",base_url="https://auto-lackierzentrum.de")
            self.assertEqual(response.status_code,200)
            self.price(self.search(base_url="https://auto-lackierzentrum.de"))
            self.assertEqual(self.client.get("/admin/reifenportal",base_url="https://auto-lackierzentrum.de").status_code,404)

    def test_stateless_shop_redirects_to_central_portal_and_never_accepts_checkout(self):
        with patch.object(portal,"PUBLIC_SITE_ONLY",True),patch.object(portal,"PORTAL_BASE_URL","https://portal.synthetic.invalid"):
            response=self.client.get("/reifen")
        self.assertEqual(response.status_code,302)
        self.assertEqual(response.headers["Location"],"https://portal.synthetic.invalid/reifen")

    def test_public_response_is_a_strict_whitelist_without_purchase_prices_or_raw_provider_data(self):
        item = article(api_key="SECRET-MARKER-KEY", credentials={"password": "SECRET-MARKER-PASSWORD"},
                       raw={"wholesale": "SECRET-MARKER-RAW"}, internal_margin="SECRET-MARKER-MARGIN",
                       bestell_url="https://SECRET-MARKER-PROVIDER-LINK.invalid/checkout")
        self.provider([item], token="SECRET-MARKER-TOKEN")
        response = self.search()
        price = self.price(response)
        self.assertLessEqual(set(response.json), {"status", "verfuegbar", "produkte", "nachricht"})
        product = response.json["produkte"][0]
        self.assertLessEqual(set(product), {"artikel_id", "ean", "bezeichnung", "groesse", "saison",
                                            "qualitaet", "bestand", "lieferzeit", "preisstand", "gueltig_bis", "preis", "bestell_url"})
        self.assertEqual(set(price), {"einzel_brutto", "gesamt_reifen_brutto", "versand_brutto",
                                     "montage_brutto", "gesamt_brutto", "waehrung"})
        self.assertTrue(product["bestell_url"].startswith("/reifen/bestellen/"))
        body = response.get_data(as_text=True)
        for private in ("SECRET-MARKER", "netto_einkauf", "fracht_netto", "aufschlag_prozent",
                        "kalkulationsbasis", "credentials", "api_key", "abgerufen_am", "montage_standard_bestaetigt"):
            self.assertNotIn(private, body)
        self.assertTrue(response.content_type.startswith("application/json"))
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_unconfirmed_or_runflat_mounting_never_uses_a_free_or_standard_fallback(self):
        for changes in ({"montage_standard_bestaetigt": False},
                        {"montage_standard_bestaetigt": None},
                        {"montage_standard_bestaetigt": "true"}, {"runflat": True}):
            with self.subTest(changes=changes):
                self.clear_rate()
                self.provider([article(**changes)])
                self.assert_no_offer(self.search({"montage": "ja"}))
        for query in ({"anzahl": "8", "montage": "ja"},
                      {"groesse": "205/55 R22 91V", "montage": "ja"}):
            with self.subTest(query=query):
                self.clear_rate()
                hook = self.provider()
                self.assert_no_offer(self.search(query))
                hook.assert_not_called()

    def test_stale_expired_future_or_timezone_less_prices_do_not_become_offers(self):
        now = datetime.now(timezone.utc)
        cases = (
            {"abgerufen_am": (now - timedelta(minutes=16)).isoformat()},
            {"abgerufen_am": (now + timedelta(minutes=1)).isoformat()},
            {"gueltig_bis": (now - timedelta(seconds=1)).isoformat()},
            {"abgerufen_am": now.replace(tzinfo=None).isoformat()},
            {"gueltig_bis": (now + timedelta(hours=1)).replace(tzinfo=None).isoformat()},
            {"abgerufen_am": "invalid"}, {"gueltig_bis": ""},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.clear_rate()
                self.provider([article(**changes)])
                self.assert_no_offer(self.search())

    def test_public_validity_ends_at_the_earlier_supplier_expiry_or_price_freshness_limit(self):
        now = datetime.now(timezone.utc)
        retrieved = now - timedelta(minutes=10)
        freshness_limit = retrieved + timedelta(minutes=15)
        for supplier_expiry in (now + timedelta(hours=1), now + timedelta(minutes=2)):
            with self.subTest(supplier_expiry=supplier_expiry):
                self.clear_rate()
                self.provider([article(abgerufen_am=retrieved.isoformat(), gueltig_bis=supplier_expiry.isoformat())])
                response = self.search()
                self.price(response)
                product = response.json["produkte"][0]
                public_expiry = datetime.fromisoformat(product["gueltig_bis"])
                self.assertEqual(public_expiry, min(supplier_expiry, freshness_limit))
                self.assertEqual(datetime.fromisoformat(product["preisstand"]), retrieved)

    def test_stock_must_cover_the_requested_quantity(self):
        for stock in (0, 3, -1, None, True, "8", 8.0):
            with self.subTest(stock=stock):
                self.clear_rate()
                self.provider([article(bestand=stock)])
                self.assert_no_offer(self.search())
        self.clear_rate()
        self.provider([article(bestand=4)])
        self.price(self.search())

    def test_wrong_dimension_season_quality_or_insufficient_indices_are_filtered(self):
        cases = (
            {"breite": 215}, {"querschnitt": 60}, {"zoll": 17},
            {"saison": "winter"}, {"qualitaet": "mittelklasse"},
            {"lastindex": 90}, {"lastindex": None},
            {"geschwindigkeitsindex": "H"}, {"geschwindigkeitsindex": "unknown"},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.clear_rate()
                self.provider([article(**changes)])
                self.assert_no_offer(self.search())

    def test_higher_load_and_speed_are_allowed_using_speed_order_not_alphabet(self):
        self.provider([article(lastindex=95, geschwindigkeitsindex="H")])
        self.price(self.search({"groesse": "205/55 R16 91T"}))
        self.clear_rate()
        self.provider([article(lastindex=95, geschwindigkeitsindex="T")])
        self.assert_no_offer(self.search({"groesse": "205/55 R16 91H"}))

    def test_unconfirmed_missing_non_eur_or_malformed_purchase_costs_never_get_a_price(self):
        cases = (
            {"preis_bestaetigt": False}, {"preis_bestaetigt": "true"},
            {"netto_einkauf": ""}, {"netto_einkauf": "-1"}, {"netto_einkauf": "0"},
            {"netto_einkauf": 100.0}, {"netto_einkauf": "NaN"},
            {"fracht_netto": None}, {"fracht_netto": ""}, {"fracht_netto": "-1"},
            {"fracht_netto": 10.0}, {"waehrung": "USD"}, {"waehrung": ""},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.clear_rate()
                self.provider([article(**changes)])
                self.assert_no_offer(self.search())

    def test_one_bad_article_does_not_hide_a_separate_valid_match(self):
        self.provider([article(artikel_id="OUT-OF-STOCK", bestand=0),
                       article(artikel_id="WRONG-SEASON", saison="winter"),
                       article(artikel_id="VALID")])
        response = self.search()
        self.price(response)
        self.assertEqual(response.json["produkte"][0]["artikel_id"], "VALID")

    def test_invalid_inputs_are_rejected_before_calling_the_provider(self):
        hook = self.provider()
        cases = (
            {"groesse": ""}, {"groesse": "20/55 R16"}, {"groesse": "205/abc R16"},
            {"groesse": "<script>alert(1)</script>"}, {"groesse": "205/55R16; DROP TABLE leads"},
            {"saison": ""}, {"saison": "unsafe"}, {"qualitaet": "unsafe"},
            {"anzahl": "0"}, {"anzahl": "9"}, {"anzahl": "4.0"}, {"anzahl": "04"},
            {"anzahl": "٤"}, {"montage": "unsafe"},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                self.clear_rate()
                response = self.search(changes)
                self.assert_no_offer(response, statuses=(400,))
        hook.assert_not_called()

    def test_duplicate_unknown_or_client_provider_parameters_are_not_accepted(self):
        hook = self.provider()
        for extra in ("netto_einkauf", "montage_brutto", "provider", "api_key"):
            with self.subTest(extra=extra):
                self.clear_rate()
                self.assert_no_offer(self.search({extra: "untrusted-client-value"}), statuses=(400,))
        self.clear_rate()
        duplicate = MultiDict(QUERY)
        duplicate.add("groesse", "225/40 R18 92V")
        self.assert_no_offer(self.search(query=duplicate), statuses=(400,))
        hook.assert_not_called()

    def test_search_has_no_post_ordering_action(self):
        hook = self.provider()
        # The app rejects missing CSRF before Flask checks allowed methods.
        self.assertEqual(self.client.post(ENDPOINT, data=QUERY).status_code, 400)
        self.client.get("/anfrage?anliegen=reifenangebot")
        with self.client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
        payload = {**QUERY, portal.CSRF_FIELD_NAME: csrf}
        self.assertEqual(self.client.post(ENDPOINT, data=payload).status_code, 405)
        hook.assert_not_called()

    def test_provider_exception_is_generic_and_never_discloses_supplier_details(self):
        hook = Mock(side_effect=RuntimeError("SECRET-MARKER-KEY https://supplier.invalid/private?password=SECRET"))
        portal.app.config["REIFENANGEBOT_PROVIDER"] = hook
        response = self.search()
        self.assert_no_offer(response, statuses=(503,))
        for value in ("SECRET", "supplier.invalid", "RuntimeError", "password"):
            self.assertNotIn(value, response.get_data(as_text=True))

    def test_rate_limit_prevents_additional_supplier_calls(self):
        hook = self.provider()
        with patch.object(portal, "PUBLIC_FORM_RATE_LIMIT_MAX", 3):
            for _ in range(3):
                self.assertEqual(self.search().status_code, 200)
            response = self.search()
        self.assert_no_offer(response, statuses=(429,))
        self.assertEqual(hook.call_count, 3)

    def test_search_never_creates_a_lead_slot_order_or_sends_messages(self):
        before = {table: self.count(table) for table in ("leads", "reifen_termin_slots", "auftraege", "lead_notifications")}
        self.provider()
        self.price(self.search({"montage": "ja"}))
        for table, count in before.items():
            self.assertEqual(self.count(table), count)
        portal.lead_notice.assert_not_called()
        portal.notify_lead_workshop.assert_not_called()

    def test_contact_fallback_still_works_without_prices_and_ignores_client_price_fields(self):
        self.assert_no_offer(self.search())
        before = {table: self.count(table) for table in ("leads", "reifen_termin_slots", "auftraege")}
        response = self.client.post("/anfrage", data=self.contact_payload(
            angebot_preis="0.01", netto_einkauf="0.01", artikel_id="UNVERIFIED-CLIENT-ITEM"
        ))
        self.assertEqual(response.status_code, 302)
        lead = self.latest_lead()
        self.assertEqual(lead["angebot_preis"], "")
        self.assertEqual(lead["angebot_status"], "entwurf")
        self.assertNotIn("UNVERIFIED-CLIENT-ITEM", json.dumps(lead["portal_data"]))
        self.assertEqual(self.count("leads"), before["leads"] + 1)
        for table in ("reifen_termin_slots", "auftraege"):
            self.assertEqual(self.count(table), before[table])

    def test_optional_product_wish_is_unconfirmed_text_without_price_or_order_effects(self):
        wish = "Beispielreifen B (205/55 R16 91V), Artikel DEMO-B"
        note = "Mein eigener Hinweis bleibt erhalten."
        before = {table: self.count(table) for table in ("reifen_termin_slots", "auftraege")}
        payload = self.contact_payload(reifen_produktwunsch=wish, nachricht=note,
                                       angebot_preis="0.01", gesamt_brutto="0.01", netto_einkauf="0.01")
        response = self.client.post("/anfrage", data=payload)
        self.assertEqual(response.status_code, 302)
        lead = self.latest_lead()
        self.assertEqual(lead["portal_data"]["reifenangebot"]["produktwunsch"], wish)
        self.assertIn("Kundenwunsch (ungeprüft): " + wish, lead["beschreibung"])
        self.assertEqual(lead["portal_data"]["kunden_wunsch_hinweis"], note)
        self.assertEqual(lead["angebot_preis"], "")
        self.assertEqual(lead["angebot_status"], "entwurf")
        self.assertFalse(lead["auftrag_id"])
        for table, count in before.items():
            self.assertEqual(self.count(table), count)

    def test_product_wish_survives_email_validation_and_is_html_escaped(self):
        wish = 'Beispielreifen <script>alert("DEMO-B")</script> & Zubehör'
        before = self.count("leads")
        response = self.client.post("/anfrage", data=self.contact_payload(
            email="not-an-email", reifen_produktwunsch=wish
        ))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.count("leads"), before)
        inputs = HiddenInputs(response.text).values
        self.assertEqual(inputs.get("reifen_produktwunsch"), wish)
        self.assertNotIn('<script>alert("DEMO-B")</script>', response.text)

    def test_product_wish_accepts_four_hundred_text_characters_without_truncation(self):
        wish = "R" * 400
        response = self.client.post("/anfrage", data=self.contact_payload(reifen_produktwunsch=wish))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.latest_lead()["portal_data"]["reifenangebot"]["produktwunsch"], wish)

    def test_overlong_or_control_character_product_wishes_do_not_create_leads(self):
        for wish in ("R" * 401, "Reifen\nB", "Reifen\tB", "Reifen\x00B",
                     "Reifen\x7fB", "Reifen\u202eB", "Reifen\u200bB"):
            with self.subTest(wish=repr(wish)):
                self.clear_rate()
                before = self.count("leads")
                response = self.client.post("/anfrage", data=self.contact_payload(reifen_produktwunsch=wish))
                self.assertEqual(response.status_code, 400)
                self.assertEqual(self.count("leads"), before)

    def test_empty_product_wish_preserves_the_previous_summary(self):
        summaries = []
        for changes in ({}, {"reifen_produktwunsch": ""}, {"reifen_produktwunsch": "   "}):
            with self.subTest(changes=changes):
                self.clear_rate()
                response = self.client.post("/anfrage", data=self.contact_payload(**changes))
                self.assertEqual(response.status_code, 302)
                lead = self.latest_lead()
                self.assertNotIn("produktwunsch", lead["portal_data"]["reifenangebot"])
                summaries.append(portal.reifenangebot_summary(lead["portal_data"]["reifenangebot"]))
        self.assertEqual(summaries, ["Sommerreifen · Premium · 4 Stück · Größe: 205/55 R16 91V · ohne Montage"] * 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
