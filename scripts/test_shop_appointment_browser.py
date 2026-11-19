"""Local browser coverage for tyre selection and its optional order-bound appointment."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import sys
import unittest
from urllib.parse import urlsplit

from flask import Flask

try:
    from playwright.sync_api import expect, sync_playwright
except ImportError:
    sync_playwright = None

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shop_frontend import create_shop_frontend_blueprint


@unittest.skipUnless(sync_playwright, "Playwright unavailable")
class AppointmentBrowser(unittest.TestCase):
    def test_selection_reload_conflict_checkout_and_status(self):
        app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        app.register_blueprint(create_shop_frontend_blueprint())
        for flag in ("FRONTEND", "BUYER", "CART_QUOTE", "ORDER_DRAFT", "CHECKOUT", "CONSUMER"):
            app.config["SHOP_" + flag + "_ENABLED"] = True
        client = app.test_client()
        with sync_playwright() as playwright:
            executable = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", playwright.chromium.executable_path)
            if os.name == "nt" and not Path(executable).is_file():
                executable = str(Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe")
            if not Path(executable).is_file():
                self.skipTest("Playwright Chromium unavailable")
            browser = playwright.chromium.launch(executable_path=executable)
            try:
                for width in (320, 390, 1440):
                    with self.subTest(width=width):
                        self.exercise(browser, client, width)
            finally:
                browser.close()

    def exercise(self, browser, client, width):
        context = browser.new_context(viewport={"width": width, "height": 850}, locale="de-DE")
        page = context.new_page()
        page.set_default_timeout(10000)
        page.set_default_navigation_timeout(20000)
        now = datetime.now(timezone.utc)
        expiry = (now + timedelta(minutes=12)).isoformat()
        day = (now + timedelta(days=7)).date().isoformat()
        product_id = "tyre:" + "a" * 24
        order_id = "sho_" + "1" * 32
        quote_hash = "b" * 64
        pack = "c" * 64
        product = {
            "id": product_id, "category": "tyre", "department": "tyres", "size": "205/55 R16",
            "name": "Testreifen", "brand": "Testmarke", "images": [], "purchase_status": "supplier_check_required",
            "price_status": "current", "price": {"currency": "EUR", "vat_percent": "19.00",
                "observed_at": now.isoformat(), "expires_at": expiry, "stock_reported": 8,
                "prices_by_quantity": {str(i): {"quantity": i, "items_gross_cent": 5000*i, "unit_gross_cent": 5000}
                                      for i in range(1, 5)}}}
        orders, unexpected, addresses, quotes = [], [], [], []
        saved_address = width == 320
        pref = {"date": day, "time": "11:00"}
        times = ["09:00", "11:00"]
        api = "/shop/api/v2"

        def route_request(route):
            request = route.request
            url = urlsplit(request.url)
            path = url.path
            if url.netloc == "checkout.stripe.com":
                return route.fulfill(body="Testzahlung", content_type="text/html")
            if url.netloc != "localhost":
                return route.abort()
            if not path.startswith(api):
                with client.get(path + ("?" + url.query if url.query else "")) as response:
                    return route.fulfill(status=response.status_code, body=response.get_data(), content_type=response.content_type)
            path = path[len(api):]
            data, status = {}, 200
            if path == "/products":
                data = {"items": [product], "page": 1, "total": 1}
            elif path.startswith("/products/"):
                data = {"item": product}
            elif path == "/appointment-slots":
                self.assertIn("datum=" + day, url.query)
                data = {"zeiten": times}
            elif path in ("/buyer/session", "/buyer/address"):
                if path == "/buyer/address":
                    self.assertEqual(request.method, "PUT")
                    addresses.append(request.post_data_json)
                data = {"address_ready": saved_address or bool(addresses), "csrf_token": "Test_CSRF_1234567890abcdefgh",
                        "guest": True, "expires_at": expiry,
                        "workshop_delivery": addresses[-1]["workshop_delivery"] if addresses else "collection"}
            elif path == "/cart/quote":
                quotes.append(request.post_data_json)
                data = {"quote_retained": True, "quote": {"ready": True, "currency": "eur", "expires_at": expiry,
                    "quote_hash": quote_hash, "total_gross_cent": 7250,
                    "lines": [{"id": product_id, "name": "Testreifen", "quantity": 1, "items_gross_cent": 5000,
                               "services": [{"code": "mounting", "gross_cent": 2250}]}],
                    "shipments": [{"destination": "workshop", "gross_cent": 0}]}}
            elif path == "/consumer/terms":
                data = {"pack_hash": pack, "consumer_only": True, "version": "Test",
                        "texts": {key: "Test" for key in ("agb", "widerruf", "widerrufsformular", "lieferung", "datenschutz")}}
            elif path == "/consumer/acceptance":
                data = {"accepted": True, "quote_hash": quote_hash, "pack_hash": pack, "accepted_at": now.isoformat()}
            elif path == "/orders":
                payload = request.post_data_json
                orders.append(payload)
                if len(orders) == 1:
                    times.remove("09:00")
                    data, status = {"error": "appointment_slot_unavailable"}, 409
                else:
                    data = {"payment_enabled": True, "order": {"id": order_id, "quote_hash": quote_hash, "amount_gross_cent": 7250,
                            "appointment_preference": {**pref, "status": "requested"}}}
            elif path == "/orders/" + order_id + "/checkout":
                data = {"checkout": {"order_id": order_id, "currency": "eur", "amount_gross_cent": 7250,
                        "checkout_url": "https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC", "capture_triggered": False, "supplier_order_created": False}}
            elif path == "/orders/" + order_id:
                data = {"order": {"id": order_id, "status": "paid", "amount_gross_cent": 7250, "quote_hash": quote_hash,
                        "mode": "test", "appointment_preference": {**pref, "status": "requested"}}}
            else:
                unexpected.append(path)
                data, status = {"error": "unexpected"}, 404
            route.fulfill(status=status, json=data)

        context.route("**/*", route_request)
        try:
            page.goto("http://localhost/shop/reifen/?montage=1")
            page.get_by_role("button", name="Nein danke").click()
            self.assertTrue(page.locator("#catalog-mounting").is_checked())
            page.get_by_role("button", name="Artikel ansehen").first.click()
            self.assertTrue(page.locator("#product-dialog [name=mounting]").is_checked())
            page.locator("#close-product").click()
            page.get_by_role("button", name="In den Warenkorb").first.click()
            self.assertEqual(page.evaluate("JSON.parse(localStorage.getItem('gaertner-shop-v2-selection'))[0].services"), {"mounting": True})
            page.goto("http://localhost/shop/warenkorb/")
            def assert_step(number):
                for other in (1, 2, 3):
                    panel = page.locator(f'[data-cart-panel="{other}"]')
                    if other == number:
                        expect(panel).to_be_visible()
                    else:
                        expect(panel).to_be_hidden()
                self.assertLessEqual(page.evaluate("document.documentElement.scrollWidth"), width)

            assert_step(1)
            page.locator("#request-quote").click()
            assert_step(2)
            panel = page.locator("#cart-appointment")
            req = panel.locator("[name=appointment_requested]")
            req.check()
            panel.locator("[name=appointment_date]").fill(day)
            time_field = panel.locator("[name=appointment_time]")
            time_field.select_option("09:00")
            self.assertLessEqual(page.evaluate("document.documentElement.scrollWidth"), width)
            page.reload()
            page.wait_for_function("document.querySelector('[name=appointment_time]').value === '09:00'")
            assert_step(1)
            page.locator("#request-quote").click()
            assert_step(2)
            self.assertEqual((addresses, quotes, orders), ([], [], []))
            address = page.locator("#buyer-address")
            if saved_address:
                page.locator("#use-saved-address").click()
            else:
                submit = address.locator('button[type=submit]')
                submit.click()
                assert_step(2)
                self.assertEqual((addresses, quotes), ([], []))
                fields = {"name": "Alex Muster", "email": "alex@example.test", "recipient": "Alex Muster",
                          "street": "Testgasse 1", "postcode": "74821", "city": "Mosbach"}
                for name, value in fields.items():
                    address.locator(f'[name="{name}"]').fill(value)
                address.locator('[name=different_delivery]').check()
                for name, value in {"delivery_recipient": "Testbetrieb", "delivery_street": "Beispielweg 2",
                                    "delivery_postcode": "74821", "delivery_city": "Mosbach"}.items():
                    address.locator(f'[name="{name}"]').fill(value)
                address.locator('[name=delivery_salutation]').select_option("Firma")
                page.locator('[data-cart-panel="2"] [data-cart-back]').click()
                assert_step(1)
                expect(time_field).to_have_value("09:00")
                page.locator("#request-quote").click()
                assert_step(2)
                expect(address.locator("[name=email]")).to_have_value("alex@example.test")
                expect(address.locator("[name=delivery_street]")).to_have_value("Beispielweg 2")
                expect(address.locator("[name=different_delivery]")).to_be_checked()
                submit.click()
            assert_step(3)
            self.assertEqual(len(quotes), 1)
            if not saved_address:
                self.assertEqual(addresses[-1]["delivery"]["street"], "Beispielweg 2")
            self.assertIn("09:00 Uhr", page.locator("#checkout-appointment-note").inner_text())
            self.assertEqual(orders, [])
            page.locator("#quote-review input[name=accept],#quote-review input[name=consumer]").first.wait_for()
            page.get_by_role("button", name="Zu den Bestätigungen", exact=False).click()
            customer_consent = page.locator('#quote-review [name="consumer"]')
            expect(customer_consent).to_be_focused()
            expect(customer_consent).not_to_be_checked()
            expect(page.locator("#checkout-button")).to_be_in_viewport()
            for checkbox in page.locator("#quote-review input[type=checkbox]").all():
                checkbox.check()
            page.locator("#checkout-button").click()
            page.wait_for_function("document.querySelector('[name=appointment_time]').value === '' && !document.querySelector('[name=appointment_time]').disabled")
            assert_step(2)
            self.assertEqual(orders[0]["appointment_preference"], {"date": day, "time": "09:00"})
            time_field.select_option("11:00")
            page.locator("#use-saved-address").click()
            assert_step(3)
            page.locator("#quote-review input[type=checkbox]").first.wait_for()
            for checkbox in page.locator("#quote-review input[type=checkbox]").all():
                checkbox.check()
            expect(page.locator("#checkout-button")).to_be_enabled()
            page.locator("#checkout-button").click()
            page.wait_for_url("https://checkout.stripe.com/**")
            self.assertEqual(orders[1]["appointment_preference"], pref)
            self.assertEqual(orders[1]["idempotency_key"], orders[0]["idempotency_key"])
            page.goto("http://localhost/shop/orders/" + order_id + "/success")
            page.get_by_text("Montagetermin angefragt", exact=False).wait_for()
            self.assertIn("11:00 Uhr", page.locator("#order-summary").inner_text())
            page.get_by_role("link", name="Zur Auswahl", exact=True).click()
            assert_step(1)
            expect(page.locator("#request-quote")).to_be_disabled()
            expect(page.locator('[data-cart-go="2"]')).to_be_disabled()
            self.assertEqual(unexpected, [])
        finally:
            context.close()


if __name__ == "__main__":
    unittest.main()
