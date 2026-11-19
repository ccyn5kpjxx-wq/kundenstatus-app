"""Exercise the approval form in a real browser using a synthetic receipt."""
import os
from pathlib import Path
import unittest

import test_buero_belege as fixtures

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


@unittest.skipUnless(sync_playwright, "Playwright unavailable")
class ApprovalBrowserTests(unittest.TestCase):
    def test_approval_is_submitted_once_and_recovers_on_back(self):
        office = fixtures.OfficeDocumentTests()
        office.setUp()
        self.addCleanup(office.doCleanups)
        key = office.create()
        html = office.admin.get(f"/admin/buero/belege/{key}").get_data(as_text=True)
        with sync_playwright() as playwright:
            chrome = Path(os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", playwright.chromium.executable_path))
            if not chrome.is_file() and os.name == "nt":
                chrome = Path(os.environ.get("PROGRAMFILES", "")) / "Google/Chrome/Application/chrome.exe"
            if not chrome.is_file():
                self.skipTest("Chromium unavailable")
            browser = playwright.chromium.launch(executable_path=str(chrome))
            try:
                page = browser.new_page()
                page.route("**/*", lambda route: route.abort())
                page.set_content(html)
                page.add_script_tag(path=str(fixtures.ROOT / "static/buero_belege.js"))
                form = page.locator(".bb-chef-review")
                # Observe native validation and submitter data; cancel navigation only
                # after the production handler had the chance to accept or reject it.
                form.evaluate("""form => {
                    const button = form.querySelector('[data-review-button]');
                    window.accepted = [];
                    button.name = 'button_check'; button.value = 'kept';
                    form.addEventListener('submit', e => {
                        if (!e.defaultPrevented) window.accepted.push(Object.fromEntries(new FormData(form, e.submitter)));
                        e.preventDefault();
                    });
                }""")
                def submit():
                    form.evaluate("f => f.requestSubmit(f.querySelector('[data-review-button]'))")

                submit()
                self.assertEqual(page.evaluate("window.accepted.length"), 0)
                form.locator('[name="kategorie"]').select_option("sonstige")
                kinds = form.locator('[name="typ"] option').evaluate_all("nodes => nodes.map(o => o.value).filter(Boolean)")
                cases = [(kind, kind == "zahlungsauftrag", "privat") for kind in kinds]
                cases.extend([("bescheid", True, "geschaeftlich"), ("zahlungsauftrag", True, "geschaeftlich")])
                for count, (kind, release_payment, area) in enumerate(cases, start=1):
                    with self.subTest(kind=kind, payment=release_payment, area=area):
                        form.locator('[name="bereich"]').select_option(area)
                        form.locator('[name="typ"]').select_option(kind)
                        if release_payment:
                            form.locator('[name="zahlungspflichtig"]').check()
                        form.locator('[name="confirmed"]').check()
                        submit()
                        submit()
                        accepted = page.evaluate("window.accepted")
                        self.assertEqual(len(accepted), count)
                        self.assertEqual((accepted[-1]["typ"], accepted[-1]["button_check"]), (kind, "kept"))
                        self.assertEqual(accepted[-1].get("zahlungspflichtig") == "ja", release_payment)
                        button = form.locator("[data-review-button]")
                        self.assertEqual(button.text_content(), "Freigabe wird gespeichert …")
                        page.evaluate("window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted:true}))")
                        self.assertIsNone(button.get_attribute("aria-disabled"))
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
