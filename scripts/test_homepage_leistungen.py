"""Service -> detail -> preselected enquiry, with isolated data and no network."""
from html.parser import HTMLParser
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import smoke_test as isolated

portal = isolated.portal


class Document(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.tags = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def find(self, tag, css_class=None):
        return [attrs for name, attrs in self.tags if name == tag and
                (css_class is None or css_class in attrs.get("class", "").split())]


class ServiceNavigationTests(unittest.TestCase):
    def setUp(self):
        portal.app.config["TESTING"] = True
        self.client = portal.app.test_client()

    def page(self, path, **kwargs):
        response = self.client.get(path, **kwargs)
        self.assertEqual(response.status_code, 200, path)
        html = response.get_data(as_text=True)
        return html, Document(html)

    def test_homepage_nine_distinct_details(self):
        _, doc = self.page("/homepage")
        links = doc.find("a", "service-item")
        self.assertEqual(len(links), 9)
        self.assertEqual({a["href"] for a in links},
                         {f"/leistungen/{slug}" for slug in portal.HOMEPAGE_LEISTUNGEN})
        for a in links:
            self.assertNotIn("target", a)
            self.page(a["href"])

    def test_all_work_photos_have_matching_detail(self):
        # Both pages render the shared partial and need its routing context.
        for path in ("/leistungen", "/geschaeftspartner"):
            with self.subTest(page=path):
                _, doc = self.page(path)
                photos = doc.find("a", "arbeitsbild-foto")
                self.assertEqual(len(photos), 15)
                self.assertEqual({a["href"] for a in photos},
                                 {f"/leistungen/{slug}" for slug in portal.HOMEPAGE_LEISTUNGEN})
                self.assertTrue(all("Leistungen ansehen" in a["aria-label"] for a in photos))

    def test_each_detail_opens_its_contact_form_without_second_selection(self):
        for slug, service in portal.HOMEPAGE_LEISTUNGEN.items():
            with self.subTest(service=slug):
                html, doc = self.page(f"/leistungen/{slug}")
                self.assertEqual(len(doc.find("h1")), 1)
                self.assertIn(service["titel"].replace("&", "&amp;"), html)
                self.assertIn('href="/homepage"', html)
                self.assertIn('href="/leistungen"', html)
                actions = doc.find("a", "service-enquiry")
                self.assertGreaterEqual(len(actions), 2)
                for a in actions:
                    target = urlsplit(a["href"])
                    self.assertEqual(target.path, "/anfrage")
                    self.assertEqual(target.fragment, "kurzanfrage")
                    category = parse_qs(target.query)["anliegen"][0]
                    self.assertIn(category, portal.WEBSITE_KURZANFRAGEN)
                    form_html, form = self.page(a["href"])
                    self.assertNotIn('aria-label="Anliegen auswählen"', form_html)
                    self.assertNotIn("Was möchten Sie anfragen?", form_html)
                    fields = {f.get("name"): f for f in form.find("input")}
                    self.assertEqual(fields["anliegen"].get("value"), category)
                    self.assertIn("name", fields)
                    self.assertIn("telefon", fields)
                    self.assertIn("csrf_token", fields)
                    self.assertTrue(any(t.get("name") == "nachricht" for t in form.find("textarea")))
                    self.assertTrue(all("required" not in f for f in form.find("input") if f.get("type") == "file"))
                    if slug in {"achsvermessung", "klimaservice"}:
                        expected_category = "klima" if slug == "klimaservice" else "achsvermessung"
                        self.assertEqual(parse_qs(target.query)["mechanik_kategorie"], [expected_category])
                        selected = [f["value"] for f in form.find("input")
                                    if f.get("name") == "mechanik_kategorie" and "checked" in f]
                        self.assertEqual(selected, [expected_category])

    def test_public_host_details_and_sitemap_but_no_internal_access(self):
        with patch.multiple(portal, PUBLIC_SITE_ONLY=True, PUBLIC_SITE_INDEXABLE=True,
                            PUBLIC_BASE_URL="https://site.example.test",
                            PUBLIC_HOSTS={"site.example.test"},
                            PORTAL_BASE_URL="https://portal.example.test"):
            for slug, service in portal.HOMEPAGE_LEISTUNGEN.items():
                with self.subTest(service=slug):
                    path = f"/leistungen/{slug}"
                    html, doc = self.page(path, base_url="https://site.example.test")
                    self.assertIn(f'content="index, follow"', html)
                    self.assertIn(f'href="https://site.example.test{path}"', html)
                    for a in doc.find("a", "service-enquiry"):
                        self.assertEqual(urlsplit(a["href"]).netloc, "portal.example.test")
                        self.assertNotIn("auswahl", a["href"])
            self.assertEqual(self.client.get("/admin", base_url="https://site.example.test").status_code, 404)
            self.assertEqual(self.client.get("/anfrage", base_url="https://site.example.test").status_code, 404)
            xml, _ = self.page("/sitemap.xml", base_url="https://site.example.test")
            urls = {x.text for x in ElementTree.fromstring(xml).iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")}
            self.assertTrue(all(f"https://site.example.test/leistungen/{slug}" in urls for slug in portal.HOMEPAGE_LEISTUNGEN))

    def test_unknown_service_is_404(self):
        self.assertEqual(self.client.get("/leistungen/nicht-vorhanden").status_code, 404)

    def test_wax_and_exhaust_are_offered_without_ceramic(self):
        html, _ = self.page("/leistungen/fahrzeugaufbereitung")
        self.assertIn("Wachsversiegelung", html)
        self.assertNotIn("Keramik", html)
        form, _ = self.page("/anfrage?anliegen=fahrzeugpflege")
        self.assertIn('value="wachs"', form)
        self.assertIn("Wachsversiegelung", form)
        mechanics, _ = self.page("/leistungen/wartung-mechanik")
        self.assertIn("Auspuff", mechanics)
        self.assertIn("externen Prüfpartner", mechanics)
        form, _ = self.page("/anfrage?anliegen=mechanikanfrage")
        self.assertIn('value="auspuff"', form)

    def test_general_enquiry_gateway_still_works(self):
        html, _ = self.page("/anfrage?anliegen=auswahl")
        self.assertIn("Was möchten Sie anfragen?", html)
        self.assertIn('aria-label="Anliegen auswählen"', html)
        self.assertIn("Fahrzeugpflege", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)
