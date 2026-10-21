"""Public image payload/format and responsive-preload checks on isolated data."""
from html.parser import HTMLParser
from pathlib import Path
import re
import sys
import unittest
from urllib.parse import unquote, urlparse

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent))
import smoke_test as isolated

ROOT = Path(__file__).resolve().parents[1]
portal = isolated.portal


class Markup(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.images, self.preloads = [], []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "img":
            self.images.append(attrs)
        if tag == "link" and attrs.get("rel") == "preload" and attrs.get("as") == "image":
            self.preloads.append(attrs)


def static_path(url):
    return ROOT / unquote(urlparse(url).path.lstrip("/"))


class HomepageImagePerformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        cls.client = portal.app.test_client()
        cls.data = portal.app.jinja_env.get_template("_homepage_image_data.html").module.images

    def test_variants_have_correct_dimensions_and_no_crop(self):
        for original, data in self.data.items():
            for variant in data["variants"]:
                with self.subTest(original=original, width=variant["width"]):
                    with Image.open(ROOT / "static" / variant["path"]) as image:
                        self.assertEqual(image.format, "WEBP")
                        self.assertEqual(image.width, variant["width"])
                        self.assertEqual(image.height, round(data["height"] * image.width / data["width"]))
                        self.assertLessEqual(image.width, data["width"])

    def test_large_png_previews_are_at_least_80_percent_smaller(self):
        for name in ("team-daniel-hannes-abdul-v2.png", "werkstatt-collage-mosbach.png", "schadenaufnahme-christopher-v1.png"):
            data = self.data["homepage/" + name]
            self.assertLess((ROOT / "static" / data["src"]).stat().st_size, data["original_bytes"] * .2)

    def test_homepage_first_images_have_a_mobile_payload_budget(self):
        # Representative 390px phone at DPR2; include all five visible stack edges.
        files = ["homepage/hero-herbst-20261021.webp", "logo-transparent.png"] + [
            "homepage/" + name for name in ("karte-reifen-v2-20261021.webp", "karte-bremsen-v2-20261021.webp",
            "karte-oelservice-20261021.webp", "karte-pflege-v2-20261021.webp", "karte-leasing-v2-20261021.webp")]
        before = sum(self.data[name]["original_bytes"] for name in files)
        after = 0
        for name in files:
            wanted = 460 if name == "logo-transparent.png" else (780 if "hero-" in name else 600)
            variant = next(v for v in self.data[name]["variants"] if v["width"] >= wanted)
            after += variant["bytes"]
        self.assertLess(after, 450_000)
        self.assertLess(after, before * .3)
        print(f"First images, representative mobile: {before:,} -> {after:,} bytes")

    def test_hero_preload_selects_the_same_resource_as_the_image(self):
        markup = Markup(self.client.get("/homepage").get_data(as_text=True))
        hero = next(image for image in markup.images if image.get("fetchpriority") == "high")
        self.assertEqual(len(markup.preloads), 1)
        preload = markup.preloads[0]
        self.assertEqual(preload["href"], hero["src"])
        self.assertEqual(preload["imagesrcset"], hero["srcset"])
        self.assertEqual(preload["imagesizes"], hero["sizes"])

    def test_public_pages_render_responsive_reachable_images(self):
        paths = ["/homepage", "/team", "/leistungen", "/geschaeftspartner", "/portale", "/karriere", "/anfrage?anliegen=reifenservice"]
        paths += ["/leistungen/" + slug for slug in portal.HOMEPAGE_LEISTUNGEN]
        for path in paths:
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                for image in Markup(response.get_data(as_text=True)).images:
                    if "optimized/" not in image.get("src", ""):
                        continue
                    self.assertIn("sizes", image)
                    self.assertIn("srcset", image)
                    self.assertIn("width", image)
                    self.assertIn("height", image)
                    self.assertTrue(static_path(image["src"]).is_file())
                    for candidate in image["srcset"].split(", "):
                        self.assertTrue(static_path(candidate.rsplit(" ", 1)[0]).is_file())

    def test_gallery_enlargements_and_career_background_are_reachable(self):
        for route in ("/homepage", "/team", "/karriere"):
            html = self.client.get(route).get_data(as_text=True)
            links = re.findall(r'(?:href=|url\()"(/static/homepage/optimized/[^"<>]+)"', html)
            self.assertTrue(links, route)
            for link in links:
                with self.subTest(route=route, link=link):
                    self.assertTrue(static_path(link).is_file())


if __name__ == "__main__":
    unittest.main(verbosity=2)
