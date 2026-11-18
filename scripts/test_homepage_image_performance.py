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
        html = self.client.get("/homepage").get_data(as_text=True)
        eager = [img for img in Markup(html).images if img.get("loading") != "lazy"]
        self.assertEqual(len(eager), 2, "Only the logo and cover photo load eagerly")
        logo, cover = eager
        self.assertIn("logo-transparent", logo["src"])
        logo_mobile = next(v for v in self.data["logo-transparent.png"]["variants"] if v["width"] >= 460)
        self.assertLess(logo_mobile["bytes"] + static_path(cover["src"]).stat().st_size, 150_000)
        deck_html = html.split("data-current-deck", 1)[1].split("</section>", 1)[0]
        cards = Markup(deck_html).images
        self.assertEqual(len(cards), 8)
        self.assertTrue(all(image.get("loading") == "lazy" for image in cards))

    def test_hero_preload_selects_the_same_resource_as_the_image(self):
        markup = Markup(self.client.get("/homepage").get_data(as_text=True))
        priority_images = [image for image in markup.images if image.get("fetchpriority") == "high"]
        self.assertEqual(len(priority_images), 1)
        hero = priority_images[0]
        self.assertEqual(hero["src"], "/static/homepage/fahrzeuglackierung.webp")
        with Image.open(static_path(hero["src"])) as photo:
            self.assertEqual(photo.size, (int(hero["width"]), int(hero["height"])))
        self.assertEqual(len(markup.preloads), 1)
        preload = markup.preloads[0]
        self.assertEqual(preload["href"], hero["src"])
        self.assertEqual(preload.get("imagesrcset"), hero.get("srcset"))
        self.assertEqual(preload.get("imagesizes"), hero.get("sizes"))

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
