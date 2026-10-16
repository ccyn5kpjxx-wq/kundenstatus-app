"""Isolated routing/link checks for the new public business partner page."""
from html.parser import HTMLParser
from pathlib import Path
import unittest
from urllib.parse import urlparse
import smoke_test as isolated

portal = isolated.portal
portal.app.config.update(TESTING=True)


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get('id'):
            self.ids.add(attrs['id'])
        if tag in ('a', 'img'):
            value = attrs.get('href' if tag == 'a' else 'src')
            if value:
                self.urls.append(value)


class PartnerPageTests(unittest.TestCase):
    def test_public_host_route_and_sitemap(self):
        old = portal.PUBLIC_SITE_ONLY, portal.PUBLIC_HOSTS, portal.PUBLIC_BASE_URL, portal.PUBLIC_SITE_INDEXABLE
        try:
            portal.PUBLIC_SITE_ONLY = True
            portal.PUBLIC_HOSTS = {'www.example.test'}
            portal.PUBLIC_BASE_URL = 'https://www.example.test'
            portal.PUBLIC_SITE_INDEXABLE = True
            client = portal.app.test_client()
            response = client.get('/geschaeftspartner', base_url=portal.PUBLIC_BASE_URL)
            self.assertEqual(response.status_code, 200)
            self.assertIn('https://www.example.test/geschaeftspartner', response.text)
            sitemap = client.get('/sitemap.xml', base_url=portal.PUBLIC_BASE_URL)
            self.assertEqual(sitemap.status_code, 200)
            self.assertIn('/geschaeftspartner</loc>', sitemap.text)
        finally:
            portal.PUBLIC_SITE_ONLY, portal.PUBLIC_HOSTS, portal.PUBLIC_BASE_URL, portal.PUBLIC_SITE_INDEXABLE = old

    def test_links_and_assets_resolve(self):
        client = portal.app.test_client()
        parser = Links()
        response = client.get('/geschaeftspartner')
        self.assertEqual(response.status_code, 200)
        parser.feed(response.text)
        for target in parser.urls:
            with self.subTest(target=target):
                if target.startswith('#'):
                    self.assertIn(target[1:], parser.ids)
                elif target.startswith('/'):
                    with client.get(target) as linked:
                        self.assertNotEqual(linked.status_code, 404)
                elif urlparse(target).scheme not in ('tel', 'mailto'):
                    self.assertEqual(urlparse(target).scheme, 'https')

    def test_local_preview_is_not_indexable(self):
        html = portal.app.test_client().get('/geschaeftspartner').text
        self.assertIn('noindex, nofollow', html)
        self.assertIn('tel:+4917660477486', html)
        self.assertNotIn('mailto:service@', html)

    def test_driver_preview_link_is_available_only_with_local_opt_in(self):
        old = portal.app.config.get('KUNDENFLOTTEN_DEMO_ENABLED', False)
        client = portal.app.test_client()
        try:
            for enabled, remote, expected in [
                (False, '127.0.0.1', False),
                (True, '192.0.2.15', False),
                (True, '127.0.0.1', True),
                (True, '::1', True),
            ]:
                with self.subTest(enabled=enabled, remote=remote):
                    portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = enabled
                    response = client.get('/geschaeftspartner', environ_base={'REMOTE_ADDR': remote})
                    self.assertEqual(response.status_code, 200)
                    parser = Links()
                    parser.feed(response.text)
                    demo_links = [url for url in parser.urls if url == '/kunden-app/vorschau']
                    self.assertEqual(bool(demo_links), expected)
                    if expected:
                        demo = client.get(demo_links[0], environ_base={'REMOTE_ADDR': remote})
                        self.assertEqual(demo.status_code, 200)
                        self.assertIn('Beispielfahrzeug', demo.text)
                    else:
                        self.assertIn('Fahrzeugbetreuung kennenlernen', response.text)
        finally:
            portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = old

    def test_homepage_discovery_and_team_assets(self):
        client = portal.app.test_client()
        self.assertIn('href="/geschaeftspartner"', client.get('/homepage').text)
        response = client.get('/team')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Geschäftsführer · Meister · Fahrzeuglackierer', response.text)
        self.assertIn('<h2>Ümit</h2>', response.text)
        parser = Links()
        parser.feed(response.text)
        for path in parser.urls:
            if path.startswith('/static/'):
                with client.get(path) as asset:
                    self.assertEqual(asset.status_code, 200, path)


if __name__ == '__main__':
    unittest.main(verbosity=2)
