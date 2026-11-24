"""Actual navigation templates and route bodies in an isolated Flask fixture.

No production app import, environment, database, provider or external network.
"""
import ast
from html.parser import HTMLParser
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

from flask import Flask, has_request_context, jsonify, redirect, render_template, request, url_for

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shop_frontend import create_shop_frontend_blueprint


class Links(HTMLParser):
    def __init__(self, source):
        super().__init__()
        self.links, self.current = [], None
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.current = {**dict(attrs), 'text': ''}
            self.links.append(self.current)

    def handle_data(self, text):
        if self.current is not None:
            self.current['text'] += text

    def handle_endtag(self, tag):
        if tag == 'a':
            self.current = None


class HomepageShopNavigationChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse((ROOT/'app.py').read_text(encoding='utf-8-sig'))
        names = {'homepage_shop_links', 'inject_homepage_shop_links', 'oeffentlicher_shop'}
        selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        assert {node.name for node in selected} == names
        cls.route_code = compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])), str(ROOT/'app.py'), 'exec')

    def setUp(self):
        network = patch.object(socket, 'socket', side_effect=AssertionError('No external network'))
        network.start(); self.addCleanup(network.stop)
        self.app = Flask(__name__, root_path=str(ROOT), template_folder='templates', static_folder='static')
        self.app.config.update(TESTING=True, SHOP_FRONTEND_ENABLED=True)
        self.app.register_blueprint(create_shop_frontend_blueprint())

        def legacy_render(template, **values):
            self.assertEqual(template, 'homepage_shop.html')
            return jsonify(values)

        self.namespace = {'app': self.app, 'url_for': url_for, 'redirect': redirect,
                          'has_request_context': has_request_context, 'request': request,
                          'render_template': legacy_render, 'homepage_portal_links': lambda: {'anfrage': '/anfrage'},
                          'public_site_is_indexable_request': lambda: False, 'PUBLIC_BASE_URL': 'https://example.invalid'}
        exec(self.route_code, self.namespace)
        self.service = {'titel': 'SYNTHETIC Reifenservice', 'intro': 'SYNTHETIC service description',
                        'anliegen': 'reifenservice', 'aktion': 'Reifen-Termin jetzt buchen',
                        'bild': 'leistungsfotos-v2/reifenmontage.jpg', 'arbeiten': [('Sichtcheck', 'SYNTHETIC work')],
                        'hinweis': 'SYNTHETIC note'}
        self.context = {
            'werkstatt_oeffentlich': {'name': 'SYNTHETIC workshop', 'strasse': 'SYNTHETIC street', 'ort': 'Mosbach',
                'plz': '74821', 'adresse': 'SYNTHETIC address', 'telefon': 'SYNTHETIC phone',
                'telefon_e164': '+490000000000', 'telefon_url': 'tel:+490000000000', 'email': 'test@example.invalid'},
            'google_reviews': {'available': False, 'maps_url': 'https://example.invalid'},
            'jahreszeit': {'key': 'herbst', 'label': 'Herbst', 'hero_bild': 'homepage/hero-herbst-20261021.webp',
                'hero_alt': 'SYNTHETIC photo', 'reifen_kicker': 'Reifenzeit', 'reifen_titel': 'SYNTHETIC tyre title'},
            'homepage_stellenanzeigen': [], 'homepage_oeffnungszeiten': 'SYNTHETIC opening hours',
            'werkstatt_anfahrt_url': '#kontakt', 'site_indexable': False, 'canonical_url': '',
            'portal_links': {key: '/anfrage' for key in ('auswahl', 'schaden', 'schadenaufnahme', 'status', 'privat',
                'mietwagen', 'anfrage', 'reifenservice', 'fahrer_demo', 'partner', 'flotte', 'service', 'versicherung', 'karriere')},
            'leistungen': {'reifenservice': self.service}, 'besucher_ohne_google_ads': True,
            'static_asset_url': lambda filename: url_for('static', filename=filename),
        }

        @self.app.before_request
        def deny_mutations():
            if request.method != 'GET':
                raise AssertionError('No form, lead, order or payment submission')

        self.app.add_url_rule('/homepage', 'oeffentliche_homepage', lambda: render_template('homepage.html', **self.context))
        self.app.add_url_rule('/leistungen/<slug>', 'oeffentliche_leistung_detail',
                              lambda slug: render_template('homepage_leistung_detail.html', slug=slug,
                                                           leistung=self.service, **self.context))
        for endpoint, path in (('oeffentliche_leistungen', '/leistungen'), ('oeffentliche_geschaeftspartner', '/geschaeftspartner'),
                               ('oeffentlicher_autohaus_einstieg', '/autohaeuser'),
                               ('oeffentliches_team', '/team'), ('oeffentliche_portale', '/portale'),
                               ('impressum_seite', '/impressum'), ('datenschutz_seite', '/datenschutz'),
                               ('api_besucher_event', '/api/besucher'), ('reifen_vertrag.start', '/reifen/widerruf')):
            self.app.add_url_rule(path, endpoint, lambda: 'SYNTHETIC read-only page')
        self.client = self.app.test_client()

    def test_home_general_links_open_shop_home_directly_and_categories_remain_specific(self):
        response = self.client.get('/homepage')
        self.assertEqual(response.status_code, 200)
        links = {item['data-shop-click']: item for item in Links(response.text).links if 'data-shop-click' in item}
        for key in ('home-nav-shop', 'home-hero-shop', 'home-card-shop'):
            self.assertEqual(links[key]['href'], '/shop/')
        self.assertEqual(links['home-felgen']['href'], '/shop/felgen/')
        self.assertEqual(links['home-card-reifen']['href'], '/shop/reifen/')
        for item in links.values():
            target = self.client.get(item['href'], follow_redirects=False)
            self.assertEqual(target.status_code, 200)
            self.assertNotIn('Location', target.headers)

    def test_reifenservice_crossover_keeps_service_and_personal_offer_options(self):
        response = self.client.get('/leistungen/reifenservice')
        links = Links(response.text).links
        shop = [item for item in links if 'Neue Reifen im Shop auswählen' in item['text']]
        self.assertEqual(len(shop), 1)
        self.assertEqual(shop[0]['href'], '/shop/reifen/')
        self.assertEqual(self.client.get(shop[0]['href']).status_code, 200)
        self.assertTrue(any('?anliegen=reifenangebot#kurzanfrage' in item['href'] for item in links))
        self.assertTrue(any('anliegen=reifenservice' in item['href'] and 'leistungsart=reifenservice' in item['href'] for item in links))
        self.app.config['SHOP_CHECKOUT_ENABLED'] = True
        active_links = Links(self.client.get('/leistungen/reifenservice').text).links
        self.assertTrue(any(item['href'] == '/shop/reifen/?montage=1' and 'Reifen mit Montage' in item['text']
                            for item in active_links))

    def test_other_service_does_not_gain_tyre_purchase_action(self):
        response = self.client.get('/leistungen/autoglas')
        self.assertNotIn('Neue Reifen im Shop auswählen', response.text)

    def test_literal_disabled_flag_uses_real_legacy_routes_and_correct_canonical(self):
        for value in (False, None, 'true', 1):
            with self.subTest(flag=value):
                self.app.config['SHOP_FRONTEND_ENABLED'] = value
                with self.app.test_request_context('/homepage'):
                    links = self.namespace['homepage_shop_links']()
                self.assertEqual(links, {'home': '/shop', 'reifen': '/shop/reifen', 'felgen': '/shop/felgen'})
                for key, path in links.items():
                    result = self.client.get(path, follow_redirects=False)
                    self.assertEqual(result.status_code, 200)
                    self.assertNotIn('Location', result.headers)
                    self.assertEqual(result.json['shop_category'], None if key == 'home' else key)
                    self.assertEqual(result.json['canonical_url'], 'https://example.invalid' + path)

    def test_disabled_home_and_tyreservice_templates_keep_reachable_fallback_links(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = False
        links = {item.get('data-shop-click'): item['href'] for item in Links(self.client.get('/homepage').text).links}
        self.assertEqual(links['home-card-shop'], '/shop')
        self.assertEqual(links['home-hero-shop'], '/shop')
        service = Links(self.client.get('/leistungen/reifenservice').text).links
        self.assertEqual(next(item['href'] for item in service if 'Neue Reifen im Shop auswählen' in item['text']), '/shop/reifen')

    def test_old_shop_urls_still_redirect_to_their_exact_frontend_destination(self):
        for old, target in (('/shop', '/shop/'), ('/shop/reifen', '/shop/reifen/'), ('/shop/felgen', '/shop/felgen/')):
            result = self.client.get(old, follow_redirects=False)
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.headers['Location'], target)

    def test_production_aliases_link_and_redirect_to_the_public_shop(self):
        shop = 'https://auto-lackierzentrum.de'
        self.app.config['SHOP_PUBLIC_ORIGIN'] = shop
        for base in ('https://kundenstatus-app.onrender.com', 'https://www.auto-lackierzentrum.de'):
            with self.subTest(base=base):
                home = Links(self.client.get('/homepage', base_url=base).text).links
                sections = {'home-nav-shop': '', 'home-hero-shop': '', 'home-card-shop': '',
                            'home-felgen': 'felgen/', 'home-card-reifen': 'reifen/'}
                self.assertEqual({a['data-shop-click']: a['href'] for a in home if 'data-shop-click' in a},
                                 {key: shop + '/shop/' + value for key, value in sections.items()})
                service = Links(self.client.get('/leistungen/reifenservice', base_url=base).text).links
                self.assertIn(shop + '/shop/reifen/', [a['href'] for a in service])
                for path in ('/shop', '/shop/felgen', '/shop/reifen'):
                    response = self.client.get(path, base_url=base)
                    self.assertEqual(response.status_code, 302)
                    self.assertEqual(response.location, shop + path + '/')

    def test_other_origins_and_local_previews_keep_shop_links_relative(self):
        shop = 'https://auto-lackierzentrum.de'
        portal = 'https://kundenstatus-app.onrender.com'
        for base, configured in ((shop, shop), ('http://localhost:5000', shop),
                                 ('https://preview.example', shop), (portal, 'https://preview.example')):
            with self.subTest(base=base, configured=configured):
                self.app.config['SHOP_PUBLIC_ORIGIN'] = configured
                with self.app.test_request_context('/homepage', base_url=base):
                    self.assertEqual(self.namespace['homepage_shop_links']()['home'], '/shop/')
                self.assertEqual(self.client.get('/shop', base_url=base).location, '/shop/')


if __name__ == '__main__':
    unittest.main()
