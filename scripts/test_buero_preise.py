"""Read-only price access and unchanged source PDF; no production app or DB."""
import hashlib
from io import BytesIO
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask, session
from jinja2 import ChoiceLoader, DictLoader
from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_buero_preise import (register_office_prices, PAINT_PRICES, EXTRAS,
                                    PDF_PATH, PDF_SHA256, euro, TYRE_TARIFFS,
                                    TYRE_PDF_PATH, TYRE_PDF_SHA256, TYRE_SET_EXAMPLES)


class OfficePriceTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        self.app = Flask(__name__, template_folder=str(root / 'templates'), static_folder=str(root / 'static'))
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-prices-only')
        self.app.jinja_loader = ChoiceLoader([DictLoader({'buero_nav.html': '<nav>Büro</nav>'}), self.app.jinja_loader])
        self.granted = True
        self.active = True

        def identity():
            return {'actor': 'mitarbeiter:7', 'office_categories': ('preise',) if self.granted else ()} if self.active and session.get('office') else None

        self.p = SimpleNamespace(app=self.app, office_access=SimpleNamespace(identity=identity))
        self.service = register_office_prices(self.p)
        self.client = self.app.test_client()
        with self.client.session_transaction() as state:
            state['office'] = True

    def test_fresh_identity_required_for_index_pdf_and_head(self):
        guest = self.app.test_client()
        for path in ('/werkstatt/buero/preise', '/werkstatt/buero/preise/pdf', '/werkstatt/buero/preise/reifen-pdf'):
            with self.subTest(path=path):
                self.assertEqual(guest.get(path).status_code, 403)
                self.assertEqual(self.client.head(path).status_code, 200)
                self.granted = False
                self.assertEqual(self.client.get(path).status_code, 403)
                self.assertEqual(self.client.head(path).status_code, 403)
                self.granted = True
                self.active = False
                self.assertEqual(self.client.get(path).status_code, 403)
                self.active = True

    def test_actual_admin_can_preview_without_office_grant(self):
        self.active = False
        with self.client.session_transaction() as state:
            state.clear()
            state['admin'] = True
        self.assertEqual(self.client.get('/werkstatt/buero/preise').status_code, 200)
        self.assertEqual(self.client.get('/werkstatt/buero/preise/pdf').status_code, 200)

    def test_source_pdf_is_exact_and_download_is_authenticated(self):
        raw = PDF_PATH.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), PDF_SHA256)
        self.assertEqual(len(PdfReader(BytesIO(raw)).pages), 2)
        response = self.client.get('/werkstatt/buero/preise/pdf')
        self.assertEqual(response.data, raw)
        self.assertEqual(response.mimetype, 'application/pdf')
        self.assertTrue(response.headers['Content-Disposition'].startswith('inline;'))
        download = self.client.get('/werkstatt/buero/preise/pdf?download=1')
        self.assertEqual(download.data, raw)
        self.assertTrue(download.headers['Content-Disposition'].startswith('attachment;'))
        self.assertEqual(self.client.get('/static/resources/buero/Interne_Preisliste_Netto_Arbeitsstand.pdf').status_code, 404)
        self.assertEqual(self.client.get('/resources/buero/Interne_Preisliste_Netto_Arbeitsstand.pdf').status_code, 404)

    def test_display_matrix_matches_all_source_pdf_rows_without_new_calculation(self):
        text = PdfReader(PDF_PATH).pages[0].extract_text()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for label, *prices, provisional in PAINT_PRICES:
            with self.subTest(label=label):
                start = lines.index(label + ('*' if provisional else ''))
                cents = []
                for line in lines[start + 1:start + 5]:
                    match = re.fullmatch(r'(\d+),(\d{2}) €', line)
                    self.assertIsNotNone(match, 'Expected a source table price after ' + label)
                    cents.append(int(match[1]) * 100 + int(match[2]))
                self.assertEqual(prices, cents)
        for label, _hint, partner, private, _surcharge in EXTRAS:
            source_label = {'Demontage und Montage zusammen': 'Einheitliche Pauschale je Bauteil',
                            'Dreischichtlack': 'Dreischichtlack-Zuschlag'}.get(label, label)
            start = lines.index(source_label)
            source_prices = []
            for line in lines[start + 1:start + 5]:
                match = re.fullmatch(r'(?:\+ )?(\d+),(\d{2}) €', line)
                if match: source_prices.append(int(match[1]) * 100 + int(match[2]))
            self.assertEqual(source_prices[:2], [partner, private])

    def test_readable_source_bound_net_prices_and_open_repair_rule(self):
        html = self.client.get('/werkstatt/buero/preise').get_data(as_text=True)
        for phrase in ('30.10.2026', '222,75 €', '247,50 €', 'Neuteil', 'Gebrauchtteil',
                       'Partner', 'Privat', 'zuzüglich 19 % MwSt.', '100–150 € netto',
                       'ca. 200 € netto', 'Partner-/Privatzuordnung noch abzustimmen',
                       'Demontage und Montage zusammen', '+ 50,00 €', 'vorläufig',
                       'nicht zusätzlich als Beilackierbauteil'):
            self.assertIn(phrase, html)
        self.assertNotIn('Noch nicht in Lexware übernommen', html)
        self.assertNotIn('Lexware wurde', html)
        self.assertEqual(euro(22275), '222,75 €')
        self.assertEqual(euro(120000), '1.200,00 €')
        self.assertEqual(euro(-420), '-4,20 €')

    def test_private_no_cache_and_no_writing_routes(self):
        for path in ('/werkstatt/buero/preise', '/werkstatt/buero/preise/pdf', '/werkstatt/buero/preise/reifen-pdf'):
            response = self.client.get(path)
            self.assertIn('no-store', response.headers['Cache-Control'])
            self.assertIn('noindex', response.headers['X-Robots-Tag'])
            self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
            self.assertNotIn('ETag', response.headers)
            self.assertEqual(self.client.post(path).status_code, 405)

    def test_registration_does_not_open_original_and_invalid_original_fails_closed(self):
        other = Flask('synthetic-other')
        with patch('pathlib.Path.read_bytes', side_effect=AssertionError('inert registration')):
            register_office_prices(SimpleNamespace(app=other))
        with patch('pathlib.Path.read_bytes', return_value=b'%PDF-not-the-confirmed-source'):
            self.assertEqual(self.client.get('/werkstatt/buero/preise/pdf').status_code, 503)
            self.assertEqual(self.client.get('/werkstatt/buero/preise').status_code, 503)

    def test_twenty_verified_services_match_all_applicable_source_pdf_price_pairs(self):
        text = PdfReader(TYRE_PDF_PATH).pages[0].extract_text()
        amounts = []
        for line in text.splitlines():
            match = re.fullmatch(r'(-?)(\d+),(\d{2}) €', line.strip())
            if match:
                value = int(match[2]) * 100 + int(match[3])
                amounts.append(-value if match[1] else value)
        self.assertEqual(len(amounts), 48, 'The source PDF has exactly 24 net/gross price rows.')
        pairs = list(zip(amounts[::2], amounts[1::2]))
        source_positions = (0, 1, 2, 3, 4, 5, 7, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23)
        self.assertEqual(len(TYRE_TARIFFS), 20)
        self.assertEqual(len({row[1] for row in TYRE_TARIFFS}), 20)
        for row, position in zip(TYRE_TARIFFS, source_positions):
            with self.subTest(article=row[1]):
                self.assertEqual(row[4:6], pairs[position])
        self.assertEqual(tuple((row[1], row[2]) for row in TYRE_SET_EXAMPLES), tuple(pairs[i] for i in (6, 8, 10)))
        self.assertEqual(pairs[21], (-420, -500))
        self.assertEqual([row[6] for row in TYRE_TARIFFS], ['GROSS'] * 18 + ['NET'] * 2)

    def test_eighteen_tyre_services_match_current_repository_catalogue_without_price_markup(self):
        from reifenservice import reifen_preiskatalog, berechne_reifenservice
        current = {row['id']: row for row in reifen_preiskatalog()}
        self.assertEqual(set(current), {row[0] for row in TYRE_TARIFFS[:18]})
        for source_id, article, title, unit, net, gross, basis, scope in TYRE_TARIFFS[:18]:
            with self.subTest(article=article):
                self.assertEqual(gross, current[source_id]['brutto_cent'])
                self.assertEqual(unit, 'Satz (4 Räder)' if current[source_id]['einheit'] == 'Satz' else current[source_id]['einheit'])
                single = berechne_reifenservice([{'preis_id': source_id, 'menge': 1}]) if current[source_id]['gruppe'] != 'montage_zusatz' else None
                if single is not None:
                    self.assertEqual(single['positionen'][0]['netto_cent'], net)
        for source_id, example in zip(('montage_13_16', 'montage_17_18', 'montage_19_20'), TYRE_SET_EXAMPLES):
            result = berechne_reifenservice([{'preis_id': source_id, 'menge': 4}])
            self.assertEqual((result['positionen'][0]['netto_cent'], result['positionen'][0]['brutto_cent']), example[1:])

    def test_tyres_alignment_and_discount_units_are_clear_and_no_duplicate_services_created(self):
        html = self.client.get('/werkstatt/buero/preise').get_data(as_text=True)
        self.assertEqual(html.count('data-price-article='), 20)
        for row in TYRE_TARIFFS:
            with self.subTest(article=row[1]):
                self.assertIn('data-price-article="' + row[1] + '"', html)
                self.assertIn(euro(row[4]), html)
                self.assertIn(euro(row[5]), html)
        for phrase in ('href="#lackierung"', 'href="#reifenservice"', 'href="#achsvermessung"',
                       '28.10.2026', '20 Reifen-/Achsartikel am 30.10.2026 abgeglichen',
                       'Brutto · 19 % MwSt.', 'Satz (4 Räder)', 'keine zusätzlichen Artikel',
                       '-4,20 € netto / -5,00 € brutto', 'nicht nochmals abziehen',
                       'Saisonpakete enthalten genau einen Wechsel', 'keine Jahrespreise',
                       'Netto-Tarife', 'Zusatzarbeiten und Kosten vorab abstimmen'):
            self.assertIn(phrase, html)
        self.assertNotIn('C:\\Users\\', html)
        self.assertNotIn('lexware_id', html)

    def test_tyre_pdf_is_exact_one_page_private_original_and_not_public(self):
        raw = TYRE_PDF_PATH.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), TYRE_PDF_SHA256)
        self.assertEqual(len(PdfReader(BytesIO(raw)).pages), 1)
        result = self.client.get('/werkstatt/buero/preise/reifen-pdf')
        self.assertEqual(result.data, raw)
        self.assertTrue(result.headers['Content-Disposition'].startswith('inline;'))
        result = self.client.get('/werkstatt/buero/preise/reifen-pdf?download=1')
        self.assertTrue(result.headers['Content-Disposition'].startswith('attachment;'))
        self.assertEqual(self.client.get('/static/resources/buero/Preisliste_Reifenservice_Achsvermessung.pdf').status_code, 404)
        self.assertEqual(self.client.get('/resources/buero/Preisliste_Reifenservice_Achsvermessung.pdf').status_code, 404)


if __name__ == '__main__':
    unittest.main()
