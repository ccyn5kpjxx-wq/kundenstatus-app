"""Synthetic column fixtures and temporary PDFs; no real invoice data/network."""

from copy import deepcopy
import pathlib
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_topcolor_positionen import parse_topcolor_pages, explicit_package_evidence
import werkstatt_rechnungsquelle as reader


def word(x, y, text):
    return (x, y, x + max(1, len(text)) * 4, y + 10, text, 0, 0, 0)


def page(number=1, lines=(), header=True):
    words = [word(x, 50, text) for x, text in (
        (26, "Pos."), (53, "Art.-Nr."), (103, "Bezeichnung"), (303, "Menge"),
        (360, "Inhalt"), (391, "ME"), (415, "Preis"), (440, "ME"), (514, "Gesamt"))] if header else []
    words.extend(item for row in lines for item in row)
    return {"page": number, "height": 842, "words": words}


def product(position=1, article="87650001", name="Testlack 0,5 Liter", y=100,
            quantity="2,00", content="0,500", measure="Ltr/KG", base="40,00", discount="-20,00%", total="32,00"):
    return [word(x, y, text) for x, text in (
        (38, f"{position}."), (53, article), (103, name), (315, quantity),
        (360, content), (391, measure), (433, base), (470, discount), (530, total)) if text]


class TableParserTests(unittest.TestCase):
    def parse(self, pages):
        return parse_topcolor_pages(pages, "Auto-Color / Topcolor")

    def test_supplier_and_complete_header_layout_required(self):
        self.assertIsNone(parse_topcolor_pages([page(lines=[product()])], "Other supplier"))
        self.assertIsNone(self.parse([page(header=False, lines=[product()])]))
        wrong = page(lines=[product()])
        wrong["words"] = [w for w in wrong["words"] if w[4] != "Inhalt"]
        self.assertIsNone(self.parse([wrong]))

    def test_quantity_content_discount_and_package_net_price_are_separate(self):
        result = self.parse([page(lines=[product()])])
        self.assertTrue(result["complete"])
        item = result["positions"][0]
        self.assertEqual(item["artikelnummer"], "87650001")
        self.assertEqual(item["stueckzahl"], "2")
        self.assertEqual(item["gebinde"], "0.5 L")
        self.assertEqual(item["ve"], "Gebinde")
        self.assertEqual(item["preis"], "16.00")
        self.assertEqual(item["price_evidence"]["line_total_net"], "32")
        self.assertEqual(item["price_evidence"]["content"], "0.5")
        self.assertEqual(item["price_evidence"]["base_price_per_measure"], "40")
        self.assertFalse(item["price_verified"])
        self.assertFalse(item["preis_geprueft"])
        self.assertEqual(item["price_evidence"]["basis"], "gebindepreis_netto_abgeleitet")
        self.assertEqual(item['quantity_evidence']['value'], '2')
        self.assertEqual(item['quantity_evidence']['unit'], 'Gebinde')
        self.assertEqual(item['quantity_evidence']['source_field'], 'Menge')
        self.assertEqual(item['package_evidence']['value'], '0.5')
        self.assertEqual(item['package_evidence']['unit'], 'L')
        self.assertEqual(item['package_evidence']['per_unit'], 'Gebinde')

    def test_variants_and_pack_sizes_preserve_multiline_descriptions(self):
        pages = [page(lines=[
            product(name="Test-Schleifblatt", content="1,000", measure="Pack", quantity="1,00", total="32,00"),
            [word(103, 112, "80x200mm P80 25/Pack")],
            product(2, "87650002", "Test-Schleifblatt", y=140, content="1,000", measure="Pack", quantity="1,00", total="32,00"),
            [word(103, 152, "80x200mm P180 25/Pack")]])]
        items = self.parse(pages)["positions"]
        self.assertEqual(items[0]["gebinde"], "25 Stück/Pack")
        self.assertEqual(items[0]["groesse"], "80x200mm / P80")
        self.assertEqual(items[1]["groesse"], "80x200mm / P180")
        self.assertNotEqual(items[0]["produkt_name"], items[1]["produkt_name"])
        self.assertEqual(items[0]['quantity_evidence']['value'], '1')
        self.assertEqual(items[0]['quantity_evidence']['unit'], 'Pack')
        self.assertEqual(items[0]['package_evidence']['value'], '25')
        self.assertEqual(items[0]['package_evidence']['unit'], 'Stück')
        self.assertEqual(items[0]['package_evidence']['per_unit'], 'Pack')

    def test_one_pack_in_content_column_does_not_mean_one_piece_per_pack(self):
        item = self.parse([page(lines=[product(name='Test Schleifpapier', quantity='2,00',
            content='1,000', measure='Pack', total='64,00')])])['positions'][0]
        self.assertEqual(item['quantity_evidence']['value'], '2')
        self.assertEqual(item['quantity_evidence']['unit'], 'Pack')
        self.assertIsNone(item['package_evidence']['value'])
        self.assertEqual(item['package_evidence']['basis'], 'unknown')

    def test_price_measure_is_not_invoice_unit_for_multi_piece_content(self):
        item = self.parse([page(lines=[product(name='Schleifpapier 25/Pack', quantity='2,00',
            content='25,000', measure='Stück', base='1,00', discount='', total='50,00')])])['positions'][0]
        self.assertEqual(item['quantity_evidence']['value'], '2')
        self.assertIsNone(item['quantity_evidence']['unit'])
        self.assertEqual(item['ve'], '')
        self.assertEqual(item['package_evidence']['value'], '25')
        self.assertEqual(item['package_evidence']['per_unit'], 'Pack')

    def test_conflicting_package_description_does_not_choose_arbitrary_count(self):
        item = self.parse([page(lines=[product(name='Schleifpapier 25/Pack 50/Pack',
            content='1,000', measure='Pack', total='64,00')])])['positions'][0]
        self.assertEqual(item['package_evidence']['basis'], 'unknown')
        item = self.parse([page(lines=[product(name='Testlack 5 Liter')])])['positions'][0]
        self.assertEqual(item['package_evidence']['basis'], 'unknown')

    def test_rolls_per_carton_keep_content_unit_and_do_not_count_roll_length(self):
        evidence = explicit_package_evidence('Abdeckband 48 mm x 50 m, 6 Rollen/Karton', 'Karton')
        self.assertEqual((evidence['value'], evidence['unit'], evidence['per_unit']), ('6', 'Rolle', 'Karton'))
        evidence = explicit_package_evidence('Abdeckband 48 mm x 50 m', 'Rolle')
        self.assertEqual(evidence['basis'], 'unknown')

    def test_pieces_per_ve_are_explicit_content_independent_of_invoice_quantity(self):
        for width, count in ((25, 36), (30, 32), (50, 24)):
            name = f'Test-Abdeckband 50 m Rolle x {width} mm ({count} Stück/VE)'
            item = self.parse([page(lines=[product(name=name, quantity='2,00', content='1,000',
                measure='Stück', base='1,00', discount='', total='2,00')])])['positions'][0]
            evidence = item['package_evidence']
            self.assertEqual((evidence['value'], evidence['unit'], evidence['per_unit']), (str(count), 'Stück', 'VE'))
            self.assertEqual(evidence['basis'], 'explicit_description')
            self.assertEqual(evidence['text'], f'{count} Stück/VE')
            self.assertFalse(evidence['verified'])
            self.assertEqual(item['quantity_evidence']['value'], '2')
        for spelling in ('36 Stueck / VE', '36 Stk./VE', '36 stück/ve'):
            self.assertEqual(explicit_package_evidence('Abdeckband ('+spelling+')')['value'], '36')
        self.assertEqual(explicit_package_evidence('Abdeckband (36 Stück/VE) (24 Stück/VE)')['basis'], 'unknown')

    def test_fee_is_not_a_product_or_continuation(self):
        result = self.parse([page(lines=[product(),
            product(2, "00000071", "Logistik-", y=130), [word(103, 142, "/Energiekostenpauschale")],
            product(3, "87650003", "Weiterer Testlack 0,5 Liter", y=160)])])
        self.assertEqual(len(result["positions"]), 2)
        self.assertEqual(len(result["fees"]), 1)
        self.assertNotIn("Energiekosten", result["positions"][0]["produkt_name"])
        self.assertTrue(result["complete"])

    def test_cross_page_description_and_provenance(self):
        result = self.parse([page(lines=[product(name="Test-Klarlack 1", content="1,000", total="64,00")]),
                             page(2, lines=[[word(103, 75, "Liter")], product(2, "87650002", y=95)])])
        item = result["positions"][0]
        self.assertEqual(item["produkt_name"], "Test-Klarlack 1 Liter")
        self.assertEqual(item["native_source"]["pages"], [1, 2])
        self.assertEqual(item["native_source"]["position"], 1)

    def test_metadata_footer_and_notes_never_become_product_content(self):
        lines = [product(), [word(90, 112, "AUFTRAG"), word(210, 112, "SYNTHETIC")],
                 [word(90, 125, "LIEFERSCHEIN"), word(210, 125, "SYNTHETIC")],
                 [word(90, 138, "ACHTUNG:"), word(103, 138, "untrusted note")],
                 [word(103, 150, "Never append this")], [word(103, 780, "private footer")]]
        item = self.parse([page(lines=lines)])["positions"][0]
        self.assertEqual(item["produkt_name"], "Testlack 0,5 Liter")

    def test_missing_quantity_or_inconsistent_price_is_not_guessed(self):
        for changes in ({"quantity": ""}, {"total": "99,00"}, {"discount": "-120,00%"}, {"base": "unknown"}):
            with self.subTest(changes=changes):
                result = self.parse([page(lines=[product(**changes)])])
                self.assertFalse(result["complete"])
                self.assertEqual(result["positions"][0]["preis"], "")
                self.assertTrue(result["positions"][0]["auslese_hinweise"])
        self.assertIsNone(self.parse([page(lines=[product(quantity="")])])["positions"][0]["stueckzahl"])

    def test_missing_page_layout_and_position_gaps_require_review(self):
        result = self.parse([page(lines=[product()]), page(2, header=False, lines=[product(2)])])
        self.assertFalse(result["complete"])
        result = self.parse([page(lines=[product(2)])])
        self.assertFalse(result["complete"])

    def test_unrecognized_lower_rows_and_conflicting_packaging_require_review(self):
        result = self.parse([page(lines=[product(), product(2, "87650002", y=730)])])
        self.assertFalse(result["complete"])
        result = self.parse([page(lines=[product(name="Testlack 5 Liter")])])
        self.assertFalse(result["complete"])
        self.assertEqual(result["positions"][0]["preis"], "")

    def test_rounding_and_no_discount(self):
        result = self.parse([page(lines=[product(name="Testlack 1 Liter", quantity="1,00", content="1,000", base="12,35", discount="-10,00%", total="11,12")])])
        self.assertEqual(result["positions"][0]["preis"], "11.12")
        result = self.parse([page(lines=[product(name="Testlack 1 Liter", quantity="1,00", content="1,000", base="12,35", discount="", total="12,35")])])
        self.assertEqual(result["positions"][0]["preis"], "12.35")

    def test_inputs_remain_unchanged(self):
        pages = [page(lines=[product()])]
        before = deepcopy(pages)
        self.parse(pages)
        self.assertEqual(pages, before)


class SourceIntegrationTests(unittest.TestCase):
    def test_rich_text_without_products_is_incomplete(self):
        portal = SimpleNamespace(extract_einkauf_beleg_positions=Mock(return_value=[]),
                                 extract_einkauf_beleg_positions_openai=Mock())
        result = reader._result("einkauf", 1)
        self.assertFalse(reader._extract_page(portal, None, "synthetic.pdf", "Rich table text " * 20,
                                             result, "file", 1, "digest"))
        self.assertTrue(result["warnings"])
        portal.extract_einkauf_beleg_positions_openai.assert_not_called()

    def test_native_candidate_keeps_packaging_and_price_provenance_unverified(self):
        position = parse_topcolor_pages([page(lines=[product()])], "Topcolor")["positions"][0]
        item = reader._candidate(position, {"supplier": "Topcolor"}, "file", 1, "digest", native=True)
        self.assertEqual(item["gebinde"], "0.5 L")
        self.assertEqual(item["source"]["position"], 1)
        self.assertEqual(item["source"]["pages"], [1])
        self.assertFalse(item["verified"])
        self.assertFalse(item["price_evidence"]["verified"])
        self.assertEqual(item["price_evidence"]["basis"], "gebindepreis_netto_abgeleitet")
        self.assertEqual(item['quantity_evidence']['value'], '2')
        self.assertEqual(item['package_evidence']['value'], '0.5')
        self.assertEqual(item['source']['quantity_version'], 1)

    def test_read_source_uses_native_pdf_columns_without_cloud_or_generic_parser(self):
        try:
            import fitz
        except ImportError:
            self.skipTest("PyMuPDF unavailable")
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source_page = page(lines=[product(name="Synthetic paint 0,5 Liter")])
            with fitz.open() as document:
                pdf_page = document.new_page(width=595, height=842)
                for w in source_page["words"]:
                    pdf_page.insert_text((w[0], w[1] + 10), w[4], fontsize=8)
                document.save(root / "synthetic.pdf")
            path = root / "source.db"
            def get_db():
                db = sqlite3.connect(path)
                db.row_factory = sqlite3.Row
                return db
            db = get_db()
            db.executescript("""CREATE TABLE einkauf_belege(id INTEGER,beleg_typ TEXT,lieferant TEXT,original_name TEXT,
                stored_name TEXT,extrahierter_text TEXT,status TEXT);
                INSERT INTO einkauf_belege VALUES(1,'rechnung','Topcolor','synthetic.pdf','synthetic.pdf','','importiert');""")
            db.commit()
            db.close()
            generic, vision = Mock(), Mock()
            portal = SimpleNamespace(get_db=get_db, UPLOAD_DIR=root, get_fitz=lambda: fitz,
                extract_einkauf_beleg_positions=generic, extract_einkauf_beleg_positions_openai=vision)
            result = reader.read_source(portal, "einkauf", 1)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(len(result["candidates"]), 1)
            self.assertEqual(result["candidates"][0]["artikelnummer"], "87650001")
            self.assertEqual(result["candidates"][0]["preis"], "16.00")
            generic.assert_not_called()
            vision.assert_not_called()


if __name__ == "__main__":
    unittest.main()
