"""Keep printed checkboxes faithful to the saved work scope."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_druck_checkliste import checklist_rows


class ChecklistTests(unittest.TestCase):
    def test_partner_rows_and_normal_list_separators(self):
        text = ("Arbeitsumfang:\r\n1. Tür vorne rechts lackieren; Tür hinten rechts lackieren\n"
                "• Stoßfänger (hinten): angeliefertes Neuteil lackieren – beauftragt\n"
                "- Leiste montieren • Halter prüfen")
        self.assertEqual(checklist_rows(text), [
            {'text': 'Tür vorne rechts lackieren', 'new_part': False},
            {'text': 'Tür hinten rechts lackieren', 'new_part': False},
            {'text': 'Stoßfänger (hinten): angeliefertes Neuteil lackieren – beauftragt', 'new_part': True},
            {'text': 'Leiste montieren', 'new_part': False},
            {'text': 'Halter prüfen', 'new_part': False},
        ])

    def test_only_clearly_scoped_door_pairs_split(self):
        self.assertEqual([x['text'] for x in checklist_rows('Türen vorne rechts und hinten rechts lackieren.')],
                         ['Tür vorne rechts lackieren.', 'Tür hinten rechts lackieren.'])
        self.assertEqual(len(checklist_rows('Tür vorne und hinten rechts lackieren')), 1)
        for text in ('Kotflügel instand setzen und lackieren', 'Position 3: Tür vorne rechts, Farbton beibehalten'):
            self.assertEqual(checklist_rows(text), [{'text': text, 'new_part': False}])
        self.assertEqual(checklist_rows(''), [])

    def test_explicit_new_parts_and_negative_or_historical_mentions(self):
        for text in ('Kotflügel: Neuteillackierung', 'Neue Stoßfängerleiste in Schwarz lackieren',
                     'Stoßfänger wurde als Neuteil angeliefert, in Wagenfarbe lackieren',
                     'Motorhaube (neu) montieren', 'Leiste als Neues Teil lackieren'):
            with self.subTest(text=text):
                self.assertTrue(checklist_rows(text)[0]['new_part'])
        for text in ('Stoßfänger neu lackieren', 'Kotflügel gebraucht ersetzen', 'Kein Neuteil verwenden',
                     'Neuteil nicht erforderlich', 'Ohne Neuteile instand setzen',
                     'Falls erforderlich Neuteil bestellen', 'Neuteil?', 'Früher als Neuteil montiert',
                     'Das war ein Neuteil', 'Tür wurde als Neuteil eingebaut', 'Neuteil-Angebot anfragen',
                     'Vor zwei Jahren als Neuteil verbaut', 'Neue Lackierung vornehmen'):
            with self.subTest(text=text):
                self.assertEqual(checklist_rows(text), [{'text': text, 'new_part': False}])


if __name__ == '__main__':
    unittest.main()
