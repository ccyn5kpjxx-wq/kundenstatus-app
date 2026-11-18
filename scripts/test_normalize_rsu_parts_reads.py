"""Strict semantic parsing of original supplier DOM; temporary local inputs only."""
from hashlib import sha256
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parent))
from normalize_rsu_parts_reads import dom_product, normalize
from reconcile_rsu_parts import ReconciliationError
from test_reconcile_rsu_parts import row, EAN


def dom(quantity=1,offers=1):
    lines=['- heading "SYNTHETIC OIL A-1" [level=1]:','- generic: Fahrzeug auswählen',f'- generic: "{quantity}"',
           '    - row "Lieferoptionen Bestand EK Stück Versand EK Gesamt VK1":']
    for _ in range(offers):lines.append(f'    - row "vorauss. 20.11.26-26.11.26 2 10,00 € 6,00 € {10*quantity+6},00 € 15,00 €":')
    lines += ['- button "In den Warenkorb":','  - row "Marke MAKER":','  - row "Hersteller Artikelnummer A-1":',
              '  - row "RSU Artikelnummer 123":',f'  - row "EAN {EAN}":']
    return '\n'.join(lines).encode()


class NormalizeReadChecks(unittest.TestCase):
    def setUp(self):
        guard=patch.object(socket,'socket',side_effect=AssertionError('No HTTP'));guard.start();self.addCleanup(guard.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.path=self.root/'supplier.dom.txt';self.path.write_bytes(dom())
        self.rest={'schema_version':1,'supplier':'RSU','mode':'live','requests':1,'orders':0,'payments':0,
            'records':[{'supplier_article_id':'123','expected_ean':EAN,'ean':EAN,'quantity':1,'mode':'live',
                        'operation':'REST_INQUIRY_GET','ok':True,'unit_net':'10.00','stock':2,
                        'retrieved_at':'2026-11-18T11:59:00+00:00'}]}

    def call(self):
        return normalize({'products':[row()]},'a'*64,None,self.rest,{'file':'rest.json','sha256':'d'*64},
            {'schema_version':1,'captures':[{'file':str(self.path),'sha256':sha256(self.path.read_bytes()).hexdigest(),
                                            'observed_at':'2026-11-18T11:58:00+00:00'}]},capture_root=self.root)

    def test_unique_offer_identity_quantity_stock_net_and_arithmetic_are_retained(self):
        parsed=dom_product(dom());self.assertEqual(parsed['ean'],EAN);self.assertEqual(parsed['quantity'],1)
        self.assertEqual(parsed['offers'],[{'stock':2,'unit_net':'10.00','freight_net':'6.00'}])
        value,diagnostics=self.call();self.assertEqual(len(value['records']),1)
        proof=value['records'][0]['freight'];self.assertEqual(proof['offer_count'],1);self.assertEqual(proof['freight_net'],'6.00')
        self.assertTrue(diagnostics[0]['ui_rest_net_and_stock_correlated'])

    def test_two_offers_q2_or_changed_rest_values_do_not_generate_freight(self):
        for quantity,offers in [(1,2),(2,1)]:
            self.path.write_bytes(dom(quantity,offers));value,_=self.call();self.assertIsNone(value['records'][0]['freight'])
        self.path.write_bytes(dom());self.rest['records'][0]['stock']=3
        value,_=self.call();self.assertIsNone(value['records'][0]['freight'])

    def test_wrong_original_ean_duplicate_labels_and_total_mismatch_fail(self):
        for payload in [dom().replace(EAN.encode(),b'5901234123457'),dom()+f'\n  - row "EAN {EAN}":'.encode(),
                        dom().replace(b'16,00',b'17,00')]:
            self.path.write_bytes(payload)
            with self.assertRaises(ReconciliationError):self.call()

    def test_failed_non_get_or_payment_source_cannot_be_promoted(self):
        self.rest['records'][0]['operation']='POST_ORDER'
        with self.assertRaises(ReconciliationError):self.call()
        self.rest['records'][0]['operation']='REST_INQUIRY_GET';self.rest['orders']=1
        with self.assertRaises(ReconciliationError):self.call()


if __name__=='__main__':unittest.main()
