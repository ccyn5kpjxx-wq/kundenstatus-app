"""No provider access. Exercise the actual mapping and the adapter with synthetic replies."""
from copy import deepcopy
import ast
from datetime import date
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_lexware_invoices import LexwareShopInvoices, ShopLexwareError, build_shop_invoice_payload
from test_shop_order_store import quote
from test_shop_order_workflow import CONTEXT

RID = str(UUID(int=1))
OID = 'sho_' + 'c' * 32


def fixture():
    snapshot = quote(mode='live', context=CONTEXT)
    order = {'id': OID, 'mode': 'live', 'status': 'paid', 'quote_hash': snapshot['quote_hash'],
             'customer_id': snapshot['customer_id'], 'currency': 'eur', 'amount_gross_cent': 53200,
             'paid_at': '2026-06-14T23:01:00Z'}
    return snapshot, order


class LexwareShopInvoiceTests(unittest.TestCase):
    def setUp(self):
        throttle = patch('werkstatt_rechnungsquelle._throttle')
        throttle.start()
        self.addCleanup(throttle.stop)
        self.snapshot, self.order = fixture()
        self.payload = self.build()
        self.remote = {**deepcopy(self.payload), 'id': RID, 'voucherStatus': 'open', 'voucherNumber': 'RE-45'}
        self.remote['totalPrice']['totalGrossAmount'] = 532.00
        self.portal = Mock(LEXWARE_API_KEY='mock-key', LEXWARE_API_BASE_URL='https://api.lexware.io')
        self.service = LexwareShopInvoices(self.portal)
        self.transport = self.portal.lexware_request

    def build(self):
        return build_shop_invoice_payload(self.snapshot, self.order, CONTEXT, voucher_date=date(2026, 6, 15))

    def page(self, *ids):
        return {'number': 0, 'last': True, 'totalElements': len(ids), 'content': [
            {'id': rid, 'voucherType': 'invoice', 'totalAmount': 532} for rid in ids]}

    def test_paid_snapshot_lines_billing_address_and_summer_date(self):
        self.assertEqual(self.payload['voucherDate'], '2026-06-15T00:00:00.000+02:00')
        self.assertEqual(self.payload['shippingConditions'], {'shippingType': 'none'})
        self.assertIn('15.06.2026', self.payload['remark'])
        self.assertIn('Stripe', self.payload['paymentConditions']['paymentTermLabel'])
        self.assertEqual(self.payload['address']['name'], CONTEXT['billing']['recipient'])
        self.assertEqual(self.payload['totalPrice'], {'currency': 'EUR'})
        self.assertEqual([(l['quantity'], l['unitPrice']['grossAmount']) for l in self.payload['lineItems']],
                         [(4, 100), (1, 80), (2, 20), (1, 5), (1, 7)])
        self.assertNotIn('supplier_article_id', str(self.payload))
        self.assertNotIn('email', str(self.payload))

    def test_unauthorized_or_mismatched_bindings_rejected_before_network(self):
        for field, value in [('status','authorized'), ('mode','test'), ('quote_hash','b'*64),
                             ('customer_id','another'), ('amount_gross_cent',1), ('currency','usd'),
                             ('paid_at','2026-06-16T00:00:00Z')]:
            with self.subTest(field=field), self.assertRaises(ShopLexwareError):
                build_shop_invoice_payload(self.snapshot, {**self.order,field:value}, CONTEXT,
                                           voucher_date=date(2026,6,15))
        with self.assertRaises(ShopLexwareError):
            build_shop_invoice_payload(self.snapshot, self.order, {**CONTEXT, 'billing': {}}, voucher_date='2026-06-15')
        self.transport.assert_not_called()

    def test_only_one_create_after_search_with_requested_finality(self):
        for finalize in (True, False):
            with self.subTest(finalize=finalize):
                self.transport.reset_mock()
                self.transport.side_effect = [self.page(), {'id': RID}]
                self.assertEqual(self.service.create_invoice(self.payload, finalize=finalize), RID)
                search, create = self.transport.call_args_list
                self.assertEqual(search.args[:2], ('GET','/v1/voucherlist'))
                self.assertIn('voucherStatus=any', search.kwargs['query'])
                self.assertEqual(create.args, ('POST','/v1/invoices',self.payload))
                self.assertEqual(create.kwargs, {'single_attempt': True, 'query':'?finalize=true' if finalize else ''})

    def test_recovery_after_lost_response_and_reuse_of_existing_draft(self):
        self.transport.side_effect = [self.page(), TimeoutError('private data')]
        with self.assertRaisesRegex(ShopLexwareError, '^lexware_invoice_response_unavailable$'):
            self.service.create_invoice(self.payload, finalize=True)
        draft = {**self.remote, 'voucherStatus': 'draft', 'voucherNumber': None}
        self.transport.side_effect = [self.page(RID), draft, self.page(RID), draft]
        self.assertEqual(self.service.recover(self.payload), RID)
        self.assertEqual(self.service.create_invoice(self.payload, finalize=True), RID)
        self.assertEqual(sum(c.args[0] == 'POST' for c in self.transport.call_args_list), 1)

    def test_transient_preflight_read_failure_is_distinct_from_uncertain_write(self):
        self.transport.side_effect = TimeoutError('private reply')
        with self.assertRaisesRegex(ShopLexwareError, '^lexware_invoice_preflight_unavailable$'):
            self.service.create_invoice(self.payload, finalize=True)
        self.assertEqual([call.args[0] for call in self.transport.call_args_list], ['GET'])

    def test_ambiguous_records_incomplete_pages_and_modified_match_are_rejected(self):
        id2 = str(UUID(int=2))
        for replies in ([self.page(RID, id2), self.remote, {**self.remote, 'id': id2}],
                        [{**self.page(), 'totalElements': 1}],
                        [self.page(RID), {**self.remote, 'voucherStatus': 'voided'}],
                        [self.page(RID), {**self.remote, 'address': {}}],
                        [self.page(RID), {**self.remote, 'lineItems': self.remote['lineItems'][:-1]}]):
            with self.subTest(reply=len(replies)), self.assertRaises(ShopLexwareError):
                self.transport.side_effect = replies
                self.service.create_invoice(self.payload, finalize=True)
        self.assertFalse(any(c.args[0] == 'POST' for c in self.transport.call_args_list))

    def test_documented_voucherlist_pagination_and_invoice_read_projection(self):
        other = str(UUID(int=3))
        self.remote.update(voucherDate='2026-06-14T22:00:00.000Z',
                           introduction=self.payload['introduction'].replace(' ', '\u00a0'))
        self.remote['paymentConditions'].update(paymentTermLabel='  Bereits bezahlt\nüber Stripe.  ',
                                               paymentTermLabelTemplate='Bereits bezahlt über Stripe.')
        self.remote['address'].update(contactId=None, supplement=None)
        first = {**self.page(other), 'last':False, 'totalElements':2, 'totalPages':2}
        first['content'][0].update(totalAmount=99.80, contactId=None, voucherStatus='paid')
        second = {**self.page(RID), 'number':1, 'totalElements':2, 'totalPages':2}
        second['content'][0].update(contactId=None, voucherStatus='overdue')
        self.transport.side_effect = [first, second, self.remote]
        self.assertEqual(self.service.recover(self.payload), RID)
        self.assertIn('page=1', self.transport.call_args_list[1].kwargs['query'])
        for status in ['open', 'paid']:
            self.transport.side_effect = [{**self.remote, 'voucherStatus':status}]
            self.assertEqual(self.service.inspect(RID, self.payload)['state'], 'final')

    def test_conflicting_payment_terms_and_non_invoice_detail_status_are_rejected(self):
        for change in [{'paymentConditions':{'paymentTermLabel':'Bitte binnen 14 Tagen überweisen'}},
                       {'voucherStatus':'overdue'}, {'voucherStatus':'paidoff'}]:
            with self.subTest(change=list(change)), self.assertRaises(ShopLexwareError):
                self.transport.return_value = {**self.remote, **change}
                self.service.inspect(RID, self.payload)

    def test_bound_final_original_pdf_get_and_validation(self):
        self.transport.return_value = self.remote
        pdf = b'%PDF-1.7\nSynthetic\n%%EOF\n'
        response = self.portal.get_requests.return_value.request.return_value
        response.status_code, response.headers = 200, {'Content-Type':'application/pdf'}
        response.iter_content.return_value = [pdf]
        self.assertEqual(self.service.pdf(RID, self.payload), pdf)
        req = self.portal.get_requests.return_value.request.call_args
        self.assertEqual(req.args, ('GET', 'https://api.lexware.io/v1/invoices/' + RID + '/file'))
        self.assertEqual(req.kwargs['headers']['Accept'], '*/*')
        self.assertFalse(req.kwargs['allow_redirects'])
        for mime, content in [('application/xml', [pdf]), ('application/pdf', [b'%PDF-partial'])]:
            response.headers = {'Content-Type': mime}
            response.iter_content.return_value = content
            with self.assertRaisesRegex(ShopLexwareError, 'pdf_unavailable'):
                self.service.pdf(RID, self.payload)
        self.assertEqual(response.close.call_count, 3)

    def test_draft_void_and_foreign_documents_cannot_be_downloaded(self):
        for change in [{'voucherStatus':'draft'}, {'voucherStatus':'voided'}, {'id':str(UUID(int=99))},
                       {'introduction':'Ihre Shopbestellung eines anderen Kunden'}, {'remark':'Bitte überweisen'}]:
            self.transport.return_value = {**self.remote, **change}
            with self.assertRaises(ShopLexwareError):
                self.service.pdf(RID, self.payload)
        self.portal.get_requests.assert_not_called()

    def test_invalid_host_never_receives_an_account_request(self):
        self.portal.LEXWARE_API_BASE_URL = 'https://invalid.example'
        self.assertFalse(self.service.configured())
        with self.assertRaisesRegex(ShopLexwareError, 'preflight_unavailable'):
            self.service.create_invoice(self.payload, finalize=True)
        self.transport.assert_not_called()

    def test_shared_json_transport_post_never_replays_or_follows_redirects(self):
        # Load just the existing portal transport without initializing app/db/workers.
        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8-sig')
        tree = ast.parse(source)
        func = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'lexware_request')
        http = Mock()
        scope = {'LEXWARE_API_KEY': 'mock-only', 'LEXWARE_API_BASE_URL': 'https://api.lexware.io',
                 'get_requests': lambda: http}
        exec(compile(ast.Module(body=[func], type_ignores=[]), '<shop transport test>', 'exec'), scope)
        for status in (307,429):
            with self.subTest(status=status):
                http.reset_mock()
                http.request.return_value.status_code = status
                with self.assertRaises(RuntimeError):
                    scope['lexware_request']('POST', '/v1/invoices', self.payload,
                                             query='?finalize=true', single_attempt=True)
                self.assertEqual(http.request.call_count, 1)
                self.assertFalse(http.request.call_args.kwargs['allow_redirects'])


if __name__ == '__main__':
    unittest.main()
