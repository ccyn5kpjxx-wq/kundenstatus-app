"""Sales invoices from frozen, paid shop orders using the portal's Lexware account.

The caller commits its claim before create_invoice. A lost POST response is
reconciled via recover/inspect. Never repeat a POST to resolve uncertainty.
"""
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import re
from unicodedata import normalize
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

from shop_checkout import ShopCheckout
from shop_order_receipts import _receipt_context
from shop_order_store import ORDER_ID, _delivery_context, _validate_snapshot


class ShopLexwareError(ValueError):
    """Safe error code only. Provider response data may contain personal details."""


def _day(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError()
        return value.astimezone(ZoneInfo('Europe/Berlin')).date()
    if type(value) is date:
        return value
    if isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        return date.fromisoformat(value)
    raise ValueError()


def _uuid(value):
    try:
        result = str(UUID(value))
        if value.lower() != result:
            raise ValueError()
        return result
    except (ValueError, TypeError, AttributeError):
        raise ShopLexwareError('lexware_invoice_reference_invalid') from None


def _printed_text(value):
    if not isinstance(value, str):
        raise ShopLexwareError('lexware_invoice_binding_mismatch')
    return ' '.join(normalize('NFC', value).split())


def build_shop_invoice_payload(snapshot, order, context, *, voucher_date):
    """Map exact captured checkout positions to a gross invoice. No contacts are written."""
    try:
        if (order['status'] != 'paid' or order['mode'] != 'live' or order['currency'] != 'eur'
                or not ORDER_ID.fullmatch(order['id']) or type(order['amount_gross_cent']) is not int
                or order['amount_gross_cent'] != snapshot['total_gross_cent']):
            raise ValueError()
        _validate_snapshot(snapshot, order['quote_hash'], order['customer_id'], order['mode'])
        _delivery_context(snapshot, context)
        buyer = _receipt_context(context)
        address = buyer['billing']
        items = ShopCheckout._line_items(snapshot, order['amount_gross_cent'])
        shipment_start = len(items) - sum(row['gross_cent'] > 0 for row in snapshot['shipments'])
        day = _day(voucher_date)
        issue = datetime.combine(day, time(0), tzinfo=ZoneInfo('Europe/Berlin'))
        paid_note = 'Ihre Vorauszahlung wurde vollständig über Stripe eingezogen.'
        if order.get('paid_at'):
            paid_day = _day(datetime.fromisoformat(order['paid_at'].replace('Z', '+00:00')))
            if paid_day > day:
                raise ValueError()
            paid_note = f'Ihre Vorauszahlung wurde am {paid_day:%d.%m.%Y} vollständig über Stripe eingezogen.'
        lines = []
        for index, item in enumerate(items):
            price = item['price_data']
            name = price['product_data']['name']
            if (index >= shipment_start and name == 'Versand zur Werkstatt'
                    and buyer.get('workshop_delivery') == 'customer_reshipment'):
                name = 'Versand zur Werkstatt und Weiterversand zum Kunden'
            lines.append({'type': 'custom', 'name': name, 'quantity': item['quantity'], 'unitName': 'Stk.',
                          'unitPrice': {'currency': 'EUR', 'grossAmount': price['unit_amount'] / 100,
                                        'taxRatePercentage': 19}, 'discountPercentage': 0})
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        raise ShopLexwareError('shop_invoice_binding_mismatch') from None

    return {'voucherDate': issue.isoformat(timespec='milliseconds'),
            'address': {'name': address['recipient'], 'street': address['street'],
                        'zip': address['postcode'], 'city': address['city'], 'countryCode': address['country']},
            'lineItems': lines, 'totalPrice': {'currency': 'EUR'}, 'taxConditions': {'taxType': 'gross'},
            'shippingConditions': {'shippingType': 'none'}, 'title': 'Rechnung',
            'introduction': 'Ihre Shopbestellung ' + order['id'],
            'paymentConditions': {'paymentTermLabel': 'Bereits bezahlt über Stripe.', 'paymentTermDuration': 0},
            'remark': paid_note + ' Der offene Betrag für diese Bestellung beträgt 0,00 €.'}


def _amount(value):
    if type(value) not in (int, float, Decimal):
        raise ShopLexwareError('lexware_invoice_binding_mismatch')
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise ValueError()
        return amount
    except (InvalidOperation, ValueError):
        raise ShopLexwareError('lexware_invoice_binding_mismatch') from None


def _gross(payload):
    return sum(_amount(row['unitPrice']['grossAmount']) * _amount(row['quantity']) for row in payload['lineItems'])


def _line(row):
    price = row['unitPrice']
    return (row['type'], row['name'], row['unitName'], _amount(row['quantity']), price['currency'],
            _amount(price['grossAmount']), _amount(price['taxRatePercentage']),
            _amount(row.get('discountPercentage') or 0))


class LexwareShopInvoices:
    def __init__(self, portal):
        self.portal = portal

    def configured(self):
        return (getattr(self.portal, 'LEXWARE_API_BASE_URL', '').rstrip('/') == 'https://api.lexware.io'
                and bool(getattr(self.portal, 'LEXWARE_API_KEY', ''))
                and callable(getattr(self.portal, 'lexware_request', None)))

    def _request(self, method, path, *, payload=None, query=''):
        if not self.configured():
            raise ShopLexwareError('lexware_invoice_not_configured')
        try:
            from werkstatt_rechnungsquelle import _throttle
            _throttle()
            result = self.portal.lexware_request(method, path, payload, query=query, single_attempt=True)
            if not isinstance(result, dict):
                raise ValueError()
            return result
        except Exception:
            raise ShopLexwareError('lexware_invoice_response_unavailable') from None

    def create_invoice(self, payload, *, finalize):
        """Single POST for an exclusively claimed first attempt, after read-only reconciliation."""
        if type(finalize) is not bool:
            raise ShopLexwareError('invalid_invoice_action')
        try:
            existing = self.recover(payload)
        except ShopLexwareError as exc:
            if str(exc) in {'lexware_invoice_response_unavailable', 'lexware_invoice_not_configured'}:
                raise ShopLexwareError('lexware_invoice_preflight_unavailable') from None
            raise
        if existing:
            return existing
        created = self._request('POST', '/v1/invoices', payload=payload,
                                query='?finalize=true' if finalize else '')
        return _uuid(created.get('id'))

    @staticmethod
    def _verify(raw, remote_id, payload, *, voucher_number=None):
        try:
            invoice_day = _day(datetime.fromisoformat(raw['voucherDate'].replace('Z', '+00:00')))
            terms = raw['paymentConditions']
            expected_terms = payload['paymentConditions']
            if (_uuid(raw['id']) != remote_id or _printed_text(raw['introduction']) != payload['introduction']
                    or invoice_day.isoformat() != payload['voucherDate'][:10]
                    or _printed_text(raw['title']) != payload['title']
                    or any(raw['address'].get(k) != v for k, v in payload['address'].items())
                    or _printed_text(raw['remark']) != payload['remark']
                    or _printed_text(terms['paymentTermLabel']) != expected_terms['paymentTermLabel']
                    or type(terms.get('paymentTermDuration')) is not int
                    or terms['paymentTermDuration'] != expected_terms['paymentTermDuration']
                    or raw['shippingConditions']['shippingType'] != payload['shippingConditions']['shippingType']
                    or raw['taxConditions']['taxType'] != 'gross' or raw['totalPrice']['currency'] != 'EUR'
                    or _amount(raw['totalPrice']['totalGrossAmount']) != _gross(payload)
                    or [_line(row) for row in raw['lineItems']] != [_line(row) for row in payload['lineItems']]):
                raise ValueError()
            status = raw['voucherStatus']
            # A voucherlist row may be overdue/paidoff; an invoice resource
            # itself reports draft, open, paid or voided.
            if status not in {'draft', 'open', 'paid'} or raw.get('archived') is True:
                raise ValueError()
            number = raw.get('voucherNumber') or ''
            if ((status != 'draft' and not number) or not isinstance(number, str) or len(number) > 80
                    or voucher_number is not None and number != voucher_number
                    or any(ord(c) < 32 for c in number)):
                raise ValueError()
            return {'state': 'draft' if status == 'draft' else 'final', 'voucher_number': number,
                    'voucher_status': status}
        except (ValueError, KeyError, TypeError, AttributeError):
            raise ShopLexwareError('lexware_invoice_binding_mismatch') from None

    def inspect(self, remote_id, payload, *, voucher_number=None):
        remote_id = _uuid(remote_id)
        raw = self._request('GET', '/v1/invoices/' + remote_id)
        return self._verify(raw, remote_id, payload, voucher_number=voucher_number)

    def recover(self, payload):
        """Search the frozen voucher day, including drafts. Never choose among competing references."""
        day = payload['voucherDate'][:10]
        amount = _gross(payload)
        seen, matches = set(), []
        for page in range(10):
            query = urlencode({'voucherType': 'invoice', 'voucherStatus': 'any',
                               'voucherDateFrom': day, 'voucherDateTo': day,
                               'page': page, 'size': 100, 'sort': 'createdDate,ASC'})
            rows = self._request('GET', '/v1/voucherlist', query='?' + query)
            if (not isinstance(rows.get('content'), list) or type(rows.get('last')) is not bool
                    or type(rows.get('totalElements')) is not int or rows.get('number') != page):
                raise ShopLexwareError('lexware_invoice_search_incomplete')
            for row in rows['content']:
                if not isinstance(row, dict) or row.get('voucherType') != 'invoice':
                    raise ShopLexwareError('lexware_invoice_search_incomplete')
                candidate = _uuid(row.get('id'))
                if candidate in seen:
                    raise ShopLexwareError('lexware_invoice_search_incomplete')
                seen.add(candidate)
                if row.get('totalAmount') is not None and _amount(row['totalAmount']) != amount:
                    continue
                raw = self._request('GET', '/v1/invoices/' + candidate)
                if _printed_text(raw.get('introduction') or '') == payload['introduction']:
                    self._verify(raw, candidate, payload)
                    matches.append(candidate)
            if rows['last']:
                if len(seen) != rows['totalElements'] or len(matches) > 1:
                    raise ShopLexwareError('lexware_invoice_search_ambiguous')
                return matches[0] if matches else None
        raise ShopLexwareError('lexware_invoice_search_incomplete')

    def pdf(self, remote_id, payload, *, voucher_number=None):
        remote_id = _uuid(remote_id)
        if self.inspect(remote_id, payload, voucher_number=voucher_number)['state'] != 'final':
            raise ShopLexwareError('lexware_invoice_pdf_not_ready')
        # Request the original representation. A visual PDF preview of an XML
        # XRechnung must never be delivered as the accounting original.
        try:
            from werkstatt_buero_anbindung import read_official_lexware_file
            from werkstatt_rechnungsquelle import _throttle
            _throttle()
            raw = read_official_lexware_file(self.portal, '/v1/invoices/' + remote_id + '/file',
                                             accept='*/*', expected_type='application/pdf', max_bytes=20*1024*1024)
            if not raw.startswith(b'%PDF-') or b'%%EOF' not in raw[-1024:]:
                raise ValueError()
            return raw
        except Exception:
            raise ShopLexwareError('lexware_invoice_pdf_unavailable') from None
