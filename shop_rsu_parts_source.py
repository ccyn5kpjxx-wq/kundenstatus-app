"""Pure parsing of retained original RSU views and read-only inquiry receipts."""
from datetime import datetime
from decimal import Decimal
import re


class PartsSourceError(ValueError):
    pass


def require(value, code):
    if not value:
        raise PartsSourceError(code)


def dom_product(raw):
    require(isinstance(raw, bytes) and 0 < len(raw) <= 1024 * 1024, 'ui_source_size')
    text = raw.decode('utf-8')
    def field(label):
        values = re.findall(r'^\s*- row "' + re.escape(label) + r' ([^"\r\n]+)":\s*$', text, re.M)
        require(len(values) == 1, 'ui_identity_field_not_unique')
        return values[0]
    headings = re.findall(r'^- heading "[^"\r\n]+" \[level=1\]:', text, re.M)
    require(len(headings) == 1, 'ui_product_heading_not_unique')
    header = 'row "Lieferoptionen Bestand EK Stück Versand EK Gesamt VK1":'
    require(text.count(header) == 1, 'ui_offer_table_not_unique')
    before, table = text.split(header)
    quantities = re.findall(r'^- generic: "([1-9][0-9]*)"\s*$', before.split(headings[0], 1)[1], re.M)
    require(len(quantities) == 1, 'ui_selected_quantity_not_unique')
    quantity = int(quantities[0])
    require(quantity <= 48, 'ui_selected_quantity_invalid')
    table = table.split('- button ', 1)[0]
    rows = re.findall(r'^\s*- row "([^"\r\n]+)":\s*$', table, re.M)
    require(1 <= len(rows) <= 50, 'ui_offer_rows_missing')
    offers = []
    for row in rows:
        prices = list(re.finditer(r'(?<![0-9.,])([0-9]+(?:\.[0-9]{3})*,[0-9]{2})\s*€', row))
        unknown = len(prices) == 3 and row[prices[0].end():prices[1].start()].strip() == '-'
        require(len(prices) == 4 or unknown, 'ui_offer_price_fields_invalid')
        stock = re.search(r'\s([0-9]+)\s+$', row[:prices[0].start()])
        require(stock is not None, 'ui_offer_stock_not_explicit')
        values = [Decimal(m[1].replace('.', '').replace(',', '.')) for m in prices]
        net = values[0]
        freight = None if unknown else values[1]
        total = values[1] if unknown else values[2]
        require(net > 0 and total >= net * quantity, 'ui_offer_total_mismatch')
        if freight is not None:
            require(freight >= 0 and net * quantity + freight == total, 'ui_offer_total_mismatch')
        offers.append({'stock': int(stock[1]), 'unit_net': format(net, 'f'),
                       'freight_net': None if freight is None else format(freight, 'f')})
    return {'article_id': field('RSU Artikelnummer'), 'manufacturer': field('Marke'),
            'mpn': field('Hersteller Artikelnummer'), 'ean': field('EAN'),
            'quantity': quantity, 'offers': offers}


def retained_inquiries(value):
    require(isinstance(value, dict) and type(value.get('schema_version')) is int and value['schema_version'] == 1
            and value.get('supplier') == 'RSU' and value.get('mode') == 'live'
            and type(value.get('orders')) is int and value['orders'] == 0
            and type(value.get('payments')) is int and value['payments'] == 0
            and isinstance(value.get('records'), list) and type(value.get('requests')) is int
            and value['requests'] == len(value['records']) <= 20, 'invalid_retained_rest_envelope')
    result = {}
    for row in value['records']:
        require(isinstance(row, dict) and row.get('ok') is True and row.get('mode') == 'live'
                and row.get('operation') == 'REST_INQUIRY_GET' and type(row.get('quantity')) is int
                and row['quantity'] == 1 and row.get('ean') == row.get('expected_ean')
                and isinstance(row.get('supplier_article_id'), str)
                and re.fullmatch(r'[1-9][0-9]{0,11}', row['supplier_article_id'])
                and isinstance(row.get('ean'), str) and re.fullmatch(r'[0-9]{13}', row['ean'])
                and type(row.get('stock')) is int and row['stock'] >= 0
                and isinstance(row.get('unit_net'), str)
                and re.fullmatch(r'[0-9]{1,9}(?:\.[0-9]{1,4})?', row['unit_net'])
                and Decimal(row['unit_net']) > 0, 'retained_rest_identity_or_quantity_invalid')
        stamp = datetime.fromisoformat(row['retrieved_at'].replace('Z', '+00:00'))
        require(stamp.tzinfo is not None and stamp.utcoffset() is not None, 'retained_rest_timestamp_invalid')
        require(row['supplier_article_id'] not in result, 'duplicate_retained_rest_id')
        result[row['supplier_article_id']] = row
    return result
