"""Collect exact Q1 GET receipts for previously released RSU inquiry-only parts."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_parts_inquiry import NAME, load_parts_inquiry
from shop_parts_release import decode, read
from shop_parts_daily_prices import require, scope
from shop_rsu_parts_evidence import validate_parts_mapping
from shop_rsu_quotes import XMLInquiryTransport
from tyresystem_api import TyreSystemAPIError, TyreSystemInquiryClient


def fetch(root, config, destination):
    descriptor = decode(read(root / 'current.json', 2 * 1024 * 1024))
    require(descriptor.get('parts_inquiry_prices_approved') is True, 'rsu_inquiry_not_approved')
    record = descriptor['parts_inquiry_prices']
    name = descriptor['bundle']['file']
    require(NAME.fullmatch(name) and NAME.fullmatch(record['path']), 'invalid_release_name')
    source = read(root / name, 200 * 1024 * 1024)
    base_sha = sha256(source).hexdigest()
    require(base_sha == record['base_bundle_sha256'] == descriptor['bundle']['sha256'], 'rsu_bundle_pin_mismatch')
    manifest, prices = scope(root, record)
    bundle = decode(source)
    proven = load_parts_inquiry(root, record, bundle, base_bundle_sha256=base_sha, allowed_ids=set(prices))
    require(proven.ids == prices.keys(), 'rsu_scope_mismatch')
    bindings = {}
    for pid, witness in manifest['identity_proofs'].items():
        checked = validate_parts_mapping(witness['mapping'], product_id=pid, category=witness['category'],
                                         expected_ean=witness['expected_ean'], identity_hash=witness['identity_hash'])
        bindings[pid] = (checked['supplier_article_id'], checked['expected_ean'])
    allowed = {article for article, _ in bindings.values()}
    require(len(allowed) == len(bindings) <= 30, 'rsu_scope_over_budget')
    client = TyreSystemInquiryClient(decode(read(config, 64 * 1024)), http=XMLInquiryTransport(),
                                     authorize=lambda article, count: article in allowed and count == 1)
    require(client.configured_mode == 'live', 'inquiry_account_mode_mismatch')
    require(not destination.exists(), 'fresh_rsu_receipt_path_required')
    results = {}
    for index, (pid, (article, ean)) in enumerate(sorted(bindings.items())):
        if index:
            time.sleep(3.1)
        try:
            response = client.inquire(article, 1)
        except TyreSystemAPIError as exc:
            if exc.code not in ('not_found', 'not_available'):
                raise
            results[pid] = {'status': 'unavailable', 'article_id': article, 'ean': ean, 'quantity': 1,
                            'observed_at': datetime.now(timezone.utc).isoformat()}
            continue
        require(response.configured_mode == 'live' and response.article_id == article and response.amount == 1
                and response.ean == ean and type(response.stock) is int, 'rsu_receipt_identity_mismatch')
        amount = Decimal(response.price) * 100
        require(amount > 0 and amount == amount.to_integral_value(), 'rsu_receipt_price_invalid')
        results[pid] = {'status': 'confirmed', 'article_id': article, 'ean': ean, 'quantity': 1, 'stock': response.stock,
                        'unit_net_cent': int(amount), 'observed_at': response.retrieved_at.isoformat()}
    data = {'format': 'rsu-inquiry-display-q1-daily-v1', 'manifest_sha256': record['sha256'],
            'bundle_sha256': base_sha, 'receipts': results}
    raw = json.dumps(data, sort_keys=True, separators=(',', ':')).encode()
    with destination.open('xb') as stream:
        stream.write(raw)
    print(json.dumps({'GET': len(results), 'confirmed': sum(r['status'] == 'confirmed' for r in results.values()),
                      'sha256': sha256(raw).hexdigest(), 'bytes': len(raw)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('release-root', 'config', 'output'):
        parser.add_argument('--' + key, required=True, type=Path)
    args = parser.parse_args()
    fetch(args.release_root.resolve(), args.config.resolve(), args.output.resolve())
