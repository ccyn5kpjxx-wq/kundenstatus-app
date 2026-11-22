"""Read approved ALZURA parts pages locally, then verify/stage their inquiry prices."""
import argparse
import base64
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_partner_native import alzura_readonly_catalog as api
from shop_partner_native.shop_parts_provider import normalize_wearparts
from shop_parts_daily_prices import FORMAT, MAX_BYTES, POINTER, RSU_POINTER, asset, projections, require, scope, validate
from shop_parts_inquiry import NAME
from shop_parts_release import _stamp, decode, read
from stage_rsu_daily_prices import atomic, exclusive_stage


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(raw):
    return sha256(raw).hexdigest()


def root_data(root, source='ALZURA'):
    descriptor = decode(read(root / 'current.json', 2 * 1024 * 1024))
    require(source in ('RSU', 'ALZURA'), 'invalid_inquiry_source')
    key = 'parts_inquiry_prices' if source == 'RSU' else 'parts_partner_inquiry_prices'
    record = descriptor[key]
    require(descriptor.get(key + '_approved') is True
            and NAME.fullmatch(record['path'])
            and record['base_bundle_sha256'] == descriptor['bundle']['sha256'], 'partner_release_not_approved')
    manifest, prices = scope(root, record)
    require(all(p['source'] == source for p in prices.values()), 'inquiry_record_source_mismatch')
    return record, manifest, prices


def group(request):
    # A supplier can move an identical item between pages of the same group.
    return encode({**request, 'params': {k: v for k, v in request['params'].items() if k != 'page'}})


def expected(root, manifest, prices):
    ref = manifest['snapshot']
    require(NAME.fullmatch(ref['file']), 'invalid_snapshot_name')
    raw = read(root / 'partner-price-sources' / ref['file'], 8 * 1024 * 1024)
    require(digest(raw) == ref['sha256'], 'partner_snapshot_changed')
    snapshot = decode(raw)['products']
    selected, plan = {}, {}
    for pid, values in prices.items():
        item = snapshot[values['snapshot_row_number'] - 1]
        native = item['provider_id']
        require(pid == 'parts:' + digest((item['category'] + ':' + native).encode())[:24]
                and native not in selected, 'partner_identity_mismatch')
        request = item['source_request']
        require(request['area'] == 'wearparts' and request['operation'] == 'articles', 'unexpected_endpoint')
        plan[json.dumps(request, sort_keys=True)] = request
        selected[native] = (pid, item)
    # Include pages with existing RSU-priority items only when they belong to
    # the exact same released source groups (page positions can shift).
    groups = {group(x) for x in plan.values()}
    for item in snapshot:
        request = item.get('source_request', {})
        if request.get('area') == 'wearparts' and request.get('operation') == 'articles' and group(request) in groups:
            plan[json.dumps(request, sort_keys=True)] = request
    return selected, api.validate_plan([plan[k] for k in sorted(plan)])


def fetch(args):
    _, manifest, prices = root_data(args.release_root)
    _, requests = expected(args.release_root, manifest, prices)
    require(not args.run_dir.exists(), 'new_private_run_directory_required')
    args.run_dir.mkdir(parents=True, mode=0o700)
    api._atomic_json(args.run_dir / 'plan.json', {'requests': requests})
    client = api.ReadOnlyCatalogClient(api.load_tokens(args.token_file), output_dir=args.run_dir,
        ledger=api.BudgetLedger(args.ledger, monthly_limit=9900, run_limit=len(requests)),
        authorize=lambda area, operation, params: {'area': area, 'operation': operation, 'params': params} in requests)
    for request in requests:
        result = client.read(request['area'], request['operation'], request['params'])
        require(result.summary['ok'] is True, 'partner_get_failed')
    print(json.dumps({'verified_gets': len(requests)}))


def build(args):
    record, manifest, prices = root_data(args.release_root)
    selected, requests = expected(args.release_root, manifest, prices)
    require(decode(read(args.run_dir / 'plan.json', 64 * 1024))['requests'] == requests, 'partner_plan_changed')
    found = defaultdict(list)
    sources = []
    for i, request in enumerate(requests):
        stem = f'{i + 1:02}-wearparts-articles'
        meta_raw = read(args.run_dir / (stem + '.meta.json'), 64 * 1024)
        meta = decode(meta_raw)
        require(meta.get('ok') is True and meta.get('area') == 'wearparts' and meta.get('operation') == 'articles'
                and meta.get('http_status') == 200 and meta.get('body_file') == stem + '.body'
                and _stamp(meta['requested_at']) <= _stamp(meta['received_at']), 'invalid_partner_receipt')
        raw = read(args.run_dir / (stem + '.body'), api.MAX_RESPONSE_BYTES)
        require(meta['body_sha256'] == digest(raw), 'partner_body_hash_mismatch')
        sources.append([digest(encode(request)), digest(meta_raw), digest(raw), meta['received_at']])
        for original in decode(raw)['result']:
            article = original.get('assignedArticle') or original.get('directArticle') or {}
            market = original.get('assignedTyre24Data') or {}
            native = f"wearparts:{article.get('articleId')}:{article.get('brandNo')}"
            if native not in selected:
                continue
            pid, old = selected[native]
            if group(old['source_request']) != group(request):
                continue
            if str(market.get('vat')) != '0.19' or market.get('depositPrice') not in (None, '', '0', '0.00', 0):
                found[pid].append(None)
                continue
            rows = normalize_wearparts({'result': [original]}, meta['received_at'], request, old['source_context'])
            fields = ('provider_id', 'category', 'name', 'brand', 'article_number', 'ean', 'unit_label',
                      'price_basis', 'price_type', 'currency', 'country')
            valid = len(rows) == 1 and all(rows[0].get(k) == old.get(k) for k in fields)
            if not valid or not rows[0].get('unit_net_eur') or type(rows[0].get('stock')) is not int:
                found[pid].append(None)
                continue
            net = Decimal(rows[0]['unit_net_eur'])
            if net != Decimal(str(market.get('wholesaler_price'))) or net <= 0 or net*100 != (net*100).to_integral_value():
                found[pid].append(None)
                continue
            found[pid].append([i, int(net * 100), rows[0]['stock']])
    values = {}
    for pid in prices:
        occurrences = found[pid]
        if not occurrences or None in occurrences or len({x[1] for x in occurrences}) != 1:
            values[pid] = None
        else:
            values[pid] = min(occurrences, key=lambda x: x[2])
    # A complete outage or a request error cannot withdraw the whole catalogue.
    require(any(values.values()), 'partner_batch_has_no_exact_prices')
    raw = encode({'format': FORMAT, 'bundle_sha256': record['base_bundle_sha256'],
                  'manifest_sha256': record['sha256'], 'sources': sources, 'rows': values})
    validate(raw, record, prices, now=datetime.now(timezone.utc), fresh=True)
    with args.output.open('xb') as stream:
        stream.write(raw)
    print(json.dumps({'sha256': digest(raw), 'bytes': len(raw), 'groups': len(requests),
                      'priced': sum(x is not None for x in values.values()), 'missing': list(values.values()).count(None)}))


def stage(args):
    root = args.release_root
    source = getattr(args, 'source', 'ALZURA')
    record, manifest, prices = root_data(root, source)
    proofs = manifest['identity_proofs'] if source == 'RSU' else None
    if args.stdin_b64:
        encoded = sys.stdin.buffer.read(2 * MAX_BYTES + 1)
        require(len(encoded) <= 2 * MAX_BYTES, 'partner_batch_too_large')
        raw = base64.b64decode(encoded.strip(), validate=True)
    else:
        raw = read(args.file.resolve(), MAX_BYTES)
    h = digest(raw)
    require(h == args.sha256, 'partner_transfer_hash_mismatch')
    now = datetime.now(timezone.utc)
    observed, stamps, rows = validate(raw, record, prices, now=now, fresh=True, rsu_proofs=proofs)
    projections(prices, stamps, rows)
    if not args.dry_run:
        with exclusive_stage(root):
            record, manifest, prices = root_data(root, source)
            proofs = manifest['identity_proofs'] if source == 'RSU' else None
            validate(raw, record, prices, now=now, fresh=True, rsu_proofs=proofs)
            pointer = root / (RSU_POINTER if source == 'RSU' else POINTER)
            require(not pointer.is_symlink(), 'unsafe_partner_pointer')
            if pointer.exists():
                previous = decode(read(pointer, 256))['sha256']
                prior = read(asset(root, previous, source), MAX_BYTES)
                require(digest(prior) == previous, 'invalid_previous_partner_batch')
                previous_time, _, _ = validate(prior, record, prices, now=now, rsu_proofs=proofs)
                require(previous == h or observed > previous_time, 'older_or_conflicting_partner_batch')
            target = asset(root, h, source)
            require(not target.is_symlink(), 'unsafe_partner_target')
            if not target.exists() or digest(read(target, MAX_BYTES)) != h:
                atomic(target, raw)
            atomic(pointer, encode({'sha256': h}))
    print(json.dumps({'staged': not args.dry_run, 'sha256': h, 'observed_at': observed.isoformat(),
                      'priced': sum(x is not None for x in rows.values()), 'missing': list(rows.values()).count(None)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name, action in [('fetch', fetch), ('build', build), ('stage', stage)]:
        cmd = commands.add_parser(name)
        cmd.set_defaults(action=action)
        cmd.add_argument('--release-root', type=Path, required=True)
        if name in ('fetch', 'build'):
            cmd.add_argument('--run-dir', type=Path, required=True)
        if name == 'fetch':
            cmd.add_argument('--token-file', type=Path, required=True)
            cmd.add_argument('--ledger', type=Path, required=True)
        if name == 'build':
            cmd.add_argument('--output', type=Path, required=True)
        if name == 'stage':
            cmd.add_argument('--sha256', required=True)
            cmd.add_argument('--source', choices=('ALZURA','RSU'), default='ALZURA')
            cmd.add_argument('--dry-run', action='store_true')
            source = cmd.add_mutually_exclusive_group(required=True)
            source.add_argument('--file', type=Path)
            source.add_argument('--stdin-b64', action='store_true')
    args = parser.parse_args()
    args.release_root = args.release_root.resolve()
    if hasattr(args, 'run_dir'):
        args.run_dir = args.run_dir.resolve()
    args.action(args)
