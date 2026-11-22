"""Build a reviewed private RSU batch or atomically stage it for the price worker."""
import argparse
import base64
from contextlib import contextmanager
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_cart_quotes import retail_unit_gross_cent
from shop_price_book import HASH, WHEELS, _stamp
from shop_rsu_daily_import import FORMAT, MAX_PACKED, POINTER, batch_path, binding_digest, decode_batch


def required(valid, message):
    if not valid:
        raise ValueError(message)


def build(args):
    report = json.loads(args.report.read_bytes())
    coverage = args.coverage.read_bytes()
    with args.csv.open("rb") as stream:
        import hashlib
        csv_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    required(csv_hash == report["source_sha256"] and sha256(coverage).hexdigest() == report["jsonl_sha256"],
             "Supplier or reconciliation file hash does not match")
    receipt = json.loads(args.receipt.read_bytes())
    required(receipt["sha256"] == csv_hash and receipt["observed_at"] == report["observed_at"],
             "Original export receipt differs")
    source_time = _stamp(receipt["observed_at"])
    modified = parsedate_to_datetime(receipt["last_modified"]).isoformat() if receipt.get("last_modified") else None
    price_bytes = args.price_rows.read_bytes()
    price_records = json.loads(price_bytes)
    old_book = {r["product_id"]:r for r in price_records}
    required(len(old_book) == len(price_records) and old_book, "Invalid current merchant book")
    proof_bytes = args.q1.read_bytes()
    evidence = [json.loads(line) for line in proof_bytes.splitlines() if line.strip()]
    by_id = {v["product_id"]:v for v in evidence}
    required(len(by_id) == len(evidence), "Duplicate Q1 observation")
    rows = sorted((json.loads(line) for line in coverage.splitlines() if line.strip()), key=lambda r:r["product_id"])
    required(len(rows) == report["binding_count"] == len({r["product_id"] for r in rows})
             == len({r["supplier_article_id"] for r in rows}), "Incomplete or ambiguous binding coverage")
    ids, offers = [], []
    direct_count = held = price_move = 0
    source_ids = {r["product_id"] for r in rows}
    required(by_id.keys() <= source_ids, "Unbound Q1 observation")
    for r in rows:
        required(r["observed_at"] == report["observed_at"] and r["rsu_csv_sha256"] == csv_hash
                 and r["category"] in WHEELS and r["currency"] == "EUR" and r["vat_percent"] == "19.00"
                 and r["net_material_markup_percent"] == "15.00" and HASH.fullmatch(r["identity_hash"]),
                 "Invalid release identity, observation, or price policy")
        ids.append([r["product_id"],r["identity_hash"],r["category"],r["supplier_article_id"]])
        witness = by_id.get(r["product_id"])
        if witness:
            required(witness.get("supplier_article_id") == r["supplier_article_id"]
                     and witness.get("expected_ean") == r["expected_ean"] and type(witness.get("quantity")) is int
                     and witness.get("quantity") == 1 and witness.get("source") == "RSU Inquiry"
                     and witness.get("csv_sha256") == csv_hash and witness.get("status") in {"confirmed","unconfirmed"},
                     "Q1 observation not bound to the exact supplier article, EAN and daily source")
        if r["status"] != "exact":
            offers.append(None)
            continue
        net = Decimal(r["export_unit_net_eur"])
        required(r["observed_ean"] == r["expected_ean"] and r["identity_match_count"] == 1
                 and net.is_finite() and 0 < net*100 == (net*100).to_integral_value()
                 and retail_unit_gross_cent(net, r["category"]) == r["base_unit_gross_cent"],
                 "Unverified price, EAN or variant")
        previous = r.get("previous_export_unit_net_eur")
        old = old_book.get(r["product_id"])
        required(not old or old["identity_hash"] == r["identity_hash"], "Frozen pricebook variant mismatch")
        if old:
            required(type(old["unit_gross_cent"]) is int and old["unit_gross_cent"] > 0, "Invalid former merchant price")
        unusual = (previous is not None and 4*abs(net - Decimal(previous)) >= Decimal(previous)
                   or old is not None and 4*abs(r["base_unit_gross_cent"] - old["unit_gross_cent"]) >= old["unit_gross_cent"])
        price_move += int(unusual)
        if witness and witness["status"] == "confirmed":
            observed = _stamp(witness["observed_at"])
            required(source_time <= observed < source_time + timedelta(days=1)
                     and witness.get("configured_mode") == "live" and witness.get("ean") == r["expected_ean"],
                     "Conflicting Q1 response metadata")
            fresh = Decimal(witness["unit_net_eur"])
            required(fresh.is_finite() and 0 < fresh*100 == (fresh*100).to_integral_value()
                     and type(witness["stock"]) is int and 0 <= witness["stock"] <= 1_000_000,
                     "Invalid Q1 amount or stock")
            offers.append([int(fresh*100),witness["stock"],observed.isoformat()])
            direct_count += 1
        elif unusual or witness:
            offers.append(None)
            held += 1
        else:
            offers.append([int(net*100),r["stock"]])
    required(sum(v is not None for v in offers) + held == report["counts"]["exact"], "Incorrect matched offer count")
    batch = {"format":FORMAT, "bundle_sha256":report["bundle_sha256"],"rsu_csv_sha256":csv_hash,
             "q1_evidence_sha256":sha256(proof_bytes).hexdigest(), "pricebook_snapshot_sha256":sha256(price_bytes).hexdigest(),
             "source_last_modified":modified,"binding_sha256":binding_digest(ids), "observed_at":report["observed_at"], "offers":offers}
    packed = gzip.compress(json.dumps(batch, separators=(",",":")).encode("ascii"), mtime=0)
    decode_batch(packed)
    with args.output.open("xb") as stream:
        stream.write(packed)
    print(json.dumps({"sha256":sha256(packed).hexdigest(),"bytes":len(packed),"matched":len(offers)-offers.count(None),
                      "excluded":offers.count(None),"held_for_q1":held,"unusual_csv_vs_reference":price_move,
                      "confirmed_q1":direct_count,"source_sha256":csv_hash,"observed_at":report["observed_at"]}))


def atomic(path, data):
    fd, tmp = tempfile.mkstemp(prefix=".rsu-stage-", dir=path.parent)
    try:
        with os.fdopen(fd,"wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp,path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        Path(tmp).unlink(missing_ok=True)


@contextmanager
def exclusive_stage(root):
    path = root / ".rsu-daily-stage.lock"
    required(not path.is_symlink(), "Invalid staging lock")
    with open(path,"a+b") as stream:
        if os.name == "nt":
            import msvcrt
            if not stream.tell():
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(),msvcrt.LK_LOCK,1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(),fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
            else:
                fcntl.flock(stream.fileno(),fcntl.LOCK_UN)


def stage(args):
    root = args.release_root
    required(root.is_absolute() and root.is_dir() and not root.is_symlink(), "Invalid release directory")
    if args.stdin_b64:
        raw = sys.stdin.buffer.read(MAX_PACKED * 2 + 1)
        required(len(raw) <= MAX_PACKED*2, "Input too large")
        packed = base64.b64decode(b"".join(raw.split()), validate=True)
    else:
        required(args.file.is_file() and args.file.stat().st_size <= MAX_PACKED, "Invalid input file")
        packed = args.file.read_bytes()
    digest = sha256(packed).hexdigest()
    required(digest == args.sha256, "Transfer checksum mismatch")
    batch = decode_batch(packed)
    descriptor = json.loads((root / "current.json").read_bytes())
    required(descriptor.get("fixed_pricebook_enabled") is True and descriptor["bundle"]["sha256"] == batch["bundle_sha256"],
             "Release bundle mismatch")
    timestamp = _stamp(batch["observed_at"])
    now = datetime.now(timezone.utc)
    required(timestamp <= now < timestamp + timedelta(days=1), "Source date is outside the approved daily window")
    if not args.dry_run:
        with exclusive_stage(root):
            ptr = root / POINTER
            required(not ptr.is_symlink(), "Invalid staging pointer")
            if ptr.exists():
                previous_hash = json.loads(ptr.read_bytes())["sha256"]
                previous_path = batch_path(root, previous_hash)
                required(not previous_path.is_symlink() and previous_path.is_file() and previous_path.stat().st_size <= MAX_PACKED,
                         "Previous batch missing")
                previous_bytes = previous_path.read_bytes()
                required(sha256(previous_bytes).hexdigest() == previous_hash, "Previous batch checksum mismatch")
                previous = decode_batch(previous_bytes)
                required(previous_hash == digest or _stamp(previous["observed_at"]) < timestamp,
                         "Previous batch is newer or conflicting")
            asset = batch_path(root, digest)
            required(not asset.is_symlink(), "Invalid batch path")
            if not asset.exists() or sha256(asset.read_bytes()).hexdigest() != digest:
                atomic(asset, packed)
            atomic(ptr, json.dumps({"sha256":digest},separators=(",",":")).encode("ascii"))
    print(json.dumps({"staged":not args.dry_run,"sha256":digest,"observed_at":batch["observed_at"],
                      "matched":sum(x is not None for x in batch["offers"]),"unmatched":batch["offers"].count(None)}))


def verify(args):
    from shop_production import _private_file, _json
    from shop_rsu_quotes import _binding
    from shop_tyre_pricing import tyre_unit_gross_cent
    root, database = args.release_root, args.pricebook
    required(root.is_absolute() and not root.is_symlink() and database.is_absolute() and database.is_file()
             and not database.is_symlink(), "Invalid release or pricebook path")
    pointer = root / POINTER
    required(pointer.is_file() and not pointer.is_symlink() and pointer.stat().st_size <= 256
             and _json(pointer,256) == {"sha256":args.sha256}, "Expected batch is not staged")
    asset = batch_path(root,args.sha256)
    required(asset.is_file() and not asset.is_symlink() and asset.stat().st_size <= MAX_PACKED, "Batch missing")
    packed = asset.read_bytes()
    required(sha256(packed).hexdigest() == args.sha256,"Batch checksum mismatch")
    batch = decode_batch(packed)
    descriptor = _json(root / "current.json",65536)
    required(descriptor["bundle"]["sha256"] == batch["bundle_sha256"],"Bundle differs")
    catalogue = _json(_private_file(root,descriptor["bundle"]))
    ids = []
    for pid, value in sorted(catalogue["rsu_bindings"].items()):
        if value["category"] in WHEELS:
            bound = _binding(pid,value)
            ids.append([pid,bound["identity_hash"],bound["category"],bound["supplier_article_id"]])
    required(binding_digest(ids) == batch["binding_sha256"] and len(ids) == len(batch["offers"]),"Variant scope differs")
    requested = set(args.article)
    required(requested <= {i[0] for i in ids}, "Inspection requested for unknown ID")
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro",uri=True,timeout=5)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        required(db.execute("SELECT format,mode FROM shop_public_pricebook_meta").fetchall() == [("merchant-rsu-prices-v1","live")], "Wrong pricebook")
        db.row_factory = sqlite3.Row
        book = {row["product_id"]:dict(row) for row in db.execute("SELECT * FROM shop_public_prices_v1")}
    counts, examples, articles = Counter(), [], []
    fetched = _stamp(batch["observed_at"])
    failed = set()
    for (pid,identity,category,_), offer in zip(ids,batch["offers"]):
        row = book.get(pid)
        if row and row["identity_hash"] != identity:
            status="unexpected_identity"
        elif offer is None:
            status="excluded_with_older_price" if row else "excluded_without_price"
        elif row is None:
            status="unexpected_missing"
        else:
            direct = len(offer) == 3
            stamp = _stamp(offer[2]) if direct else _stamp(batch.get("source_last_modified") or batch["observed_at"])
            expected = retail_unit_gross_cent(Decimal(offer[0])/100, category)
            pub, stock_at = _stamp(row["published_at"]), _stamp(row["stock_observed_at"])
            right_price = row["unit_gross_cent"] == expected
            expected_source = batch["q1_evidence_sha256"] if direct else batch["rsu_csv_sha256"]
            if pub == stamp and row["evidence_id"] == expected_source:
                if not right_price or stock_at <= stamp and row["stock"] != offer[1]:
                    status="unexpected_applied_values"
                else:
                    status="applied_q1" if direct else "applied_csv"
            elif max(pub,stock_at) >= stamp:
                status="protected_newer_price_matches" if right_price else "protected_newer_price_differs"
            elif not direct and batch["source_last_modified"] is None and max(pub,stock_at) >= fetched-timedelta(days=1):
                status="protected_recent_price_matches" if right_price else "protected_recent_price_differs"
            elif not direct and 4*abs(expected-row["unit_gross_cent"]) >= row["unit_gross_cent"]:
                status="held_for_q1_price_swing"
            else:
                status="unapplied_matches_amount" if right_price else "unexpected_unprotected_difference"
        counts[status]+=1
        if status.startswith("unexpected") or status=="unapplied_matches_amount":
            failed.add(pid)
            if len(examples)<20:
                examples.append({"article":pid,"status":status})
        if pid in requested:
            public = None if not row else tyre_unit_gross_cent(row["unit_gross_cent"]) if category=="tyre" else row["unit_gross_cent"]
            articles.append({"article":pid,"status":status,"public_unit_gross_cent":public,
                             "stock_at_observation":None if not row else row["stock"]})
    print(json.dumps({"batch_sha256":args.sha256,"variants":len(ids),"counts":counts,
                      "unexplained":len(failed),"examples":examples,"articles":articles},sort_keys=True))
    if failed:
        raise SystemExit(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(required=True)
    prepare = commands.add_parser("build")
    for field in ("report","coverage","csv","receipt","price-rows","q1","output"):
        prepare.add_argument("--"+field,type=Path,required=True)
    prepare.set_defaults(action=build)
    upload = commands.add_parser("stage")
    upload.add_argument("--release-root",type=Path,required=True)
    upload.add_argument("--sha256",required=True)
    upload.add_argument("--dry-run",action="store_true")
    inputs = upload.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--stdin-b64",action="store_true")
    inputs.add_argument("--file",type=Path)
    upload.set_defaults(action=stage)
    inspect = commands.add_parser("verify")
    inspect.add_argument("--release-root",type=Path,required=True)
    inspect.add_argument("--pricebook",type=Path,required=True)
    inspect.add_argument("--sha256",required=True)
    inspect.add_argument("--article",action="append",default=[])
    inspect.set_defaults(action=verify)
    arguments = parser.parse_args()
    arguments.action(arguments)
