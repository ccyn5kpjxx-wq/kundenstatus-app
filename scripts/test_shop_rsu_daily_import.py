import gzip
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_price_book import PublishedPriceBook, PriceBookError
from shop_cart_quotes import retail_unit_gross_cent
from shop_rsu_daily_import import DailyPriceImport, FORMAT, POINTER, batch_path, binding_digest, decode_batch


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.now = datetime(2026,11,22,20,0,tzinfo=timezone.utc)
        self.stamp = self.now - timedelta(minutes=10)
        self.ids = ["tyre:" + c*24 for c in "abc"]
        self.products = {pid:{"id":pid,"category":"tyre","identity_hash":c*64} for c,pid in zip("abc",self.ids)}
        self.book = PublishedPriceBook(self.root/"book"/"prices.sqlite3", products=self.products,
                                       approved_ids=set(self.ids), clock=lambda:self.now).provision(create=True)
        self.book.seed({pid:{"source":"RSU","identity_hash":p["identity_hash"],"observed_at":(self.stamp-timedelta(hours=2)).isoformat(),
                             "stock_reported":1,"unit_gross_cent":7000} for pid,p in self.products.items()},
                       evidence_id="0"*64)
        self.importer = DailyPriceImport(self.book,self.root,"f"*64,self)

    def rsu_mapping(self, product):
        return {"confirmed":True,"supplier":"RSU","product_id":product["id"],
                "identity_hash":product["identity_hash"],"supplier_article_id":product["id"][-1]}

    def put(self, **changes):
        rows = [[p["id"],p["identity_hash"],"tyre",p["id"][-1]] for p in self.products.values()]
        data = dict(format=FORMAT,bundle_sha256="f"*64,binding_sha256=binding_digest(rows),rsu_csv_sha256="1"*64,
                    observed_at=self.stamp.isoformat(),source_last_modified=self.stamp.isoformat(),
                    q1_evidence_sha256="2"*64,pricebook_snapshot_sha256="3"*64,offers=[[4844,20],[4800,0],None])
        data.update(changes)
        if "observed_at" in changes and "source_last_modified" not in changes:
            data["source_last_modified"] = changes["observed_at"]
        packed = gzip.compress(json.dumps(data).encode(),mtime=0)
        digest = sha256(packed).hexdigest()
        batch_path(self.root,digest).write_bytes(packed)
        (self.root/POINTER).write_text(json.dumps({"sha256":digest}))
        return digest

    def test_supplement_preserves_complete_original_batch_and_supports_next_full_batch(self):
        extra = self.ids[-1]
        importer = DailyPriceImport(self.book, self.root, "f"*64, self, supplementary_ids={extra})
        original_scope = [[pid, self.products[pid]["identity_hash"], "tyre", pid[-1]] for pid in self.ids[:-1]]
        self.put(binding_sha256=binding_digest(original_scope), offers=[[4844, 20], [4800, 0]])
        self.assertEqual(importer()["applied"], 2)
        self.assertEqual(self.book.get(extra)["stock_reported"], 1)
        self.put(binding_sha256=binding_digest(original_scope[:1]), offers=[[4844, 20]])
        with self.assertRaises(PriceBookError):
            importer()
        self.put(offers=[[4844, 20], [4800, 0], [4800, 9]])
        self.assertEqual(importer()["applied"], 1)
        self.assertEqual(self.book.get(extra)["stock_reported"], 9)

    def test_batch_up_and_down_unmatched_and_restart_idempotence(self):
        self.book._requested_reviews.add(self.ids[0])
        self.put()
        initial = self.importer()
        self.assertEqual((initial["applied"],initial["unmatched"]),(2,1))
        self.assertEqual(self.book.get(self.ids[0])["prices_by_quantity"]["1"]["unit_gross_cent"],retail_unit_gross_cent("48.44","tyre"))
        self.assertEqual(self.book.get(self.ids[0])["stock_reported"],20)
        self.assertEqual(self.book.get(self.ids[1])["stock_reported"],0)
        self.assertEqual(self.book.get(self.ids[2])["stock_reported"],1)
        self.assertNotIn(self.ids[0],self.book._requested_reviews)
        with self.book._connect() as db:
            self.assertEqual(db.execute("select evidence_id from shop_public_prices_v1 where product_id=?",(self.ids[0],)).fetchone()[0],"1"*64)
        self.assertEqual(self.importer(),initial)
        restarted=DailyPriceImport(self.book,self.root,"f"*64,self)
        self.assertEqual(restarted()["newer_or_same"],2)
        self.assertEqual(self.book.get(self.ids[0])["observed_at"],self.stamp.isoformat())

    def test_conflicts_source_age_and_unmatched(self):
        self.put(binding_sha256="0"*64)
        with self.assertRaises(PriceBookError):
            self.importer()
        self.put(observed_at=(self.now-timedelta(days=2)).isoformat(),source_last_modified=None)
        self.assertTrue(self.importer()["expired"])
        self.put(offers=[[4844,20],[4900,0],None],observed_at=(self.now+timedelta(seconds=2)).isoformat())
        with self.assertRaises(PriceBookError):
            self.importer()
        self.assertEqual(self.book.get(self.ids[0])["stock_reported"],1)

    def test_newer_api_observation_and_rollback(self):
        with self.book._connect() as db:
            db.execute("update shop_public_prices_v1 set stock_observed_at=?,stock=5 where product_id=?",
                       ((self.stamp+timedelta(minutes=1)).isoformat(),self.ids[0]))
        self.put()
        self.assertEqual(self.importer()["newer_or_same"],1)
        self.assertEqual(self.book.get(self.ids[0])["stock_reported"],5)
        prior=dict(self.book._entries)
        self.put(observed_at=(self.stamp+timedelta(minutes=2)).isoformat())
        insert=self.book._insert
        def fail_second(conn,row,evidence):
            if row["product_id"]==self.ids[1]:
                raise sqlite3.OperationalError("simulated failure")
            insert(conn,row,evidence)
        with patch.object(self.book,"_insert",side_effect=fail_second), self.assertRaises(sqlite3.OperationalError):
            self.importer()
        self.assertEqual(self.book._entries,prior)
        with self.book._connect() as db:
            self.assertEqual({r["product_id"]:dict(r) for r in db.execute("select * from shop_public_prices_v1")},prior)
        self.assertEqual(self.importer()["applied"],2)

    def test_undated_export_keeps_recent_api_observation_and_q1_replaces_extreme_csv(self):
        later=(self.stamp+timedelta(minutes=1)).isoformat()
        self.put(source_last_modified=None,offers=[[71383,20],[7608,17,later],None])
        outcome=self.importer()
        self.assertEqual(outcome["recent_observation_preserved"],1)
        self.assertEqual(outcome["q1_applied"],1)
        self.assertEqual(self.book.get(self.ids[0])["stock_reported"],1)
        self.assertEqual(self.book.get(self.ids[1])["prices_by_quantity"]["1"]["unit_gross_cent"],retail_unit_gross_cent("76.08","tyre"))
        self.assertEqual(self.book.get(self.ids[1])["stock_observed_at"],later)
        self.assertEqual(self.book._entries[self.ids[1]]["evidence_id"],"2"*64)
        self.put(offers=[[71383,20],None,None])
        self.assertEqual(self.importer()["price_change_needs_q1"],1)
        self.assertIn(self.ids[0], self.book._requested_reviews)

    def test_builder_holds_spike_without_independent_q1_and_uses_exact_confirmed_q1(self):
        from scripts.stage_rsu_daily_prices import build
        args = SimpleNamespace(**{k:self.root/k for k in ("report","coverage","csv","receipt","price_rows","q1","output")})
        args.csv.write_bytes(b"synthetic-original-csv")
        src=sha256(args.csv.read_bytes()).hexdigest()
        stamp=self.stamp.isoformat()
        ean="4000000000012"
        r={"product_id":self.ids[0], "category":"tyre","supplier_article_id":"4711","identity_hash":"a"*64,
           "currency":"EUR","vat_percent":"19.00","net_material_markup_percent":"15.00","observed_at":stamp,
           "rsu_csv_sha256":src,"expected_ean":ean,"observed_ean":ean,"identity_match_count":1,
           "export_unit_net_eur":"713.83","previous_export_unit_net_eur":"75.00","stock":5,
           "base_unit_gross_cent":retail_unit_gross_cent("713.83","tyre"),"status":"exact"}
        args.coverage.write_text(json.dumps(r),encoding="ascii")
        args.receipt.write_text(json.dumps({"sha256":src,"observed_at":stamp,"last_modified":""}),encoding="ascii")
        args.report.write_text(json.dumps({"observed_at":stamp,"source_sha256":src,"binding_count":1,"bundle_sha256":"f"*64,
                   "counts":{"exact":1},"jsonl_sha256":sha256(args.coverage.read_bytes()).hexdigest()}))
        args.price_rows.write_text(json.dumps([{"product_id":self.ids[0],"identity_hash":"a"*64,
                                                 "unit_gross_cent":retail_unit_gross_cent("76.00","tyre")}]))
        args.q1.write_bytes(b"")
        # An all-held release cannot be staged at all.
        with self.assertRaises(PriceBookError):
            build(args)
        proof={"product_id":self.ids[0],"supplier_article_id":"4711","expected_ean":ean,"ean":ean,"quantity":1,
               "unit_net_eur":"76.08","stock":3,"observed_at":self.now.isoformat(),"source":"RSU Inquiry",
               "configured_mode":"live","csv_sha256":src,"status":"confirmed"}
        args.q1.write_text(json.dumps({**proof,"supplier_article_id":"wrong"}))
        with self.assertRaisesRegex(ValueError,"not bound"):
            build(args)
        args.q1.write_text(json.dumps(proof))
        build(args)
        verified=decode_batch(args.output.read_bytes())
        self.assertEqual(verified["offers"],[[7608,3,self.now.isoformat()]])
        self.assertIsNone(verified["source_last_modified"])


if __name__ == "__main__":
    unittest.main()
