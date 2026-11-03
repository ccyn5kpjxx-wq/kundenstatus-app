"""One private GET -> two real caches. Offline HTTP, temp directories only."""
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scripts import refresh_tyresystem_catalog as refresh
from tyresystem_export import TyreSystemExportClient
from tyresystem_provider import TyreSystemCatalogCache
from felgen_katalog import FelgenCatalogCache
from test_tyresystem_export import CONFIG, HTTP, Response, TOKEN, csv_data
from test_felgen_katalog import raw_feed


def dual_feed(*rims):
    return csv_data() + b"\n".join(raw_feed(*(rims or ({},))).splitlines()[1:]) + b"\n"


class RefreshTests(unittest.TestCase):
    def setUp(self):
        blocker = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden"))
        blocker.start()
        self.addCleanup(blocker.stop)
        temporary = tempfile.TemporaryDirectory(prefix="dual-catalogue-refresh-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.cache = TyreSystemCatalogCache(self.root/"tires", private_root=self.root, clock=lambda: self.now)
        self.rims = FelgenCatalogCache(self.root/"rims", private_root=self.root, clock=lambda: self.now)

    def client(self, raw=None, *, received=None, response=None, authorize=lambda: True):
        self.response = response or Response(raw=dual_feed() if raw is None else raw)
        self.http = HTTP(self.response)
        return TyreSystemExportClient(CONFIG, authorize=authorize, http=self.http,
                                      clock=lambda: received or self.now)

    def run_refresh(self, client=None, **options):
        return refresh.refresh_catalogs(self.cache,self.rims,client or self.client(),clock=lambda: self.now,**options)

    def seed(self):
        self.assertEqual(self.run_refresh()[1],0)
        return self.cache.path.read_bytes(), self.rims.path.read_bytes()

    def assert_preserved(self, before):
        self.assertEqual((self.cache.path.read_bytes(),self.rims.path.read_bytes()),before)

    def test_one_bounded_get_has_identical_original_receipt_source_and_fifteen_minute_ttl(self):
        raw=dual_feed()
        result,code=self.run_refresh(self.client(raw))
        self.assertEqual(code,0)
        self.assertEqual(len(self.http.calls),1)
        options=self.http.calls[0][1]
        self.assertEqual(options["timeout"],(3.05,10.0))
        self.assertIs(options["allow_redirects"],False)
        self.assertIs(options["verify"],True)
        self.assertTrue(self.response.closed)
        tire,rim=self.cache.load().snapshot,self.rims.load()
        self.assertEqual(tire.abgerufen_am,self.now)
        self.assertEqual(rim.abgerufen_am,self.now)
        self.assertEqual(tire.sha256,rim.sha256)
        self.assertEqual(tire.sha256,hashlib.sha256(raw).hexdigest())
        self.assertEqual((len(tire.artikel),len(rim.artikel)),(1,1))
        self.assertEqual(result["valid_until"],(self.now+timedelta(minutes=15)).isoformat())
        self.assertFalse(result["public_release_changed"])
        encoded=json.dumps(result)
        for private in (TOKEN,"login=","https://",str(self.root),"netto","einkaufspreis"):
            self.assertNotIn(private,encoded)
        self.now += timedelta(minutes=14)
        self.assertEqual(self.cache.load().snapshot.abgerufen_am,tire.abgerufen_am)
        self.assertEqual(self.rims.load().abgerufen_am,rim.abgerufen_am)

    def test_authentication_redirect_size_and_transport_failure_preserve_both_caches(self):
        before=self.seed()
        cases=(Response(status=401),Response(status=302),
               Response(headers={"Content-Length":str(64*1024*1024+1)}),RuntimeError(TOKEN))
        for response in cases:
            with self.subTest(kind=type(response).__name__):
                client=self.client(response=response)
                result,code=self.run_refresh(client)
                self.assertEqual(code,1)
                self.assertFalse(result["partially_updated"])
                self.assertNotIn(TOKEN,json.dumps(result))
                self.assert_preserved(before)
                self.assertEqual(len(self.http.calls),1)
        client=self.client(authorize=lambda:False)
        self.assertEqual(self.run_refresh(client)[1],1)
        self.assertFalse(self.http.calls)

    def test_structural_error_or_empty_category_validates_both_before_writing_either(self):
        before=self.seed()
        for raw in (b"not;the;header",b"\xff",csv_data(),raw_feed()):
            result,code=self.run_refresh(self.client(raw))
            self.assertEqual(code,1)
            self.assertFalse(result["partially_updated"])
            self.assert_preserved(before)

    def test_identical_rows_deduplicate_and_cross_category_conflicts_quarantine_both(self):
        raw=dual_feed()
        repeated=raw+b"\n".join(raw.splitlines()[1:])+b"\n"
        self.assertEqual(self.run_refresh(self.client(repeated))[1],0)
        self.assertEqual(len(self.cache.load().snapshot.artikel),1)
        self.assertEqual(len(self.rims.load().artikel),1)
        # Conflicting SKU 123 is neither a tire nor a rim; unaffected 124/1 remain.
        other_tire=csv_data().splitlines()[1].replace(b"123;",b"124;",1)
        conflict=raw_feed({"Artikelnummer":"123"}).splitlines()[1]
        raw=dual_feed()+other_tire+b"\n"+conflict+b"\n"
        self.assertEqual(self.run_refresh(self.client(raw))[1],0)
        self.assertEqual([a.artikel_id for a in self.cache.load().snapshot.artikel],["124"])
        self.assertEqual([a.artikel_id for a in self.rims.load().artikel],["1"])

    def test_stale_future_or_naive_receipt_cannot_be_relabelled_fresh(self):
        before=self.seed()
        for received in (self.now-timedelta(minutes=15),self.now+timedelta(seconds=31),self.now.replace(tzinfo=None)):
            result,code=self.run_refresh(self.client(received=received))
            self.assertEqual(code,1)
            self.assertFalse(result["partially_updated"])
            self.assert_preserved(before)

    def test_slow_receipt_cannot_overwrite_a_newer_cache_in_either_category(self):
        before=self.seed()
        result,code=self.run_refresh(self.client(received=self.now-timedelta(seconds=60)))
        self.assertEqual(code,1)
        self.assertFalse(result["partially_updated"])
        self.assert_preserved(before)

    def test_second_file_failure_is_explicit_partial_without_unsafe_rollback(self):
        before=self.seed()
        self.now+=timedelta(seconds=1)
        with patch.object(self.rims,"replace",side_effect=OSError(TOKEN)):
            result,code=self.run_refresh()
        self.assertEqual(code,1)
        self.assertTrue(result["partially_updated"])
        self.assertTrue(result["cache_must_be_checked"])
        self.assertEqual(self.cache.load().snapshot.abgerufen_am,self.now)
        self.assertEqual(self.rims.path.read_bytes(),before[1])
        self.assertNotIn(TOKEN,json.dumps(result))
        # Retry uses one new bounded GET and repairs the pair with its real receipt.
        self.now+=timedelta(seconds=1)
        self.assertEqual(self.run_refresh()[1],0)
        self.assertEqual(self.cache.load().snapshot.abgerufen_am,self.rims.load().abgerufen_am)

    def test_first_file_failure_preserves_both_and_releases_own_locks(self):
        before=self.seed()
        with patch.object(self.cache,"replace",side_effect=OSError("private")):
            result,code=self.run_refresh()
        self.assertEqual(code,1)
        self.assertFalse(result["partially_updated"])
        self.assert_preserved(before)
        self.assertEqual(self.run_refresh()[1],0)

    def test_overlapping_lock_blocks_before_http_and_inactive_file_is_reusable(self):
        for target in (self.cache,self.rims):
            with refresh._refresh_locks((target,)):
                client=self.client()
                self.assertEqual(self.run_refresh(client)[1],1)
                self.assertFalse(self.http.calls)
            self.assertTrue((target.directory/".tyresystem-refresh.lock").exists())
            self.assertEqual(self.run_refresh()[1],0)

    def test_process_termination_releases_os_lock_without_deleting_the_file(self):
        ready=self.root/"lock-ready"
        code=("import sys,time; from pathlib import Path\n"
              f"sys.path.insert(0,{str(Path(__file__).resolve().parents[1])!r})\n"
              "from scripts.refresh_tyresystem_catalog import _refresh_locks\n"
              "from tyresystem_provider import TyreSystemCatalogCache\n"
              f"cache=TyreSystemCatalogCache({str(self.cache.directory)!r},private_root={str(self.root)!r})\n"
              "with _refresh_locks((cache,)):\n"
              f" Path({str(ready)!r}).write_bytes(b'ready')\n"
              " time.sleep(30)\n")
        process=subprocess.Popen([sys.executable,"-B","-c",code],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+5
            while not ready.exists() and process.poll() is None and time.monotonic()<deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists(),"Offline lock subprocess failed to start")
            client=self.client()
            self.assertEqual(self.run_refresh(client)[1],1)
            self.assertFalse(self.http.calls)
            process.terminate()
            process.communicate(timeout=5)
            self.assertTrue((self.cache.directory/".tyresystem-refresh.lock").exists())
            self.assertEqual(self.run_refresh()[1],0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)

    def test_readers_observe_complete_old_file_until_atomic_replace_for_each_category(self):
        before=self.seed()
        self.now+=timedelta(seconds=1)
        seen=[]
        real_replace=__import__("os").replace
        def inspect(source,destination):
            destination=Path(destination)
            index=0 if destination==self.cache.path else 1
            self.assertEqual(destination.read_bytes(),before[index])
            json.loads(Path(source).read_text(encoding="utf-8"))
            seen.append(destination.name)
            return real_replace(source,destination)
        with patch("os.replace",side_effect=inspect):
            self.assertEqual(self.run_refresh()[1],0)
        self.assertEqual(seen,["catalog.json","felgen-catalog.json"])

    def test_deadline_before_publication_changes_neither_cache(self):
        before=self.seed()
        for times in ((0,91),(0,0,91),(0,0,0,91),(0,0,0,0,91)):
            result,code=self.run_refresh(monotonic=iter(times).__next__)
            self.assertEqual(code,1)
            self.assertFalse(result["partially_updated"])
            self.assertFalse(result["cache_may_have_changed"])
            self.assert_preserved(before)

    def test_validated_export_repairs_corrupted_cache_without_renewing_old_receipts_on_read(self):
        self.seed()
        self.rims.path.write_bytes(b"damaged")
        self.now+=timedelta(seconds=1)
        self.assertEqual(self.run_refresh()[1],0)
        self.assertEqual(self.rims.load().abgerufen_am,self.now)

    def test_cli_requires_explicit_run_and_valid_paths_before_reading_credentials(self):
        cases=(["--cache-dir",str(self.cache.directory),"--private-root",str(self.root)],
               ["--run","--cache-dir","relative","--private-root",str(self.root)],
               ["--run","--cache-dir",str(self.root/"static"/"cache"),"--private-root",str(self.root)],
               ["--run","--cache-dir",str(self.cache.directory),"--private-root",str(self.root),"--felgen-private-root",str(self.root)])
        for arguments in cases:
            with patch("tyresystem_export.TyreSystemExportClient.from_env") as factory,redirect_stdout(io.StringIO()) as output:
                self.assertIn(refresh.main(arguments),(1,2))
            factory.assert_not_called()
            self.assertNotIn(TOKEN,output.getvalue())
        self.assertFalse(self.cache.directory.exists())

    def test_dual_cli_and_legacy_cli_fetch_exactly_once_without_loading_app_or_dotenv(self):
        client=self.client()
        arguments=["--run","--cache-dir",str(self.cache.directory),"--private-root",str(self.root),
                   "--felgen-cache-dir",str(self.rims.directory)]
        with patch("tyresystem_export.TyreSystemExportClient.from_env",return_value=client) as factory,redirect_stdout(io.StringIO()) as output:
            self.assertEqual(refresh.main(arguments),0)
        self.assertTrue(factory.call_args.kwargs["authorize"]())
        self.assertEqual(json.loads(output.getvalue())["rim_articles"],1)
        self.assertEqual(len(self.http.calls),1)
        self.assertNotIn("app",sys.modules)
        self.assertNotIn("dotenv",sys.modules)
        client=self.client()
        with patch("tyresystem_export.TyreSystemExportClient.from_env",return_value=client),redirect_stdout(io.StringIO()):
            self.assertEqual(refresh.main(arguments[:5]),0)
        self.assertEqual(len(self.http.calls),1)

    def test_cli_sigterm_unwinds_cache_writer_and_restores_previous_signal_handler(self):
        before=self.seed()
        old_handler=signal.getsignal(signal.SIGTERM)
        client=self.client()
        arguments=["--run","--cache-dir",str(self.cache.directory),"--private-root",str(self.root),
                   "--felgen-cache-dir",str(self.rims.directory)]
        def interrupt(*args):
            installed=signal.getsignal(signal.SIGTERM)
            self.assertIs(installed,refresh._terminate)
            installed(signal.SIGTERM,None)
        with patch("tyresystem_export.TyreSystemExportClient.from_env",return_value=client), \
                patch("os.replace",side_effect=interrupt),redirect_stdout(io.StringIO()) as output:
            self.assertEqual(refresh.main(arguments),1)
        self.assertIs(signal.getsignal(signal.SIGTERM),old_handler)
        self.assertFalse(json.loads(output.getvalue())["updated"])
        self.assert_preserved(before)
        self.assertFalse((self.cache.directory/".catalog-write.lock").exists())
        self.assertFalse((self.rims.directory/".felgen-write.lock").exists())
        self.assertEqual(self.run_refresh()[1],0)

    def test_sigterm_after_actual_first_rename_reports_possible_change_not_untouched(self):
        before=self.seed()
        self.now+=timedelta(seconds=1)
        client=self.client()
        real_replace=__import__("os").replace
        def commit_then_interrupt(source,destination):
            real_replace(source,destination)
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM,None)
        arguments=["--run","--cache-dir",str(self.cache.directory),"--private-root",str(self.root),
                   "--felgen-cache-dir",str(self.rims.directory)]
        with patch("tyresystem_export.TyreSystemExportClient.from_env",return_value=client), \
                patch("os.replace",side_effect=commit_then_interrupt),redirect_stdout(io.StringIO()) as output:
            self.assertEqual(refresh.main(arguments),1)
        result=json.loads(output.getvalue())
        self.assertFalse(result["updated"])
        self.assertTrue(result["cache_may_have_changed"])
        self.assertTrue(result["cache_must_be_checked"])
        self.assertEqual(self.cache.load().snapshot.abgerufen_am,self.now)
        self.assertEqual(self.rims.path.read_bytes(),before[1])
        self.assertFalse((self.cache.directory/".catalog-write.lock").exists())


if __name__=="__main__":
    unittest.main(verbosity=2)
