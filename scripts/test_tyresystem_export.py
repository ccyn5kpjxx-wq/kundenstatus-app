"""Offline transport/secret-boundary checks for the read-only CSV client."""
from datetime import datetime, timezone
from decimal import Decimal
import csv
import io
from pathlib import Path
import socket
import sys
import traceback
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tyresystem_export as export
from tyresystem_feed import HEADERS


TOKEN = "offline-only-token-never-a-real-credential"
URL = "https://www.tyresystem.de/Services/TyresystemExport?index&login=" + TOKEN
CONFIG = {"TYRESYSTEM_EXPORT_URL": URL}
NOW = datetime(2026, 11, 2, 13, 0, tzinfo=timezone.utc)


def csv_data():
    row = dict.fromkeys(HEADERS, "")
    row.update(Artikelnummer="123", ean="0012345678905", Lagerbestand="20",
        Einkaufspreis="68,34", Hersteller="Example", Profil="Winter",
        Reifenbreite="225", Reifenquerschnitt="40", Reifendurchmesser="18",
        Loadindex="92", Speedindex="V", Saison="w", Fahrzeugtyp="PKW",
        Artikeltyp="Reifen", Artikeluntertyp="PKW", Bauart="R")
    out = io.StringIO(newline="")
    writer = csv.writer(out, delimiter=";", lineterminator="\r\n")
    writer.writerow(HEADERS)
    writer.writerow([row[key] for key in HEADERS])
    return out.getvalue().encode()


class Response:
    def __init__(self, *, raw=None, status=200, headers=None, url=URL, chunks=None):
        self.raw = csv_data() if raw is None else raw
        self.status_code = status
        self.headers = headers or {}
        self.url = url
        self.chunks = chunks
        self.closed = False
        self.iterated = False

    def iter_content(self, chunk_size):
        self.iterated = True
        if self.chunks is not None:
            yield from self.chunks
        else:
            for index in range(0, len(self.raw), chunk_size):
                yield self.raw[index:index + chunk_size]

    def close(self):
        self.closed = True


class HTTP:
    def __init__(self, response=None):
        self.response = response if response is not None else Response()
        self.calls = []

    def get(self, url, **options):
        self.calls.append((url, options))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def client(self, response=None, *, config=CONFIG, authorize=lambda: True, **options):
        self.http = HTTP(response)
        return export.TyreSystemExportClient(config, authorize=authorize,
            http=self.http, clock=lambda: NOW, **options)

    def rejected(self, response, code):
        with self.assertRaises(export.TyreSystemExportError) as failure:
            self.client(response).fetch()
        self.assertEqual(failure.exception.code, code)
        if isinstance(response, Response):
            self.assertTrue(response.closed)
        return failure.exception

    def test_success_is_explicit_get_and_internal_parser_result(self):
        response = Response()
        result = self.client(response).fetch()
        self.assertEqual(len(self.http.calls), 1)
        url, options = self.http.calls[0]
        self.assertEqual(url, URL)
        self.assertEqual(options["timeout"], export.TIMEOUT)
        self.assertIs(options["verify"], True)
        self.assertIs(options["allow_redirects"], False)
        self.assertIs(options["stream"], True)
        self.assertEqual(options["headers"]["Accept-Encoding"], "identity")
        self.assertNotIn("auth", options)
        self.assertNotIn("cookies", options)
        self.assertFalse(result.public_ready)
        self.assertIsNone(result.artikel[0].fracht_netto)
        self.assertEqual(result.artikel[0].einkaufspreis, Decimal("68.34"))
        self.assertEqual(result.abgerufen_am, NOW)
        self.assertTrue(response.closed)

    def test_construct_from_environment_is_inert_and_repr_redacts_url(self):
        with patch("tyresystem_export.requests.Session") as session:
            client = export.TyreSystemExportClient.from_env(environment=CONFIG)
        session.assert_not_called()
        self.assertNotIn(TOKEN, repr(client))
        self.assertNotIn("login=", str(client))
        self.assertIn("redacted", repr(client))

    def test_authorization_requires_exact_true_on_each_call(self):
        for authorize in (None, lambda: False, lambda: 1, lambda: "yes"):
            with self.subTest(authorize=authorize):
                client = self.client(authorize=authorize)
                with self.assertRaises(export.TyreSystemExportError) as failure:
                    client.fetch()
                self.assertEqual(failure.exception.code, "not_authorized")
                self.assertEqual(self.http.calls, [])
        permits = iter((True, False))
        client = self.client(authorize=lambda: next(permits))
        client.fetch()
        with self.assertRaises(export.TyreSystemExportError):
            client.fetch()
        self.assertEqual(len(self.http.calls), 1)

    def test_authorization_exception_is_redacted_and_never_transmitted(self):
        def deny():
            raise RuntimeError(URL)
        client = self.client(authorize=deny)
        try:
            client.fetch()
        except export.TyreSystemExportError as error:
            rendered = "".join(traceback.format_exception(error))
            self.assertNotIn(TOKEN, rendered)
            self.assertEqual(error.code, "not_authorized")
        else:
            self.fail("Expected a safe refusal")
        self.assertEqual(self.http.calls, [])

    def test_only_exact_read_endpoint_and_query_are_accepted(self):
        invalid = [None, "", URL + "#secret", URL.replace("https:", "http:"),
            URL.replace("www.tyresystem.de", "evil.invalid"),
            URL.replace("www.tyresystem.de", "www.tyresystem.de.evil.invalid"),
            URL.replace("www.tyresystem.de", "www.tyresystem.de:444"),
            URL.replace("www.tyresystem.de", "user@www.tyresystem.de"),
            URL.replace("www.tyresystem.de", "www.tyresystem.de."),
            URL.replace("TyresystemExport", "../Rest/Order"),
            URL.replace("?index&", "?index=x&"), URL + "&login=other",
            URL + "&index", URL + "&x=1", URL.replace("?index&", "?"),
            URL.replace(TOKEN, ""), URL.replace(TOKEN, "%0d%0aInjected"),
            " " + URL, URL + "\n", URL.replace("https://", "https://password:secret@")]
        for value in invalid:
            with self.subTest(value_type=type(value).__name__), self.assertRaises(export.TyreSystemExportError) as failure:
                self.client(config={"TYRESYSTEM_EXPORT_URL": value})
            self.assertEqual(failure.exception.code, "configuration")
            self.assertNotIn(TOKEN, str(failure.exception))

    def test_environment_has_no_unrelated_credential_fallback(self):
        for config in ({}, {"URL": URL}, {"TYRESYSTEM_API_PASSWORD": TOKEN}, None):
            with self.subTest(config_type=type(config).__name__), self.assertRaises(export.TyreSystemExportError):
                export.TyreSystemExportClient.from_env(environment=config if config is not None else [])

    def test_query_order_and_explicit_https_port_are_canonicalized(self):
        variant = "https://www.tyresystem.de:443/Services/TyresystemExport?login=" + TOKEN + "&index="
        client = self.client(config={"TYRESYSTEM_EXPORT_URL": variant})
        client.fetch()
        self.assertEqual(self.http.calls[0][0], URL)

    def test_default_transport_is_isolated_and_closed(self):
        http = Mock()
        response = Response()
        http.get.return_value = response
        with patch("tyresystem_export.requests.Session", return_value=http):
            client = export.TyreSystemExportClient(CONFIG, authorize=lambda: True, clock=lambda: NOW)
            client.fetch()
        self.assertIs(http.trust_env, False)
        prefix, adapter = http.mount.call_args.args
        self.assertEqual(prefix, "https://")
        self.assertEqual(adapter.max_retries.total, 0)
        http.close.assert_called_once()
        self.assertTrue(response.closed)

    def test_no_session_is_created_on_missing_runtime_authorization(self):
        with patch("tyresystem_export.requests.Session") as session:
            with self.assertRaises(export.TyreSystemExportError):
                export.TyreSystemExportClient(CONFIG).fetch()
        session.assert_not_called()

    def test_status_failures_do_not_parse_or_retry(self):
        for status, code in [(401, "authentication"), (403, "authentication"),
                             (429, "rate_limited"), (302, "unavailable"), (500, "unavailable")]:
            response = Response(status=status)
            self.rejected(response, code)
            self.assertFalse(response.iterated)
            self.assertEqual(len(self.http.calls), 1)

    def test_response_url_change_is_rejected(self):
        self.rejected(Response(url="https://evil.invalid/"), "invalid_response")

    def test_content_length_limit_rejects_before_streaming(self):
        for length in (str(export.MAX_EXPORT_BYTES + 1), "-1", "unknown", "1,000", 123):
            response = Response(headers={"Content-Length": length})
            self.rejected(response, "invalid_response")
            self.assertFalse(response.iterated)

    def test_stream_limit_applies_even_with_missing_or_false_small_length(self):
        for headers in ({}, {"Content-Length": "1"}):
            with patch("tyresystem_export.MAX_EXPORT_BYTES", 10):
                self.rejected(Response(chunks=[b"123456", b"78901"], headers=headers), "invalid_response")

    def test_empty_chunks_and_split_rows_are_valid(self):
        raw = csv_data()
        result = self.client(Response(chunks=[b"", raw[:37], b"", raw[37:]])).fetch()
        self.assertEqual(result.statistik["articles"], 1)

    def test_non_byte_chunks_are_rejected(self):
        self.rejected(Response(chunks=["secret " + TOKEN]), "invalid_response")

    def test_html_wrong_header_and_invalid_utf8_are_not_snapshots(self):
        for raw in (b"<html>Login required</html>", b"unexpected;header\n1;2", b"\xff"):
            self.rejected(Response(raw=raw), "invalid_response")

    def test_stream_failure_and_transport_failure_redact_secrets(self):
        class BrokenResponse(Response):
            def iter_content(self, chunk_size):
                yield b"123"
                raise RuntimeError(URL)
        for response in (RuntimeError(URL), BrokenResponse()):
            client = self.client(response)
            try:
                client.fetch()
            except export.TyreSystemExportError as error:
                self.assertEqual(error.code, "unavailable")
                self.assertNotIn(TOKEN, "".join(traceback.format_exception(error)))
                self.assertIsNone(error.__cause__)
            else:
                self.fail("Expected a safe transport error")
            self.assertEqual(len(self.http.calls), 1)

    def test_deadline_after_connect_and_after_final_eof(self):
        for times in ([0, 46], [0, 0, 0, 46]):
            response = Response()
            client = self.client(response, monotonic=iter(times).__next__)
            with self.assertRaises(export.TyreSystemExportError) as failure:
                client.fetch()
            self.assertEqual(failure.exception.code, "unavailable")
            self.assertTrue(response.closed)

    def test_deadline_after_parse_and_clock_errors_are_safe(self):
        client = self.client(monotonic=iter((0, 0, 0, 0, 46)).__next__)
        with self.assertRaises(export.TyreSystemExportError) as failure:
            client.fetch()
        self.assertEqual(failure.exception.code, "unavailable")
        client = self.client()
        client._clock = lambda: datetime(2026, 11, 2)
        with self.assertRaises(export.TyreSystemExportError) as failure:
            client.fetch()
        self.assertEqual(failure.exception.code, "invalid_response")

    def test_response_and_owned_session_close_when_get_fails(self):
        http = Mock()
        http.get.side_effect = RuntimeError(URL)
        with patch("tyresystem_export.requests.Session", return_value=http):
            with self.assertRaises(export.TyreSystemExportError):
                export.TyreSystemExportClient(CONFIG, authorize=lambda: True).fetch()
        http.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
