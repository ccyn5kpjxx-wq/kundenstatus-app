"""Single-position supplier ordering: synthetic HTTP and disposable SQLite only."""
from copy import deepcopy
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import traceback
import unittest
from unittest.mock import Mock, patch
from xml.etree import ElementTree as ET

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import tyresystem_order as orders
from tyresystem_api import TyreSystemInquiryClient, account_fingerprint


NOW=datetime(2026,11,3,16,0,tzinfo=timezone.utc)
SECRET="SYNTHETIC-PRIVATE-PASSWORD"
CONFIG={"TYRESYSTEM_API_USERNAME":"synthetic-api-account", "TYRESYSTEM_API_PASSWORD":SECRET,"TYRESYSTEM_API_MODE":"test"}
REQUEST={"produktart":"reifen","artikel_id":"123","ean":"0012345678905","anzahl":4,
         "einkauf_netto":"68.34","waehrung":"EUR","reference":"synthetic_order_42",
         "delivery_address":{"neutraldelivery":0,"salutation":"Firma","name":"Gärtner GmbH & Co.",
                             "street":"Syntheticstraße 1","zipcode":"01234","city":"Mosbach","countrycode":"DE"}}


def ack(**changes):
    return {"orderResponse":{"positions":{"position":[{"ordernumber":"803305","orderStatus":True,
               "error":0,"errorMessage":None,"pos":1,**changes}]}}}


class Response:
    def __init__(self, data=None, *, raw=None, status=200, content_type="application/json", headers=None, chunks=None, url=orders.ORDER_URL):
        self.raw=raw if raw is not None else json.dumps(data if data is not None else ack()).encode()
        self.status_code,self.url=status,url
        self.headers={"Content-Type":content_type,**(headers or {})}
        self.chunks,self.closed,self.iterated=chunks,False,False
    def iter_content(self,chunk_size):
        self.iterated=True
        if self.chunks is not None:
            yield from self.chunks
        else:
            yield self.raw
    def close(self):
        self.closed=True


class HTTP:
    def __init__(self,response=None,callback=None):
        self.response=response if response is not None else Response()
        self.callback=callback
        self.calls=[]
    def post(self,url,**options):
        self.calls.append((url,options))
        if self.callback:
            self.callback(url,options)
        if isinstance(self.response,BaseException):
            raise self.response
        return self.response


class CommitDB:
    def __init__(self,connection,counter,fail,after):
        self.db,self.counter,self.fail,self.after=connection,counter,fail,after
    def execute(self,*args):
        return self.db.execute(*args)
    def commit(self):
        self.counter[0]+=1
        if self.counter[0]==self.fail and not self.after:
            raise sqlite3.OperationalError(SECRET)
        self.db.commit()
        if self.counter[0]==self.fail and self.after:
            raise sqlite3.OperationalError(SECRET)
    def close(self):
        self.db.close()


class OrderTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect","socket.socket.connect_ex","socket.create_connection","socket.getaddrinfo"):
            block=patch(target,side_effect=AssertionError("External network forbidden"))
            block.start()
            self.addCleanup(block.stop)
        temporary=tempfile.TemporaryDirectory(prefix="supplier-order-offline-")
        self.addCleanup(temporary.cleanup)
        self.path=Path(temporary.name)/"attempts.sqlite3"
        db=self.db()
        orders.init_schema(db)
        db.commit()
        db.close()
        self.request=deepcopy(REQUEST)
        self.client=self.make_client()
        self.store=orders.DurableTyreSystemOrders(self.db,self.client)

    def db(self):
        connection=sqlite3.connect(self.path,timeout=3)
        connection.row_factory=sqlite3.Row
        return connection

    def rows(self):
        db=self.db()
        try:
            return [dict(row) for row in db.execute("SELECT * FROM tyresystem_order_attempts")]
        finally:
            db.close()

    def change(self,**values):
        db=self.db()
        db.execute("UPDATE tyresystem_order_attempts SET "+",".join(k+"=?" for k in values),tuple(values.values()))
        db.commit()
        db.close()

    def make_client(self,response=None,*,authorize=lambda _:True,config=None,callback=None,**kwargs):
        self.http=HTTP(response,callback)
        return orders.TyreSystemOrderClient(config or CONFIG,authorize=authorize,http=self.http,clock=lambda:NOW,**kwargs)

    def send(self,request=None,store=None):
        request=self.request if request is None else request
        return (store or self.store).submit_once(request["reference"],request)

    def test_fixed_xml_post_preserves_ean_zip_and_decimal_and_returns_only_request_bound_fields(self):
        receipt=self.client.submit(self.request)
        self.assertEqual(len(self.http.calls),1)
        url,options=self.http.calls[0]
        self.assertEqual(url,"https://api.tyresystem.de/Rest/Order")
        self.assertEqual(options["auth"],(CONFIG["TYRESYSTEM_API_USERNAME"],SECRET))
        self.assertIs(options["allow_redirects"],False)
        self.assertIs(options["verify"],True)
        self.assertIs(options["stream"],True)
        self.assertEqual(options["timeout"],(3.05,8.0))
        root=ET.fromstring(options["data"])
        self.assertEqual(len(root.findall("./items/item")),1)
        self.assertEqual(root.findtext("./items/item/pos"),"1")
        self.assertEqual(root.findtext("./items/item/ean"),REQUEST["ean"])
        self.assertEqual(root.findtext("./items/item/amount"),"4")
        self.assertEqual(root.findtext("./items/item/price"),"68.3400")
        self.assertEqual(root.findtext("./delivery_address/zipcode"),"01234")
        self.assertEqual(root.findtext("./delivery_address/name"),REQUEST["delivery_address"]["name"])
        self.assertEqual(root.findtext("./ordernumber_customer"),REQUEST["reference"])
        self.assertEqual(receipt["receiptbinding_source"],"submitted_request")
        self.assertEqual(receipt["configured_mode"],"test")
        self.assertEqual(receipt["einkauf_netto"],"68.3400")
        for unsupported in ("lieferdatum","gueltig_bis","reservierung_id","livemode","errorMessage"):
            self.assertNotIn(unsupported,receipt)
        self.assertTrue(self.http.response.closed)

    def test_documented_xml_ack_and_json_status_variants_are_normalized(self):
        xml=b'<orderResponse><positions><position><ordernumber>803303</ordernumber><orderStatus>1</orderStatus><error>0</error><errorMessage/><pos>1</pos></position></positions></orderResponse>'
        client=self.make_client(Response(raw=xml,content_type="application/xml"))
        self.assertEqual(client.submit(self.request)["bestellnummer"],"803303")
        client=self.make_client(Response(raw=xml.replace(b"><",b">\n  <"),content_type="application/xml"))
        self.assertEqual(client.submit(self.request)["bestellnummer"],"803303")
        for value in (True,1,"1"):
            client=self.make_client(Response(ack(orderStatus=value)))
            self.assertTrue(client.submit(self.request)["bestaetigt"])

    def test_auth_defaults_deny_before_post_and_before_new_claim(self):
        for callback in (None,lambda _:False,lambda _:1,lambda _:"true",Mock(side_effect=RuntimeError(SECRET))):
            client=self.make_client(authorize=callback)
            store=orders.DurableTyreSystemOrders(self.db,client)
            with self.assertRaises(orders.TyreSystemOrderError) as caught:
                self.send(store=store)
            self.assertEqual(caught.exception.code,"not_authorized")
            self.assertFalse(self.http.calls or self.rows())

    def test_authorization_closed_after_claim_does_not_post_or_make_claim_retryable(self):
        permits=iter((True,False))
        client=self.make_client(authorize=lambda _:next(permits))
        store=orders.DurableTyreSystemOrders(self.db,client)
        with self.assertRaises(orders.TyreSystemOrderError) as caught: self.send(store=store)
        self.assertEqual(caught.exception.outcome,"not_sent")
        self.assertEqual(self.rows()[0]["state"],"blocked")
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertFalse(self.http.calls)

    def test_authorization_envelope_and_request_are_deeply_immutable_and_hash_is_authoritative(self):
        seen=[]
        def authorize(record):
            seen.append(record)
            self.assertEqual(set(record),{"reference","request","request_sha256","configured_mode","account_fingerprint"})
            self.assertEqual(record["request_sha256"],orders.request_sha256(self.request))
            with self.assertRaises(TypeError): record["request"]["anzahl"]=8
            with self.assertRaises(TypeError): record["request"]["delivery_address"]["city"]="evil"
            with self.assertRaises(TypeError): record["configured_mode"]="live"
            return True
        client=self.make_client(authorize=authorize)
        self.send(store=orders.DurableTyreSystemOrders(self.db,client))
        self.assertEqual(len(seen),2)
        frozen=orders.freeze_order_request(self.request)
        self.assertIn("redacted",repr(frozen))
        for private in ("68.3400",self.request["delivery_address"]["name"],self.request["delivery_address"]["street"]):
            self.assertNotIn(private,repr(frozen))
        same=deepcopy(self.request)
        same["einkauf_netto"]="68.3400"
        self.assertEqual(orders.request_sha256(same),orders.request_sha256(frozen))
        self.request["delivery_address"]["city"]="changed"
        self.assertEqual(frozen.delivery_address["city"],"Mosbach")

    def test_shared_inquiry_and_order_account_binding_differs_for_mode_and_user_not_password_rotation(self):
        inquiry=TyreSystemInquiryClient(CONFIG)
        self.assertEqual(inquiry.account_fingerprint,self.client.account_fingerprint)
        self.assertEqual(self.store.account_fingerprint,inquiry.account_fingerprint)
        self.assertEqual(self.store.configured_mode,"test")
        for changes in ({"TYRESYSTEM_API_MODE":"live"},{"TYRESYSTEM_API_USERNAME":"other-api-account"}):
            client=self.make_client(config={**CONFIG,**changes})
            self.assertNotEqual(client.account_fingerprint,inquiry.account_fingerprint)
        self.assertEqual(account_fingerprint(CONFIG["TYRESYSTEM_API_USERNAME"],"test"),inquiry.account_fingerprint)
        client=self.make_client(config={**CONFIG,"TYRESYSTEM_API_PASSWORD":"rotated-synthetic-password"})
        self.assertEqual(client.account_fingerprint,inquiry.account_fingerprint)
        with self.assertRaises(AttributeError): self.store.account_fingerprint="changed"
        with self.assertRaises(AttributeError): inquiry.account_fingerprint="changed"

    def test_invalid_or_additional_request_fields_and_floats_never_claim_or_post(self):
        invalid=({"produktart":"komplettraeder"},{"produktart":"felgen","montage":True},
                 {"artikel_id":"0123"},{"artikel_id":"../123"},{"ean":1234567890123},{"ean":"123"},
                 {"anzahl":True},{"anzahl":0},{"anzahl":9},{"anzahl":"4"},
                 {"einkauf_netto":68.34},{"einkauf_netto":"NaN"},{"einkauf_netto":"1e2"},
                 {"einkauf_netto":"0.0000"},{"einkauf_netto":"1.00001"},{"waehrung":"USD"},
                 {"reference":"https://private.invalid/"},{"preis":"0.01"})
        for changes in invalid:
            with self.subTest(keys=list(changes)):
                request={**deepcopy(self.request),**changes}
                with self.assertRaises(orders.TyreSystemOrderError): self.send(request)
        for changes in ({"zipcode":1234},{"countrycode":"AT"},{"salutation":"Herr"},
                        {"neutraldelivery":True},{"name":"private\nheader"},{"city":""}):
            request=deepcopy(self.request)
            request["delivery_address"].update(changes)
            with self.assertRaises(orders.TyreSystemOrderError): self.send(request)
        self.assertFalse(self.rows() or self.http.calls)

    def test_constructor_from_env_is_inert_dedicated_and_repr_is_redacted(self):
        with patch("tyresystem_order.requests.Session") as session:
            client=orders.TyreSystemOrderClient.from_env(environment=CONFIG)
        session.assert_not_called()
        self.assertNotIn(SECRET,repr(client))
        self.assertNotIn(CONFIG["TYRESYSTEM_API_USERNAME"],repr(client))
        for environment in ({},{"SHOP_USER":"shop","SHOP_PASS":SECRET},[],None):
            with patch.dict("os.environ",{},clear=True),self.assertRaises(orders.TyreSystemOrderError):
                orders.TyreSystemOrderClient.from_env(environment=environment)

    def test_default_transport_has_no_proxy_or_retries_and_closes_owned_session(self):
        http=Mock()
        http.post.return_value=Response()
        client=orders.TyreSystemOrderClient(CONFIG,authorize=lambda _:True,clock=lambda:NOW)
        with patch("tyresystem_order.requests.Session",return_value=http):
            client.submit(self.request)
        self.assertIs(http.trust_env,False)
        self.assertEqual(http.mount.call_args.args[0],"https://")
        self.assertEqual(http.mount.call_args.args[1].max_retries.total,0)
        http.close.assert_called_once()
        self.assertEqual(http.post.call_count,1)

    def test_partial_missing_duplicate_foreign_and_contradictory_ack_positions_are_unknown(self):
        second={"ordernumber":"803306","orderStatus":False,"error":1,"errorMessage":SECRET,"pos":2}
        data=ack()
        data["orderResponse"]["positions"]["position"].append(second)
        cases=[data,{"orderResponse":{"positions":{"position":[]}}},ack(pos=2),ack(pos=True),
               ack(error=True),ack(orderStatus=True,error=1),ack(orderStatus=False,error=0),
               ack(ordernumber=""),ack(errorMessage=SECRET),ack(unexpected="private")]
        duplicate=ack()
        duplicate["orderResponse"]["positions"]["position"]*=2
        cases.append(duplicate)
        for data in cases:
            client=self.make_client(Response(data))
            with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
            self.assertEqual(caught.exception.outcome,"unknown")
            self.assertEqual(len(self.http.calls),1)
            self.assertNotIn(SECRET,str(caught.exception))

    def test_only_explicit_full_rejection_is_definitive_and_is_never_resent(self):
        client=self.make_client(Response(ack(ordernumber="",orderStatus=False,error=1,errorMessage=SECRET)))
        store=orders.DurableTyreSystemOrders(self.db,client)
        with self.assertRaises(orders.TyreSystemOrderError) as caught: self.send(store=store)
        self.assertEqual(caught.exception.outcome,"rejected")
        self.assertEqual(self.rows()[0]["state"],"rejected")
        with self.assertRaises(orders.TyreSystemOrderError) as recovered: self.send(store=store)
        self.assertEqual(recovered.exception.code,"supplier_rejected")
        self.assertEqual(recovered.exception.outcome,"rejected")
        self.assertEqual(len(self.http.calls),1)
        self.assertNotIn(SECRET,json.dumps(self.rows()))

    def test_duplicate_json_keys_xml_entities_nested_fields_and_wrong_encoding_are_unknown(self):
        raw=(b'{"orderResponse":{"positions":{"position":[]}},"orderResponse":{}}',
             b'<!DOCTYPE orderResponse [<!ENTITY p "PRIVATE">]><orderResponse>&p;</orderResponse>',
             b'<orderResponse><positions><position><pos><child>1</child></pos></position></positions></orderResponse>',
             b'<?xml version="1.0" encoding="iso-8859-1"?><orderResponse/>',b'\xff')
        for value in raw:
            content="application/json" if value.startswith(b'{') else "application/xml"
            client=self.make_client(Response(raw=value,content_type=content))
            with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
            self.assertEqual(caught.exception.outcome,"unknown")

    def test_xml_unexpected_mixed_text_outside_documented_leaf_values_is_unknown(self):
        xml='<orderResponse><positions><position><ordernumber>803303</ordernumber><orderStatus>1</orderStatus><error>0</error><errorMessage/><pos>1</pos></position></positions></orderResponse>'
        targets=((".","text"),("positions","text"),("positions/position","text"),
                 ("positions","tail"),("positions/position","tail"),
                 *(("positions/position/"+name,"tail") for name in ("ordernumber","orderStatus","error","errorMessage","pos")))
        for path,attribute in targets:
            with self.subTest(path=path,attribute=attribute):
                root=ET.fromstring(xml)
                setattr(root if path=="." else root.find(path),attribute,SECRET)
                client=self.make_client(Response(raw=ET.tostring(root),content_type="application/xml"))
                with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
                self.assertEqual(caught.exception.outcome,"unknown")
                self.assertNotIn(SECRET,str(caught.exception))
                self.assertEqual(len(self.http.calls),1)

    def test_http_auth_redirect_rate_limit_provider_failure_timeout_and_url_mismatch_never_retry(self):
        for response in (Response(status=401),Response(status=403),Response(status=429),Response(status=302),
                         Response(status=500),Response(url="https://private.invalid/"),TimeoutError(SECRET)):
            client=self.make_client(response)
            with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
            self.assertEqual(caught.exception.outcome,"unknown")
            self.assertEqual(len(self.http.calls),1)
            rendered="".join(traceback.format_exception(caught.exception))
            self.assertNotIn(SECRET,rendered)

    def test_request_response_and_stream_limits_fail_closed(self):
        with patch("tyresystem_order.MAX_REQUEST_BYTES",8),self.assertRaises(orders.TyreSystemOrderError):
            self.client.submit(self.request)
        self.assertFalse(self.http.calls)
        for response in (Response(headers={"Content-Length":"65537"}),Response(headers={"Content-Length":"-1"}),
                         Response(chunks=[b"x"*40000,b"y"*30000]),Response(chunks=["private "+SECRET])):
            client=self.make_client(response)
            with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
            self.assertEqual(caught.exception.outcome,"unknown")
            self.assertTrue(response.closed)

    def test_deadline_after_post_chunk_eof_and_parse_is_unknown(self):
        for times in ((0,16),(0,0,16),(0,0,0,16),(0,0,0,0,16)):
            client=self.make_client(monotonic=iter(times).__next__)
            with self.assertRaises(orders.TyreSystemOrderError) as caught: client.submit(self.request)
            self.assertEqual(caught.exception.outcome,"unknown")
            self.assertEqual(len(self.http.calls),1)
            self.assertTrue(self.http.response.closed)

    def test_claim_is_committed_before_http_and_ack_committed_before_success_is_returned(self):
        def inspect_claim(url,options):
            rows=self.rows()
            self.assertEqual(len(rows),1)
            self.assertEqual(rows[0]["state"],"claimed")
            self.assertEqual(json.loads(rows[0]["request_json"])["ean"],REQUEST["ean"])
            self.assertEqual(rows[0]["request_xml_sha256"],orders._hash(options["data"]))
            db=self.db()
            db.execute("UPDATE tyresystem_order_attempts SET updated_at=updated_at")
            db.commit()  # No transaction is held across external IO.
            db.close()
        client=self.make_client(callback=inspect_claim)
        receipt=self.send(store=orders.DurableTyreSystemOrders(self.db,client))
        self.assertEqual(receipt["receiptbinding_source"],"persisted_request")
        row=self.rows()[0]
        self.assertEqual(row["state"],"acknowledged")
        self.assertEqual(json.loads(row["receipt_json"]),receipt)
        self.assertNotIn(SECRET,json.dumps(row))
        self.assertEqual(row["receipt_hash"],orders._hash(orders._canonical(receipt)))

    def test_identical_ack_reuses_stored_receipt_even_when_new_orders_are_disabled(self):
        receipt=self.send()
        self.client._authorize=lambda _:False
        self.assertEqual(self.send(),receipt)
        self.assertEqual(len(self.http.calls),1)

    def test_acknowledged_recovers_saved_ack_readonly_after_core_crash_without_authorization_or_post(self):
        receipt=self.send()
        before=self.path.read_bytes()
        before_rows=self.rows()
        queries=[]
        def reader_db():
            db=self.db()
            db.set_trace_callback(queries.append)
            return db
        authorize=Mock(side_effect=AssertionError("Readonly recovery must not authorize"))
        fresh_client=self.make_client(authorize=authorize)
        fresh_store=orders.DurableTyreSystemOrders(reader_db,fresh_client)
        with patch.object(fresh_store,"submit_once",side_effect=AssertionError("No submission in recovery")):
            self.assertEqual(fresh_store.acknowledged(self.request["reference"],self.request),receipt)
        authorize.assert_not_called()
        self.assertFalse(self.http.calls)
        self.assertTrue(queries)
        self.assertTrue(all(query.lstrip().upper().startswith("SELECT ") for query in queries))
        self.assertEqual(self.rows(),before_rows)
        self.assertEqual(self.path.read_bytes(),before)

    def test_acknowledged_absent_and_nonack_states_return_none_without_changing_or_sending(self):
        self.assertIsNone(self.store.acknowledged(self.request["reference"],self.request))
        self.assertFalse(self.rows() or self.http.calls)
        self.send()
        for state in ("claimed","unknown","rejected","blocked"):
            self.change(state=state,failure_code="supplier_rejected" if state=="rejected" else "unknown")
            before=self.rows()
            self.assertIsNone(self.store.acknowledged(self.request["reference"],self.request))
            self.assertEqual(self.rows(),before)
        self.assertEqual(len(self.http.calls),1)

    def test_acknowledged_rejects_identity_and_receipt_tampering_without_io_or_mutation(self):
        receipt=self.send()
        for changes in ({"einkauf_netto":"68.35"},{"produktart":"felgen"},{"anzahl":2}):
            with self.assertRaises(orders.TyreSystemOrderError) as caught:
                self.store.acknowledged(self.request["reference"],{**self.request,**changes})
            self.assertEqual(caught.exception.code,"attempt_conflict")
        for config in ({**CONFIG,"TYRESYSTEM_API_MODE":"live"},{**CONFIG,"TYRESYSTEM_API_USERNAME":"different"}):
            client=self.make_client(config=config)
            with self.assertRaises(orders.TyreSystemOrderError) as caught:
                orders.DurableTyreSystemOrders(self.db,client).acknowledged(self.request["reference"],self.request)
            self.assertEqual(caught.exception.code,"attempt_conflict")
            self.assertFalse(self.http.calls)
        self.change(receipt_json=orders._canonical({**receipt,"anzahl":8}))
        before=self.rows()
        with self.assertRaises(orders.TyreSystemOrderError) as caught:
            self.store.acknowledged(self.request["reference"],self.request)
        self.assertEqual(caught.exception.code,"attempt_unknown")
        self.assertEqual(self.rows(),before)

    def test_request_price_type_sku_ean_quantity_reference_address_and_account_mode_changes_conflict(self):
        self.send()
        cases=({"produktart":"felgen"},{"artikel_id":"124"},{"ean":"0012345678906"},
               {"anzahl":2},{"einkauf_netto":"68.35"})
        for changes in cases:
            with self.assertRaises(orders.TyreSystemOrderError) as caught: self.send({**deepcopy(self.request),**changes})
            self.assertEqual(caught.exception.code,"attempt_conflict")
        changed=deepcopy(self.request)
        changed["delivery_address"]["city"]="Berlin"
        with self.assertRaises(orders.TyreSystemOrderError): self.send(changed)
        for changes in ({"TYRESYSTEM_API_MODE":"live"},{"TYRESYSTEM_API_USERNAME":"another-api"}):
            client=self.make_client(config={**CONFIG,**changes})
            with self.assertRaises(orders.TyreSystemOrderError) as caught:
                self.send(store=orders.DurableTyreSystemOrders(self.db,client))
            self.assertEqual(caught.exception.code,"attempt_conflict")
            self.assertFalse(self.http.calls)
        with self.assertRaises(orders.TyreSystemOrderError): self.store.submit_once("different",self.request)
        self.assertEqual(len(self.rows()),1)

    def test_unknown_timeout_and_crash_before_post_consume_claim_and_never_repeat(self):
        client=self.make_client(TimeoutError(SECRET))
        store=orders.DurableTyreSystemOrders(self.db,client)
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertEqual(self.rows()[0]["state"],"unknown")
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertEqual(len(self.http.calls),1)
        self.request["reference"]="synthetic_crash_before_post"
        with patch.object(self.client,"submit",side_effect=SystemExit("offline simulated crash")):
            with self.assertRaises(SystemExit): self.send()
        with self.assertRaises(orders.TyreSystemOrderError): self.send()
        self.assertEqual(len(self.rows()),2)

    def test_old_claimed_record_never_becomes_sendable_by_age_or_process_restart(self):
        with patch.object(self.client,"submit",side_effect=SystemExit()):
            with self.assertRaises(SystemExit): self.send()
        self.change(state="claimed",created_at="2000-01-01T00:00:00+00:00")
        fresh_client=self.make_client()
        fresh_store=orders.DurableTyreSystemOrders(self.db,fresh_client)
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=fresh_store)
        self.assertFalse(self.http.calls)

    def test_two_concurrent_callers_cannot_issue_two_posts(self):
        started,finish=threading.Event(),threading.Event()
        result=[]
        def hold(url,options):
            started.set()
            if not finish.wait(3): raise AssertionError("Offline test timed out")
        client=self.make_client(callback=hold)
        store=orders.DurableTyreSystemOrders(self.db,client)
        def first():
            try: result.append(self.send(store=store))
            except BaseException as error: result.append(error)
        thread=threading.Thread(target=first)
        thread.start()
        try:
            self.assertTrue(started.wait(3))
            with self.assertRaises(orders.TyreSystemOrderError) as caught: self.send(store=store)
            self.assertEqual(caught.exception.code,"attempt_unknown")
        finally:
            finish.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.http.calls),1)
        self.assertIsInstance(result[0],dict)

    def test_claim_commit_failure_before_or_after_commit_never_reaches_http(self):
        for after in (False,True):
            self.request["reference"]="synthetic_commit_"+str(after)
            counter=[0]
            store=orders.DurableTyreSystemOrders(lambda:CommitDB(self.db(),counter,1,after),self.client)
            with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
            self.assertFalse(self.http.calls)
            if after:
                with self.assertRaises(orders.TyreSystemOrderError): self.send()
                self.assertEqual(self.rows()[-1]["state"],"claimed")

    def test_ack_storage_failure_before_commit_is_unknown_and_after_commit_reuses_durable_ack(self):
        for after in (False,True):
            self.request["reference"]="synthetic_ack_commit_"+str(after)
            counter=[0]
            client=self.make_client()
            store=orders.DurableTyreSystemOrders(lambda:CommitDB(self.db(),counter,2,after),client)
            with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
            retry=orders.DurableTyreSystemOrders(self.db,client)
            if after:
                self.assertEqual(self.send(store=retry)["receiptbinding_source"],"persisted_request")
            else:
                with self.assertRaises(orders.TyreSystemOrderError): self.send(store=retry)
            self.assertEqual(len(self.http.calls),1)

    def test_lost_ack_cas_never_returns_success_or_repeats_post(self):
        client=self.make_client(callback=lambda *_:self.change(state="unknown"))
        store=orders.DurableTyreSystemOrders(self.db,client)
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertEqual(self.rows()[0]["state"],"unknown")
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertEqual(len(self.http.calls),1)

    def test_late_transport_error_cannot_overwrite_a_concurrently_persisted_ack(self):
        frozen=orders.freeze_order_request(self.request)
        receipt=orders._receipt(frozen,self.client.configured_mode,self.client.account_fingerprint,
                               "803305",NOW.isoformat(),"persisted_request")
        encoded=orders._canonical(receipt)
        def persist_ack(*_):
            self.change(state="acknowledged",receipt_json=encoded,receipt_hash=orders._hash(encoded))
        client=self.make_client(TimeoutError(SECRET),callback=persist_ack)
        store=orders.DurableTyreSystemOrders(self.db,client)
        with self.assertRaises(orders.TyreSystemOrderError): self.send(store=store)
        self.assertEqual(self.rows()[0]["state"],"acknowledged")
        self.assertEqual(self.send(store=store),receipt)
        self.assertEqual(len(self.http.calls),1)

    def test_mutated_receipt_or_persisted_request_never_gets_reused(self):
        receipt=self.send()
        self.change(receipt_json=json.dumps({**receipt,"anzahl":8}))
        with self.assertRaises(orders.TyreSystemOrderError): self.send()
        self.change(receipt_json=orders._canonical(receipt),receipt_hash=orders._hash(orders._canonical(receipt)),request_json="{}")
        with self.assertRaises(orders.TyreSystemOrderError): self.send()
        self.assertEqual(len(self.http.calls),1)

    def test_felgen_stay_individual_no_montage_and_no_supplier_sku_echo_claim(self):
        self.request["produktart"]="felgen"
        receipt=self.send()
        self.assertEqual(receipt["produktart"],"felgen")
        self.assertEqual(receipt["receiptbinding_source"],"persisted_request")
        for key in ("groesse","montage","lieferdatum","reservierung_id"):
            self.assertNotIn(key,receipt)
        self.assertEqual(len(self.http.calls),1)


if __name__=="__main__":
    unittest.main(verbosity=2)
