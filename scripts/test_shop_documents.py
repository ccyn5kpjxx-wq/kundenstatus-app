"""Synthetic released label/PDF documents; all network transports are fake."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import base64
import socket
import sys
import tempfile
import threading
import unittest
from urllib.parse import urlencode
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from flask import Flask
from PIL import Image
from pypdf import PdfWriter
from pypdf.generic import NameObject,DictionaryObject,DecodedStreamObject
from shop_documents import (DocumentError,ReleasedProductDocuments,create_shop_documents_blueprint,
    document_asset,document_eprel_id,MAX_IMAGE,MAX_PDF)

SHEET='https://eprel.ec.europa.eu/fiches/tyres/Fiche_123456_EN.pdf'
LABEL='https://media1.tyre-shopping.com/tyrelabel2/C/B/B/70/1/0/0/'+'a'*32+'-h300-w300.jpg?'+urlencode({
    'manufacturer':'Synthetic','model':'Example','size':'205/55 R16 91V',
    'qr':base64.b64encode(b'https://eprel.ec.europa.eu/qr/123456').decode()})
PDF_ASSET=document_asset(SHEET)
IMAGE_ASSET=document_asset(LABEL)
CONTI='https://www.contimediacenter.com/eulabel-download-pis-pdf/de_DE/04511370000/'


def image():
    stream=BytesIO();Image.new('RGB',(300,300),'white').save(stream,format='JPEG');return stream.getvalue()


def pdf():
    stream=BytesIO();writer=PdfWriter();writer.add_blank_page(width=100,height=100);writer.write(stream);return stream.getvalue()


def tyre_pdf():
    stream=BytesIO();writer=PdfWriter();page=writer.add_blank_page(width=400,height=200)
    font=DictionaryObject({NameObject('/Type'):NameObject('/Font'),NameObject('/Subtype'):NameObject('/Type1'),NameObject('/BaseFont'):NameObject('/Helvetica')})
    page[NameObject('/Resources')]=DictionaryObject({NameObject('/Font'):DictionaryObject({NameObject('/F1'):writer._add_object(font)})})
    content=DecodedStreamObject();content.set_data(b'BT /F1 12 Tf 10 100 Td (CONTINENTAL 195/65 R15 95 T) Tj ET')
    page[NameObject('/Contents')]=writer._add_object(content);writer.write(stream);return stream.getvalue()


class Response:
    def __init__(self,url,body,mime):
        self.url,self.body,self.status_code=url,body,200
        self.headers={'Content-Type':mime,'Content-Length':str(len(body))}
        self.closed=False
    def iter_content(self,**kwargs):yield self.body
    def close(self):self.closed=True


class HTTP:
    def __init__(self):self.calls=[];self.responses={LABEL:Response(LABEL,image(),'image/jpeg'),SHEET:Response(SHEET,pdf(),'application/pdf')}
    def get(self,url,**kwargs):self.calls.append((url,kwargs));return self.responses[url]


class DocumentTests(unittest.TestCase):
    def test_reviewed_wheel_report_is_hash_bound_local_only_and_keeps_original_pages(self):
        stream=BytesIO();writer=PdfWriter()
        for _ in range(30):writer.add_blank_page(width=100,height=100)
        writer.write(stream);data=stream.getvalue();digest=sha256(data).hexdigest();asset=digest+'.pdf'
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();(root/asset).write_bytes(data)
            resource={'kind':'wheel_report','file':asset,'sha256':digest}
            service=ReleasedProductDocuments({asset:resource},approved_assets={asset},enabled=True,local_root=root)
            self.assertEqual(service.read(asset),(data,'application/pdf'))
            self.assertEqual(service.read(asset)[0],data)
            for invalid in ({**resource,'file':'../'+asset},{**resource,'sha256':'f'*64},
                            {**resource,'kind':'unreviewed_report'},{**resource,'url':SHEET}):
                with self.subTest(invalid=invalid),self.assertRaises(DocumentError):
                    ReleasedProductDocuments({asset:invalid},approved_assets={asset},enabled=True,local_root=root)
            (root/asset).write_bytes(b'changed')
            fresh=ReleasedProductDocuments({asset:resource},approved_assets={asset},enabled=True,local_root=root)
            with self.assertRaises(DocumentError):fresh.read(asset)
        with self.assertRaises(DocumentError):ReleasedProductDocuments._validate(data,PDF_ASSET)

    def setUp(self):
        self.now=0.
        self.http=HTTP()
        self.resources={IMAGE_ASSET:{'url':LABEL},PDF_ASSET:{'url':SHEET}}
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('real network forbidden'));self.network.start()
    def tearDown(self):self.network.stop()

    def service(self,**changes):
        values=dict(approved_assets=set(self.resources),enabled=True,allow_network=True,http=self.http,
            max_network_reads_per_period=2,period_seconds=60,spacing_seconds=2,monotonic=lambda:self.now)
        values.update(changes)
        return ReleasedProductDocuments(self.resources,**values)

    def test_defaults_construction_no_file_network_threads_and_explicit_gates(self):
        with patch.object(Path,'read_bytes',side_effect=AssertionError('file IO forbidden')), \
             patch.object(Path,'resolve',side_effect=AssertionError('file IO forbidden')), \
             patch('threading.Thread',side_effect=AssertionError('thread forbidden')):
            default=ReleasedProductDocuments(self.resources,approved_assets=set(self.resources))
        with self.assertRaisesRegex(DocumentError,'document_not_found'):default.read(PDF_ASSET)
        for changes in ({'enabled':False},{'allow_network':False},{'max_network_reads_per_period':0}):
            with self.subTest(changes=changes),self.assertRaises(DocumentError):self.service(**changes).read(PDF_ASSET)
        self.assertEqual(self.http.calls,[])

    def test_original_full_url_asset_identity_qr_and_static_sheet_binding(self):
        self.assertEqual(document_eprel_id(LABEL),'123456')
        self.assertEqual(document_eprel_id(SHEET),'123456')
        self.assertEqual(IMAGE_ASSET,sha256(LABEL.encode()).hexdigest()+'.jpg')
        self.assertNotEqual(document_asset(LABEL.replace('Example','Other')),IMAGE_ASSET)
        forbidden=(LABEL+'&token=SECRET',LABEL.replace('https://','http://',1),LABEL.replace('media1.','evil.'),
            LABEL.replace('/tyrelabel2/','/tyrelabel2/../'),LABEL.replace('qr=','auth='),SHEET+'?token=SECRET',
            SHEET.replace('eprel.ec.europa.eu','eprel.ec.europa.eu.evil.invalid'),
            LABEL[:LABEL.index('?')]+'?'+urlencode({'manufacturer':'Synthetic','model':'Example','size':'205/55 R16',
                'qr':base64.b64encode(b'https://evil.invalid/qr/123456').decode()}))
        for url in forbidden:
            with self.subTest(url=url),self.assertRaises(DocumentError):document_asset(url)
        with self.assertRaises(DocumentError):
            ReleasedProductDocuments({PDF_ASSET:{'url':SHEET.replace('123456','654321')}},approved_assets={PDF_ASSET})

    def test_valid_bytes_unchanged_verified_and_cache_only_reads(self):
        service=self.service()
        data,mime=service.read(IMAGE_ASSET)
        self.assertEqual(data,image());self.assertEqual(mime,'image/jpeg')
        self.assertEqual(service.read(IMAGE_ASSET),(data,mime));self.assertEqual(len(self.http.calls),1)
        self.now=2.
        data,mime=service.read(PDF_ASSET)
        self.assertEqual(data,pdf());self.assertEqual(mime,'application/pdf')
        self.assertEqual(len(self.http.calls),2)
        for _,kwargs in self.http.calls:
            self.assertIs(kwargs['verify'],True);self.assertIs(kwargs['allow_redirects'],False)
            self.assertTrue(kwargs['stream']);self.assertNotIn('Authorization',kwargs['headers'])
        self.assertTrue(all(response.closed for response in self.http.responses.values()))

    def test_redirect_wrong_mime_malformed_and_oversize_never_cache(self):
        for asset,change in ((PDF_ASSET,lambda r:setattr(r,'status_code',302)),
            (PDF_ASSET,lambda r:setattr(r,'url','https://evil.invalid/file.pdf')),
            (PDF_ASSET,lambda r:r.headers.update({'Content-Type':'text/html'})),
            (PDF_ASSET,lambda r:setattr(r,'body',b'%PDF-1.7\nnot a real PDF\n%%EOF')),
            (PDF_ASSET,lambda r:r.headers.update({'Content-Length':str(MAX_PDF+1)})),
            (PDF_ASSET,lambda r:setattr(r,'body',b'x'*(MAX_PDF+1))),
            (IMAGE_ASSET,lambda r:setattr(r,'body',b'not a real JPEG')),
            (IMAGE_ASSET,lambda r:setattr(r,'body',b'x'*(MAX_IMAGE+1))),
            (IMAGE_ASSET,lambda r:r.headers.update({'Content-Length':str(MAX_IMAGE+1)}))):
            with self.subTest(asset=asset,change=change):
                self.http=HTTP();change(self.http.responses[SHEET if asset==PDF_ASSET else LABEL]);service=self.service()
                with self.assertRaises(DocumentError):service.read(asset)
                self.assertEqual(service.used,0)
                with self.assertRaises(DocumentError):service.read(asset)
                self.assertEqual(len(self.http.calls),1)

    def test_encrypted_or_active_pdf_is_not_a_released_product_sheet(self):
        for action in ('encrypted','javascript'):
            stream=BytesIO();writer=PdfWriter();writer.add_blank_page(width=100,height=100)
            if action=='encrypted':writer.encrypt('SYNTHETIC_ONLY')
            else:writer.add_js('app.alert("synthetic")')
            writer.write(stream)
            with self.subTest(action=action),self.assertRaises(DocumentError):
                ReleasedProductDocuments._validate(stream.getvalue(),PDF_ASSET)

    def test_global_period_budget_spacing_and_clock_rollback(self):
        service=self.service(max_network_reads_per_period=1)
        service.read(IMAGE_ASSET)
        for now in (1.,2.,59.):
            self.now=now
            with self.assertRaises(DocumentError):service.read(PDF_ASSET)
        self.assertEqual(len(self.http.calls),1)
        self.now=60.;self.assertEqual(service.read(PDF_ASSET)[1],'application/pdf')
        self.now=59.
        with self.assertRaises(DocumentError):service.read(IMAGE_ASSET)
        self.assertFalse(service.enabled)

    def test_same_asset_inflight_and_global_concurrency_are_nonblocking(self):
        entered,release=threading.Event(),threading.Event()
        original=self.http.get
        def blocking(url,**kwargs):entered.set();release.wait(3);return original(url,**kwargs)
        self.http.get=blocking
        service=self.service()
        result=[]
        thread=threading.Thread(target=lambda:result.append(service.read(IMAGE_ASSET)))
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            self.now=2.
            for asset in (IMAGE_ASSET,PDF_ASSET):
                with self.assertRaisesRegex(DocumentError,'document_busy'):service.read(asset)
        finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(len(result),1);self.assertEqual(len(self.http.calls),1)

    def test_hash_bound_private_local_source_and_no_escape(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);body=pdf();(root/'sheet.pdf').write_bytes(body)
            resources={PDF_ASSET:{'url':SHEET,'file':'sheet.pdf','sha256':sha256(body).hexdigest()}}
            service=ReleasedProductDocuments(resources,approved_assets={PDF_ASSET},enabled=True,local_root=root)
            self.assertEqual(service.read(PDF_ASSET),(body,'application/pdf'))
            changed=ReleasedProductDocuments(resources,approved_assets={PDF_ASSET},enabled=True,local_root=root)
            (root/'sheet.pdf').write_bytes(pdf()+b'changed')
            with self.assertRaises(DocumentError):changed.read(PDF_ASSET)
            with self.assertRaises(DocumentError):
                ReleasedProductDocuments({PDF_ASSET:{**resources[PDF_ASSET],'file':'../outside.pdf'}},approved_assets={PDF_ASSET},local_root=root)
        self.assertEqual(self.http.calls,[])

    def test_http_routes_default_off_opaque_paths_csp_and_no_mutations(self):
        app=Flask(__name__);app.register_blueprint(create_shop_documents_blueprint(self.service()))
        client=app.test_client()
        self.assertEqual(client.get('/shop/documents/'+IMAGE_ASSET).status_code,404)
        app.config['SHOP_DOCUMENTS_ENABLED']=True
        result=client.get('/shop/documents/'+IMAGE_ASSET)
        self.assertEqual(result.status_code,200);self.assertEqual(result.mimetype,'image/jpeg')
        self.assertEqual(result.headers['X-Content-Type-Options'],'nosniff')
        self.assertEqual(result.headers['Referrer-Policy'],'no-referrer')
        self.assertIn('sandbox',result.headers['Content-Security-Policy'])
        self.assertNotIn('tyre-shopping',str(result.headers));self.assertNotIn('manufacturer=',str(result.headers))
        self.assertEqual(client.get('/shop/documents/'+'f'*64+'.pdf').status_code,404)
        self.assertEqual(client.post('/shop/documents/'+IMAGE_ASSET).status_code,405)

    def test_exact_manufacturer_pdf_route_requires_private_technical_binding_and_matching_content(self):
        asset=document_asset(CONTI)
        identity={'manufacturer':'continental','width_mm':'195','aspect_ratio_percent':'65','diameter_inch':'15','load_index':'95','speed_index':'t'}
        self.assertIsNone(document_eprel_id(CONTI))
        self.assertTrue(asset.endswith('.pdf'))
        for url in (CONTI.replace('de_DE','en'),CONTI+'?token=SECRET',CONTI.replace('www.contimediacenter.com','evil.invalid'),CONTI.rstrip('/')):
            with self.assertRaises(DocumentError):document_asset(url)
        with self.assertRaises(DocumentError):ReleasedProductDocuments({asset:{'url':CONTI}},approved_assets={asset})
        body=tyre_pdf();self.http.responses[CONTI]=Response(CONTI,body,'application/pdf')
        resources={asset:{'url':CONTI,'tyre_identity':identity}}
        service=ReleasedProductDocuments(resources,approved_assets={asset},enabled=True,allow_network=True,http=self.http,
            max_network_reads_per_period=1,monotonic=lambda:self.now)
        self.assertEqual(service.read(asset),(body,'application/pdf'))
        wrong={asset:{'url':CONTI,'tyre_identity':{**identity,'width_mm':'205'}}}
        service=ReleasedProductDocuments(wrong,approved_assets={asset},enabled=True,allow_network=True,http=self.http,
            max_network_reads_per_period=1,monotonic=lambda:self.now)
        with self.assertRaises(DocumentError):service.read(asset)
        self.assertEqual(service.used,0)


if __name__=='__main__':unittest.main()
