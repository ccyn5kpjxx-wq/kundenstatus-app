"""Explicitly released original tyre labels and product sheets, unchanged.

No provider API, credentials, caller-supplied URLs or inferred EPREL IDs. Both
the service and network reads are disabled by default; construction performs no
file/network IO. Opaque same-origin paths expose no original source query.
"""
from collections import OrderedDict,deque
from collections.abc import Mapping
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import base64
import math
import re
import threading
import time
from urllib.parse import parse_qsl,urlsplit

from flask import Blueprint,Response,current_app
from PIL import Image

ASSET=re.compile(r'[a-f0-9]{64}\.(?:jpg|png|pdf)')
MAX_PDF=4*1024*1024
MAX_IMAGE=1024*1024
SHEET=re.compile(r'/fiches/tyres/Fiche_([1-9][0-9]{0,11})_([A-Z]{2})\.pdf')
QR=re.compile(r'/(?:qr/|screen/product/tyres/)([1-9][0-9]{0,11})')
CONTI=re.compile(r'/eulabel-download-pis-pdf/de_DE/([0-9]{11})/')
TYRE_IDENTITY={'manufacturer','width_mm','aspect_ratio_percent','diameter_inch','load_index','speed_index'}


class DocumentError(ValueError):
    pass


def _url(value):
    if (not isinstance(value,str) or not 1<=len(value)<=2000 or '\\' in value
            or any(ord(c)<33 or ord(c)==127 for c in value)
            or re.search(r'%(?![0-9A-Fa-f]{2})',value)):
        raise DocumentError('invalid_document_release')
    try:
        parsed=urlsplit(value)
        if (parsed.scheme!='https' or parsed.username or parsed.password or parsed.port not in (None,443)
                or parsed.fragment):raise ValueError()
        return parsed
    except (ValueError,TypeError):raise DocumentError('invalid_document_release') from None


def _qr_id(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_+/=-]{1,300}',value):
        raise DocumentError('invalid_document_release')
    try:
        decoded=base64.b64decode(value+'='*((-len(value))%4),altchars=b'-_',validate=True).decode('ascii')
        target=_url(decoded)
        match=QR.fullmatch(target.path)
        if target.hostname!='eprel.ec.europa.eu' or target.query or not match:raise ValueError()
        return match[1]
    except Exception:raise DocumentError('invalid_document_release') from None


def document_eprel_id(url):
    parsed=_url(url)
    if parsed.hostname=='www.contimediacenter.com':
        match=CONTI.fullmatch(parsed.path)
        if parsed.query or not match or int(match[1])==0:raise DocumentError('invalid_document_release')
        # This is the original manufacturer article number, not an EPREL id.
        # A reviewed source-chain proof must bind the latter separately.
        return None
    if parsed.hostname=='eprel.ec.europa.eu':
        match=SHEET.fullmatch(parsed.path)
        if parsed.query or not match:raise DocumentError('invalid_document_release')
        return match[1]
    if (not re.fullmatch(r'media[1-5]\.tyre-shopping\.com',parsed.hostname or '')
            or not re.fullmatch(r'/tyrelabel2/[A-Za-z0-9/_.-]{1,350}\.(?:jpg|png)',parsed.path)
            or any(part in {'.','..'} for part in parsed.path.split('/'))):
        raise DocumentError('invalid_document_release')
    try:params=parse_qsl(parsed.query,keep_blank_values=True,strict_parsing=True,max_num_fields=4)
    except ValueError:raise DocumentError('invalid_document_release') from None
    if len(params)!=4 or {key for key,_ in params}!={'manufacturer','model','size','qr'}:
        raise DocumentError('invalid_document_release')
    for key,value in params:
        if (not value or len(value)>(300 if key=='qr' else 200)
                or any(ord(c)<32 or ord(c)==127 for c in value)):
            raise DocumentError('invalid_document_release')
    return _qr_id(dict(params)['qr'])


def document_asset(url):
    document_eprel_id(url)
    suffix=(urlsplit(url).path.rsplit('.',1)[1] if re.fullmatch(r'media[1-5]\.tyre-shopping\.com',urlsplit(url).hostname or '') else 'pdf')
    return sha256(url.encode('utf-8')).hexdigest()+'.'+suffix


def _tyre_identity(value):
    if (not isinstance(value,Mapping) or set(value)!=TYRE_IDENTITY
            or any(not isinstance(v,str) for v in value.values())
            or not 1<=len(value['manufacturer'])<=90 or any(ord(c)<32 for c in value['manufacturer'])
            or any(not re.fullmatch(r'[0-9]{2,3}',value[k]) for k in ('width_mm','load_index'))
            or any(not re.fullmatch(r'[0-9]{1,3}',value[k]) for k in ('aspect_ratio_percent','diameter_inch'))
            or not re.fullmatch(r'[A-Za-z]',value['speed_index'])):
        raise DocumentError('invalid_document_release')
    if (not 100<=int(value['width_mm'])<=455 or not 15<=int(value['aspect_ratio_percent'])<=100
            or not 10<=int(value['diameter_inch'])<=30 or not 30<=int(value['load_index'])<=150):
        raise DocumentError('invalid_document_release')
    return dict(value)


class ReleasedProductDocuments:
    def __init__(self,resources,*,approved_assets,enabled=False,allow_network=False,local_root=None,http=None,
                 max_cache_bytes=32*1024*1024,max_network_reads_per_period=0,period_seconds=3600,
                 spacing_seconds=2,max_concurrent_reads=1,monotonic=None):
        if (not isinstance(resources,Mapping) or len(resources)>100_000 or not isinstance(approved_assets,(set,frozenset))
                or type(enabled) is not bool or type(allow_network) is not bool
                or type(max_cache_bytes) is not int or not MAX_PDF<=max_cache_bytes<=64*1024*1024
                or type(max_network_reads_per_period) is not int or not 0<=max_network_reads_per_period<=10_000
                or type(period_seconds) not in (int,float) or not 1<=period_seconds<=2_678_400
                or type(spacing_seconds) not in (int,float) or not .1<=spacing_seconds<=3600
                or type(max_concurrent_reads) is not int or not 1<=max_concurrent_reads<=2):
            raise DocumentError('invalid_document_release')
        root=Path(local_root) if local_root is not None else None
        if root is not None and (not root.is_absolute() or '..' in root.parts):raise DocumentError('invalid_document_release')
        selected={}
        for asset in approved_assets:
            if not isinstance(asset,str) or not ASSET.fullmatch(asset) or asset not in resources:raise DocumentError('invalid_document_release')
            source=resources[asset]
            if isinstance(source,Mapping) and set(source)=={'kind','file','sha256'}:
                # Previously reviewed original rim reports are installed as
                # hash-named private files. They have no network fallback.
                if (source.get('kind')!='wheel_report' or root is None or not asset.endswith('.pdf')
                        or source.get('file')!=asset or source.get('sha256')!=asset[:-4]):
                    raise DocumentError('invalid_document_release')
                selected[asset]={'file':root/asset,'sha256':source['sha256'],'wheel_report':True}
                continue
            if not isinstance(source,Mapping) or 'url' not in source or document_asset(source['url'])!=asset:
                raise DocumentError('invalid_document_release')
            expected={'url'}
            identity=None
            if urlsplit(source['url']).hostname=='www.contimediacenter.com':
                identity=_tyre_identity(source.get('tyre_identity'));expected.add('tyre_identity')
            if set(source)==expected:selected[asset]={'url':source['url']}
            elif set(source)==expected|{'file','sha256'} and root is not None:
                name=Path(source['file'])
                if (name.is_absolute() or '..' in name.parts or not name.parts or name==Path('.')
                        or not isinstance(source['sha256'],str) or not re.fullmatch(r'[a-f0-9]{64}',source['sha256'])):
                    raise DocumentError('invalid_document_release')
                selected[asset]={'url':source['url'],'file':root/name,'sha256':source['sha256']}
            else:raise DocumentError('invalid_document_release')
            if identity is not None:selected[asset]['tyre_identity']=identity
        self.resources,self.enabled,self.allow_network=selected,enabled,allow_network
        self.local_root,self.http=root,http
        self.maximum,self.budget,self.period,self.spacing=max_cache_bytes,max_network_reads_per_period,period_seconds,spacing_seconds
        self.cache,self.failures,self.attempts=OrderedDict(),OrderedDict(),deque()
        self.used,self.next_read,self.last_mono=0,0.,None
        self.loading=set()
        self.lock=threading.RLock()
        self.slots=threading.BoundedSemaphore(max_concurrent_reads)
        self.monotonic=monotonic or time.monotonic

    def __repr__(self):return '<ReleasedProductDocuments exact_source_only no_credentials>'

    def _time(self):
        now=self.monotonic()
        if type(now) not in (int,float) or not math.isfinite(now) or self.last_mono is not None and now<self.last_mono:
            self.enabled=False
            raise DocumentError('document_unavailable')
        self.last_mono=now
        return now

    @staticmethod
    def _validate(data,asset,*,wheel_report=False):
        maximum=MAX_PDF if asset.endswith('.pdf') else MAX_IMAGE
        if not isinstance(data,bytes) or not 0<len(data)<=maximum:raise DocumentError('document_unavailable')
        try:
            if asset.endswith('.pdf'):
                if not re.match(rb'%PDF-(?:1\.[0-7]|2\.0)\b',data[:16]) or b'%%EOF' not in data[-1024:]:raise ValueError()
                from pypdf import PdfReader
                reader=PdfReader(BytesIO(data),strict=True)
                if reader.is_encrypted or not 1<=len(reader.pages)<=(300 if wheel_report else 20):raise ValueError()
                # Product sheets have no executable/document-attachment role.
                if re.search(rb'/(?:JavaScript|JS|Launch|EmbeddedFile|RichMedia|XFA)\b',data):raise ValueError()
                return data,'application/pdf'
            with Image.open(BytesIO(data)) as image:
                expected='JPEG' if asset.endswith('.jpg') else 'PNG'
                if (image.format!=expected or not 1<=image.width<=4096 or not 1<=image.height<=4096
                        or image.width*image.height>8_000_000):raise ValueError()
                image.verify()
            return data,'image/jpeg' if expected=='JPEG' else 'image/png'
        except Exception:raise DocumentError('document_unavailable') from None

    def _read(self,asset,source):
        maximum=MAX_PDF if asset.endswith('.pdf') else MAX_IMAGE
        if 'file' in source:
            path=source['file']
            root=self.local_root
            resolved=path.resolve()
            if (root is None or root.is_symlink() or path.is_symlink() or not resolved.is_relative_to(root.resolve())
                    or not path.is_file() or path.stat().st_size>maximum):raise DocumentError('document_unavailable')
            data=path.read_bytes()
            if sha256(data).hexdigest()!=source['sha256']:raise DocumentError('document_unavailable')
            return self._validate(data,asset,wheel_report=source.get('wheel_report') is True)
        response=owned=None
        try:
            if self.http is None:
                import requests
                owned=requests.Session()
                owned.trust_env=False
                owned.mount('https://',requests.adapters.HTTPAdapter(max_retries=0))
            http=self.http if self.http is not None else owned
            expected='application/pdf' if asset.endswith('.pdf') else 'image/jpeg' if asset.endswith('.jpg') else 'image/png'
            response=http.get(source['url'],timeout=(3.05,8),verify=True,allow_redirects=False,stream=True,
                headers={'Accept':expected,'Accept-Encoding':'identity'})
            if response.status_code!=200 or response.url!=source['url']:raise ValueError()
            declared=response.headers.get('Content-Type','').split(';',1)[0].strip().lower()
            if declared=='image/jpg':declared='image/jpeg'
            if declared!=expected:raise ValueError()
            size=response.headers.get('Content-Length')
            if size is not None and (not re.fullmatch(r'[0-9]{1,10}',size) or int(size)>maximum):raise ValueError()
            body=bytearray();deadline=self.monotonic()+15
            for chunk in response.iter_content(chunk_size=8192):
                if not isinstance(chunk,bytes) or len(body)+len(chunk)>maximum or self.monotonic()>deadline:raise ValueError()
                body.extend(chunk)
            return self._validate(bytes(body),asset)
        except Exception:raise DocumentError('document_unavailable') from None
        finally:
            if response is not None:response.close()
            if owned is not None:owned.close()

    @staticmethod
    def _validate_tyre_identity(data,identity):
        """Manufacturer download URLs have article IDs, never invented EPREL IDs.

        Their exact reviewed source/page/anchor binding is outside this service;
        before serving, independently reject a PDF for another technical type.
        """
        try:
            from pypdf import PdfReader
            text=' '.join(page.extract_text() or '' for page in PdfReader(BytesIO(data),strict=True).pages)
            compact=re.sub(r'\s+','',text).casefold()
            size=identity['width_mm']+'/'+identity['aspect_ratio_percent']+'r'+identity['diameter_inch']
            if (' '.join(identity['manufacturer'].casefold().split()) not in ' '.join(text.casefold().split())
                    or size not in compact
                    or not re.search(r'(?<![0-9])'+re.escape(identity['load_index'])+r'(?![0-9])',text)
                    or not re.search(r'(?<![A-Za-z])'+re.escape(identity['speed_index'])+r'(?![A-Za-z])',text,re.I)):
                raise ValueError()
        except Exception:raise DocumentError('document_identity_unconfirmed') from None

    def read(self,asset):
        if not self.enabled or not isinstance(asset,str) or not ASSET.fullmatch(asset) or asset not in self.resources:
            raise DocumentError('document_not_found')
        remote='file' not in self.resources[asset]
        with self.lock:
            now=self._time()
            if asset in self.cache:
                self.cache.move_to_end(asset)
                return self.cache[asset]
            if asset in self.loading or self.failures.get(asset,-float('inf'))+30>now:raise DocumentError('document_busy')
            if remote:
                while self.attempts and self.attempts[0]<=now-self.period:self.attempts.popleft()
                if not self.allow_network or self.budget<=len(self.attempts) or now<self.next_read:
                    raise DocumentError('document_unavailable')
                if not self.slots.acquire(blocking=False):raise DocumentError('document_busy')
                self.attempts.append(now);self.next_read=now+self.spacing
            self.loading.add(asset)
        try:
            result=self._read(asset,self.resources[asset])
            if 'tyre_identity' in self.resources[asset]:
                self._validate_tyre_identity(result[0],self.resources[asset]['tyre_identity'])
            with self.lock:
                self._time()
                while self.cache and self.used+len(result[0])>self.maximum:
                    _,old=self.cache.popitem(last=False);self.used-=len(old[0])
                self.cache[asset]=result;self.used+=len(result[0]);self.failures.pop(asset,None)
            return result
        except Exception:
            with self.lock:
                self.failures[asset]=self.monotonic()
                while len(self.failures)>1024:self.failures.popitem(last=False)
            raise DocumentError('document_unavailable') from None
        finally:
            with self.lock:self.loading.discard(asset)
            if remote:self.slots.release()


def create_shop_documents_blueprint(documents=None):
    blueprint=Blueprint('shop_documents',__name__,url_prefix='/shop/documents')
    @blueprint.get('/<string:asset>')
    def document(asset):
        if current_app.config.get('SHOP_DOCUMENTS_ENABLED') is not True or documents is None:return Response(status=404)
        try:
            data,mime=documents.read(asset)
            response=Response(data,content_type=mime)
            response.headers['Cache-Control']='public, max-age=3600'
            response.headers['Content-Disposition']='inline; filename="'+asset+'"'
            response.headers['Content-Security-Policy']="default-src 'none'; sandbox"
            response.headers['X-Content-Type-Options']='nosniff'
            response.headers['Referrer-Policy']='no-referrer'
            response.set_etag(sha256(data).hexdigest())
            return response
        except DocumentError as error:
            response=Response(status=404 if str(error)=='document_not_found' else 503)
            response.headers['Cache-Control']='no-store'
            return response
    return blueprint
