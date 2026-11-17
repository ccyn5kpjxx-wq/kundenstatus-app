"""Released product images, opaque same-origin paths and bounded read cache.

Only explicitly released source URLs or hash-checked local files can be read.
No caller URL, vendor credential, upload, image alteration or disk cache. The
original supplier image/watermark is returned unchanged after format validation.
"""
from collections import OrderedDict
from collections.abc import Mapping
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
import threading
import time
from urllib.parse import urlsplit

from flask import Blueprint, Response, current_app
from PIL import Image

ASSET = re.compile(r'[a-f0-9]{64}\.(?:jpg|jpeg|png|webp)')
# Original parts photos from the licensed catalogue. Only the observed CDN,
# two path components and literal/explicitly observed filename escapes qualify.
# In particular, encoded separators, dots and double encoding cannot pass.
ALZURA_PART_IMAGE_URL = re.compile(
    r'https://cdn01\.alzura\.com/[0-9]{4}_PIC/'
    r'(?:[A-Za-z0-9_.-]|%20|%23|%28|%29){1,160}\.(?:JPG|PNG)')
MIME = {'JPEG': 'image/jpeg', 'PNG': 'image/png', 'WEBP': 'image/webp'}
MAX_IMAGE = 1024 * 1024
MAX_PIXELS = 4_000_000
MAX_PART_IMAGE_PIXELS = 16_000_000


class MediaError(ValueError):
    pass


class ReleasedMedia:
    def __init__(self, resources, *, approved_assets, local_root=None, allow_network=False,
                 http=None, max_cache_bytes=32 * 1024 * 1024, monotonic=None, max_network_reads=1):
        if (not isinstance(resources, Mapping) or not isinstance(approved_assets, (set, frozenset))
                or len(resources) > 100_000 or type(allow_network) is not bool
                or type(max_cache_bytes) is not int or not MAX_IMAGE <= max_cache_bytes <= 128 * 1024 * 1024):
            raise MediaError('invalid_media_release')
        if type(max_network_reads) is not int or not 1 <= max_network_reads <= 3:
            raise MediaError('invalid_media_release')
        self.resources = {}
        root = Path(local_root).resolve() if local_root is not None else None
        for asset in approved_assets:
            if not isinstance(asset, str) or not ASSET.fullmatch(asset) or asset not in resources:
                raise MediaError('invalid_media_release')
            source = resources[asset]
            if not isinstance(source, Mapping):
                raise MediaError('invalid_media_release')
            if set(source) == {'url'}:
                url = source['url']
                try:
                    parsed = urlsplit(url)
                    valid = (isinstance(url, str) and len(url) <= 2000 and '\\' not in url
                             and not any(ord(c) < 33 for c in url) and parsed.scheme == 'https'
                             and not parsed.username and not parsed.password
                             and not parsed.query and not parsed.fragment
                             and ((re.fullmatch(r'media[1-5]\.tyre-shopping\.com', parsed.hostname or '')
                                   and parsed.port in (None, 443) and parsed.path.startswith('/'))
                                  or ALZURA_PART_IMAGE_URL.fullmatch(url)))
                except (TypeError, ValueError):
                    valid = False
                if not valid:
                    raise MediaError('invalid_media_release')
                self.resources[asset] = {'url': url}
            elif set(source) == {'file', 'sha256'} and root is not None:
                path = Path(source['file'])
                if not path.is_absolute():
                    path = root / path
                resolved = path.resolve()
                if (path.is_symlink() or not resolved.is_relative_to(root) or resolved == root
                        or not isinstance(source['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', source['sha256'])):
                    raise MediaError('invalid_media_release')
                self.resources[asset] = {'file': resolved, 'sha256': source['sha256']}
            else:
                raise MediaError('invalid_media_release')
        self.allow_network, self.http = allow_network, http
        self.maximum, self.used = max_cache_bytes, 0
        self.cache, self.failures = OrderedDict(), {}
        self.lock = threading.RLock()
        self.loading = set()
        self.network_slot = threading.BoundedSemaphore(max_network_reads)
        self.monotonic = monotonic or time.monotonic

    @staticmethod
    def _validate(data, *, max_pixels=MAX_PIXELS):
        if (not isinstance(data, bytes) or not 0 < len(data) <= MAX_IMAGE
                or type(max_pixels) is not int or max_pixels not in (MAX_PIXELS, MAX_PART_IMAGE_PIXELS)):
            raise MediaError('image_unavailable')
        try:
            with Image.open(BytesIO(data)) as picture:
                mime = MIME.get(picture.format)
                if (mime is None or picture.width > 4096 or picture.height > 4096
                        or picture.width * picture.height > max_pixels):
                    raise ValueError()
                picture.verify()
            return data, mime
        except Exception:
            raise MediaError('image_unavailable') from None

    def _read(self, source):
        if 'file' in source:
            path = source['file']
            if not path.is_file() or path.stat().st_size > MAX_IMAGE:
                raise MediaError('image_unavailable')
            data = path.read_bytes()
            if sha256(data).hexdigest() != source['sha256']:
                raise MediaError('image_unavailable')
            return self._validate(data)
        if not self.allow_network:
            raise MediaError('image_unavailable')
        response = owned_http = None
        try:
            if self.http is None:
                import requests
                owned_http = requests.Session()
                owned_http.trust_env = False
                owned_http.mount('https://', requests.adapters.HTTPAdapter(max_retries=0))
            http = self.http if self.http is not None else owned_http
            response = http.get(source['url'], timeout=(3.05, 8), allow_redirects=False,
                                     stream=True, verify=True, headers={'Accept': 'image/jpeg,image/png,image/webp'})
            if response.status_code != 200 or response.url != source['url']:
                raise MediaError('image_unavailable')
            declared = response.headers.get('Content-Type', '').split(';')[0].strip().lower()
            if declared == 'image/jpg':
                # This spelling is returned by the reviewed supplier CDN;
                # byte-level Pillow verification still proves JPEG below.
                declared = 'image/jpeg'
            if declared not in {'image/jpeg', 'image/png', 'image/webp'}:
                raise MediaError('image_unavailable')
            content_length = response.headers.get('Content-Length')
            if content_length is not None and (not re.fullmatch(r'[0-9]{1,9}', content_length) or int(content_length) > MAX_IMAGE):
                raise MediaError('image_unavailable')
            data, deadline = bytearray(), self.monotonic() + 15
            for chunk in response.iter_content(chunk_size=8192):
                data.extend(chunk)
                if len(data) > MAX_IMAGE or self.monotonic() > deadline:
                    raise MediaError('image_unavailable')
            # Reviewed parts originals include 2500x2500 photos. The larger
            # pixel allowance is source-bound; byte and edge limits stay fixed.
            max_pixels = MAX_PART_IMAGE_PIXELS if ALZURA_PART_IMAGE_URL.fullmatch(source['url']) else MAX_PIXELS
            result = self._validate(bytes(data), max_pixels=max_pixels)
            if result[1] != declared:
                raise MediaError('image_unavailable')
            return result
        except Exception:
            raise MediaError('image_unavailable') from None
        finally:
            if response is not None:
                response.close()
            if owned_http is not None:
                owned_http.close()

    def read(self, asset):
        if not isinstance(asset, str) or not ASSET.fullmatch(asset) or asset not in self.resources:
            raise MediaError('image_not_found')
        with self.lock:
            if asset in self.cache:
                self.cache.move_to_end(asset)
                return self.cache[asset]
            if self.failures.get(asset, -float('inf')) + 30 > self.monotonic():
                raise MediaError('image_unavailable')
            if asset in self.loading:
                raise MediaError('image_busy')
            remote = 'url' in self.resources[asset]
            # A cold CDN image must not occupy every portal HTTP thread while
            # they wait on a global lock. Busy callers can retry a bounded time.
            if remote and not self.network_slot.acquire(blocking=False):
                raise MediaError('image_busy')
            self.loading.add(asset)
        try:
            result = self._read(self.resources[asset])
        except Exception:
            with self.lock:
                self.failures[asset] = self.monotonic()
                if len(self.failures) > 1024:
                    self.failures.pop(next(iter(self.failures)))
            raise MediaError('image_unavailable') from None
        else:
            with self.lock:
                while self.cache and self.used + len(result[0]) > self.maximum:
                    _, removed = self.cache.popitem(last=False)
                    self.used -= len(removed[0])
                self.cache[asset] = result
                self.used += len(result[0])
                self.failures.pop(asset, None)
                return result
        finally:
            with self.lock:
                self.loading.discard(asset)
            if remote:
                self.network_slot.release()


def create_shop_media_blueprint(media=None):
    bp = Blueprint('shop_media_v2', __name__, url_prefix='/shop/media')

    @bp.get('/<string:asset>')
    def image(asset):
        if current_app.config.get('SHOP_MEDIA_ENABLED') is not True or media is None:
            return Response(status=404)
        try:
            data, mime = media.read(asset)
            response = Response(data, content_type=mime)
            response.headers['Cache-Control'] = 'public, max-age=3600'
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.set_etag(sha256(data).hexdigest())
            return response
        except MediaError as error:
            response = Response(status=404 if str(error) == 'image_not_found' else 503)
            response.headers['Cache-Control'] = 'no-store'
            return response

    return bp
