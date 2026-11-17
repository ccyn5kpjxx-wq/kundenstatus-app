"""Released image boundary, unchanged synthetic bytes and fake CDN only."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from PIL import Image
from flask import Flask
from shop_media import ReleasedMedia, MediaError, create_shop_media_blueprint


class MediaChecks(unittest.TestCase):
    def setUp(self):
        stream = BytesIO()
        Image.new('RGB', (2, 2), 'black').save(stream, 'PNG')
        self.data = stream.getvalue()
        self.asset = sha256(self.data).hexdigest() + '.png'

    def test_local_hash_approval_and_original_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'synthetic.png'
            path.write_bytes(self.data)
            resources = {self.asset: {'file': 'synthetic.png', 'sha256': sha256(self.data).hexdigest()}}
            media = ReleasedMedia(resources, approved_assets={self.asset}, local_root=directory)
            self.assertEqual(media.read(self.asset), (self.data, 'image/png'))
            with self.assertRaises(MediaError):
                ReleasedMedia(resources, approved_assets=set(), local_root=directory).read(self.asset)
            resources[self.asset]['sha256'] = '0' * 64
            with self.assertRaises(MediaError):
                ReleasedMedia(resources, approved_assets={self.asset}, local_root=directory).read(self.asset)

    def test_arbitrary_urls_credentials_query_redirect_sources_and_path_escape_are_rejected(self):
        for url in ('http://media1.tyre-shopping.com/a.png', 'https://attacker.test/a.png',
                    'https://media1.tyre-shopping.com/a.png?token=PRIVATE',
                    'https://user:PRIVATE@media1.tyre-shopping.com/a.png'):
            with self.assertRaises(MediaError):
                ReleasedMedia({self.asset: {'url': url}}, approved_assets={self.asset})
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(MediaError):
                ReleasedMedia({self.asset: {'file': '../other.png', 'sha256': 'a' * 64}},
                              approved_assets={self.asset}, local_root=directory)

    def test_network_off_by_default_and_enabled_transport_is_fixed_tls_no_redirect_no_auth(self):
        url = 'https://media1.tyre-shopping.com/synthetic.png'
        http = Mock()
        response = SimpleNamespace(status_code=200, url=url, headers={'Content-Type': 'image/png'},
                                   iter_content=lambda **kwargs: [self.data], close=Mock())
        http.get.return_value = response
        resources = {self.asset: {'url': url}}
        with self.assertRaises(MediaError):
            ReleasedMedia(resources, approved_assets={self.asset}, http=http).read(self.asset)
        http.get.assert_not_called()
        media = ReleasedMedia(resources, approved_assets={self.asset}, http=http, allow_network=True)
        self.assertEqual(media.read(self.asset)[0], self.data)
        self.assertEqual(media.read(self.asset)[0], self.data)
        self.assertEqual(http.get.call_count, 1)
        self.assertIs(http.get.call_args.kwargs['verify'], True)
        self.assertIs(http.get.call_args.kwargs['allow_redirects'], False)
        self.assertNotIn('auth', http.get.call_args.kwargs)
        response.close.assert_called_once()

    def test_html_svg_and_invalid_image_payload_never_served_as_image(self):
        for data in (b'<html>PRIVATE</html>', b'<svg></svg>', b'not an image'):
            with self.assertRaises(MediaError):
                ReleasedMedia._validate(data)

    def test_slow_cdn_does_not_block_cached_local_or_second_http_thread(self):
        started, finish = threading.Event(), threading.Event()
        remote = 'b' * 64 + '.png'
        other = 'c' * 64 + '.png'
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'local.png').write_bytes(self.data)
            resources = {
                self.asset: {'file': 'local.png', 'sha256': sha256(self.data).hexdigest()},
                remote: {'url': 'https://media1.tyre-shopping.com/remote.png'},
                other: {'url': 'https://media1.tyre-shopping.com/other.png'},
            }
            media = ReleasedMedia(resources, approved_assets=set(resources), local_root=directory,
                                  allow_network=True)
            original = media._read

            def controlled(source):
                if 'url' in source:
                    started.set()
                    if not finish.wait(2):
                        raise AssertionError('offline fixture was not released')
                    return self.data, 'image/png'
                return original(source)

            media._read = controlled
            self.assertEqual(media.read(self.asset)[0], self.data)
            worker = threading.Thread(target=media.read, args=(remote,))
            worker.start()
            try:
                self.assertTrue(started.wait(1))
                self.assertEqual(media.read(self.asset)[0], self.data)
                for asset in (remote, other):
                    with self.assertRaisesRegex(MediaError, 'image_busy'):
                        media.read(asset)
                    self.assertNotIn(asset, media.failures)
            finally:
                finish.set()
                worker.join(2)
            self.assertFalse(worker.is_alive())
            self.assertEqual(media.read(other)[0], self.data)

    def test_http_default_off_and_only_released_opaque_asset_responds(self):
        media = Mock()
        media.read.return_value = (self.data, 'image/png')
        app = Flask(__name__)
        app.register_blueprint(create_shop_media_blueprint(media))
        client = app.test_client()
        path = '/shop/media/' + self.asset
        self.assertEqual(client.get(path).status_code, 404)
        media.read.assert_not_called()
        app.config['SHOP_MEDIA_ENABLED'] = True
        result = client.get(path)
        self.assertEqual(result.data, self.data)
        self.assertEqual(result.content_type, 'image/png')
        self.assertEqual(result.headers['X-Content-Type-Options'], 'nosniff')


if __name__ == '__main__':
    unittest.main()
