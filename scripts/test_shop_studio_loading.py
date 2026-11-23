"""Exercise actual viewport clipping, selected-design priority and recovery offline."""
from io import BytesIO
import os
from pathlib import Path
import sys
import unittest
from urllib.parse import urlsplit

from flask import Flask
from PIL import Image
try:
    from playwright.sync_api import expect, sync_playwright
except ImportError:
    sync_playwright = None

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shop_frontend import create_shop_frontend_blueprint


@unittest.skipUnless(sync_playwright, "Playwright unavailable")
class StudioLoading(unittest.TestCase):
    def test_selected_image_recovers_before_grid_downloads(self):
        app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
        app.register_blueprint(create_shop_frontend_blueprint())
        app.config['SHOP_FRONTEND_ENABLED'] = True
        client = app.test_client()
        face = dict(reviewed=True, width=160, height=160, cx=80, cy=80,
                    rx=68, ry=68, rotation=0, hub={'x':80, 'y':80})
        products = [dict(id='alloy_wheel:'+format(i, '024x'), name=f'Design {i}', brand='Test',
                         category='alloy_wheel', geometry={'diameter':'18'}, images=[f'/shop/media/rim{i}.png'],
                         photo_face=face) for i in range(12)]
        picture = BytesIO()
        Image.new('RGB', (160,160), 'silver').save(picture, format='PNG')
        requests = []

        def route(resource):
            url = urlsplit(resource.request.url)
            if resource.request.method != 'GET' or url.netloc != 'studio.test':
                return resource.abort()
            if url.path == '/shop/api/v2/products':
                return resource.fulfill(json=dict(items=products, page=1, total=len(products), has_more=False))
            if url.path.startswith('/shop/media/'):
                requests.append((url.path, url.query))
                if len(requests) == 1:
                    return resource.fulfill(status=503, headers={'Cache-Control':'no-store'})
                return resource.fulfill(body=picture.getvalue(), content_type='image/png')
            with client.get(url.path) as response:
                return resource.fulfill(status=response.status_code, body=response.data, content_type=response.content_type)

        with sync_playwright() as pw:
            executable = os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH', pw.chromium.executable_path)
            if not Path(executable).is_file():
                self.skipTest('Chromium unavailable')
            with pw.chromium.launch(executable_path=executable) as browser:
                page = browser.new_page(viewport={'width':390, 'height':600})
                page.route('**/*', route)
                page.goto('http://studio.test/shop/felgenstudio/')
                expect(page.locator('#ws-status')).to_contain_text('Felgenansicht geladen')
                self.assertEqual(requests[:2], [('/shop/media/rim0.png', ''), ('/shop/media/rim0.png', 'shop_retry=1')])
                self.assertNotIn('/shop/media/rim11.png', [path for path, _ in requests])
                last = page.get_by_role('button', name='Design 11 am Auto ansehen')
                last.scroll_into_view_if_needed()
                expect(last.locator('img')).to_have_js_property('naturalWidth', 160)
                last.click()
                expect(page.locator('#ws-look-name')).to_have_text('Design 11')
                expect(page.locator('#ws-status')).to_contain_text('Felgenansicht geladen')
