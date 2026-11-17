"""Disabled bootstrap safety without importing the real portal or its workers."""
import ast
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask, abort, redirect, request
from shop_runtime import FLAGS, register_shop_runtime


class ShopRuntimeChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.portal = SimpleNamespace(app=Flask(__name__), get_db=Mock(side_effect=AssertionError('No DB on startup')))

    def tearDown(self):
        self.network.stop()

    def test_default_registration_has_no_io_and_all_new_routes_are_off(self):
        state = register_shop_runtime(self.portal)
        self.assertTrue(all(value is False for value in state['enabled'].values()))
        self.portal.get_db.assert_not_called()
        client = self.portal.app.test_client()
        for path in ('/shop/api/v2/products', '/shop/api/v2/orders/sho_' + 'a' * 32):
            self.assertEqual(client.get(path).status_code, 404)
        self.assertEqual(client.post('/shop/api/v2/cart/quote', json={}).status_code, 404)

    def test_flags_cannot_enable_missing_production_adapters_or_inherit_portal_db(self):
        self.portal.app.config.update({key: True for key in FLAGS})
        state = register_shop_runtime(self.portal)
        self.assertEqual(state['missing_dependencies'], list(FLAGS))
        self.assertTrue(all(value is False for value in state['enabled'].values()))
        self.portal.get_db.assert_not_called()

    def test_browse_can_be_enabled_without_any_quote_provider(self):
        catalogue = Mock()
        catalogue.page.return_value = {'items': [], 'page': 1, 'total': 0}
        self.portal.app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        state = register_shop_runtime(self.portal, catalogue=catalogue)
        self.assertTrue(state['enabled']['SHOP_PUBLIC_CATALOGUE_ENABLED'])
        self.assertFalse(state['enabled']['SHOP_CART_QUOTE_ENABLED'])
        self.assertEqual(self.portal.app.test_client().get('/shop/api/v2/products').status_code, 200)
        self.assertEqual(self.portal.app.test_client().post('/shop/api/v2/orders', json={}).status_code, 404)

    def test_released_media_needs_both_its_explicit_flag_and_catalogue(self):
        asset = 'a' * 64 + '.png'
        media = Mock()
        media.read.return_value = (b'SYNTHETIC_IMAGE_BYTES', 'image/png')
        self.portal.app.config['SHOP_MEDIA_ENABLED'] = True
        state = register_shop_runtime(self.portal, media_service=media)
        self.assertFalse(state['enabled']['SHOP_MEDIA_ENABLED'])
        self.assertEqual(self.portal.app.test_client().get('/shop/media/' + asset).status_code, 404)
        media.read.assert_not_called()
        other = SimpleNamespace(app=Flask('released-media'))
        other.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_MEDIA_ENABLED=True)
        state = register_shop_runtime(other, catalogue=Mock(), media_service=media)
        self.assertTrue(state['enabled']['SHOP_MEDIA_ENABLED'])
        self.assertEqual(other.app.test_client().get('/shop/media/' + asset).data, b'SYNTHETIC_IMAGE_BYTES')

    def test_strict_flags_and_sequential_prerequisites_prevent_partial_activation(self):
        self.portal.app.config.update({key: 'true' for key in FLAGS})
        state = register_shop_runtime(self.portal, catalogue=Mock(), order_store=Mock(), checkout_service=Mock())
        self.assertTrue(all(value is False for value in state['enabled'].values()))
        with self.assertRaises(ValueError):
            register_shop_runtime(self.portal)

    def test_app_syntax_and_explicit_disabled_registration_are_present_without_app_import(self):
        tree = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == 'register_shop_runtime']
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(calls[0].keywords), 1)
        self.assertIsNone(calls[0].keywords[0].arg)
        self.assertIsInstance(calls[0].keywords[0].value, ast.Call)
        self.assertEqual(calls[0].keywords[0].value.func.id, 'prepare_shop_runtime')

    def test_real_public_host_gate_separates_stateless_reads_from_persistent_v2_handlers(self):
        # Exercise the actual gate without starting portal workers/databases.
        tree = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == 'restrict_public_site_service')
        function.decorator_list = []
        module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
        namespace = {'request': request, 'abort': abort, 'redirect': redirect,
                     'is_public_site_request': lambda: True, 'PUBLIC_SITE_ONLY': True,
                     'PORTAL_BASE_URL': ''}
        exec(compile(module, str(ROOT / 'app.py'), 'exec'), namespace)
        app = Flask('isolated-public-gate')
        app.before_request(namespace['restrict_public_site_service'])
        handlers = ('products', 'product', 'cart_quote', 'create_order_draft',
                    'order_status', 'start_checkout', 'stripe_webhook')
        for name in handlers:
            methods = ['GET'] if name in {'products', 'product', 'order_status'} else ['POST']
            app.add_url_rule('/' + name, 'shop_public_v2.' + name, lambda: ('', 204), methods=methods)
        app.add_url_rule('/admin', 'internal_admin', lambda: ('', 204))
        app.add_url_rule('/media', 'shop_media_v2.image', lambda: ('', 204))
        client = app.test_client()
        self.assertEqual(client.get('/products').status_code, 204)
        self.assertEqual(client.get('/product').status_code, 204)
        self.assertEqual(client.get('/media').status_code, 204)
        self.assertEqual(client.get('/order_status').status_code, 404)
        for name in ('cart_quote', 'create_order_draft', 'start_checkout', 'stripe_webhook'):
            self.assertEqual(client.post('/' + name).status_code, 404)
        namespace['PUBLIC_SITE_ONLY'] = False
        self.assertEqual(client.get('/order_status').status_code, 204)
        for name in ('cart_quote', 'create_order_draft', 'start_checkout', 'stripe_webhook'):
            self.assertEqual(client.post('/' + name).status_code, 204)
        self.assertEqual(client.get('/admin').status_code, 404)


if __name__ == '__main__':
    unittest.main()
