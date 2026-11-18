"""Synthetic release/bootstrap boundaries; temporary files and no network."""
import ast
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask, abort, redirect, request
from PIL import Image
from shop_catalog_public import variant_fingerprint
from shop_media import MediaError
from shop_production import (ProductionError, _json, _private_file, _service_quote,
                             existing_shop_backup_storage, prepare_shop_runtime)
from shop_runtime import register_shop_runtime
from shop_storage import ShopStorage, StorageError
from test_shop_catalog_public import PHOTO, wheel

FLAGS = ('SHOP_PUBLIC_CATALOGUE_ENABLED', 'SHOP_FRONTEND_ENABLED', 'SHOP_MEDIA_ENABLED',
         'SHOP_BUYER_ENABLED', 'SHOP_CART_QUOTE_ENABLED', 'SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED',
         'SHOP_MAINLAND_DELIVERY_ENABLED')


def write_json(path, value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    path.write_bytes(data)
    return {'file': path.name, 'sha256': sha256(data).hexdigest()}


class ReleaseChecks(unittest.TestCase):
    def setUp(self):
        self.resources = ExitStack()
        self.addCleanup(self.resources.close)
        self.resources.enter_context(patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden')))
        self.resources.enter_context(patch('threading.Thread', side_effect=AssertionError('Background thread forbidden')))
        self.data = Path(self.resources.enter_context(tempfile.TemporaryDirectory())).resolve()
        self.root = self.data / 'shop-release'
        self.root.mkdir()
        (self.root / 'media').mkdir()
        self.env = {'DATA_DIR': str(self.data), 'SHOP_RELEASE_ENABLED': '1'}
        self.portal = SimpleNamespace(app=Flask(__name__, root_path=str(ROOT)),
                                      get_db=Mock(side_effect=AssertionError('Portal DB forbidden')))
        self.released = wheel(quote_public=None)
        self.hidden = wheel('b', name='UNRELEASED_PRIVATE_NAME', quote_public=None)
        image = BytesIO()
        Image.new('RGB', (2, 2), 'white').save(image, format='PNG')
        self.image = image.getvalue()
        self.asset = sha256(self.image).hexdigest() + '.png'
        (self.root / 'media' / self.asset).write_bytes(self.image)
        self.bundle = {'products': [self.released, self.hidden],
            'publication': {'released_ids': [self.released['selection_id']],
                            'image_urls': {PHOTO: '/shop/media/' + self.asset}},
            'media': {'resources': {self.asset: {'file': self.asset, 'sha256': sha256(self.image).hexdigest()}},
                      'approved_assets': [self.asset]},
            'private_evidence': 'PRIVATE_CANARY'}
        self.descriptor = {'schema_version': 1, 'publication_approved': True, 'mode': 'live',
                           'public_origin': 'https://shop.example.test', 'release_id': 'synthetic-release'}
        self.install()

    def install(self):
        self.descriptor['bundle'] = write_json(self.root / 'bundle.json', self.bundle)
        write_json(self.root / 'current.json', self.descriptor)

    def prepare(self, **kwargs):
        result = prepare_shop_runtime(self.portal, environ=self.env, background=False, **kwargs)
        cache = self.portal.app.extensions.get('shop_production', {}).get('price_cache')
        if cache:
            self.addCleanup(cache.stop)
        self.portal.get_db.assert_not_called()
        return result

    def assert_closed(self):
        self.assertTrue(all(self.portal.app.config.get(flag) is False for flag in FLAGS))
        self.assertEqual(self.portal.app.extensions['shop_production'],
                         {'ready': False, 'reason': 'release_configuration_incomplete'})

    def test_literal_enable_is_required_without_file_db_or_provider_access(self):
        for value in (None, True, 'true', '0', 'yes'):
            with self.subTest(value=value), patch('shop_production._json', side_effect=AssertionError('No file read')):
                self.portal.app.config['SHOP_MAINLAND_DELIVERY_ENABLED'] = True
                result = prepare_shop_runtime(self.portal, environ={'SHOP_RELEASE_ENABLED': value},
                    provider_factory=Mock(side_effect=AssertionError('No provider construction')))
                self.assertEqual(result, {})
                self.assertFalse(self.portal.app.config['SHOP_MAINLAND_DELIVERY_ENABLED'])
        self.assertFalse((self.data / 'shop-v2').exists())

    def test_recommendation_policy_and_source_document_chain_are_explicit_and_read_only(self):
        self.descriptor.update(tyre_recommendation_selection_approved=True,
            tyre_recommendation_sources={'path':'sources.json','sha256':'1'*64},
            tyre_recommendation_documents={'path':'documents.json','sha256':'2'*64})
        self.install()
        observed=[]
        def sources(root, record, bundle, **pins):
            observed.append(('sources',record,pins));return bundle
        def documents(root, record, bundle, resources, **pins):
            observed.append(('documents',record,pins));return resources
        with patch('shop_tyre_source_extension.load_tyre_source_extension',side_effect=sources), \
                patch('shop_recommendation_documents.local_recommendation_document_resources',side_effect=documents):
            dependencies=self.prepare()
        catalogue=dependencies['catalogue']
        self.assertTrue(catalogue._recommendation_ladder)
        self.assertEqual(catalogue._recommendation_document_ids,set())
        self.assertEqual([item[0] for item in observed],['sources','documents'])
        self.assertEqual(observed[0][2]['base_bundle_sha256'],self.descriptor['bundle']['sha256'])
        self.assertEqual(observed[1][2]['source_extension_sha256'],'1'*64)
        self.assertFalse(self.portal.app.config['SHOP_CHECKOUT_ENABLED'])
        self.assertFalse(self.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'])
        self.assertFalse((self.data/'shop-v2').exists())

    def test_invalid_recommendation_originals_close_release_before_database_or_provider(self):
        self.descriptor['tyre_recommendation_sources']={'path':'missing.json','sha256':'1'*64,
            'base_bundle_sha256':self.descriptor['bundle']['sha256']}
        self.install()
        factory=Mock(side_effect=AssertionError('No provider construction'))
        self.assertEqual(self.prepare(provider_factory=factory),{})
        self.assert_closed();factory.assert_not_called()
        self.assertFalse((self.data/'shop-v2').exists())

    def test_approved_browse_release_exposes_only_allowlisted_projection_and_original_image(self):
        dependencies = self.prepare()
        self.assertEqual(set(dependencies), {'catalogue', 'media_service', 'catalogue_warmup', 'feature_links'})
        self.assertEqual(dependencies['feature_links'], {})
        state = register_shop_runtime(self.portal, **dependencies)
        self.assertTrue(state['enabled']['SHOP_PUBLIC_CATALOGUE_ENABLED'])
        for flag in ('SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED', 'SHOP_CART_QUOTE_ENABLED', 'SHOP_BUYER_ENABLED'):
            self.assertFalse(state['enabled'][flag])
        client = self.portal.app.test_client()
        response = client.get('/shop/api/v2/products')
        self.assertEqual(response.status_code, 200)
        page = response.get_json()
        self.assertEqual(page['total'], 1)
        self.assertIsNone(page['items'][0]['price'])
        for text in ('PRIVATE_CANARY', 'UNRELEASED_PRIVATE_NAME', 'provider_id', 'identity_hash', 'net_price'):
            self.assertNotIn(text, response.get_data(as_text=True))
        self.assertEqual(client.get('/shop/media/' + self.asset).data, self.image)
        self.assertEqual(client.post('/shop/api/v2/orders', json={}).status_code, 404)
        self.assertFalse((self.data / 'shop-v2').exists())

    def test_pinned_original_document_cache_is_wired_locally_and_corruption_closes_release(self):
        from test_shop_document_overlay import LocalDocumentChecks
        local = LocalDocumentChecks('runTest'); local.setUp(); self.addCleanup(local.doCleanups)
        self.bundle['documents'] = deepcopy(local.bundle['documents'])
        (self.root / 'documents').mkdir()
        for asset, data in local.payloads.items():
            (self.root / 'documents' / asset).write_bytes(data)
        self.install()
        manifest = deepcopy(local.manifest)
        manifest['base_bundle_sha256'] = self.descriptor['bundle']['sha256']
        pinned = write_json(self.root / 'originals.json', manifest)
        self.descriptor['local_product_documents'] = {'path': pinned['file'], 'sha256': pinned['sha256'],
            'base_bundle_sha256': self.descriptor['bundle']['sha256']}
        self.descriptor['nominal_tyre_overlay'] = {'sha256': local.overlay}
        write_json(self.root / 'current.json', self.descriptor)
        # Nominal-source validation has its own strict suite; isolate only that
        # unrelated prerequisite while exercising the real document bootstrap.
        with patch('shop_nominal_overlay.load_nominal_tyre_overlay', side_effect=lambda root, record, bundle, **kw: bundle):
            dependencies = self.prepare()
        self.assertTrue(dependencies)
        documents = dependencies['documents_service']
        self.assertFalse(documents.allow_network)
        for asset, data in local.payloads.items():
            self.assertIn('file', documents.resources[asset])
            self.assertEqual(documents.read(asset), (data, 'application/pdf'))
        self.assertEqual(len(documents.attempts), 0)
        self.assertEqual(self.bundle['documents']['resources'], local.resources)
        first = next(iter(local.payloads))
        (self.root / 'documents' / first).write_bytes(b'CORRUPT')
        with patch('shop_nominal_overlay.load_nominal_tyre_overlay', side_effect=lambda root, record, bundle, **kw: bundle):
            with self.assertLogs(self.portal.app.logger, level='ERROR'):
                self.assertEqual(self.prepare(), {})
        self.assert_closed()

    def test_invalid_release_clears_preexisting_purchase_flags_and_hides_errors(self):
        self.portal.app.config.update({flag: True for flag in FLAGS})
        self.descriptor['publication_approved'] = 'PRIVATE_CANARY'
        self.install()
        with self.assertLogs(self.portal.app.logger, level='ERROR') as log:
            self.assertEqual(self.prepare(), {})
        self.assert_closed()
        self.assertNotIn('PRIVATE_CANARY', '\n'.join(log.output))
        self.assertFalse((self.data / 'shop-v2').exists())

    def test_bundle_hash_mismatch_duplicate_fields_and_nonfinite_values_fail_closed(self):
        original = (self.root / 'bundle.json').read_bytes()
        (self.root / 'bundle.json').write_bytes(original + b' ')
        self.assertEqual(self.prepare(), {})
        self.assert_closed()
        for content in (b'{"mode":"live","mode":"test"}', b'{"number":NaN}', b'{"number":Infinity}'):
            with self.subTest(content=content):
                (self.root / 'current.json').write_bytes(content)
                self.assertEqual(self.prepare(), {})
                self.assert_closed()

    def test_private_file_rejects_escape_and_changed_hash(self):
        record = write_json(self.root / 'private.json', {'token': 'PRIVATE_CANARY'})
        self.assertEqual(_private_file(self.root, record), self.root / 'private.json')
        for file in ('../private.json', '..', 'nested/private.json', str(self.root / 'private.json')):
            with self.subTest(file=file), self.assertRaises(ProductionError):
                _private_file(self.root, {**record, 'file': file})
        with self.assertRaises(ProductionError):
            _private_file(self.root, {**record, 'sha256': 'f' * 64})

    def test_local_media_hash_is_verified_before_returning_file_bytes(self):
        dependencies = self.prepare()
        (self.root / 'media' / self.asset).write_bytes(b'PRIVATE_CANARY_NOT_AN_IMAGE')
        with self.assertRaises(MediaError):
            dependencies['media_service'].read(self.asset)

    def test_unbound_read_approval_never_constructs_display_reader_or_warms_market_prices(self):
        self.descriptor.update(product_reads_approved=True,
            provider_config={'file': 'unused-missing-private.json', 'sha256': 'f' * 64})
        self.install()
        for injected in (False, True):
            factory = Mock(side_effect=AssertionError('No ALZURA reader construction'))
            with self.subTest(injected=injected), patch.dict(sys.modules,
                    {'shop_alzura_display': SimpleNamespace(provider_factory=factory)}):
                dependencies = self.prepare(**({'provider_factory': factory} if injected else {}))
                self.assertTrue(dependencies)
                factory.assert_not_called()
                adapters = self.portal.app.extensions['shop_production']
                self.assertIsNone(adapters['read_provider'])
                cache = adapters['price_cache']
                identifier = self.released['selection_id']
                self.assertFalse(dependencies['catalogue_warmup']([identifier], quantities=(3,)))
                self.assertFalse(cache.run_once())
                self.assertEqual(cache.status()['reserved_reads_in_period'], 0)
                product = dependencies['catalogue'].get(identifier)
                self.assertIsNone(product['price'])
                self.assertEqual(product['price_status'], 'not_offered')
                self.assertEqual(product['price_label'], '')
                self.assertFalse(self.portal.app.config['SHOP_CHECKOUT_ENABLED'])
                self.assertFalse(self.portal.app.config['SHOP_ORDER_DRAFT_ENABLED'])

    def test_quote_stage_creates_only_dedicated_storage_and_never_opens_orders_or_payments(self):
        self.descriptor.update(buyer_and_quotes_enabled=True, initial_storage_creation=True, rsu_reads_approved=True,
            rsu_config=write_json(self.root / 'rsu-private.json', {'token': 'PRIVATE_CANARY'}))
        self.install()
        provider = SimpleNamespace(retail_quote=Mock(side_effect=AssertionError('No Inquiry on startup')),
                                   rsu_mapping=Mock(side_effect=AssertionError('No Inquiry on startup')))
        with patch('shop_production.RSUQuoteProvider.from_config', return_value=provider) as construct:
            dependencies = self.prepare()
        construct.assert_called_once()
        self.assertTrue(dependencies)
        register_shop_runtime(self.portal, **dependencies)
        self.assertTrue((self.data / 'shop-v2' / 'initialized').is_file())
        self.assertTrue(self.portal.app.config['SHOP_BUYER_ENABLED'])
        self.assertTrue(self.portal.app.config['SHOP_CART_QUOTE_ENABLED'])
        self.assertFalse(self.portal.app.config['SHOP_ORDER_DRAFT_ENABLED'])
        self.assertFalse(self.portal.app.config['SHOP_CHECKOUT_ENABLED'])
        client = self.portal.app.test_client()
        session = client.get('/shop/api/v2/buyer/session', base_url='https://shop.example.test')
        self.assertEqual(session.status_code, 200)
        self.assertFalse(session.get_json()['address_ready'])
        self.assertIn('Secure', session.headers['Set-Cookie'])
        self.assertEqual(client.post('/shop/api/v2/orders', json={}).status_code, 404)
        provider.retail_quote.assert_not_called()
        provider.rsu_mapping.assert_not_called()

    def synthetic_provider(self, source, calls, unit):
        def retail(product, quantity):
            calls.append((product['id'], quantity))
            now = datetime.now(timezone.utc)
            return {'confirmed': True, 'source': source, 'procurement_confirmed': source == 'RSU',
                'product_id': product['id'], 'identity_hash': product['identity_hash'], 'mode': 'live',
                'currency': 'eur', 'vat_percent': '19.00', 'quantity': quantity, 'unit_gross_cent': unit,
                'stock': 20, 'observed_at': now.isoformat(), 'expires_at': (now+timedelta(minutes=10)).isoformat()}
        return SimpleNamespace(retail_quote=retail, rsu_mapping=Mock(side_effect=AssertionError('No mapping on startup')))

    def test_fixed_price_release_is_immediate_and_survives_restart_without_provider_reads(self):
        identifier=self.released['selection_id'];identity=variant_fingerprint(self.released)
        observed=datetime.now(timezone.utc)-timedelta(hours=2)
        self.bundle['rsu_bindings']={identifier:{'synthetic':True}}
        self.bundle['rsu_price_seeds']={'schema_version':1,'evidence_id':'e'*64,'records':{
            identifier:{'source':'RSU','identity_hash':identity,'unit_gross_cent':13685,
                        'stock_reported':20,'observed_at':observed.isoformat()}}}
        self.descriptor.update(fixed_pricebook_enabled=True,rsu_reads_approved=True,
            rsu_config=write_json(self.root/'rsu-private.json',{}))
        self.install();calls=[];supplier=self.synthetic_provider('RSU',calls,10000)
        with patch('shop_production.RSUQuoteProvider.from_config',return_value=supplier):
            dependencies=self.prepare()
        price=dependencies['catalogue'].get(identifier)['price']
        self.assertEqual(price['prices_by_quantity']['4']['items_gross_cent'],54740)
        self.assertEqual(price['kind'],'merchant_pricebook')
        self.assertFalse(price['stock_current'])
        self.assertFalse(dependencies['catalogue_warmup']([identifier],quantities=(1,4)))
        self.assertEqual(calls,[])
        self.assertFalse((self.data/'shop-v2').exists())
        self.assertTrue((self.data/'shop-pricebook'/'prices.sqlite3').is_file())
        self.portal.app.extensions['shop_production']['price_cache'].stop()
        self.bundle['rsu_price_seeds']['records'][identifier]['unit_gross_cent']=99999
        self.install()
        with patch('shop_production.RSUQuoteProvider.from_config',return_value=supplier):
            restarted=self.prepare()
        self.assertEqual(restarted['catalogue'].get(identifier)['price'],price)
        self.assertEqual(calls,[])

    def test_standalone_rim_policy_requires_private_literal_approval_and_reviewed_dimensions(self):
        identifier = self.released['selection_id']
        identity = variant_fingerprint(self.released)
        geometry = {'width': '8', 'diameter': '18', 'offset': '45',
                    'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}
        proof = {'confirmed': True, 'product_id': identifier, 'identity_hash': identity,
                 'supplier': 'RSU', 'evidence_id': 'synthetic-reviewed-rim', 'geometry': geometry}
        self.bundle.update(rsu_bindings={identifier: {'synthetic': True}},
                           shipping_classes={identifier: 'pkw_rim'}, delivery_postcodes=['74821'])
        self.descriptor.update(buyer_and_quotes_enabled=True, initial_storage_creation=True,
            rsu_reads_approved=True, rsu_config=write_json(self.root / 'rsu-private.json', {}))
        for approved in (False, 'true', True):
            with self.subTest(approved=approved):
                self.descriptor['standalone_rim_sales_approved'] = approved
                self.install()
                calls = []
                supplier = self.synthetic_provider('RSU', calls, 10000)
                supplier.rim_geometry = Mock(return_value=proof)
                supplier.rsu_mapping = lambda p: {'confirmed': True, 'product_id': identifier,
                    'identity_hash': identity, 'supplier': 'RSU', 'supplier_article_id': '123',
                    'evidence_id': 'synthetic-reviewed-mapping'}
                with patch('shop_production.RSUQuoteProvider.from_config', return_value=supplier):
                    dependencies = self.prepare()
                self.assertEqual(calls, [])
                product = dependencies['catalogue'].get(identifier)
                service = self.portal.app.extensions['shop_production']['quotes']
                self.assertEqual(service.allow_standalone_rims, approved is True)
                quote = service.quote({'lines': [{'id': identifier, 'quantity': 4}]},
                    customer_id='synthetic-customer', delivery_context={'delivery': {
                        'salutation': 'Herr', 'recipient': 'Synthetic Buyer', 'street': 'Teststraße 12',
                        'country': 'DE', 'city': 'Mosbach', 'postcode': '74821'}}).snapshot()
                self.assertEqual(quote['ready'], approved is True)
                if approved is True:
                    self.assertEqual(product['sale_dimensions'], geometry)
                    self.assertEqual(quote['total_gross_cent'], 40000)
                    self.assertIn('8×18 Zoll · ET 45 · 5×112 · ML 66.6 mm', quote['lines'][0]['name'])
                    self.assertEqual(quote['product_checks'][identifier]['standalone_rim_geometry'], proof)
                    self.assertIsNone(quote['product_checks'][identifier]['fitment_evidence_id'])
                else:
                    supplier.rim_geometry.assert_not_called()
                    self.assertNotIn('sale_dimensions', product)
                    self.assertIn('fitment_unconfirmed', [r['code'] for r in quote['blocked_reasons']])
                self.assertFalse(self.portal.app.config['SHOP_CHECKOUT_ENABLED'])

    def test_mainland_flag_is_wired_to_real_quote_freight_without_opening_other_gates(self):
        identifier = self.released['selection_id']
        identity = variant_fingerprint(self.released)
        proof = {'confirmed': True, 'product_id': identifier, 'identity_hash': identity,
            'supplier': 'RSU', 'evidence_id': 'synthetic-reviewed-rim',
            'geometry': {'width': '8', 'diameter': '18', 'offset': '45',
                         'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}}
        self.bundle.update(rsu_bindings={identifier: {'synthetic': True}},
                           shipping_classes={identifier: 'pkw_rim'}, delivery_postcodes=['74821'])
        self.descriptor.update(buyer_and_quotes_enabled=True, initial_storage_creation=True,
            standalone_rim_sales_approved=True, rsu_reads_approved=True,
            rsu_config=write_json(self.root / 'rsu-private.json', {}))
        delivery = {'salutation': 'Herr', 'recipient': 'Synthetic Buyer', 'street': 'Teststraße 12',
                    'country': 'DE', 'city': 'Berlin', 'postcode': '10115'}
        cart = {'lines': [{'id': identifier, 'quantity': 1}]}
        for flag in (None, False, 'true', 1, True):
            self.descriptor['mainland_delivery_approved'] = flag
            self.install()
            supplier = self.synthetic_provider('RSU', [], 10000)
            supplier.rim_geometry = Mock(return_value=proof)
            supplier.rsu_mapping = lambda p: {'confirmed': True, 'product_id': identifier,
                'identity_hash': identity, 'supplier': 'RSU', 'supplier_article_id': '123',
                'evidence_id': 'synthetic-reviewed-mapping'}
            with patch('shop_production.RSUQuoteProvider.from_config', return_value=supplier):
                dependencies = self.prepare()
            self.assertTrue(dependencies)
            self.assertIs(self.portal.app.config['SHOP_MAINLAND_DELIVERY_ENABLED'], flag is True)
            service = self.portal.app.extensions['shop_production']['quotes']
            self.assertTrue(service.manual_mixed_orders)
            self.assertIsNone(service.assembly_fitment(None, None, None))
            for declaration in (None, False, True):
                context = {'delivery': deepcopy(delivery)}
                if declaration is not None:
                    context['delivery_mainland'] = declaration
                result = service.quote(cart, customer_id='synthetic-buyer', delivery_context=context).snapshot()
                self.assertIs(result['ready'], flag is True and declaration is True)
                if result['ready']:
                    self.assertEqual(result['total_gross_cent'], 10821)
                else:
                    self.assertIn('freight', [r.get('dependency') for r in result['blocked_reasons']])
            legacy = service.quote(cart, customer_id='synthetic-buyer',
                delivery_context={'delivery': {**delivery, 'postcode': '74821'}}).snapshot()
            self.assertTrue(legacy['ready'])
            self.assertEqual(legacy['total_gross_cent'], 10821)
            island = service.quote(cart, customer_id='synthetic-buyer',
                delivery_context={'delivery': {**delivery, 'postcode': '27498'}, 'delivery_mainland': True}).snapshot()
            self.assertFalse(island['ready'])
            self.assertFalse(self.portal.app.config['SHOP_CHECKOUT_ENABLED'])

    def test_rsu_display_read_stage_works_without_buyer_and_skips_alzura_for_bound_rows(self):
        identifier = self.released['selection_id']
        self.bundle['rsu_bindings'] = {identifier: {'synthetic': True}}
        self.descriptor.update(rsu_reads_approved=True, product_reads_approved=True,
            rsu_config=write_json(self.root / 'rsu-private.json', {'synthetic': True}))
        self.install()
        calls = []
        supplier = self.synthetic_provider('RSU', calls, 23456)
        market = Mock(side_effect=AssertionError('No ALZURA construction for exact RSU-only catalogue'))
        with patch('shop_production.RSUQuoteProvider.from_config', return_value=supplier) as constructor:
            dependencies = self.prepare(provider_factory=market)
        constructor.assert_called_once()
        self.assertEqual(constructor.call_args.kwargs['approved_ids'], {identifier})
        market.assert_not_called()
        self.assertEqual(calls, [])
        self.assertTrue(dependencies['catalogue_warmup']([identifier], quantities=(3,)))
        self.assertTrue(self.portal.app.extensions['shop_production']['price_cache'].run_once())
        product = dependencies['catalogue'].get(identifier)
        self.assertEqual(self.portal.app.extensions['shop_production']['price_cache'].get(identifier)['source'], 'RSU')
        self.assertEqual(product['price']['prices_by_quantity']['3']['items_gross_cent'], 3*23456)
        self.assertEqual(product['purchase_status'], 'supplier_check_required')
        self.assertEqual(calls, [(identifier, 3)])
        self.assertFalse((self.data / 'shop-v2').exists())
        self.assertFalse(self.portal.app.config['SHOP_ORDER_DRAFT_ENABLED'])

    def test_missing_rsu_read_approval_never_relabels_import_or_falls_back_to_alzura(self):
        identifier = self.released['selection_id']
        now = datetime.now(timezone.utc)
        self.released['quote_public'] = {'source': 'ALZURA', 'currency': 'EUR', 'vat_percent': '19.00',
            'observed_at': now.isoformat(), 'expires_at': (now+timedelta(minutes=10)).isoformat(),
            'stock_reported': 20, 'prices_by_quantity': {'1': {'quantity': 1, 'unit_gross_cent': 77777, 'items_gross_cent': 77777, 'vat_percent': '19.00'}}}
        self.bundle['rsu_bindings'] = {identifier: {'synthetic': True}}
        self.descriptor['product_reads_approved'] = True
        self.install()
        factory = Mock(side_effect=AssertionError('No ALZURA fallback'))
        with patch('shop_production.RSUQuoteProvider.from_config', side_effect=AssertionError('RSU reads not approved')) as supplier:
            dependencies = self.prepare(provider_factory=factory)
        supplier.assert_not_called()
        factory.assert_not_called()
        product = dependencies['catalogue'].get(identifier)
        self.assertIsNone(product['price'])
        self.assertEqual(product['price_status'], 'update_required')
        self.assertEqual(product['purchase_status'], 'supplier_check_required')
        self.assertFalse(dependencies['catalogue_warmup']([identifier], quantities=(1,)))

    def test_mixed_browse_release_warms_only_exact_rsu_bindings(self):
        bound, market_id = self.released['selection_id'], self.hidden['selection_id']
        self.bundle['publication']['released_ids'].append(market_id)
        self.bundle['rsu_bindings'] = {bound: {'synthetic': True}}
        self.descriptor.update(rsu_reads_approved=True, product_reads_approved=True,
            rsu_config=write_json(self.root / 'rsu-private.json', {'synthetic': True}),
            provider_config=write_json(self.root / 'market-private.json', {'synthetic': True}))
        self.install()
        rsu_calls = []
        supplier = self.synthetic_provider('RSU', rsu_calls, 11111)
        market = Mock(side_effect=AssertionError('No market-price reader'))
        with patch('shop_production.RSUQuoteProvider.from_config', return_value=supplier):
            dependencies = self.prepare(provider_factory=market)
        market.assert_not_called()
        self.assertEqual(rsu_calls, [])
        self.assertTrue(dependencies['catalogue_warmup']([bound, market_id], quantities=(2,)))
        cache = self.portal.app.extensions['shop_production']['price_cache']
        self.assertTrue(cache.run_once())
        self.assertFalse(cache.run_once())
        self.assertEqual(rsu_calls, [(bound, 2)])
        product = dependencies['catalogue'].get(bound)
        self.assertEqual(cache.get(bound)['source'], 'RSU')
        self.assertEqual(product['price']['prices_by_quantity']['2']['unit_gross_cent'], 11111)
        self.assertEqual(product['purchase_status'], 'supplier_check_required')
        unavailable = dependencies['catalogue'].get(market_id)
        self.assertIsNone(cache.get(market_id))
        self.assertIsNone(unavailable['price'])
        self.assertEqual(unavailable['price_status'], 'not_offered')
        self.assertEqual(unavailable['price_label'], '')
        self.assertEqual(cache.status()['sources']['ALZURA']['queued'], 0)
        self.assertFalse((self.data / 'shop-v2').exists())

    def test_quote_stage_requires_its_own_literal_rsu_read_approval(self):
        for flag in (None, False, 'true', 1):
            self.descriptor.update(buyer_and_quotes_enabled=True, rsu_reads_approved=flag)
            self.install()
            with patch('shop_production.RSUQuoteProvider.from_config', side_effect=AssertionError('No unauthorized read client')) as supplier:
                self.assertEqual(self.prepare(), {})
            supplier.assert_not_called()
            self.assert_closed()

    def test_storage_creation_requires_explicit_descriptor_not_implicit_empty_recovery(self):
        self.descriptor.update(buyer_and_quotes_enabled=True, rsu_reads_approved=True,
            rsu_config=write_json(self.root / 'rsu-private.json', {}))
        self.install()
        self.assertEqual(self.prepare(), {})
        self.assert_closed()
        self.assertFalse((self.data / 'shop-v2').exists())

    def test_backup_resolver_works_with_release_disabled_and_never_creates_missing_ledger(self):
        self.assertIsNone(existing_shop_backup_storage(environ={'DATA_DIR': str(self.data)}))
        self.assertFalse((self.data / 'shop-v2').exists())
        storage = ShopStorage(self.data).provision(create=True)
        resolved = existing_shop_backup_storage(environ={'DATA_DIR': str(self.data), 'SHOP_RELEASE_ENABLED': '0'})
        self.assertEqual(resolved.path, storage.path)
        self.assertTrue(resolved.snapshot().startswith(b'SQLite format 3'))
        storage.path.unlink()
        with self.assertRaises(StorageError):
            existing_shop_backup_storage(environ={'DATA_DIR': str(self.data)})

    def test_mounting_tariff_counts_runflat_and_disposal_once_for_each_tyre(self):
        product = {'id': 'tyre:' + 'a' * 24, 'identity_hash': 'a' * 64,
                   'category': 'tyre', 'diameter': 18, '_mode': 'live'}
        base = _service_quote('mounting', product, 4, {'runflat': False, 'disposal': False})
        extras = _service_quote('mounting', product, 4, {'runflat': True, 'disposal': True})
        self.assertEqual(base['gross_cent'], 10000)
        self.assertEqual(extras['gross_cent'], 13600)
        self.assertNotEqual(base['options_hash'], extras['options_hash'])
        self.assertIsNone(_service_quote('mounting', product, 4, {'runflat': 'yes'}))
        self.assertIsNone(_service_quote('painting', product, 4, {}))

    def test_public_host_allows_new_browse_routes_without_opening_writes_or_portal(self):
        tree = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'restrict_public_site_service')
        function.decorator_list = []
        module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
        namespace = {'request': request, 'abort': abort, 'redirect': redirect,
                     'is_public_site_request': lambda: True, 'PUBLIC_SITE_ONLY': True, 'PORTAL_BASE_URL': ''}
        exec(compile(module, str(ROOT / 'app.py'), 'exec'), namespace)
        app = Flask('public-enriched-gate')
        app.before_request(namespace['restrict_public_site_service'])
        for index, endpoint in enumerate(('shop_public_v2.product_variants', 'shop_public_v2.compatible_tyres',
                                          'shop_frontend.felgenstudio', 'shop_public_v2.create_order_draft', 'internal_admin')):
            app.add_url_rule('/' + str(index), endpoint, lambda: ('', 204), methods=['GET', 'POST'])
        client = app.test_client()
        for index in range(3):
            self.assertEqual(client.get('/' + str(index)).status_code, 204)
        for index in (3, 4):
            self.assertEqual(client.post('/' + str(index)).status_code, 404)


if __name__ == '__main__':
    unittest.main()
