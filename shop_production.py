"""Reviewed shop bootstrap for the existing persistent portal.

Nothing runs on import. An explicit SHOP_RELEASE_ENABLED=1 and a private,
hash-verified release descriptor are required. Provider credentials stay in
private files, never catalogue JSON responses or the Git repository. Shop
failure leaves the workshop portal available and all purchasing gates closed.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import base64
import re

from shop_catalog_enriched import EnrichedPublicCatalog
from shop_catalog_public import variant_fingerprint
from shop_cart_quotes import CartQuoteService
from shop_buyer import ShopBuyer
from shop_freight import RSURetailFreight, MainlandDeliveryEligibility
from shop_media import ReleasedMedia
from shop_documents import ReleasedProductDocuments
from shop_price_cache import PublicPriceCache
from shop_price_hybrid import HybridPriceCache
from shop_price_book import PublishedPriceBook
from shop_pricebook_runtime import PriceBookRuntime
from shop_rsu_quotes import RSUQuotePolicy, RSUQuoteProvider
from shop_storage import ShopStorage


# Reviewed 4,047-product catalogue plus fingerprint-bound tyre candidates is
# 37.9 MB. Keep a finite bound with room for that measured release.
MAX_RELEASE_BYTES = 48 * 1024 * 1024


class ProductionError(ValueError):
    pass


def _parts_api_channel_supported():
    """RSU-written-20261118-KFZ-not-offered: no automatic parts orders.

    Successful Inquiry reads do not establish ordering support. This supplier
    boundary is deliberately independent of environment, profiles, catalogue
    approvals or cached observations. Private correspondence is not embedded.
    """
    return False


def _json(path, maximum=MAX_RELEASE_BYTES):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= maximum:
        raise ProductionError('release_file_unavailable')
    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result:
                raise ValueError()
            result[k] = v
        return result
    try:
        return json.loads(path.read_bytes(), object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except Exception:
        raise ProductionError('release_file_invalid') from None


def _private_file(root, record):
    if (not isinstance(record, Mapping) or set(record) != {'file', 'sha256'}
            or not isinstance(record['file'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', record['file'])
            or not isinstance(record['sha256'], str) or not re.fullmatch(r'[a-f0-9]{64}', record['sha256'])):
        raise ProductionError('release_file_invalid')
    path = root / record['file']
    if path.resolve().parent != root.resolve() or path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_RELEASE_BYTES:
        raise ProductionError('release_file_invalid')
    if sha256(path.read_bytes()).hexdigest() != record['sha256']:
        raise ProductionError('release_hash_mismatch')
    return path


def existing_shop_backup_storage(*, environ=None):
    """Back up an installed order ledger even while the catalogue is disabled.

    Missing/corrupt marked storage raises instead of advertising a complete
    portal backup without its order/idempotency fences. Never creates a DB.
    """
    env = os.environ if environ is None else environ
    data = Path(env.get('DATA_DIR', '/var/data'))
    if not data.is_absolute():
        raise ProductionError('persistent_root_unavailable')
    if not (data / 'shop-v2').exists():
        return None
    return ShopStorage(data).provision(create=False)


def _service_quote(code, product, quantity, options):
    """Existing published tyre-service tariff; other services require a quote."""
    if code not in {'mounting', 'runflat', 'disposal'} or product['category'] != 'tyre':
        return None
    diameter = product.get('diameter')
    if type(diameter) is not int or not 13 <= diameter <= 20:
        return None
    unit = {'runflat': 500, 'disposal': 400}.get(code)
    if code == 'mounting':
        unit = 2250 if diameter <= 16 else 2500 if diameter <= 18 else 3000
        if not isinstance(options, Mapping) or any(type(options.get(k, False)) is not bool for k in ('runflat', 'disposal')):
            return None
        unit += (500 if options.get('runflat') else 0) + (400 if options.get('disposal') else 0)
    now = datetime.now(timezone.utc)
    return {'confirmed': True, 'product_id': product['id'], 'identity_hash': product['identity_hash'],
            'quantity': quantity, 'options_hash': sha256(json.dumps(options, ensure_ascii=False,
                sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'mode': 'live', 'currency': 'eur', 'vat_percent': '19.00',
            'gross_cent': quantity * unit, 'observed_at': now.isoformat(),
            'expires_at': (now + timedelta(minutes=15)).isoformat()}


def prepare_shop_runtime(portal, *, environ=None, provider_factory=None, background=True):
    """Load one deliberately installed release; returns runtime keyword args.

    Checkout is deliberately not inferred from a catalogue or provider login.
    Until its required frozen legal/delivery/payment/workflow configuration is
    fully supplied, this release serves real catalogue browsing and quotes only.
    Prices are RSU-only. provider_factory remains accepted for call compatibility
    but is never invoked; ALZURA supplies released product/media data only.
    """
    env = os.environ if environ is None else environ
    app = portal.app
    app.config['SHOP_MAINLAND_DELIVERY_ENABLED'] = False
    app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'] = False
    app.config['SHOP_PARTS_DIRECT_ENABLED'] = False
    app.config['SHOP_PARTS_VEHICLE_OPTIONS'] = []
    if env.get('SHOP_RELEASE_ENABLED') != '1':
        return {}
    # A failure cannot retain a previously requested purchasing flag.
    for flag in ('SHOP_PUBLIC_CATALOGUE_ENABLED', 'SHOP_FRONTEND_ENABLED', 'SHOP_MEDIA_ENABLED', 'SHOP_DOCUMENTS_ENABLED',
                 'SHOP_BUYER_ENABLED', 'SHOP_CART_QUOTE_ENABLED', 'SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED', 'SHOP_CONSUMER_ENABLED',
                 'SHOP_MAINLAND_DELIVERY_ENABLED', 'SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'):
        app.config[flag] = False
    try:
        data = Path(env.get('DATA_DIR', '/var/data'))
        if not data.is_absolute() or not data.is_dir():
            raise ProductionError('persistent_root_unavailable')
        root = data / 'shop-release'
        if root.is_symlink() or not root.is_dir() or root.resolve().parent != data.resolve():
            raise ProductionError('release_root_unavailable')
        descriptor = _json(root / 'current.json', 65536)
        if (not isinstance(descriptor, dict) or descriptor.get('schema_version') != 1
                or descriptor.get('publication_approved') is not True or descriptor.get('mode') != 'live'):
            raise ProductionError('release_not_approved')
        bundle = _json(_private_file(root, descriptor.get('bundle')))
        from shop_nominal_overlay import load_nominal_tyre_overlay, unavailable_document_product_ids
        bundle = load_nominal_tyre_overlay(root, descriptor.get('nominal_tyre_overlay'), bundle,
            base_bundle_sha256=descriptor['bundle']['sha256'])
        from shop_document_overlay import local_product_document_resources
        document_resources = local_product_document_resources(root, descriptor.get('local_product_documents'), bundle,
            base_bundle_sha256=descriptor['bundle']['sha256'],
            overlay_sha256=(descriptor.get('nominal_tyre_overlay') or {}).get('sha256'))
        from shop_parts_release import load_parts_release
        parts_record = descriptor.get('parts_direct_sales')
        if parts_record is not None and not _parts_api_channel_supported():
            raise ProductionError('rsu_parts_api_not_supported')
        if parts_record is not None and descriptor.get('parts_direct_sales_approved') is not True:
            raise ProductionError('parts_release_not_approved')
        bundle, parts_release = load_parts_release(root, parts_record, bundle,
            base_bundle_sha256=descriptor['bundle']['sha256'])
        blocked_document_ids = unavailable_document_product_ids(bundle, descriptor.get('unavailable_product_documents'))
        rows, publication = bundle['products'], bundle['publication']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 100000 or not isinstance(publication, dict):
            raise ProductionError('release_catalogue_invalid')
        identities = {r.get('selection_id', r.get('id')): {'id': r.get('selection_id', r.get('id')),
                        'identity_hash': variant_fingerprint(r), 'category': r.get('category')}
                      for r in rows if isinstance(r, dict)}
        parts_fitments, parts_notices = None, {}
        fitment_record = descriptor.get('parts_vehicle_fitments')
        if fitment_record is not None:
            if (not parts_release or not isinstance(fitment_record, Mapping)
                    or set(fitment_record) != {'source', 'approvals'}):
                raise ProductionError('parts_vehicle_release_invalid')
            from shop_parts_fitment import PartsVehicleFitments
            vehicle_source = _private_file(root, fitment_record['source'])
            reviews = _json(_private_file(root, fitment_record['approvals']), 1024 * 1024)
            if (not isinstance(reviews, dict) or set(reviews) not in (
                    {'approved_conditions', 'approved_certificate_hashes'},
                    {'approved_conditions', 'approved_certificate_hashes', 'public_notices'})):
                raise ProductionError('parts_vehicle_release_invalid')
            from shop_catalog_enriched import validated_parts_notices
            parts_notices = validated_parts_notices(rows, publication['released_ids'], reviews.get('public_notices', {}))
            if not set(parts_notices) <= parts_release['ids']:
                raise ProductionError('parts_vehicle_release_invalid')
            parts_fitments = PartsVehicleFitments(vehicle_source.read_bytes(),
                artifact_sha256=fitment_record['source']['sha256'], base_bundle_sha256=descriptor['bundle']['sha256'],
                products=identities, approved_conditions=reviews['approved_conditions'],
                approved_certificate_hashes=reviews['approved_certificate_hashes'])
        released_ids = set(publication['released_ids'])
        bindings = bundle.get('rsu_bindings', {})
        if not isinstance(bindings, Mapping) or not set(bindings) <= released_ids:
            raise ProductionError('procurement_bindings_invalid')
        if (any(isinstance(identifier, str) and identifier.startswith('parts:') for identifier in bindings)
                and not _parts_api_channel_supported()):
            # Moving an overlay into a newly pinned base bundle must not bypass
            # the supplier's unsupported ordering-channel boundary either.
            raise ProductionError('rsu_parts_api_not_supported')
        procurement_ids = set(bindings)
        inquiry_prices = {}
        inquiry_record = descriptor.get('parts_inquiry_prices')
        if inquiry_record is not None:
            if descriptor.get('parts_inquiry_prices_approved') is not True:
                raise ProductionError('inquiry_prices_not_approved')
            from shop_parts_inquiry import load_parts_inquiry
            from shop_catalog_enriched import validated_inquiry_prices
            allowed_inquiries = {identifier for identifier in released_ids - procurement_ids
                if identifier in identities and identities[identifier]['category'] in {'body_part', 'accessory'}}
            inquiry_source = load_parts_inquiry(root, inquiry_record, bundle,
                base_bundle_sha256=descriptor['bundle']['sha256'], allowed_ids=allowed_inquiries)
            inquiry_prices = validated_inquiry_prices(rows, released_ids, procurement_ids,
                {identifier: inquiry_source.get(identifier) for identifier in inquiry_source.ids})
        supplier_config, supplier, rsu_cache = None, None, None
        rsu_reads_approved = descriptor.get('rsu_reads_approved') is True
        if descriptor.get('buyer_and_quotes_enabled') is True and not rsu_reads_approved:
            raise ProductionError('supplier_reads_not_approved')
        if rsu_reads_approved:
            supplier_config = _json(_private_file(root, descriptor.get('rsu_config')), 65536)
            supplier = RSUQuoteProvider.from_config(config=supplier_config, bindings=bindings,
                approved_ids=procurement_ids, policy=RSUQuotePolicy(mode='live', currency='EUR',
                    net_unit_price_confirmed=True, evidence_id='rsu-written-20261117-net-EUR'))
            rsu_cache = PublicPriceCache(identities, approved_ids=procurement_ids, provider=supplier,
                mode='live', enabled=True, reads_approved=True, max_provider_reads_per_period=300,
                spacing_seconds=2.1, max_cache_entries=4096, source='RSU', require_procurement=True)
        fixed_prices = descriptor.get('fixed_pricebook_enabled') is True
        pricebook = None
        if fixed_prices:
            if supplier is None:
                raise ProductionError('fixed_price_supplier_unavailable')
            seeds = bundle.get('rsu_price_seeds')
            if (not isinstance(seeds, Mapping) or seeds.get('schema_version') != 1
                    or not isinstance(seeds.get('records'), Mapping)
                    or not set(seeds['records']) <= procurement_ids
                    or not isinstance(seeds.get('evidence_id'), str)
                    or not re.fullmatch(r'[a-f0-9]{64}', seeds['evidence_id'])):
                raise ProductionError('fixed_price_seeds_invalid')
            pricebook = PublishedPriceBook(data / 'shop-pricebook' / 'prices.sqlite3',
                products=identities, approved_ids=procurement_ids, mode='live').provision(create=True)
            pricebook.seed(seeds['records'], evidence_id=seeds['evidence_id'])
            cache = PriceBookRuntime(pricebook, identities, approved_ids=procurement_ids, provider=supplier)
        else:
            cache = HybridPriceCache(identities, released_ids=released_ids, procurement_ids=procurement_ids,
                                     rsu_cache=rsu_cache, market_cache=None)
        # Merchant explicitly permits exact-variant rim sales without making a
        # vehicle-fitment promise. Only independently reviewed RSU dimensions
        # qualify; assemblies and services retain their separate proof gates.
        allow_standalone_rims = descriptor.get('standalone_rim_sales_approved') is True
        rim_sales = {}
        if allow_standalone_rims and supplier is not None:
            for identifier in procurement_ids:
                if identities[identifier]['category'] in {'alloy_wheel', 'steel_wheel'}:
                    proof = supplier.rim_geometry(identities[identifier])
                    if proof is not None:
                        rim_sales[identifier] = proof
        # Purchasing prices come only from currently bound RSU goods. A separate
        # original-verified inquiry snapshot may display a retained retail
        # amount; it is never added to supplier, stock-refresh or checkout IDs.
        display_rows = [{**row, 'quote_public': None, 'price': None, 'sample_price_cents': None} for row in rows]
        compatible_pairs = bundle.get('compatible_pairs')
        if fixed_prices and bundle.get('reviewed_rim_width_registry') is not None:
            from shop_rim_width_candidates import released_width_pairs
            compatible_pairs = released_width_pairs(bundle, approved_ids=set(seeds['records']))
        approved_postcodes = frozenset(bundle.get('delivery_postcodes', []))
        mainland_approved = descriptor.get('mainland_delivery_approved') is True
        delivery_eligibility = MainlandDeliveryEligibility(approved_postcodes,
            mainland_delivery_approved=mainland_approved)
        freight = RSURetailFreight(bundle.get('shipping_classes', {}), mode='live', basis='net',
            evidence_id='RSU-20261117-net-shipping-one-position', checked_at='2026-11-17T10:28:00+00:00',
            destination_eligibility=delivery_eligibility)
        shipping_ready = set(freight.classes) & procurement_ids
        if parts_release:
            if supplier is None:
                raise ProductionError('parts_supplier_unavailable')
            observed_freight = {pid: p for pid,p in parts_release['freight'].items()
                                if p['rule'] == 'rsu_ui_unique_q1_freight_v1'}
            fixed_freight = {pid: p for pid,p in parts_release['freight'].items()
                            if p['rule'] == 'merchant_fixed_parts_freight_v1'}
            from shop_parts_freight import PartsRetailFreight
            freight = PartsRetailFreight(observed_freight, bindings=parts_release['bindings'], mode='live',
                destination_eligibility=delivery_eligibility, fresh_inquiry=supplier.retained_inquiry,
                source_refs=parts_release['source_refs'], fallback=freight, allow_retained_inquiry=True)
            if fixed_freight:
                from shop_parts_fixed_freight import FixedPartsRetailFreight
                freight = FixedPartsRetailFreight(fixed_freight, bindings=parts_release['bindings'], mode='live',
                    destination_eligibility=delivery_eligibility, retained_inquiry=supplier.retained_inquiry,
                    source_refs=parts_release['source_refs'], fallback=freight)
            shipping_ready.update(parts_release['freight'])
            app.config['SHOP_PARTS_DIRECT_ENABLED'] = True
            if parts_fitments is not None:
                app.config['SHOP_PARTS_VEHICLE_OPTIONS'] = parts_fitments.vehicles()
        app.config['SHOP_MAINLAND_DELIVERY_ENABLED'] = mainland_approved
        catalogue = EnrichedPublicCatalog(display_rows, released_ids=publication['released_ids'],
            image_urls=publication.get('image_urls'), document_urls=publication.get('document_urls'),
            verified_fitments=bundle.get('verified_fitments'), browsing_evidence=bundle.get('browsing_evidence'),
            compatible_pairs=compatible_pairs, quote_lookup=cache.get,
            quote_lookup_authoritative=True, procurement_ids=procurement_ids,
            product_documents=bundle.get('product_documents'), standalone_rims=rim_sales,
            checkout_excluded_ids=blocked_document_ids,
            rsu_only_prices=True, allow_published_prices=fixed_prices,
            preferred_ids=set(seeds['records']) if fixed_prices else None,
            shipping_ready_ids=shipping_ready, parts_notices=parts_notices, inquiry_prices=inquiry_prices)
        media_manifest = bundle['media']
        media = ReleasedMedia(media_manifest['resources'], approved_assets=set(media_manifest['approved_assets']),
                              local_root=root / 'media', allow_network=descriptor.get('media_reads_approved') is True,
                              max_network_reads=3, max_cache_bytes=64 * 1024 * 1024)
        app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_FRONTEND_ENABLED=True,
                          SHOP_MEDIA_ENABLED=True, SHOP_ORDER_MODE='live',
                          SHOP_FRONTEND_INDEXABLE=descriptor.get('indexable') is True,
                          SHOP_PUBLIC_ORIGIN=descriptor['public_origin'])
        dependencies = {'catalogue': catalogue, 'media_service': media,
                        'catalogue_warmup': cache.warmup if cache else None}
        documents = None
        if isinstance(bundle.get('documents'), Mapping):
            manifest = bundle['documents']
            documents = ReleasedProductDocuments(document_resources, approved_assets=set(manifest['approved_assets']),
                local_root=root / 'documents', enabled=True,
                allow_network=descriptor.get('document_reads_approved') is True,
                max_network_reads_per_period=5000, max_concurrent_reads=1, spacing_seconds=.2)
            dependencies['documents_service'] = documents
            app.config['SHOP_DOCUMENTS_ENABLED'] = True
        from shop_service_requests import prepare_service_request_links
        dependencies['feature_links'] = prepare_service_request_links(portal, enabled=True)
        adapters = {'catalogue': catalogue, 'media': media, 'documents': documents, 'price_cache': cache,
                    'read_provider': None, 'release_id': descriptor.get('release_id', ''),
                    'purchasing_enabled': False}
        if inquiry_record is not None:
            adapters['inquiry_price_count'] = len(inquiry_prices)
        adapters['complete_wheel_review_requested'] = descriptor.get('complete_wheel_checkout_approved') is True
        if supplier is not None:
            adapters['supplier_quotes'] = supplier
        if pricebook is not None:
            adapters['published_prices'] = pricebook
        app.extensions['shop_production'] = adapters
        if descriptor.get('buyer_and_quotes_enabled') is True:
            storage = ShopStorage(data).provision(create=descriptor.get('initial_storage_creation') is True)
            store = storage.order_store()
            buyer = ShopBuyer(storage.buyer_backend(), public_origin=descriptor['public_origin'], mode='live', enabled=True)
            def lookup(identifier):
                product = catalogue.get(identifier)
                if product is None:
                    return None
                diameter = product['geometry'].get('diameter', '')
                name = product['name']
                if identifier in rim_sales:
                    g = rim_sales[identifier]['geometry']
                    suffix = (f" · {g['width']}×{g['diameter']} Zoll · ET {g['offset']} · "
                              f"{g['bolt_count']}×{g['pcd']} · ML {g['centre_bore']} mm")
                    name = name + suffix if len(name + suffix) <= 180 else name[:178-len(suffix)].rstrip() + '…' + suffix
                requires_vehicle = (product['category'] in {'alloy_wheel', 'steel_wheel', 'body_part'}
                    or identifier in parts_release.get('ids', ()) and product.get('department') == 'parts')
                return {**identities[identifier], 'name': name, '_mode': 'live',
                        'fitment_required': requires_vehicle,
                        'fitment': None, 'required_fees_confirmed': identifier in bindings,
                        'standalone_rim_geometry': rim_sales.get(identifier),
                        'required_documents_confirmed': (identifier not in blocked_document_ids and (product['category'] != 'tyre' or
                            documents is not None and all(product.get('product_documents', {}).get(k)
                                for k in ('tyre_label_url', 'tyre_sheet_url')))),
                        'required_fees': [], 'new_part': False, 'painting_eligible': False,
                        'diameter': int(diameter) if re.fullmatch(r'[0-9]{2}', diameter) else None,
                        'tyre_size': product['size']}
            retail_quote = (lambda product, quantity: pricebook.checkout_quote(product, quantity, supplier.retail_quote)
                            if pricebook is not None else supplier.retail_quote(product, quantity))
            quotes = CartQuoteService(catalogue_lookup=lookup, retail_quote=retail_quote,
                rsu_mapping=supplier.rsu_mapping, service_quote=_service_quote, freight_quote=freight,
                assembly_fitment=lambda *a: None, mode='live', allow_standalone_rims=allow_standalone_rims,
                assembly_proposal=catalogue.assembly_proposal,
                fitment_lookup=parts_fitments.lookup if parts_fitments is not None else None)
            def sink(quote, *, customer_id, delivery_context):
                return store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
                    customer_id=customer_id, mode='live', delivery_context=delivery_context)
            dependencies.update(buyer_service=buyer, quote_service=quotes, quote_sink=sink, order_store=store)
            app.config.update(SHOP_BUYER_ENABLED=True, SHOP_CART_QUOTE_ENABLED=True)
            adapters.update(storage=storage, store=store, buyer=buyer, supplier_quotes=supplier, quotes=quotes)
            if descriptor.get('checkout_requested') is True:
                try:
                    from shop_production_payments import prepare_production_payments
                    profile = _json(_private_file(root, descriptor.get('payment_profile')), 65536)
                    stripe_config = _json(_private_file(root, descriptor.get('stripe_config')), 65536)
                    pack = _json(_private_file(root, descriptor.get('consumer_pack')), 4 * 1024 * 1024)
                    attachments = []
                    for attachment in pack.get('attachments', []):
                        if not isinstance(attachment, dict) or set(attachment) != {'filename', 'mimetype', 'content_b64'}:
                            raise ProductionError('consumer_pack_invalid')
                        attachments.append({'filename': attachment['filename'], 'mimetype': attachment['mimetype'],
                                            'content': base64.b64decode(attachment['content_b64'], validate=True)})
                    pack['attachments'] = attachments
                    payment_dependencies, starter = prepare_production_payments(portal, adapters, profile,
                        pack=pack, supplier_config=supplier_config, stripe_config=stripe_config)
                    dependencies.update(payment_dependencies)
                    if adapters['complete_wheel_review_requested']:
                        quotes.complete_wheel_review = True
                        app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'] = True
                    adapters['start_workflow'] = starter
                    dependencies['feature_links'].update(terms='/shop/recht/agb', privacy='/shop/recht/datenschutz',
                                                         withdrawal='/shop/widerruf/')
                    app.config.update(SHOP_CONSUMER_ENABLED=True, SHOP_ORDER_DRAFT_ENABLED=True, SHOP_CHECKOUT_ENABLED=True)
                except Exception:
                    adapters['purchasing_enabled'] = False
                    adapters['checkout_blocked_reason'] = 'payment_or_consumer_profile_incomplete'
                    app.config.update(SHOP_CONSUMER_ENABLED=False, SHOP_ORDER_DRAFT_ENABLED=False, SHOP_CHECKOUT_ENABLED=False)
        if cache:
            cache.start(background=background)
        return dependencies
    except Exception:
        for flag in ('SHOP_PUBLIC_CATALOGUE_ENABLED', 'SHOP_FRONTEND_ENABLED', 'SHOP_MEDIA_ENABLED', 'SHOP_DOCUMENTS_ENABLED',
                     'SHOP_BUYER_ENABLED', 'SHOP_CART_QUOTE_ENABLED', 'SHOP_ORDER_DRAFT_ENABLED', 'SHOP_CHECKOUT_ENABLED', 'SHOP_CONSUMER_ENABLED',
                     'SHOP_MAINLAND_DELIVERY_ENABLED', 'SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'):
            app.config[flag] = False
        app.config['SHOP_PARTS_DIRECT_ENABLED'] = False
        app.config['SHOP_PARTS_VEHICLE_OPTIONS'] = []
        app.extensions['shop_production'] = {'ready': False, 'reason': 'release_configuration_incomplete'}
        app.logger.error('Shop release not activated: private release configuration incomplete')
        return {}
