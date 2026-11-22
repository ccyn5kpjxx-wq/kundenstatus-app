"""Explicit production-only checkout wiring; never a testing entry point.

Requires a reviewed legal/delivery profile, dedicated Stripe configuration and
all durable adapters. Imports/construction never create a Stripe session,
supplier order, customer message or payment. The caller starts workers only
after the complete reviewed release is registered.
"""
from contextlib import closing
from copy import copy, deepcopy
from functools import partial
from hashlib import sha256

from mailbox_client import Mailbox
from mailbox_outbox import MailOutbox
from shop_checkout import ShopCheckout
from shop_consumer import ShopConsumer, init_consumer_schema, _freeze_pack
from shop_combined_cart_terms import checkout_terms
from shop_fulfillment import FulfillmentJournal, ShopFulfillment, init_fulfillment_schema
from shop_order_receipts import ShopOrderReceipts, init_order_receipt_schema
from shop_order_workflow import WorkflowJournal, ShopOrderWorkflow, WorkflowCheckout, init_order_workflow_schema
from shop_payment_actions import PaymentActions, PaymentActionJournal, init_payment_action_schema
from shop_rsu_orders import RSUOrderPolicy, RSUOrderTransport
from shop_stripe_gateway import StripeShopGateway
from reifen_rechtstexte import ANBIETER


class PaymentProfileError(ValueError):
    pass


def _frozen_price_service(quotes, snapshot, legacy_retail_quote):
    """Older unbooked tyre quotes used the fresh price at the purchased quantity."""
    if legacy_retail_quote is not None and any(
            line['category'] == 'tyre' and 'base_unit_gross_cent' not in line for line in snapshot['lines']):
        legacy = copy(quotes)
        legacy.retail_quote = legacy_retail_quote
        legacy.tyre_payment_costs = False
        return legacy
    return quotes


def requote_remaining_goods(quotes, requests, snapshot, context, shipments, *, legacy_retail_quote=None):
    """Refresh open goods, preserving the customer's freight cap for the whole shipment."""
    quotes = _frozen_price_service(quotes, snapshot, legacy_retail_quote)
    fresh = quotes.quote({'lines': requests}, customer_id=snapshot['customer_id'],
                         vehicle_ref=snapshot['vehicle_ref'], delivery_context=context).snapshot()
    deadlines = []
    for shipment in shipments:
        freight = quotes.freight_quote(deepcopy(shipment), deepcopy(context))
        until, error = quotes._current(freight, quotes.clock())
        if (error or freight.get('shipment_hash') != shipment['shipment_hash']
                or freight.get('context_hash') != shipment['context_hash']
                or type(freight.get('gross_cent')) is not int
                or not 0 <= freight['gross_cent'] <= shipment['gross_cent']):
            raise PaymentProfileError('freight_unconfirmed')
        deadlines.append(until)
    return fresh, min(deadlines)


class ConsumerOrderStore:
    """Enforce legal acceptance at the server-side order creation boundary."""
    def __init__(self, store, consumer):
        self.store, self.consumer = store, consumer

    def __getattr__(self, name):
        return getattr(self.store, name)

    def create_order(self, quote_hash, *, customer_id, mode, idempotency_key,
                     appointment_preference=None, appointment_calendar=None):
        self.consumer.require_acceptance(quote_hash, customer_id=customer_id)
        return self.store.create_order(quote_hash, customer_id=customer_id, mode=mode, idempotency_key=idempotency_key,
            appointment_preference=appointment_preference, appointment_calendar=appointment_calendar)


class _ReceiptDelivery:
    def __init__(self, receipts, consumer):
        self.receipts, self.consumer = receipts, consumer

    def __getattr__(self, name):
        return getattr(self.receipts, name)

    def deliver_once(self):
        try:
            return self.receipts.deliver_once()
        finally:
            self.consumer.deliver_once()


def prepare_production_payments(portal, adapters, profile, *, pack, supplier_config, stripe_config):
    """Return complete dependencies and a deferred worker starter, or raise.

    All inputs are explicit private server configuration. A catalogue login,
    test token, or previous rental configuration cannot arm this integration.
    """
    if (not isinstance(profile, dict) or not isinstance(stripe_config, dict) or not isinstance(supplier_config, dict)
            or not isinstance(pack, dict) or pack.get('seller', ANBIETER) != ANBIETER
            or profile.get('mode') != 'live'
            or any(profile.get(k) is not True for k in ('checkout_enabled', 'supplier_live_enabled',
                       'customer_mail_enabled', 'withdrawals_enabled', 'delivery_terms_reviewed', 'single_worker_confirmed'))
            or stripe_config.get('SHOP_STRIPE_MODE') != 'live'
            or supplier_config.get('TYRESYSTEM_API_MODE') != 'live'):
        raise PaymentProfileError('payment_profile_incomplete')
    storage, original_store, buyer, quotes = (adapters[k] for k in ('storage', 'store', 'buyer', 'quotes'))
    complete_wheel_review = adapters.get('complete_wheel_review_requested') is True
    if complete_wheel_review and profile.get('complete_wheel_review_approved') is not True:
        raise PaymentProfileError('complete_wheel_review_not_approved')
    if complete_wheel_review:
        from shop_complete_wheels import NOTICE
        frozen = _freeze_pack(pack)
        if (not isinstance(pack.get('texts'), dict) or pack['texts'].get('komplettraeder_zahlung') != NOTICE
                or frozen is None
                or profile.get('complete_wheel_notice_sha256') != sha256(NOTICE.encode('utf-8')).hexdigest()
                or profile.get('complete_wheel_frozen_pack_hash') != sha256(frozen.encode('utf-8')).hexdigest()):
            raise PaymentProfileError('complete_wheel_terms_not_approved')
    supplier_quotes = adapters['supplier_quotes']
    if (not storage.ready or not buyer.readiness()['ready'] or getattr(buyer, 'mode', None) != 'live'
            or stripe_config.get('SHOP_STRIPE_PUBLIC_ORIGIN') != buyer.public_origin):
        raise PaymentProfileError('payment_storage_unavailable')
    smtp = portal.get_werkstatt_smtp_config(include_local_hostname=False)
    if smtp.get('smtp_configured') is not True or smtp.get('from_address') != 'info@auto-lackierzentrum.de':
        raise PaymentProfileError('customer_mail_unavailable')
    with closing(storage.connect()) as db:
        init_payment_action_schema(db)
        init_fulfillment_schema(db)
        init_order_receipt_schema(db)
        init_order_workflow_schema(db)
        init_consumer_schema(db)
        if complete_wheel_review:
            from shop_order_review import init_order_review_schema
            init_order_review_schema(db)
    mail_service = Mailbox(portal.get_werkstatt_imap_config)
    outbox = MailOutbox(storage.connect, storage.directory / 'mail-outbox', mail_service)
    smtp_config = lambda: portal.get_werkstatt_smtp_config(include_local_hostname=False)
    pack, combined_checkout = checkout_terms(pack, profile.get('complete_wheel_frozen_pack_hash'))
    consumer = ShopConsumer(original_store, storage.connect, mode='live', public_origin=buyer.public_origin,
        pack=pack, enabled=True, withdrawals_enabled=True, delivery_enabled=True,
        outbox=outbox, smtp_config=smtp_config, sender_address='info@auto-lackierzentrum.de')
    if consumer.readiness().get('checkout_ready') is not True:
        raise PaymentProfileError('consumer_terms_or_withdrawal_unavailable')
    store = ConsumerOrderStore(original_store, consumer)
    gateway = StripeShopGateway(stripe_config)
    checkout = ShopCheckout(store, gateway, mode='live', public_origin=buyer.public_origin,
        enabled=True, order_lookup=lambda order_id: store.lookup_payment_owner(order_id, mode='live'))
    journal = FulfillmentJournal(storage.connect)
    holder = {}
    payment = PaymentActions(store, gateway, PaymentActionJournal(storage.connect),
        lambda order_id, **kwargs: holder['saga'].proof_resolver(order_id, **kwargs), mode='live', enabled=True)
    review = None
    if complete_wheel_review:
        from shop_order_review import OrderReviewJournal, ShopOrderReview
        review = ShopOrderReview(store, OrderReviewJournal(storage.connect), payment, mode='live', enabled=True)
        checkout.review_service = review
    policy = RSUOrderPolicy(mode='live', mode_confirmed_evidence='RSU-written-20261117-live',
        acceptance_policy_evidence='RSU-written-20261106-full-quantity-net-price',
        rejection_policy_evidence='RSU-written-20261106-production404', enabled=True, live_armed=True)
    transport = RSUOrderTransport(supplier_config, policy=policy,
        authorize=lambda envelope: holder['saga'].authorize_submission(envelope))
    # The published price book binds every quantity to the same base.
    # Only pre-policy quotes from a shop without that book need their original adapter.
    legacy_retail_quote = (supplier_quotes.retail_quote if adapters.get('published_prices') is None
                           and getattr(quotes, 'tyre_payment_costs', False) else None)
    def revalidate(snapshot, context, *, destinations):
        requests = [request for request, line in zip(snapshot['request'], snapshot['lines'])
                    if line['destination'] in destinations]
        selected = _frozen_price_service(quotes, snapshot, legacy_retail_quote)
        return selected.quote({'lines': requests}, customer_id=snapshot['customer_id'],
            vehicle_ref=snapshot['vehicle_ref'], delivery_context=context).snapshot()
    def address(destination, context):
        source = ({'recipient': 'Gärtner GmbH Karosserie & Lack', 'street': 'Alte Neckarelzer Straße 18/1',
                   'postcode': '74821', 'city': 'Mosbach', 'country': 'DE'} if destination == 'workshop'
                  else deepcopy(context['delivery']))
        salutation = 'Firma' if destination == 'workshop' else source.get('salutation')
        if salutation not in {'Firma', 'Herr', 'Frau'}:
            raise PaymentProfileError('delivery_address_unconfirmed')
        return {'neutraldelivery': 1, 'salutation': salutation,
                'name': source['recipient'], 'street': source['street'], 'zipcode': source['postcode'],
                'city': source['city'], 'countrycode': source['country']}
    saga = ShopFulfillment(store, journal, payment, transport, mode='live', enabled=True,
        supplier_policy={'account_fingerprint': transport.account_fingerprint,
                         'mode_evidence_id': policy.mode_confirmed_evidence,
                         'acceptance_evidence_id': policy.acceptance_policy_evidence},
        fresh_inquiry=supplier_quotes.fresh_inquiry, revalidate=revalidate, delivery_address=address,
        review_service=review, revalidate_remaining=partial(requote_remaining_goods, quotes,
                                                            legacy_retail_quote=legacy_retail_quote))
    holder['saga'] = saga
    workshop_order_create = getattr(portal, 'erstelle_shop_montageauftrag', None)
    workshop_order_lookup = getattr(portal, 'shop_montageauftraege', None)
    if not callable(workshop_order_create) or not callable(workshop_order_lookup):
        raise PaymentProfileError('workshop_order_integration_unavailable')
    receipts = ShopOrderReceipts(store, storage.connect, mode='live', enabled=True, delivery_enabled=True,
        sender_address='info@auto-lackierzentrum.de', outbox=outbox, smtp_config=smtp_config,
        legal_snapshot=consumer.contract_pack, require_legal=True, workshop_order_lookup=workshop_order_lookup)
    workflow = ShopOrderWorkflow(store, WorkflowJournal(storage.connect), saga, payment,
        mode='live', enabled=True, receipt_service=_ReceiptDelivery(receipts, consumer), review_service=review,
        workshop_order_create=workshop_order_create, workshop_order_lookup=workshop_order_lookup)
    wrapped = WorkflowCheckout(checkout, workflow)
    if (not wrapped.readiness()['ready'] or not receipts.readiness()['delivery_ready']
            or not payment.readiness()['ready']):
        raise PaymentProfileError('payment_workflow_unavailable')
    quotes.manual_mixed_orders = not combined_checkout
    adapters.update(consumer=consumer, guarded_store=store, gateway=gateway, checkout=wrapped,
        fulfillment=saga, receipts=receipts, workflow=workflow, outbox=outbox, purchasing_enabled=True)
    if review is not None:
        adapters['order_review'] = review
        from shop_checkout import _origin
        portal.app.config['SHOP_REVIEW_ADMIN_ORIGIN'] = _origin(profile.get('review_admin_origin', buyer.public_origin))
    # The ordinary runtime owns registration; callers must not start this
    # worker if any later blueprint/readiness setup fails.
    return {'order_store': store, 'checkout_service': wrapped, 'consumer_service': consumer,
            'workshop_order_view': receipts.customer_workshop_order}, workflow.start
