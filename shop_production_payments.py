"""Explicit production-only checkout wiring; never a testing entry point.

Requires a reviewed legal/delivery profile, dedicated Stripe configuration and
all durable adapters. Imports/construction never create a Stripe session,
supplier order, customer message or payment. The caller starts workers only
after the complete reviewed release is registered.
"""
from contextlib import closing
from copy import deepcopy

from mailbox_client import Mailbox
from mailbox_outbox import MailOutbox
from shop_checkout import ShopCheckout
from shop_consumer import ShopConsumer, init_consumer_schema
from shop_fulfillment import FulfillmentJournal, ShopFulfillment, init_fulfillment_schema
from shop_order_receipts import ShopOrderReceipts, init_order_receipt_schema
from shop_order_workflow import WorkflowJournal, ShopOrderWorkflow, WorkflowCheckout, init_order_workflow_schema
from shop_payment_actions import PaymentActions, PaymentActionJournal, init_payment_action_schema
from shop_rsu_orders import RSUOrderPolicy, RSUOrderTransport
from shop_stripe_gateway import StripeShopGateway
from reifen_rechtstexte import ANBIETER


class PaymentProfileError(ValueError):
    pass


class ConsumerOrderStore:
    """Enforce legal acceptance at the server-side order creation boundary."""
    def __init__(self, store, consumer):
        self.store, self.consumer = store, consumer

    def __getattr__(self, name):
        return getattr(self.store, name)

    def create_order(self, quote_hash, *, customer_id, mode, idempotency_key):
        self.consumer.require_acceptance(quote_hash, customer_id=customer_id)
        return self.store.create_order(quote_hash, customer_id=customer_id, mode=mode, idempotency_key=idempotency_key)


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
    mail_service = Mailbox(portal.get_werkstatt_imap_config)
    outbox = MailOutbox(storage.connect, storage.directory / 'mail-outbox', mail_service)
    smtp_config = lambda: portal.get_werkstatt_smtp_config(include_local_hostname=False)
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
    policy = RSUOrderPolicy(mode='live', mode_confirmed_evidence='RSU-written-20261117-live',
        acceptance_policy_evidence='RSU-written-20261106-full-quantity-net-price',
        rejection_policy_evidence='RSU-written-20261106-production404', enabled=True, live_armed=True)
    transport = RSUOrderTransport(supplier_config, policy=policy,
        authorize=lambda envelope: holder['saga'].authorize_submission(envelope))
    def revalidate(snapshot, context, *, destinations):
        requests = [request for request, line in zip(snapshot['request'], snapshot['lines'])
                    if line['destination'] in destinations]
        return quotes.quote({'lines': requests}, customer_id=snapshot['customer_id'],
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
        fresh_inquiry=supplier_quotes.fresh_inquiry, revalidate=revalidate, delivery_address=address)
    holder['saga'] = saga
    receipts = ShopOrderReceipts(store, storage.connect, mode='live', enabled=True, delivery_enabled=True,
        sender_address='info@auto-lackierzentrum.de', outbox=outbox, smtp_config=smtp_config,
        legal_snapshot=consumer.contract_pack, require_legal=True)
    workflow = ShopOrderWorkflow(store, WorkflowJournal(storage.connect), saga, payment,
        mode='live', enabled=True, receipt_service=_ReceiptDelivery(receipts, consumer))
    wrapped = WorkflowCheckout(checkout, workflow)
    if (not wrapped.readiness()['ready'] or not receipts.readiness()['delivery_ready']
            or not payment.readiness()['ready']):
        raise PaymentProfileError('payment_workflow_unavailable')
    adapters.update(consumer=consumer, guarded_store=store, gateway=gateway, checkout=wrapped,
        fulfillment=saga, receipts=receipts, workflow=workflow, outbox=outbox, purchasing_enabled=True)
    # The ordinary runtime owns registration; callers must not start this
    # worker if any later blueprint/readiness setup fails.
    return {'order_store': store, 'checkout_service': wrapped, 'consumer_service': consumer}, workflow.start
