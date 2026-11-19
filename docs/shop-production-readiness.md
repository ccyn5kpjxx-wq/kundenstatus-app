# Shared shop: production handoff

Status: 2026-11-17, release source `048581573b7b1cbe49b6a9eabc226ecc04b50e13`. The production catalogue, buyer, quote, payment and fulfillment boundaries are implemented and have synthetic coverage. A real Stripe **test-mode** card authorization and subsequent **test-mode capture** succeeded with simulated supplier and mail adapters. This is not a completed production checkout acceptance test. No deployment, real supplier order, real customer email or live payment was performed. No production checkout flag has been activated.

## Prepared catalogue and prices

- The private reviewed release contains **2,943 product identities**: 1,665 tyres, 488 alloy-wheel variants, one steel-wheel variant, 590 accessories, 72 oils, 55 body parts and 72 care products. These categories differ from storefront departments. The wheel browser groups variants into **126 wheel families**. This prepared selection is not a claim to contain the complete supplier catalogue.
- **483 exact RSU procurement bindings** are prepared: **481 tyres and two BORBET V wheel variants**. This is not the count of wheel reports or confirmed fitments. Products without a confirmed RSU binding may have an explicitly sourced browsing price; they cannot silently become purchasable through RSU.
- Wheel evidence contains **two exact vehicle/report variants**, five model/type associations in total and four original PDFs. A model association without confirmed years or HSN/TSN cannot satisfy those filters. Exact variant conditions still apply. Reviewed reports are hash-bound local originals with no network replacement fallback.
- Public projections allowlist retail fields and same-origin media/documents. Raw supplier IDs, buying prices, credentials, customer records and private evidence remain private. Current prices use scoped RSU/ALZURA caches with expiry, stock and exact quantity tiers. Imported example prices cannot revive a missing current quote. Browsing GETs do not call providers.
- Goods retain **10% markup for tyres/rims and 15% for parts/accessories**. Customer freight separately adds **15% to net shipping costs, then VAT**. Unknown prices, fees, stock, required documents or destination eligibility block a payable quote.

## Implemented server boundaries

- `shop_cart_quotes.py` freezes quantities, services, assembly evidence and validated delivery context. Price tiers and freight apply per exact SKU/destination, with stock checked across all destinations. Services are counted once. Browser totals never determine the charge.
- `shop_order_store.py` stores trusted server quotes and immutable orders with transactional idempotency. One payment attempt binds the exact customer, mode, amount, currency, Session and Intent. Lost responses require reconciliation, not another create request.
- `shop_checkout.py`, `shop_stripe_gateway.py` and `shop_payment_actions.py` implement manual card authorization, signed-event verification, current provider-state checks and durable capture/cancellation claims. Browser returns and Checkout completion are not proof of capture. Cancellation is not a refund implementation.
- `shop_rsu_orders.py` and `shop_fulfillment.py` use one exact position per supplier order, stable references, exact quantities/net unit prices and fixed `neutraldelivery=1`. Capture requires full persisted acceptance. Partial/unknown outcomes require manual review rather than new orders.
- `shop_order_workflow.py` and `shop_order_receipts.py` provide the durable authorization/fulfillment queue and frozen customer receipt through the existing durable mail outbox. They do not invent customer invoices. Order/status and staff withdrawal projections enforce ownership/admin access.
- `shop_buyer.py` provides separate opaque guest sessions, server-held addresses and same-origin CSRF protection. Portal-admin sessions do not become retail customers. A full recurring retail/business account system remains separate work; demo purchase history is not published.
- `shop_consumer.py` freezes the privately reviewed mixed-retail legal pack and quote-bound acceptance, serves legal pages and records two-step withdrawal declarations for manual review. The original guarantee-notice attachment is prepared. A declaration never automatically cancels a supplier order or refunds a payment.
- `shop_storage.py` uses dedicated persistent SQLite, not the portal's possibly PostgreSQL connection. Authenticated backups include its snapshot, initialization marker and bounded referenced outbox files/journals. Active/changing mail delivery fails backup conservatively. The old portal-only importer rejects shop financial state. Recovery must preserve ledger, marker and outbox together and reconcile external state before actors restart.
- `shop_production.py` and `shop_runtime.py` require an explicit hash-verified private release and a separate complete payment profile. Import/default registration performs no shop provider operation. Failed preparation closes purchasing gates while preserving the portal.
- The paginated frontend retains grouped wheel variants, the wheel studio, click zoom, compatible-tyre selection and shared cart. Anonymous tracking reuses Werkstattcockpit counters without product IDs, prices, basket contents, vehicle data or customer photos. Montage requests use the real persistent inquiry form; opening a dialog does not reserve a slot.

## Confirmed operating rules

RSU confirmed on 17 November that its account test mode is **off**. Existing credentials must not be used for further test orders. Shipping amounts are net. Parcels contain no supplier invoice; delivery notes contain no prices. Neutral delivery uses `neutraldelivery=1`. Only one position is accepted per order and parcel count is unknown in advance. Island or unconfirmed package/destination charges remain blocked until the exact cost is known.

The merchant's delivery commitment is **five working days after order acceptance**, defined as Monday to Friday excluding Baden-Wuerttemberg public holidays. This is not an invented RSU ETA. Montage can be purchased with the goods; the appointment is agreed after delivery clarification. Assembly and painted body parts route to the workshop; eligible unprocessed goods can route to the customer.

Loose buyable articles can share one checkout when every position, service, delivery and freight charge is confirmed. One card authorization covers the full server quote. RSU still receives one order per supplier position. Capture requires all positions to have been accepted at the agreed price. A partial or ambiguous supplier result goes to manual review. Inquiry-only articles remain outside the charge. Mounted packages keep their technical review before procurement. The joint purchase flow is enabled only with its matching frozen consumer terms.

## Validation evidence and limits

- The previous combined synthetic shop suite passed **450 tests**. At `2589d5d5`, **86/86 focused tests passed**: documents 11, catalogue 22, consumer 13, frontend 11, production bootstrap 16 and payment bootstrap 13. Sockets were blocked; persistence was disposable.
- At merged source `04858157`, the actual-full-app isolated smoke passed **nine shop pages**, **three legacy redirects**, public/private host isolation, **five disabled financial POST routes**, three anonymous click counters and ten homepage/shop analytics sections. No shop ledger was created and financial workers stayed disabled.
- Rental/startup regressions on the same merged source passed **37/37**: rental production 19, booking-error handling 8 and portal startup locking 10. Separate processes used temporary storage with provider environment removed, sockets and DNS blocked.
- The real Stripe sandbox reached manual card authorization and **test capture**. The persisted result is test-mode `paid`, with **three fake supplier submissions and one fake SMTP submission**, zero real supplier calls, zero real emails and zero live payments. Replay protection was exercised.
- The sandbox verified a **locally synthesized signed event** against retrieved test-provider state. **Actual Stripe-to-application webhook delivery was not exercised**. This is not an end-to-end production webhook test.
- Private catalogue and reviewed consumer-pack hashes were verified. The last local preparation had buyer/quote activation false, no checkout request, financial activation flags false, no Stripe configuration reference and no provisioned shop ledger/marker. These local observations do not establish current Render configuration.

## Remaining production acceptance

1. Install the approved public/private bundle and original assets with hash checks, publication rights and scoped current-price configuration. Verify public pages, homepage links and analytics after deployment. Do not publish the old large loopback preview tree.
2. Provision and verify the persistent ledger/marker/outbox, authenticated backup and recovery process. Confirm single-worker configuration before arming financial actors.
3. Supply dedicated **live** Stripe credentials, endpoint signing secret and fixed HTTPS origin, plus the complete reviewed payment, delivery, consumer and mail profile. Test-key permission changes do not enable live checkout. Verify actual webhook delivery without generating a live payment and mail readiness without unsolicited customer messages.
4. Verify the complete activation boundary: trusted buyer context, current quote, cart eligibility, legal acceptance, authorization expiry, exact supplier acceptance for all positions, capture and durable contract confirmation. Keep checkout off if any dependency is absent. Never use the now-live RSU order endpoint for an agent test.
5. Preserve explicit customer invoice/return handling. PayPal is not integrated; Apple Pay/other wallet availability is not verified in the live account. Do not advertise unverified payment methods or fabricate invoice/account history.
6. Refresh TomorrowWorks incidents/deployment claims and the exact Render workspace, service and commit before publishing. The user selected workspace **Gaertner**. Coordinate any persistent-disk restart and monitor the result. No deployment or production activation is claimed here.

Official technical references: [ALZURA exports](https://tyre24.alzura.com/assets/pdf/Article_Export_Technical_Documentation_De.pdf), [RSU shipping](https://www.tyresystem.de/lieferbedingungen), [Stripe manual authorization](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method), [Stripe webhook verification](https://docs.stripe.com/events/manage-webhook-endpoints).

## Authorization retained

The user authorized publishing the completed shop. No real supplier order or live payment may be generated during implementation or testing. No extra paid service or supplier contract is authorized by publication. A prepared catalogue or successful sandbox capture must not be presented as a fully accepted paid production shop.
