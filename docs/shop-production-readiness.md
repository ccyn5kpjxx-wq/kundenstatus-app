# New shared shop: production handoff

Status: 2026-11-17. The local catalogue/configurator is not yet connected to a production checkout. The new server implementation is registered with every feature disabled. No deployment, supplier order or real payment was performed.

## Implemented and tested locally

- `shop_catalog_public.py`: allowlisted retail projection, bounded catalogue pages, exact detail IDs, reviewed variant-specific fitments and released same-origin media/documents. Supplier prices, raw IDs, credentials and evidence stay private. Projections read only the price cache; optional warmup schedules a separately budgeted background read.
- `shop_cart_quotes.py`: authoritative mixed-cart quote, aggregate stock, confirmed RSU identity, 10% tyre/rim and 15% part/accessory markup, services once per assembly, exact vehicle evidence and separately confirmed customer/workshop freight. Unknown costs block completion; zero freight needs explicit confirmation. Frozen delivery context is bound to the quote.
- `shop_order_store.py`: immutable server quote/order ledger, transactional idempotency, one payment attempt and exact Session/Intent binding. A lost payment response requires reconciliation. Its private webhook-owner lookup is not exposed through HTTP.
- `shop_checkout.py` and `shop_stripe_gateway.py`: dedicated mode-bound Stripe transport and manual card authorization, frozen gross EUR line amounts, raw signed webhook checks, current Session/Intent verification and replay handling. A Checkout completion or browser return is not proof of captured funds. The Checkout transport does not perform supplier purchases, capture or refunds; the separate internal payment-action journal controls capture after fulfillment evidence.
- `shop_public_api.py`: bounded catalogue GETs, authenticated same-origin/CSRF quote/draft/checkout writes and a narrow signed webhook endpoint. Responses project only public fields and fixed errors.
- `shop_runtime.py`: explicit disabled registration beside existing portal integrations. Startup opens no shop database and performs no provider call. The stateless homepage can only read released catalogue endpoints; it cannot receive new order/payment writes.
- `shop_alzura_feed.py`: bounded offline reader for the inspected full tyre CSV. A separate publication allowlist is required. File timestamps cannot refresh source prices; raw partner evidence and media references remain private. Net/currency semantics need external confirmation and exact RSU mapping is not inferred.
- `shop_buyer.py` and `shop_storage.py`: private guest/address sessions and explicitly provisioned persistent SQLite; a missing or foreign database after initialization fails closed. Consistent backup snapshots exist but are not yet integrated with the portal backup workflow.
- `shop_frontend.py`, `shop_media.py` and `shop_price_cache.py`: bounded paginated frontend, unchanged approved images and budgeted background price reads. Exact quantity tiers and pending jobs are retained; catalogue GETs never perform provider IO. Media needs its own flag and released catalogue.
- `shop_rsu_match.py` and `shop_rsu_quotes.py`: exact manufacturer-SKU/geometry mapping evidence and verified read-only Inquiry prices. A private first release contains 481 tyre identities and 164 original photos; it has no invented current prices.
- `shop_payment_actions.py`, `shop_rsu_orders.py` and `shop_fulfillment.py`: durable internal capture/cancellation claims and one-position supplier commands. Every exact SKU/destination receives a stable reference and `neutraldelivery=1`. Capture requires full acceptance of all commands; partial/unknown outcomes require manual review. Execution remains disabled.

RSU confirmed on 17 November that its account test mode is now **off**. The existing access must not be used for further test orders. Shipping amounts are net. Customer parcels have no supplier invoice, delivery notes contain no prices, and neutral delivery uses `NeutralDelivery=1`. Package counts cannot be known in advance. The API accepts only one position per order, so shipping thresholds/fees and quantity-specific prices are calculated per exact SKU and destination, with aggregate stock checked across the cart. Customer shipping receives 15% markup and VAT; goods retain 10% tyre/rim and 15% parts/accessory markup. Unknown destination/package eligibility still blocks quoting.

Synthetic integration covers four tyres, four rims, mounting, oil, a painted body part and both delivery groups: 1,484.49 EUR total, with mounting counted once. SDK HTTP is replaced by a fake client; real signature verification is exercised using synthetic HMACs and temporary SQLite. No credentials or real records are used.

Validation on 17 November: **289 synthetic tests passed in the combined shop suite**. After the final media/runtime wiring, **17 focused runtime/media/integration tests passed**. The earlier isolated portal smoke test passed with network blocked and temporary database/uploads. The source changes do not enable a production profile. No full production bootstrap or real payment-provider sandbox round trip has been claimed.

## Provider and deployment observations

- ALZURA's 16 November reply confirms activation. The account export page now lists complete German tyre and alloy files, shown as updated 16 November at 12:14. The full tyre ZIP was downloaded privately and contains 144,111 unique rows, including 97,090 S/W/G tyre entries. Its 44-column schema has no EAN, explicit RSU ID, currency or source timestamp.
- The alloy download failed with Chrome `ERR_BLOCKED_BY_CLIENT`; no alloy archive was imported. No browser protection was disabled.
- The official export documentation describes separate single-item and four-or-more price columns, but the reviewed CSV does not itself establish net EUR semantics, two/three-unit pricing or source timezone. Do not infer procurement pricing from a marketplace minimum.
- The approved RSU inquiry was sent in the existing mail thread. Its 17 November response confirms production mode and the shipping/parcel conditions recorded above. The public shipping table was rechecked; islands still need a confirmed package count before an exact charge can be quoted.
- The user approved Render workspace Gaertner. The last observed portal deployment was live at `af9970e5`, health check HTTP 200. TomorrowWorks project 1 was read after login; the visible project notes were older than that build. No deployment claim was acquired and no deploy/restart was triggered. Refresh both contexts before any live action.

Official references: [ALZURA export documentation](https://tyre24.alzura.com/assets/pdf/Article_Export_Technical_Documentation_De.pdf), [RSU shipping conditions](https://www.tyresystem.de/lieferbedingungen), [Stripe manual authorization](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method), [Stripe webhook verification](https://docs.stripe.com/events/manage-webhook-endpoints).

## Verified current boundaries

- The shared cart always reports `orderEnabled: false`, `paymentEnabled: false` and `freightConfirmed: false`. Its payment choices are local UI previews; there is no checkout route in its loopback server.
- The existing tyre Stripe adapter is a separate flow, uses card Checkout and signed webhooks, and cannot accept the new mixed cart unchanged.
- PayPal is not wired into the shared cart. Wallet/account availability has not been verified in the live Stripe account.
- Local fresh-price and customer-account clients restrict themselves to loopback origins. Public deployment requires server integration rather than copying the preview tree.
- Public quote projection whitelists gross retail prices, stock and timestamps. Private raw catalogues, provider evidence, auth files, account databases, logs and customer photos must remain outside public assets and Git.
- The local output is approximately 613 MiB and includes very large inline catalogue documents. A deliberate public export with pagination and bounded JSON data is needed.

## Required implementation sequence

1. Wire the implemented server boundaries to real reviewed product identities, current retail quotes and exact RSU purchasing mappings. Confirm actual export field semantics and source times, fetch the alloy archive and implement current acquisition. Keep supplier net costs private. Use confirmed painting tariffs.
2. Wire the confirmed per-position freight policy and neutral delivery configuration; verify destination eligibility and send Gärtner's customer invoice independently. Route assembly and painted bodywork to the workshop; route eligible goods directly to the customer. Mounting date is agreed after goods arrive.
3. Select and verify production persistence/backups. The new journal explicitly requires SQLite and must not blindly receive `portal.get_db()`, which can return PostgreSQL. Wire the real buyer session/CSRF and frozen delivery context; client prices never establish acceptance or payment.
4. Configure a dedicated Stripe mode/key/webhook only after the actual checkout/legal/invoice flow is wired. Connect the implemented fulfillment/capture journal and authorization-deadline handling. A real Stripe sandbox round trip remains unverified; mock supplier behavior for that test because the existing RSU access is now live. Add PayPal only after account activation and an integrated test; wallets are not verified active.
5. Connect real orders and released invoices to the authenticated customer account. Do not publish seeded demo history as actual purchases.
6. Produce a public catalogue export with production URLs, fresh-price APIs and no draft pages. Preserve the approved studio/configurator and progressive brand-first parts browser when moving the local design into the paginated frontend. The local parts preview shows exact reviewed vehicles, fresh offer stock and an explicitly unconfirmed ETA; no paid supplier-list endpoint is called. Verify public assets, publication rights and relevant legal/checkout pages.
7. Before deployment, read current TomorrowWorks incidents and deployment claims; coordinate the release and announce the persistent-disk service restart. Verify the exact Render workspace, service and commit, then monitor deployment and test the actual live pages.

## Authorization retained

The user authorized publishing the completed shop. No real supplier order or live payment may be generated during implementation or testing. No extra subscriptions or supplier contracts are authorized by this publication request. Do not publish the local demo as a functioning paid shop.
