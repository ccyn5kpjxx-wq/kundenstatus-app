# RSU parts direct sales

The existing release is unchanged unless `current.json` explicitly contains
`parts_direct_sales_approved: true` and a pinned `parts_direct_sales` record:

```json
{
  "parts_direct_sales_approved": true,
  "parts_direct_sales": {
    "path": "parts-direct.private.json",
    "sha256": "<sha256>",
    "base_bundle_sha256": "<original bundle sha256>"
  },
  "parts_vehicle_fitments": {
    "source": {"file": "parts-vehicle-source.private.json", "sha256": "<sha256>"},
    "approvals": {"file": "parts-conditions.private.json", "sha256": "<sha256>"}
  }
}
```

The parts overlay adds exact supplier bindings, real RSU price seeds and an
explicit freight record per SKU. Original sources live under `parts-sources/`.
The loader verifies their bytes, rebuilds identity from original product views,
and checks price, stock and timestamps against separate successful read-only
GET receipts. Normalized JSON cannot authenticate its own claims. Existing
products, publication, media, documents and legacy freight classes are preserved.
This initial activation contract requires a valid exact EAN. Maker/MPN-only and
CSV candidates remain inactive until sale-unit/packing and original observation
time have their own verified source contract.

Material retail price is the RSU net price × 1.15 × 1.19, rounded to cents.
Published prices use the existing five-day pricebook. Display reads do not wait
for supplier requests; checkout still validates exact-quantity stock and supplier
cost. Freight uses that private retained inquiry and adds no extra API reads.

The approved merchant freight rule `merchant_fixed_parts_freight_v1` charges
821 cents per SKU position for direct delivery or workshop collection, and
1642 cents for supplier → workshop → customer delivery. It requires an explicit
identity/source-bound standard-item whitelist and acceptance of higher supplier
costs by the merchant. The activation loader checks that every retained original
offer for the whitelisted SKU shows 6 EUR net freight; unknown or bulky offers
are excluded. The fixed customer fee is not represented as current supplier cost.
The separate observed-freight rule remains limited to one exact q1 offer and its
original 15-minute UI/GET window, including pinned original capture time.

Vehicle parts need a separately reviewed exact vehicle association. The source
query alone does not approve fitment. Conditions, production months, PR/VIN
restrictions and any compulsory certificate must be reviewed before activating
that product/vehicle pair. The buyer sends only a canonical server-listed vehicle
reference; choosing a make, free text or another vehicle cannot create approval.
Optional `public_notices` in the conditions profile are identity-bound
`{product_id: {identity_hash, notes}}` records. They are validated before database
work and rendered as plain text in the article dialog.

Existing mixed-cart manual confirmation, complete-wheel review, service pricing,
payment authorization/capture, idempotency and paid-only Ads conversion policies
are unchanged. Missing prices, freight, conditions or stock do not become
invented defaults or supplier identifiers. Provider credentials and private
release/source/approval files must never be committed.

Validation uses isolated temporary storage and synthetic suppliers/payments.
No RSU order endpoint or live Stripe payment is required to test this change.
