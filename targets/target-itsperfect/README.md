# target-itsperfect

Replay-safe Singer target for exporting Optiply buy orders to ItsPerfect v3 purchase orders.

## Direction and accepted streams

| Singer stream | Direction | Destination | Create | Update | Cancel/delete | Readback |
|---|---|---|---|---|---|---|
| `BuyOrders` | Optiply → ItsPerfect | `POST /api/v3/purchase_orders` | Supported, header and lines atomically | Rejected in v1 | Rejected in v1 | Lookup by integration `reference`, then GET lines |

All other streams fail explicitly. Products, stock, suppliers, sales orders, receipts, and compositions remain owned by the ItsPerfect tap. The target never calls purchase-order PUT or DELETE.

## Mapping

| Singer field | Destination field | Required | Transformation |
|---|---|---:|---|
| `id` / `optiplyId` | `reference` | Yes | Hardcoded `OPTIPLY-` + immutable Optiply ID |
| `customer_id` / `supplier_remoteId` / `targetSupplierId` | `vendor.id` | Yes | Positive integer |
| configured `warehouse_id` | `warehouse.id` | Yes | Positive integer |
| `transaction_date` / `order_date` / `date` | `date` | Yes | UTC/ISO value normalized to `YYYY-MM-DD` |
| `line_items[].product_remoteId` | `lines[].item_id` | Yes | String destination item ID |
| `line_items[].quantity` | `lines[].quantity` | Yes | Positive number |
| `line_items[].purchase_price` / `unit_price` / `price` | `lines[].price` | No | Positive decimal string |

The target does not set destination lifecycle status or order type; ItsPerfect owns those defaults. SKU is not assumed to be a barcode.

## Identity and replay safety

The immutable integration reference is `OPTIPLY-{buyOrder.id}` and is intentionally not configurable. Before every create, the target performs an exact, single-page lookup:

```text
GET /api/v3/purchase_orders?reference=OPTIPLY-{id}&includes=lines
```

- no match: submit one atomic purchase-order POST;
- identical match: reuse its remote ID without writing;
- changed match: fail because updates are intentionally unsupported;
- timeout, connection loss, 429, or transient 5xx during create: read back by reference and never blindly repost;
- no authoritative readback after an ambiguous create: persist the ambiguous reference and fail closed;
- retries of ambiguous records are lookup-only, including after restored target state.

A successful POST is not acknowledged until readback confirms its ID, supplier, warehouse, date, item identities, quantities, and supplied prices. Duplicate line identities are compared as a multiset rather than by array position.

## Reliability and state

- authentication tokens remain in memory and refresh before expiry;
- rejected credentials are permanent failures;
- GET/readback retries are bounded to four attempts and honor `Retry-After` up to 60 seconds;
- writes are serial within one process to avoid lookup/create races;
- deployment must enforce one active target job per ItsPerfect tenant because destination reference uniqueness is not independently proven;
- each header and its lines are one atomic destination request;
- SDK target state is updated only after confirmed `upsert_record` success;
- errors contain operation/status categories, not response bodies or raw records.

## Configuration

```json
{
  "api_url": "https://your-domain.itsperfect.it",
  "username": "your-username",
  "password": "your-password",
  "warehouse_id": 1,
  "request_timeout_seconds": 60
}
```

Username and password are secret config fields. Local config, Singer output, state, and target exception artifacts are ignored.

## Verification

```bash
python3.10 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest -q
```

Tests use redacted fixtures and mocked HTTP only. No live purchase order has been written.

## Unsupported and blocked capabilities

- updates are unsupported because safe line updates require destination line IDs and an approved field-mutability contract;
- cancellation and deletion are unsupported because ItsPerfect documents DELETE but not the business lifecycle or financial safety rules;
- receipts, inventory, products, and supplier writes are outside the approved target direction;
- overlapping target jobs for the same tenant are blocked until deployment enforces single-flight or ItsPerfect reference uniqueness is proven;
- deployment, HotGlue linking, scheduling, and production safety are not proven by this repository.
