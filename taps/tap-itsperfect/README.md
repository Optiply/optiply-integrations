# tap-itsperfect

Read-only Singer tap for the ItsPerfect v3 API, built with `hotglue_singer_sdk`.

## Streams

| Stream | Endpoint | Primary key | Replication | Coverage |
| --- | --- | --- | --- | --- |
| `products` | `/api/v3/items?includes=colors,barcodes` | `id` | `last_update_timestamp` | Products and variants/barcodes |
| `stocks` | `/api/v3/stock` | `id` | `availability_timestamp` | Stock by item and warehouse |
| `stores` | `/api/v3/stores` | `id` | Full table | Store/location reference |
| `warehouses` | `/api/v3/warehouses` | `id` | Full table | Warehouse reference |
| `vendors` | `/api/v3/vendors` | `id` | Full table | Suppliers |
| `sales_orders` | `/api/v3/sales_orders` | `id` | `last_update_timestamp` | Sent (`1`) and cancelled (`2`) orders |
| `sales_order_lines` | `/api/v3/sales_orders/{id}/lines` | `sales_order_id,id` | Incremental parent-scoped | Order/product join |
| `purchase_orders` | `/api/v3/purchase_orders` | `id` | `last_update_timestamp` | Purchase orders |
| `purchase_order_lines` | `/api/v3/purchase_orders/{id}/lines` | `purchase_order_id,id` | Incremental parent-scoped | Purchase-order items |
| `puts` | `/api/v3/puts` | `id` | `last_update_timestamp` | Receipts/item deliveries |
| `put_lines` | `/api/v3/puts/{id}/lines` | `put_id,id` | Incremental parent-scoped | Receipt items |
| `qualities` | `/api/v3/qualities` | `id` | Full table | Product quality reference |
| `quality_compositions` | `/api/v3/qualities/{id}/composition` | `quality_id,id` | Parent-scoped | Material compositions |

ItsPerfect documents page-number pagination through `X-Pagination-*` headers. The tap stops at the documented page count and fails on missing or non-progressing pagination. Authentication tokens are cached until shortly before expiry. Permanent 4xx responses are fatal; only network failures, 429, and documented transient 5xx responses are retried with finite backoff.

Incremental streams use inclusive resume filters, so boundary records may repeat rather than be missed. Bounded read-only API checks confirmed `last_update_timestamp` filtering for products, sales orders, purchase orders, and puts, plus `availability_timestamp` filtering for stocks. Child lines are traversed for parents returned by the incremental parent stream.

Stores, warehouses, vendors, and qualities remain full-table because their records expose no stable source cursor. The API does not document a separate product BOM endpoint or deletion/change feed. Product status and active fields are emitted so downstream processing can handle disabled records.

## Local use

```bash
python3.10 -m venv .venv
.venv/bin/pip install -e '.[test]'
cp config.json.example config.json
.venv/bin/tap-itsperfect --config config.json --discover > catalog.json
.venv/bin/pytest -q
```

`config.json`, Singer state, catalogs, and output are ignored. Never commit credentials.
