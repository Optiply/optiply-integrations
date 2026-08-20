# tap-itsperfect

Read-only Singer tap for the ItsPerfect v3 API, built with `hotglue_singer_sdk`.

## Streams

| Stream | Endpoint | Primary key | Replication | Coverage |
|---|---|---|---|---|
| `products` | `/api/v3/items?includes=colors,barcodes` | `id` | `last_update_timestamp` | Products and variants/barcodes |
| `stocks` | `/api/v3/stock` | `id` | Full table | Stock by item and warehouse |
| `stores` | `/api/v3/stores` | `id` | Full table | Store/location reference |
| `warehouses` | `/api/v3/warehouses` | `id` | Full table | Warehouse reference |
| `vendors` | `/api/v3/vendors` | `id` | Full table | Suppliers |
| `sales_orders` | `/api/v3/sales_orders` | `id` | Full table | Sent (`1`) and cancelled (`2`) orders |
| `sales_order_lines` | `/api/v3/sales_orders/{id}/lines` | `sales_order_id,id` | Parent-scoped | Order/product join |
| `purchase_orders` | `/api/v3/purchase_orders` | `id` | Full table | Purchase orders |
| `purchase_order_lines` | `/api/v3/purchase_orders/{id}/lines` | `purchase_order_id,id` | Parent-scoped | Purchase-order items |
| `puts` | `/api/v3/puts` | `id` | Full table | Receipts/item deliveries |
| `put_lines` | `/api/v3/puts/{id}/lines` | `put_id,id` | Parent-scoped | Receipt items |
| `qualities` | `/api/v3/qualities` | `id` | Full table | Product quality reference |
| `quality_compositions` | `/api/v3/qualities/{id}/composition` | `quality_id,id` | Parent-scoped | Material compositions |

ItsPerfect documents page-number pagination through `X-Pagination-*` headers. The tap stops at the documented page count and fails on missing or non-progressing pagination. Authentication tokens are cached until shortly before expiry. Permanent 4xx responses are fatal; only network failures, 429, and documented transient 5xx responses are retried with finite backoff.

Products use an inclusive `last_update_timestamp` resume filter, so boundary records may repeat rather than be missed. Sales orders, purchase orders, and puts remain full-table because the API does not document that child-line changes advance the parent timestamp; this guarantees child traversal.

The API does not document a separate product BOM endpoint or deletion/change feed. Product status and active fields are emitted so downstream processing can handle disabled records. Full-table streams are intentional where a stable marker or parent/child cascade guarantee is not documented.

## Local use

```bash
python3.10 -m venv .venv
.venv/bin/pip install -e '.[test]'
cp config.json.example config.json
.venv/bin/tap-itsperfect --config config.json --discover > catalog.json
.venv/bin/pytest -q
```

`config.json`, Singer state, catalogs, and output are ignored. Never commit credentials.
