# target-colleqtive

Singer target for exporting Optiply `BuyOrders` records to Colleqtive delivery orders.

Endpoint used:

- `POST /api/v2/public/orders/deliveries`

The target is built on `target-hotglue` and expects the HotGlue/Singer stream name `BuyOrders`.

## Auth

Preferred config uses Colleqtive's OAuth2 client-credentials flow:

```json
{
  "api_url": "https://bbq-test.colleqtive.net",
  "token_url": "https://login.microsoftonline.com/ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token",
  "client_id": "...",
  "client_secret": "...",
  "scope": "api://.../.default",
  "store_number": "001"
}
```

A pre-fetched `access_token`/`token` can also be supplied for local smoke tests.

## Mapping

Each Optiply buyOrder becomes one Colleqtive `orders[]` item:

- `store_number`: record `store_number`/`storeNumber`, falling back to config `store_number`
- `order_number`: record `order_number`/`orderNumber`/`remoteId`/`remote_id`/`reference`/`id`
- `supplier_name`: record `supplier_name`/`supplierName`/`supplier`, falling back to config `default_supplier_name`
- `no_of_containers`: record `no_of_containers`/`noOfContainers`, falling back to config `default_no_of_containers` (default `1`)
- `order_lines`: from `line_items`/`lineItems`/`order_lines`/`orderLines`/`lines`

Line mapping:

- `quantity`: `quantity`/`amount`/`qty`
- `product_number`: `product_number`/`productNumber`/`product_remoteId`/`productRemoteId`/`productId`/`product_id`/`skuCode`/`sku`/`articleCode`/`article_code`
- optional barcode, description, line number, container/customer/secondary order fields when present

Records missing required header fields or valid lines are skipped and logged instead of crashing the whole export batch.
