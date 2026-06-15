import json
import requests
from singer_sdk import typing as th
from typing import Optional, Any, Dict, Iterable
from tap_exact.client import ExactStream
from tap_exact.client_sync import ExactSyncStream
from datetime import timedelta

with open("config.json", "r") as jsonfile:
    data = json.load(jsonfile)

if "sync_endpoints" in data.keys():
    use_sync_endpoint = data["sync_endpoints"]
else:
    use_sync_endpoint = False


class DynamicStream(ExactSyncStream if use_sync_endpoint else ExactStream):
    pass


class ItemsStream(DynamicStream):
    name = "items"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("AverageCost", th.StringType),
        th.Property("Code", th.StringType),
        th.Property(
            "CopyRemarks",
            th.StringType,
        ),
        th.Property("CostPriceCurrency", th.StringType),
        th.Property("CostPriceNew", th.StringType),
        th.Property("CostPriceStandard", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EndDate", th.DateTimeType),
        th.Property("Description", th.StringType),
        th.Property("ExtraDescription", th.StringType),
        th.Property("FreeBoolField_01", th.BooleanType),
        th.Property("FreeBoolField_02", th.BooleanType),
        th.Property("FreeBoolField_03", th.BooleanType),
        th.Property("FreeBoolField_04", th.BooleanType),
        th.Property("FreeBoolField_05", th.BooleanType),
        th.Property("FreeDateField_01", th.DateTimeType),
        th.Property("FreeDateField_02", th.DateTimeType),
        th.Property("FreeDateField_03", th.DateTimeType),
        th.Property("FreeDateField_04", th.DateTimeType),
        th.Property("FreeDateField_05", th.DateTimeType),
        th.Property("FreeNumberField_01", th.StringType),
        th.Property("FreeNumberField_02", th.StringType),
        th.Property("FreeNumberField_03", th.StringType),
        th.Property("FreeNumberField_04", th.StringType),
        th.Property("FreeNumberField_05", th.StringType),
        th.Property("FreeNumberField_06", th.StringType),
        th.Property("FreeNumberField_07", th.StringType),
        th.Property("FreeNumberField_08", th.StringType),
        th.Property("FreeTextField_01", th.StringType),
        th.Property("FreeTextField_02", th.StringType),
        th.Property("FreeTextField_03", th.StringType),
        th.Property("FreeTextField_04", th.StringType),
        th.Property("FreeTextField_05", th.StringType),
        th.Property("FreeTextField_06", th.StringType),
        th.Property("FreeTextField_07", th.StringType),
        th.Property("FreeTextField_08", th.StringType),
        th.Property("FreeTextField_09", th.StringType),
        th.Property("FreeTextField_10", th.StringType),
        th.Property("GLCosts", th.StringType),
        th.Property("GLCostsCode", th.StringType),
        th.Property("GLCostsDescription", th.StringType),
        th.Property("GLRevenueCode", th.StringType),
        th.Property("GLRevenue", th.StringType),
        th.Property("GLRevenueDescription", th.StringType),
        th.Property("GLStockCode", th.StringType),
        th.Property("GLStockDescription", th.StringType),
        th.Property("GLStock", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("IsBatchItem", th.StringType),
        th.Property(
            "IsFractionAllowedItem",
            th.BooleanType,
        ),
        th.Property("IsMakeItem", th.StringType),
        th.Property(
            "IsNewContract",
            th.StringType,
        ),
        th.Property(
            "IsOnDemandItem",
            th.StringType,
        ),
        th.Property(
            "IsPackageItem",
            th.BooleanType,
        ),
        th.Property(
            "IsPurchaseItem",
            th.BooleanType,
        ),
        th.Property(
            "IsSalesItem",
            th.BooleanType,
        ),
        th.Property("IsSerialItem", th.BooleanType),
        th.Property("IsStockItem", th.BooleanType),
        th.Property("IsSubcontractedItem", th.BooleanType),
        th.Property("IsTaxableItem", th.BooleanType),
        th.Property("IsTime", th.BooleanType),
        th.Property("Class_01", th.StringType),
        th.Property("Class_02", th.StringType),
        th.Property("Class_03", th.StringType),
        th.Property("Class_04", th.StringType),
        th.Property("Class_05", th.StringType),
        th.Property("Class_06", th.StringType),
        th.Property("Class_07", th.StringType),
        th.Property("Class_08", th.StringType),
        th.Property("Class_09", th.StringType),
        th.Property("Class_10", th.StringType),
        th.Property("Barcode", th.StringType),
        th.Property(
            "IsWebshopItem",
            th.StringType,
        ),
        th.Property("ItemGroup", th.StringType),
        th.Property("ItemGroupCode", th.StringType),
        th.Property("ItemGroupDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property(
            "SecurityLevel",
            th.StringType,
        ),
        th.Property(
            "StandardSalesPrice",
            th.StringType,
        ),
        th.Property("Stock", th.DateTimeType),
        th.Property("Unit", th.DateTimeType),
        th.Property("UnitDescription", th.StringType),
        th.Property("UnitType", th.StringType),
        th.Property("GrossWeight", th.StringType),
        th.Property("NetWeight", th.StringType),
        th.Property("NetWeightUnit", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("PictureName", th.StringType),
        th.Property("PictureUrl", th.StringType),
        th.Property("PictureThumbnailUrl", th.StringType),
        th.Property("SalesVatCode", th.StringType),
        th.Property("SalesVatCodeDescription", th.StringType),
        th.Property("SearchCode", th.StringType),
        th.Property("SecuriytLevel", th.StringType),
        th.Property("StartDate", th.StringType),
        th.Property("StatisticalCode", th.StringType),
        th.Property("StatisticalNetWeight", th.StringType),
        th.Property("StatisticalUnits", th.StringType),
        th.Property("StatisticalValue", th.StringType),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def path(self):
        type = "sync" if self.sync_endpoint else "bulk"
        return f"/{type}/Logistics/Items"

    @property
    def select(self):
        if self.sync_endpoint:
            return "ID,Timestamp,AverageCost,Barcode,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CopyRemarks,CostPriceCurrency,CostPriceNew,CostPriceStandard,Created,Creator,CreatorFullName,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,GLCosts,GLCostsCode,GLCostsDescription,GLRevenue,GLRevenueCode,GLRevenueDescription,GLStock,GLStockCode,GLStockDescription,GrossWeight,IsBatchItem,IsFractionAllowedItem,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NetWeight,NetWeightUnit,Notes,PictureName,PictureThumbnailUrl,PictureUrl,SalesVatCode,SalesVatCodeDescription,SearchCode,SecurityLevel,StartDate,StatisticalCode,StatisticalNetWeight,StatisticalUnits,StatisticalValue,Stock,Unit,UnitDescription,UnitType,Timestamp"
        return "ID,AverageCost,Barcode,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CopyRemarks,CostPriceCurrency,CostPriceNew,CostPriceStandard,Created,Creator,CreatorFullName,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,GLCosts,GLCostsCode,GLCostsDescription,GLRevenue,GLRevenueCode,GLRevenueDescription,GLStock,GLStockCode,GLStockDescription,GrossWeight,IsBatchItem,IsFractionAllowedItem,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NetWeight,NetWeightUnit,Notes,PictureName,PictureThumbnailUrl,PictureUrl,SalesVatCode,SalesVatCodeDescription,SearchCode,SecurityLevel,StandardSalesPrice,StartDate,StatisticalCode,StatisticalNetWeight,StatisticalUnits,StatisticalValue,Stock,Unit,UnitDescription,UnitType"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "item_id": record["ID"],
            "division_id": record["Division"],
        }


class SalesOrderStream(DynamicStream):
    name = "sales_order"
    primary_keys = ["OrderID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property(
            "AmountDC",
            th.StringType,
        ),
        th.Property(
            "AmountFC",
            th.StringType,
        ),
        th.Property(
            "AmountFCExclVat",
            th.StringType,
        ),
        th.Property(
            "DeliverToContactPerson",
            th.StringType,
        ),
        th.Property(
            "DeliverToContactPersonFullName",
            th.StringType,
        ),
        th.Property(
            "DeliverToName",
            th.StringType,
        ),
        th.Property(
            "OrderedByName",
            th.StringType,
        ),
        th.Property(
            "TaxSchedule",
            th.StringType,
        ),
        th.Property(
            "WarohouseID",
            th.StringType,
        ),
        th.Property(
            "YourRef",
            th.StringType,
        ),
        th.Property(
            "AmountDiscount",
            th.StringType,
        ),
        th.Property(
            "AmountDiscountExclVat",
            th.StringType,
        ),
        th.Property(
            "ApprovalStatus",
            th.StringType,
        ),
        th.Property("ApprovalStatusDescription", th.StringType),
        th.Property(
            "Approved",
            th.StringType,
        ),
        th.Property(
            "Approver",
            th.StringType,
        ),
        th.Property(
            "ApproverFullName",
            th.StringType,
        ),
        th.Property(
            "Created",
            th.DateTimeType,
        ),
        th.Property(
            "Creator",
            th.StringType,
        ),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property(
            "DeliverTo",
            th.StringType,
        ),
        th.Property(
            "DeliveryDate",
            th.DateTimeType,
        ),
        th.Property(
            "DeliveryStatus",
            th.StringType,
        ),
        th.Property("DeliveryStatusDescription", th.StringType),
        th.Property(
            "DeliveryAddress",
            th.StringType,
        ),
        th.Property("Description", th.StringType),
        th.Property(
            "OrderDate",
            th.DateTimeType,
        ),
        th.Property(
            "OrderedBy",
            th.StringType,
        ),
        th.Property(
            "OrderedNyName",
            th.StringType,
        ),
        th.Property(
            "OrderID",
            th.StringType,
        ),
        th.Property(
            "OrderNumber",
            th.StringType,
        ),
        th.Property(
            "Salesperson",
            th.StringType,
        ),
        th.Property(
            "Status",
            th.StringType,
        ),
        th.Property("StatusDescription", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseID", th.StringType),
        th.Property("WarehouseDescription", th.StringType),
        th.Property("Timestamp", th.StringType),
        th.Property("Modified", th.DateTimeType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/SalesOrder/SalesOrderHeaders"
        else:
            return f"/bulk/SalesOrder/SalesOrders"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"OrderID,AmountDC,ID,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,ApprovalStatus,ApprovalStatusDescription,Approved,Approver,ApproverFullName,Created,Creator,CreatorFullName,Currency,DeliverTo,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,DeliveryAddress,DeliveryDate,DeliveryStatus,DeliveryStatusDescription,Description,OrderDate,OrderedBy,OrderedByName,OrderNumber,Salesperson,Status,StatusDescription,WarehouseCode,WarehouseDescription,WarehouseID,YourRef,Timestamp,Modified"
        else:
            return f"OrderID,AmountDC,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,ApprovalStatus,ApprovalStatusDescription,Approved,Approver,ApproverFullName,Created,Creator,CreatorFullName,Currency,DeliverTo,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,DeliveryAddress,DeliveryDate,DeliveryStatus,DeliveryStatusDescription,Description,OrderDate,OrderedBy,OrderedByName,OrderNumber,Salesperson,Status,StatusDescription,TaxSchedule,WarehouseCode,WarehouseDescription,WarehouseID,YourRef,Modified"

    @property
    def filter(self):
        use_multiple_warehouses = self.config.get(
            "use_sales_orders_multiple_warehouses"
        )
        if self.default_warehouse_uuid and not use_multiple_warehouses:
            warehouse_uuid = self.default_warehouse_uuid
            return f"WarehouseID eq guid'{warehouse_uuid}'"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "order_id": record["OrderID"],
        }


class PurchaseOrdersStream(DynamicStream):
    name = "purchase_orders"
    primary_keys = ["PurchaseOrderID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("AmountFC", th.StringType),
        th.Property("ApprovalStatus", th.StringType),
        th.Property("ApprovalStatusDescription", th.StringType),
        th.Property("Approved", th.StringType),
        th.Property("Approver", th.StringType),
        th.Property("ApproverFullName", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("CostCenterCode", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("CostUnitCode", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("DeliveryAccount", th.StringType),
        th.Property("DeliveryAccountCode", th.StringType),
        th.Property("DeliveryAccountName", th.StringType),
        th.Property("DeliveryAddress", th.StringType),
        th.Property("DeliveryContact", th.StringType),
        th.Property("DeliveryContactPersonFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Discount", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Document", th.StringType),
        th.Property("DocumentNumber", th.StringType),
        th.Property("DocumentSubject", th.StringType),
        th.Property("DropShipment", th.BooleanType),
        th.Property("ExchangeRate", th.StringType),
        th.Property("Expense", th.StringType),
        th.Property("ExpenseDescription", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("IncotermAddress", th.StringType),
        th.Property("IncotermCode", th.StringType),
        th.Property("IncotermVersion", th.StringType),
        th.Property("InvoicedQuantity", th.StringType),
        th.Property("InvoiceStatus", th.StringType),
        th.Property("IsBatchNumberItem", th.StringType),
        th.Property("IsSerialNumberItem", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemBarcode", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemDivisable", th.BooleanType),
        th.Property("LineNumber", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("NetPrice", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("OrderDate", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("OrderStatus", th.StringType),
        th.Property("PaymentCondition", th.StringType),
        th.Property("PaymentConditionDescription", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("ProjectCode", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("PurchaseAgent", th.StringType),
        th.Property("PurchaseAgentFullName", th.StringType),
        th.Property("PurchaseOrderID", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("QuantityInPurchaseUnits", th.StringType),
        th.Property("Rebill", th.BooleanType),
        th.Property("PurchaseOrderLineCount", th.StringType),
        th.Property("ReceiptDate", th.StringType),
        th.Property("ReceivedQuantity", th.StringType),
        th.Property("ReceiptStatus", th.StringType),
        th.Property("Remarks", th.StringType),
        th.Property("SalesOrder", th.StringType),
        th.Property("SalesOrderLine", th.StringType),
        th.Property("SalesOrderLineNumber", th.StringType),
        th.Property("SalesOrderNumber", th.StringType),
        th.Property("SelectionCode", th.StringType),
        th.Property("SelectionCodeCode", th.StringType),
        th.Property("SelectionCodeDescription", th.StringType),
        th.Property("SendingMethod", th.StringType),
        th.Property("ShippingMethod", th.StringType),
        th.Property("ShippingMethodCode", th.StringType),
        th.Property("ShippingMethodDescription", th.StringType),
        th.Property("Source", th.StringType),
        th.Property("Supplier", th.StringType),
        th.Property("SupplierCode", th.StringType),
        th.Property("SupplierContact", th.StringType),
        th.Property("SupplierContactPersonFullName", th.StringType),
        th.Property("SupplierName", th.StringType),
        th.Property("Unit", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("UnitPrice", th.StringType),
        th.Property("VATAmount", th.StringType),
        th.Property("VATCode", th.StringType),
        th.Property("VATDescription", th.StringType),
        th.Property("VATPercentage", th.StringType),
        th.Property("Warehouse", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseDescription", th.StringType),
        th.Property("YourRef", th.StringType),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/PurchaseOrder/PurchaseOrders"
        else:
            return f"/purchaseorder/PurchaseOrders"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"Timestamp,AmountDC,AmountFC,ApprovalStatus,ApprovalStatusDescription,Approved,Approver,ApproverFullName,CostCenter,CostCenterCode,CostCenterDescription,CostUnit,CostUnitCode,CostUnitDescription,Created,Creator,CreatorFullName,Currency,CustomField,DeliveryAccount,DeliveryAccountCode,DeliveryAccountName,DeliveryAddress,DeliveryContact,DeliveryContactPersonFullName,Description,Discount,Division,Document,DocumentNumber,DocumentSubject,DropShipment,ExchangeRate,Expense,ExpenseDescription,ID,IncotermAddress,IncotermCode,IncotermVersion,InvoicedQuantity,InvoiceStatus,IsBatchNumberItem,IsSerialNumberItem,Item,ItemBarcode,ItemCode,ItemDescription,ItemDivisable,LineNumber,Modified,Modifier,ModifierFullName,NetPrice,Notes,OrderDate,OrderNumber,OrderStatus,PaymentCondition,PaymentConditionDescription,Project,ProjectCode,ProjectDescription,PurchaseAgent,PurchaseAgentFullName,PurchaseOrderID,Quantity,QuantityInPurchaseUnits,Rebill,ReceiptDate,ReceiptStatus,ReceivedQuantity,Remarks,SalesOrder,SalesOrderLine,SalesOrderLineNumber,SalesOrderNumber,SelectionCode,SelectionCodeCode,SelectionCodeDescription,SendingMethod,ShippingMethod,ShippingMethodCode,ShippingMethodDescription,Source,Supplier,SupplierCode,SupplierContact,SupplierContactPersonFullName,SupplierItemCode,SupplierItemCopyRemarks,SupplierName,Unit,UnitDescription,UnitPrice,VATAmount,VATCode,VATDescription,VATPercentage,Warehouse,WarehouseCode,WarehouseDescription,YourRef"
        else:
            return f"PurchaseOrderID,AmountDC,AmountFC,Created,Creator,CreatorFullName,Currency,DeliveryAccount,DeliveryAccountCode,DeliveryAccountName,DeliveryAddress,DeliveryContact,DeliveryContactPersonFullName,Description,Division,Document,DocumentSubject,DropShipment,ExchangeRate,IncotermAddress,IncotermCode,IncotermVersion,InvoiceStatus,Modified,Modifier,ModifierFullName,OrderDate,OrderNumber,OrderStatus,PaymentCondition,PaymentConditionDescription,PurchaseAgent,PurchaseAgentFullName,PurchaseOrderLineCount,PurchaseOrderLines,ReceiptDate,ReceiptStatus,Remarks,SalesOrder,SalesOrderNumber,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShippingMethod,ShippingMethodCode,ShippingMethodDescription,Source,Supplier,SupplierCode,SupplierContact,SupplierContactPersonFullName,SupplierName,VATAmount,Warehouse,WarehouseCode,WarehouseDescription,YourRef"

    @property
    def filter(self):
        use_multiple_warehouses = self.config.get("use_buy_orders_multiple_warehouses")
        if self.default_warehouse_uuid and not use_multiple_warehouses:
            return f"(OrderStatus eq 20 or OrderStatus eq 10) and ( ReceiptStatus eq 10 or ReceiptStatus eq 20) and (Warehouse eq guid'{self.default_warehouse_uuid}')"
        else:
            return f"(OrderStatus eq 20 or OrderStatus eq 10) and ( ReceiptStatus eq 10 or ReceiptStatus eq 20)"

    @property
    def expand(self):
        if self.sync_endpoint:
            return None
        else:
            return "PurchaseOrderLines"


class WarehouseStream(ExactStream):
    name = "warehouses"
    primary_keys = ["ID"]
    path = "/inventory/ItemWarehouses"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property(
            "Created",
            th.StringType,
        ),
        th.Property(
            "CreatorFullName",
            th.CustomType({"type": ["array", "object", "string"]}),
        ),
        th.Property(
            "CurrentStock",
            th.StringType,
        ),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property(
            "ItemStartDate",
            th.StringType,
        ),
        th.Property("ItemUnit", th.StringType),
        th.Property("ItemUnitDescription", th.StringType),
        th.Property(
            "MaximumStock",
            th.StringType,
        ),
        th.Property(
            "Modified",
            th.DateTimeType,
        ),
        th.Property(
            "Modifier",
            th.StringType,
        ),
        th.Property(
            "ModifierFullName",
            th.CustomType({"type": ["array", "object", "string"]}),
        ),
        th.Property(
            "ProjectedStock",
            th.StringType,
        ),
        th.Property(
            "ReorderPoint",
            th.StringType,
        ),
        th.Property(
            "SafetyStock",
            th.StringType,
        ),
        th.Property("StorageLocationUrl", th.StringType),
        th.Property(
            "Warehouse",
            th.StringType,
        ),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseDescription", th.StringType),
        th.Property("ItemIsSotckItem", th.StringType),
        th.Property("ItemEndDate", th.DateTimeType),
        th.Property("PlanningDetailsUrl", th.StringType),
        th.Property("PlannedStockIn", th.StringType),
        th.Property("PlannedStockOut", th.StringType),
        th.Property("Creator", th.StringType),
        th.Property("DefaultStorageLocation", th.StringType),
        th.Property("DefaultStorageLocationCode", th.StringType),
        th.Property("DefaultStorageLocationDescription", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("OrderPolicy", th.StringType),
        th.Property("Period", th.StringType),
        th.Property("ReorderQuantity", th.StringType),
        th.Property("ReplenishmentType", th.StringType),
        th.Property("ReservedStock", th.StringType),
        th.Property("ItemIsFractionAllowedItem", th.BooleanType),
        th.Property("ItemIsStockItem", th.BooleanType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,Created,Creator,CreatorFullName,CurrentStock,DefaultStorageLocation,DefaultStorageLocationCode,DefaultStorageLocationDescription,Division,Item,ItemCode,ItemDescription,ItemEndDate,ItemIsFractionAllowedItem,ItemIsStockItem,ItemStartDate,ItemUnit,ItemUnitDescription,MaximumStock,Modified,Modifier,ModifierFullName,OrderPolicy,Period,PlannedStockIn,PlannedStockOut,PlanningDetailsUrl,ProjectedStock,ReorderPoint,ReorderQuantity,ReplenishmentType,ReservedStock,SafetyStock,StorageLocationUrl,Warehouse,WarehouseCode,WarehouseDescription"

    @property
    def filter(self):
        use_multiple_warehouses = self.config.get("use_stock_multiple_warehouses")
        if self.default_warehouse_uuid and not use_multiple_warehouses:
            warehouse_uuid = self.default_warehouse_uuid
            return f"Warehouse eq guid'{warehouse_uuid}'"


class StockPositionsStream(ExactSyncStream):
    name = "stock_positions"
    primary_keys = ["ID"]
    path = "/sync/Inventory/StockPositions"
    replication_key = "Timestamp"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Timestamp", th.DateTimeType),
        th.Property("ItemId", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("UnitCode", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("CurrentStock", th.StringType),
        th.Property("PlanningIn", th.StringType),
        th.Property("PlanningOut", th.StringType),
        th.Property("ProjectedStock", th.StringType),
        th.Property("ReservedStock", th.StringType),
        th.Property(
            "FreeStock",
            th.StringType,
        ),
        th.Property("ReorderPoint", th.StringType),
        th.Property("Warehouse", th.StringType),
        th.Property(
            "WarehouseDescription",
            th.StringType,
        ),
        th.Property("Division", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"Timestamp,CurrentStock,Division,FreeStock,ID,ItemCode,ItemDescription,ItemId,PlanningIn,PlanningOut,ProjectedStock,ReorderPoint,ReservedStock,UnitCode,UnitDescription,Warehouse,WarehouseDescription"


class LogisticsStockPositionsStream(ExactStream):
    name = "logistics_stock_positions"
    primary_keys = ["ID"]
    parent_stream_type = ItemsStream
    records_jsonpath = "$.StockPosition.element"
    path = "/read/logistics/StockPosition?itemId=guid'{item_id}'"
    select = None

    schema = th.PropertiesList(
        th.Property(
            "ItemId",
            th.StringType,
        ),
        th.Property(
            "InStock",
            th.StringType,
        ),
        th.Property("PlanningIn", th.StringType),
        th.Property("PlanningOut", th.StringType),
    ).to_dict()

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        return None

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        content = row
        new_content = {}
        for key in content:
            if type(content[key]) == type(""):
                new_content[key] = content[key]
            elif "Edm.Boolean" == content[key].get("@p2:type"):
                if content[key].get("#text") == "true":
                    new_content[key] = True
                elif content[key].get("#text") == "false":
                    new_content[key] = False
                else:
                    new_content[key] = None
            else:
                new_content[key] = content[key].get("#text", None)
        row = new_content
        return row


class SupplierProductsStream(DynamicStream):
    name = "supplierProducts"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property(
            "Creator",
            th.StringType,
        ),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("CurrencyDescription", th.StringType),
        th.Property(
            "Division",
            th.StringType,
        ),
        th.Property(
            "DropShipment",
            th.StringType,
        ),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property(
            "MinimumQuantity",
            th.StringType,
        ),
        th.Property(
            "Modified",
            th.DateTimeType,
        ),
        th.Property(
            "Modifier",
            th.StringType,
        ),
        th.Property("ModifierFullName", th.StringType),
        th.Property(
            "PurchaseLeadTime",
            th.StringType,
        ),
        th.Property(
            "PurchasePrice",
            th.StringType,
        ),
        th.Property("PurchaseUnit", th.StringType),
        th.Property("PurchaseUnitDescription", th.StringType),
        th.Property(
            "StartDate",
            th.StringType,
        ),
        th.Property(
            "Supplier",
            th.StringType,
        ),
        th.Property(
            "SupplierCode",
            th.StringType,
        ),
        th.Property("SupplierDescription", th.StringType),
        th.Property("SupplierItemCode", th.StringType),
        th.Property("CopyRemarks", th.StringType),
        th.Property("CountryOfOrigin", th.StringType),
        th.Property("CountryOfOriginDescription", th.StringType),
        th.Property("Created", th.StringType),
        th.Property("EndDate", th.BooleanType),
        th.Property("MainSupplier", th.BooleanType),
        th.Property("Notes", th.StringType),
        th.Property("PurchaseUnitFactor", th.StringType),
        th.Property("PurchaseLotSize", th.StringType),
        th.Property("PurchaseVATCode", th.StringType),
        th.Property("PurchaseVATCodeDescription", th.StringType),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return "/sync/Logistics/SupplierItem"
        return "/logistics/SupplierItem"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,CopyRemarks,CountryOfOrigin,CountryOfOriginDescription,Created,Creator,CreatorFullName,Currency,CurrencyDescription,Division,DropShipment,EndDate,Item,ItemCode,ItemDescription,MainSupplier,MinimumQuantity,Modified,Modifier,ModifierFullName,Notes,PurchaseLeadTime,PurchasePrice,PurchaseUnit,PurchaseUnitDescription,PurchaseUnitFactor,PurchaseLotSize,PurchaseVATCode,PurchaseVATCodeDescription,StartDate,Supplier,SupplierCode,SupplierDescription,SupplierItemCode,Timestamp"
        return f"ID,CopyRemarks,CountryOfOrigin,CountryOfOriginDescription,Created,Creator,CreatorFullName,Currency,CurrencyDescription,Division,DropShipment,EndDate,Item,ItemCode,ItemDescription,MainSupplier,MinimumQuantity,Modified,Modifier,ModifierFullName,Notes,PurchaseLeadTime,PurchasePrice,PurchaseUnit,PurchaseUnitDescription,PurchaseUnitFactor,PurchaseLotSize,PurchaseVATCode,PurchaseVATCodeDescription,StartDate,Supplier,SupplierCode,SupplierDescription,SupplierItemCode"


class SalesOrderLinesStream(DynamicStream):
    name = "sales_orderlines"
    primary_keys = ["ID"]
    parent_stream_type = SalesOrderStream
    replication_key = "Timestamp"

    @property
    def ignore_parent_stream(self):
        if self.sync_endpoint:
            return True
        else:
            False

    schema = th.PropertiesList(
        th.Property(
            "AmountDC",
            th.StringType,
        ),
        th.Property(
            "AmountFC",
            th.StringType,
        ),
        th.Property(
            "DeliveryDate",
            th.StringType,
        ),
        th.Property("Description", th.StringType),
        th.Property("DeliveryDate", th.DateTimeType),
        th.Property("Discount", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property(
            "LineNumber",
            th.StringType,
        ),
        th.Property(
            "NetPrice",
            th.StringType,
        ),
        th.Property(
            "OrderID",
            th.StringType,
        ),
        th.Property(
            "OrderNumber",
            th.StringType,
        ),
        th.Property(
            "Quantity",
            th.StringType,
        ),
        th.Property("UnitCode", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property(
            "UnitPrice",
            th.StringType,
        ),
        th.Property(
            "UseDropShipment",
            th.StringType,
        ),
        th.Property(
            "VATAmount",
            th.StringType,
        ),
        th.Property(
            "CostCenter",
            th.StringType,
        ),
        th.Property(
            "CostCenterDescription",
            th.StringType,
        ),
        th.Property(
            "CostPriceFC",
            th.StringType,
        ),
        th.Property(
            "CostUnit",
            th.StringType,
        ),
        th.Property(
            "CostUnitDescription",
            th.StringType,
        ),
        th.Property(
            "CustomerItemCode",
            th.StringType,
        ),
        th.Property(
            "Division",
            th.StringType,
        ),
        th.Property(
            "ItemVersion",
            th.StringType,
        ),
        th.Property(
            "ItemVersionDescription",
            th.StringType,
        ),
        th.Property(
            "Notes",
            th.StringType,
        ),
        th.Property(
            "Pricelist",
            th.StringType,
        ),
        th.Property(
            "PricelistDescription",
            th.StringType,
        ),
        th.Property(
            "ProjectDescription",
            th.StringType,
        ),
        th.Property(
            "Project",
            th.StringType,
        ),
        th.Property(
            "PurchaseOrder",
            th.StringType,
        ),
        th.Property(
            "PurchaseOrderLine",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "PurchaseOrderLineNumber",
            th.StringType,
        ),
        th.Property(
            "PurchaseOrderNumber",
            th.StringType,
        ),
        th.Property(
            "ShopOrder",
            th.StringType,
        ),
        th.Property(
            "VATCode",
            th.StringType,
        ),
        th.Property(
            "VATCodeDescription",
            th.StringType,
        ),
        th.Property(
            "VATPercentage",
            th.StringType,
        ),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/SalesOrder/SalesOrderLines"
        return f"/bulk/SalesOrder/SalesOrderLines"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,AmountDC,AmountFC,CostCenter,CostCenterDescription,CostPriceFC,CostUnit,CostUnitDescription,CustomerItemCode,DeliveryDate,Description,Discount,Division,Item,ItemCode,ItemDescription,ItemVersion,ItemVersionDescription,LineNumber,NetPrice,Notes,OrderID,OrderNumber,Pricelist,PricelistDescription,Project,ProjectDescription,PurchaseOrder,PurchaseOrderLine,PurchaseOrderLineNumber,PurchaseOrderNumber,Quantity,ShopOrder,UnitCode,UnitDescription,UnitPrice,UseDropShipment,VATAmount,VATCode,VATCodeDescription,VATPercentage,Timestamp"
        return f"ID,AmountDC,AmountFC,CostCenter,CostCenterDescription,CostPriceFC,CostUnit,CostUnitDescription,CustomerItemCode,DeliveryDate,Description,Discount,Division,Item,ItemCode,ItemDescription,ItemVersion,ItemVersionDescription,LineNumber,NetPrice,Notes,OrderID,OrderNumber,Pricelist,PricelistDescription,Project,ProjectDescription,PurchaseOrder,PurchaseOrderLine,PurchaseOrderLineNumber,PurchaseOrderNumber,Quantity,ShopOrder,UnitCode,UnitDescription,UnitPrice,UseDropShipment,VATAmount,VATCode,VATCodeDescription,VATPercentage"

    @property
    def filter(self):
        if not self.sync_endpoint:
            order_id = self.tap_state["bookmarks"]["sales_orderlines"]["partitions"][
                -1
            ]["context"]["order_id"]
            return f"OrderID eq guid'{order_id}'"


class PurchaseOrderLinesStream(ExactStream):
    name = "purchase_orderlines"
    primary_keys = ["ID"]
    path = "/purchaseorder/PurchaseOrderLines"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("AmountFC", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Discount", th.StringType),
        th.Property("Expense", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("ExpenseDescription", th.StringType),
        th.Property("InStock", th.StringType),
        th.Property("InvoiceQuantity", th.StringType),
        th.Property("IsBatchNumberItem", th.BooleanType),
        th.Property("IsSerialNumberItem", th.BooleanType),
        th.Property("Item", th.StringType),
        th.Property("ItemBarcode", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemDivisable", th.BooleanType),
        th.Property("LineNumber", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("NetPrice", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("ProjectCode", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("ProjectedStock", th.StringType),
        th.Property("PurchaseOrderID", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("QuantityInPurchaseUnits", th.StringType),
        th.Property("Rebill", th.BooleanType),
        th.Property("ReceiptDate", th.StringType),
        th.Property("ReceivedQuantity", th.StringType),
        th.Property("SalesOrder", th.StringType),
        th.Property("SalesOrderLine", th.StringType),
        th.Property("SalesOrderLineNumber", th.StringType),
        th.Property("SalesOrderNumber", th.StringType),
        th.Property("SupplierItemCode", th.StringType),
        th.Property("SupplierItemCopyRemarks", th.StringType),
        th.Property("Unit", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("UnitPrice", th.StringType),
        th.Property("VATAmount", th.StringType),
        th.Property("VATCode", th.StringType),
        th.Property("VATDescription", th.StringType),
        th.Property("VATPercentage", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,AmountDC,AmountFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Created,Creator,CreatorFullName,Description,Discount,Division,Expense,ExpenseDescription,InStock,InvoicedQuantity,IsBatchNumberItem,IsSerialNumberItem,Item,ItemBarcode,ItemCode,ItemDescription,ItemDivisable,LineNumber,Modified,Modifier,ModifierFullName,NetPrice,Notes,Project,ProjectCode,ProjectDescription,ProjectedStock,PurchaseOrderID,Quantity,QuantityInPurchaseUnits,Rebill,ReceiptDate,ReceivedQuantity,SalesOrder,SalesOrderLine,SalesOrderLineNumber,SalesOrderNumber,SupplierItemCode,SupplierItemCopyRemarks,Unit,UnitDescription,UnitPrice,VATAmount,VATCode,VATDescription,VATPercentage"



class AccountsStream(DynamicStream):
    name = "accounts"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Timestamp", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Email", th.StringType),
        th.Property("PurchaseLeadDays", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("IsSupplier", th.BooleanType),
        th.Property("Code", th.BooleanType),
        th.Property("EndDate", th.DateTimeType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/CRM/Accounts"
        return f"/crm/Accounts"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,Name,Email,PurchaseLeadDays,Timestamp,Modified,IsSupplier,Code,EndDate"
        return f"ID,Name,Email,PurchaseLeadDays,Modified,IsSupplier,Code,EndDate"


class SupplierStream(AccountsStream):
    name = "suppliers"
    primary_keys = ["ID"]
    replication_key = "Modified"

    @property
    def filter(self):
        if not self.sync_endpoint:
            return "IsSupplier eq true"

    def post_process(self, row, context):
        row = super().post_process(row, context)

        if not self.sync_endpoint:
            return row

        if row.get("IsSupplier"):
            return row


class SalesInvoicesStream(DynamicStream):
    name = "sales_invoices"
    primary_keys = ["InvoiceID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("InvoiceID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("InvoiceDate", th.DateTimeType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Timestamp", th.StringType),
        th.Property("Warehouse", th.StringType),
        th.Property("DeliverToContactPersonFullName", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("SalesInvoiceLines", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("WithholdingTaxAmountFC", th.StringType),
        th.Property("TaxSchedule", th.StringType),
        th.Property("YourRef", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("EndTime", th.StringType),
        th.Property("IncotermCode", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Pricelist", th.StringType),
        th.Property("OrderedByContactPersonFullName", th.StringType),
        th.Property("SelectionCodeCode", th.StringType),
        th.Property("DiscountType", th.StringType),
        th.Property("InvoiceToContactPersonFullName", th.StringType),
        th.Property("GLAccountDescription", th.StringType),
        th.Property("Salesperson", th.StringType),
        th.Property("IsExtraDuty", th.BooleanType),
        th.Property("VATAmountDC", th.StringType),
        th.Property("UnitPrice", th.StringType),
        th.Property("SelectionCode", th.StringType),
        th.Property("DocumentNumber", th.StringType),
        th.Property("Created", th.StringType),
        th.Property("IncotermVersion", th.StringType),
        th.Property("DocumentSubject", th.StringType),
        th.Property("AmountFCExclVat", th.StringType),
        th.Property("AmountDiscountExclVat", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("AmountFC", th.StringType),
        th.Property("DeliverTo", th.StringType),
        th.Property("SalesOrder", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("ProjectWBS", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("SelectionCodeDescription", th.StringType),
        th.Property("Discount", th.StringType),
        th.Property("DeliverToContactPerson", th.StringType),
        th.Property("VATCode", th.StringType),
        th.Property("DeliverToName", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("DeliveryDate", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("Creator", th.StringType),
        th.Property("WithholdingTaxBaseAmount", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("TaxScheduleDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("EmployeeFullName", th.StringType),
        th.Property("SalespersonFullName", th.StringType),
        th.Property("IncotermAddress", th.StringType),
        th.Property("OrderedByName", th.StringType),
        th.Property("PaymentReference", th.StringType),
        th.Property("SalesOrderNumber", th.StringType),
        th.Property("OrderedByContactPerson", th.StringType),
        th.Property("NetPrice", th.StringType),
        th.Property("OrderedBy", th.StringType),
        th.Property("TaxScheduleCode", th.StringType),
        th.Property("UnitCode", th.StringType),
        th.Property("PaymentCondition", th.StringType),
        th.Property("ExtraDutyAmountFC", th.StringType),
        th.Property("StartTime", th.StringType),
        th.Property("InvoiceNumber", th.StringType),
        th.Property("Modifier", th.StringType),
        th.Property("Document", th.StringType),
        th.Property("Subscription", th.StringType),
        th.Property("InvoiceToContactPerson", th.StringType),
        th.Property("VATAmountFC", th.StringType),
        th.Property("StarterSalesInvoiceStatus", th.StringType),
        th.Property("VATPercentage", th.StringType),
        th.Property("TypeDescription", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("OrderDate", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("DeliverToAddress", th.StringType),
        th.Property("ShippingMethodDescription", th.StringType),
        th.Property("Employee", th.StringType),
        th.Property("SalesInvoiceOrderChargeLines", th.StringType),
        th.Property("ExtraDutyPercentage", th.StringType),
        th.Property("SalesChannel", th.StringType),
        th.Property("GAccountAmountFC", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("GLAccount", th.StringType),
        th.Property("ShippingMethodCode", th.StringType),
        th.Property("DueDate", th.StringType),
        th.Property("SalesOrderLine", th.StringType),
        th.Property("PaymentConditionDescription", th.StringType),
        th.Property("PricelistDescription", th.StringType),
        th.Property("WithholdingTaxPercentage", th.StringType),
        th.Property("InvoiceToName", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("Remarks", th.StringType),
        th.Property("AmountDiscount", th.StringType),
        th.Property("StarterSalesInvoiceStatusDescription", th.StringType),
        th.Property("CustomerItemCode", th.StringType),
        th.Property("JournalDescription", th.StringType),
        th.Property("SalesOrderLineNumber", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("SalesChannelCode", th.StringType),
        th.Property("SubscriptionDescription", th.StringType),
        th.Property("Journal", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("VATCodeDescription", th.StringType),
        th.Property("StatusDescription", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("SalesChannelDescription", th.StringType),
        th.Property("ProjectWBSDescription", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("InvoiceTo", th.StringType),
        th.Property("ShippingMethod", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/SalesInvoice/SalesInvoices"
        else:
            return f"/salesinvoice/SalesInvoices"

    @property
    def select(self):
        if self.sync_endpoint:
            return (
                f"InvoiceID,AmountDC,InvoiceDate,Warehouse,Modified,Timestamp,Warehouse,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Created,Creator,CreatorFullName,Currency,CustomerItemCode,CustomField,DeliverTo,DeliverToAddress,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,DeliveryDate,Description,Discount,DiscountType,Division,Document,DocumentNumber,DocumentSubject,DueDate,Employee,EmployeeFullName,EndTime,ExtraDutyAmountFC,ExtraDutyPercentage,GAccountAmountFC,GLAccount,GLAccountDescription,ID,IncotermAddress,IncotermCode,IncotermVersion,InvoiceNumber,InvoiceTo,InvoiceToContactPerson,InvoiceToContactPersonFullName,InvoiceToName,IsExtraDuty,Item,ItemCode,ItemDescription,Journal,JournalDescription,LineNumber,Modifier,ModifierFullName,NetPrice,Notes,OrderDate,OrderedBy,OrderedByContactPerson,OrderedByContactPersonFullName,OrderedByName,OrderNumber,PaymentCondition,PaymentConditionDescription,PaymentReference,Pricelist,PricelistDescription,Project,ProjectDescription,ProjectWBS,ProjectWBSDescription,Quantity,Remarks,SalesChannel,SalesChannelCode,SalesChannelDescription,SalesOrder,SalesOrderLine,SalesOrderLineNumber,SalesOrderNumber,Salesperson,SalespersonFullName,StarterSalesInvoiceStatus,StarterSalesInvoiceStatusDescription,StartTime,Status,StatusDescription,Subscription,SubscriptionDescription,TaxSchedule,TaxScheduleCode,TaxScheduleDescription,Type,TypeDescription,UnitCode,UnitDescription,UnitPrice,VATAmountDC,VATAmountFC,VATCode,VATCodeDescription,VATPercentage,WithholdingTaxAmountFC,WithholdingTaxBaseAmount,WithholdingTaxPercentage,YourRef"
            )
        else:
            return f"InvoiceID,AmountDC,InvoiceDate,Warehouse,Modified,AmountDiscount,AmountDiscountExclVat,AmountFC,AmountFCExclVat,Created,Creator,CreatorFullName,Currency,DeliverTo,DeliverToAddress,DeliverToContactPerson,DeliverToContactPersonFullName,DeliverToName,Description,Discount,DiscountType,Division,Document,DocumentNumber,DocumentSubject,DueDate,ExtraDutyAmountFC,GAccountAmountFC,IncotermAddress,IncotermCode,IncotermVersion,InvoiceNumber,InvoiceTo,InvoiceToContactPerson,InvoiceToContactPersonFullName,InvoiceToName,IsExtraDuty,Journal,JournalDescription,Modifier,ModifierFullName,OrderDate,OrderedBy,OrderedByContactPerson,OrderedByContactPersonFullName,OrderedByName,OrderNumber,PaymentCondition,PaymentConditionDescription,PaymentReference,Remarks,SalesChannel,SalesChannelCode,SalesChannelDescription,SalesInvoiceLines,SalesInvoiceOrderChargeLines,Salesperson,SalespersonFullName,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShippingMethod,ShippingMethodCode,ShippingMethodDescription,StarterSalesInvoiceStatus,StarterSalesInvoiceStatusDescription,Status,StatusDescription,TaxSchedule,TaxScheduleCode,TaxScheduleDescription,Type,TypeDescription,VATAmountDC,VATAmountFC,WithholdingTaxAmountFC,WithholdingTaxBaseAmount,WithholdingTaxPercentage,YourRef"

    @property
    def filter(self):
        use_multiple_warehouses = self.config.get(
            "use_sales_invoices_multiple_warehouses"
        )
        if self.default_warehouse_uuid and not use_multiple_warehouses:
            warehouse_uuid = self.default_warehouse_uuid
            return f"Warehouse eq guid'{warehouse_uuid}'"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "invoice_id": record["InvoiceID"],
        }


class SalesInvoiceLinesStream(ExactStream):
    name = "sales_invoice_lines"
    primary_keys = ["ID"]
    path = "/salesinvoice/SalesInvoiceLines?$select=ID,Item,AmountDC,ItemCode,InvoiceID,Quantity,SalesOrderNumber&$filter=InvoiceID eq guid'{invoice_id}'"
    select = None
    parent_stream_type = SalesInvoicesStream

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("InvoiceID", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("SalesOrderNumber", th.StringType),
    ).to_dict()


class SalesItemsPrices(DynamicStream):
    name = "sales_items_prices"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Account", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("DefaultItemUnit", th.StringType),
        th.Property("DefaultItemUnitDescription", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Employee", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("NumberOfItemsPerUnit", th.StringType),
        th.Property("Unit", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("Price", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("StartDate", th.DateTimeType),
        th.Property("EndDate", th.DateTimeType),
        th.Property("Timestamp", th.StringType),
        th.Property("Modified", th.DateTimeType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/Logistics/SalesItemPrices"
        return f"/logistics/SalesItemPrices"

    @property
    def select(self):
        if self.sync_endpoint:
            return (
                f"ID,Account,AccountName,Created,Creator,CreatorFullName,Currency,DefaultItemUnit,DefaultItemUnitDescription,Division,Employee,Item,ItemCode,NumberOfItemsPerUnit,Unit,UnitDescription,Price,Quantity,StartDate,EndDate,Modified,Timestamp"
            )
        return f"ID,Account,AccountName,Created,Creator,CreatorFullName,Currency,DefaultItemUnit,DefaultItemUnitDescription,Division,Employee,Item,ItemCode,NumberOfItemsPerUnit,Unit,UnitDescription,Price,Quantity,StartDate,EndDate,Modified"


class Deleted(ExactSyncStream):
    name = "deleted"
    primary_keys = ["ID"]
    path = "/sync/Deleted"
    replication_key = "Timestamp"

    schema = th.PropertiesList(
        th.Property("Timestamp", th.StringType),
        th.Property("DeletedBy", th.StringType),
        th.Property("DeletedDate", th.DateTimeType),
        th.Property("Division", th.StringType),
        th.Property("EntityKey", th.StringType),
        th.Property("EntityType", th.StringType),
        th.Property("ID", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"DeletedBy,ID,EntityType,EntityKey,Timestamp"


class ReportingBalanceStream(ExactStream):
    name = "reporting_balance"
    primary_keys = ["ID"]

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Amount", th.CustomType({"type": ["number", "string"]})),
        th.Property(
            "AmountCredit", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property(
            "AmountDebit", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property("BalanceType", th.StringType),
        th.Property("CostCenterCode", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnitCode", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("Count", th.CustomType({"type": ["number", "string"]})),
        th.Property("Division", th.CustomType({"type": ["number", "string"]})),
        th.Property("GLAccount", th.StringType),
        th.Property("GLAccountCode", th.StringType),
        th.Property("GLAccountDescription", th.StringType),
        th.Property(
            "ReportingPeriod", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property(
            "ReportingYear", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property("Status", th.CustomType({"type": ["number", "string"]})),
        th.Property("Type", th.CustomType({"type": ["number", "string"]})),
    ).to_dict()

    @property
    def path(self):
        return "/financial/ReportingBalance"

    @property
    def select(self):
        return (
            "ID,Amount,AmountCredit,AmountDebit,BalanceType,CostCenterCode,"
            "CostCenterDescription,CostUnitCode,CostUnitDescription,Count,Division,"
            "GLAccount,GLAccountCode,GLAccountDescription,ReportingPeriod,"
            "ReportingYear,Status,Type"
        )

    def get_url_params(self, context, next_page_token):
        params = {"$select": self.select}
        reporting_year = self.config.get("reporting_balance_reporting_year")
        if reporting_year:
            params["$filter"] = f"ReportingYear eq {reporting_year}"
        if next_page_token:
            params["$skiptoken"] = next_page_token
        return params


class GLAccountsStream(DynamicStream):
    name = "gl_accounts"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AllowCostsInSales", th.StringType),
        th.Property("AssimilatedVATBox", th.NumberType),
        th.Property("BalanceSide", th.StringType),
        th.Property("BalanceType", th.StringType),
        th.Property("BelcotaxType", th.NumberType),
        th.Property("Code", th.StringType),
        th.Property("Compress", th.BooleanType),
        th.Property("Costcenter", th.StringType),
        th.Property("CostcenterDescription", th.StringType),
        th.Property("Costunit", th.StringType),
        th.Property("CostunitDescription", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("DeductibilityPercentages", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("ExcludeVATListing", th.StringType),
        th.Property("ExpenseNonDeductiblePercentage", th.StringType),
        th.Property("IsBlocked", th.BooleanType),
        th.Property("Matching", th.BooleanType),
        th.Property("ExcludeVATListing", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("PrivateGLAccount", th.StringType),
        th.Property("PrivatePercentage", th.StringType),
        th.Property("ReportingCode", th.StringType),
        th.Property("RevalueCurrency", th.BooleanType),
        th.Property("SearchCode", th.StringType),
        th.Property("Type", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/financial/GLAccounts"
        return f"/financial/GLAccounts"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,AllowCostsInSales,AssimilatedVATBox,BalanceSide,BalanceType,BelcotaxType,Code,Compress,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Created,Creator,CreatorFullName,CustomField,DeductibilityPercentages,Description,Division,ExcludeVATListing,ExpenseNonDeductiblePercentage,IsBlocked,Matching,Modified,Modifier,ModifierFullName,PrivateGLAccount,PrivatePercentage,ReportingCode,RevalueCurrency,SearchCode,Type,Timestamp"
        return f"ID,AllowCostsInSales,AssimilatedVATBox,BalanceSide,BalanceType,BelcotaxType,Code,Compress,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Created,Creator,CreatorFullName,CustomField,DeductibilityPercentages,Description,Division,ExcludeVATListing,ExpenseNonDeductiblePercentage,IsBlocked,Matching,Modified,Modifier,ModifierFullName,PrivateGLAccount,PrivatePercentage,ReportingCode,RevalueCurrency,SearchCode,Type"


class PurchaseInvoicesStream(DynamicStream):
    name = "purchase_invoices"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Amount", th.StringType),
        th.Property("ContactPerson", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Document", th.StringType),
        th.Property("DueDate", th.DateTimeType),
        th.Property("EntryNumber", th.CustomType({"type": ["number", "string"]})),
        th.Property("ExchangeRate", th.StringType),
        th.Property("FinancialPeriod", th.StringType),
        th.Property("FinancialYear", th.StringType),
        th.Property("InvoiceDate", th.DateTimeType),
        th.Property("Journal", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("PaymentCondition", th.StringType),
        th.Property("PaymentReference", th.StringType),
        th.Property(
            "PurchaseInvoiceLines", th.CustomType({"type": ["object", "array"]})
        ),
        th.Property("Remarks", th.StringType),
        th.Property("Source", th.CustomType({"type": ["number", "string"]})),
        th.Property(
            "Status", th.CustomType({"type": ["number", "string"]})
        ),  # The status of the invoice. 10 Draft, 20 Open, 50 Processed.
        th.Property("Supplier", th.StringType),
        th.Property("Type", th.CustomType({"type": ["number", "string"]})),
        th.Property("VATAmount", th.CustomType({"type": ["number", "string"]})),
        th.Property("Warehouse", th.StringType),
        th.Property("YourRef", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/purchase/PurchaseInvoices"
        return f"/purchase/PurchaseInvoices"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,Amount,ContactPerson,Currency,Description,Document,DueDate,EntryNumber,ExchangeRate,FinancialPeriod,FinancialYear,InvoiceDate,Journal,Modified,PaymentCondition,PaymentReference,PurchaseInvoiceLines,Remarks,Source,Status,Supplier,Type,VATAmount,Warehouse,YourRef,Timestamp"
        return f"ID,Amount,ContactPerson,Currency,Description,Document,DueDate,EntryNumber,ExchangeRate,FinancialPeriod,FinancialYear,InvoiceDate,Journal,Modified,PaymentCondition,PaymentReference,PurchaseInvoiceLines,Remarks,Source,Status,Supplier,Type,VATAmount,Warehouse,YourRef"


class VatCodesStream(DynamicStream):
    name = "vat_codes"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Account", th.StringType),
        th.Property("AccountCode", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("CalculationBasis", th.StringType),
        th.Property("Charged", th.BooleanType),
        th.Property("Code", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EUSalesListing", th.StringType),
        th.Property("ExcludeVATListing", th.StringType),
        th.Property("GLDiscountPurchase", th.StringType),
        th.Property("GLDiscountPurchaseCode", th.StringType),
        th.Property("GLDiscountPurchaseDescription", th.StringType),
        th.Property("GLDiscountSales", th.StringType),
        th.Property("GLDiscountSalesCode", th.StringType),
        th.Property("GLDiscountSalesDescription", th.StringType),
        th.Property("GLToClaim", th.StringType),
        th.Property("GLToClaimCode", th.StringType),
        th.Property("GLToClaimDescription", th.StringType),
        th.Property("GLToPay", th.StringType),
        th.Property("GLToPayCode", th.StringType),
        th.Property("GLToPayDescription", th.StringType),
        th.Property("IntraStat", th.BooleanType),
        th.Property("IntrastatType", th.StringType),
        th.Property("IsBlocked", th.BooleanType),
        th.Property("LegalText", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        # th.Property("OssCountry", th.StringType), # Not enabled in account by default
        th.Property("Percentage", th.StringType),
        th.Property("TaxReturnType", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("VatDocType", th.StringType),
        th.Property("VatMargin", th.StringType),
        th.Property("VATPartialRatio", th.StringType),
        th.Property("VATPercentages", th.StringType),
        th.Property("VATTransactionType", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/vat/VATCodes"
        return f"/vat/VATCodes"

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,Account,AccountCode,AccountName,CalculationBasis,Charged,Code,Country,Created,Creator,CreatorFullName,CustomField,Description,Division,EUSalesListing,ExcludeVATListing,GLDiscountPurchase,GLDiscountPurchaseCode,GLDiscountPurchaseDescription,GLDiscountSales,GLDiscountSalesCode,GLDiscountSalesDescription,GLToClaim,GLToClaimCode,GLToClaimDescription,GLToPay,GLToPayCode,GLToPayDescription,IntraStat,IntrastatType,IsBlocked,LegalText,Modified,Modifier,ModifierFullName,Percentage,TaxReturnType,Type,VatDocType,VatMargin,VATPartialRatio,VATPercentages,VATTransactionType,Timestamp"
        return f"ID,Account,AccountCode,AccountName,CalculationBasis,Charged,Code,Country,Created,Creator,CreatorFullName,CustomField,Description,Division,EUSalesListing,ExcludeVATListing,GLDiscountPurchase,GLDiscountPurchaseCode,GLDiscountPurchaseDescription,GLDiscountSales,GLDiscountSalesCode,GLDiscountSalesDescription,GLToClaim,GLToClaimCode,GLToClaimDescription,GLToPay,GLToPayCode,GLToPayDescription,IntraStat,IntrastatType,IsBlocked,LegalText,Modified,Modifier,ModifierFullName,Percentage,TaxReturnType,Type,VatDocType,VatMargin,VATPartialRatio,VATPercentages,VATTransactionType"


class BillOfMaterialsVersionsStream(ExactStream):
    name = "bill_of_materials_versions"
    primary_keys = ["ID"]
    replication_key = "Modified"

    @property
    def path(self):
        # Bill of materials versions doesnt have a sync endpoint
        return f"/manufacturing/BillOfMaterialVersions"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("BatchQuantity", th.StringType),
        th.Property("CadDrawingUrl", th.StringType),
        th.Property("CalculatedCostPrice", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("IsDefault", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("OrderLeadDays", th.StringType),
        th.Property("ProductionLeadDays", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("StatusDescription", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("TypeDescription", th.StringType),
        th.Property("VersionDate", th.StringType),
        th.Property("VersionNumber", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return "ID,BatchQuantity,CadDrawingUrl,CalculatedCostPrice,Created,Creator,CreatorFullName,Description,Division,IsDefault,Item,ItemDescription,Modified,Modifier,ModifierFullName,Notes,OrderLeadDays,ProductionLeadDays,Status,StatusDescription,Type,TypeDescription,VersionDate,VersionNumber"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "billofmaterials_id": record["ID"],
        }

    def _sync_records(self, context = None) -> None:
        # always fetch children stream using its own rep key, even if parent stream has no data
        super()._sync_children({"billofmaterials_id": None})
        # normal behaviour syncing child streams using context
        return super()._sync_records(context)


class BillOfMaterialsStream(ExactStream):
    name = "bill_of_materials"
    primary_keys = ["ID"]
    path = "/manufacturing/BillOfMaterialMaterials"
    select = None
    parent_stream_type = BillOfMaterialsVersionsStream
    fetch_from_parent_stream = False
    replication_key = "sysmodified"
    ids = set()
    default_rep_key_field = "sysmodified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AverageCost", th.StringType),
        th.Property("Backflush", th.StringType),  # Byte type
        th.Property("CalculatorType", th.StringType),
        th.Property("CostBatch", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("DetailDrawing", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("ItemVersion", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("NetWeight", th.StringType),
        th.Property("NetWeightUnit", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("PartItem", th.StringType),
        th.Property("PartItemCode", th.StringType),
        th.Property("PartItemCostPriceStandard", th.StringType),
        th.Property("PartItemDescription", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("QuantityBatch", th.StringType),
        th.Property("RoutingStepID", th.StringType),
        th.Property("syscreated", th.StringType),
        th.Property("syscreator", th.StringType),
        th.Property("sysmodified", th.DateTimeType),
        th.Property("sysmodifier", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("WastePercentage", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,AverageCost,Backflush,CalculatorType,CostBatch,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,CreatorFullName,Description,DetailDrawing,Division,ItemVersion,LineNumber,NetWeight,NetWeightUnit,Notes,PartItem,PartItemCode,PartItemCostPriceStandard,PartItemDescription,Quantity,QuantityBatch,RoutingStepID,syscreated,syscreator,sysmodified,sysmodifier,Type,WastePercentage"

    def request_records(self, context) -> Iterable[dict]:
        # 1. Fetch all BOMMaterials using rep key
        billofmaterials_id = None
        if not self.fetch_from_parent_stream:
            # BOMMaterials is a child stream but it also fetches data using its own rep key
            # so we need to keep the rep_key_value at the header level
            if "replication_key_value" in self.stream_state:
                self.stream_state['starting_replication_value'] = self.stream_state['replication_key_value']
            #---
            billofmaterials_id = context.pop("billofmaterials_id", None)
            yield from super().request_records(context)
            self.fetch_from_parent_stream = True
        # 2. Fetch BOMMaterials from parent stream
        if self.fetch_from_parent_stream:
            if billofmaterials_id:
                context.update({"billofmaterials_id": billofmaterials_id})
            yield from super().request_records(context)

    def get_url_params(self, context, next_page_token) -> Dict[str, Any]:
        # filter by bomversion if fetch_from_parent_stream is True
        if self.fetch_from_parent_stream:
            billofmaterials_id = context.get("billofmaterials_id")
            if billofmaterials_id:
                self.filter = f"ItemVersion eq guid'{billofmaterials_id}'"

        return super().get_url_params(context, next_page_token)

    def post_process(self, row, context) -> dict:
        row = super().post_process(row, context)
        # avoid duplicates from normal rep key fetch and child stream fetch
        if row["ID"] not in self.ids:
            self.ids.add(row["ID"])
            return row


class ManufacturingShopOrdersStream(DynamicStream):
    name = "manufacturing_shop_orders"
    primary_keys = ["ID"]
    replication_key = "Modified"

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/manufacturing/ShopOrders"
        return f"/manufacturing/ShopOrders"

    schema = th.PropertiesList(
        th.Property("CADDrawingURL", th.StringType),
        th.Property("Costcenter", th.StringType),
        th.Property("CostcenterDescription", th.StringType),
        th.Property("Costunit", th.StringType),
        th.Property("CostunitDescription", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.CustomType({"type": ["number", "string"]})),
        th.Property("EntryDate", th.DateTimeType),
        th.Property("ID", th.StringType),
        th.Property("IsBatch", th.CustomType({"type": ["number", "string"]})),
        th.Property("IsFractionAllowedItem", th.CustomType({"type": ["number", "string"]})),
        th.Property("IsInPlanning", th.CustomType({"type": ["number", "string"]})),
        th.Property("IsOnHold", th.CustomType({"type": ["number", "string"]})),
        th.Property("IsReleased", th.CustomType({"type": ["number", "string"]})),
        th.Property("IsSerial", th.CustomType({"type": ["number", "string"]})),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemPictureUrl", th.StringType),
        th.Property("ItemVersion", th.StringType),
        th.Property("ItemVersionDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("PlannedDate", th.DateTimeType),
        th.Property("PlannedQuantity", th.CustomType({"type": ["number", "string"]})),
        th.Property("PlannedStartDate", th.DateTimeType),
        th.Property("ProducedQuantity", th.CustomType({"type": ["number", "string"]})),
        th.Property("ProductionLeadDays", th.CustomType({"type": ["number", "string"]})),
        th.Property("Project", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("ReadyToShipQuantity", th.CustomType({"type": ["number", "string"]})),
        th.Property("SalesOrderLineCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("SelectionCode", th.StringType),
        th.Property("SelectionCodeCode", th.StringType),
        th.Property("SelectionCodeDescription", th.StringType),
        th.Property("ShopOrderMain", th.StringType),
        th.Property("ShopOrderMainNumber", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderByProductPlanCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderByProductPlanBackflushCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderMaterialPlanCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderMaterialPlanBackflushCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderNumber", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderNumberString", th.StringType),
        th.Property("ShopOrderParent", th.StringType),
        th.Property("ShopOrderParentNumber", th.CustomType({"type": ["number", "string"]})),
        th.Property("ShopOrderRoutingStepPlanCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("Status", th.CustomType({"type": ["number", "string"]})),
        th.Property("SubShopOrderCount", th.CustomType({"type": ["number", "string"]})),
        th.Property("Type", th.CustomType({"type": ["number", "string"]})),
        th.Property("Unit", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("Warehouse", th.StringType),
        th.Property("YourRef", th.StringType),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def select(self):
        if self.sync_endpoint:
            return f"ID,CADDrawingURL,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Created,Creator,CreatorFullName,Description,Division,EntryDate,IsBatch,IsFractionAllowedItem,IsInPlanning,IsOnHold,IsReleased,IsSerial,Item,ItemCode,ItemDescription,ItemPictureUrl,ItemVersion,ItemVersionDescription,Modified,Modifier,ModifierFullName,Notes,PlannedDate,PlannedQuantity,PlannedStartDate,ProducedQuantity,ProductionLeadDays,Project,ProjectDescription,ReadyToShipQuantity,SalesOrderLineCount,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShopOrderByProductPlanBackflushCount,ShopOrderByProductPlanCount,ShopOrderMain,ShopOrderMainNumber,ShopOrderMaterialPlanBackflushCount,ShopOrderMaterialPlanCount,ShopOrderNumber,ShopOrderNumberString,ShopOrderParent,ShopOrderParentNumber,ShopOrderRoutingStepPlanCount,Status,SubShopOrderCount,Type,Unit,UnitDescription,Warehouse,YourRef,Timestamp"
        return f"ID,CADDrawingURL,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Created,Creator,CreatorFullName,Description,Division,EntryDate,IsBatch,IsFractionAllowedItem,IsInPlanning,IsOnHold,IsReleased,IsSerial,Item,ItemCode,ItemDescription,ItemPictureUrl,ItemVersion,ItemVersionDescription,Modified,Modifier,ModifierFullName,Notes,PlannedDate,PlannedQuantity,PlannedStartDate,ProducedQuantity,ProductionLeadDays,Project,ProjectDescription,ReadyToShipQuantity,SalesOrderLineCount,SalesOrderLines,SelectionCode,SelectionCodeCode,SelectionCodeDescription,ShopOrderByProductPlanBackflushCount,ShopOrderByProductPlanCount,ShopOrderMain,ShopOrderMainNumber,ShopOrderMaterialPlanBackflushCount,ShopOrderMaterialPlanCount,ShopOrderMaterialPlans,ShopOrderNumber,ShopOrderNumberString,ShopOrderParent,ShopOrderParentNumber,ShopOrderRoutingStepPlanCount,ShopOrderRoutingStepPlans,Status,SubShopOrderCount,Type,Unit,UnitDescription,Warehouse,YourRef"


class BillOfMaterialDownloadStream(ExactStream):
    # Download Streams don't have a sync endpoint
    # Obs: This endpoint is going to be very hard to replicate
    # in development environment if the data is not persistent
    # after one download.

    dont_use_current_division = True

    name = "bill_of_material_download"
    primary_keys = ["ID"]
    records_jsonpath = "$.eExact.BillOfMaterials.[*]"
    replication_key = None


    schema = th.PropertiesList(
        th.Property("Items", th.ArrayType(th.ObjectType(
                    th.Property("Id", th.StringType()),
                    th.Property("Code", th.StringType()),
                    th.Property("Description", th.StringType()),
                    th.Property("CostPrice", th.StringType()),
                    th.Property("BatchQuantity", th.StringType()),
                    th.Property("AssembledLeadDays", th.StringType()),
                    th.Property("AssembledAtDelivery", th.StringType()),
                    th.Property("BillOfMaterialItemDetails", th.ArrayType(
                        th.ObjectType(
                            th.Property("Id", th.StringType()),
                            th.Property("LineNumber", th.StringType()),
                            th.Property("Description", th.StringType()),
                            th.Property("QuantityPerBatch", th.StringType()),
                            th.Property("Notes", th.StringType()),
                        )
                ))
        )))
    ).to_dict()

    @property
    def path(self):
        return f"/docs/XMLDownload.aspx"

    def get_url_params(self, context, next_page_token):
        return {
            "Topic": "BillOfMaterials",
            "Params_DownloadID": self.config.get("download_id", "f_new_materials_DownloadID"),
            "_Division_": self.config.get("current_division"),
        }

    def parse_response(self, response):
        for row in super().parse_response(response):
            yield row["BillOfMaterial"]

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        self.logger.info(f"Processing row {row}")
        content_list = []
        rows = row
        if isinstance(rows, dict):
            rows = [row]
        for row in rows:
            content = {}
            for key, value in row.items():
                if isinstance(value, (str, int, float)):
                    content[key] = value

            for key, value in row.get("Item", {}).items():
                if "@" in key:
                    key = key[1:].capitalize()
                content[key] = value
            content["Id"] = content["Id"].replace("{", "").replace("}", "")
            content['BillOfMaterialItemDetails'] = []
            bom_item_detail = row["BillOfMaterialItemDetails"]["BillOfMaterialItemDetail"]

            if isinstance(bom_item_detail, dict):
                bom_item_detail = [bom_item_detail]

            for item_detail in bom_item_detail:
                item_detail_content = {}
                for key, value in item_detail.items():
                    if isinstance(value, (str, int, float)):
                        if "@" in key:
                            key = key[1:].capitalize()
                        item_detail_content[key] = value

                    item_detail_content["ItemId"] = item_detail["Item"]["@ID"].replace("{", "").replace("}", "")
                    item_detail_content["ItemCode"] = item_detail["Item"]["@code"]

                content['BillOfMaterialItemDetails'].append(item_detail_content)
            content_list.append(content)
        return {"Items": content_list}


class GoodsReceiptLinesStream(ExactStream):
    name = "good_receipt_lines_stream"
    primary_keys = ["ID"]
    replication_key = "Modified"

    @property
    def path(self):
        return f"/purchaseorder/GoodsReceiptLines"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property('BatchNumbers', th.StringType),
        th.Property('Created', th.StringType),
        th.Property('Creator', th.StringType),
        th.Property('CreatorFullName', th.StringType),
        th.Property('Description', th.StringType),
        th.Property('Division', th.StringType),
        th.Property('Expense', th.StringType),
        th.Property('ExpenseDescription', th.StringType),
        th.Property('GoodsReceiptID', th.StringType),
        th.Property('Item', th.StringType),
        th.Property('ItemCode', th.StringType),
        th.Property('ItemDescription', th.StringType),
        th.Property('ItemUnitCode', th.StringType),
        th.Property('LineNumber', th.StringType),
        th.Property('Location', th.StringType),
        th.Property('LocationCode', th.StringType),
        th.Property('LocationDescription', th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property('Modifier', th.StringType),
        th.Property('ModifierFullName', th.StringType),
        th.Property('Notes', th.StringType),
        th.Property('Project', th.StringType),
        th.Property('ProjectCode', th.StringType),
        th.Property('ProjectDescription', th.StringType),
        th.Property('PurchaseOrderLineID', th.StringType),
        th.Property('PurchaseOrderID', th.StringType),
        th.Property('PurchaseOrderNumber', th.StringType),
        th.Property('QuantityOrdered', th.StringType),
        th.Property('QuantityReceived', th.StringType),
        th.Property('Rebill', th.BooleanType),
        th.Property('SupplierItemCode', th.StringType),
        th.Property('SerialNumbers', th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,BatchNumbers,Created,Creator,CreatorFullName,Description,Division,Expense,ExpenseDescription,GoodsReceiptID,Item,ItemCode,ItemDescription,ItemUnitCode,LineNumber,Location,LocationCode,LocationDescription,Modified,Modifier,ModifierFullName,Notes,Project,ProjectCode,ProjectDescription,PurchaseOrderID,PurchaseOrderLineID,PurchaseOrderNumber,QuantityOrdered,QuantityReceived,Rebill,SerialNumbers,SupplierItemCode"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        return super().post_process(row, context)

class PurchaseEntiesStream(ExactStream):
    name = "purchase_entries"
    primary_keys = ["EntryID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("EntryID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("AmountFC", th.StringType),
        th.Property("BatchNumber", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Document", th.StringType),
        th.Property("DocumentNumber", th.StringType),
        th.Property("DocumentSubject", th.StringType),
        th.Property("DueDate", th.DateTimeType),
        th.Property("EntryDate", th.DateTimeType),
        th.Property("EntryNumber", th.StringType),
        th.Property("ExternalLinkDescription", th.StringType),
        th.Property("ExternalLinkReference", th.StringType),
        th.Property("GAccountAmountFC", th.StringType),
        th.Property("InvoiceNumber", th.StringType),
        th.Property("Journal", th.StringType),
        th.Property("JournalDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("PaymentCondition", th.StringType),
        th.Property("PaymentConditionDescription", th.StringType),
        th.Property("PaymentConditionPaymentMethod", th.StringType),
        th.Property("PaymentReference", th.StringType),
        th.Property("ProcessNumber", th.StringType),
        th.Property("PurchaseEntryLines", th.ArrayType(
            th.ObjectType(
                th.Property("AmountDC", th.StringType),
                th.Property("AmountFC", th.StringType),
                th.Property("Asset", th.StringType),
                th.Property("AssetDescription", th.BooleanType),
                th.Property("CostCenter", th.StringType),
                th.Property("CostCenterDescription", th.StringType),
                th.Property("CostUnit", th.StringType),
                th.Property("CostUnitDescription", th.StringType),
                th.Property("CustomField", th.StringType),
                th.Property("Description", th.StringType),
                th.Property("Division", th.StringType),
                th.Property("EntryID", th.StringType),
                th.Property("From", th.StringType),
                th.Property("GLAccount", th.StringType),
                th.Property("GLAccountCode", th.StringType),
                th.Property("GLAccountDescription", th.StringType),
                th.Property("IntraStatArea", th.BooleanType),
                th.Property("IntraStatCountry", th.StringType),
                th.Property("IntraStatDeliveryTerm", th.StringType),
                th.Property("IntraStatTransactionA", th.StringType),
                th.Property("IntraStatTransactionB", th.StringType),
                th.Property("IntraStatTransportMethod", th.StringType),
                th.Property("LineNumber", th.StringType),
                th.Property("Notes", th.StringType),
                th.Property("PrivateUsePercentage", th.StringType),
                th.Property("Project", th.StringType),
                th.Property("ProjectDescription", th.StringType),
                th.Property("Quantity", th.StringType),
                th.Property("SerialNumber", th.StringType),
                th.Property("StatisticalNetWeight", th.StringType),
                th.Property("StatisticalNumber", th.StringType),
                th.Property("StatisticalQuantity", th.StringType),
                th.Property("StatisticalValue", th.StringType),
                th.Property("Subscription", th.StringType),
                th.Property("SubscriptionDescription", th.StringType),
                th.Property("To", th.DateTimeType),
                th.Property("TrackingNumber", th.StringType),
                th.Property("TrackingNumberDescription", th.StringType),
                th.Property("Type", th.StringType),
                th.Property("VATAmountDC", th.StringType),
                th.Property("VATAmountFC", th.StringType),
                th.Property("VATBaseAmountDC", th.StringType),
                th.Property("VATBaseAmountFC", th.StringType),
                th.Property("VATCode", th.StringType),
                th.Property("VATCodeDescription", th.StringType),
                th.Property("VATNonDeductiblePercentage", th.StringType),
                th.Property("VATPercentage", th.StringType),
                th.Property("WithholdingAmountDC", th.StringType),
                th.Property("WithholdingTax", th.DateTimeType),
            )
        )),
        th.Property("Rate", th.StringType),
        th.Property("ReportingPeriod", th.StringType),
        th.Property("ReportingYear", th.StringType),
        th.Property("Reversal", th.BooleanType),
        th.Property("Status", th.StringType),
        th.Property("StatusDescription", th.StringType),
        th.Property("Supplier", th.StringType),
        th.Property("SupplierName", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("TypeDescription", th.StringType),
        th.Property("VATAmountDC", th.StringType),
        th.Property("VATAmountFC", th.StringType),
        th.Property("YourRef", th.StringType),
    ).to_dict()

    @property
    def path(self):
        return f"/purchaseentry/PurchaseEntries"

    @property
    def select(self):
        return f"*"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "purchaseentry_id": record["EntryID"],
        }

class PurchaseItemsPricesStream(DynamicStream):
    name = "purchase_items_prices"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Account", th.StringType),
        th.Property("AccountName", th.StringType),
        th.Property("Barcode", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("DefaultItemUnit", th.StringType),
        th.Property("DefaultItemUnitDescription", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EndDate", th.DateTimeType),
        th.Property("ID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("NumberOfItemsPerUnit", th.StringType),
        th.Property("Price", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("StartDate", th.DateTimeType),
        th.Property("Unit", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("Timestamp", th.StringType)
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/Logistics/PurchaseItemPrices"
        return f"/logistics/PurchaseItemPrices"

    @property
    def select(self):
        if self.sync_endpoint:
            return (
                f"Account,AccountName,Barcode,Created,Creator,CreatorFullName,Currency,DefaultItemUnit,DefaultItemUnitDescription,Division,EndDate,ID,Item,ItemCode,ItemDescription,Modified,Modifier,ModifierFullName,NumberOfItemsPerUnit,Price,Quantity,StartDate,Unit,UnitDescription,Timestamp"
            )
        return f"Account,AccountName,Barcode,Created,Creator,CreatorFullName,Currency,DefaultItemUnit,DefaultItemUnitDescription,Division,EndDate,ID,Item,ItemCode,ItemDescription,Modified,Modifier,ModifierFullName,NumberOfItemsPerUnit,Price,Quantity,StartDate,Unit,UnitDescription,Timestamp"


class PurchaseReturnLinesStream(ExactStream):
    name = "purchase_returnlines"
    primary_keys = ["ID"]
    path = "/purchaseorder/PurchaseReturnLines"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("BatchNumbers", th.StringType),
        th.Property("CreateCredit", th.BooleanType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EntryID", th.StringType),
        th.Property("Expense", th.StringType),
        th.Property("ExpenseDescription", th.StringType),
        th.Property("GoodsReceiptLineID", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("Expense", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("Location", th.StringType),
        th.Property("LocationCode", th.StringType),
        th.Property("LocationDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("ProjectCode", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("PurchaseOrderLineID", th.StringType),
        th.Property("PurchaseOrderNumber", th.StringType),
        th.Property("Rebill", th.BooleanType),
        th.Property("ReceiptNumber", th.StringType),
        th.Property("ReceivedQuantity", th.StringType),
        th.Property("ReturnQuantity", th.StringType),
        th.Property("ReturnReasonCodeDescription", th.StringType),
        th.Property("ReturnReasonCodeID", th.StringType),
        th.Property("SerialNumbers", th.StringType),
        th.Property("SupplierItemCode", th.BooleanType),
        th.Property("UnitCode", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,BatchNumbers,CreateCredit,Created,Creator,CreatorFullName,Division,EntryID,Expense,ExpenseDescription,GoodsReceiptLineID,Item,ItemCode,ItemDescription,LineNumber,Location,LocationCode,LocationDescription,Modified,Modifier,ModifierFullName,Notes,Project,ProjectCode,ProjectDescription,PurchaseOrderLineID,PurchaseOrderNumber,Rebill,ReceiptNumber,ReceivedQuantity,ReturnQuantity,ReturnReasonCodeDescription,ReturnReasonCodeID,SerialNumbers,SupplierItemCode,UnitCode"


class AssemblyOrdersStream(ExactStream):
    name = "assembly_orders"
    primary_keys = ["ID"]
    path = "/inventory/AssemblyOrders"
    replication_key = "OrderDate"
    expand = "PartItems"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AssemblyDate", th.DateTimeType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("FinishedAssemblyDate", th.DateTimeType),
        th.Property("FinishedQuantity", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("OrderDate", th.DateTimeType),
        th.Property("OrderNumber", th.StringType),
        th.Property("OrderStatus", th.StringType),
        th.Property("PartItems", th.StringType),
        th.Property("PlannedQuantity", th.StringType),
        th.Property("StorageLocation", th.StringType),
        th.Property("StorageLocationCode", th.StringType),
        th.Property("StorageLocationDescription", th.StringType),
        th.Property("StorageLocationSequenceNumber", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("Unit", th.StringType),
        th.Property("UnitCode", th.StringType),
        th.Property("UnitDescription", th.StringType),
        th.Property("Warehouse", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("WarehouseDescription", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,AssemblyDate,Description,Division,FinishedAssemblyDate,FinishedQuantity,Item,ItemCode,ItemDescription,Notes,OrderDate,OrderNumber,OrderStatus,PartItems,PlannedQuantity,StorageLocation,StorageLocationCode,StorageLocationDescription,StorageLocationSequenceNumber,Type,Unit,UnitCode,UnitDescription,Warehouse,WarehouseCode,WarehouseDescription"


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if self.select:
            params["$select"] = self.select
        if hasattr(self, "expand"):
            params["$expand"] = self.expand
        if next_page_token:
            params["$skiptoken"] = next_page_token

        start_date = self.get_starting_time(context)
        # if it's an incremental sync use state date - 120 days to filter
        if self.stream_state.get("replication_key_value"):
            start_date = start_date - timedelta(days=120)

        start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        params["$filter"] = f"{self.replication_key} gt datetime'{start_date}'"

        return params

class ExchangeRatesStream(ExactStream):
    name = "exchange_rates"
    primary_keys = ["ID"]
    path = "/financial/ExchangeRates"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.DateTimeType),
        th.Property("Division", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Rate", th.StringType),
        th.Property("SourceCurrency", th.StringType),
        th.Property("SourceCurrencyDescription", th.StringType),
        th.Property("StartDate", th.StringType),
        th.Property("TargetCurrency", th.StringType),
        th.Property("TargetCurrencyDescription", th.StringType),

    ).to_dict()

    @property
    def select(self):
        return f"ID,Created,Creator,CreatorFullName,Division,Modified,Modifier,ModifierFullName,Rate,SourceCurrency,SourceCurrencyDescription,StartDate,TargetCurrency,TargetCurrencyDescription"

class AssemblyBillOfMaterialHeaderStream(ExactStream):
    name = "assembly_bill_of_material_header"
    primary_keys = ["ID"]
    path = "/inventory/AssemblyBillOfMaterialHeader"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AssembledLeadDays", th.StringType),
        th.Property(
            "AssemblyBillOfMaterialMaterials",
            th.StringType  # Changed from ArrayType to StringType
        ),
        th.Property("BatchQuantity", th.StringType),
        th.Property("Code", th.StringType),
        th.Property("CostPrice", th.StringType),
        th.Property("Created", th.StringType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("UpdateCostPrice", th.StringType),
        th.Property("UseExplosion", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,AssembledLeadDays,AssemblyBillOfMaterialMaterials,BatchQuantity,Code,CostPrice,Created,Creator,CreatorFullName,Description,Division,Modified,Modifier,ModifierFullName,Notes,UpdateCostPrice,UseExplosion"

class AssemblyBillOfMaterialMaterialsStream(ExactStream):
    name = "assembly_bill_of_material_materials"
    primary_keys = ["ID"]
    path = "/inventory/AssemblyBillOfMaterialMaterials"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AssembledItem", th.StringType),
        th.Property("AssembledItemCode", th.StringType),
        th.Property("AssembledItemDescription", th.StringType),
        th.Property("AssembledLeadDays", th.StringType),
        th.Property("BatchQuantity", th.StringType),
        th.Property("Created", th.StringType),
        th.Property("Creator", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("PartItem", th.StringType),
        th.Property("PartItemCode", th.StringType),
        th.Property("PartItemDescription", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("QuantityBatch", th.StringType),
        th.Property("UpdateCostPrice", th.StringType),
        th.Property("UseExplosion", th.StringType),
    ).to_dict()

    @property
    def select(self):
        return f"ID,AssembledItem,AssembledItemCode,AssembledItemDescription,AssembledLeadDays,BatchQuantity,Created,Creator,Division,LineNumber,Modified,Modifier,PartItem,PartItemCode,PartItemDescription,Quantity,QuantityBatch,UpdateCostPrice,UseExplosion"

class TransactionLinesStream(DynamicStream):
    name = "transaction_lines"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Account", th.StringType),
        th.Property("AccountCode", th.StringType),
        th.Property("AccountName",th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("AmountVATBaseFC", th.StringType),
        th.Property("AmountVATFC", th.StringType),
        th.Property("Asset", th.StringType),
        th.Property("AssetCode", th.StringType),
        th.Property("AssetDescription", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("Date", th.DateTimeType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Document", th.StringType),
        th.Property("DocumentNumber", th.StringType),
        th.Property("DocumentSubject", th.StringType),
        th.Property("DueDate", th.DateTimeType),
        th.Property("EntryID", th.StringType),
        th.Property("EntryNumber", th.StringType),
        th.Property("ExchangeRate", th.StringType),
        th.Property("ExternalLinkDescription", th.StringType),
        th.Property("ExternalLinkReference", th.StringType),
        th.Property("ExtraDutyAmountFC", th.StringType),
        th.Property("ExtraDutyPercentage", th.StringType),
        th.Property("FinancialPeriod", th.StringType),
        th.Property("FinancialYear", th.StringType),
        th.Property("GLAccount", th.StringType),
        th.Property("GLAccountCode", th.StringType),
        th.Property("GLAccountDescription", th.StringType),
        th.Property("ID", th.StringType),
        th.Property("InvoiceNumber", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("JournalCode", th.StringType),
        th.Property("JournalDescription", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("LineType", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("OffsetID", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("PaymentDiscountAmount",th.StringType),
        th.Property("PaymentReference", th.StringType),
        th.Property("Project",th.StringType),
        th.Property("ProjectCode",th.StringType),
        th.Property("ProjectDescription",th.StringType),
        th.Property("Quantity",th.StringType),
        th.Property("SerialNumber",th.StringType),
        th.Property("Status", th.StringType),
        th.Property("Subscription", th.StringType),
        th.Property("SubscriptionDescription", th.StringType),
        th.Property("TrackingNumber", th.StringType),
        th.Property("TrackingNumberDescription", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("VATCode", th.StringType),
        th.Property("VATCodeDescription", th.StringType),
        th.Property("VATPercentage", th.StringType),
        th.Property("VATType", th.StringType),
        th.Property("YourRef", th.StringType),
        th.Property("Timestamp", th.StringType),
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/Financial/TransactionLines"
        return f"/financialtransaction/TransactionLines"

    @property
    def select(self):
        if self.sync_endpoint:
            return (
                f"Account,AccountCode,AccountName,AmountDC,AmountVATBaseFC,AmountVATFC,Asset,AssetCode,AssetDescription,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Created,Creator,CreatorFullName,Currency,CustomField,Date,Description,Division,Document,DocumentNumber,DocumentSubject,DueDate,EntryID,EntryNumber,ExchangeRate,ExternalLinkDescription,ExternalLinkReference,ExtraDutyAmountFC,ExtraDutyPercentage,FinancialPeriod,FinancialYear,GLAccount,GLAccountCode,GLAccountDescription,ID,InvoiceNumber,Item,ItemCode,ItemDescription,JournalCode,JournalDescription,LineNumber,LineType,Modified,Modifier,ModifierFullName,Notes,OffsetID,OrderNumber,PaymentDiscountAmount,PaymentReference,Project,ProjectCode,ProjectDescription,Quantity,SerialNumber,Status,Subscription,SubscriptionDescription,TrackingNumber,TrackingNumberDescription,Type,VATCode,VATCodeDescription,VATPercentage,VATType,YourRef,Timestamp"
            )
        return f"Account,AccountCode,AccountName,AmountDC,AmountVATBaseFC,AmountVATFC,Asset,AssetCode,AssetDescription,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Created,Creator,CreatorFullName,Currency,CustomField,Date,Description,Division,Document,DocumentNumber,DocumentSubject,DueDate,EntryID,EntryNumber,ExchangeRate,ExternalLinkDescription,ExternalLinkReference,ExtraDutyAmountFC,ExtraDutyPercentage,FinancialPeriod,FinancialYear,GLAccount,GLAccountCode,GLAccountDescription,ID,InvoiceNumber,Item,ItemCode,ItemDescription,JournalCode,JournalDescription,LineNumber,LineType,Modified,Modifier,ModifierFullName,Notes,OffsetID,OrderNumber,PaymentDiscountAmount,PaymentReference,Project,ProjectCode,ProjectDescription,Quantity,SerialNumber,Status,Subscription,SubscriptionDescription,TrackingNumber,TrackingNumberDescription,Type,VATCode,VATCodeDescription,VATPercentage,VATType,YourRef"

class SalesPriceListLinkedAccountsStream(ExactStream):
    name = "sales_prices_linked_accounts"
    primary_keys = ["ID"]
    path = "/sales/SalesPriceListLinkedAccounts"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Code", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("PriceList", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,Code,Created,Creator,CreatorFullName,Division,Modified,Modifier,ModifierFullName,Name,PriceList"

class SalesPriceListsStream(ExactStream):
    name = "sales_prices_lists"
    primary_keys = ["ID"]
    path = "/sales/SalesPriceLists"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Code", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("Entity", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Type", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,Code,Created,Creator,CreatorFullName,Currency,Description,Division,Entity,Modified,Modifier,ModifierFullName,Notes,Type"

class SalesPriceListPeriodsStream(ExactStream):
    name = "sales_prices_list_periods"
    primary_keys = ["ID"]
    path = "/sales/SalesPriceListPeriods"
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EndDate", th.DateTimeType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("PriceList", th.StringType),
        th.Property("StartDate", th.DateTimeType),
        th.Property("Type", th.StringType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,Created,Creator,CreatorFullName,Currency,Division,EndDate,Modified,Modifier,ModifierFullName,PriceList,StartDate,Type"

class SalesPriceListVolumeDiscountsStream(DynamicStream):
    name = "sales_price_list_volume_discounts"
    primary_keys = ["ID"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("BasePrice", th.StringType),
        th.Property("BasePriceAmount", th.StringType),
        th.Property("Created", th.DateTimeType),
        th.Property("Creator", th.StringType),
        th.Property("CreatorFullName", th.StringType),
        th.Property("Discount", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EntryMethod", th.StringType),
        th.Property("Item", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemGroup", th.StringType),
        th.Property("ItemGroupCode", th.StringType),
        th.Property("ItemGroupDescription", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Modifier", th.StringType),
        th.Property("ModifierFullName", th.StringType),
        th.Property("NewPrice", th.StringType),
        th.Property("NumberOfItemsPerUnit", th.StringType),
        th.Property("PriceListCode", th.StringType),
        th.Property("PriceListDescription",th.StringType),
        th.Property("PriceListPeriod", th.StringType),
        th.Property("Quantity",th.StringType),
        th.Property("Unit",th.StringType),
        th.Property("UnitDescription",th.StringType),
        th.Property("Timestamp", th.StringType)
    ).to_dict()

    @property
    def path(self):
        if self.sync_endpoint:
            return f"/sync/Sales/SalesPriceListVolumeDiscounts"
        return f"/sales/SalesPriceListVolumeDiscounts"

    @property
    def select(self):
        if self.sync_endpoint:
            return (
                f"BasePrice,BasePriceAmount,Created,Creator,CreatorFullName,Discount,Division,EntryMethod,ID,Item,ItemCode,ItemDescription,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NewPrice,NumberOfItemsPerUnit,PriceListCode,PriceListDescription,PriceListPeriod,Quantity,Unit,UnitDescription,Timestamp"
            )
        return f"BasePrice,BasePriceAmount,Created,Creator,CreatorFullName,Discount,Division,EntryMethod,ID,Item,ItemCode,ItemDescription,ItemGroup,ItemGroupCode,ItemGroupDescription,Modified,Modifier,ModifierFullName,NewPrice,NumberOfItemsPerUnit,PriceListCode,PriceListDescription,PriceListPeriod,Quantity,Unit,UnitDescription"

class PurchaseEntryLinesStream(ExactStream):
    name = "purchase_entry_lines"
    primary_keys = ["ID"]
    path = "/purchaseentry/PurchaseEntryLines?$filter=EntryID eq guid'{purchaseentry_id}'"
    select = None
    parent_stream_type = PurchaseEntiesStream

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AmountDC", th.StringType),
        th.Property("AmountFC", th.StringType),
        th.Property("Asset", th.StringType),
        th.Property("AssetDescription", th.StringType),
        th.Property("CostCenter", th.StringType),
        th.Property("CostCenterDescription", th.StringType),
        th.Property("CostUnit", th.StringType),
        th.Property("CostUnitDescription", th.StringType),
        th.Property("CustomField", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Division", th.StringType),
        th.Property("EntryID", th.StringType),
        th.Property("From", th.StringType),
        th.Property("GLAccount", th.StringType),
        th.Property("GLAccountCode", th.StringType),
        th.Property("GLAccountDescription", th.StringType),
        th.Property("IntraStatArea", th.StringType),
        th.Property("IntraStatCountry", th.StringType),
        th.Property("IntraStatDeliveryTerm", th.StringType),
        th.Property("IntraStatTransactionA", th.StringType),
        th.Property("IntraStatTransactionB", th.StringType),
        th.Property("IntraStatTransportMethod", th.StringType),
        th.Property("LineNumber", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("PrivateUsePercentage", th.StringType),
        th.Property("Project", th.StringType),
        th.Property("ProjectDescription", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("SerialNumber", th.StringType),
        th.Property("StatisticalNetWeight", th.StringType),
        th.Property("StatisticalNumber", th.StringType),
        th.Property("StatisticalQuantity", th.StringType),
        th.Property("StatisticalValue", th.StringType),
        th.Property("Subscription", th.StringType),
        th.Property("SubscriptionDescription", th.StringType),
        th.Property("To", th.DateTimeType),
        th.Property("TrackingNumber", th.StringType),
        th.Property("TrackingNumberDescription", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("VATAmountDC", th.StringType),
        th.Property("VATAmountFC", th.StringType),
        th.Property("VATBaseAmountDC", th.StringType),
        th.Property("VATBaseAmountFC", th.StringType),
        th.Property("VATCode", th.StringType),
        th.Property("VATCodeDescription", th.StringType),
        th.Property("VATNonDeductiblePercentage", th.StringType),
        th.Property("VATPercentage", th.StringType),
        th.Property("WithholdingAmountDC", th.StringType),
        th.Property("WithholdingTax", th.DateTimeType)
    ).to_dict()

    @property
    def select(self):
        return f"ID,AmountDC,AmountFC,Asset,AssetDescription,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,CustomField,Description,Division,EntryID,From,GLAccount,GLAccountCode,GLAccountDescription,IntraStatArea,IntraStatCountry,IntraStatDeliveryTerm,IntraStatTransactionA,IntraStatTransactionB,IntraStatTransportMethod,LineNumber,Notes,PrivateUsePercentage,Project,ProjectDescription,Quantity,SerialNumber,StatisticalNetWeight,StatisticalNumber,StatisticalQuantity,StatisticalValue,Subscription,SubscriptionDescription,To,TrackingNumber,TrackingNumberDescription,Type,VATAmountDC,VATAmountFC,VATBaseAmountDC,VATBaseAmountFC,VATCode,VATCodeDescription,VATNonDeductiblePercentage,VATPercentage,WithholdingAmountDC,WithholdingTax"
