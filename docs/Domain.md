# Purchase Order and Bill of Landing

### Tables used and their relationships

- tblPurchaseOrder
- tblPurchaseOrderDetail
- tblBillofLadingDetail
- tblBillofLadingHeader

#### Joining tables

- tblPurchaseOrder and tblPurchaseOrderDetail can be joined on PONumber
- tblBillofLadingDetail and tblBillofLadingHeader can be joined on ShippingNumber and PONumber
- previous PO and BOL joins can further be joined on PONumber and CustomerCode

tblPurchaseOrderDetail connects to tblBillofLadingDetail through PONumber, CustomerCode.

Each tblPurchaseOrderDetail can have multiple corresponding tblBillofLadingDetail records (as rolls are produced they get sent in batches to the customer, until total amount for certain CustomerCode from tblPurchaseOrderDetail is fullfilled).

### Scheduled loads

Scheduled loads are defined as loads (tblBillofLadingDetail records) that have LoadDate set, but are yet to be shipped (ShippingDate is NULL).

### Old inventory

Old inventory is defined as inventory that is in the warehouse, but is not yet shipped (DateShipped is NULL).
