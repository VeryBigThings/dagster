import os
import pandas as pd
from dagster import (
    AssetIn,
    asset,
)
from etl.assets.common import convert_columns_to_Int64, get_distinct_across_columns, get_ingestion_assets, config


@asset(
    metadata={"path": os.path.join(config["silver_path_prefix"], "customers")},
    io_manager_key="parquet_io_manager",
    group_name="silver",
    description="Collect unique customers",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblPurchaseOrder",
            "production_tlkpCustomer",
            "production_tblBillofLadingHeader",
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def unique_customers(
    production_tblPurchaseOrder: pd.DataFrame,
    production_tlkpCustomer: pd.DataFrame,
    production_tblBillofLadingHeader: pd.DataFrame,
):
    customers = pd.concat(
        [
            production_tblPurchaseOrder["Customer"],
            production_tlkpCustomer["Customer"],
            production_tblBillofLadingHeader["Customer"],
        ]
    ).str.strip().str.upper()

    return customers.drop_duplicates().to_frame(name="Customer")

# unique CustomerGradeCode
@asset(
    metadata={"path": os.path.join(config["silver_path_prefix"], "customer_grade_code")},
    io_manager_key="parquet_io_manager",
    group_name="silver",
    description="Collect unique combination of CustomerGradeCode, Customer and GradeCode",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblWrapperProduction",
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def unique_customer_grade_code(production_tblWrapperProduction: pd.DataFrame):
    return get_distinct_across_columns(production_tblWrapperProduction, ["Customer", "GradeCode", "CustomerGradeCode"])


@asset(
    metadata={"path": os.path.join(config["silver_path_prefix"], "bol_po_joined")},
    io_manager_key="parquet_io_manager",
    group_name="silver",
    description="Collect BOL and PO joined",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblBillofLadingDetail",
            "production_tblBillofLadingHeader",
            "production_tblPurchaseOrder"
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def bol_po_joined(production_tblBillofLadingDetail: pd.DataFrame, production_tblBillofLadingHeader: pd.DataFrame, production_tblPurchaseOrder: pd.DataFrame):
    tblBillofLadingDetail = production_tblBillofLadingDetail.drop(columns=['Comment'])
    tblBillofLadingHeader = production_tblBillofLadingHeader.drop(columns=['PONumber', 'CustomerCode', 'Customer'])
    tblPurchaseOrder = production_tblPurchaseOrder.drop(columns=['CustLocation'])

    convert_columns_to_Int64(tblBillofLadingHeader, ['ShippingNumber'])
    convert_columns_to_Int64(tblBillofLadingDetail, ['ShippingNumber'])

    BOL_joined = pd.merge(tblBillofLadingDetail, tblBillofLadingHeader, on='ShippingNumber', how='inner')
    BOL_PO_joined = pd.merge(BOL_joined, tblPurchaseOrder, on='PONumber', how='inner')

    return BOL_PO_joined

# inv_bol_joined asset
@asset(
    metadata={"path": os.path.join(config["silver_path_prefix"], "inv_bol_joined")},
    io_manager_key="parquet_io_manager",
    group_name="silver",
    description="Collect INV and BOL joined",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblBillofLadingDetail",
            "production_tblBillofLadingHeader",
            "production_tblInventoryInquiry"
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def inv_bol_joined(production_tblBillofLadingDetail: pd.DataFrame, production_tblBillofLadingHeader: pd.DataFrame, production_tblInventoryInquiry: pd.DataFrame):
    tblBillofLadingDetail = production_tblBillofLadingDetail
    tblBillofLadingHeader = production_tblBillofLadingHeader
    tblInventoryInquiry = production_tblInventoryInquiry

    bol_joined = pd.merge(tblBillofLadingDetail, tblBillofLadingHeader, on='ShippingNumber', how='inner')
    inv_bol_joined = pd.merge(tblInventoryInquiry, bol_joined, on='RollID', how='outer')

    return inv_bol_joined
