import os
import pandas as pd
from dagster import (
    AssetIn,
    asset,
)
from etl.assets.common import (
    convert_columns_to_Int64,
    get_distinct_across_columns,
    get_ingestion_assets,
    config,
)


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
    customers = (
        pd.concat(
            [
                production_tblPurchaseOrder["Customer"],
                production_tlkpCustomer["Customer"],
                production_tblBillofLadingHeader["Customer"],
            ]
        )
        .str.strip()
        .str.upper()
    )

    return customers.drop_duplicates().to_frame(name="Customer")


# unique CustomerGradeCode
@asset(
    metadata={
        "path": os.path.join(config["silver_path_prefix"], "customer_grade_code")
    },
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
    return get_distinct_across_columns(
        production_tblWrapperProduction, ["Customer", "GradeCode", "CustomerGradeCode"]
    )


@asset(
    metadata={"path": os.path.join(config["silver_path_prefix"], "po_bol_joined")},
    io_manager_key="parquet_io_manager",
    group_name="silver",
    description="Join PO, POD, BOLH, BOLD on PONumber, CustomerCode and ShippingNumber",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblBillofLadingDetail",
            "production_tblBillofLadingHeader",
            "production_tblPurchaseOrder",
            "production_tblPurchaseOrderDetail",
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def po_bol_joined(
    production_tblBillofLadingDetail: pd.DataFrame,
    production_tblBillofLadingHeader: pd.DataFrame,
    production_tblPurchaseOrder: pd.DataFrame,
    production_tblPurchaseOrderDetail: pd.DataFrame,
):
    tblPurchaseOrder = production_tblPurchaseOrder.add_prefix('PO_')
    tblPurchaseOrderDetail = production_tblPurchaseOrderDetail.add_prefix('POD_')
    tblBillofLadingHeader = production_tblBillofLadingHeader.add_prefix('BOLH_')
    tblBillofLadingDetail = production_tblBillofLadingDetail.add_prefix('BOLD_')

    PO_join = pd.merge(tblPurchaseOrder, tblPurchaseOrderDetail, left_on="PO_PONumber", right_on="POD_PONumber", how="inner")
    BOL_join = pd.merge(tblBillofLadingDetail, tblBillofLadingHeader, left_on=["BOLD_ShippingNumber", "BOLD_PONumber"], right_on=["BOLH_ShippingNumber", "BOLH_PONumber"], how="inner")
    PO_BOL_join = pd.merge(PO_join, BOL_join, left_on=["POD_PONumber", "POD_CustomerCode"], right_on=["BOLD_PONumber", "BOLD_CustCode"], how="inner")

    return PO_BOL_join
