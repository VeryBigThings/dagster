import os
import pandas as pd
from dagster import (
    AssetIn,
    asset,
)

from etl.assets.common import get_ingestion_assets
from ..config import config


@asset(
    group_name="gold_dim",
    description="Store customers into warehouse",
    ins={
        "unique_customers": AssetIn(
            "unique_customers",
            input_manager_key="parquet_io_manager",
            metadata={"path": os.path.join(config["silver_path_prefix"], "customers")},
        ),
    },
)
def dim_customers(unique_customers: pd.DataFrame):
    customer_df = unique_customers[["Customer"]]
    customer_df.set_index("Customer", inplace=True)

    return customer_df.to_sql(
        "dimCustomer",
        config["warehouse_uri"],
        if_exists="replace",
        index=True,
        index_label="Customer",
    )


@asset(
    group_name="gold_dim",
    description="Store customer grade codes into warehouse",
    ins={
        "unique_customer_grade_code": AssetIn(
            "unique_customer_grade_code",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(
                    config["silver_path_prefix"], "customer_grade_code"
                )
            },
        ),
    },
)
def dim_customer_grade_codes(unique_customer_grade_code: pd.DataFrame):
    customer_grade_code_df = unique_customer_grade_code[
        ["Customer", "GradeCode", "CustomerGradeCode"]
    ]
    customer_grade_code_df.set_index(["Customer", "GradeCode"], inplace=True)

    return customer_grade_code_df.to_sql(
        "dimCustomerGradeCode",
        config["warehouse_uri"],
        if_exists="replace",
        index=True,
        index_label=["Customer", "GradeCode"],
    )


@asset(
    group_name="gold_fact",
    description="Facts about scheduled loads",
    ins={
        "po_bol_joined": AssetIn(
            "po_bol_joined",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(config["silver_path_prefix"], "po_bol_joined")
            },
        )
    },
)
def fact_scheduled_loads(po_bol_joined: pd.DataFrame):
    scheduled_loads = po_bol_joined[
        (po_bol_joined["BOLH_LoadDate"].notnull()) & (po_bol_joined["BOLH_ShippingDate"].isnull())
    ]

    scheduled_loads = scheduled_loads.groupby(
        [
            "PO_Customer",
            "POD_CustomerCode",
            "PO_PONumber",
            "PO_PODate",
            "BOLH_ShippingNumber",
        ]
    ).agg(
        LoadPounds=("BOLD_RollWeight", "sum"),
        LoadPieces=("BOLD_Pieces", "sum"),
    )

    scheduled_loads["LoadTons"] = scheduled_loads["LoadPounds"] / 2000
    scheduled_loads = scheduled_loads.reset_index()
    scheduled_loads = scheduled_loads.drop(columns=["LoadPounds"])

    scheduled_loads = scheduled_loads.rename(
        columns={
            "PO_Customer": "Customer",
            "POD_CustomerCode": "CustomerCode",
            "PO_PONumber": "PONumber",
            "PO_PODate": "PODate",
            "BOLH_ShippingNumber": "ShippingNumber",
        }
    )

    return scheduled_loads.to_sql(
        "factScheduledLoads",
        config["warehouse_uri"],
        if_exists="replace",
        index=False,
    )


@asset(
    group_name="gold_fact",
    description="Facts about non-shipped (old) inventory",
    ins=get_ingestion_assets(
        asset_keys=[
            "production_tblWrapperProduction",
        ],
        per_asset_params=["key", "metadata"],
        asset_direction_class=AssetIn,
    ),
)
def fact_inv_not_shipped(production_tblWrapperProduction: pd.DataFrame):
    tblWrapperProduction = production_tblWrapperProduction

    inv_non_shipped = tblWrapperProduction[
        tblWrapperProduction["DateShipped"].isnull() &
        tblWrapperProduction["CustomerGradeCode"].notnull()
    ].sort_values("DateEntered")

    return inv_non_shipped.to_sql(
        "factInventoryNotShipped",
        config["warehouse_uri"],
        if_exists="replace",
        index=False,
    )

# asset fact_po_production, ins po_bol_joined
@asset(
    group_name="gold_fact",
    description="Facts about PO production",
    ins={
        "po_bol_joined": AssetIn(
            "po_bol_joined",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(config["silver_path_prefix"], "po_bol_joined")
            },
        )
    },
)
def fact_po_production(po_bol_joined: pd.DataFrame):
    po_production = po_bol_joined

    po_production = po_production.groupby(
        [
            "PO_Customer",
            "POD_CustomerCode",
            "POD_TonQty",
            "PO_PONumber",
            "PO_PODate",
            "BOLH_ShippingNumber",
            "BOLH_LoadDate",
            "BOLH_ShippingDate",
        ]
    ).agg(
        LoadPounds=("BOLD_RollWeight", "sum"),
        LoadPieces=("BOLD_Pieces", "sum"),
    )

    po_production["LoadTons"] = po_production["LoadPounds"] / 2000
    po_production = po_production.reset_index()
    po_production = po_production.drop(columns=["LoadPounds"])

    po_production = po_production.rename(
        columns={
            "PO_Customer": "Customer",
            "POD_CustomerCode": "CustomerCode",
            "PO_PONumber": "PONumber",
            "PO_PODate": "PODate",
            "POD_TonQty": "TotalTons",
            "BOLH_ShippingNumber": "ShippingNumber",
            "BOLH_LoadDate": "LoadDate",
            "BOLH_ShippingDate": "ShippingDate",
        }
    )

    return po_production.to_sql(
        "factPOProduction",
        config["warehouse_uri"],
        if_exists="replace",
        index=False,
    )