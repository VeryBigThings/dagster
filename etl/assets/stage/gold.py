import os
import pandas as pd
from dagster import (
    AssetIn,
    asset,
)
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

# asset dim_customer_grade_codes
@asset(
    group_name="gold_dim",
    description="Store customer grade codes into warehouse",
    ins={
        "unique_customer_grade_code": AssetIn(
            "unique_customer_grade_code",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(config["silver_path_prefix"], "customer_grade_code")
            },
        ),
    },
)
def dim_customer_grade_codes(unique_customer_grade_code: pd.DataFrame):
    customer_grade_code_df = unique_customer_grade_code[["Customer", "GradeCode", "CustomerGradeCode"]]
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
    description="Collect scheduled loads facts",
    ins={
        "bol_po_joined": AssetIn(
            "bol_po_joined",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(config["silver_path_prefix"], "bol_po_joined")
            },
        ),
    },
)
def fact_scheduled_loads(bol_po_joined: pd.DataFrame):
    scheduled_loads = bol_po_joined[
        (bol_po_joined["LoadDate"].notnull()) & (bol_po_joined["ShippingDate"].isnull())
    ]
    scheduled_loads["LoadWeight"] = (
        scheduled_loads["RollWeight"] * scheduled_loads["Pieces"]
    )
    scheduled_loads = scheduled_loads.groupby(
        [
            "Customer",
            "CustomerCode",
            "PONumber",
            "PODate",
            "ShippingNumber",
            "Status",
            "TotalWeight",
            "RollID",
            "RollNumber",
            "RollSize",
            "GradeCode",
        ]
    )["LoadWeight"].sum()
    scheduled_loads = scheduled_loads.reset_index()

    return scheduled_loads.to_sql(
        "factScheduledLoads",
        config["warehouse_uri"],
        if_exists="replace",
        index=False,
    )


# asset fact_inv_not_shipped
@asset(
    group_name="gold_fact",
    description="Collect inventory not shipped facts",
    ins={
        "inv_bol_joined": AssetIn(
            "inv_bol_joined",
            input_manager_key="parquet_io_manager",
            metadata={
                "path": os.path.join(config["silver_path_prefix"], "inv_bol_joined")
            },
        ),
    },
)
def fact_inv_not_shipped(inv_bol_joined: pd.DataFrame):
    inv_bol_joined = inv_bol_joined[
        inv_bol_joined["ShippingNumber"].isnull() &
        inv_bol_joined["CustomerGradeCode"].notnull()
    ]

    return inv_bol_joined.to_sql(
        "factInventoryNotShipped",
        config["warehouse_uri"],
        if_exists="replace",
        index=False,
    )
