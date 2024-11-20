import os
import pandas as pd
from dagster import (
    AssetIn,
    asset,
)
from ..config import config


@asset(
    group_name="gold",
    description="Store customers into warehouse",
    ins={
        "unique_customers": AssetIn(
            "unique_customers",
            input_manager_key="parquet_io_manager",
            metadata={"path": os.path.join(config["silver_path_prefix"], "customers")},
        ),
    },
)
def customer_to_warehouse(unique_customers: pd.DataFrame):
    customer_df = unique_customers[["Customer"]]
    customer_df.set_index("Customer", inplace=True)
    return customer_df.to_sql(
        "dimCustomer",
        config["warehouse_uri"],
        if_exists="replace",
        index=True,
        index_label="Customer",
    )
