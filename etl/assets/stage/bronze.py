import pandas as pd
from dagster import (
    Output,
    multi_asset,
)
from etl.assets.common.assets import get_ingestion_assets
from etl.assets.config import config

@multi_asset(outs=get_ingestion_assets())
def db_ingestion():
    for source_key, source_value in config["ingestion_sources"].items():
        for table in source_value["tables"]:
            yield Output(
                pd.read_sql_table(table, source_value["db_uri"]),
                output_name=f"{source_key}_{table}",
            )
