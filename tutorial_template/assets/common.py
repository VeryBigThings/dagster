import os
from .config import config
from dagster import AssetOut


def get_ingestion_asset_params(source_key, table, keys):
    all_keys = {
        "key": f"{source_key}_{table}",
        "metadata": {
            "path": os.path.join(
                config["ingestion_sources"][source_key]["storage_path_prefix"], table
            )
        },
        "io_manager_key": "parquet_io_manager",
        "group_name": "bronze",
        "description": f"Store {source_key}: {table} to parquet",
    }
    return {k: all_keys[k] for k in keys}


def get_ingestion_assets(
    asset_keys=None, per_asset_params=None, asset_direction_class=AssetOut
):
    """
    Get ingestion assets for the given asset keys, or all assets if asset_keys is None

    Args:
        asset_keys (List[str]): List of asset keys to get
        per_asset_params (List[str]): List of parameters to get for each asset
        asset_direction_class (AssetIn or AssetOut): Class to use for the asset
    """
    assets = {
        f"{source_key}_{table}": asset_direction_class(
            **get_ingestion_asset_params(
                source_key,
                table,
                (
                    ["key", "metadata", "io_manager_key", "group_name", "description"]
                    if per_asset_params is None
                    else per_asset_params
                ),
            )
        )
        for source_key, source_value in config["ingestion_sources"].items()
        for table in source_value["tables"]
    }

    if asset_keys:
        return {k: assets[k] for k in asset_keys}

    return assets
