from dagster import Definitions, load_assets_from_modules
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from etl.resources.parquet_io_manager import S3PartitionedParquetIOManager

from . import assets

s3_bucket = "st-paper-dev-storage"

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "io_manager": S3PickleIOManager(
            s3_bucket=s3_bucket,
            s3_resource=S3Resource(),
        ),
        "parquet_io_manager": S3PartitionedParquetIOManager(s3_bucket=s3_bucket),
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
    },
)
