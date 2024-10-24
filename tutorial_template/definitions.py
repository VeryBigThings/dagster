from dagster import Definitions, load_assets_from_modules
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagstermill import ConfigurableLocalOutputNotebookIOManager

from . import assets

s3_bucket = "st-paper-dev"

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        "io_manager": S3PickleIOManager(
            s3_bucket=s3_bucket,
            s3_resource=S3Resource(),
        ),
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
    },
)
