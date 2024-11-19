# Source: https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py

import os
from typing import Union

import pandas
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    _check as check,
)
from dagster._seven.temp_dir import get_system_temp_directory


class PartitionedParquetIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    @property
    def _base_path(self):
        raise NotImplementedError()

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        path = self._get_path(context)
        if "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(path=path, index=False)
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> Union[pandas.DataFrame, str]:
        path = self._get_path(context)
        if context.dagster_type.typing_type == pandas.DataFrame:
            return pandas.read_parquet(path, engine="pyarrow")

        return check.failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        path = context.definition_metadata.get("path", "")

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            return os.path.join(self._base_path, path, f"{partition_str}.parquet")
        else:
            return os.path.join(self._base_path, f"{path}.parquet")


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    base_path: str = get_system_temp_directory()

    @property
    def _base_path(self):
        return self.base_path


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    s3_bucket: str

    @property
    def _base_path(self):
        return "s3://" + self.s3_bucket
