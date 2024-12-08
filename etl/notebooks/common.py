import os
import boto3
import pandas as pd
import polars as pl
from io import BytesIO
from etl.assets.config import config

class S3Helper:
    @staticmethod
    def download_fileobj(bucket: str, key: str) -> BytesIO:
        s3_client = boto3.client("s3")
        file_obj = BytesIO()
        s3_client.download_fileobj(bucket, key, file_obj)
        file_obj.seek(0)
        return file_obj

    @staticmethod
    def upload_fileobj(bucket: str, key: str, fileobj: BytesIO):
        if fileobj is None:
            raise ValueError("fileobj cannot be None.")

        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=fileobj.getvalue()
        )

def get_parquet_path(source_name, table_name, config_key="ingestion_sources"):
    source_config = config["ingestion_sources"][source_name]
    storage_path_prefix = source_config["storage_path_prefix"]
    return os.path.join(storage_path_prefix, table_name)

def load_ingested_parquet(source_name, table_name, lib="pandas"):
    path = get_parquet_path(source_name, f'{table_name}.parquet')
    file_obj = S3Helper.download_fileobj(os.environ["AWS_BUCKET_S3"], path)

    if lib == "polars":
        return pl.read_parquet(file_obj)

    return pd.read_parquet(file_obj)

def load_parquet(storage_path, lib="pandas"):
    file_obj = S3Helper.download_fileobj(os.environ["AWS_BUCKET_S3"], storage_path)

    if lib == "polars":
        return pl.read_parquet(file_obj)

    return pd.read_parquet(file_obj)