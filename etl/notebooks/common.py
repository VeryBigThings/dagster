import os
import boto3
import pandas as pd
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

def get_parquet_path(source_name, table_name):
    source_config = config["ingestion_sources"][source_name]
    storage_path_prefix = source_config["storage_path_prefix"]
    return os.path.join(storage_path_prefix, table_name)

def load_parquet(source_name, table_name):
    path = get_parquet_path(source_name, f'{table_name}.parquet')
    file_obj = S3Helper.download_fileobj(os.environ["AWS_BUCKET_S3"], path)
    return pd.read_parquet(file_obj)
