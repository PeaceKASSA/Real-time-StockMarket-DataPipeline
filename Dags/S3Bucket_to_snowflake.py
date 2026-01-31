import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = "<<YOUR_USERNAME>>"
SNOWFLAKE_PASSWORD = "<<YOUR_PASSWORD>>"
SNOWFLAKE_ACCOUNT = "<<YOUR_ACCOUNT>>"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"


#Helpers
def _minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )


def _snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )


#Tasks
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = _minio_client()
    response = s3.list_objects_v2(Bucket=BUCKET)
    objects = response.get("Contents", [])

    local_files = []

    for item in objects:
        key = item["Key"]
        filename = os.path.basename(key)
        destination = os.path.join(LOCAL_DIR, filename)

        s3.download_file(BUCKET, key, destination)
        print(f"Downloaded {key} -> {destination}")

        local_files.ap_
