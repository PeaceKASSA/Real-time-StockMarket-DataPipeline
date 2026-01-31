
import json
import time
import boto3
from kafka import KafkaConsumer


#MinIO S3bucket Setup(I left my default credentials)
bucket_name = "bronze-transactions"

s3 = boto3.client(
    service_name="s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)


def ensure_bucket():
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket {bucket_name}.")


#Kafka Consumer Setup
def create_consumer():
    return KafkaConsumer(
        "stock-quotes",
        bootstrap_servers=["localhost:29092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-consumer1",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )


#Processing Logic
def persist_record(record):
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )

    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")


def run():
    ensure_bucket()
    consumer = create_consumer()

    print("Consumer streaming and saving to MinIO...")

    for message in consumer:
        persist_record(message.value)


if __name__ == "__main__":
    run()
