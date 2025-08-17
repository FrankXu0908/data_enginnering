# Orchestrate pipeline: consume → process → store
# main.py

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    PARQUET_OUTPUT_PATH,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET, MINIO_OBJECT_NAME
)

from consumer.consumer import consume_batch
from processing.transformer import transform_messages
from storage.writer import write_parquet
from storage.uploader import upload_to_minio


def main():
    print("📥 Consuming messages from Kafka...")
    messages = consume_batch(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )

    if not messages:
        print("⚠️  No messages received.")
        return

    print(f"✅ Received {len(messages)} messages")

    print("🔧 Transforming with Polars...")
    df = transform_messages(messages)

    print("💾 Writing Parquet...")
    saved_path = write_parquet(df, PARQUET_OUTPUT_PATH)

    print("☁️ Uploading to MinIO...")
    upload_success = upload_to_minio(
        file_path=saved_path,
        bucket=MINIO_BUCKET,
        object_name=MINIO_OBJECT_NAME,
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    if upload_success:
        print("✅ All done!")
    else:
        print("❌ Upload failed.")

if __name__ == "__main__":
    main()