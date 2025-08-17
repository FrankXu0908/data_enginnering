# config/settings.py

KAFKA_BOOTSTRAP_SERVERS = 'K:kafka:9092'
KAFKA_TOPIC = 'sensor-data'

PARQUET_OUTPUT_PATH = 'data/processed/batch_output.parquet'

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'qa-metadata'
MINIO_OBJECT_NAME = 'batches/batch_output.parquet'
