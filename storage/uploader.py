# Upload to MinIO
# storage/uploader.py

import boto3
from botocore.exceptions import NoCredentialsError
import os

def upload_to_minio(file_path: str, bucket: str, object_name: str,
                    endpoint_url: str, access_key: str, secret_key: str) -> bool:
    """
    Uploads file to MinIO/S3.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3.upload_file(file_path, bucket, object_name)
        print(f"✅ Uploaded {file_path} to s3://{bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return False
    except NoCredentialsError:
        print("❌ Invalid MinIO credentials")
        return False
    
upload_to_minio(
    file_path="data/processed/batch_output.parquet",
    bucket="qa-metadata",
    object_name="batches/batch_output.parquet",
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin"
)