#!/bin/sh

echo "Waiting for MinIO to become available..."

try_connect() {
  /usr/bin/mc alias set minio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && return 0
  /usr/bin/mc alias set minio http://host.docker.internal:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && return 0
  return 1
}

until try_connect; do 
  echo "Waiting for MinIO..."
  sleep 2
done

echo "Creating buckets..."
/usr/bin/mc mb -p minio/qa-images || echo "Bucket qa-images already exists."
/usr/bin/mc mb -p minio/qa-metadata || echo "Bucket qa-metadata already exists."
/usr/bin/mc mb -p minio/sensor-data || echo "Bucket sensor-data already exists."
echo "Done."