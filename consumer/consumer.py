# kafka/consumer.py
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
import os
from datetime import datetime

# Add project root to Python path to allow module imports
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from processing.transformer import transform_messages
from storage.writer import write_parquet
from storage.uploader import upload_to_minio


def connect_to_kafka(topic, bootstrap_servers):
    """Attempt to connect to Kafka with retries."""
    consumer = None
    attempts = 0
    while attempts < 10:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id='my-consumer-group-1',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False  # Disable auto-commit for manual control
            )
            print("✅ Connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            attempts += 1
            print(f"⏳ Kafka not available, attempt {attempts}/10. Retrying in 5s...")
            time.sleep(5)
    raise NoBrokersAvailable("❌ Could not connect to Kafka brokers after 10 attempts")


def consume_and_process(topic, bootstrap_servers, batch_size=10):
    """
    Connects once to Kafka and processes messages as a long-running service,
    using manual offset commits for reliability.
    """
    consumer = connect_to_kafka(topic, bootstrap_servers)
    
    try:
        messages = []
        print("👂 Listening for messages...")
        for message in consumer:
            messages.append(message.value)
            
            if len(messages) >= batch_size:
                try:
                    print(f"📦 Batch of {len(messages)} messages received. Processing...")
                    
                    # 1. Transform messages
                    transformed_df = transform_messages(messages)
                    
                    if not transformed_df.is_empty():
                        # 2. Write to Parquet
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_filename = f"batch_{timestamp}.parquet"
                        local_path = f"/tmp/{output_filename}"
                        
                        write_parquet(transformed_df, local_path)
                        print(f"📝 Saved transformed data to {local_path}")

                        # 3. Upload to MinIO
                        upload_to_minio(
                            file_path=local_path,
                            bucket="sensor-data",
                            object_name=f"processed/{output_filename}",
                            endpoint_url="http://minio:9000",
                            access_key="minioadmin",
                            secret_key="minioadmin"
                        )
                        
                        # 4. Manually commit the offset after successful processing
                        consumer.commit()
                        print("✅ Batch processed and offset committed.")

                        # 5. Clean up local file
                        os.remove(local_path)
                        print(f"🗑️ Removed local file {local_path}")

                except Exception as e:
                    print(f"❌ Error processing batch: {e}. Offsets will not be committed.")
                    # Note: We might want to add a dead-letter queue here in a real scenario
                finally:
                    messages = [] # Reset batch

    except KeyboardInterrupt:
        print("\n🛑 User interrupted. Shutting down...")
    except Exception as e:
        print(f"❌ A fatal error occurred: {e}")
    finally:
        if consumer:
            consumer.close()
            print("🛑 Consumer closed.")


if __name__ == "__main__":
    consume_and_process('sensor-data', ['kafka:9092'])
