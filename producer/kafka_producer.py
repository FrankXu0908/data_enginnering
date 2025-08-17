from opcua import Client
from datetime import datetime, timedelta
import time
import threading
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ✅ Global dictionary to cache latest values
latest_values = {
    "temperature": (None, None),  # (timestamp, value)
    "pressure": (None, None),
    "cycle_time": (None, None),
    "vibration": (None, None),
    "failure_next_hour": (None, None)
}
lock = threading.Lock()  # To make it thread-safe



producer = None
for attempt in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka broker")
        break
    except NoBrokersAvailable:
        print(f"❌ Kafka broker not available, retrying... ({attempt+1}/10)")
        time.sleep(5)
if producer is None:
    raise RuntimeError("Could not connect to Kafka broker after multiple attempts")
topic = 'sensor-data'

class SubHandler:
    def __init__(self, node_id_map):
        self.node_id_map = node_id_map
    def datachange_notification(self, node, val, data):
        tag = self.node_id_map.get(node.nodeid.to_string(), "unknown")
        timestamp = datetime.now()
        with lock:
            # ✅ Store value based on tag
            if tag == "temperature":
                latest_values["temperature"] = (datetime.now().replace(microsecond=1), float(val))
            elif tag == "pressure":
                latest_values["pressure"] = (datetime.now().replace(microsecond=1), float(val))
            elif tag == "cycle_time":
                latest_values["cycle_time"] = (datetime.now().replace(microsecond=1), float(val))
            elif tag == "vibration":
                latest_values["vibration"] = (datetime.now().replace(microsecond=1), float(val))
            elif tag == "failure_next_hour":
                latest_values["failure_next_hour"] = (datetime.now().replace(microsecond=1), int(val))

        times = [ts for ts, _ in latest_values.values()]
        # Send to Kafka topic
        if all(ts is not None for ts in times) and max(times) - min(times) < timedelta(seconds=1):
            try:
                values = {k: v for k, (_, v) in latest_values.items()}
                values["timestamp"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                producer.send(topic, value=values)
                print("✅ Sent to Kafka:", values)
            except Exception as e:
                print("Kafka send error:", e)
            for key in latest_values:
                latest_values[key] = (None, None)

def main():
    # Connect to OPC UA Server with retry logic
    client = None
    for attempt in range(20):
        try:
            client = Client("opc.tcp://host.docker.internal:4840/freeopcua/server/")
            client.connect()
            print("✅ Connected to OPC UA server")
            break
        except Exception as e:
            print(f"❌ OPC UA connection failed: {e} (attempt {attempt+1}/20)")
            time.sleep(5)
    if client is None:
        raise RuntimeError("Could not connect to OPC UA server after multiple attempts")
    try:
        print("Namespace Array:", client.get_namespace_array())
        # Browse nodes dynamically
        root = client.get_root_node()
        print("Root children:", root.get_children())

        objects = client.get_objects_node()
        print("Objects children:", objects.get_children())
        machine1 = objects.get_child(["2:Machine1"])  
        nodes = {
            "temperature": machine1.get_child(["2:Temperature"]),
            "pressure": machine1.get_child(["2:Pressure"]),
            "cycle_time": machine1.get_child(["2:CycleTime"]),
            "vibration": machine1.get_child(["2:Vibration"]),
            "failure_next_hour": machine1.get_child(["2:Failures"])
        }
        # Reverse map for quick lookup during callback
        node_id_map = {v.nodeid.to_string(): k for k, v in nodes.items()}
        handler = SubHandler(node_id_map)
        sub = client.create_subscription(1000, handler)

        for node in nodes.values():
            print("Subscribing to:", node)
            sub.subscribe_data_change(node, queuesize=1)
        print("📡 Subscribed to data changes... Press Ctrl+C to exit.")       
        try:
            while True:
                time.sleep(1)  # Keep the script running
        except KeyboardInterrupt:
            print("👋 Exiting...")
    except Exception as e:
        print("❌ OPC UA Error:", e)
    finally:
        try:
            client.disconnect()
            print("🔌 Disconnected from OPC UA Server")
        except Exception:
            pass  # ignore if connection never succeeded


if __name__ == "__main__":
    main()
