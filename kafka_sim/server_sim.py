from opcua import Server
import datetime
import random
import time
# Create a new OPC UA server instance
server = Server()
server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

name = "OPCUA_SIM_SERVER"
addspace = server.register_namespace(name)
objects = server.get_objects_node()
print(f"Server Namespace: {name}", f"URI: {addspace}")

machine = objects.add_object(addspace, "Machine1")
temp = machine.add_variable(addspace, "Temperature", 20.0)
pressure = machine.add_variable(addspace, "Pressure", 1.0)
cycle_time = machine.add_variable(addspace, "CycleTime", 0.5)
vibe = machine.add_variable(addspace, "Vibration", 0.1)
failures = machine.add_variable(addspace, "Failures", 0)

temp.set_writable()
pressure.set_writable()
cycle_time.set_writable()
failures.set_writable()
vibe.set_writable()

server.start()
print("OPC UA Server started at opc.tcp://localhost:4840")

try:
    count = 0
    while True:
        # --- Create a "snapshot" of values at a single point in time ---
        temp_val = 20.0 + random.random() * 5
        vibe_val = 0.1 + random.random() * 0.2
        pressure_val = 1.0 + random.random() * 0.5
        cycle_time_val = 0.5 + random.random() * 0.2
        # To ensure 'failures' changes and sends a notification, we'll make it alternate between 0 and 1
        # In a real scenario, you would just use the actual value.
        failures_val = count % 2 

        # --- Set all values at once ---
        print(f"Updating values for timestamp {count}...")
        temp.set_value(temp_val)
        vibe.set_value(vibe_val)
        pressure.set_value(pressure_val)
        cycle_time.set_value(cycle_time_val)
        failures.set_value(failures_val)
        
        count += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("Shutting down server")
    server.stop()
