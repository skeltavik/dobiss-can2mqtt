import can
import time
import yaml
import paho.mqtt.client as mqtt

# Load the configuration file
with open('config.yaml') as f:
    config = yaml.safe_load(f)
print(f"Loaded configuration: {config}")

# Create a CAN bus interface
bus = can.interface.Bus(channel='can0', bustype='socketcan', bitrate=125000)

# Create an MQTT client and connect to the MQTT broker
client = mqtt.Client()
client.connect("localhost", 1883)  # Replace with your MQTT broker URL

# Function to send a CAN message
def send_can_message(module, relay, state):
    # Construct the CAN data
    data = [module, relay, state, 0xFF, 0xFF] if state else [module, relay, 0, 0xFF, 0xFF]

    # Create a CAN message
    message = can.Message(arbitration_id=0x01FC0002 | (module << 8), data=data, is_extended_id=True)

    # Send the CAN message
    print(f"Sending CAN message: {message}")
    bus.send(message)

# Function to control a light
def control_light(address, state):
    # Convert the address to a string
    address = str(address)

    for light in config['lights']:
        if light['address'] == address:
            # Convert the address to a module and relay
            module = int(light['address'][:2], 16)
            relay = int(light['address'][2:], 16)

            # Send the CAN message
            print(f"Controlling light: {light['name']} (address: {address}, state: {state})")
            send_can_message(module, relay, state)
            return light['name']
    else:
        print(f"No light found with address: {address}")
        return None

# The on_connect callback function
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("dobiss/light/+/state/set")  # Subscribe to the 'set' topics of all devices and nodes

# The on_message callback function
def on_message(client, userdata, msg):
    print(f"Received message: {msg.topic} {msg.payload}")
    # Print the topic before splitting
    print(f"Original topic: {msg.topic}")
    # Split the topic into parts
    parts = msg.topic.split('/')
    print(f"Split parts: {parts}")
    if len(parts) == 5:
        if parts[0] == 'dobiss' and parts[1] == 'light' and parts[3] == 'state' and parts[4] == 'set':
            # The topic is in the correct format
            # Extract the light address from the topic
            light_address = parts[2]
            print(f"Extracted light address: {light_address}")
            # The payload is the desired state
            try:
                state = int(msg.payload)  # Assuming the state is sent as an integer
                print(f"Extracted state: {state}")
            except ValueError:
                print(f"Invalid state: {msg.payload}")
                return
            # Control the light
            light_name = control_light(light_address, state)
            if light_name is not None:
                print(f"Changed state of light {light_name} to {state}")
            else:
                print(f"Failed to change state of light with address {light_address} to {state}")
        else:
            print("Invalid topic format")
    else:
        print("Invalid topic length")

client.on_connect = on_connect
client.on_message = on_message

client.loop_start()

while True:
    time.sleep(1)  # Keep the script running
