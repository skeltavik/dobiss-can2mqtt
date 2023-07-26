import asyncio
import can
import paho.mqtt.client as mqtt
import yaml
import logging

# Enable logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load configuration
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)
logger.debug("Loaded configuration: %s", config)

# MQTT settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# CAN settings
CAN_INTERFACE = "socketcan"
CAN_CHANNEL = "can0"

# CAN bus setup
bus = can.Bus(bustype=CAN_INTERFACE, channel=CAN_CHANNEL, bitrate=125000, receive_own_messages=True)

# Set up CAN filters
bus.set_filters([
    {"can_id": 0x0002FF01, "can_mask": 0x1FFFFFFF, "extended": True},  # Reply to SET
    {"can_id": 0x01FDFF01, "can_mask": 0x1FFFFFFF, "extended": True},  # Reply to GET
])

# MQTT client setup
client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    logger.debug("Connected with result code %s", rc)
    # Subscribe to MQTT topics for all lights
    for light in config:
        client.subscribe(f"dobiss/light/{light['address']}/state/set")

def on_message(client, userdata, msg):
    logger.debug("%s %s", msg.topic, msg.payload)
    # Find the light that corresponds to the MQTT topic
    for light in config:
        if msg.topic == f"dobiss/light/{light['address']}/state/set":
            # Convert the light's address to a CAN message and send it
            address = int(light["address"], 16)
            module = address >> 8
            relay = address & 0xFF
            state = 1 if msg.payload in [b"ON", b"1"] else 0
            arbitration_id = 0x01FC0002 | (module << 8)
            data = [module, relay, state, 0xFF, 0xFF]
            message = can.Message(arbitration_id=arbitration_id, data=data, is_extended_id=True)
            bus.send(message)
            logger.debug("Sent CAN message: %s", message)
            break

client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start MQTT loop
client.loop_start()

# CAN bus loop
while True:
    message = bus.recv()
    # Find the light that corresponds to the CAN message
    for light in config:
        address = int(light["address"], 16)
        module = address >> 8
        relay = address & 0xFF
        if message.arbitration_id == 0x0002FF01 and message.data[0] == module and message.data[1] == relay:
            # Publish an MQTT message with the light's state
            client.publish(f"dobiss/light/{light['address']}/state", "ON" if message.data[2] == 1 else "OFF", retain=True)
            logger.debug("Published MQTT message: %s", message)
        elif message.arbitration_id == 0x01FDFF01:
            # Update the light's state based on the reply to the GET command
            client.publish(f"dobiss/light/{light['address']}/state", "ON" if message.data[0] == 1 else "OFF", retain=True)
            logger.debug("Updated light state based on GET reply: %s", message)

# Stop MQTT loop
client.loop_stop()
