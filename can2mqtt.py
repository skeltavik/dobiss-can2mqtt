from collections import deque
import can
import paho.mqtt.client as mqtt
import yaml
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Enable logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# MQTT settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# CAN settings
CAN_INTERFACE = "socketcan"
CAN_CHANNEL = "can0"

# CAN protocol arbitration IDs (Dobiss, reverse-engineered by dries007)
ARBIT_GET_REQUEST = 0x01FCFF01  # GET state request:  [module, relay]
ARBIT_GET_REPLY   = 0x01FDFF01  # GET state reply:    [state]
ARBIT_SET_REPLY   = 0x0002FF01  # SET state reply:    [module, relay, state]


def load_config(path="config.yaml"):
    """Load light configuration from a YAML file."""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def parse_address(address_str):
    """Parse a hex address string into a (module, relay) tuple.

    Example: '0107' -> (1, 7)
    """
    address = int(address_str, 16)
    return address >> 8, address & 0xFF


def parse_state(payload):
    """Parse an MQTT payload into a CAN state value.

    Returns 1 for ON/1, 0 for OFF/0, or None for unrecognised payloads.
    """
    if payload in [b"ON", b"1"]:
        return 1
    if payload in [b"OFF", b"0"]:
        return 0
    return None


def build_set_message(module, relay, state):
    """Build a CAN message that sets a relay to a given state."""
    arbitration_id = 0x01FC0002 | (module << 8)
    data = [module, relay, state, 0xFF, 0xFF]
    return can.Message(arbitration_id=arbitration_id, data=data, is_extended_id=True)


def handle_mqtt_message(topic, payload, config, bus):
    """Process an incoming MQTT message and send the corresponding CAN command.

    Returns True if a matching light was found, False otherwise.
    """
    for light in config:
        if topic == f"dobiss/light/{light['address']}/state/set":
            module, relay = parse_address(light["address"])
            state = parse_state(payload)
            if state is not None:
                message = build_set_message(module, relay, state)
                bus.send(message)
                logger.debug("Sent CAN message: %s", message)
            return True
    return False


def handle_can_message(message, config, client, pending_gets=None):
    """Process an incoming CAN message and publish the corresponding MQTT state.

    pending_gets is a collections.deque used to pair GET requests with their
    replies. Pass the same instance on every call within a bus session; the
    queue is populated when a GET request is snooped and consumed when the
    matching GET reply arrives. When omitted (or None) GET replies are silently
    ignored.

    Background: the GET reply frame (0x01FDFF01) carries only a state byte — it
    contains no module/relay address. Without tracking which GET request was
    issued, it is impossible to determine which light the reply refers to.
    """
    arb = message.arbitration_id

    if arb == ARBIT_GET_REQUEST:
        # Snoop the GET request so we can correlate the reply later.
        if pending_gets is not None:
            pending_gets.append((message.data[0], message.data[1]))
        return

    if arb == ARBIT_SET_REPLY:
        for light in config:
            module, relay = parse_address(light["address"])
            if message.data[0] == module and message.data[1] == relay:
                state_str = "ON" if message.data[2] == 1 else "OFF"
                client.publish(f"dobiss/light/{light['address']}/state", state_str, retain=True)
                logger.debug("Published MQTT message: %s", message)
                return

    if arb == ARBIT_GET_REPLY and pending_gets:
        req_module, req_relay = pending_gets.popleft()
        for light in config:
            module, relay = parse_address(light["address"])
            if module == req_module and relay == req_relay:
                state_str = "ON" if message.data[0] == 1 else "OFF"
                client.publish(f"dobiss/light/{light['address']}/state", state_str, retain=True)
                logger.debug("Updated light state based on GET reply: %s", message)
                return


def make_on_connect(config):
    """Return an on_connect callback that subscribes to all configured lights."""
    def on_connect(client, userdata, flags, rc):
        logger.debug("Connected with result code %s", rc)
        for light in config:
            client.subscribe(f"dobiss/light/{light['address']}/state/set")
    return on_connect


def make_on_message(config, bus):
    """Return an on_message callback that forwards MQTT messages to the CAN bus."""
    def on_message(client, userdata, msg):
        logger.debug("%s %s", msg.topic, msg.payload)
        handle_mqtt_message(msg.topic, msg.payload, config, bus)
    return on_message


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves the config file."""

    config_path = "config.yaml"

    def do_GET(self):
        if self.path == "/config.yaml":
            self.send_response(200)
            self.send_header("Content-type", "text/yaml")
            self.end_headers()
            with open(self.config_path, "r") as file:
                self.wfile.write(file.read().encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        logger.debug(format, *args)


if __name__ == "__main__":
    config = load_config("config.yaml")

    # CAN bus setup
    bus = can.Bus(bustype=CAN_INTERFACE, channel=CAN_CHANNEL, bitrate=125000, receive_own_messages=True)
    bus.set_filters([
        {"can_id": ARBIT_GET_REQUEST, "can_mask": 0x1FFFFFFF, "extended": True},  # GET request (snoop)
        {"can_id": ARBIT_SET_REPLY,   "can_mask": 0x1FFFFFFF, "extended": True},  # Reply to SET
        {"can_id": ARBIT_GET_REPLY,   "can_mask": 0x1FFFFFFF, "extended": True},  # Reply to GET
    ])

    # MQTT client setup
    client = mqtt.Client()
    client.on_connect = make_on_connect(config)
    client.on_message = make_on_message(config, bus)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # HTTP server
    httpd = HTTPServer(("0.0.0.0", 8000), RequestHandler)
    threading.Thread(target=httpd.serve_forever).start()

    # CAN bus loop
    pending_gets = deque()
    while True:
        message = bus.recv()
        handle_can_message(message, config, client, pending_gets)

    client.loop_stop()
