from collections import deque
import os
import re
import time
import can
import paho.mqtt.client as mqtt
import yaml
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

logger = logging.getLogger(__name__)

# MQTT settings
MQTT_BROKER = os.getenv("DOBISS_MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("DOBISS_MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("DOBISS_MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("DOBISS_MQTT_PASSWORD")
MQTT_TLS = os.getenv("DOBISS_MQTT_TLS", "false").lower() in {"1", "true", "yes"}

# CAN settings
CAN_INTERFACE = os.getenv("DOBISS_CAN_INTERFACE", "socketcan")
CAN_CHANNEL = os.getenv("DOBISS_CAN_CHANNEL", "can0")

CONFIG_PATH = os.getenv("DOBISS_CONFIG_PATH", "config.yaml")
HTTP_HOST = os.getenv("DOBISS_HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.getenv("DOBISS_HTTP_PORT", "8000"))

# CAN protocol arbitration IDs (Dobiss, reverse-engineered by dries007)
ARBIT_GET_REQUEST = 0x01FCFF01  # GET state request:  [module, relay]
ARBIT_GET_REPLY   = 0x01FDFF01  # GET state reply:    [state]
ARBIT_SET_REPLY   = 0x0002FF01  # SET state reply:    [module, relay, state]


class PendingGetTracker:
    """Bounded, expiring FIFO for address-less Dobiss GET replies."""

    def __init__(self, max_pending=256, ttl_seconds=5.0, clock=time.monotonic):
        if max_pending < 1 or ttl_seconds <= 0:
            raise ValueError("max_pending and ttl_seconds must be positive")
        self._entries = deque()
        self._max_pending = max_pending
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._blocked_until = 0.0

    def _purge(self):
        now = self._clock()
        if now < self._blocked_until:
            self._entries.clear()
            return
        self._blocked_until = 0.0
        cutoff = now - self._ttl_seconds
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()

    def append(self, address):
        self._purge()
        now = self._clock()
        if now < self._blocked_until:
            return False
        if len(self._entries) >= self._max_pending:
            self._entries.clear()
            self._blocked_until = now + self._ttl_seconds
            logger.warning("GET correlation overflow; ignoring replies during cooldown")
            return False
        self._entries.append((now, address))
        return True

    def popleft(self):
        self._purge()
        if not self._entries:
            raise IndexError("pop from an empty PendingGetTracker")
        return self._entries.popleft()[1]

    def __bool__(self):
        self._purge()
        return bool(self._entries)

    def __len__(self):
        self._purge()
        return len(self._entries)

    def __iter__(self):
        self._purge()
        return (address for _, address in tuple(self._entries))


def load_config(path="config.yaml"):
    """Load and validate light configuration from a YAML file."""
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if not isinstance(config, list):
        raise ValueError("Configuration must be a list of lights")
    addresses = set()
    for index, light in enumerate(config):
        if not isinstance(light, dict):
            raise ValueError(f"Light {index} must be a mapping")
        name = light.get("name")
        address = light.get("address")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Light {index} requires a non-empty name")
        if not isinstance(address, str):
            raise ValueError(f"Light {index} has an invalid address")
        try:
            parse_address(address)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Light {index} has an invalid address") from exc
        normalized = address.upper()
        if normalized in addresses:
            raise ValueError(f"Light {index} has duplicate address {normalized}")
        addresses.add(normalized)
        light["address"] = normalized
    return config


def parse_address(address_str):
    """Parse a 4-char hex address string into a (module, relay) tuple.

    Example: '0107' -> (1, 7)
    """
    if not isinstance(address_str, str) or re.fullmatch(r"[0-9A-Fa-f]{4}", address_str) is None:
        raise ValueError("Address must be a 4-character hex string")
    address = int(address_str, 16)
    module = address >> 8
    relay = address & 0xFF
    if not (0 <= module <= 0xFF and 0 <= relay <= 0xFF):
        raise ValueError("Address bytes must be within 0x00..0xFF")
    return module, relay


def build_lookup_tables(config):
    """Pre-compute CAN↔MQTT lookup dicts from the config list.

    Returns:
        can_to_mqtt:  {(module, relay): state_topic_str}
        mqtt_to_can:  {set_topic_str:   (module, relay)}
    """
    can_to_mqtt = {}
    mqtt_to_can = {}
    for light in config:
        addr = light["address"]
        key = parse_address(addr)
        can_to_mqtt[key] = f"dobiss/light/{addr}/state"
        mqtt_to_can[f"dobiss/light/{addr}/state/set"] = key
    return can_to_mqtt, mqtt_to_can


def parse_state(payload):
    """Parse an MQTT payload into a CAN state value.

    Returns 1 for ON/1, 0 for OFF/0, or None for unrecognised payloads.
    """
    if payload in [b"ON", b"1"]:
        return 1
    if payload in [b"OFF", b"0"]:
        return 0
    return None


def parse_can_state(value):
    """Map valid Dobiss state bytes and reject ambiguous values."""
    return {0: "OFF", 1: "ON"}.get(value)


def build_set_message(module, relay, state):
    """Build a CAN message that sets a relay to a given state."""
    arbitration_id = 0x01FC0002 | (module << 8)
    data = [module, relay, state, 0xFF, 0xFF]
    return can.Message(arbitration_id=arbitration_id, data=data, is_extended_id=True)


def handle_mqtt_message(topic, payload, mqtt_to_can, bus):
    """Process an incoming MQTT message and send the corresponding CAN command.

    mqtt_to_can is a {set_topic: (module, relay)} dict built by build_lookup_tables().

    Returns True if a matching light was found, False otherwise.
    """
    key = mqtt_to_can.get(topic)
    if key is None:
        return False
    module, relay = key
    state = parse_state(payload)
    if state is not None:
        message = build_set_message(module, relay, state)
        bus.send(message)
        logger.debug("Sent CAN message: %s", message)
    return True


def _has_min_data_len(message, min_len):
    """Return True when message has at least min_len bytes in its data field."""
    return hasattr(message, "data") and len(message.data) >= min_len


def handle_can_message(message, can_to_mqtt, client, pending_gets=None):
    """Process an incoming CAN message and publish the corresponding MQTT state.

    can_to_mqtt is a {(module, relay): state_topic} dict built by build_lookup_tables().

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
        if pending_gets is not None and _has_min_data_len(message, 2):
            pending_gets.append((message.data[0], message.data[1]))
        else:
            logger.warning("Ignoring malformed GET request frame")
        return

    if arb == ARBIT_SET_REPLY:
        if not _has_min_data_len(message, 3):
            logger.warning("Ignoring malformed SET reply frame")
            return
        topic = can_to_mqtt.get((message.data[0], message.data[1]))
        if topic is not None:
            state_str = parse_can_state(message.data[2])
            if state_str is None:
                logger.warning("Ignoring unknown SET reply state: %r", message.data[2])
                return
            client.publish(topic, state_str, retain=True)
            logger.debug("Published MQTT message: %s", message)
        return

    if arb == ARBIT_GET_REPLY and pending_gets:
        if not _has_min_data_len(message, 1):
            logger.warning("Ignoring malformed GET reply frame")
            return
        req_module, req_relay = pending_gets.popleft()
        topic = can_to_mqtt.get((req_module, req_relay))
        if topic is not None:
            state_str = parse_can_state(message.data[0])
            if state_str is None:
                logger.warning("Ignoring unknown GET reply state: %r", message.data[0])
                return
            client.publish(topic, state_str, retain=True)
            logger.debug("Updated light state based on GET reply: %s", message)


def make_on_connect(config):
    """Return an on_connect callback that subscribes to all configured lights."""
    def on_connect(client, userdata, flags, rc):
        logger.debug("Connected with result code %s", rc)
        for light in config:
            client.subscribe(f"dobiss/light/{light['address']}/state/set")
    return on_connect


def make_on_message(mqtt_to_can, bus):
    """Return an on_message callback that forwards MQTT messages to the CAN bus."""
    def on_message(client, userdata, msg):
        logger.debug("%s %s", msg.topic, msg.payload)
        handle_mqtt_message(msg.topic, msg.payload, mqtt_to_can, bus)
    return on_message


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves the config file."""

    config_path = "config.yaml"
    config_data: list[dict] | None = None

    def do_GET(self):
        if self.path == "/config.yaml":
            self.send_response(200)
            self.send_header("Content-type", "text/yaml")
            self.end_headers()
            if self.config_data is None:
                with open(self.config_path, "r", encoding="utf-8") as file:
                    payload = file.read()
            else:
                payload = yaml.safe_dump(self.config_data, sort_keys=False)
            self.wfile.write(payload.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        logger.debug(format, *args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    config = load_config(CONFIG_PATH)
    can_to_mqtt, mqtt_to_can = build_lookup_tables(config)

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
    client.on_message = make_on_message(mqtt_to_can, bus)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        client.tls_set()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # HTTP server
    # Loopback is the secure default; set DOBISS_HTTP_HOST for trusted LAN access.
    RequestHandler.config_path = CONFIG_PATH
    RequestHandler.config_data = config
    httpd = HTTPServer((HTTP_HOST, HTTP_PORT), RequestHandler)
    threading.Thread(target=httpd.serve_forever).start()

    # CAN bus loop
    pending_gets = PendingGetTracker()
    while True:
        message = bus.recv()
        handle_can_message(message, can_to_mqtt, client, pending_gets)

    client.loop_stop()
