"""Integration tests for can2mqtt using the DobissSimulator virtual controller.

These tests exercise the full MQTT ↔ CAN round-trip without real hardware by
running both the application logic and the virtual Dobiss controller on the
same python-can virtual bus.

Round-trip flow:
  MQTT command
      └─► handle_mqtt_message  ──► CAN SET request ──► DobissSimulator
                                                              │
  MQTT state update ◄── handle_can_message ◄── CAN SET reply ┘
"""

import itertools
import os
import sys
import time
import threading
from unittest.mock import MagicMock, call

import can
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from can2mqtt import handle_can_message, handle_mqtt_message
from tests.dobiss_simulator import (
    ARBIT_GET_REPLY,
    ARBIT_GET_REQUEST,
    ARBIT_SET_REPLY,
    DobissSimulator,
)

# ---------------------------------------------------------------------------
# Sample configuration (mirrors a subset of config.yaml)
# ---------------------------------------------------------------------------

CONFIG = [
    {"name": "Entrance Outdoor Light", "address": "0100"},  # module=1, relay=0
    {"name": "Kitchen Spots",           "address": "0107"},  # module=1, relay=7
    {"name": "Hallway Light",           "address": "0200"},  # module=2, relay=0
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_channel_counter = itertools.count()

def _unique_channel() -> str:
    """Return a unique virtual channel name per test to avoid cross-talk."""
    return f"dobiss_{os.getpid()}_{next(_channel_counter)}"


@pytest.fixture()
def sim_and_bus():
    """Yield (simulator, app_bus) sharing the same virtual CAN channel."""
    channel = _unique_channel()

    sim = DobissSimulator(channel=channel)
    sim.start()

    app_bus = can.Bus(interface="virtual", channel=channel)

    yield sim, app_bus

    app_bus.shutdown()
    sim.stop()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def recv_one(bus: can.Bus, timeout: float = 1.0) -> can.Message | None:
    """Receive a single message, retrying until timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = bus.recv(timeout=0.05)
        if msg is not None:
            return msg
    return None


# ---------------------------------------------------------------------------
# SET command round-trip (MQTT → CAN → CAN reply → MQTT)
# ---------------------------------------------------------------------------

class TestSetRoundTrip:
    def test_mqtt_on_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 1

    def test_mqtt_off_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 1)  # pre-set to ON
        handle_mqtt_message("dobiss/light/0100/state/set", b"OFF", CONFIG, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 0

    def test_mqtt_numeric_1_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0107/state/set", b"1", CONFIG, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 7) == 1

    def test_mqtt_numeric_0_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 7, 1)
        handle_mqtt_message("dobiss/light/0107/state/set", b"0", CONFIG, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 7) == 0

    def test_second_module_relayed_correctly(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0200/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        assert sim.get_state(2, 0) == 1

    def test_set_reply_triggers_mqtt_publish(self, sim_and_bus):
        """The CAN SET reply produced by the simulator should cause an MQTT publish."""
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG, app_bus)

        # Receive the SET reply the simulator sends back
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None, "No CAN reply received from simulator"
        assert reply.arbitration_id == ARBIT_SET_REPLY

        handle_can_message(reply, CONFIG, mqtt_client)
        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "ON", retain=True
        )

    def test_off_reply_triggers_mqtt_off(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        handle_mqtt_message("dobiss/light/0100/state/set", b"OFF", CONFIG, app_bus)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        handle_can_message(reply, CONFIG, mqtt_client)

        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "OFF", retain=True
        )

    def test_invalid_mqtt_payload_no_can_message(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"GARBAGE", CONFIG, app_bus)
        time.sleep(0.15)
        # Simulator should not have received any message
        assert len(sim.received_messages) == 0

    def test_unknown_light_no_can_message(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/9999/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        assert len(sim.received_messages) == 0


# ---------------------------------------------------------------------------
# GET command round-trip (GET request → simulator → GET reply)
# ---------------------------------------------------------------------------

class TestGetRoundTrip:
    def _send_get(self, bus: can.Bus, module: int, relay: int) -> None:
        msg = can.Message(
            arbitration_id=ARBIT_GET_REQUEST,
            data=[module, relay],
            is_extended_id=True,
        )
        bus.send(msg)

    def test_get_reply_contains_correct_on_state(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 1)
        self._send_get(app_bus, module=1, relay=0)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        assert reply.arbitration_id == ARBIT_GET_REPLY
        assert reply.data[0] == 1

    def test_get_reply_contains_correct_off_state(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 0)
        self._send_get(app_bus, module=1, relay=0)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        assert reply.data[0] == 0

    def test_get_reply_default_state_is_off(self, sim_and_bus):
        """A relay that has never been set should default to OFF."""
        sim, app_bus = sim_and_bus
        self._send_get(app_bus, module=5, relay=3)  # never configured
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        assert reply.data[0] == 0

    def test_get_reply_broadcasts_to_all_lights(self, sim_and_bus):
        """Documents the current (buggy) behavior: GET reply updates all lights.

        NOTE: This is a known limitation of the protocol — the GET reply frame
        (0x01FDFF01) carries only a state byte with no module/relay address.
        Without tracking which GET request was issued, the application cannot
        determine *which* light the reply refers to and currently updates every
        configured light with the same state. A future fix would require pairing
        GET requests with their replies via a pending-request queue.
        """
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        sim.set_state(1, 0, 1)
        self._send_get(app_bus, module=1, relay=0)

        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        handle_can_message(reply, CONFIG, mqtt_client)

        # Current behavior: all lights get the same state
        assert mqtt_client.publish.call_count == len(CONFIG)

    def test_get_reply_state_reflects_previous_set(self, sim_and_bus):
        """State read back via GET must match what was written via SET."""
        sim, app_bus = sim_and_bus

        # Write state via MQTT/SET
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        recv_one(app_bus, timeout=0.5)  # drain the SET reply

        # Now GET the state
        self._send_get(app_bus, module=1, relay=0)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        assert reply.data[0] == 1


# ---------------------------------------------------------------------------
# Simulator self-checks
# ---------------------------------------------------------------------------

class TestSimulator:
    def test_toggle_flips_on_to_off(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 1)

        toggle_msg = can.Message(
            arbitration_id=0x01FC0102,  # SET for module=1
            data=[1, 0, 2, 0xFF, 0xFF, 0x64, 0xFF, 0xFF],
            is_extended_id=True,
        )
        app_bus.send(toggle_msg)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 0

    def test_toggle_flips_off_to_on(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 0)

        toggle_msg = can.Message(
            arbitration_id=0x01FC0102,
            data=[1, 0, 2, 0xFF, 0xFF, 0x64, 0xFF, 0xFF],
            is_extended_id=True,
        )
        app_bus.send(toggle_msg)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 1

    def test_simulator_logs_received_messages(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        assert len(sim.received_messages) == 1
        assert sim.received_messages[0].data[2] == 1

    def test_simulator_logs_sent_messages(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG, app_bus)
        time.sleep(0.15)
        recv_one(app_bus, timeout=0.5)  # drain
        assert len(sim.sent_messages) == 1
        assert sim.sent_messages[0].arbitration_id == ARBIT_SET_REPLY

    def test_independent_relay_states(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON",  CONFIG, app_bus)
        handle_mqtt_message("dobiss/light/0107/state/set", b"OFF", CONFIG, app_bus)
        handle_mqtt_message("dobiss/light/0200/state/set", b"ON",  CONFIG, app_bus)
        time.sleep(0.2)

        assert sim.get_state(1, 0) == 1
        assert sim.get_state(1, 7) == 0
        assert sim.get_state(2, 0) == 1
