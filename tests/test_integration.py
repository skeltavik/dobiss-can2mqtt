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
from collections import deque
from unittest.mock import MagicMock, call

import can
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from can2mqtt import build_lookup_tables, handle_can_message, handle_mqtt_message
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
CONFIG_CAN_TO_MQTT, CONFIG_MQTT_TO_CAN = build_lookup_tables(CONFIG)


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


@pytest.fixture()
def sim_bus_and_panel():
    """Yield (simulator, app_bus, panel_bus) on the same virtual channel.

    panel_bus simulates a Dobiss wall panel that issues GET requests.
    app_bus  simulates the can2mqtt application receiving all traffic.
    """
    channel = _unique_channel()
    sim = DobissSimulator(channel=channel)
    sim.start()
    app_bus = can.Bus(interface="virtual", channel=channel)
    panel_bus = can.Bus(interface="virtual", channel=channel)
    yield sim, app_bus, panel_bus
    panel_bus.shutdown()
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
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 1

    def test_mqtt_off_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 0, 1)  # pre-set to ON
        handle_mqtt_message("dobiss/light/0100/state/set", b"OFF", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 0) == 0

    def test_mqtt_numeric_1_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0107/state/set", b"1", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 7) == 1

    def test_mqtt_numeric_0_reaches_simulator(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        sim.set_state(1, 7, 1)
        handle_mqtt_message("dobiss/light/0107/state/set", b"0", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert sim.get_state(1, 7) == 0

    def test_second_module_relayed_correctly(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0200/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert sim.get_state(2, 0) == 1

    def test_set_reply_triggers_mqtt_publish(self, sim_and_bus):
        """The CAN SET reply produced by the simulator should cause an MQTT publish."""
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)

        # Receive the SET reply the simulator sends back
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None, "No CAN reply received from simulator"
        assert reply.arbitration_id == ARBIT_SET_REPLY

        handle_can_message(reply, CONFIG_CAN_TO_MQTT, mqtt_client)
        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "ON", retain=True
        )

    def test_off_reply_triggers_mqtt_off(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        handle_mqtt_message("dobiss/light/0100/state/set", b"OFF", CONFIG_MQTT_TO_CAN, app_bus)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        handle_can_message(reply, CONFIG_CAN_TO_MQTT, mqtt_client)

        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "OFF", retain=True
        )

    def test_invalid_mqtt_payload_no_can_message(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"GARBAGE", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        # Simulator should not have received any message
        assert len(sim.received_messages) == 0

    def test_unknown_light_no_can_message(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/9999/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
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

    def test_get_reply_without_pending_gets_does_not_publish(self, sim_and_bus):
        """Without a pending_gets queue, GET replies are silently ignored."""
        sim, app_bus = sim_and_bus
        mqtt_client = MagicMock()

        sim.set_state(1, 0, 1)
        self._send_get(app_bus, module=1, relay=0)

        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        handle_can_message(reply, CONFIG_CAN_TO_MQTT, mqtt_client)  # no pending_gets

        mqtt_client.publish.assert_not_called()

    def test_get_reply_state_reflects_previous_set(self, sim_and_bus):
        """State read back via GET must match what was written via SET."""
        sim, app_bus = sim_and_bus

        # Write state via MQTT/SET
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        recv_one(app_bus, timeout=0.5)  # drain the SET reply

        # Now GET the state
        self._send_get(app_bus, module=1, relay=0)
        reply = recv_one(app_bus, timeout=1.0)
        assert reply is not None
        assert reply.data[0] == 1


# ---------------------------------------------------------------------------
# GET round-trip with pending_gets (fixed behaviour)
# ---------------------------------------------------------------------------

class TestGetRoundTripFixed:
    """Full GET cycle: wall panel sends request → app snoops → simulator replies
    → app updates only the queried light.

    Uses panel_bus to simulate a wall panel sending GET requests so that
    app_bus receives the request as an external message (not its own echo).
    """

    def _panel_get(self, panel_bus, module, relay):
        panel_bus.send(can.Message(
            arbitration_id=ARBIT_GET_REQUEST,
            data=[module, relay],
            is_extended_id=True,
        ))

    def _process_n(self, app_bus, can_to_mqtt, client, pending_gets, n, timeout=1.0):
        for _ in range(n):
            msg = recv_one(app_bus, timeout=timeout)
            if msg:
                handle_can_message(msg, can_to_mqtt, client, pending_gets)

    def test_only_queried_light_is_updated(self, sim_bus_and_panel):
        sim, app_bus, panel_bus = sim_bus_and_panel
        mqtt_client = MagicMock()
        pending_gets = deque()

        sim.set_state(1, 0, 1)  # 0100 = ON
        self._panel_get(panel_bus, module=1, relay=0)

        # app_bus sees: (1) GET request from panel, (2) GET reply from simulator
        self._process_n(app_bus, CONFIG_CAN_TO_MQTT, mqtt_client, pending_gets, n=2)

        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "ON", retain=True
        )

    def test_correct_state_published(self, sim_bus_and_panel):
        sim, app_bus, panel_bus = sim_bus_and_panel
        mqtt_client = MagicMock()
        pending_gets = deque()

        sim.set_state(1, 7, 0)  # 0107 Kitchen Spots = OFF
        self._panel_get(panel_bus, module=1, relay=7)
        self._process_n(app_bus, CONFIG_CAN_TO_MQTT, mqtt_client, pending_gets, n=2)

        mqtt_client.publish.assert_called_once_with(
            "dobiss/light/0107/state", "OFF", retain=True
        )

    def test_consecutive_gets_processed_in_order(self, sim_bus_and_panel):
        sim, app_bus, panel_bus = sim_bus_and_panel
        mqtt_client = MagicMock()
        pending_gets = deque()

        sim.set_state(1, 0, 1)  # 0100 = ON
        sim.set_state(1, 7, 0)  # 0107 = OFF

        self._panel_get(panel_bus, module=1, relay=0)
        self._panel_get(panel_bus, module=1, relay=7)

        # 2 GET requests + 2 GET replies = 4 messages
        self._process_n(app_bus, CONFIG_CAN_TO_MQTT, mqtt_client, pending_gets, n=4)

        calls = mqtt_client.publish.call_args_list
        assert len(calls) == 2
        assert calls[0][0] == ("dobiss/light/0100/state", "ON")
        assert calls[1][0] == ("dobiss/light/0107/state", "OFF")

    def test_pending_gets_empty_after_replies_consumed(self, sim_bus_and_panel):
        sim, app_bus, panel_bus = sim_bus_and_panel
        pending_gets = deque()

        self._panel_get(panel_bus, module=1, relay=0)
        self._process_n(app_bus, CONFIG_CAN_TO_MQTT, MagicMock(), pending_gets, n=2)

        assert len(pending_gets) == 0


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
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        assert len(sim.received_messages) == 1
        assert sim.received_messages[0].data[2] == 1

    def test_simulator_logs_sent_messages(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.15)
        recv_one(app_bus, timeout=0.5)  # drain
        assert len(sim.sent_messages) == 1
        assert sim.sent_messages[0].arbitration_id == ARBIT_SET_REPLY

    def test_independent_relay_states(self, sim_and_bus):
        sim, app_bus = sim_and_bus
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON",  CONFIG_MQTT_TO_CAN, app_bus)
        handle_mqtt_message("dobiss/light/0107/state/set", b"OFF", CONFIG_MQTT_TO_CAN, app_bus)
        handle_mqtt_message("dobiss/light/0200/state/set", b"ON",  CONFIG_MQTT_TO_CAN, app_bus)
        time.sleep(0.2)

        assert sim.get_state(1, 0) == 1
        assert sim.get_state(1, 7) == 0
        assert sim.get_state(2, 0) == 1
