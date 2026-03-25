"""Tests for can2mqtt.py.

Run with:  pytest tests/
"""
import http.client
import os
import sys
import tempfile
import threading
from collections import deque

import pytest
from unittest.mock import MagicMock, call, patch

# Ensure the project root is importable regardless of how pytest is invoked.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from can2mqtt import (
    ARBIT_GET_REPLY,
    ARBIT_GET_REQUEST,
    ARBIT_SET_REPLY,
    RequestHandler,
    build_lookup_tables,
    build_set_message,
    handle_can_message,
    handle_mqtt_message,
    load_config,
    make_on_connect,
    make_on_message,
    parse_address,
    parse_state,
)
from http.server import HTTPServer

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SAMPLE_CONFIG = [
    {"name": "Entrance Outdoor Light", "address": "0100"},
    {"name": "Kitchen Spots", "address": "0107"},
    {"name": "Hallway Light", "address": "0200"},
]
SAMPLE_CAN_TO_MQTT, SAMPLE_MQTT_TO_CAN = build_lookup_tables(SAMPLE_CONFIG)


# ---------------------------------------------------------------------------
# parse_address
# ---------------------------------------------------------------------------

class TestParseAddress:
    def test_module_and_relay_basic(self):
        module, relay = parse_address("0100")
        assert module == 1
        assert relay == 0

    def test_nonzero_relay(self):
        module, relay = parse_address("0107")
        assert module == 1
        assert relay == 7

    def test_second_module(self):
        module, relay = parse_address("0200")
        assert module == 2
        assert relay == 0

    def test_hex_uppercase(self):
        module, relay = parse_address("010A")
        assert module == 1
        assert relay == 10

    def test_hex_lowercase(self):
        module, relay = parse_address("010a")
        assert module == 1
        assert relay == 10

    def test_max_relay_value(self):
        module, relay = parse_address("01FF")
        assert module == 1
        assert relay == 255


# ---------------------------------------------------------------------------
# parse_state
# ---------------------------------------------------------------------------

class TestParseState:
    def test_on_string(self):
        assert parse_state(b"ON") == 1

    def test_on_numeric(self):
        assert parse_state(b"1") == 1

    def test_off_string(self):
        assert parse_state(b"OFF") == 0

    def test_off_numeric(self):
        assert parse_state(b"0") == 0

    def test_invalid_returns_none(self):
        assert parse_state(b"TOGGLE") is None

    def test_empty_returns_none(self):
        assert parse_state(b"") is None

    def test_lowercase_on_is_invalid(self):
        # Payload matching is intentionally case-sensitive.
        assert parse_state(b"on") is None

    def test_lowercase_off_is_invalid(self):
        assert parse_state(b"off") is None

    def test_arbitrary_bytes_returns_none(self):
        assert parse_state(b"\x00\x01") is None


# ---------------------------------------------------------------------------
# build_set_message
# ---------------------------------------------------------------------------

class TestBuildSetMessage:
    def test_arbitration_id_module1(self):
        msg = build_set_message(module=1, relay=0, state=1)
        assert msg.arbitration_id == 0x01FC0102

    def test_arbitration_id_module2(self):
        msg = build_set_message(module=2, relay=0, state=1)
        assert msg.arbitration_id == 0x01FC0202

    def test_data_bytes_on(self):
        msg = build_set_message(module=1, relay=5, state=1)
        assert list(msg.data) == [1, 5, 1, 0xFF, 0xFF]

    def test_data_bytes_off(self):
        msg = build_set_message(module=1, relay=5, state=0)
        assert list(msg.data) == [1, 5, 0, 0xFF, 0xFF]

    def test_is_extended_id(self):
        msg = build_set_message(module=1, relay=0, state=1)
        assert msg.is_extended_id is True

    def test_module_encoded_in_arbitration_id(self):
        msg_m1 = build_set_message(module=1, relay=0, state=0)
        msg_m2 = build_set_message(module=2, relay=0, state=0)
        # Bit-shift of module must be reflected
        assert msg_m2.arbitration_id - msg_m1.arbitration_id == (1 << 8)


# ---------------------------------------------------------------------------
# build_lookup_tables
# ---------------------------------------------------------------------------

class TestBuildLookupTables:
    def test_can_to_mqtt_keys_are_module_relay_tuples(self):
        can_to_mqtt, _ = build_lookup_tables(SAMPLE_CONFIG)
        assert (1, 0) in can_to_mqtt
        assert (1, 7) in can_to_mqtt
        assert (2, 0) in can_to_mqtt

    def test_can_to_mqtt_values_are_state_topics(self):
        can_to_mqtt, _ = build_lookup_tables(SAMPLE_CONFIG)
        assert can_to_mqtt[(1, 0)] == "dobiss/light/0100/state"
        assert can_to_mqtt[(1, 7)] == "dobiss/light/0107/state"

    def test_mqtt_to_can_keys_are_set_topics(self):
        _, mqtt_to_can = build_lookup_tables(SAMPLE_CONFIG)
        assert "dobiss/light/0100/state/set" in mqtt_to_can
        assert "dobiss/light/0107/state/set" in mqtt_to_can

    def test_mqtt_to_can_values_are_module_relay_tuples(self):
        _, mqtt_to_can = build_lookup_tables(SAMPLE_CONFIG)
        assert mqtt_to_can["dobiss/light/0100/state/set"] == (1, 0)
        assert mqtt_to_can["dobiss/light/0107/state/set"] == (1, 7)

    def test_empty_config_returns_empty_dicts(self):
        can_to_mqtt, mqtt_to_can = build_lookup_tables([])
        assert can_to_mqtt == {}
        assert mqtt_to_can == {}

    def test_table_length_matches_config(self):
        can_to_mqtt, mqtt_to_can = build_lookup_tables(SAMPLE_CONFIG)
        assert len(can_to_mqtt) == len(SAMPLE_CONFIG)
        assert len(mqtt_to_can) == len(SAMPLE_CONFIG)

    def test_roundtrip_consistency(self):
        """Every (module, relay) in can_to_mqtt must be reachable via mqtt_to_can."""
        can_to_mqtt, mqtt_to_can = build_lookup_tables(SAMPLE_CONFIG)
        for set_topic, key in mqtt_to_can.items():
            state_topic = set_topic.replace("/state/set", "/state")
            assert can_to_mqtt[key] == state_topic


# ---------------------------------------------------------------------------
# handle_mqtt_message
# ---------------------------------------------------------------------------

class TestHandleMqttMessage:
    def setup_method(self):
        self.bus = MagicMock()

    def test_sends_can_message_for_on(self):
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_called_once()
        msg = self.bus.send.call_args[0][0]
        assert msg.data[2] == 1

    def test_sends_can_message_for_off(self):
        handle_mqtt_message("dobiss/light/0100/state/set", b"OFF", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_called_once()
        msg = self.bus.send.call_args[0][0]
        assert msg.data[2] == 0

    def test_no_send_for_invalid_state(self):
        handle_mqtt_message("dobiss/light/0100/state/set", b"INVALID", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_not_called()

    def test_no_send_for_unknown_topic(self):
        handle_mqtt_message("dobiss/light/9999/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_not_called()

    def test_correct_module_and_relay_in_data(self):
        # address "0107" → module=1, relay=7
        handle_mqtt_message("dobiss/light/0107/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        msg = self.bus.send.call_args[0][0]
        assert msg.data[0] == 1  # module
        assert msg.data[1] == 7  # relay

    def test_correct_module_and_relay_second_module(self):
        # address "0200" → module=2, relay=0
        handle_mqtt_message("dobiss/light/0200/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        msg = self.bus.send.call_args[0][0]
        assert msg.data[0] == 2
        assert msg.data[1] == 0

    def test_returns_true_for_matching_topic(self):
        result = handle_mqtt_message("dobiss/light/0100/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        assert result is True

    def test_returns_true_even_when_state_invalid(self):
        # Light was found (topic matched) even if state is not sent
        result = handle_mqtt_message("dobiss/light/0100/state/set", b"BAD", SAMPLE_MQTT_TO_CAN, self.bus)
        assert result is True

    def test_returns_false_for_no_match(self):
        result = handle_mqtt_message("dobiss/light/9999/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        assert result is False

    def test_only_sends_once_even_with_multiple_lights(self):
        # Only the first matching light should trigger a send
        handle_mqtt_message("dobiss/light/0100/state/set", b"ON", SAMPLE_MQTT_TO_CAN, self.bus)
        assert self.bus.send.call_count == 1

    def test_numeric_on_payload(self):
        handle_mqtt_message("dobiss/light/0100/state/set", b"1", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_called_once()
        assert self.bus.send.call_args[0][0].data[2] == 1

    def test_numeric_off_payload(self):
        handle_mqtt_message("dobiss/light/0100/state/set", b"0", SAMPLE_MQTT_TO_CAN, self.bus)
        self.bus.send.assert_called_once()
        assert self.bus.send.call_args[0][0].data[2] == 0


# ---------------------------------------------------------------------------
# handle_can_message
# ---------------------------------------------------------------------------

def _mock_can_message(arbitration_id, data):
    msg = MagicMock()
    msg.arbitration_id = arbitration_id
    msg.data = data
    return msg


class TestHandleCanMessageSetReply:
    """Tests for CAN messages with arbitration_id 0x0002FF01 (reply to SET)."""

    def setup_method(self):
        self.client = MagicMock()

    def test_publishes_on(self):
        msg = _mock_can_message(0x0002FF01, [1, 0, 1, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "ON", retain=True
        )

    def test_publishes_off(self):
        msg = _mock_can_message(0x0002FF01, [1, 0, 0, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "OFF", retain=True
        )

    def test_matches_correct_light_by_module_and_relay(self):
        # module=1, relay=7 → address "0107" = Kitchen Spots
        msg = _mock_can_message(0x0002FF01, [1, 7, 1, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0107/state", "ON", retain=True
        )

    def test_no_publish_for_unmatched_module_relay(self):
        # module=9, relay=9 not in config
        msg = _mock_can_message(0x0002FF01, [9, 9, 1, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)
        self.client.publish.assert_not_called()

    def test_data_byte2_nonzero_but_not_1_is_off(self):
        # Only data[2] == 1 means ON; anything else is OFF
        msg = _mock_can_message(0x0002FF01, [1, 0, 2, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "OFF", retain=True
        )


class TestHandleCanMessageGetRequest:
    """Tests for GET request snooping (0x01FCFF01).

    The app cannot know which light a GET reply refers to unless it first
    snoops the corresponding GET request.  handle_can_message records every
    GET request into pending_gets so it can be matched with the reply.
    """

    def test_adds_module_relay_to_pending_gets(self):
        pending = deque()
        msg = _mock_can_message(ARBIT_GET_REQUEST, [1, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, MagicMock(), pending_gets=pending)
        assert list(pending) == [(1, 0)]

    def test_does_not_publish(self):
        client = MagicMock()
        msg = _mock_can_message(ARBIT_GET_REQUEST, [1, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, client, pending_gets=deque())
        client.publish.assert_not_called()

    def test_no_pending_gets_param_does_not_crash(self):
        msg = _mock_can_message(ARBIT_GET_REQUEST, [1, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, MagicMock())  # pending_gets omitted

    def test_fifo_ordering_preserved(self):
        pending = deque()
        handle_can_message(_mock_can_message(ARBIT_GET_REQUEST, [1, 0]), SAMPLE_CAN_TO_MQTT, MagicMock(), pending)
        handle_can_message(_mock_can_message(ARBIT_GET_REQUEST, [1, 7]), SAMPLE_CAN_TO_MQTT, MagicMock(), pending)
        assert list(pending) == [(1, 0), (1, 7)]

    def test_full_get_cycle_publishes_correct_light(self):
        """Snoop request + process reply → only the queried light is updated."""
        pending = deque()
        client = MagicMock()
        # Step 1: snoop the GET request for Kitchen Spots (module=1, relay=7)
        handle_can_message(_mock_can_message(ARBIT_GET_REQUEST, [1, 7]), SAMPLE_CAN_TO_MQTT, client, pending)
        # Step 2: process the GET reply (state=ON)
        handle_can_message(_mock_can_message(ARBIT_GET_REPLY, [1]), SAMPLE_CAN_TO_MQTT, client, pending)
        client.publish.assert_called_once_with("dobiss/light/0107/state", "ON", retain=True)
        assert len(pending) == 0


class TestHandleCanMessageGetReply:
    """Tests for GET reply (0x01FDFF01) with pending-request correlation.

    The GET reply frame carries only a state byte — no module/relay address.
    The handler resolves which light to update by consuming the oldest entry
    from pending_gets (populated when the GET request was snooped).
    """

    def setup_method(self):
        self.client = MagicMock()

    def test_no_pending_gets_param_does_not_publish(self):
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client)  # pending_gets omitted
        self.client.publish.assert_not_called()

    def test_empty_pending_gets_does_not_publish(self):
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=deque())
        self.client.publish.assert_not_called()

    def test_publishes_on_for_pending_light(self):
        pending = deque([(1, 0)])
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "ON", retain=True
        )

    def test_publishes_off_for_pending_light(self):
        pending = deque([(1, 0)])
        msg = _mock_can_message(ARBIT_GET_REPLY, [0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0100/state", "OFF", retain=True
        )

    def test_publishes_only_to_queried_light_not_all(self):
        pending = deque([(1, 7)])  # queried Kitchen Spots, not all lights
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        self.client.publish.assert_called_once_with(
            "dobiss/light/0107/state", "ON", retain=True
        )

    def test_retain_flag_is_set(self):
        pending = deque([(1, 0)])
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        assert self.client.publish.call_args[1]["retain"] is True

    def test_pending_request_consumed_after_reply(self):
        pending = deque([(1, 0)])
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        assert len(pending) == 0

    def test_unconfigured_pending_light_does_not_publish(self):
        pending = deque([(9, 9)])  # not in config
        msg = _mock_can_message(ARBIT_GET_REPLY, [1])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, self.client, pending_gets=pending)
        self.client.publish.assert_not_called()

    def test_fifo_queue_processes_in_order(self):
        """Two consecutive GET replies must update lights in request order."""
        pending = deque([(1, 0), (1, 7)])  # 0100 asked first, then 0107
        client = MagicMock()
        handle_can_message(_mock_can_message(ARBIT_GET_REPLY, [1]), SAMPLE_CAN_TO_MQTT, client, pending)
        handle_can_message(_mock_can_message(ARBIT_GET_REPLY, [0]), SAMPLE_CAN_TO_MQTT, client, pending)
        calls = client.publish.call_args_list
        assert calls[0][0] == ("dobiss/light/0100/state", "ON")
        assert calls[1][0] == ("dobiss/light/0107/state", "OFF")


class TestHandleCanMessageUnknown:
    def test_unrecognised_arbitration_id_does_not_publish(self):
        client = MagicMock()
        msg = _mock_can_message(0xDEADBEEF, [0, 0, 0, 0, 0])
        handle_can_message(msg, SAMPLE_CAN_TO_MQTT, client)
        client.publish.assert_not_called()


# ---------------------------------------------------------------------------
# make_on_connect
# ---------------------------------------------------------------------------

class TestMakeOnConnect:
    def test_subscribes_to_all_lights(self):
        mock_client = MagicMock()
        on_connect = make_on_connect(SAMPLE_CONFIG)
        on_connect(mock_client, None, None, 0)
        expected = [
            call(f"dobiss/light/{light['address']}/state/set")
            for light in SAMPLE_CONFIG
        ]
        mock_client.subscribe.assert_has_calls(expected, any_order=False)

    def test_subscribe_count_matches_config(self):
        mock_client = MagicMock()
        on_connect = make_on_connect(SAMPLE_CONFIG)
        on_connect(mock_client, None, None, 0)
        assert mock_client.subscribe.call_count == len(SAMPLE_CONFIG)

    def test_empty_config_no_subscriptions(self):
        mock_client = MagicMock()
        on_connect = make_on_connect([])
        on_connect(mock_client, None, None, 0)
        mock_client.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# make_on_message
# ---------------------------------------------------------------------------

class TestMakeOnMessage:
    def test_delegates_to_bus_send_on_valid_message(self):
        mock_bus = MagicMock()
        on_message = make_on_message(SAMPLE_MQTT_TO_CAN, mock_bus)

        msg = MagicMock()
        msg.topic = "dobiss/light/0100/state/set"
        msg.payload = b"ON"
        on_message(None, None, msg)

        mock_bus.send.assert_called_once()

    def test_no_bus_send_for_invalid_payload(self):
        mock_bus = MagicMock()
        on_message = make_on_message(SAMPLE_MQTT_TO_CAN, mock_bus)

        msg = MagicMock()
        msg.topic = "dobiss/light/0100/state/set"
        msg.payload = b"INVALID"
        on_message(None, None, msg)

        mock_bus.send.assert_not_called()

    def test_no_bus_send_for_unknown_topic(self):
        mock_bus = MagicMock()
        on_message = make_on_message(SAMPLE_MQTT_TO_CAN, mock_bus)

        msg = MagicMock()
        msg.topic = "dobiss/light/9999/state/set"
        msg.payload = b"ON"
        on_message(None, None, msg)

        mock_bus.send.assert_not_called()


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_valid_yaml(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("- name: Test Light\n  address: '0100'\n")
        result = load_config(str(cfg_file))
        assert result == [{"name": "Test Light", "address": "0100"}]

    def test_raises_for_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "nonexistent.yaml"))

    def test_loads_multiple_entries(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            "- name: Light A\n  address: '0100'\n"
            "- name: Light B\n  address: '0101'\n"
        )
        result = load_config(str(cfg_file))
        assert len(result) == 2
        assert result[0]["address"] == "0100"
        assert result[1]["address"] == "0101"


# ---------------------------------------------------------------------------
# RequestHandler (HTTP server)
# ---------------------------------------------------------------------------

class TestRequestHandler:
    """Integration tests for the HTTP server using a real (loopback) socket."""

    @pytest.fixture()
    def server_with_config(self, tmp_path):
        """Start a one-shot HTTP server serving a temporary config file."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("- name: Test\n  address: '0100'\n")

        # Point the handler at the temp config
        RequestHandler.config_path = str(cfg_file)

        httpd = HTTPServer(("127.0.0.1", 0), RequestHandler)
        port = httpd.server_address[1]

        # Serve until the fixture tears down
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        yield port

        httpd.shutdown()
        # Restore default after the test
        RequestHandler.config_path = "config.yaml"

    def test_config_yaml_returns_200(self, server_with_config):
        conn = http.client.HTTPConnection("127.0.0.1", server_with_config, timeout=5)
        conn.request("GET", "/config.yaml")
        response = conn.getresponse()
        assert response.status == 200
        conn.close()

    def test_config_yaml_content_type(self, server_with_config):
        conn = http.client.HTTPConnection("127.0.0.1", server_with_config, timeout=5)
        conn.request("GET", "/config.yaml")
        response = conn.getresponse()
        assert "text/yaml" in response.getheader("Content-type", "")
        conn.close()

    def test_config_yaml_body(self, server_with_config):
        conn = http.client.HTTPConnection("127.0.0.1", server_with_config, timeout=5)
        conn.request("GET", "/config.yaml")
        response = conn.getresponse()
        body = response.read().decode()
        assert "0100" in body
        conn.close()

    def test_unknown_path_returns_404(self, server_with_config):
        conn = http.client.HTTPConnection("127.0.0.1", server_with_config, timeout=5)
        conn.request("GET", "/unknown")
        response = conn.getresponse()
        assert response.status == 404
        conn.close()

    def test_root_path_returns_404(self, server_with_config):
        conn = http.client.HTTPConnection("127.0.0.1", server_with_config, timeout=5)
        conn.request("GET", "/")
        response = conn.getresponse()
        assert response.status == 404
        conn.close()
