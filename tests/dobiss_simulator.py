"""Virtual Dobiss CAN controller for integration testing.

Protocol reference (reverse-engineered by dries007):
  https://gist.github.com/dries007/436fcd0549a52f26137bca942fef771a

Frame summary
─────────────────────────────────────────────────────────────
Direction  Arbitration ID      Data bytes
─────────────────────────────────────────────────────────────
GET req    0x01FCFF01          [module, relay]
GET reply  0x01FDFF01          [state]          ← state only, no addr
SET req    0x01FC0002|(m<<8)   [m, r, s, FF, FF, 64, FF, FF]
SET reply  0x0002FF01          [module, relay, state]
─────────────────────────────────────────────────────────────

State values: 0 = OFF, 1 = ON, 2 = TOGGLE (SET req only)
Bitrate: 125 kbit/s, 29-bit extended frames, CAN mask 0x1FFFFFFF
"""

import threading
import can

# Arbitration IDs
ARBIT_GET_REQUEST  = 0x01FCFF01
ARBIT_GET_REPLY    = 0x01FDFF01
ARBIT_SET_REPLY    = 0x0002FF01
ARBIT_SET_REQ_MASK = 0xFFFF00FF   # 0x01FC??02 – module sits in byte 1 (bits 8-15)
ARBIT_SET_REQ_BASE = 0x01FC0002


class DobissSimulator:
    """Simulates a Dobiss relay module on a python-can virtual bus.

    Usage::

        sim = DobissSimulator(channel="test")
        sim.start()
        # ... run test code that talks to the same channel ...
        sim.stop()

    The simulator tracks the state of every (module, relay) pair it has
    ever seen and keeps a log of every message it has received so tests
    can make assertions on interactions.
    """

    def __init__(self, channel: str = "dobiss_test"):
        self.channel = channel
        # (module, relay) -> 0|1
        self._relay_states: dict[tuple[int, int], int] = {}
        # All CAN messages received by the simulator
        self.received_messages: list[can.Message] = []
        # All CAN messages sent by the simulator
        self.sent_messages: list[can.Message] = []

        self._bus = can.Bus(interface="virtual", channel=channel)
        self._running = False
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def set_state(self, module: int, relay: int, state: int) -> None:
        """Pre-seed the state of a relay (useful for test setup)."""
        self._relay_states[(module, relay)] = state

    def get_state(self, module: int, relay: int) -> int:
        """Return the current simulated state of a relay (0 or 1)."""
        return self._relay_states.get((module, relay), 0)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="DobissSimulator")
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._bus.shutdown()

    # ------------------------------------------------------------------
    # Internal message loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        while self._running:
            msg = self._bus.recv(timeout=0.05)
            if msg is None:
                continue
            self.received_messages.append(msg)
            self._dispatch(msg)

    def _dispatch(self, msg: can.Message) -> None:
        if msg.arbitration_id == ARBIT_GET_REQUEST:
            self._handle_get(msg)
        elif self._is_set_request(msg.arbitration_id):
            self._handle_set(msg)

    @staticmethod
    def _is_set_request(arb_id: int) -> bool:
        return (arb_id & ARBIT_SET_REQ_MASK) == ARBIT_SET_REQ_BASE

    def _handle_get(self, msg: can.Message) -> None:
        """Respond to a GET state request with the current relay state."""
        module = msg.data[0]
        relay = msg.data[1]
        state = self.get_state(module, relay)
        reply = can.Message(
            arbitration_id=ARBIT_GET_REPLY,
            data=[state],
            is_extended_id=True,
        )
        self._send(reply)

    def _handle_set(self, msg: can.Message) -> None:
        """Apply a SET command and send the confirmation reply."""
        module = msg.data[0]
        relay = msg.data[1]
        state = msg.data[2]

        if state == 2:  # TOGGLE
            state = 1 - self.get_state(module, relay)

        self.set_state(module, relay, state)

        reply = can.Message(
            arbitration_id=ARBIT_SET_REPLY,
            data=[module, relay, state],
            is_extended_id=True,
        )
        self._send(reply)

    def _send(self, msg: can.Message) -> None:
        self.sent_messages.append(msg)
        self._bus.send(msg)
