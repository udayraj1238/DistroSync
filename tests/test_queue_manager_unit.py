"""
Unit Tests: Backoff, Task model, and Protocol framing

Four focused unit tests:
  5. Exponential backoff grows correctly over successive attempts
  6. Backoff never exceeds the configured cap
  7. Task dataclass initializes with correct defaults
  8. Length-prefix encode/decode roundtrip preserves data exactly

These tests are pure unit tests — no broker, no TCP, no async.
"""

import sys
import os
import json
import random
import struct
from uuid import uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.backoff import ExponentialBackoff
from broker.queue_manager import Task


# ── Protocol helpers (the same 4-byte big-endian length-prefix framing
#    used throughout DistroSync's TCP wire protocol) ──────────────────────

def encode_message(msg: dict) -> bytes:
    """
    Encode a dict into the DistroSync wire format:
    [4-byte big-endian length][UTF-8 JSON payload]
    """
    payload = json.dumps(msg).encode("utf-8")
    length_prefix = struct.pack(">I", len(payload))
    return length_prefix + payload


def decode_message(data: bytes) -> dict:
    """
    Decode a DistroSync wire-format message back to a dict.
    Reads the 4-byte length prefix, then the JSON payload.
    """
    if len(data) < 4:
        raise ValueError("Data too short for length prefix")
    length = struct.unpack(">I", data[:4])[0]
    payload = data[4:4 + length]
    if len(payload) != length:
        raise ValueError(
            f"Expected {length} bytes of payload, got {len(payload)}"
        )
    return json.loads(payload.decode("utf-8"))


# ── Test runner ──────────────────────────────────────────────────────────

class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    def run(self, name, func):
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            self.failed += 1


# ── Test 5: Exponential backoff grows correctly ──────────────────────────

def test_backoff_grows():
    """
    Create ExponentialBackoff(base=1.0, cap=30.0). Call next_wait() 5 times.
    Assert the last wait is greater than the first — the backoff is growing.

    Uses random.seed(42) for reproducibility since full jitter is random.

    What this proves: Validates the backoff grows — crucial for preventing
    retry storms in distributed systems.
    """
    random.seed(42)
    b = ExponentialBackoff(base_delay=1.0, max_delay=30.0)
    waits = [b.next_wait() for _ in range(5)]

    # With seed=42 and full jitter, the exponential ceiling doubles each
    # attempt (1, 2, 4, 8, 16), so even with random jitter the trend
    # should clearly grow.
    assert waits[-1] > waits[0], (
        f"Backoff should grow over attempts. "
        f"First wait: {waits[0]:.3f}s, last wait: {waits[-1]:.3f}s. "
        f"All waits: {[f'{w:.3f}' for w in waits]}"
    )

    # Verify the exponential ceiling is actually growing by checking
    # without jitter
    random.seed(42)
    b_no_jitter = ExponentialBackoff(
        base_delay=1.0, max_delay=30.0, jitter=False
    )
    deterministic_waits = [b_no_jitter.next_wait() for _ in range(5)]
    for i in range(1, len(deterministic_waits)):
        assert deterministic_waits[i] >= deterministic_waits[i - 1], (
            f"Without jitter, wait[{i}]={deterministic_waits[i]:.3f} "
            f"should be >= wait[{i-1}]={deterministic_waits[i-1]:.3f}"
        )


# ── Test 6: Backoff never exceeds cap ────────────────────────────────────

def test_backoff_capped():
    """
    Call next_wait() 10 times on a backoff with cap=5.0. Assert every
    single return value is <= 5.0.

    What this proves: Ensures producers never wait longer than your
    intended maximum — protects user experience and system liveness.
    """
    b = ExponentialBackoff(base_delay=1.0, max_delay=5.0, max_attempts=20)

    waits = [b.next_wait() for _ in range(10)]

    for i, w in enumerate(waits):
        assert w <= 5.0, (
            f"Wait #{i+1} was {w:.3f}s which exceeds cap of 5.0s"
        )


# ── Test 7: Task dataclass initializes correctly ─────────────────────────

def test_task_init():
    """
    Create a Task object. Assert task_id is a non-empty string,
    status is 'pending', attempts is 0, assigned_worker is None.

    What this proves: Baseline sanity check — confirms the data model
    defaults are correct before any state transitions happen.
    """
    task_id = str(uuid4())
    t = Task(task_id=task_id, queue_name="test_queue", payload={"x": 1})

    assert t.task_id == task_id and len(t.task_id) > 0, (
        f"task_id should be a non-empty string, got '{t.task_id}'"
    )
    assert t.status == "pending", (
        f"Default status should be 'pending', got '{t.status}'"
    )
    assert t.attempts == 0, (
        f"Default attempts should be 0, got {t.attempts}"
    )
    assert t.assigned_worker is None, (
        f"Default assigned_worker should be None, got '{t.assigned_worker}'"
    )
    assert t.queue_name == "test_queue", (
        f"queue_name should be 'test_queue', got '{t.queue_name}'"
    )
    assert t.payload == {"x": 1}, (
        f"payload should be {{'x': 1}}, got {t.payload}"
    )
    assert t.created_at > 0, (
        f"created_at should be a positive timestamp, got {t.created_at}"
    )


# ── Test 8: Length-prefix encode/decode roundtrip ─────────────────────────

def test_framing_roundtrip():
    """
    Take a representative message dict, encode it with the 4-byte
    big-endian length-prefix encoder, decode it with the decoder,
    assert the output equals the input exactly.

    What this proves: Critical — if framing is wrong, nothing else works.
    The entire TCP protocol depends on this roundtrip being lossless.
    """
    msg = {"command": "PRODUCE", "queue": "test", "task": {"id": 1}}
    encoded = encode_message(msg)
    decoded = decode_message(encoded)

    assert decoded == msg, (
        f"Roundtrip failed.\n  Input:   {msg}\n  Output:  {decoded}"
    )

    # Verify the wire format structure
    length_bytes = encoded[:4]
    payload_bytes = encoded[4:]
    declared_length = struct.unpack(">I", length_bytes)[0]

    assert declared_length == len(payload_bytes), (
        f"Length prefix ({declared_length}) doesn't match "
        f"actual payload length ({len(payload_bytes)})"
    )

    # Test with edge cases: empty payload, nested structures, unicode
    edge_cases = [
        {"command": "HEARTBEAT"},
        {"command": "PRODUCE", "queue": "q", "task": {"nested": {"deep": [1, 2, 3]}}},
        {"command": "ACK", "task_id": "abc-123-def-456"},
        {"message": "héllo wörld 🚀"},
    ]
    for original in edge_cases:
        roundtripped = decode_message(encode_message(original))
        assert roundtripped == original, (
            f"Edge case roundtrip failed.\n  Input:  {original}\n  Output: {roundtripped}"
        )


def main():
    runner = TestRunner()

    runner.run("Exponential backoff grows correctly", test_backoff_grows)
    runner.run("Backoff never exceeds cap", test_backoff_capped)
    runner.run("Task dataclass initializes correctly", test_task_init)
    runner.run("Length-prefix encode/decode roundtrip", test_framing_roundtrip)

    print(f"\n{'=' * 60}")
    print(f"  Unit Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
