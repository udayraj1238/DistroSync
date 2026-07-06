"""
Async Unit Tests: Event Loop Behavior

Four focused async tests for QueueManager core operations:
   9. Enqueue returns unique IDs (UUID collision check)
  10. Dequeue preserves FIFO order
  11. Dequeue from empty queue returns None
  12. ACK removes task from in_flight

These tests run on a real asyncio event loop but require NO broker,
NO TCP connections, and NO workers — pure in-memory QueueManager.
"""

import asyncio
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.queue_manager import QueueManager


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    async def run(self, name, coro_func):
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            await coro_func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
            self.failed += 1


# ── Test 9: QueueManager enqueue returns unique IDs ──────────────────────

async def test_unique_task_ids():
    """
    Enqueue 100 tasks into the same queue. Collect all returned task_ids.
    Assert len(set(task_ids)) == 100 — every ID is unique.

    What this proves: UUID collision is rare but a job interview classic —
    shows you care about correctness in distributed ID generation.
    """
    qm = QueueManager()
    ids = [await qm.enqueue("q", {"i": i}) for i in range(100)]

    assert len(ids) == 100, (
        f"Expected 100 task IDs returned, got {len(ids)}"
    )
    assert len(set(ids)) == 100, (
        f"Expected 100 unique IDs, got {len(set(ids))} unique out of 100. "
        f"UUID collision detected!"
    )

    # Every ID should be a non-empty string
    for i, tid in enumerate(ids):
        assert isinstance(tid, str) and len(tid) > 0, (
            f"Task ID #{i} is not a valid string: {tid!r}"
        )


# ── Test 10: QueueManager dequeue preserves FIFO order ───────────────────

async def test_fifo_order():
    """
    Enqueue tasks with payloads {"order": 0}, {"order": 1}, ... {"order": 9}.
    Dequeue all 10. Assert the order values come back as 0,1,2,...,9.

    What this proves: First In First Out is the fundamental queue guarantee.
    If this fails, nothing about the system is correct.
    """
    qm = QueueManager()

    # Enqueue in order 0..9
    for i in range(10):
        await qm.enqueue("fifo_q", {"order": i})

    # Dequeue all 10 and verify order
    for expected in range(10):
        task = await qm.dequeue("fifo_q", "worker-1")
        assert task is not None, (
            f"Dequeue returned None at position {expected} — queue ran out early"
        )
        actual = task["payload"]["order"]
        assert actual == expected, (
            f"FIFO violation: expected order={expected}, got order={actual}"
        )


# ── Test 11: Dequeue from empty queue returns None ───────────────────────

async def test_dequeue_empty():
    """
    Call dequeue on a queue that has never had any tasks. Assert the return
    value is None.

    What this proves: Edge case — a worker polling an empty queue must not
    crash. This is what every real worker does on startup before any
    producer has submitted work.
    """
    qm = QueueManager()

    result = await qm.dequeue("empty_queue", "w1")

    assert result is None, (
        f"Expected None from empty queue, got {result!r}"
    )

    # Also test: dequeue from a queue that existed but is now drained
    await qm.enqueue("drain_q", {"data": "only_one"})
    first = await qm.dequeue("drain_q", "w1")
    assert first is not None, "First dequeue should return the task"

    second = await qm.dequeue("drain_q", "w1")
    assert second is None, (
        f"Second dequeue from drained queue should be None, got {second!r}"
    )


# ── Test 12: ACK removes task from in_flight ─────────────────────────────

async def test_ack_removes_from_inflight():
    """
    Enqueue 1 task. Dequeue it (moves to in_flight). Assert it appears
    in _in_flight. Call acknowledge(task_id). Assert it no longer appears
    in _in_flight.

    What this proves: The ACK lifecycle is the most interview-asked part
    of any queue system. A task must be tracked while in-flight and
    cleaned up after acknowledgment.
    """
    qm = QueueManager()

    # Enqueue and capture the task_id
    tid = await qm.enqueue("ack_q", {"work": "important"})

    # Before dequeue: task should NOT be in_flight yet
    assert tid not in qm._in_flight, (
        f"Task {tid[:8]}... should not be in_flight before dequeue"
    )

    # Dequeue: task moves to in_flight
    task = await qm.dequeue("ack_q", "w1")
    assert task is not None
    assert tid in qm._in_flight, (
        f"Task {tid[:8]}... should be in_flight after dequeue"
    )

    # Verify the in_flight task has the right state
    inflight_task = qm._in_flight[tid]
    assert inflight_task.status == "in_flight", (
        f"In-flight task status should be 'in_flight', got '{inflight_task.status}'"
    )
    assert inflight_task.assigned_worker == "w1", (
        f"In-flight task should be assigned to 'w1', got '{inflight_task.assigned_worker}'"
    )

    # ACK: task should be removed from in_flight
    ack_result = await qm.acknowledge(tid)
    assert ack_result is True, (
        f"acknowledge() should return True for a valid in-flight task"
    )
    assert tid not in qm._in_flight, (
        f"Task {tid[:8]}... should NOT be in_flight after ACK"
    )


# ── Main ─────────────────────────────────────────────────────────────────

async def run_tests():
    runner = TestRunner()

    await runner.run("QueueManager enqueue returns unique IDs", test_unique_task_ids)
    await runner.run("QueueManager dequeue preserves FIFO order", test_fifo_order)
    await runner.run("Dequeue from empty queue returns None", test_dequeue_empty)
    await runner.run("ACK removes task from in_flight", test_ack_removes_from_inflight)

    print(f"\n{'=' * 60}")
    print(f"  Async Unit Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
