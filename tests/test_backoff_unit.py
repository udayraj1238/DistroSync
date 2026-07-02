"""
June 19 — Async Unit Tests: NACK retry, DLQ routing, and Worker Registry

Four focused async tests:
  13. NACK re-enqueues task on first failure
  14. Task goes to DLQ after 3 NACKs
  15. Worker registry registers on heartbeat (idempotent)
  16. Worker registry evicts after timeout (mock time)

These tests run on a real asyncio event loop with in-memory
QueueManager and WorkerRegistry — no broker, no TCP.

Usage:
    python -m tests.test_june19_unit
    python tests/test_june19_unit.py
"""

import asyncio
import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.queue_manager import QueueManager
from broker.worker_registry import WorkerRegistry


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


# ── Test 13: NACK re-enqueues task on first failure ──────────────────────

async def test_nack_requeues():
    """
    Enqueue 1 task. Dequeue it (moves to in_flight, attempts becomes 1).
    NACK it. Assert queue depth is back to 1 — the task was re-enqueued.

    What this proves: Retry logic is where most interview candidates go
    wrong — prove it works on the first failure.
    """
    qm = QueueManager(max_retries=3)

    tid = await qm.enqueue("q", {"data": "retry_me"})

    # Dequeue: task moves to in_flight, attempts increments to 1
    task = await qm.dequeue("q", "w1")
    assert task is not None
    assert qm.queue_depth("q") == 0, (
        "Queue should be empty after dequeue"
    )

    # NACK: task should be re-enqueued (attempt 1 < max_retries 3)
    result = await qm.negative_acknowledge(tid)
    assert result is not None
    assert result["action"] == "requeued", (
        f"Expected action='requeued', got '{result['action']}'"
    )

    # Queue depth should be back to 1
    assert qm.queue_depth("q") == 1, (
        f"Expected queue depth 1 after NACK re-enqueue, got {qm.queue_depth('q')}"
    )

    # Dequeue the re-enqueued task and verify attempt count incremented
    task2 = await qm.dequeue("q", "w1")
    assert task2 is not None
    assert task2["task_id"] == tid, (
        "Re-enqueued task should have the same task_id"
    )
    assert task2["attempts"] == 2, (
        f"Expected attempts=2 after dequeue of NACKed task, got {task2['attempts']}"
    )


# ── Test 14: Task goes to DLQ after 3 NACKs ─────────────────────────────

async def test_dlq_after_3_nacks():
    """
    Enqueue 1 task. Dequeue and NACK 3 times (simulating 3 worker failures).
    Assert the task ends up in the dead letter queue and queue depth is 0.

    What this proves: The DLQ boundary is a system design interview question
    — show you built it with the exact max_retries threshold.
    """
    qm = QueueManager(max_retries=3)

    tid = await qm.enqueue("q", {"data": "will_fail"})

    last_result = None
    for attempt in range(3):
        task = await qm.dequeue("q", "w1")
        assert task is not None, (
            f"Dequeue returned None on attempt #{attempt + 1}"
        )
        last_result = await qm.negative_acknowledge(tid)
        assert last_result is not None, (
            f"NACK returned None on attempt #{attempt + 1}"
        )

    # After 3 NACKs, the task should be dead-lettered
    assert last_result["action"] == "dead_lettered", (
        f"Expected 'dead_lettered' after 3 NACKs, got '{last_result['action']}'"
    )

    # Queue should be empty (task went to DLQ, not back to queue)
    assert qm.queue_depth("q") == 0, (
        f"Queue depth should be 0 after DLQ routing, got {qm.queue_depth('q')}"
    )

    # Verify the task is actually in the dead letter queue
    assert qm.dead_letter_queue.count() > 0, (
        "DLQ should have at least 1 task after 3 NACKs"
    )
    dlq_entries = await qm.dead_letter_queue.peek(limit=10)
    dlq_ids = [entry["task_id"] for entry in dlq_entries]
    assert tid in dlq_ids, (
        f"Task {tid[:8]}... should be in DLQ but wasn't found. "
        f"DLQ contains: {[t[:8] for t in dlq_ids]}"
    )


# ── Test 15: Worker registry registers on heartbeat (idempotent) ─────────

async def test_heartbeat_registers_idempotently():
    """
    Register a worker via register(). Call register() again with the same
    worker_id. Assert the worker appears in _workers exactly once (not
    duplicated).

    What this proves: Idempotency — registering twice should produce one
    entry, not two. Critical for reconnection scenarios.
    """
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)

    # First registration
    is_new = await wr.register("w1", queues=["tasks"])
    assert is_new is True, "First registration should return is_new=True"
    assert len(wr._workers) == 1, (
        f"Expected 1 worker after first register, got {len(wr._workers)}"
    )

    # Second registration (same worker_id — simulates reconnection)
    is_new = await wr.register("w1", queues=["tasks"])
    assert is_new is False, "Second registration should return is_new=False"
    assert len(wr._workers) == 1, (
        f"Expected 1 worker after re-register (idempotent), "
        f"got {len(wr._workers)}"
    )

    # Heartbeat should also not create duplicates
    await wr.record_heartbeat("w1")
    assert len(wr._workers) == 1, (
        f"Expected 1 worker after heartbeat, got {len(wr._workers)}"
    )


# ── Test 16: Worker registry evicts after timeout (mock time) ────────────

async def test_eviction_after_timeout():
    """
    Register a worker. Manually set last_heartbeat to 10 seconds ago
    (past the 6-second timeout). Call evict_dead_workers(). Assert the
    worker is gone from _workers.

    What this proves: Use mock time — never make your test suite wait 6
    seconds for a real timeout. Shows you understand test performance.
    """
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)

    # Register a worker
    await wr.register("w1", queues=["q"])
    assert "w1" in wr._workers, "Worker should be registered"

    # Simulate time passing: set last_heartbeat to 10 seconds ago
    # (well past the 6-second HEARTBEAT_TIMEOUT_SECONDS)
    wr._workers["w1"].last_heartbeat = time.time() - 10

    # Run eviction check
    await wr.evict_dead_workers()

    # Worker should be evicted
    assert "w1" not in wr._workers, (
        "Worker 'w1' should have been evicted after 10s without heartbeat"
    )

    # Verify a still-alive worker is NOT evicted
    await wr.register("w2", queues=["q"])
    # w2 just registered so its heartbeat is fresh
    await wr.evict_dead_workers()
    assert "w2" in wr._workers, (
        "Worker 'w2' with fresh heartbeat should NOT be evicted"
    )


# ── Main ─────────────────────────────────────────────────────────────────

async def run_tests():
    runner = TestRunner()

    await runner.run("NACK re-enqueues task on first failure", test_nack_requeues)
    await runner.run("Task goes to DLQ after 3 NACKs", test_dlq_after_3_nacks)
    await runner.run("Worker registry registers idempotently", test_heartbeat_registers_idempotently)
    await runner.run("Worker registry evicts after timeout", test_eviction_after_timeout)

    print(f"\n{'=' * 60}")
    print(f"  June 19 Async Unit Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
