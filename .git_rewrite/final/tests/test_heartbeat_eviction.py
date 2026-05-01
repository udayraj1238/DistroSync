"""
Integration test for Worker Registry Heartbeat Eviction (Week 2, Day 1-2).

Tests the heartbeat/eviction system:
  1. Workers that send heartbeats stay alive
  2. Workers that stop sending heartbeats get evicted after 6 seconds
  3. Evicted workers have their in-flight tasks reassigned to the queue
  4. Healthy workers can pick up tasks reassigned from dead workers
  5. Registry stats track evictions correctly

These tests use shorter timeout values to keep test runtime reasonable.

Usage:
    python -m tests.test_heartbeat_eviction
"""

import asyncio
import json
import time
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from broker.worker_registry import WorkerRegistry, WorkerInfo, HEARTBEAT_TIMEOUT_SECONDS
from broker.queue_manager import QueueManager
from producer.client import ProducerClient

TEST_PORT = 15558


# ── Helper: raw TCP client ────────────────────────────────────────

class RawClient:
    """Lightweight TCP client for precise control in eviction tests."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )

    async def send(self, message: dict) -> dict:
        encoded = json.dumps(message).encode("utf-8")
        self._writer.write(len(encoded).to_bytes(4, byteorder="big") + encoded)
        await self._writer.drain()
        raw_len = await self._reader.readexactly(4)
        msg_len = int.from_bytes(raw_len, byteorder="big")
        raw_resp = await self._reader.readexactly(msg_len)
        return json.loads(raw_resp.decode("utf-8"))

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    async def test_heartbeat_keeps_worker_alive(self):
        """Test 1: Workers that send heartbeats are not evicted."""
        print("\n--- Test 1: Heartbeat keeps worker alive ---")

        qm = QueueManager()
        registry = WorkerRegistry(queue_manager=qm)

        # Register a worker
        await registry.register("worker_1", queues=["test_q"])
        assert registry.get_worker_count() == 1

        # Simulate heartbeats for 3 seconds (well past 2s check interval)
        for _ in range(6):
            await asyncio.sleep(0.5)
            await registry.record_heartbeat("worker_1")

        # Worker should still be alive
        await registry.evict_dead_workers()
        assert registry.get_worker_count() == 1
        print(f"  PASS: Worker survived with heartbeats")
        self.passed += 1

    async def test_no_heartbeat_causes_eviction(self):
        """Test 2: Workers that stop sending heartbeats get evicted."""
        print("\n--- Test 2: Missing heartbeats -> eviction ---")

        qm = QueueManager()
        registry = WorkerRegistry(queue_manager=qm)

        # Register a worker
        await registry.register("doomed_worker", queues=["test_q"])
        assert registry.get_worker_count() == 1

        # Don't send any heartbeats, wait for timeout
        await asyncio.sleep(HEARTBEAT_TIMEOUT_SECONDS + 1)

        # Trigger eviction check
        await registry.evict_dead_workers()

        # Worker should be gone
        assert registry.get_worker_count() == 0
        assert registry.get_worker("doomed_worker") is None

        print(f"  PASS: Worker evicted after {HEARTBEAT_TIMEOUT_SECONDS}s silence")
        self.passed += 1

    async def test_eviction_reassigns_in_flight_tasks(self):
        """Test 3: Evicted worker's in-flight tasks are requeued."""
        print("\n--- Test 3: Eviction reassigns in-flight tasks ---")

        qm = QueueManager()
        registry = WorkerRegistry(queue_manager=qm)

        # Produce 2 tasks
        task_id_1 = await qm.enqueue("requeue_q", {"item": 1})
        task_id_2 = await qm.enqueue("requeue_q", {"item": 2})

        # Worker consumes both tasks (they become in-flight)
        await registry.register("crash_worker", queues=["requeue_q"])
        task_data_1 = await qm.dequeue("requeue_q", "crash_worker")
        task_data_2 = await qm.dequeue("requeue_q", "crash_worker")

        # Track them in the registry
        await registry.assign_task("crash_worker", task_id_1)
        await registry.assign_task("crash_worker", task_id_2)

        # Verify: queue is empty, 2 tasks in-flight
        assert qm.queue_depth("requeue_q") == 0
        assert qm.in_flight_count() == 2

        # Simulate worker death: wait for timeout
        await asyncio.sleep(HEARTBEAT_TIMEOUT_SECONDS + 1)
        await registry.evict_dead_workers()

        # Worker should be evicted
        assert registry.get_worker_count() == 0

        # Both tasks should be back in the queue
        assert qm.queue_depth("requeue_q") == 2
        assert qm.in_flight_count() == 0

        # Verify stats
        stats = registry.get_stats()
        assert stats["total_evictions"] == 1
        assert stats["total_tasks_reassigned"] == 2

        print(f"  PASS: 2 tasks reassigned from dead worker back to queue")
        self.passed += 1

    async def test_healthy_worker_picks_up_reassigned_task(self):
        """Test 4: Full pipeline — dead worker's task picked up by healthy worker."""
        print("\n--- Test 4: Dead worker task -> reassigned -> healthy worker ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Step 1: Produce a task
            async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
                task_id = await producer.produce("recovery_q", {"critical": True})

            # Step 2: A "doomed" worker consumes the task
            doomed = RawClient("127.0.0.1", TEST_PORT)
            await doomed.connect()
            await doomed.send({
                "command": "REGISTER",
                "worker_id": "doomed_worker",
                "queues": ["recovery_q"],
            })
            consume_resp = await doomed.send({
                "command": "CONSUME",
                "queue": "recovery_q",
                "worker_id": "doomed_worker",
            })
            assert consume_resp["status"] == "ok"
            consumed_task_id = consume_resp["task"]["task_id"]

            # Step 3: Doomed worker "crashes" (close connection, no ACK)
            await doomed.close()

            # Step 4: Wait for heartbeat timeout + eviction check
            await asyncio.sleep(HEARTBEAT_TIMEOUT_SECONDS + 3)

            # Step 5: Verify the task was reassigned back to the queue
            # A healthy worker should be able to consume it
            healthy = RawClient("127.0.0.1", TEST_PORT)
            await healthy.connect()
            await healthy.send({
                "command": "REGISTER",
                "worker_id": "healthy_worker",
                "queues": ["recovery_q"],
            })
            retry_resp = await healthy.send({
                "command": "CONSUME",
                "queue": "recovery_q",
                "worker_id": "healthy_worker",
            })

            assert retry_resp["status"] == "ok", \
                f"Expected task to be available, got: {retry_resp}"
            assert retry_resp["task"]["task_id"] == consumed_task_id
            assert retry_resp["task"]["attempts"] == 2  # Was attempt 1, now attempt 2

            # Step 6: Healthy worker ACKs it
            ack_resp = await healthy.send({
                "command": "ACK",
                "task_id": consumed_task_id,
            })
            assert ack_resp["status"] == "ok"
            await healthy.close()

            print(f"  PASS: Dead worker's task reassigned and completed by healthy worker")
            self.passed += 1
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_multiple_workers_selective_eviction(self):
        """Test 5: Only dead workers are evicted; alive ones remain."""
        print("\n--- Test 5: Selective eviction (dead vs alive) ---")

        qm = QueueManager()
        registry = WorkerRegistry(queue_manager=qm)

        # Register 3 workers
        await registry.register("alive_1", queues=["q"])
        await registry.register("alive_2", queues=["q"])
        await registry.register("dead_1", queues=["q"])

        assert registry.get_worker_count() == 3

        # Keep alive_1 and alive_2 heartbeating, let dead_1 expire
        for _ in range(7):
            await asyncio.sleep(1)
            await registry.record_heartbeat("alive_1")
            await registry.record_heartbeat("alive_2")
            # dead_1 gets no heartbeat

        await registry.evict_dead_workers()

        assert registry.get_worker_count() == 2
        assert registry.get_worker("alive_1") is not None
        assert registry.get_worker("alive_2") is not None
        assert registry.get_worker("dead_1") is None

        print(f"  PASS: Only dead_1 evicted; alive_1 and alive_2 survived")
        self.passed += 1

    async def test_registry_stats(self):
        """Test 6: Registry stats correctly track evictions."""
        print("\n--- Test 6: Registry stats tracking ---")

        qm = QueueManager()
        registry = WorkerRegistry(queue_manager=qm)

        # Register and assign tasks to a worker
        await registry.register("stats_worker", queues=["q"])
        task_id = await qm.enqueue("q", {"data": "stats_test"})
        await qm.dequeue("q", "stats_worker")
        await registry.assign_task("stats_worker", task_id)

        stats = registry.get_stats()
        assert stats["total_workers"] == 1
        assert stats["active_workers"] == 1
        assert stats["total_evictions"] == 0
        assert stats["workers"]["stats_worker"]["in_flight_tasks"] == 1

        # Evict the worker
        await asyncio.sleep(HEARTBEAT_TIMEOUT_SECONDS + 1)
        await registry.evict_dead_workers()

        stats = registry.get_stats()
        assert stats["total_workers"] == 0
        assert stats["total_evictions"] == 1
        assert stats["total_tasks_reassigned"] == 1

        print(f"  PASS: Stats correctly track eviction and task reassignment")
        self.passed += 1


async def run_tests():
    runner = TestRunner()

    tests = [
        runner.test_heartbeat_keeps_worker_alive,
        runner.test_no_heartbeat_causes_eviction,
        runner.test_eviction_reassigns_in_flight_tasks,
        runner.test_healthy_worker_picks_up_reassigned_task,
        runner.test_multiple_workers_selective_eviction,
        runner.test_registry_stats,
    ]

    for test_func in tests:
        try:
            await test_func()
        except AssertionError as e:
            print(f"  FAIL: ASSERTION: {e}")
            runner.failed += 1
        except Exception as e:
            print(f"  FAIL: ERROR: {e}")
            traceback.print_exc()
            runner.failed += 1

    print(f"\n{'='*50}")
    print(f"  Results: {runner.passed} passed, {runner.failed} failed")
    print(f"{'='*50}")
    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
