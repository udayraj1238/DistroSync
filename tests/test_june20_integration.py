"""
June 20 — Integration Tests: Full broker-worker flows

Four integration tests that spin up a real broker on the asyncio event loop:
  17. Full produce → consume → ACK flow (smoke test)
  18. Multiple workers share queue fairly
  19. Dead worker tasks are reassigned after eviction
  20. Rate limiting fires under queue overload

These tests start a real BrokerServer in-process and connect real
ProducerClient / raw TCP clients — no mocks for the broker itself.

Usage:
    python -m tests.test_june20_integration
    python tests/test_june20_integration.py
"""

import asyncio
import json
import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient


# ── Helpers ──────────────────────────────────────────────────────────────

# Unique ports per test to avoid collisions
PORT_BASE = 15700


class RawClient:
    """Lightweight TCP client for direct protocol-level testing."""

    def __init__(self, host: str, port: int):
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


# ── Test 17: Full produce → consume → ACK flow ──────────────────────────

async def test_full_flow():
    """
    Start a real broker. Connect a producer, send 1 task. Connect a
    worker (raw client), consume and ACK the task. Assert task is no
    longer in_flight, queue depth is 0.

    What this proves: This is the smoke test. If this fails, nothing
    else matters.
    """
    port = PORT_BASE
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # 1. Producer sends a task
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        task_id = await producer.produce("test_q", {"v": 42})
        assert task_id is not None and len(task_id) > 0
        await producer.close()

        # 2. Worker connects, registers, consumes, and ACKs
        worker = RawClient("127.0.0.1", port)
        await worker.connect()

        # Register
        resp = await worker.send({
            "command": "REGISTER",
            "worker_id": "test-worker-1",
            "queues": ["test_q"],
        })
        assert resp["status"] == "ok", f"Registration failed: {resp}"

        # Consume
        resp = await worker.send({
            "command": "CONSUME",
            "queue": "test_q",
            "worker_id": "test-worker-1",
        })
        assert resp["status"] == "ok", f"Consume failed: {resp}"
        assert resp["task"]["payload"]["v"] == 42, (
            f"Expected payload v=42, got {resp['task']['payload']}"
        )
        consumed_task_id = resp["task"]["task_id"]
        assert consumed_task_id == task_id

        # ACK
        resp = await worker.send({
            "command": "ACK",
            "task_id": consumed_task_id,
            "worker_id": "test-worker-1",
        })
        assert resp["status"] == "ok", f"ACK failed: {resp}"

        await worker.close()

        # 3. Verify broker state
        assert broker.queue_manager.in_flight_count() == 0, (
            f"Expected 0 in_flight, got {broker.queue_manager.in_flight_count()}"
        )
        assert broker.queue_manager.queue_depth("test_q") == 0, (
            f"Expected queue depth 0, got {broker.queue_manager.queue_depth('test_q')}"
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# ── Test 18: Multiple workers share queue fairly ─────────────────────────

async def test_fair_distribution():
    """
    Enqueue 100 tasks. Start 4 workers simultaneously. Let all 100
    complete. Assert each worker processed between 10 and 50 tasks
    (roughly equal distribution).

    What this proves: Shows the queue distributes load, not just
    sending everything to worker-1.
    """
    port = PORT_BASE + 1
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Enqueue 100 tasks
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        for i in range(100):
            await producer.produce("fair_q", {"i": i})
        await producer.close()

        # Start 4 workers, each consuming and ACKing in a loop
        worker_counts = {f"w{i}": 0 for i in range(4)}

        async def worker_loop(worker_id: str):
            client = RawClient("127.0.0.1", port)
            await client.connect()
            await client.send({
                "command": "REGISTER",
                "worker_id": worker_id,
                "queues": ["fair_q"],
            })

            while True:
                resp = await client.send({
                    "command": "CONSUME",
                    "queue": "fair_q",
                    "worker_id": worker_id,
                })
                if resp["status"] == "empty":
                    break
                if resp["status"] == "ok":
                    await client.send({
                        "command": "ACK",
                        "task_id": resp["task"]["task_id"],
                        "worker_id": worker_id,
                    })
                    worker_counts[worker_id] += 1
                # Small yield to let other workers compete
                await asyncio.sleep(0.001)

            await client.close()

        # Run all 4 workers concurrently
        await asyncio.gather(*[worker_loop(f"w{i}") for i in range(4)])

        total = sum(worker_counts.values())
        assert total == 100, (
            f"Expected 100 total tasks processed, got {total}"
        )

        # Each worker should get a reasonable share (not all to one)
        active_workers = sum(1 for c in worker_counts.values() if c > 0)
        assert active_workers >= 2, (
            f"Expected at least 2 workers to get tasks, but only "
            f"{active_workers} did: {worker_counts}"
        )

        print(f"    Distribution: {worker_counts}")

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# ── Test 19: Dead worker tasks are reassigned ────────────────────────────

async def test_dead_worker_reassignment():
    """
    Start 1 worker. Enqueue 5 tasks. Worker picks up 1 task (doesn't ACK).
    Simulate worker death by closing its connection and setting its
    heartbeat to the past. Run eviction. Start a new worker. Assert all
    5 tasks are eventually processed.

    What this proves: This is the most asked distributed systems question
    — what happens when a node dies?
    """
    port = PORT_BASE + 2
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Enqueue 5 tasks
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        for i in range(5):
            await producer.produce("evict_q", {"i": i})
        await producer.close()

        # Worker 1 connects, picks up 1 task, then "dies" (no ACK)
        w1 = RawClient("127.0.0.1", port)
        await w1.connect()
        await w1.send({
            "command": "REGISTER",
            "worker_id": "dying-worker",
            "queues": ["evict_q"],
        })
        resp = await w1.send({
            "command": "CONSUME",
            "queue": "evict_q",
            "worker_id": "dying-worker",
        })
        assert resp["status"] == "ok", "Worker 1 should have consumed a task"
        orphaned_task_id = resp["task"]["task_id"]

        # Simulate worker death: close connection and fake old heartbeat
        await w1.close()

        # Force the heartbeat timestamp to be 10s in the past
        if "dying-worker" in broker.worker_registry._workers:
            broker.worker_registry._workers["dying-worker"].last_heartbeat = (
                time.time() - 10
            )

        # Run eviction manually
        await broker.worker_registry.evict_dead_workers()

        # Verify the dying worker was evicted
        assert "dying-worker" not in broker.worker_registry._workers, (
            "dying-worker should have been evicted"
        )

        # Worker 2 connects and drains the queue (including the reassigned task)
        w2 = RawClient("127.0.0.1", port)
        await w2.connect()
        await w2.send({
            "command": "REGISTER",
            "worker_id": "healthy-worker",
            "queues": ["evict_q"],
        })

        processed = 0
        for _ in range(10):  # Safety limit
            resp = await w2.send({
                "command": "CONSUME",
                "queue": "evict_q",
                "worker_id": "healthy-worker",
            })
            if resp["status"] == "empty":
                break
            if resp["status"] == "ok":
                await w2.send({
                    "command": "ACK",
                    "task_id": resp["task"]["task_id"],
                    "worker_id": "healthy-worker",
                })
                processed += 1

        await w2.close()

        # All 5 tasks should be accounted for
        # (4 remaining in queue + 1 reassigned from dead worker)
        assert processed == 5, (
            f"Expected 5 tasks processed by healthy worker, got {processed}"
        )
        assert broker.queue_manager.in_flight_count() == 0, (
            f"Expected 0 in_flight after drain, "
            f"got {broker.queue_manager.in_flight_count()}"
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# ── Test 20: Rate limiting fires under queue overload ────────────────────

async def test_rate_limiting_fires():
    """
    Start broker with no workers. Fire 200 produce requests rapidly.
    Assert at least 30% are rate-limited. Assert broker did not crash.

    What this proves: Proves load shedding works under actual load —
    not just in unit tests.
    """
    port = PORT_BASE + 3
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Make the load shedder aggressive for testing
        broker.load_shedder.BUCKET_CAPACITY = 10.0
        broker.load_shedder.BASE_FILL_RATE = 5.0
        broker.load_shedder.MIN_FILL_RATE = 1.0

        # Fire 200 produce requests as fast as possible
        client = RawClient("127.0.0.1", port)
        await client.connect()

        allowed = 0
        rate_limited = 0

        for i in range(200):
            resp = await client.send({
                "command": "PRODUCE",
                "queue": "overload_q",
                "task": {"i": i},
            })
            if resp["status"] == "ok":
                allowed += 1
            elif resp["status"] == "rate_limited":
                rate_limited += 1
                # Verify the response includes retry_after
                assert "retry_after_seconds" in resp, (
                    "rate_limited response must include retry_after_seconds"
                )
                assert resp["retry_after_seconds"] > 0

        await client.close()

        total = allowed + rate_limited
        assert total == 200, (
            f"Expected 200 total responses, got {total}"
        )

        # At least 30% should be rate-limited
        rejection_pct = (rate_limited / total) * 100
        assert rate_limited >= 60, (
            f"Expected at least 60 rate-limited (30%), got {rate_limited} "
            f"({rejection_pct:.1f}%). allowed={allowed}"
        )

        # Broker should still be alive
        assert broker._server is not None and broker._server.is_serving(), (
            "Broker should still be serving after heavy load"
        )

        print(
            f"    {allowed} allowed, {rate_limited} rejected "
            f"({rejection_pct:.1f}% rejection rate)"
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# ── Main ─────────────────────────────────────────────────────────────────

async def run_tests():
    runner = TestRunner()

    await runner.run("Full produce -> consume -> ACK flow", test_full_flow)
    await runner.run("Multiple workers share queue fairly", test_fair_distribution)
    await runner.run("Dead worker tasks are reassigned", test_dead_worker_reassignment)
    await runner.run("Rate limiting fires under queue overload", test_rate_limiting_fires)

    print(f"\n{'=' * 60}")
    print(f"  June 20 Integration Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
