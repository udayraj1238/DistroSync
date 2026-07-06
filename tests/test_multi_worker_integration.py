"""
Integration Tests: Advanced broker flows

Four integration tests for advanced distributed scenarios:
  21. Producer retries succeed after workers clear the queue
  22. DLQ replay re-enqueues task for processing
  23. Multiple queue names are isolated
  24. Metrics API returns accurate live data

All tests spin up a real BrokerServer in-process with real TCP clients.
"""

import asyncio
import json
import sys
import os
import time
import traceback
import http.client

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient


# -- Helpers ---------------------------------------------------------------

PORT_BASE = 15800


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


def http_get(host: str, port: int, path: str) -> dict:
    """Synchronous HTTP GET (runs in executor for async tests)."""
    conn = http.client.HTTPConnection(host, port, timeout=5)
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read().decode("utf-8")
    conn.close()
    return json.loads(body)


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


# -- Test 21: Producer retries succeed after workers clear queue -----------

async def test_backoff_eventually_succeeds():
    """
    Fill queue to trigger rate limiting. Then start workers to drain it.
    Assert that producer (with backoff) eventually gets all tasks accepted
    -- no tasks permanently rejected.

    What this proves: Shows the full lifecycle: rate limit -> backoff ->
    queue drains -> acceptance resumes.
    """
    port = PORT_BASE
    http_port = port + 1000
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=http_port)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Make load shedder aggressive so we trigger rate limiting quickly
        broker.load_shedder.BUCKET_CAPACITY = 5.0
        broker.load_shedder.BASE_FILL_RATE = 10.0
        broker.load_shedder.MIN_FILL_RATE = 2.0

        accepted_count = 0
        rate_limited_count = 0

        # Worker that drains the queue in the background
        async def background_worker():
            await asyncio.sleep(0.3)  # Let some tasks pile up first
            client = RawClient("127.0.0.1", port)
            await client.connect()
            await client.send({
                "command": "REGISTER",
                "worker_id": "drain-worker",
                "queues": ["backoff_q"],
            })
            while True:
                resp = await client.send({
                    "command": "CONSUME",
                    "queue": "backoff_q",
                    "worker_id": "drain-worker",
                })
                if resp["status"] == "empty":
                    await asyncio.sleep(0.05)
                    # Check again -- might be more coming
                    resp2 = await client.send({
                        "command": "CONSUME",
                        "queue": "backoff_q",
                        "worker_id": "drain-worker",
                    })
                    if resp2["status"] == "empty":
                        break
                    resp = resp2
                if resp["status"] == "ok":
                    await client.send({
                        "command": "ACK",
                        "task_id": resp["task"]["task_id"],
                        "worker_id": "drain-worker",
                    })
            await client.close()

        # Producer that submits 30 tasks with retries
        async def producer_with_retries():
            nonlocal accepted_count, rate_limited_count
            client = RawClient("127.0.0.1", port)
            await client.connect()

            for i in range(30):
                for attempt in range(20):  # max retries
                    resp = await client.send({
                        "command": "PRODUCE",
                        "queue": "backoff_q",
                        "task": {"i": i},
                    })
                    if resp["status"] == "ok":
                        accepted_count += 1
                        break
                    elif resp["status"] == "rate_limited":
                        rate_limited_count += 1
                        wait = resp.get("retry_after_seconds", 0.1)
                        await asyncio.sleep(min(wait, 0.5))

            await client.close()

        # Run producer and worker concurrently
        await asyncio.gather(
            producer_with_retries(),
            background_worker(),
        )

        assert accepted_count == 30, (
            f"Expected all 30 tasks accepted, got {accepted_count}. "
            f"Rate limited {rate_limited_count} times."
        )
        print(
            f"    All 30 accepted. Rate-limited {rate_limited_count} times "
            f"before workers drained the queue."
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 22: DLQ replay re-enqueues task for processing ------------------

async def test_dlq_replay():
    """
    Create a task that always fails (via NACK). Let it fail 3 times into
    the DLQ. Use the DLQ replay API to re-enqueue it. Start a worker that
    succeeds this time. Assert the task is processed successfully.

    What this proves: The replay workflow is what real operators do in
    production -- proves you thought about ops.
    """
    port = PORT_BASE + 1
    http_port = port + 1000
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=http_port)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # 1. Produce a task
        client = RawClient("127.0.0.1", port)
        await client.connect()

        # Register as worker first
        await client.send({
            "command": "REGISTER",
            "worker_id": "failing-worker",
            "queues": ["dlq_test_q"],
        })

        # Produce via a separate connection
        producer = RawClient("127.0.0.1", port)
        await producer.connect()
        resp = await producer.send({
            "command": "PRODUCE",
            "queue": "dlq_test_q",
            "task": {"data": "will_fail_then_succeed"},
        })
        task_id = resp["task_id"]
        await producer.close()

        # 2. Fail the task 3 times (dequeue + NACK)
        for attempt in range(3):
            consume_resp = await client.send({
                "command": "CONSUME",
                "queue": "dlq_test_q",
                "worker_id": "failing-worker",
            })
            assert consume_resp["status"] == "ok", (
                f"Expected task on attempt {attempt+1}, got {consume_resp}"
            )
            nack_resp = await client.send({
                "command": "NACK",
                "task_id": task_id,
                "worker_id": "failing-worker",
            })

        await client.close()

        # 3. Verify task is in DLQ
        assert broker.queue_manager.dead_letter_queue.count() > 0, (
            "Task should be in DLQ after 3 NACKs"
        )

        # 4. Replay: move task from DLQ back to queue
        replayed = await broker.queue_manager.dead_letter_queue.retry(
            task_id, broker.queue_manager
        )
        assert replayed is True, "DLQ replay should return True"

        # 5. Now consume and ACK (simulating a "fixed" worker)
        client2 = RawClient("127.0.0.1", port)
        await client2.connect()
        await client2.send({
            "command": "REGISTER",
            "worker_id": "fixed-worker",
            "queues": ["dlq_test_q"],
        })

        consume_resp = await client2.send({
            "command": "CONSUME",
            "queue": "dlq_test_q",
            "worker_id": "fixed-worker",
        })
        assert consume_resp["status"] == "ok", (
            f"Expected replayed task, got {consume_resp}"
        )
        assert consume_resp["task"]["task_id"] == task_id

        ack_resp = await client2.send({
            "command": "ACK",
            "task_id": task_id,
            "worker_id": "fixed-worker",
        })
        assert ack_resp["status"] == "ok"

        await client2.close()

        # 6. Verify: task is done, DLQ is empty, queue is empty
        assert broker.queue_manager.dead_letter_queue.count() == 0, (
            "DLQ should be empty after replay + ACK"
        )
        assert broker.queue_manager.queue_depth("dlq_test_q") == 0
        assert broker.queue_manager.in_flight_count() == 0

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 23: Multiple queue names are isolated ----------------------------

async def test_queue_isolation():
    """
    Produce 10 tasks to "queue-a" and 10 tasks to "queue-b". Start a
    worker subscribed only to "queue-a". Assert it processes exactly 10
    tasks and never touches "queue-b".

    What this proves: Multi-tenancy is a real production concern -- shows
    you thought beyond the happy path.
    """
    port = PORT_BASE + 2
    http_port = port + 1000
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=http_port)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Produce 10 tasks to each queue
        producer = RawClient("127.0.0.1", port)
        await producer.connect()
        for i in range(10):
            await producer.send({
                "command": "PRODUCE",
                "queue": "queue-a",
                "task": {"source": "a", "i": i},
            })
        for i in range(10):
            await producer.send({
                "command": "PRODUCE",
                "queue": "queue-b",
                "task": {"source": "b", "i": i},
            })
        await producer.close()

        # Worker only subscribes to queue-a
        worker = RawClient("127.0.0.1", port)
        await worker.connect()
        await worker.send({
            "command": "REGISTER",
            "worker_id": "worker-a-only",
            "queues": ["queue-a"],
        })

        processed_a = 0
        for _ in range(15):  # Safety limit
            resp = await worker.send({
                "command": "CONSUME",
                "queue": "queue-a",
                "worker_id": "worker-a-only",
            })
            if resp["status"] == "empty":
                break
            if resp["status"] == "ok":
                # Verify we only get queue-a payloads
                assert resp["task"]["payload"]["source"] == "a", (
                    f"Worker-a got task from wrong queue: {resp['task']}"
                )
                await worker.send({
                    "command": "ACK",
                    "task_id": resp["task"]["task_id"],
                    "worker_id": "worker-a-only",
                })
                processed_a += 1

        await worker.close()

        assert processed_a == 10, (
            f"Worker-a should process exactly 10 tasks, got {processed_a}"
        )

        # Queue-b should be completely untouched
        assert broker.queue_manager.queue_depth("queue-b") == 10, (
            f"Queue-b should still have 10 tasks, "
            f"got {broker.queue_manager.queue_depth('queue-b')}"
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 24: Metrics API returns accurate live data -----------------------

async def test_metrics_accuracy():
    """
    Enqueue 15 tasks. Hit GET /metrics via the HTTP API. Assert
    queue_depth + in_flight roughly equals 15.

    What this proves: Proves your observability layer reflects reality,
    not stale state.
    """
    port = PORT_BASE + 3
    http_port = port + 1000
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=http_port)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Enqueue 15 tasks
        producer = RawClient("127.0.0.1", port)
        await producer.connect()
        for i in range(15):
            await producer.send({
                "command": "PRODUCE",
                "queue": "metrics_q",
                "task": {"i": i},
            })
        await producer.close()

        # Let a worker pick up a few tasks (simulating in-flight)
        worker = RawClient("127.0.0.1", port)
        await worker.connect()
        await worker.send({
            "command": "REGISTER",
            "worker_id": "slow-worker",
            "queues": ["metrics_q"],
        })

        # Consume 3 tasks (don't ACK -- they stay in_flight)
        for _ in range(3):
            await worker.send({
                "command": "CONSUME",
                "queue": "metrics_q",
                "worker_id": "slow-worker",
            })

        # Query metrics via HTTP
        loop = asyncio.get_event_loop()
        metrics = await loop.run_in_executor(
            None, http_get, "127.0.0.1", http_port, "/metrics"
        )

        await worker.close()

        # Verify structure
        assert "queues" in metrics, f"Metrics missing 'queues': {metrics.keys()}"
        assert "broker" in metrics, f"Metrics missing 'broker': {metrics.keys()}"

        # Calculate totals
        queue_depth = 0
        if "metrics_q" in metrics["queues"]:
            queue_depth = metrics["queues"]["metrics_q"].get("depth", 0)

        in_flight = metrics["broker"].get("in_flight_tasks", 0)
        total = queue_depth + in_flight

        # 15 total: 12 in queue + 3 in_flight = 15
        assert 13 <= total <= 15, (
            f"Expected depth + in_flight to be ~15, got {total} "
            f"(depth={queue_depth}, in_flight={in_flight})"
        )

        print(
            f"    Metrics: depth={queue_depth}, in_flight={in_flight}, "
            f"total={total}"
        )

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Producer retries succeed after workers clear queue", test_backoff_eventually_succeeds)
    await runner.run("DLQ replay re-enqueues task for processing", test_dlq_replay)
    await runner.run("Multiple queue names are isolated", test_queue_isolation)
    await runner.run("Metrics API returns accurate live data", test_metrics_accuracy)

    print(f"\n{'=' * 60}")
    print(f"  Integration Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
