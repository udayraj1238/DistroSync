"""
Integration test for the Worker Base (Day 4-5).

This is the first full end-to-end pipeline test:
    Producer -> Broker -> Worker -> ACK/NACK

Tests:
  1. Worker connects and registers successfully
  2. Full pipeline: produce a task, worker consumes and ACKs it
  3. Worker NACKs failing tasks
  4. Worker processes multiple tasks in FIFO order
  5. Heartbeat is sent during idle periods
  6. Custom worker subclass works correctly

Usage:
    python -m tests.test_worker_base
"""

import asyncio
import json
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.base_worker import BaseWorker

TEST_PORT = 15557


# ── Test Worker Implementations ────────────────────────────────────

class EchoWorker(BaseWorker):
    """Simple worker that returns the payload as the result."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received_payloads = []

    async def execute(self, payload: dict) -> dict:
        self.received_payloads.append(payload)
        return {"echoed": payload}


class FailingWorker(BaseWorker):
    """Worker that always raises an exception."""
    async def execute(self, payload: dict) -> dict:
        raise ValueError(f"Intentional failure for task: {payload}")


class ConditionalWorker(BaseWorker):
    """Worker that fails on odd items and succeeds on even items."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = []

    async def execute(self, payload: dict) -> dict:
        item = payload.get("item", 0)
        if item % 2 != 0:
            raise ValueError(f"Item {item} is odd, failing intentionally")
        self.results.append(item)
        return {"processed": item}


# ── Helper: send raw message to broker ─────────────────────────────

async def raw_send(host, port, message):
    """Send a raw message to broker and return response (for verification)."""
    reader, writer = await asyncio.open_connection(host, port)
    encoded = json.dumps(message).encode("utf-8")
    writer.write(len(encoded).to_bytes(4, byteorder="big") + encoded)
    await writer.drain()
    raw_len = await reader.readexactly(4)
    msg_len = int.from_bytes(raw_len, byteorder="big")
    raw_resp = await reader.readexactly(msg_len)
    writer.close()
    await writer.wait_closed()
    return json.loads(raw_resp.decode("utf-8"))


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    async def test_worker_connect_register(self):
        """Test 1: Worker connects and registers with the broker."""
        print("\n--- Test 1: Worker connect and register ---")
        worker = EchoWorker(
            queue_name="test_queue",
            host="127.0.0.1",
            port=TEST_PORT,
        )
        await worker.connect()
        assert worker._connected, "Worker should be connected"
        assert worker.worker_id is not None

        # Verify registration by checking broker's worker count
        # (We can't directly query, but the connect didn't fail)
        await worker._close()
        print(f"  PASS: Worker {worker.worker_id[:8]}... connected and registered")
        self.passed += 1

    async def test_full_pipeline(self):
        """Test 2: Full pipeline — produce, consume, execute, ACK."""
        print("\n--- Test 2: Full produce -> consume -> execute -> ACK pipeline ---")

        # Step 1: Produce a task
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            task_id = await producer.produce("pipeline_queue", {
                "action": "greet",
                "name": "DistroSync",
            })

        # Step 2: Create a worker and run it for just enough time
        worker = EchoWorker(
            queue_name="pipeline_queue",
            host="127.0.0.1",
            port=TEST_PORT,
            poll_interval=0.05,  # Fast polling for test
        )

        # Run worker in background, let it process one task
        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.5)  # Give worker time to consume and execute

        # Step 3: Verify the worker processed the task
        assert len(worker.received_payloads) == 1, \
            f"Expected 1 payload, got {len(worker.received_payloads)}"
        assert worker.received_payloads[0]["action"] == "greet"
        assert worker.received_payloads[0]["name"] == "DistroSync"
        assert worker._tasks_completed == 1
        assert worker._tasks_failed == 0

        # Cleanup
        await worker.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        print(f"  PASS: Full pipeline worked -- task produced, consumed, executed, ACKed")
        self.passed += 1

    async def test_worker_nack_on_failure(self):
        """Test 3: Worker sends NACK when execute() raises an exception."""
        print("\n--- Test 3: Worker NACKs on execute failure ---")

        # Produce a task
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            await producer.produce("fail_queue", {"data": "will_fail"})

        # Run the failing worker
        worker = FailingWorker(
            queue_name="fail_queue",
            host="127.0.0.1",
            port=TEST_PORT,
            poll_interval=0.05,
        )

        worker_task = asyncio.create_task(worker.run())
        # The failing worker will keep NACKing and the task will
        # keep getting re-queued. Let it run for a bit.
        await asyncio.sleep(1.0)

        assert worker._tasks_failed >= 1, \
            f"Expected at least 1 failed task, got {worker._tasks_failed}"
        assert worker._tasks_completed == 0

        await worker.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        print(f"  PASS: Worker NACKed {worker._tasks_failed} times (task keeps retrying)")
        self.passed += 1

    async def test_multiple_tasks_fifo(self):
        """Test 4: Worker processes multiple tasks in FIFO order."""
        print("\n--- Test 4: Multiple tasks processed in FIFO order ---")

        # Produce 5 tasks with sequential items
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            for i in range(5):
                await producer.produce("ordered_queue", {"item": i, "order": i})

        worker = EchoWorker(
            queue_name="ordered_queue",
            host="127.0.0.1",
            port=TEST_PORT,
            poll_interval=0.05,
        )

        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(1.0)

        assert len(worker.received_payloads) == 5, \
            f"Expected 5 payloads, got {len(worker.received_payloads)}"

        # Verify FIFO order
        orders = [p["order"] for p in worker.received_payloads]
        assert orders == [0, 1, 2, 3, 4], f"Expected [0,1,2,3,4], got {orders}"

        await worker.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        print(f"  PASS: 5 tasks processed in correct FIFO order")
        self.passed += 1

    async def test_heartbeat_during_idle(self):
        """Test 5: Heartbeat continues while worker is idle (empty queue)."""
        print("\n--- Test 5: Heartbeat runs during idle ---")

        worker = EchoWorker(
            queue_name="empty_queue_for_heartbeat",
            host="127.0.0.1",
            port=TEST_PORT,
            poll_interval=0.1,
            heartbeat_interval=0.5,  # Fast heartbeat for test
        )

        worker_task = asyncio.create_task(worker.run())

        # Let the worker idle for 2 seconds with fast heartbeats
        # It should send multiple heartbeats without crashing
        await asyncio.sleep(2.0)

        assert worker._running, "Worker should still be running"
        assert worker._tasks_completed == 0, "No tasks should be completed"

        await worker.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        print(f"  PASS: Worker idled with heartbeats for 2s without issues")
        self.passed += 1

    async def test_worker_stats(self):
        """Test 6: Worker stats tracking works correctly."""
        print("\n--- Test 6: Worker stats tracking ---")

        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            for i in range(3):
                await producer.produce("stats_queue", {"item": i})

        worker = EchoWorker(
            queue_name="stats_queue",
            host="127.0.0.1",
            port=TEST_PORT,
            poll_interval=0.05,
        )

        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(1.0)

        stats = worker.get_stats()
        assert stats["tasks_completed"] == 3
        assert stats["tasks_failed"] == 0
        assert stats["queue_name"] == "stats_queue"
        assert stats["running"] is True

        await worker.shutdown()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        print(f"  PASS: Stats correctly track 3 completed, 0 failed")
        self.passed += 1


async def run_tests():
    """Start the broker and run all worker tests."""
    broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
    runner = TestRunner()

    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    tests = [
        runner.test_worker_connect_register,
        runner.test_full_pipeline,
        runner.test_worker_nack_on_failure,
        runner.test_multiple_tasks_fifo,
        runner.test_heartbeat_during_idle,
        runner.test_worker_stats,
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

    await broker.stop()
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    print(f"\n{'='*50}")
    print(f"  Results: {runner.passed} passed, {runner.failed} failed")
    print(f"{'='*50}")
    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
