"""
June 23 -- Worker Process Pool Crash Test

Focus: Worker robustness
  36. Worker process pool recovers from task crash

Usage:
    python -m tests.test_june23_worker
    python tests/test_june23_worker.py
"""

import asyncio
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.task_executor import ProcessPoolWorker

PORT_BASE = 16200


def crashy_task_func(payload: dict) -> dict:
    if payload.get("crash"):
        # Simulate a bug in the task handler that crashes the worker process
        # A Division by Zero is a classic unhandled exception
        return {"result": 1 / 0}
    return {"result": payload.get("value", 0)}


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


# -- Test 36: Worker process pool recovers from task crash ----------------

async def test_process_pool_survives_crash():
    """
    Submit a task whose payload causes a Python exception inside the
    process pool (e.g., division by zero). Assert the worker sends NACK,
    the process pool survives, and the next task is processed normally.
    """
    port = PORT_BASE
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Start a process pool worker with a custom function
        worker = ProcessPoolWorker(
            queue_name="crash_q",
            host="127.0.0.1",
            port=port,
            max_workers=2,
            task_func=crashy_task_func,
            poll_interval=0.1
        )
        
        # Override the run method lightly to track results for the test
        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.5)

        # Produce two tasks: one bad, one good
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        await producer.produce("crash_q", {"crash": True})
        await producer.produce("crash_q", {"value": 42})
        await producer.close()
        
        # Wait for both tasks to be processed by the worker
        # The first should crash (and get NACKed by the worker logic)
        # The second should succeed
        
        # We can poll the broker state to know when the queue is drained
        # The first task will be NACKed and put back, but after 3 retries it goes to DLQ.
        # Actually, let's just wait a bit for processing to complete.
        await asyncio.sleep(2.0)
        
        # Check broker state
        in_flight = broker.queue_manager.in_flight_count()
        depth = broker.queue_manager.queue_depth("crash_q")
        
        # The bad task should have failed and retried until it hit the DLQ
        dlq_count = broker.queue_manager.dead_letter_queue.count()
        
        print(f"    DLQ tasks: {dlq_count}, Queue depth: {depth}, In-flight: {in_flight}")
        assert dlq_count == 1, "The crashing task should have been dead-lettered"
        assert depth == 0, "The normal task should have been successfully processed and ACKed"
        assert in_flight == 0, "No tasks should be stuck in-flight"
        
        # We've proven the process pool worker survived the exception in the child process
        # and continued processing subsequent tasks!
        
    finally:
        if 'worker' in locals():
            await worker.shutdown()
        if 'worker_task' in locals():
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Worker process pool recovers from task crash", test_process_pool_survives_crash)

    print(f"\n{'=' * 60}")
    print(f"  June 23 Worker Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
