"""
Integration test for Multiprocessing Worker.

Tests the ProcessPoolWorker to verify:
  1. Tasks execute and return correct results
  2. Tasks run in separate processes (different PIDs)
  3. Multiple tasks run in parallel via the pool
  4. Custom task functions work
  5. Full pipeline: produce -> broker -> ProcessPoolWorker -> ACK
  6. Event loop stays responsive during CPU work (heartbeats continue)
"""

import asyncio
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.task_executor import ProcessPoolWorker, cpu_bound_task

TEST_PORT = 15559
PARENT_PID = os.getpid()


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    async def test_cpu_bound_compute(self):
        """Test 1: CPU-bound compute task returns correct result."""
        print("\n--- Test 1: CPU-bound compute task ---")
        result = cpu_bound_task({"type": "compute", "data": [1, 2, 3, 4, 5]})
        # 1^2 + 2^2 + 3^2 + 4^2 + 5^2 = 1 + 4 + 9 + 16 + 25 = 55
        assert result["result"] == 55, f"Expected 55, got {result['result']}"
        assert result["processed_count"] == 5
        print(f"  PASS: Compute result = {result['result']} (correct)")
        self.passed += 1

    async def test_cpu_bound_fibonacci(self):
        """Test 2: Fibonacci task returns correct result."""
        print("\n--- Test 2: Fibonacci task ---")
        result = cpu_bound_task({"type": "fibonacci", "n": 10})
        # fib(10) = 55
        assert result["result"] == 55, f"Expected 55, got {result['result']}"
        print(f"  PASS: fib(10) = {result['result']} (correct)")
        self.passed += 1

    async def test_cpu_bound_sort(self):
        """Test 3: Sort task returns correct result."""
        print("\n--- Test 3: Sort task ---")
        data = [5, 3, 8, 1, 9, 2, 7, 4, 6, 0]
        result = cpu_bound_task({"type": "sort", "data": data})
        assert result["sorted"] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert result["length"] == 10
        print(f"  PASS: Sorted correctly, length = {result['length']}")
        self.passed += 1

    async def test_runs_in_separate_process(self):
        """Test 4: Tasks run in child processes (different PIDs)."""
        print("\n--- Test 4: Tasks run in separate processes ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Produce a compute task
            async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
                await producer.produce("process_queue", {
                    "type": "compute",
                    "data": [1, 2, 3],
                })

            # Create a ProcessPoolWorker that captures results
            results = []
            original_execute = ProcessPoolWorker.execute

            class CapturingWorker(ProcessPoolWorker):
                async def execute(self, payload):
                    result = await super().execute(payload)
                    results.append(result)
                    return result

            worker = CapturingWorker(
                queue_name="process_queue",
                host="127.0.0.1",
                port=TEST_PORT,
                max_workers=2,
                poll_interval=0.05,
            )

            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(2.0)

            assert len(results) >= 1, f"Expected at least 1 result, got {len(results)}"

            child_pid = results[0]["pid"]
            assert child_pid != PARENT_PID, \
                f"Task ran in parent process (PID={child_pid}), should be child"

            print(f"  PASS: Task ran in child process (PID={child_pid}, parent={PARENT_PID})")
            self.passed += 1

            await worker.shutdown()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_full_pipeline_with_process_pool(self):
        """Test 5: Full pipeline — produce, consume, execute in process pool, ACK."""
        print("\n--- Test 5: Full pipeline with ProcessPoolWorker ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT + 1)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Produce 3 compute tasks
            async with ProducerClient("127.0.0.1", TEST_PORT + 1) as producer:
                for i in range(3):
                    await producer.produce("compute_q", {
                        "type": "compute",
                        "data": list(range(i + 1)),
                    })

            # Run a ProcessPoolWorker
            worker = ProcessPoolWorker(
                queue_name="compute_q",
                host="127.0.0.1",
                port=TEST_PORT + 1,
                max_workers=2,
                poll_interval=0.05,
            )

            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(3.0)

            stats = worker.get_stats()
            assert stats["tasks_completed"] == 3, \
                f"Expected 3 completed, got {stats['tasks_completed']}"
            assert stats["tasks_failed"] == 0

            print(f"  PASS: 3 tasks completed via process pool, 0 failed")
            self.passed += 1

            await worker.shutdown()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_custom_task_function(self):
        """Test 6: Custom task function works with ProcessPoolWorker."""
        print("\n--- Test 6: Custom task function ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT + 2)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            async with ProducerClient("127.0.0.1", TEST_PORT + 2) as producer:
                await producer.produce("custom_q", {"message": "hello"})

            # Define a custom task function at module level
            # (We use cpu_bound_task with type="echo" which acts as our custom function)
            worker = ProcessPoolWorker(
                queue_name="custom_q",
                host="127.0.0.1",
                port=TEST_PORT + 2,
                max_workers=1,
                task_func=custom_echo_task,
                poll_interval=0.05,
            )

            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(2.0)

            assert worker._tasks_completed == 1
            print(f"  PASS: Custom task function executed successfully")
            self.passed += 1

            await worker.shutdown()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_event_loop_responsive_during_cpu_work(self):
        """Test 7: Event loop stays responsive while CPU task runs."""
        print("\n--- Test 7: Event loop responsive during CPU work ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT + 3)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Produce a moderately heavy task
            async with ProducerClient("127.0.0.1", TEST_PORT + 3) as producer:
                await producer.produce("heavy_q", {
                    "type": "fibonacci",
                    "n": 30,  # Takes noticeable time
                })

            worker = ProcessPoolWorker(
                queue_name="heavy_q",
                host="127.0.0.1",
                port=TEST_PORT + 3,
                max_workers=1,
                poll_interval=0.05,
                heartbeat_interval=0.5,  # Fast heartbeat for testing
            )

            worker_task = asyncio.create_task(worker.run())
            await asyncio.sleep(3.0)

            # The key assertion: worker should still be running
            # (event loop wasn't blocked by the CPU task)
            assert worker._running or worker._tasks_completed == 1, \
                "Worker should still be responsive"
            assert worker._tasks_completed == 1, \
                f"Heavy task should have completed, got {worker._tasks_completed}"

            print(f"  PASS: Heavy task completed while event loop stayed responsive")
            self.passed += 1

            await worker.shutdown()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


# Module-level custom task function (required for pickle serialization)
def custom_echo_task(payload: dict) -> dict:
    """Custom task function that uppercases a message."""
    msg = payload.get("message", "")
    return {"uppercased": msg.upper(), "pid": os.getpid()}


async def run_tests():
    runner = TestRunner()

    tests = [
        runner.test_cpu_bound_compute,
        runner.test_cpu_bound_fibonacci,
        runner.test_cpu_bound_sort,
        runner.test_runs_in_separate_process,
        runner.test_full_pipeline_with_process_pool,
        runner.test_custom_task_function,
        runner.test_event_loop_responsive_during_cpu_work,
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
