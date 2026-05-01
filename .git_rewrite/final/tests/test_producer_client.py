"""
Integration test for the Producer Client (Day 3-4).

Tests the ProducerClient against a live broker to verify:
  - Connection management (connect, close, context manager)
  - Single task submission
  - Batch task submission
  - Error handling for invalid inputs
  - ExponentialBackoff logic (unit-level)

Usage:
    python -m tests.test_producer_client
"""

import asyncio
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from producer.backoff import ExponentialBackoff

TEST_PORT = 15556


class TestRunner:
    """Runs all producer client tests."""

    def __init__(self):
        self.passed = 0
        self.failed = 0

    async def test_single_produce(self):
        """Test 1: Produce a single task via ProducerClient."""
        print("\n--- Test 1: Single PRODUCE via ProducerClient ---")
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            task_id = await producer.produce("email_queue", {
                "action": "send_email",
                "to": "user@example.com",
            })
            assert task_id is not None
            assert len(task_id) == 36  # UUID4 format
            print(f"  PASS: Task produced with ID: {task_id[:8]}...")
            self.passed += 1

    async def test_batch_produce(self):
        """Test 2: Produce a batch of tasks."""
        print("\n--- Test 2: Batch PRODUCE (5 tasks) ---")
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            payloads = [{"item": i, "action": "process"} for i in range(5)]
            task_ids = await producer.produce_batch("batch_queue", payloads)
            assert len(task_ids) == 5
            assert all(len(tid) == 36 for tid in task_ids)
            print(f"  PASS: Batch produced {len(task_ids)} tasks")
            self.passed += 1

    async def test_context_manager(self):
        """Test 3: Verify async context manager connects and disconnects."""
        print("\n--- Test 3: Context manager lifecycle ---")
        producer = ProducerClient("127.0.0.1", TEST_PORT)
        assert not producer.is_connected, "Should not be connected yet"

        async with producer:
            assert producer.is_connected, "Should be connected inside context"
            await producer.produce("test_queue", {"test": "context_manager"})

        assert not producer.is_connected, "Should be disconnected after context"
        print(f"  PASS: Context manager correctly handles connect/disconnect")
        self.passed += 1

    async def test_manual_connect_close(self):
        """Test 4: Manual connect and close without context manager."""
        print("\n--- Test 4: Manual connect/close ---")
        producer = ProducerClient("127.0.0.1", TEST_PORT)
        await producer.connect()
        assert producer.is_connected

        task_id = await producer.produce("test_queue", {"test": "manual"})
        assert task_id is not None

        await producer.close()
        assert not producer.is_connected
        print(f"  PASS: Manual connect/close works correctly")
        self.passed += 1

    async def test_produce_without_connect(self):
        """Test 5: Producing without connecting should raise an error."""
        print("\n--- Test 5: Produce without connect ---")
        producer = ProducerClient("127.0.0.1", TEST_PORT)
        try:
            await producer.produce("test_queue", {"test": "no_connect"})
            print(f"  FAIL: Should have raised ConnectionError")
            self.failed += 1
        except ConnectionError:
            print(f"  PASS: ConnectionError raised as expected")
            self.passed += 1

    async def test_multiple_queues(self):
        """Test 6: Produce to multiple different queues."""
        print("\n--- Test 6: Multiple queues ---")
        async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
            id1 = await producer.produce("queue_alpha", {"data": "alpha"})
            id2 = await producer.produce("queue_beta", {"data": "beta"})
            id3 = await producer.produce("queue_gamma", {"data": "gamma"})
            assert id1 != id2 != id3
            print(f"  PASS: Produced to 3 different queues successfully")
            self.passed += 1

    async def test_backoff_unit(self):
        """Test 7: ExponentialBackoff logic (unit test, no broker needed)."""
        print("\n--- Test 7: ExponentialBackoff logic ---")
        backoff = ExponentialBackoff(
            base_delay=1.0,
            max_delay=10.0,
            multiplier=2.0,
            jitter=False,  # Disable jitter for deterministic testing
            max_attempts=5,
        )

        # Without jitter, delays should be: 1, 2, 4, 8, 10 (capped)
        expected = [1.0, 2.0, 4.0, 8.0, 10.0]
        for i, exp in enumerate(expected):
            wait = backoff.next_wait()
            assert abs(wait - exp) < 0.01, f"Attempt {i}: expected {exp}, got {wait}"

        # Should be exhausted now
        assert backoff.exhausted
        try:
            backoff.next_wait()
            print(f"  FAIL: Should have raised RuntimeError")
            self.failed += 1
            return
        except RuntimeError:
            pass

        # Reset should clear the counter
        backoff.reset()
        assert not backoff.exhausted
        assert backoff.attempt == 0
        wait = backoff.next_wait()
        assert abs(wait - 1.0) < 0.01

        print(f"  PASS: Exponential backoff calculates correctly")
        self.passed += 1

    async def test_backoff_with_retry_after(self):
        """Test 8: Backoff respects server retry_after hint."""
        print("\n--- Test 8: Backoff with retry_after ---")
        backoff = ExponentialBackoff(
            base_delay=1.0,
            multiplier=2.0,
            jitter=False,
            max_attempts=5,
        )

        # First attempt: base=1.0, but server says retry_after=5.0
        # Should use 5.0 (the higher value)
        wait = backoff.next_wait(retry_after=5.0)
        assert abs(wait - 5.0) < 0.01, f"Expected 5.0, got {wait}"

        # Second attempt: exp=2.0, retry_after=1.0
        # Should use 2.0 (exp is higher)
        wait = backoff.next_wait(retry_after=1.0)
        assert abs(wait - 2.0) < 0.01, f"Expected 2.0, got {wait}"

        print(f"  PASS: retry_after hint used as floor correctly")
        self.passed += 1

    async def test_backoff_with_jitter(self):
        """Test 9: Jitter produces varied delays within expected range."""
        print("\n--- Test 9: Backoff with jitter ---")
        delays = []
        for _ in range(100):
            backoff = ExponentialBackoff(
                base_delay=1.0,
                multiplier=2.0,
                jitter=True,
                max_attempts=1,
            )
            delays.append(backoff.next_wait())

        # With jitter and attempt=0: delay should be in [0, 1.0]
        assert all(0 <= d <= 1.0 for d in delays), "Jittered delays out of range"
        # With 100 samples, we should see meaningful spread (not all the same)
        unique_delays = len(set(round(d, 4) for d in delays))
        assert unique_delays > 50, f"Jitter not spreading enough: {unique_delays} unique values"

        print(f"  PASS: Jitter produces good spread ({unique_delays} unique values)")
        self.passed += 1


async def run_tests():
    """Start the broker and run all producer client tests."""
    broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
    runner = TestRunner()

    # Start broker in background
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    tests = [
        runner.test_single_produce,
        runner.test_batch_produce,
        runner.test_context_manager,
        runner.test_manual_connect_close,
        runner.test_produce_without_connect,
        runner.test_multiple_queues,
        runner.test_backoff_unit,
        runner.test_backoff_with_retry_after,
        runner.test_backoff_with_jitter,
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

    # Shutdown
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
