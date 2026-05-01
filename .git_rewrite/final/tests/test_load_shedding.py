"""
Integration test for Adaptive Load Shedding (Week 3, Day 1-3).

Tests the adaptive token-bucket rate limiter:
  1. TokenBucket basics: consume, refill, empty rejection
  2. Under-threshold: no throttling when queue is shallow
  3. Over-threshold: throttling kicks in with deep queue
  4. Adaptive rate adjustment based on queue depth
  5. Adaptive rate adjustment based on worker latency
  6. Combined depth + latency pressure
  7. Full pipeline: producer gets rate_limited and retries
  8. Per-queue isolation: overloaded queue doesn't affect others
  9. Load shedder stats tracking

Usage:
    python -m tests.test_load_shedding
"""

import asyncio
import json
import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from broker.queue_manager import QueueManager
from broker.worker_registry import WorkerRegistry
from broker.load_shedder import AdaptiveLoadShedder, TokenBucket
from producer.client import ProducerClient

TEST_PORT = 15563


class RawClient:
    """Lightweight TCP client for load shedding tests."""

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

    async def test_token_bucket_basics(self):
        """Test 1: TokenBucket consume/refill/reject."""
        print("\n--- Test 1: TokenBucket basics ---")

        bucket = TokenBucket(capacity=5.0, nominal_fill_rate=10.0)

        # Bucket starts full
        assert bucket.tokens == 5.0

        # Consume 5 tokens
        for i in range(5):
            assert bucket.consume(), f"Should consume token #{i+1}"

        # Bucket is now empty
        assert not bucket.consume(), "Should be rejected when empty"

        # Wait and refill
        await asyncio.sleep(0.5)
        bucket.refill(10.0)  # 10 tokens/sec * 0.5s = 5 tokens
        assert bucket.tokens >= 4.0, f"Expected ~5 tokens after refill, got {bucket.tokens}"

        print(f"  PASS: Consume, reject, and refill all work correctly")
        self.passed += 1

    async def test_no_throttle_under_threshold(self):
        """Test 2: No throttling when queue depth is below threshold."""
        print("\n--- Test 2: No throttle under threshold ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)

        # Enqueue a few tasks (well under threshold of 50)
        for i in range(5):
            await qm.enqueue("shallow_q", {"item": i})

        # Should all be allowed
        allowed_count = 0
        for _ in range(10):
            allowed, retry_after = await shedder.check_and_consume("shallow_q")
            if allowed:
                allowed_count += 1

        assert allowed_count == 10, f"Expected all 10 allowed, got {allowed_count}"
        print(f"  PASS: All 10 requests allowed with shallow queue")
        self.passed += 1

    async def test_throttle_deep_queue(self):
        """Test 3: Throttling kicks in with deep queue."""
        print("\n--- Test 3: Throttle with deep queue ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)

        # Set a small bucket capacity for testing
        shedder.BUCKET_CAPACITY = 10.0

        # Enqueue many tasks to exceed the threshold
        for i in range(200):
            await qm.enqueue("deep_q", {"item": i})

        depth = qm.queue_depth("deep_q")
        assert depth == 200, f"Expected depth 200, got {depth}"

        # Consume all tokens in the bucket rapidly
        allowed = 0
        rejected = 0
        for _ in range(20):
            ok, retry = await shedder.check_and_consume("deep_q")
            if ok:
                allowed += 1
            else:
                rejected += 1

        # With capacity=10, first ~10 should be allowed, rest rejected
        assert rejected > 0, f"Expected some rejections, got 0 (allowed={allowed})"
        assert allowed <= 11, f"Expected at most ~10 allowed, got {allowed}"

        print(f"  PASS: {rejected} rejected out of 20 with deep queue (depth={depth})")
        self.passed += 1

    async def test_adjusted_rate_depth(self):
        """Test 4: Adjusted rate decreases as queue depth increases."""
        print("\n--- Test 4: Adjusted rate vs queue depth ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)

        # Rate at 0 depth: should be base rate
        rate_0 = shedder._compute_adjusted_rate("test_q")
        assert rate_0 == shedder.BASE_FILL_RATE, \
            f"At depth 0, rate should be {shedder.BASE_FILL_RATE}, got {rate_0}"

        # Enqueue 100 tasks (2x threshold)
        for i in range(100):
            await qm.enqueue("test_q", {"item": i})

        rate_100 = shedder._compute_adjusted_rate("test_q")
        assert rate_100 < rate_0, \
            f"Rate at depth 100 ({rate_100}) should be less than at depth 0 ({rate_0})"

        # Enqueue 400 more (total 500 = 10x threshold)
        for i in range(400):
            await qm.enqueue("test_q", {"item": i})

        rate_500 = shedder._compute_adjusted_rate("test_q")
        assert rate_500 < rate_100, \
            f"Rate at depth 500 ({rate_500}) should be less than at depth 100 ({rate_100})"

        print(
            f"  PASS: Rate decreased: {rate_0} -> {rate_100:.1f} -> {rate_500:.1f} "
            f"as depth grew from 0 -> 100 -> 500"
        )
        self.passed += 1

    async def test_adjusted_rate_latency(self):
        """Test 5: Adjusted rate decreases as worker latency increases."""
        print("\n--- Test 5: Adjusted rate vs worker latency ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)

        # Rate with no latency data: should be base rate
        rate_no_data = shedder._compute_adjusted_rate("latency_q")
        assert rate_no_data == shedder.BASE_FILL_RATE

        # Simulate low latency (50ms average)
        for _ in range(10):
            wr._record_latency("latency_q", 50.0)

        rate_low = shedder._compute_adjusted_rate("latency_q")
        assert rate_low == shedder.BASE_FILL_RATE, \
            f"Low latency (50ms) should not throttle, got rate {rate_low}"

        # Simulate high latency (500ms average)
        # Clear old samples and add high latency ones
        wr._queue_latencies["latency_q"].clear()
        for _ in range(50):
            wr._record_latency("latency_q", 500.0)

        rate_high = shedder._compute_adjusted_rate("latency_q")
        assert rate_high < shedder.BASE_FILL_RATE, \
            f"High latency (500ms) should throttle, got rate {rate_high}"

        print(
            f"  PASS: Rate at low latency: {rate_low}, "
            f"at high latency (500ms): {rate_high:.1f}"
        )
        self.passed += 1

    async def test_combined_pressure(self):
        """Test 6: Combined depth + latency pressure reduces rate more."""
        print("\n--- Test 6: Combined depth + latency pressure ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)

        # Only depth pressure (200 tasks, no latency data)
        for i in range(200):
            await qm.enqueue("combo_q", {"item": i})
        rate_depth_only = shedder._compute_adjusted_rate("combo_q")

        # Add latency pressure too (500ms average)
        for _ in range(50):
            wr._record_latency("combo_q", 500.0)
        rate_combined = shedder._compute_adjusted_rate("combo_q")

        # Combined should be lower than depth-only
        assert rate_combined < rate_depth_only, \
            f"Combined ({rate_combined:.1f}) should be < depth-only ({rate_depth_only:.1f})"

        # Both should be above MIN_FILL_RATE
        assert rate_combined >= shedder.MIN_FILL_RATE, \
            f"Rate {rate_combined} should be >= MIN_FILL_RATE {shedder.MIN_FILL_RATE}"

        print(
            f"  PASS: Depth-only rate: {rate_depth_only:.1f}, "
            f"combined: {rate_combined:.1f} (multiplicative reduction)"
        )
        self.passed += 1

    async def test_full_pipeline_rate_limiting(self):
        """Test 7: Full pipeline — producer gets rate_limited and retries."""
        print("\n--- Test 7: Full pipeline rate limiting ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Make the load shedder aggressive for testing
            broker.load_shedder.BUCKET_CAPACITY = 3.0
            broker.load_shedder.BASE_FILL_RATE = 2.0
            broker.load_shedder.MIN_FILL_RATE = 1.0

            client = RawClient("127.0.0.1", TEST_PORT)
            await client.connect()

            # Send many produce requests rapidly
            allowed = 0
            rate_limited = 0
            for i in range(10):
                resp = await client.send({
                    "command": "PRODUCE",
                    "queue": "rate_test_q",
                    "task": {"item": i},
                })
                if resp["status"] == "ok":
                    allowed += 1
                elif resp["status"] == "rate_limited":
                    rate_limited += 1
                    # Should include retry_after
                    assert "retry_after_seconds" in resp, \
                        "rate_limited response should include retry_after_seconds"
                    assert resp["retry_after_seconds"] > 0

            assert allowed > 0, "Some requests should be allowed"
            assert rate_limited > 0, "Some requests should be rate limited"

            await client.close()

            print(
                f"  PASS: {allowed} allowed, {rate_limited} rate-limited "
                f"out of 10 rapid requests"
            )
            self.passed += 1
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_per_queue_isolation(self):
        """Test 8: Overloaded queue doesn't affect healthy queue."""
        print("\n--- Test 8: Per-queue isolation ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)
        shedder.BUCKET_CAPACITY = 5.0

        # Overload queue_a
        for i in range(500):
            await qm.enqueue("queue_a", {"item": i})

        # queue_b is empty (healthy)
        rate_a = shedder._compute_adjusted_rate("queue_a")
        rate_b = shedder._compute_adjusted_rate("queue_b")

        assert rate_b == shedder.BASE_FILL_RATE, \
            f"Healthy queue rate should be {shedder.BASE_FILL_RATE}, got {rate_b}"
        assert rate_a < rate_b, \
            f"Overloaded queue rate ({rate_a:.1f}) should be < healthy ({rate_b})"

        # Drain tokens from queue_a's bucket
        for _ in range(10):
            await shedder.check_and_consume("queue_a")

        # queue_b should still have a full bucket
        allowed_b, _ = await shedder.check_and_consume("queue_b")
        assert allowed_b, "Healthy queue should still be allowed"

        print(f"  PASS: Overloaded queue_a (rate={rate_a:.1f}) doesn't affect queue_b")
        self.passed += 1

    async def test_load_shedder_stats(self):
        """Test 9: Load shedder stats tracking."""
        print("\n--- Test 9: Load shedder stats ---")

        qm = QueueManager()
        wr = WorkerRegistry(queue_manager=qm)
        shedder = AdaptiveLoadShedder(qm, wr)
        shedder.BUCKET_CAPACITY = 3.0

        # Use up the bucket
        for _ in range(5):
            await shedder.check_and_consume("stats_q")

        stats = shedder.get_stats()
        assert stats["total_allowed"] == 3, \
            f"Expected 3 allowed, got {stats['total_allowed']}"
        assert stats["total_rejected"] == 2, \
            f"Expected 2 rejected, got {stats['total_rejected']}"
        assert stats["rejection_rate"] > 0, \
            "Rejection rate should be > 0"

        print(
            f"  PASS: Stats correct: {stats['total_allowed']} allowed, "
            f"{stats['total_rejected']} rejected, "
            f"rate={stats['rejection_rate']:.2f}"
        )
        self.passed += 1

    async def test_producer_backoff_on_rate_limit(self):
        """Test 10: ProducerClient retries with backoff on rate_limited."""
        print("\n--- Test 10: Producer backoff on rate_limited ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT + 1)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Very aggressive rate limiting for testing
            broker.load_shedder.BUCKET_CAPACITY = 1.0
            broker.load_shedder.BASE_FILL_RATE = 1.0
            broker.load_shedder.MIN_FILL_RATE = 1.0

            async with ProducerClient("127.0.0.1", TEST_PORT + 1) as producer:
                # First produce: should succeed (bucket has 1 token)
                task_id = await producer.produce(
                    "backoff_q", {"data": "first"}, max_retries=5
                )
                assert task_id is not None

                # Second produce: bucket is empty, producer must retry.
                # With rate=1 token/sec, the producer will need to
                # wait ~1 second for the next token.
                start = time.monotonic()
                task_id_2 = await producer.produce(
                    "backoff_q", {"data": "second"}, max_retries=5
                )
                elapsed = time.monotonic() - start
                assert task_id_2 is not None
                # Should have waited at least a bit for the retry
                assert elapsed > 0.1, \
                    f"Expected producer to back off, but elapsed was only {elapsed:.2f}s"

            print(
                f"  PASS: Producer retried with backoff (waited {elapsed:.2f}s "
                f"for second task)"
            )
            self.passed += 1
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


async def run_tests():
    runner = TestRunner()

    tests = [
        runner.test_token_bucket_basics,
        runner.test_no_throttle_under_threshold,
        runner.test_throttle_deep_queue,
        runner.test_adjusted_rate_depth,
        runner.test_adjusted_rate_latency,
        runner.test_combined_pressure,
        runner.test_full_pipeline_rate_limiting,
        runner.test_per_queue_isolation,
        runner.test_load_shedder_stats,
        runner.test_producer_backoff_on_rate_limit,
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
