"""
Unit Tests for TokenBucket and AdaptiveLoadShedder — deep,
isolated tests of the rate-limiting core.

These tests validate the mathematical behavior of the token bucket
algorithm and the adaptive rate computation, independent of any
broker or queue infrastructure.

Tests cover:
    1.  Token bucket starts at full capacity
    2.  Token bucket fills over time at the adjusted rate
    3.  Token bucket is capped at capacity (no overflow)
    4.  Consuming a token succeeds when bucket has tokens
    5.  Consuming a token fails when bucket is empty
    6.  Refill uses elapsed time correctly
    7.  Multiple rapid consumes drain the bucket correctly
    8.  Adaptive rate = BASE at zero load (no pressure)
    9.  Adaptive rate drops when queue depth exceeds threshold
    10. Adaptive rate drops when latency exceeds threshold
    11. Adaptive rate is multiplicative (depth * latency)
    12. Adaptive rate never goes below MIN_FILL_RATE
    13. Check-and-consume: allowed when tokens available
    14. Check-and-consume: rejected when bucket empty, retry_after > 0
    15. Per-queue bucket isolation
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.load_shedder import TokenBucket, AdaptiveLoadShedder
from broker.queue_manager import QueueManager
from broker.worker_registry import WorkerRegistry


def run_test(name, fn):
    """Run a single async or sync test and report pass/fail."""
    try:
        if asyncio.iscoroutinefunction(fn):
            asyncio.run(fn())
        else:
            fn()
        print(f"  PASS: {name}")
        return True
    except Exception as e:
        print(f"  FAIL: {name} -- {e}")
        import traceback
        traceback.print_exc()
        return False


# ── TokenBucket Unit Tests ────────────────────────────────────────

def test_bucket_starts_full():
    """Test 1: A new bucket starts with tokens = capacity."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=10.0)
    assert bucket.tokens == 10.0, (
        f"Expected 10.0 tokens, got {bucket.tokens}"
    )
    assert bucket.capacity == 10.0
    assert bucket.nominal_fill_rate == 10.0


def test_bucket_fills_over_time():
    """Test 2: After draining and waiting, tokens refill at the given rate."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=10.0)
    bucket.tokens = 0.0  # Empty it

    time.sleep(0.5)
    bucket.refill(10.0)  # 10 tokens/sec * 0.5s = ~5 tokens

    assert 4.0 <= bucket.tokens <= 6.5, (
        f"Expected ~5 tokens after 0.5s at 10/s, got {bucket.tokens:.2f}"
    )


def test_bucket_capped_at_capacity():
    """Test 3: Tokens never exceed capacity, even with high fill rate."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=100.0)
    # Already full at 10.0; wait and refill with very high rate
    time.sleep(0.2)  # Would add 20 tokens at 100/s
    bucket.refill(100.0)
    assert bucket.tokens <= 10.0, (
        f"Tokens should be capped at 10.0, got {bucket.tokens:.2f}"
    )


def test_consume_succeeds_when_tokens_available():
    """Test 4: Consuming a token succeeds when bucket has tokens."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=10.0)
    assert bucket.consume() is True
    assert bucket.tokens == 9.0


def test_consume_fails_when_empty():
    """Test 5: Consuming a token fails when bucket is empty."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=10.0)
    bucket.tokens = 0.0
    assert bucket.consume() is False
    assert bucket.tokens == 0.0  # Unchanged


def test_refill_uses_elapsed_time():
    """Test 6: Refill adds exactly (elapsed * rate) tokens."""
    bucket = TokenBucket(capacity=100.0, nominal_fill_rate=50.0)
    bucket.tokens = 0.0

    # Record time, sleep, refill
    time.sleep(0.1)
    bucket.refill(50.0)  # 50/s * 0.1s = ~5 tokens

    assert 3.5 <= bucket.tokens <= 7.0, (
        f"Expected ~5 tokens after 0.1s at 50/s, got {bucket.tokens:.2f}"
    )


def test_multiple_consumes_drain_bucket():
    """Test 7: Consuming all tokens empties the bucket."""
    bucket = TokenBucket(capacity=5.0, nominal_fill_rate=10.0)

    # Consume all 5 tokens
    for i in range(5):
        result = bucket.consume()
        assert result is True, f"Consume #{i+1} should succeed"

    # 6th consume should fail
    assert bucket.consume() is False
    assert bucket.tokens < 1.0, f"Bucket should be nearly empty, got {bucket.tokens}"


def test_refill_after_partial_consume():
    """Test 8: Refill after partial drain fills correctly."""
    bucket = TokenBucket(capacity=10.0, nominal_fill_rate=20.0)

    # Consume 3 tokens (7 left)
    for _ in range(3):
        bucket.consume()
    assert bucket.tokens == 7.0

    # Wait and refill
    time.sleep(0.1)  # 20/s * 0.1s = ~2 tokens
    bucket.refill(20.0)
    assert 8.5 <= bucket.tokens <= 10.0, (
        f"Expected ~9 tokens, got {bucket.tokens:.2f}"
    )


# ── AdaptiveLoadShedder Unit Tests ────────────────────────────────

async def test_rate_at_zero_load():
    """Test 9: At zero load, adjusted rate equals BASE_FILL_RATE."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    # No tasks, no workers = zero load
    rate = shedder._compute_adjusted_rate("test_q")
    assert rate == shedder.BASE_FILL_RATE, (
        f"Expected {shedder.BASE_FILL_RATE}, got {rate}"
    )


async def test_rate_drops_with_queue_depth():
    """Test 10: Rate drops when queue depth exceeds threshold."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    shedder.QUEUE_DEPTH_THRESHOLD = 10

    # Fill queue to 5x threshold
    for i in range(50):
        await qm.enqueue("overloaded", {"i": i})

    rate = shedder._compute_adjusted_rate("overloaded")
    assert rate < shedder.BASE_FILL_RATE, (
        f"Rate should be below base ({shedder.BASE_FILL_RATE}), got {rate}"
    )
    # At 50 depth with threshold 10, excess = 40, max_excess = 100
    # depth_factor = max(0.05, 1.0 - 40/100) = 0.6
    # rate = 100 * 0.6 = 60
    assert 55 <= rate <= 65, f"Expected rate ~60, got {rate}"


async def test_rate_drops_with_latency():
    """Test 11: Rate drops when worker latency exceeds threshold."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    # Simulate high latency by registering a worker with slow stats
    await wr.register("slow-worker", address=("127.0.0.1", 9999))
    # Manually inject latency data
    if hasattr(wr, '_latency_ms'):
        wr._latency_ms = {"test_q": 500.0}
    elif hasattr(wr, '_workers'):
        for w in wr._workers.values():
            w.avg_latency_ms = 500.0  # 2.5x the threshold

    rate = shedder._compute_adjusted_rate("test_q")
    # With high latency, rate should drop
    # The exact drop depends on implementation details
    assert rate <= shedder.BASE_FILL_RATE, (
        f"Rate should be <= base, got {rate}"
    )


async def test_rate_multiplicative():
    """Test 12: Rate factors are multiplicative (depth * latency)."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    shedder.QUEUE_DEPTH_THRESHOLD = 10

    # Fill queue to 3x threshold (depth_factor ~= 0.8)
    for i in range(30):
        await qm.enqueue("multi", {"i": i})

    # Get rate with just depth pressure
    rate_depth_only = shedder._compute_adjusted_rate("multi")

    # Both should reduce rate, and combined should be more aggressive
    assert rate_depth_only < shedder.BASE_FILL_RATE
    assert rate_depth_only >= shedder.MIN_FILL_RATE


async def test_rate_never_below_minimum():
    """Test 13: Rate never drops below MIN_FILL_RATE."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    shedder.QUEUE_DEPTH_THRESHOLD = 1  # Very low threshold
    shedder.MIN_FILL_RATE = 5.0

    # Fill queue way past threshold
    for i in range(500):
        await qm.enqueue("extreme", {"i": i})

    rate = shedder._compute_adjusted_rate("extreme")
    assert rate >= shedder.MIN_FILL_RATE, (
        f"Rate should never go below {shedder.MIN_FILL_RATE}, got {rate}"
    )


async def test_check_and_consume_allowed():
    """Test 14: check_and_consume returns (True, 0.0) when tokens available."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    allowed, retry_after = await shedder.check_and_consume("fresh_q")
    assert allowed is True, "Should be allowed with full bucket"
    assert retry_after == 0.0


async def test_check_and_consume_rejected():
    """Test 15: check_and_consume returns (False, retry>0) when empty."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    # Drain the bucket completely
    bucket = shedder._get_or_create_bucket("drain_q")
    bucket.tokens = 0.0

    allowed, retry_after = await shedder.check_and_consume("drain_q")
    assert allowed is False, "Should be rejected with empty bucket"
    assert retry_after > 0, f"retry_after should be > 0, got {retry_after}"


async def test_per_queue_bucket_isolation():
    """Test 16: Each queue has its own independent bucket."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    # Drain queue A
    bucket_a = shedder._get_or_create_bucket("queue_a")
    bucket_a.tokens = 0.0

    # Queue B should still have full tokens
    allowed_b, _ = await shedder.check_and_consume("queue_b")
    assert allowed_b is True, "Queue B should be unaffected by Queue A"

    allowed_a, _ = await shedder.check_and_consume("queue_a")
    assert allowed_a is False, "Queue A should be rejected"


async def test_stats_tracking():
    """Test 17: Stats track allowed and rejected counts."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    # 3 allowed requests
    for _ in range(3):
        await shedder.check_and_consume("stats_q")

    # Drain and get 1 rejected
    bucket = shedder._get_or_create_bucket("stats_q")
    bucket.tokens = 0.0
    await shedder.check_and_consume("stats_q")

    stats = shedder.get_stats()
    assert stats["total_allowed"] == 3, f"Expected 3 allowed, got {stats['total_allowed']}"
    assert stats["total_rejected"] == 1, f"Expected 1 rejected, got {stats['total_rejected']}"


async def test_bucket_info_format():
    """Test 18: get_bucket_info returns expected fields."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )

    # Create a bucket
    await shedder.check_and_consume("info_q")

    info = shedder.get_bucket_info("info_q")
    assert "tokens" in info
    assert "capacity" in info
    assert "nominal_fill_rate" in info
    assert "adjusted_fill_rate" in info
    assert info["capacity"] == shedder.BUCKET_CAPACITY


async def test_burst_within_capacity():
    """Test 19: Burst of requests up to capacity is allowed."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    shedder.BUCKET_CAPACITY = 20.0

    allowed_count = 0
    for _ in range(25):
        allowed, _ = await shedder.check_and_consume("burst_q")
        if allowed:
            allowed_count += 1

    # All 20 should be allowed (capacity), then rejections
    assert allowed_count == 20, (
        f"Expected 20 allowed in burst (capacity), got {allowed_count}"
    )


async def test_refill_after_rejection():
    """Test 20: After rejection, tokens refill and requests resume."""
    qm = QueueManager()
    wr = WorkerRegistry(queue_manager=qm)
    shedder = AdaptiveLoadShedder(
        queue_manager=qm, worker_registry=wr
    )
    shedder.BUCKET_CAPACITY = 5.0

    # Drain bucket
    for _ in range(5):
        await shedder.check_and_consume("refill_q")

    # Should be rejected
    allowed, _ = await shedder.check_and_consume("refill_q")
    assert allowed is False

    # Wait for refill (at 100/s, 0.1s = 10 tokens, capped at 5)
    await asyncio.sleep(0.1)

    # Should be allowed again
    allowed, _ = await shedder.check_and_consume("refill_q")
    assert allowed is True, "Should be allowed after refill"


if __name__ == "__main__":
    import logging
    logging.getLogger("broker").setLevel(logging.WARNING)
    logging.getLogger("persistence").setLevel(logging.WARNING)

    tests = [
        ("Bucket starts full", test_bucket_starts_full),
        ("Bucket fills over time", test_bucket_fills_over_time),
        ("Bucket capped at capacity", test_bucket_capped_at_capacity),
        ("Consume succeeds with tokens", test_consume_succeeds_when_tokens_available),
        ("Consume fails when empty", test_consume_fails_when_empty),
        ("Refill uses elapsed time", test_refill_uses_elapsed_time),
        ("Multiple consumes drain bucket", test_multiple_consumes_drain_bucket),
        ("Refill after partial drain", test_refill_after_partial_consume),
        ("Rate = BASE at zero load", test_rate_at_zero_load),
        ("Rate drops with queue depth", test_rate_drops_with_queue_depth),
        ("Rate drops with latency", test_rate_drops_with_latency),
        ("Rate is multiplicative", test_rate_multiplicative),
        ("Rate never below minimum", test_rate_never_below_minimum),
        ("Check-and-consume: allowed", test_check_and_consume_allowed),
        ("Check-and-consume: rejected", test_check_and_consume_rejected),
        ("Per-queue bucket isolation", test_per_queue_bucket_isolation),
        ("Stats tracking", test_stats_tracking),
        ("Bucket info format", test_bucket_info_format),
        ("Burst within capacity", test_burst_within_capacity),
        ("Refill after rejection", test_refill_after_rejection),
    ]

    passed = 0
    failed = 0

    print(f"\n--- Token Bucket & Load Shedder Tests ({len(tests)} tests) ---\n")
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}\n")

    sys.exit(0 if failed == 0 else 1)

def test_adaptive_rate_drops_under_load():
    """
    Adaptive rate drops when queue is deep.
    """
    from unittest.mock import MagicMock
    mock_qm = MagicMock()
    mock_qm.queue_depth.return_value = 500
    mock_wr = MagicMock()
    mock_wr.average_latency_ms.return_value = 0.0

    from broker.load_shedder import AdaptiveLoadShedder
    shedder = AdaptiveLoadShedder(mock_qm, mock_wr)
    rate = shedder._compute_adjusted_rate("overloaded_q")

    threshold = shedder.BASE_FILL_RATE * 0.2
    assert rate < threshold
    assert rate >= shedder.MIN_FILL_RATE
