"""
Unit Tests: Isolated Function Testing

Four focused unit tests for the load shedder's core components:
  1. Token bucket fills at the correct rate over time
  2. Token bucket is capped at capacity (burst ceiling)
  3. Token consume returns False when empty (exact boundary)
  4. Adaptive rate drops when queue is deep (the unique feature)

These tests use NO broker, NO TCP, NO async — pure unit tests
that exercise the math and logic in isolation.
"""

import sys
import os
import time
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.load_shedder import TokenBucket, AdaptiveLoadShedder


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    def run(self, name, func):
        """Run a single test function and track results."""
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            self.failed += 1


def test_bucket_fills_over_time():
    """
    Token bucket fills at correct rate.

    Create a TokenBucket with capacity=10 and fill_rate=10 tokens/sec.
    Empty it to 0. Sleep 0.5 seconds. Call refill(10).
    Assert tokens is between 4.5 and 5.5 (~5 tokens after 0.5s at 10/s).

    What this proves: You understand time-based rate limiting, not just counting.
    """
    b = TokenBucket(capacity=10.0, nominal_fill_rate=10.0)

    # Drain the bucket completely
    b.tokens = 0.0

    # Wait for tokens to accumulate in real time
    time.sleep(0.5)

    # Refill at the nominal rate (10 tokens/sec)
    b.refill(10.0)

    assert 4.5 <= b.tokens <= 5.5, (
        f"Expected ~5.0 tokens after 0.5s at 10/s, got {b.tokens:.3f}"
    )


def test_bucket_capped_at_capacity():
    """
    Token bucket capped at capacity.

    Create bucket with capacity=10, fill_rate=100. Sleep 0.5s
    (would add 50 tokens at 100/s). Call refill. Assert tokens
    never exceeds 10.

    What this proves: Confirms the burst-limiting behavior of the bucket ceiling.
    """
    b = TokenBucket(capacity=10.0, nominal_fill_rate=100.0)

    # Bucket starts full at capacity=10
    # Sleep would add 50 more tokens (100/s * 0.5s) if uncapped
    time.sleep(0.5)
    b.refill(100.0)

    assert b.tokens <= 10.0, (
        f"Tokens ({b.tokens:.3f}) exceeded capacity (10.0) — "
        f"bucket ceiling is broken"
    )


def test_consume_returns_false_when_empty():
    """
    Token consume returns False when empty.

    Create a bucket with capacity=5, drain it completely by calling
    consume() in a loop until it returns False. Verify the exact
    count consumed equals the initial capacity.

    What this proves: Validates that the bucket rejects exactly at
    capacity, not before or after.
    """
    b = TokenBucket(capacity=5.0, nominal_fill_rate=10.0)

    # Try to consume 10 times — only 5 should succeed (capacity=5)
    consumed = sum(1 for _ in range(10) if b.consume())

    assert consumed == 5, (
        f"Expected exactly 5 successful consumes from capacity=5 bucket, "
        f"got {consumed}"
    )

    # One more explicit check: bucket is definitely empty now
    assert not b.consume(), (
        "Bucket should reject after being fully drained"
    )


def test_adaptive_rate_drops_under_load():
    """
    Adaptive rate drops when queue is deep.

    Create AdaptiveLoadShedder with mocked dependencies. Set
    queue_depth to return 500 (10x the default threshold of 50).
    Call _compute_adjusted_rate(). Assert rate is less than 20%
    of BASE_FILL_RATE.

    What this proves: Shows the adaptive part actually works —
    this is the unique feature that distinguishes DistroSync.
    """
    # Mock QueueManager: report a very deep queue
    mock_qm = MagicMock()
    mock_qm.queue_depth.return_value = 500

    # Mock WorkerRegistry: report no latency issues (isolate depth signal)
    mock_wr = MagicMock()
    mock_wr.average_latency_ms.return_value = 0.0

    shedder = AdaptiveLoadShedder(mock_qm, mock_wr)

    rate = shedder._compute_adjusted_rate("overloaded_q")

    # At depth=500 (10x threshold of 50):
    #   excess = 500 - 50 = 450
    #   max_excess = 10 * 50 = 500
    #   depth_factor = max(0.05, 1.0 - 450/500) = max(0.05, 0.1) = 0.1
    #   latency_factor = 1.0 (no latency pressure)
    #   adjusted = 100 * 0.1 * 1.0 = 10.0
    #
    # 10.0 < 100 * 0.2 = 20.0 ✓
    threshold = shedder.BASE_FILL_RATE * 0.2

    assert rate < threshold, (
        f"At queue depth 500, rate ({rate:.1f}) should be < 20% of "
        f"BASE_FILL_RATE ({threshold:.1f}). The adaptive algorithm "
        f"isn't throttling aggressively enough."
    )

    # Also verify it doesn't go below the floor
    assert rate >= shedder.MIN_FILL_RATE, (
        f"Rate ({rate:.1f}) should never go below "
        f"MIN_FILL_RATE ({shedder.MIN_FILL_RATE})"
    )


def main():
    runner = TestRunner()

    runner.run("Token bucket fills at correct rate", test_bucket_fills_over_time)
    runner.run("Token bucket capped at capacity", test_bucket_capped_at_capacity)
    runner.run("Token consume returns False when empty", test_consume_returns_false_when_empty)
    runner.run("Adaptive rate drops when queue is deep", test_adaptive_rate_drops_under_load)

    print(f"\n{'=' * 60}")
    print(f"  Unit Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
