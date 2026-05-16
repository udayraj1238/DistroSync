"""
Adaptive Load Shedder — Token-bucket rate limiter with dynamic adjustment.

This is the core innovation of DistroSync. When queues back up and workers
slow down, the broker doesn't just keep accepting tasks until it falls over.
Instead, it dynamically throttles producers using an adaptive token-bucket
algorithm.

What is a token bucket?
    A rate-limiting algorithm used everywhere in production systems:
    AWS API Gateway, Cloudflare, Nginx, Envoy, Google Cloud Endpoints.

    Imagine a bucket that can hold N tokens. Tokens are added at a fixed
    rate (e.g., 100 tokens/second). Each accepted request consumes one
    token. When the bucket is empty, requests are rejected. The bucket
    cannot hold more than its capacity (excess tokens are discarded).

    Formally:
        tokens(t) = min(capacity, tokens(t-1) + elapsed * fill_rate)

    This gives you:
        - Burst tolerance: a full bucket allows short bursts up to capacity
        - Sustained rate limiting: over time, throughput = fill_rate
        - No fixed windows: unlike fixed-window counters, there are no
          boundary effects at window edges

What makes our version "adaptive"?
    Most token buckets have a FIXED fill rate. Our version adjusts the
    fill rate based on two real-time signals:

    1. Queue depth — how many tasks are waiting to be processed.
       If the queue is long, workers can't keep up, so we should
       slow down producers.

    2. Average worker latency — how long tasks are taking to complete.
       If workers are slow (maybe due to CPU contention, network latency,
       or downstream service issues), we should slow down producers.

    Both signals are combined multiplicatively:
        adjusted_rate = BASE_RATE * depth_factor * latency_factor

    This means either signal alone can reduce throughput, and both
    together reduce it dramatically. The multiplicative approach is
    better than additive because it prevents one healthy signal from
    masking a badly deteriorated one.

    Similar systems:
        - Netflix's concurrency-limits library (adaptive algorithms)
        - TCP congestion control (AIMD: Additive Increase,
          Multiplicative Decrease)
        - CoDel (Controlled Delay) active queue management

How it integrates:
    1. Producer sends PRODUCE command
    2. Broker calls load_shedder.check_and_consume(queue_name)
    3. If allowed: enqueue the task normally
    4. If rejected: respond with "rate_limited" + retry_after hint
    5. Producer uses ExponentialBackoff with jitter to retry
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from typing import Dict

logger = logging.getLogger(__name__)


@dataclass
class TokenBucket:
    """
    A single token bucket for rate-limiting.

    The bucket starts full (tokens = capacity). Tokens are consumed
    by accepted requests and refilled over time at the adjusted rate.

    Why float tokens instead of int?
        Token refill is time-based. If 0.3 seconds elapsed at 100
        tokens/sec, we add 30.0 tokens. Using int would truncate
        fractional tokens on every refill, causing drift over time.

    Attributes:
        capacity:          Maximum tokens the bucket can hold.
        nominal_fill_rate: Tokens per second at zero load.
        tokens:            Current token count.
        last_refill:       Monotonic timestamp of the last refill.
    """
    capacity: float
    nominal_fill_rate: float
    tokens: float = field(init=False)
    last_refill: float = field(init=False)

    def __post_init__(self):
        self.tokens = self.capacity
        self.last_refill = time.monotonic()

    def refill(self, adjusted_rate: float):
        """
        Add tokens based on time elapsed and the current adjusted rate.

        Uses monotonic clock because wall clock (time.time) can jump
        backwards during NTP corrections, which would break the elapsed
        time calculation.

        Args:
            adjusted_rate: Current fill rate in tokens/second.
                           This is the rate AFTER adaptive adjustment.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * adjusted_rate)
        self.last_refill = now

    def consume(self) -> bool:
        """
        Try to consume one token from the bucket.

        Returns:
            True if a token was available and consumed (request allowed).
            False if the bucket is empty (request rejected).
        """
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class AdaptiveLoadShedder:
    """
    Rate limiter that adjusts its fill rate based on system pressure.

    The load shedder maintains one TokenBucket per queue. Each bucket's
    fill rate is adjusted every time a produce request comes in, based
    on the current queue depth and average worker latency.

    Tuning parameters:
        BASE_FILL_RATE (100 tokens/sec):
            The nominal rate at zero load. 100 means the system can
            handle 100 tasks/sec per queue when things are healthy.

        MIN_FILL_RATE (5 tokens/sec):
            Floor rate. Even under extreme load, we still allow some
            tasks through so the system doesn't completely starve.
            This prevents a total blackout during load spikes.

        QUEUE_DEPTH_THRESHOLD (50):
            Start throttling when queue depth exceeds this. 50 is a
            good default for most workloads — it gives workers a buffer
            without letting the queue grow unbounded.

        LATENCY_THRESHOLD_MS (200):
            Start throttling when average worker latency exceeds 200ms.
            This catches cases where the queue isn't deep yet but workers
            are clearly struggling (e.g., downstream service is slow).

        BUCKET_CAPACITY (200):
            Max tokens in each bucket. This allows short bursts of up to
            200 tasks even during throttling, which is useful for
            legitimate traffic spikes (e.g., batch job submission).
    """

    def __init__(self, queue_manager, worker_registry):
        """
        Initialize the adaptive load shedder.

        Args:
            queue_manager:   Reference to the QueueManager for queue depth.
            worker_registry: Reference to the WorkerRegistry for latency data.
        """
        # One bucket per queue name
        self._buckets: Dict[str, TokenBucket] = {}

        # References to system components for signal gathering
        self._queue_manager = queue_manager
        self._worker_registry = worker_registry

        # Asyncio lock — protects bucket access during concurrent PRODUCE
        # commands. Multiple producers can PRODUCE simultaneously.
        self._lock = asyncio.Lock()

        # ── Tuning parameters ──────────────────────────────────────

        # Tokens per second at zero load
        self.BASE_FILL_RATE: float = 100.0

        # Minimum fill rate (never go below this)
        self.MIN_FILL_RATE: float = 5.0

        # Start throttling when queue depth exceeds this
        self.QUEUE_DEPTH_THRESHOLD: int = 50

        # Start throttling when average latency exceeds this (ms)
        self.LATENCY_THRESHOLD_MS: float = 200.0

        # Maximum tokens per bucket (burst capacity)
        self.BUCKET_CAPACITY: float = 200.0

        # Stats
        self._total_allowed: int = 0
        self._total_rejected: int = 0

        logger.info(
            f"AdaptiveLoadShedder initialized "
            f"(base_rate={self.BASE_FILL_RATE}/s, "
            f"depth_threshold={self.QUEUE_DEPTH_THRESHOLD}, "
            f"latency_threshold={self.LATENCY_THRESHOLD_MS}ms)"
        )

    def _get_or_create_bucket(self, queue_name: str) -> TokenBucket:
        """
        Get the token bucket for a queue, creating it if needed.

        Each queue gets its own bucket so that one overloaded queue
        doesn't cause rate limiting on other healthy queues.

        Args:
            queue_name: The queue to get the bucket for.

        Returns:
            The TokenBucket for this queue.
        """
        if queue_name not in self._buckets:
            self._buckets[queue_name] = TokenBucket(
                capacity=self.BUCKET_CAPACITY,
                nominal_fill_rate=self.BASE_FILL_RATE,
            )
            logger.info(
                f"Created token bucket for queue '{queue_name}' "
                f"(capacity={self.BUCKET_CAPACITY}, "
                f"nominal_rate={self.BASE_FILL_RATE}/s)"
            )
        return self._buckets[queue_name]

    def _compute_adjusted_rate(self, queue_name: str) -> float:
        """
        Compute the token fill rate based on current system pressure.

        The formula:
            adjusted_rate = BASE_RATE * depth_factor * latency_factor

        Where:
            depth_factor   = 1.0 when depth <= threshold, drops linearly
                             to 0.05 at 10x the threshold
            latency_factor = 1.0 when latency <= threshold, drops linearly
                             to 0.1 at 10x the threshold

        The multiplicative combination means:
            - Queue depth 5x threshold + latency 2x threshold =
              very aggressive throttling
            - Queue depth 2x threshold + latency normal =
              moderate throttling

        This is analogous to TCP AIMD (Additive Increase, Multiplicative
        Decrease): the system quickly reduces throughput under pressure
        and slowly recovers when pressure eases.

        Args:
            queue_name: The queue to compute the rate for.

        Returns:
            Adjusted fill rate in tokens/second, clamped to MIN_FILL_RATE.
        """
        depth = self._queue_manager.queue_depth(queue_name)
        avg_latency_ms = self._worker_registry.average_latency_ms(queue_name)

        # ── Depth factor ──────────────────────────────────────────
        # At or below threshold: no throttling (factor = 1.0)
        # Above threshold: linear decrease toward 0.05
        # At 10x threshold: factor = 0.05 (floor)
        if depth <= self.QUEUE_DEPTH_THRESHOLD:
            depth_factor = 1.0
        else:
            excess = depth - self.QUEUE_DEPTH_THRESHOLD
            max_excess = 10 * self.QUEUE_DEPTH_THRESHOLD
            depth_factor = max(0.05, 1.0 - excess / max_excess)

        # ── Latency factor ────────────────────────────────────────
        # At or below threshold: no throttling (factor = 1.0)
        # Above threshold: linear decrease toward 0.1
        # At 10x threshold: factor = 0.1 (floor)
        if avg_latency_ms <= self.LATENCY_THRESHOLD_MS:
            latency_factor = 1.0
        else:
            excess = avg_latency_ms - self.LATENCY_THRESHOLD_MS
            max_excess = 10 * self.LATENCY_THRESHOLD_MS
            latency_factor = max(0.1, 1.0 - excess / max_excess)

        # Combine multiplicatively and clamp to floor
        adjusted = self.BASE_FILL_RATE * depth_factor * latency_factor
        clamped = max(self.MIN_FILL_RATE, adjusted)

        return clamped

    async def check_and_consume(self, queue_name: str) -> tuple:
        """
        Check if a produce request should be allowed or rejected.

        This is the main entry point, called by the broker for every
        PRODUCE command. It:
            1. Gets (or creates) the bucket for this queue
            2. Computes the current adjusted fill rate
            3. Refills the bucket based on elapsed time
            4. Tries to consume a token

        If allowed: returns (True, 0.0)
        If rejected: returns (False, retry_after_seconds)

        The retry_after hint tells the producer how long to wait before
        trying again. It's calculated as 1/adjusted_rate — the time
        it takes for one token to refill.

        Args:
            queue_name: The queue being produced to.

        Returns:
            Tuple of (allowed: bool, retry_after_seconds: float).
        """
        async with self._lock:
            bucket = self._get_or_create_bucket(queue_name)
            adjusted_rate = self._compute_adjusted_rate(queue_name)
            bucket.refill(adjusted_rate)
            allowed = bucket.consume()

            if allowed:
                self._total_allowed += 1
                return True, 0.0
            else:
                self._total_rejected += 1
                # How long until one token refills at the current rate?
                retry_after = 1.0 / adjusted_rate
                logger.debug(
                    f"Rate limited on queue '{queue_name}' "
                    f"(depth={self._queue_manager.queue_depth(queue_name)}, "
                    f"rate={adjusted_rate:.1f}/s, "
                    f"retry_after={retry_after:.3f}s)"
                )
                return False, retry_after

    def get_bucket_info(self, queue_name: str) -> dict:
        """
        Return the current state of a queue's token bucket.

        Useful for debugging and the metrics layer.

        Args:
            queue_name: The queue to inspect.

        Returns:
            Dict with bucket state, or empty dict if no bucket exists.
        """
        bucket = self._buckets.get(queue_name)
        if bucket is None:
            return {}
        return {
            "tokens": round(bucket.tokens, 2),
            "capacity": bucket.capacity,
            "nominal_fill_rate": bucket.nominal_fill_rate,
            "adjusted_fill_rate": round(
                self._compute_adjusted_rate(queue_name), 2
            ),
        }

    def get_stats(self) -> dict:
        """Return a snapshot of load shedder statistics."""
        return {
            "total_allowed": self._total_allowed,
            "total_rejected": self._total_rejected,
            "rejection_rate": (
                self._total_rejected / max(1, self._total_allowed + self._total_rejected)
            ),
            "buckets": {
                name: {
                    "tokens": round(b.tokens, 2),
                    "capacity": b.capacity,
                    "adjusted_rate": round(
                        self._compute_adjusted_rate(name), 2
                    ),
                }
                for name, b in self._buckets.items()
            },
        }

# added adaptive load shedder
