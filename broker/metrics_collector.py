"""
Metrics Collector — Real-time observability for the DistroSync broker.

This module provides a MetricsCollector class that tracks:
    - Per-queue throughput (tasks processed per second)
    - Per-queue latency percentiles (p50, p95, p99)
    - Broker uptime
    - Task lifecycle event counts (produced, consumed, acked, nacked, dlq'd)

How throughput is measured:
    We use a SLIDING WINDOW approach. Instead of counting "tasks in the last
    second" (which gives spiky, unreliable numbers), we keep a deque of
    timestamps for recent events. Throughput = count of events in the last
    N seconds / N. This gives a smooth, accurate rate.

How latency percentiles work:
    When a task is consumed (dequeued), we record the time. When it's ACKed,
    we compute the processing latency (ACK time - dequeue time). We keep a
    rolling buffer of the last 1000 latency samples per queue and compute
    percentiles on demand using sorted insertion.

Why not use a library like prometheus_client?
    DistroSync is designed to be zero-dependency. The metrics collector is
    ~200 lines of pure Python using only collections.deque, time, and
    threading. This keeps the Docker image small and deployment simple.

Thread safety:
    All mutable state is protected by threading.Lock since the metrics
    collector may be accessed from the HTTP API server concurrently with
    the broker's asyncio event loop (via run_in_executor or from a
    separate thread).
"""

import time
import threading
import logging
from collections import deque, defaultdict
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects and exposes real-time broker metrics.

    Usage:
        collector = MetricsCollector()
        collector.record_produce("emails")           # Task produced
        collector.record_consume("emails", task_id)  # Task dequeued
        collector.record_ack("emails", task_id)       # Task completed
        collector.record_nack("emails", task_id)      # Task failed

        metrics = collector.snapshot()  # Get current metrics
    """

    # How many seconds of history to keep for throughput calculation
    THROUGHPUT_WINDOW_SECONDS = 10

    # Maximum number of latency samples to keep per queue
    MAX_LATENCY_SAMPLES = 1000

    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.monotonic()

        # Per-queue event timestamps for throughput (deque of float timestamps)
        self._produce_times: Dict[str, deque] = defaultdict(deque)
        self._ack_times: Dict[str, deque] = defaultdict(deque)

        # Per-queue latency tracking
        # Maps task_id -> dequeue timestamp (for computing processing time)
        self._dequeue_timestamps: Dict[str, float] = {}
        # Maps task_id -> queue_name (for attributing latency to correct queue)
        self._task_queues: Dict[str, str] = {}
        # Per-queue latency samples in milliseconds
        self._latency_samples: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.MAX_LATENCY_SAMPLES)
        )

        # Lifetime counters
        self._total_produced: Dict[str, int] = defaultdict(int)
        self._total_consumed: Dict[str, int] = defaultdict(int)
        self._total_acked: Dict[str, int] = defaultdict(int)
        self._total_nacked: Dict[str, int] = defaultdict(int)
        self._total_dlq: Dict[str, int] = defaultdict(int)

        logger.info("MetricsCollector initialized")

    def _prune_window(self, timestamps: deque, now: float) -> None:
        """Remove timestamps older than the throughput window."""
        cutoff = now - self.THROUGHPUT_WINDOW_SECONDS
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()

    # ── Recording Events ──────────────────────────────────────────────

    def record_produce(self, queue_name: str) -> None:
        """Record that a task was produced (enqueued) to a queue."""
        now = time.monotonic()
        with self._lock:
            self._produce_times[queue_name].append(now)
            self._total_produced[queue_name] += 1

    def record_consume(self, queue_name: str, task_id: str) -> None:
        """Record that a task was consumed (dequeued) from a queue."""
        now = time.monotonic()
        with self._lock:
            self._dequeue_timestamps[task_id] = now
            self._task_queues[task_id] = queue_name
            self._total_consumed[queue_name] += 1

    def record_ack(self, queue_name: str, task_id: str) -> None:
        """
        Record that a task was acknowledged (completed successfully).

        Computes the processing latency if we have the dequeue timestamp.
        """
        now = time.monotonic()
        with self._lock:
            self._ack_times[queue_name].append(now)
            self._total_acked[queue_name] += 1

            # Compute latency if we tracked the dequeue time
            dequeue_time = self._dequeue_timestamps.pop(task_id, None)
            self._task_queues.pop(task_id, None)
            if dequeue_time is not None:
                latency_ms = (now - dequeue_time) * 1000
                self._latency_samples[queue_name].append(latency_ms)

    def record_nack(self, queue_name: str, task_id: str,
                    dead_lettered: bool = False) -> None:
        """Record that a task was negatively acknowledged (failed)."""
        with self._lock:
            self._total_nacked[queue_name] += 1
            if dead_lettered:
                self._total_dlq[queue_name] += 1
            # Clean up dequeue tracking (task will be re-tracked on retry)
            self._dequeue_timestamps.pop(task_id, None)
            self._task_queues.pop(task_id, None)

    # ── Querying Metrics ──────────────────────────────────────────────

    def throughput(self, queue_name: str) -> float:
        """
        Get the current throughput for a queue in tasks/second.

        Uses the ACK rate (completed tasks) as the throughput measure,
        since that represents actual work done, not just work submitted.
        """
        now = time.monotonic()
        with self._lock:
            timestamps = self._ack_times.get(queue_name, deque())
            self._prune_window(timestamps, now)
            count = len(timestamps)
        if count == 0:
            return 0.0
        return count / self.THROUGHPUT_WINDOW_SECONDS

    def produce_rate(self, queue_name: str) -> float:
        """Get the current produce rate for a queue in tasks/second."""
        now = time.monotonic()
        with self._lock:
            timestamps = self._produce_times.get(queue_name, deque())
            self._prune_window(timestamps, now)
            count = len(timestamps)
        if count == 0:
            return 0.0
        return count / self.THROUGHPUT_WINDOW_SECONDS

    def percentile_latency(self, queue_name: str, p: float) -> float:
        """
        Get the p-th percentile latency for a queue in milliseconds.

        Args:
            queue_name: The queue to query.
            p: The percentile (0-100). Common values: 50, 95, 99.

        Returns:
            Latency in milliseconds, or 0.0 if no samples exist.
        """
        with self._lock:
            samples = list(self._latency_samples.get(queue_name, []))
        if not samples:
            return 0.0
        samples.sort()
        idx = int(len(samples) * p / 100)
        idx = min(idx, len(samples) - 1)
        return samples[idx]

    def p50_latency(self, queue_name: str) -> float:
        """Get p50 (median) latency for a queue in ms."""
        return self.percentile_latency(queue_name, 50)

    def p95_latency(self, queue_name: str) -> float:
        """Get p95 latency for a queue in ms."""
        return self.percentile_latency(queue_name, 95)

    def p99_latency(self, queue_name: str) -> float:
        """Get p99 latency for a queue in ms."""
        return self.percentile_latency(queue_name, 99)

    def uptime_seconds(self) -> float:
        """Get broker uptime in seconds."""
        return time.monotonic() - self._start_time

    def get_stats(self) -> dict:
        """Get lifetime counter stats (for the STATS command)."""
        with self._lock:
            return {
                "total_produced": dict(self._total_produced),
                "total_consumed": dict(self._total_consumed),
                "total_acked": dict(self._total_acked),
                "total_nacked": dict(self._total_nacked),
                "total_dlq": dict(self._total_dlq),
                "uptime_seconds": round(self.uptime_seconds(), 1),
                "tracked_in_flight": len(self._dequeue_timestamps),
            }

    # ── Full Snapshot (for the METRICS command / HTTP API) ────────────

    def snapshot(self, queue_names: list = None) -> dict:
        """
        Get a complete metrics snapshot suitable for the dashboard.

        Args:
            queue_names: List of queue names to include. If None,
                         includes all known queues.

        Returns:
            A dictionary matching the metrics API schema.
        """
        if queue_names is None:
            with self._lock:
                queue_names = list(set(
                    list(self._total_produced.keys()) +
                    list(self._total_consumed.keys())
                ))

        queues = {}
        for name in queue_names:
            queues[name] = {
                "depth": 0,  # Will be filled by the broker
                "produce_rate": round(self.produce_rate(name), 1),
                "throughput_per_second": round(self.throughput(name), 1),
                "p50_latency_ms": round(self.p50_latency(name), 1),
                "p95_latency_ms": round(self.p95_latency(name), 1),
                "p99_latency_ms": round(self.p99_latency(name), 1),
                "total_produced": self._total_produced.get(name, 0),
                "total_acked": self._total_acked.get(name, 0),
                "total_nacked": self._total_nacked.get(name, 0),
            }

        return {
            "queues": queues,
            "broker": {
                "uptime_seconds": round(self.uptime_seconds(), 1),
                "tracked_in_flight": len(self._dequeue_timestamps),
            },
        }
