"""
Worker Registry — Tracks connected workers, heartbeats, and eviction.

In a distributed system, the broker needs to know which workers are
alive. Workers prove they're alive by sending HEARTBEAT messages
every 2 seconds. If a worker misses 3 consecutive heartbeats (6
seconds of silence), the broker evicts it and reassigns its in-flight
tasks to other workers.

Why 6 seconds and not 30?
    There's a tradeoff between false positives and recovery time:
    - Too short (e.g., 1s): network hiccups cause false evictions
    - Too long (e.g., 60s): tasks sit stuck on dead workers for a minute
    6 seconds (3 missed beats at 2s each) is the sweet spot used by
    systems like Redis Sentinel and ZooKeeper.

Why heartbeats instead of TCP keepalive?
    TCP keepalive is OS-level and typically defaults to 2-hour timeouts.
    Application-level heartbeats give us sub-second control. Also, TCP
    keepalive only detects dead connections — it can't detect a worker
    that's alive but frozen (e.g., stuck in an infinite loop or a GIL
    contention). Application heartbeats prove the worker's event loop
    is actually running.

In-flight task tracking:
    Each WorkerInfo maintains a set of task_ids that the worker is
    currently processing. When a worker dies, the eviction loop iterates
    this set and requeues each task through the QueueManager. This
    prevents tasks from being silently lost when a worker crashes.
"""

import asyncio
import time
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Set, Optional

logger = logging.getLogger(__name__)

# A worker must send a heartbeat at least every 2 seconds.
# If we miss 3 consecutive heartbeats, the worker is considered dead.
# 3 * 2s = 6s timeout. This is the same strategy used by Redis Sentinel.
HEARTBEAT_TIMEOUT_SECONDS = 6


@dataclass
class WorkerInfo:
    """
    Metadata about a single connected worker.

    Attributes:
        worker_id:        Unique identifier for this worker.
        queues:           List of queue names this worker consumes from.
        address:          Tuple of (host, port) the worker connected from.
        registered_at:    Unix timestamp when the worker first connected.
        last_heartbeat:   Unix timestamp of the most recent heartbeat.
        in_flight_tasks:  Set of task_ids currently assigned to this worker.
                          If the worker dies, these tasks get reassigned.
        task_start_times: Maps task_id -> monotonic timestamp when it was
                          assigned. Used to measure task completion latency.
        status:           One of "active", "evicted".
    """
    worker_id: str
    queues: list = field(default_factory=list)
    address: tuple = ("unknown", 0)
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    in_flight_tasks: Set[str] = field(default_factory=set)
    task_start_times: Dict[str, float] = field(default_factory=dict)
    writer: Optional[asyncio.StreamWriter] = None
    status: str = "active"


class WorkerRegistry:
    """
    Registry that tracks all known workers and evicts dead ones.

    The registry has two responsibilities:
        1. Tracking: know which workers are alive and what they're doing
        2. Eviction: detect dead workers and reassign their tasks

    The eviction loop runs as a background coroutine on the broker's
    event loop, checking every 2 seconds for workers whose last
    heartbeat is older than HEARTBEAT_TIMEOUT_SECONDS.

    Why does the registry need a reference to the QueueManager?
        When a dead worker is evicted, its in-flight tasks need to go
        back into the queue. The registry holds the worker's task_ids
        but the QueueManager holds the actual Task objects. The registry
        calls queue_manager.requeue_by_id() for each orphaned task.
    """

    def __init__(self, queue_manager=None):
        """
        Initialize the worker registry.

        Args:
            queue_manager: Reference to the broker's QueueManager.
                           Required for requeuing tasks from dead workers.
                           Can be None during unit testing if eviction
                           logic is not being tested.
        """
        # worker_id -> WorkerInfo
        self._workers: Dict[str, WorkerInfo] = {}

        # Reference to the queue manager for task requeuing on eviction
        self._queue_manager = queue_manager

        # Asyncio lock — protects concurrent access to the worker dict.
        # The eviction loop and heartbeat handlers both mutate _workers.
        self._lock = asyncio.Lock()

        # Eviction loop control
        self._eviction_task: Optional[asyncio.Task] = None
        self._running: bool = False

        # Per-queue latency tracking (sliding window of recent latencies in ms)
        # Used by the AdaptiveLoadShedder to gauge worker performance.
        # Stores the last 100 task completion latencies per queue.
        self._queue_latencies: Dict[str, deque] = {}

        # Stats
        self._total_evictions: int = 0
        self._total_tasks_reassigned: int = 0

        logger.info("WorkerRegistry initialized")

    async def register(
        self,
        worker_id: str,
        address: tuple = ("unknown", 0),
        queues: Optional[list] = None,
        writer: Optional[asyncio.StreamWriter] = None,
    ) -> bool:
        """
        Register a new worker or update an existing one.

        Called when a worker sends the REGISTER command. If the worker_id
        already exists, the registration is updated (handles reconnections).

        Args:
            worker_id: Unique identifier for the worker.
            address:   The (host, port) tuple from the TCP connection.
            queues:    List of queue names this worker wants to consume from.

        Returns:
            True if this was a new registration, False if it was an update.
        """
        async with self._lock:
            is_new = worker_id not in self._workers
            self._workers[worker_id] = WorkerInfo(
                worker_id=worker_id,
                queues=queues or [],
                address=address,
                registered_at=time.time(),
                last_heartbeat=time.time(),
                in_flight_tasks=set(),
                writer=writer,
                status="active",
            )

        action = "Registered new" if is_new else "Re-registered"
        logger.info(f"{action} worker '{worker_id}' from {address}")
        return is_new

    async def unregister(self, worker_id: str) -> bool:
        """
        Remove a worker from the registry (graceful disconnect).

        Args:
            worker_id: The ID of the worker to remove.

        Returns:
            True if the worker was found and removed, False if not found.
        """
        async with self._lock:
            worker = self._workers.pop(worker_id, None)

        if worker is None:
            logger.warning(f"Attempted to unregister unknown worker: {worker_id}")
            return False

        logger.info(f"Unregistered worker '{worker_id}'")
        return True

    async def record_heartbeat(self, worker_id: str) -> bool:
        """
        Update the last heartbeat timestamp for a worker.

        Called when the broker receives a HEARTBEAT command. This
        resets the eviction timer for that worker.

        Args:
            worker_id: The ID of the worker that sent the heartbeat.

        Returns:
            True if the worker was found, False if it's unknown.
        """
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                logger.warning(f"Heartbeat from unknown worker: {worker_id}")
                return False

            worker.last_heartbeat = time.time()
            worker.status = "active"

        return True

    async def assign_task(self, worker_id: str, task_id: str):
        """
        Track that a task has been assigned to a worker.

        Called by the broker when a CONSUME command dequeues a task.
        If the worker dies later, the eviction loop uses this set
        to know which tasks need to be reassigned.

        Also records the assignment time (monotonic clock) so we can
        measure task completion latency when the worker ACKs/NACKs.

        Args:
            worker_id: The worker receiving the task.
            task_id:   The task being assigned.
        """
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker:
                worker.in_flight_tasks.add(task_id)
                worker.task_start_times[task_id] = time.monotonic()

    async def complete_task(self, worker_id: str, task_id: str):
        """
        Remove a task from a worker's in-flight set (ACK or NACK received).

        Called by the broker when a task is acknowledged or negative-acknowledged.
        This prevents the eviction loop from trying to requeue a task
        that's already been handled.

        Also calculates and records the task completion latency for the
        queue's latency window (used by the AdaptiveLoadShedder).

        Args:
            worker_id: The worker that was processing the task.
            task_id:   The task that completed or failed.
        """
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker:
                worker.in_flight_tasks.discard(task_id)

                # Calculate latency if we have the start time
                start_time = worker.task_start_times.pop(task_id, None)
                if start_time is not None:
                    latency_ms = (time.monotonic() - start_time) * 1000
                    # Record latency for each queue this worker serves
                    for queue_name in worker.queues:
                        self._record_latency(queue_name, latency_ms)

    async def evict_dead_workers(self):
        """
        Check for workers that have missed their heartbeat deadline
        and evict them, reassigning their in-flight tasks.

        This is called periodically by the eviction loop (every 2 seconds).
        A worker is considered dead if:
            now - last_heartbeat > HEARTBEAT_TIMEOUT_SECONDS (6 seconds)

        When a worker is evicted:
            1. It's removed from the _workers dict
            2. Each of its in_flight_tasks is requeued via the QueueManager
            3. The stats counters are updated

        The requeue puts tasks at the FRONT of their queues so they
        get picked up quickly by a healthy worker.
        """
        now = time.time()

        async with self._lock:
            dead_workers = []
            for wid, info in self._workers.items():
                if now - info.last_heartbeat > HEARTBEAT_TIMEOUT_SECONDS:
                    dead_workers.append((wid, info))

            # Remove dead workers from the registry while still under lock
            for wid, _ in dead_workers:
                del self._workers[wid]

        # Requeue tasks outside the lock to avoid holding it during I/O
        for wid, info in dead_workers:
            task_count = len(info.in_flight_tasks)
            logger.warning(
                f"Evicting dead worker {wid[:8]}... "
                f"(last heartbeat: {now - info.last_heartbeat:.1f}s ago, "
                f"reassigning {task_count} tasks)"
            )

            # Requeue each in-flight task so another worker picks it up
            if self._queue_manager:
                for task_id in info.in_flight_tasks:
                    requeued = await self._queue_manager.requeue_by_id(task_id)
                    if requeued:
                        self._total_tasks_reassigned += 1

            self._total_evictions += 1

    async def start_eviction_loop(self):
        """
        Start the background eviction loop.

        This coroutine runs forever, checking for dead workers every
        2 seconds. It's started as an asyncio.Task when the broker
        boots up.

        The check interval (2s) matches the heartbeat interval. This
        means a dead worker will be detected within 2s of its timeout
        expiring (worst case: 8s total; best case: 6s total).
        """
        self._running = True
        logger.info(
            f"Eviction loop started (timeout: {HEARTBEAT_TIMEOUT_SECONDS}s, "
            f"check interval: 2s)"
        )
        while self._running:
            await asyncio.sleep(2)
            if not self._running:
                break
            await self.evict_dead_workers()

    async def stop_eviction_loop(self):
        """Stop the eviction loop gracefully."""
        self._running = False
        if self._eviction_task:
            self._eviction_task.cancel()
            try:
                await self._eviction_task
            except asyncio.CancelledError:
                pass
            self._eviction_task = None
        logger.info("Eviction loop stopped")

    def _record_latency(self, queue_name: str, latency_ms: float):
        """
        Record a task completion latency for a specific queue.

        Maintains a sliding window of the last 100 latencies per queue.
        The window size is a tradeoff:
            - Too small (e.g., 10): single outliers skew the average
            - Too large (e.g., 10000): adapts too slowly to changes
            - 100: stable enough for smoothing, responsive enough for
              detecting sustained slowdowns

        Args:
            queue_name:  The queue the task belonged to.
            latency_ms:  Completion time in milliseconds.
        """
        if queue_name not in self._queue_latencies:
            self._queue_latencies[queue_name] = deque(maxlen=100)
        self._queue_latencies[queue_name].append(latency_ms)

    def average_latency_ms(self, queue_name: str) -> float:
        """
        Return the average task completion latency for a queue.

        Uses the sliding window of recent completions. Returns 0.0 if
        no latency data is available yet (new queue, no tasks completed).

        This is called by the AdaptiveLoadShedder to adjust the token
        fill rate. High average latency = workers are struggling =
        reduce the fill rate = throttle producers.

        Args:
            queue_name: The queue to get latency for.

        Returns:
            Average latency in milliseconds, or 0.0 if no data.
        """
        samples = self._queue_latencies.get(queue_name)
        if not samples:
            return 0.0
        return sum(samples) / len(samples)

    def get_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        """Return the WorkerInfo for a specific worker, or None."""
        return self._workers.get(worker_id)

    def get_active_workers(self) -> list[WorkerInfo]:
        """Return a list of all workers with status 'active'."""
        return [w for w in self._workers.values() if w.status == "active"]

    def get_worker_count(self) -> int:
        """Return the total number of registered workers."""
        return len(self._workers)

    def get_stats(self) -> dict:
        """
        Return a snapshot of worker registry statistics.

        Useful for the metrics/observability layer.
        """
        active = sum(1 for w in self._workers.values() if w.status == "active")
        return {
            "total_workers": len(self._workers),
            "active_workers": active,
            "total_evictions": self._total_evictions,
            "total_tasks_reassigned": self._total_tasks_reassigned,
            "queue_latencies": {
                name: {
                    "average_ms": self.average_latency_ms(name),
                    "sample_count": len(samples),
                }
                for name, samples in self._queue_latencies.items()
            },
            "workers": {
                wid: {
                    "status": w.status,
                    "queues": w.queues,
                    "last_heartbeat": w.last_heartbeat,
                    "in_flight_tasks": len(w.in_flight_tasks),
                }
                for wid, w in self._workers.items()
            },
        }
