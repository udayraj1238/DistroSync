"""
Queue Manager — In-memory task queue with named queue support.

This module manages multiple named queues. Producers enqueue tasks
into a specific queue by name, and workers dequeue from them.

For now, everything lives in memory. Crash safety via SQLite
WAL-mode persistence will be added in a later phase.

Retry and Dead Letter Queue (DLQ) policy:
    When a task fails (NACK), the QueueManager checks how many times
    it has been attempted. If attempts < max_retries (default 3), it
    goes back to the front of the queue for another try. If attempts
    >= max_retries, it's moved to the Dead Letter Queue permanently.

    This is the same pattern used by AWS SQS (maxReceiveCount), RabbitMQ
    (x-death header + x-dead-letter-exchange), and Celery (max_retries).

Key concepts:
    - Each "queue" is a named FIFO (First-In-First-Out) channel.
    - Every task gets a globally unique ID (UUID4) assigned at enqueue time.
    - When a worker dequeues a task, it moves from "pending" to "in_flight".
      The worker must ACK (acknowledge completion) or NACK (report failure)
      to resolve the task. This prevents tasks from silently disappearing
      if a worker crashes mid-execution.

Concurrency model:
    Even though asyncio is single-threaded, coroutines can interleave at
    every `await` point. Consider this scenario:
        1. Coroutine A calls dequeue(), checks the queue is non-empty
        2. Coroutine A hits an `await` (e.g., logging I/O)
        3. Coroutine B calls dequeue(), also sees the queue is non-empty
        4. Both pop the same item — data corruption!

    The asyncio.Lock prevents this by ensuring only one coroutine at a
    time can be inside a critical section (the code between `async with
    self._lock:`). It's the async equivalent of threading.Lock, but
    cooperative rather than OS-managed.
"""

import asyncio
import uuid
import time
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict

from broker.dead_letter import DeadLetterQueue

logger = logging.getLogger(__name__)


@dataclass
class Task:
    """
    Represents a single unit of work in the queue.

    Attributes:
        task_id:         Unique identifier (UUID4 string).
        queue_name:      Which named queue this task belongs to.
        payload:         The actual task data (arbitrary dict from the producer).
        status:          One of "pending", "in_flight", "done", "failed",
                         "dead_lettered".
        created_at:      Unix timestamp when the task was enqueued.
        assigned_worker: ID of the worker currently processing this task (if any).
        attempts:        How many times this task has been attempted.
        dead_lettered_at: Unix timestamp when moved to DLQ (if applicable).
    """
    task_id: str
    queue_name: str
    payload: dict
    status: str = "pending"
    created_at: float = field(default_factory=time.time)
    assigned_worker: Optional[str] = None
    attempts: int = 0


class QueueManager:
    """
    Manages multiple named in-memory FIFO queues.

    The QueueManager is the heart of the broker's data plane. It handles:
    - Creating queues on-the-fly when a producer first writes to them
    - Enqueuing tasks (PRODUCE command)
    - Dequeuing tasks and assigning them to a specific worker (CONSUME command)
    - Acknowledging successful completion (ACK command)
    - Handling task failure (NACK command) with retry counting:
        * If attempts < max_retries: re-enqueue at front of queue
        * If attempts >= max_retries: route to Dead Letter Queue (DLQ)

    Why asyncio.Lock?
        asyncio coroutines are cooperative — they yield control at every
        `await`. Without a lock, two coroutines could interleave their
        reads and writes to the same queue, causing duplicated or lost
        tasks. The lock guarantees atomicity of each queue operation.
    """

    def __init__(self, max_retries: int = 3):
        """
        Initialize the QueueManager.

        Args:
            max_retries: Maximum number of attempts before a task is
                         sent to the Dead Letter Queue. Default is 3.
                         After 3 failed attempts, the task is considered
                         permanently failed.
        """
        # Pending queues: queue_name -> deque of Task objects waiting to be consumed
        self._queues: Dict[str, deque] = {}

        # In-flight registry: task_id -> Task object currently being processed
        self._in_flight: Dict[str, Task] = {}

        # Completed archive: task_id -> Task (for inspection and debugging)
        self._completed: Dict[str, Task] = {}

        # Dead Letter Queue for permanently failed tasks
        self.dead_letter_queue = DeadLetterQueue()

        # Maximum retry attempts before DLQ routing
        self.max_retries = max_retries

        # Asyncio lock for safe concurrent access within the event loop
        self._lock = asyncio.Lock()

        # Stats counters
        self._total_enqueued: int = 0
        self._total_dequeued: int = 0
        self._total_acked: int = 0
        self._total_nacked: int = 0
        self._total_dead_lettered: int = 0

        logger.info(f"QueueManager initialized (in-memory mode, max_retries={max_retries})")

    def reset(self) -> None:
        """Completely reset the in-memory queues and stats."""
        self._queues.clear()
        self._in_flight.clear()
        self._completed.clear()
        if hasattr(self.dead_letter_queue, "_tasks"):
            self.dead_letter_queue._tasks.clear()
        elif hasattr(self.dead_letter_queue, "tasks"):
            self.dead_letter_queue.tasks.clear()
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_acked = 0
        self._total_nacked = 0
        self._total_dead_lettered = 0

    def _ensure_queue(self, queue_name: str):
        """
        Create the named queue if it doesn't already exist.

        This is called inside the lock, so it's safe from races.
        Using a helper keeps the enqueue/dequeue methods clean.
        """
        if queue_name not in self._queues:
            self._queues[queue_name] = deque()
            logger.info(f"Created new queue: '{queue_name}'")

    async def enqueue(self, queue_name: str, payload: dict) -> str:
        """
        Add a new task to the specified queue.

        Creates the queue automatically if it doesn't exist yet.
        Returns the unique task_id assigned to this task.

        Args:
            queue_name: Name of the target queue (e.g., "email_jobs").
            payload:    Arbitrary task data as a dictionary.

        Returns:
            The UUID4 string assigned to this task.
        """
        task_id = str(uuid.uuid4())
        task = Task(
            task_id=task_id,
            queue_name=queue_name,
            payload=payload,
        )

        async with self._lock:
            self._ensure_queue(queue_name)
            self._queues[queue_name].append(task)
            self._total_enqueued += 1
            depth = len(self._queues[queue_name])

        logger.info(
            f"Enqueued task {task_id[:8]}... to queue '{queue_name}' "
            f"(depth: {depth})"
        )
        return task_id

    async def enqueue_recovered(self, queue_name: str, task_id: str,
                                payload: dict, attempts: int = 0) -> str:
        """
        Re-enqueue a task recovered from persistent storage after a crash.

        Unlike enqueue(), this method preserves the original task_id and
        attempt count. This is critical for crash recovery because:
            - The task_id must match what's in the WAL store
            - The attempt count must be preserved so DLQ routing still
              works correctly (a task at attempt 2/3 shouldn't restart at 0)

        Called by BrokerServer.start() during the recovery phase, before
        the server begins accepting new connections.

        Args:
            queue_name: The queue this task belongs to.
            task_id:    The original UUID from the WAL store.
            payload:    The task's payload data.
            attempts:   How many times this task was previously attempted.

        Returns:
            The task_id (same as the input, for consistency with enqueue).
        """
        task = Task(
            task_id=task_id,
            queue_name=queue_name,
            payload=payload,
            status="pending",
        )
        task.attempts = attempts

        async with self._lock:
            self._ensure_queue(queue_name)
            self._queues[queue_name].append(task)
            self._total_enqueued += 1
            depth = len(self._queues[queue_name])

        logger.info(
            f"Recovered task {task_id[:8]}... to queue '{queue_name}' "
            f"(depth: {depth}, prior attempts: {attempts})"
        )
        return task_id

    async def dequeue(self, queue_name: str, worker_id: str) -> Optional[dict]:
        """
        Pull the next pending task from the specified queue.

        Moves the task from "pending" to "in_flight" status and assigns
        it to the given worker. The worker is responsible for sending
        an ACK or NACK once it finishes processing.

        Args:
            queue_name: Name of the queue to consume from.
            worker_id:  ID of the worker requesting a task.

        Returns:
            A dict with task_id, queue_name, payload, and attempts if a
            task was available, or None if the queue is empty.
        """
        async with self._lock:
            if queue_name == "*":
                target_q = None
                for q, dq in self._queues.items():
                    if dq:
                        target_q = q
                        break
                if not target_q:
                    return None
                queue_name = target_q
            else:
                self._ensure_queue(queue_name)
                if not self._queues[queue_name]:
                    return None

            task = self._queues[queue_name].popleft()
            task.status = "in_flight"
            task.assigned_worker = worker_id
            task.attempts += 1
            self._in_flight[task.task_id] = task
            self._total_dequeued += 1

        logger.info(
            f"Dequeued task {task.task_id[:8]}... from '{queue_name}' "
            f"-> worker '{worker_id}' (attempt #{task.attempts})"
        )
        return {
            "task_id": task.task_id,
            "queue_name": task.queue_name,
            "payload": task.payload,
            "attempts": task.attempts,
        }

    async def acknowledge(self, task_id: str) -> bool:
        """
        Mark a task as successfully completed (ACK).

        Removes the task from the in-flight registry and archives it
        as completed. Returns False if the task_id wasn't found in flight.

        Args:
            task_id: The UUID of the task to acknowledge.

        Returns:
            True if the task was found and acknowledged, False otherwise.
        """
        async with self._lock:
            task = self._in_flight.pop(task_id, None)
            if task is None:
                logger.warning(f"ACK for unknown in-flight task: {task_id[:8]}...")
                return False

            task.status = "done"
            self._completed[task_id] = task
            self._total_acked += 1

        logger.info(f"Task {task_id[:8]}... acknowledged (completed)")
        return True

    async def negative_acknowledge(self, task_id: str) -> Optional[dict]:
        """
        Mark a task as failed (NACK) and route it: retry or DLQ.

        This is the retry policy decision point. When a task fails:

            1. If attempts < max_retries (default 3):
               Re-enqueue at the FRONT of the queue for priority retry.
               The task keeps its attempt counter so the next failure
               brings it closer to the DLQ threshold.

            2. If attempts >= max_retries:
               Move to the Dead Letter Queue. The task is considered
               permanently failed and won't be retried automatically.
               An operator can manually inspect and retry DLQ tasks.

        Why handle retry logic here instead of in the broker?
            The QueueManager owns the task lifecycle and the queue data
            structures. Keeping the retry/DLQ decision here means:
            - The broker's NACK handler stays simple
            - The retry policy is in one place, easy to change
            - The DLQ is co-located with the queue it protects

        Args:
            task_id: The UUID of the task that failed.

        Returns:
            A dict with the outcome: {"action": "requeued"|"dead_lettered",
            "attempts": N, "task_id": ...}, or None if task not found.
        """
        async with self._lock:
            task = self._in_flight.pop(task_id, None)
            if task is None:
                logger.warning(f"NACK for unknown in-flight task: {task_id[:8]}...")
                return None

            self._total_nacked += 1

            if task.attempts >= self.max_retries:
                # Task has exhausted its retry budget -> Dead Letter Queue
                task.status = "dead_lettered"
                self._total_dead_lettered += 1

        # DLQ or requeue happens outside the lock (DLQ has its own lock)
        if task.status == "dead_lettered":
            await self.dead_letter_queue.add(task)
            logger.warning(
                f"Task {task_id[:8]}... moved to DLQ after "
                f"{task.attempts} attempts (queue: '{task.queue_name}')"
            )
            return {
                "action": "dead_lettered",
                "task_id": task.task_id,
                "attempts": task.attempts,
                "queue_name": task.queue_name,
            }
        else:
            # Still has retries left -> re-enqueue at front for priority retry
            task.status = "pending"
            task.assigned_worker = None
            async with self._lock:
                self._ensure_queue(task.queue_name)
                self._queues[task.queue_name].appendleft(task)

            logger.info(
                f"Task {task_id[:8]}... NACKed (attempt #{task.attempts}/{self.max_retries}), "
                f"re-queued at front of '{task.queue_name}'"
            )
            return {
                "action": "requeued",
                "task_id": task.task_id,
                "attempts": task.attempts,
                "max_retries": self.max_retries,
                "queue_name": task.queue_name,
            }

    async def requeue(self, task: Task, front: bool = True):
        """
        Return a previously failed task back to its queue for retry.

        This is called by the broker/retry handler after a NACK,
        if the task hasn't exceeded its maximum retry count.

        Args:
            task:  The Task object to re-enqueue.
            front: If True, insert at the front of the queue (retry ASAP).
                   If False, insert at the back (fair scheduling).
        """
        async with self._lock:
            self._ensure_queue(task.queue_name)
            task.status = "pending"
            task.assigned_worker = None

            if front:
                self._queues[task.queue_name].appendleft(task)
            else:
                self._queues[task.queue_name].append(task)

        position = "front" if front else "back"
        logger.info(
            f"Task {task.task_id[:8]}... re-queued at {position} of "
            f"'{task.queue_name}' (will be attempt #{task.attempts + 1})"
        )

    async def requeue_by_id(self, task_id: str) -> bool:
        """
        Requeue an in-flight task by its task_id.

        Used by the WorkerRegistry when a dead worker is evicted.
        The registry knows which task_ids were assigned to the dead worker
        (from its in_flight_tasks set) but doesn't hold the Task objects
        directly. This method bridges that gap.

        The task is placed at the FRONT of its queue so it gets picked up
        quickly by another healthy worker.

        Args:
            task_id: The UUID of the in-flight task to requeue.

        Returns:
            True if the task was found in-flight and requeued.
            False if the task_id wasn't in the in-flight registry
            (e.g., it was already ACKed by another path).
        """
        async with self._lock:
            task = self._in_flight.pop(task_id, None)
            if task is None:
                logger.warning(
                    f"requeue_by_id: task {task_id[:8]}... not found in-flight"
                )
                return False

            task.status = "pending"
            task.assigned_worker = None
            self._ensure_queue(task.queue_name)
            self._queues[task.queue_name].appendleft(task)

        logger.info(
            f"Task {task_id[:8]}... reassigned from dead worker, "
            f"re-queued at front of '{task.queue_name}'"
        )
        return True

    def queue_depth(self, queue_name: str) -> int:
        """Return the number of pending tasks in a specific queue."""
        return len(self._queues.get(queue_name, deque()))

    def get_all_queue_names(self) -> list:
        """Return a list of all known queue names."""
        return list(self._queues.keys())

    def in_flight_count(self) -> int:
        """Return the total number of tasks currently being processed."""
        return len(self._in_flight)

    def get_in_flight_tasks(self) -> Dict[str, Task]:
        """Return a copy of the in-flight task registry."""
        return dict(self._in_flight)

    def get_stats(self) -> dict:
        """
        Return a snapshot of queue manager statistics.

        Useful for the metrics/observability layer that we'll build
        in Week 5.
        """
        return {
            "total_enqueued": self._total_enqueued,
            "total_dequeued": self._total_dequeued,
            "total_acked": self._total_acked,
            "total_nacked": self._total_nacked,
            "total_dead_lettered": self._total_dead_lettered,
            "in_flight": len(self._in_flight),
            "max_retries": self.max_retries,
            "queues": {
                name: len(q) for name, q in self._queues.items()
            },
            "dlq": self.dead_letter_queue.get_stats(),
        }
