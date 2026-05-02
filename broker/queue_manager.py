"""
Queue Manager — In-memory task queue with named queue support.

This module manages multiple named queues. Producers enqueue tasks
into a specific queue by name, and workers dequeue from them.

For now (Week 1), everything lives in memory. Crash safety via SQLite
WAL-mode persistence will be added in a later phase.

Key concepts:
    - Each "queue" is a named FIFO (First-In-First-Out) channel.
    - Every task gets a globally unique ID (UUID4) assigned at enqueue time.
    - When a worker dequeues a task, it moves from "pending" to "in_flight".
      The worker must ACK (acknowledge completion) or NACK (report failure)
      to resolve the task. This prevents tasks from silently disappearing
      if a worker crashes mid-execution.
"""

import uuid
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Task:
    """
    Represents a single unit of work in the queue.

    Attributes:
        task_id:     Unique identifier (UUID4 string).
        queue_name:  Which named queue this task belongs to.
        payload:     The actual task data (arbitrary dict from the producer).
        status:      One of "pending", "in_flight", "completed", "failed".
        created_at:  Unix timestamp when the task was enqueued.
        worker_id:   ID of the worker currently processing this task (if any).
        attempts:    How many times this task has been attempted.
    """
    task_id: str
    queue_name: str
    payload: dict
    status: str = "pending"
    created_at: float = field(default_factory=time.time)
    worker_id: Optional[str] = None
    attempts: int = 0


class QueueManager:
    """
    Manages multiple named in-memory FIFO queues.

    The QueueManager is the heart of the broker's data plane. It handles:
    - Creating queues on-the-fly when a producer first writes to them
    - Enqueuing tasks (PRODUCE command)
    - Dequeuing tasks and assigning them to a specific worker (CONSUME command)
    - Acknowledging successful completion (ACK command)
    - Handling task failure and returning tasks to the queue (NACK command)

    Thread safety note:
        In Week 1, the broker runs on a single asyncio event loop, so
        all queue operations are effectively single-threaded (no two
        coroutines run concurrently within the loop). We don't need
        locks here. If we later add multiprocessing to the broker
        itself, we would need to revisit this.
    """

    def __init__(self):
        # Pending queues: queue_name -> deque of Task objects waiting to be consumed
        self._queues: dict[str, deque[Task]] = defaultdict(deque)

        # In-flight registry: task_id -> Task object currently being processed
        self._in_flight: dict[str, Task] = {}

        # Completed/failed archive: task_id -> Task (for inspection and debugging)
        self._completed: dict[str, Task] = {}

        # Stats counters
        self._total_enqueued: int = 0
        self._total_dequeued: int = 0
        self._total_acked: int = 0
        self._total_nacked: int = 0

        logger.info("QueueManager initialized (in-memory mode)")

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
        self._queues[queue_name].append(task)
        self._total_enqueued += 1

        logger.info(
            f"Enqueued task {task_id[:8]}... to queue '{queue_name}' "
            f"(depth: {len(self._queues[queue_name])})"
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
            A dict with task_id and payload if a task was available,
            or None if the queue is empty.
        """
        queue = self._queues.get(queue_name)
        if not queue:
            return None

        task = queue.popleft()
        task.status = "in_flight"
        task.worker_id = worker_id
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
        task = self._in_flight.pop(task_id, None)
        if task is None:
            logger.warning(f"ACK for unknown in-flight task: {task_id[:8]}...")
            return False

        task.status = "completed"
        self._completed[task_id] = task
        self._total_acked += 1

        logger.info(f"Task {task_id[:8]}... acknowledged (completed)")
        return True

    async def negative_acknowledge(self, task_id: str) -> bool:
        """
        Mark a task as failed (NACK) and return it to its queue for retry.

        The task goes back to the front of its queue so it gets
        re-attempted before newer tasks. Its attempt counter is preserved
        so we can track how many times it's been retried.

        Args:
            task_id: The UUID of the task that failed.

        Returns:
            True if the task was found and re-queued, False otherwise.
        """
        task = self._in_flight.pop(task_id, None)
        if task is None:
            logger.warning(f"NACK for unknown in-flight task: {task_id[:8]}...")
            return False

        task.status = "pending"
        task.worker_id = None
        # Re-insert at the front of the queue so it gets retried next
        self._queues[task.queue_name].appendleft(task)
        self._total_nacked += 1

        logger.info(
            f"Task {task_id[:8]}... NACKed, returned to queue "
            f"'{task.queue_name}' (attempt #{task.attempts})"
        )
        return True

    def get_queue_depth(self, queue_name: str) -> int:
        """Return the number of pending tasks in a specific queue."""
        return len(self._queues.get(queue_name, []))

    def get_all_queue_names(self) -> list[str]:
        """Return a list of all known queue names."""
        return list(self._queues.keys())

    def get_in_flight_count(self) -> int:
        """Return the total number of tasks currently being processed."""
        return len(self._in_flight)

    def get_stats(self) -> dict:
        """
        Return a snapshot of queue manager statistics.

        Useful for the metrics/observability layer.
        """
        return {
            "total_enqueued": self._total_enqueued,
            "total_dequeued": self._total_dequeued,
            "total_acked": self._total_acked,
            "total_nacked": self._total_nacked,
            "in_flight": len(self._in_flight),
            "queues": {
                name: len(q) for name, q in self._queues.items()
            },
        }
