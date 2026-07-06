"""
Dead Letter Queue — Storage for tasks that have permanently failed.

When a task fails more times than the maximum retry count (default: 3),
it's "dead" — the system gives up on automatic retries and moves it
to the Dead Letter Queue (DLQ) for manual inspection.

A DLQ is used instead of just dropping failed tasks because in production systems, permanently failed tasks often indicate bugs
    in the task handler, bad input data, or downstream service outages.
    You want to keep these tasks around so you can:
        - Inspect why they failed (payload, error message, attempt count)
        - Fix the bug in the handler
        - Replay the tasks after the fix

    This is the same pattern used by:
        - AWS SQS Dead-Letter Queues
        - RabbitMQ's x-dead-letter-exchange
        - Kafka's error topics
        - Celery's task_reject_on_worker_lost + custom error handlers

DLQ operations:
    - add():    Move a permanently failed task into the DLQ
    - peek():   Inspect tasks in the DLQ without removing them
    - remove():  Remove a specific task (after manual processing)
    - retry():  Move a task from DLQ back to its original queue
    - purge():  Clear all tasks from the DLQ (use with caution)
    - count():  Number of tasks currently in the DLQ
"""

import asyncio
import time
import logging
from collections import OrderedDict
from typing import Optional, List

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    In-memory Dead Letter Queue for permanently failed tasks.

    Tasks land here after exhausting their retry budget. Each task
    retains its full history: original payload, queue name, attempt
    count, and the timestamp when it was dead-lettered.

    The DLQ is implemented as an OrderedDict keyed by task_id so
    that tasks maintain insertion order (useful for FIFO inspection)
    and can be looked up by ID in O(1) for individual operations.

    Thread safety: protected by an asyncio.Lock since the broker's
    event loop, the NACK handler, and any future admin API all
    access the DLQ concurrently.
    """

    def __init__(self):
        # OrderedDict preserves insertion order AND allows O(1) lookup
        self._tasks: OrderedDict = OrderedDict()
        self._lock = asyncio.Lock()

        # Stats
        self._total_received: int = 0
        self._total_retried: int = 0
        self._total_purged: int = 0

        logger.info("DeadLetterQueue initialized")

    async def add(self, task) -> None:
        """
        Add a permanently failed task to the DLQ.

        Called by QueueManager.negative_acknowledge() when a task has
        exceeded its maximum retry count.

        Args:
            task: The Task object (from queue_manager.Task dataclass).
        """
        async with self._lock:
            task.status = "dead_lettered"
            task.dead_lettered_at = time.time()
            self._tasks[task.task_id] = task
            self._total_received += 1

        logger.warning(
            f"Task {task.task_id[:8]}... moved to DLQ "
            f"(queue: '{task.queue_name}', attempts: {task.attempts})"
        )

    async def peek(self, limit: int = 10) -> List[dict]:
        """
        Inspect tasks in the DLQ without removing them.

        Returns a list of task summaries in insertion order (oldest first).

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of task summary dicts.
        """
        async with self._lock:
            results = []
            for task_id, task in list(self._tasks.items())[:limit]:
                results.append({
                    "task_id": task.task_id,
                    "queue_name": task.queue_name,
                    "payload": task.payload,
                    "attempts": task.attempts,
                    "status": task.status,
                    "dead_lettered_at": getattr(task, "dead_lettered_at", None),
                })
            return results

    async def remove(self, task_id: str) -> bool:
        """
        Remove a specific task from the DLQ.

        Used after manual inspection/processing of a failed task.

        Args:
            task_id: The UUID of the task to remove.

        Returns:
            True if the task was found and removed, False otherwise.
        """
        async with self._lock:
            task = self._tasks.pop(task_id, None)

        if task is None:
            logger.warning(f"DLQ remove: task {task_id[:8]}... not found")
            return False

        logger.info(f"Task {task_id[:8]}... removed from DLQ")
        return True

    async def retry(self, task_id: str, queue_manager) -> bool:
        """
        Move a task from the DLQ back to its original queue for retry.

        This is used when an operator has fixed the underlying issue
        (e.g., patched a bug, restored a downstream service) and wants
        to give the task another chance.

        The task's attempt counter is reset to 0 so it gets a fresh
        set of retries.

        Args:
            task_id:       The UUID of the task to retry.
            queue_manager: Reference to the QueueManager for re-enqueuing.

        Returns:
            True if the task was found and re-enqueued, False otherwise.
        """
        async with self._lock:
            task = self._tasks.pop(task_id, None)

        if task is None:
            logger.warning(f"DLQ retry: task {task_id[:8]}... not found")
            return False

        # Reset the task for a fresh retry cycle
        task.status = "pending"
        task.attempts = 0
        task.assigned_worker = None

        await queue_manager.requeue(task, front=False)
        self._total_retried += 1

        logger.info(
            f"Task {task_id[:8]}... moved from DLQ back to "
            f"queue '{task.queue_name}' for retry"
        )
        return True

    async def purge(self) -> int:
        """
        Remove ALL tasks from the DLQ.

        Use with caution — this permanently discards all dead-lettered tasks.

        Returns:
            The number of tasks that were purged.
        """
        async with self._lock:
            count = len(self._tasks)
            self._tasks.clear()
            self._total_purged += count

        if count > 0:
            logger.warning(f"DLQ purged: {count} tasks removed")
        return count

    def count(self) -> int:
        """Return the number of tasks currently in the DLQ."""
        return len(self._tasks)

    def get_stats(self) -> dict:
        """Return DLQ statistics."""
        return {
            "current_size": len(self._tasks),
            "total_received": self._total_received,
            "total_retried": self._total_retried,
            "total_purged": self._total_purged,
        }
