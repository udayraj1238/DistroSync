"""
Task Executor — Multiprocessing pool for CPU-bound task execution.

Why multiprocessing instead of threading?
    Python's Global Interpreter Lock (GIL) is a mutex that protects
    access to Python objects, preventing multiple native threads from
    executing Python bytecodes at once. This means:

        - Threading works for I/O-bound tasks (network, disk, sleep)
          because threads release the GIL while waiting for I/O
        - Threading DOES NOT work for CPU-bound tasks (math, parsing,
          image processing) because only one thread can execute Python
          code at a time, even on a 16-core machine

    ProcessPoolExecutor solves this by spawning real OS processes.
    Each process has its own Python interpreter and its own GIL.
    They truly run in parallel on separate CPU cores.

    This is the same tradeoff that:
        - Celery makes (uses multiprocessing by default)
        - Gunicorn makes (--workers flag = number of processes)
        - Node.js addresses with worker_threads and cluster module

How run_in_executor works:
    The asyncio event loop is single-threaded. If we call a CPU-heavy
    function directly, it blocks the event loop and nothing else can
    run (no heartbeats, no new task consumption).

    loop.run_in_executor() solves this:
        1. It submits the function to the ProcessPoolExecutor
        2. The function runs in a child process (separate GIL)
        3. The event loop is NOT blocked — heartbeats keep going
        4. When the child process finishes, the result is returned
           as a resolved Future back in the event loop

    This is crucial: the worker can still send heartbeats while
    a CPU task is running, because the event loop isn't frozen.

Serialization constraint:
    ProcessPoolExecutor uses pickle to send the function and its
    arguments to the child process. Pickle can only serialize:
        - Module-level functions (not lambdas, closures, or methods)
        - Basic Python types (dicts, lists, strings, numbers)

    That's why cpu_bound_task() is defined at module level, not
    inside a class. The payload must be a plain dict (no custom
    objects) — which is already guaranteed by our JSON protocol.
"""

import asyncio
import os
import functools
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Callable

from worker.base_worker import BaseWorker

logger = logging.getLogger(__name__)


# ── Module-Level Task Functions ────────────────────────────────────
# These MUST be at module level for pickle serialization.
# Each function runs in a SEPARATE PROCESS with its own GIL.


def cpu_bound_task(payload: dict) -> dict:
    """
    Default CPU-bound task handler. Runs in a child process.

    This is an example/default implementation. In production, you'd
    register different task handlers for different task types.

    This function has NO access to the broker, the event loop, or any
    shared memory with the parent process. It receives a plain dict
    and must return a plain dict.

    Args:
        payload: Task data dictionary from the producer.

    Returns:
        Result dictionary with computation output.
    """
    task_type = payload.get("type", "compute")
    pid = os.getpid()

    if task_type == "compute":
        # Simulated CPU-intensive computation
        data = payload.get("data", [])
        result = sum(x ** 2 for x in data)
        return {
            "result": result,
            "processed_count": len(data),
            "pid": pid,
        }

    elif task_type == "fibonacci":
        # Deliberately naive recursive fibonacci — actually CPU heavy
        n = payload.get("n", 10)
        def fib(x):
            if x <= 1:
                return x
            return fib(x - 1) + fib(x - 2)
        return {
            "result": fib(n),
            "n": n,
            "pid": pid,
        }

    elif task_type == "sort":
        # Sort a large list — memory-intensive CPU work
        data = payload.get("data", [])
        sorted_data = sorted(data)
        return {
            "sorted": sorted_data[:10],  # Return only first 10 to keep response small
            "length": len(sorted_data),
            "pid": pid,
        }

    else:
        # Echo — for testing
        return {
            "echoed": payload,
            "pid": pid,
        }


def _execute_task_func(func: Callable, payload: dict) -> dict:
    """
    Wrapper that calls a task function with its payload.

    Used by run_in_executor when a custom task function is provided.
    This wrapper exists because functools.partial can sometimes cause
    issues with pickling, and having a simple two-argument function
    at module level is the most reliable approach.
    """
    return func(payload)


class ProcessPoolWorker(BaseWorker):
    """
    Worker that executes tasks in a multiprocessing pool.

    Extends BaseWorker with a ProcessPoolExecutor for CPU-bound tasks.
    While a task is running in a child process, the asyncio event loop
    remains free — heartbeats continue, and the worker stays responsive
    to the broker.

    Usage:
        # Default: uses cpu_bound_task for all tasks
        worker = ProcessPoolWorker(
            queue_name="compute_queue",
            max_workers=4,
        )
        asyncio.run(worker.run())

        # Custom: provide your own task function
        def my_processor(payload: dict) -> dict:
            # Your CPU-heavy logic here
            return {"done": True}

        worker = ProcessPoolWorker(
            queue_name="compute_queue",
            task_func=my_processor,
        )

    Attributes:
        max_workers: Number of child processes in the pool.
                     Default 4 is good for most machines. Set to
                     os.cpu_count() for maximum CPU utilization.
        task_func:   Module-level function to execute in child processes.
                     Must accept a dict and return a dict.
    """

    def __init__(
        self,
        *args,
        max_workers: int = 4,
        task_func: Optional[Callable] = None,
        **kwargs,
    ):
        """
        Initialize the process pool worker.

        Args:
            max_workers: Number of child processes to spawn.
            task_func:   Custom module-level function for task execution.
                         If None, uses the default cpu_bound_task.
            *args, **kwargs: Passed to BaseWorker.__init__.
        """
        super().__init__(*args, **kwargs)
        self.max_workers = max_workers
        self._task_func = task_func or cpu_bound_task
        self._executor: Optional[ProcessPoolExecutor] = None

    async def execute(self, payload: dict) -> dict:
        """
        Execute a task in a child process via the process pool.

        This method:
            1. Gets the current event loop
            2. Submits the task function to the ProcessPoolExecutor
            3. Awaits the result without blocking the event loop
            4. Returns the result dict

        The key insight: while step 3 is awaiting, the event loop can
        run other coroutines (like the heartbeat loop). If we called
        the CPU function directly, the event loop would freeze.

        Args:
            payload: Task data from the producer.

        Returns:
            Result dict from the task function.

        Raises:
            Any exception raised by the task function in the child process.
            The exception is pickled back to the parent process.
        """
        loop = asyncio.get_event_loop()

        # run_in_executor submits the function to the process pool
        # and returns a Future that resolves when the child process finishes.
        # functools.partial creates a callable with pre-filled arguments.
        result = await loop.run_in_executor(
            self._executor,
            functools.partial(self._task_func, payload),
        )
        return result

    async def run(self):
        """
        Start the worker with a fresh process pool.

        Creates the ProcessPoolExecutor just before starting the
        consume loop. The pool is shut down in the finally block
        to clean up child processes.

        Why create the pool here instead of __init__?
            ProcessPoolExecutor spawns child processes. If we create
            it in __init__, those processes start immediately even if
            the worker hasn't connected to the broker yet. Creating
            it here ensures processes only spawn when we're ready.
        """
        # Create the process pool just before we need it
        self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        logger.info(
            f"Worker {self.worker_id[:8]}... process pool created "
            f"(max_workers={self.max_workers}, parent_pid={os.getpid()})"
        )

        try:
            await super().run()
        finally:
            # Shut down the process pool gracefully
            if self._executor:
                self._executor.shutdown(wait=True)
                logger.info(
                    f"Worker {self.worker_id[:8]}... process pool shut down"
                )
