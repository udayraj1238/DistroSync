"""
DistroSync broker: asyncio TCP server implementing the 4-byte
length-prefixed JSON wire protocol. Handles PRODUCE, CONSUME,
HEARTBEAT, ACK, NACK, and REGISTER commands.
"""

import asyncio
import json
import signal
import logging
import sys

from broker.queue_manager import QueueManager, Task
from broker.worker_registry import WorkerRegistry
from broker.load_shedder import AdaptiveLoadShedder
from broker.metrics_collector import MetricsCollector
from broker.http_api import HTTPAPIServer
from persistence.wal_store import WALStore

# Configure logging to show timestamps, level, and module name
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class BrokerServer:
    """
    Asyncio-based TCP server that acts as the DistroSync message broker.

    Lifecycle:
        1. Create an instance: broker = BrokerServer(host, port)
        2. Call await broker.start() to begin accepting connections
        3. The server runs until interrupted (Ctrl+C) or shut down

    The server delegates data management to:
        - QueueManager: handles task enqueue/dequeue/ACK/NACK
        - WorkerRegistry: tracks which workers are connected and alive
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 5555,
                 db_path: str = None, http_port: int = 8000):
        """
        Initialize the broker server.

        Args:
            host:      The IP address to bind to. "0.0.0.0" means all interfaces.
            port:      The TCP port to listen on. 5555 is our default.
            db_path:   Path to the SQLite database for crash-safe persistence.
                       If None, persistence is disabled (in-memory only).
            http_port: The port for the HTTP dashboard/metrics API (default: 8000).
        """
        self.host = host
        self.port = port
        self.http_port = http_port
        self.queue_manager = QueueManager()
        self.worker_registry = WorkerRegistry(queue_manager=self.queue_manager)
        self.load_shedder = AdaptiveLoadShedder(
            queue_manager=self.queue_manager,
            worker_registry=self.worker_registry,
        )
        self.metrics = MetricsCollector()

        # Optional WAL-mode persistence layer
        self.wal_store: WALStore | None = None
        if db_path:
            self.wal_store = WALStore(db_path)
            logger.info(f"Persistence enabled: {db_path}")

        # HTTP API server for the dashboard (initialized in start())
        self.http_api: HTTPAPIServer | None = None

        self._server: asyncio.Server | None = None
        self._active_connections: int = 0

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """
        Handle a single client connection (producer or worker).

        This coroutine is spawned for each new TCP connection. It reads
        length-prefixed JSON messages in a loop until the client disconnects
        or an error occurs.

        Args:
            reader: asyncio stream to read data from the client.
            writer: asyncio stream to write data back to the client.
        """
        addr = writer.get_extra_info("peername")
        self._active_connections += 1
        logger.info(
            f"New connection from {addr} "
            f"(active connections: {self._active_connections})"
        )

        try:
            while True:
                # Step 1: Read the 4-byte length prefix
                # readexactly() blocks (yields) until exactly 4 bytes are available.
                # If the client disconnects mid-read, it raises IncompleteReadError.
                raw_len = await reader.readexactly(4)
                msg_len = int.from_bytes(raw_len, byteorder="big")

                # Sanity check: reject absurdly large messages (> 10 MB)
                if msg_len > 10 * 1024 * 1024:
                    logger.warning(
                        f"Client {addr} sent oversized message length: {msg_len}"
                    )
                    error_resp = {"status": "error", "reason": "Message too large"}
                    await self._send_response(writer, error_resp)
                    break

                # Step 2: Read exactly msg_len bytes of the JSON payload
                raw_msg = await reader.readexactly(msg_len)

                # Step 3: Decode and parse the JSON
                try:
                    message = json.loads(raw_msg.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"Invalid JSON from {addr}: {e}")
                    error_resp = {"status": "error", "reason": "Invalid JSON"}
                    await self._send_response(writer, error_resp)
                    continue

                # Step 4: Route the command to the appropriate handler
                response = await self._dispatch(message, writer)

                # Step 5: Send the response back to the client
                await self._send_response(writer, response)
                
                # Prevent event loop starvation if data is constantly available
                await asyncio.sleep(0)

        except asyncio.IncompleteReadError:
            # Client disconnected cleanly (or mid-message). This is normal.
            logger.info(f"Connection closed by {addr}")
        except ConnectionResetError:
            # Client forcefully terminated the connection
            logger.info(f"Connection reset by {addr}")
        except Exception as e:
            # Catch-all for unexpected errors so one bad connection
            # doesn't crash the entire broker
            logger.error(f"Unexpected error handling {addr}: {e}", exc_info=True)
        finally:
            self._active_connections -= 1
            writer.close()
            await writer.wait_closed()
            logger.info(
                f"Connection from {addr} cleaned up "
                f"(active connections: {self._active_connections})"
            )

    async def _send_response(self, writer: asyncio.StreamWriter, data: dict):
        """
        Send a length-prefixed JSON response to a client.

        This mirrors the framing used for incoming messages:
            [4-byte big-endian length][UTF-8 encoded JSON payload]

        drain() is called after writing to ensure the data is actually
        flushed to the OS send buffer. Without drain(), data might sit
        in asyncio's internal buffer indefinitely under backpressure.

        Args:
            writer: The asyncio stream writer for this connection.
            data:   The response dictionary to send.
        """
        encoded = json.dumps(data).encode("utf-8")
        length_prefix = len(encoded).to_bytes(4, byteorder="big")
        writer.write(length_prefix + encoded)
        await writer.drain()

    async def _dispatch(self, message: dict, writer: asyncio.StreamWriter) -> dict:
        """
        Route an incoming command to its handler.

        This is the central command router. Every message from a client
        must include a "command" field that determines what action to take.

        Args:
            message: The parsed JSON message from the client.
            writer:  The stream writer (in case a handler needs connection info).

        Returns:
            A response dictionary to send back to the client.
        """
        command = message.get("command")

        if not command:
            return {"status": "error", "reason": "Missing 'command' field"}

        # Command dispatch table
        handlers = {
            "PRODUCE": self._handle_produce,
            "CONSUME": self._handle_consume,
            "REGISTER": self._handle_register,
            "HEARTBEAT": self._handle_heartbeat,
            "ACK": self._handle_ack,
            "NACK": self._handle_nack,
            # Admin commands for DLQ management and observability
            "DLQ_LIST": self._handle_dlq_list,
            "DLQ_REPLAY": self._handle_dlq_replay,
            "DLQ_PURGE": self._handle_dlq_purge,
            "STATS": self._handle_stats,
            "METRICS": self._handle_metrics,
        }

        handler = handlers.get(command)
        if handler is None:
            return {"status": "error", "reason": f"Unknown command: {command}"}

        return await handler(message, writer)

    # ── Command Handlers ───────────────────────────────────────────────

    async def _handle_produce(self, message: dict, writer) -> dict:
        """
        Handle PRODUCE command — add a task to a queue (with rate limiting).

        The flow:
            1. Validate the message fields
            2. Ask the AdaptiveLoadShedder if this request is allowed
            3. If rate-limited: return retry_after hint to the producer
            4. If allowed: enqueue the task normally

        The load shedder checks the queue's token bucket, which refills
        at a rate adjusted by current queue depth and worker latency.

        Expected message format:
            {"command": "PRODUCE", "queue": "queue_name", "task": {...}}
        """
        queue_name = message.get("queue")
        task_payload = message.get("task")

        if not queue_name:
            return {"status": "error", "reason": "Missing 'queue' field"}
        if task_payload is None:
            return {"status": "error", "reason": "Missing 'task' field"}

        # Check the token bucket before enqueuing
        allowed, retry_after = await self.load_shedder.check_and_consume(queue_name)
        if not allowed:
            logger.debug(
                f"Rate limiting producer on queue '{queue_name}'. "
                f"Retry after {retry_after:.2f}s"
            )
            return {
                "status": "rate_limited",
                "retry_after_seconds": retry_after,
                "reason": "Queue overloaded. Please back off.",
            }

        task_id = await self.queue_manager.enqueue(queue_name, task_payload)

        # Record metrics
        self.metrics.record_produce(queue_name)

        # Persist the new task to WAL store for crash safety
        if self.wal_store:
            task_obj = self.queue_manager._queues.get(queue_name, [None])
            # Retrieve the Task we just enqueued (it's the last one added)
            for t in reversed(list(self.queue_manager._queues.get(queue_name, []))):
                if t.task_id == task_id:
                    self.wal_store.save_task(t)
                    break

        return {"status": "ok", "task_id": task_id}

    async def _handle_consume(self, message: dict, writer) -> dict:
        """
        Handle CONSUME command — pull the next task from a queue.

        Expected message format:
            {"command": "CONSUME", "queue": "queue_name", "worker_id": "w1"}

        Returns the task if available, or {"status": "empty"} if the queue
        has no pending tasks.
        """
        queue_name = message.get("queue")
        worker_id = message.get("worker_id")

        if not queue_name:
            return {"status": "error", "reason": "Missing 'queue' field"}
        if not worker_id:
            return {"status": "error", "reason": "Missing 'worker_id' field"}

        task = await self.queue_manager.dequeue(queue_name, worker_id)
        if task:
            # Track this task as assigned to this worker.
            # If the worker dies before ACKing, the eviction loop
            # will use this to requeue the task.
            await self.worker_registry.assign_task(worker_id, task["task_id"])

            # Record metrics — start tracking latency for this task
            self.metrics.record_consume(queue_name, task["task_id"])

            # Persist status change: pending -> in_flight
            if self.wal_store:
                self.wal_store.update_task_status(
                    task["task_id"], "in_flight",
                    attempts=task["attempts"],
                    assigned_worker=worker_id,
                )

            return {"status": "ok", "task": task}
        return {"status": "empty"}

    async def _handle_register(self, message: dict, writer) -> dict:
        """
        Handle REGISTER command — add a worker to the registry.

        Expected message format:
            {"command": "REGISTER", "worker_id": "w1", "queues": ["q1", "q2"]}
        """
        worker_id = message.get("worker_id")
        queues = message.get("queues", [])

        if not worker_id:
            return {"status": "error", "reason": "Missing 'worker_id' field"}

        addr = writer.get_extra_info("peername")
        is_new = await self.worker_registry.register(worker_id, address=addr, queues=queues, writer=writer)
        return {
            "status": "ok",
            "registered": is_new,
            "message": f"Worker '{worker_id}' registered",
        }

    async def _handle_heartbeat(self, message: dict, writer) -> dict:
        """
        Handle HEARTBEAT command — update worker liveness.

        Expected message format:
            {"command": "HEARTBEAT", "worker_id": "w1"}
        """
        worker_id = message.get("worker_id")

        if not worker_id:
            return {"status": "error", "reason": "Missing 'worker_id' field"}

        found = await self.worker_registry.record_heartbeat(worker_id)
        if found:
            return {"status": "ok"}
        return {"status": "error", "reason": f"Unknown worker: {worker_id}"}

    async def _handle_ack(self, message: dict, writer) -> dict:
        """
        Handle ACK command — mark a task as successfully completed.

        Expected message format:
            {"command": "ACK", "task_id": "uuid-here"}
        """
        task_id = message.get("task_id")

        if not task_id:
            return {"status": "error", "reason": "Missing 'task_id' field"}

        success = await self.queue_manager.acknowledge(task_id)
        if success:
            # Remove from worker's in-flight tracking so eviction
            # doesn't try to requeue an already-completed task
            worker_id = message.get("worker_id", "")
            await self.worker_registry.complete_task(worker_id, task_id)

            # Record metrics — compute processing latency
            queue_name = message.get("queue", "unknown")
            self.metrics.record_ack(queue_name, task_id)

            # Persist status change: in_flight -> done
            if self.wal_store:
                self.wal_store.update_task_status(task_id, "done")

            return {"status": "ok", "message": f"Task {task_id} acknowledged"}
        return {"status": "error", "reason": f"Task {task_id} not found in-flight"}

    async def _handle_nack(self, message: dict, writer) -> dict:
        """
        Handle NACK command — mark a task as failed.

        The QueueManager's negative_acknowledge() now handles the retry
        vs DLQ decision internally:
            - If attempts < max_retries: task is re-queued for another try
            - If attempts >= max_retries: task is moved to Dead Letter Queue

        The broker just needs to forward the result and clean up the
        worker's in-flight tracking.

        Expected message format:
            {"command": "NACK", "task_id": "uuid-here"}
        """
        task_id = message.get("task_id")

        if not task_id:
            return {"status": "error", "reason": "Missing 'task_id' field"}

        result = await self.queue_manager.negative_acknowledge(task_id)
        if result is None:
            return {"status": "error", "reason": f"Task {task_id} not found in-flight"}

        # Remove from worker's in-flight tracking
        worker_id = message.get("worker_id", "")
        await self.worker_registry.complete_task(worker_id, task_id)

        # Record metrics
        self.metrics.record_nack(
            result["queue_name"], task_id,
            dead_lettered=(result["action"] == "dead_lettered"),
        )

        # Persist the outcome: requeued or dead-lettered
        if self.wal_store:
            if result["action"] == "dead_lettered":
                # Build a minimal Task-like object for DLQ storage
                task_stub = Task(
                    task_id=task_id,
                    queue_name=result["queue_name"],
                    payload={},
                )
                task_stub.attempts = result["attempts"]
                # Try to get the real payload from the DLQ
                dlq_tasks = await self.queue_manager.dead_letter_queue.peek(limit=100)
                for dt in dlq_tasks:
                    if dt["task_id"] == task_id:
                        task_stub.payload = dt["payload"]
                        break
                self.wal_store.add_to_dlq(task_stub, "max retries exceeded")
            else:
                # Task was requeued — update status back to pending
                self.wal_store.update_task_status(
                    task_id, "pending",
                    attempts=result["attempts"],
                )

        return {
            "status": "ok",
            "action": result["action"],
            "attempts": result["attempts"],
            "message": (
                f"Task {task_id} moved to DLQ"
                if result["action"] == "dead_lettered"
                else f"Task {task_id} re-queued for retry"
            ),
        }

    # ── Admin Command Handlers ──────────────────────────────────────────────────

    async def _handle_dlq_list(self, message: dict, writer) -> dict:
        """
        Handle DLQ_LIST command — inspect tasks in the Dead Letter Queue.

        This is the operator's primary tool for understanding what failed
        and why. Returns task details including payload, error, and attempt
        count.

        Supports optional queue filtering:
            {"command": "DLQ_LIST"}                      → all DLQ tasks
            {"command": "DLQ_LIST", "queue": "emails"}   → emails only
            {"command": "DLQ_LIST", "limit": 5}          → top 5 tasks

        Data sources:
            - WAL store (persistent): primary source if persistence enabled
            - In-memory DLQ: fallback when running without persistence
        """
        queue_name = message.get("queue")
        limit = message.get("limit", 50)

        if self.wal_store:
            tasks = self.wal_store.get_dlq_tasks(
                queue_name=queue_name, limit=limit
            )
            # Parse payload JSON strings back to dicts for readability
            for t in tasks:
                if isinstance(t.get("payload"), str):
                    try:
                        t["payload"] = json.loads(t["payload"])
                    except (json.JSONDecodeError, TypeError):
                        pass
        else:
            tasks = await self.queue_manager.dead_letter_queue.peek(limit=limit)
            if queue_name:
                tasks = [t for t in tasks if t.get("queue_name") == queue_name]

        return {
            "status": "ok",
            "count": len(tasks),
            "tasks": tasks,
        }

    async def _handle_dlq_replay(self, message: dict, writer) -> dict:
        """
        Handle DLQ_REPLAY command — move a task from the DLQ back to its queue.

        This is used after an operator has fixed the underlying issue
        (e.g., patched a bug, restored a downstream service) and wants
        to give the task another chance.

        The task gets a fresh attempt counter so it has a full set of
        retries available.

        Expected message format:
            {"command": "DLQ_REPLAY", "task_id": "uuid-here"}

        Flow:
            1. Remove from DLQ (WAL store or in-memory)
            2. Re-enqueue into the original queue with fresh attempts
            3. Persist the new task to WAL if enabled
        """
        task_id = message.get("task_id")
        if not task_id:
            return {"status": "error", "reason": "Missing 'task_id' field"}

        if self.wal_store:
            row = self.wal_store.replay_dlq_task(task_id)
            if not row:
                return {
                    "status": "error",
                    "reason": f"Task {task_id} not found in DLQ",
                }
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            queue_name = row["queue_name"]

            # Re-enqueue with a fresh task (new attempts = 0)
            new_task_id = await self.queue_manager.enqueue(
                queue_name, payload
            )
            # Persist the new task
            for t in reversed(list(
                self.queue_manager._queues.get(queue_name, [])
            )):
                if t.task_id == new_task_id:
                    self.wal_store.save_task(t)
                    break
        else:
            # In-memory only: use the DLQ's retry method
            success = await self.queue_manager.dead_letter_queue.retry(
                task_id, self.queue_manager
            )
            if not success:
                return {
                    "status": "error",
                    "reason": f"Task {task_id} not found in DLQ",
                }
            new_task_id = task_id  # Same ID when using in-memory retry

        logger.info(
            f"DLQ replay: task {task_id[:8]}... replayed as {new_task_id[:8]}..."
        )
        return {
            "status": "ok",
            "action": "replayed",
            "original_task_id": task_id,
            "new_task_id": new_task_id,
        }

    async def _handle_dlq_purge(self, message: dict, writer) -> dict:
        """
        Handle DLQ_PURGE command — remove tasks from the Dead Letter Queue.

        Use with caution: purged tasks are permanently deleted.

        Expected message format:
            {"command": "DLQ_PURGE"}                     → purge all
            {"command": "DLQ_PURGE", "queue": "emails"}  → purge emails only
        """
        queue_name = message.get("queue")

        if self.wal_store:
            purged = self.wal_store.purge_dlq(queue_name)
        else:
            if queue_name:
                # In-memory DLQ doesn't support per-queue purge natively,
                # so we remove matching tasks individually
                dlq = self.queue_manager.dead_letter_queue
                to_remove = []
                async with dlq._lock:
                    for tid, task in dlq._tasks.items():
                        if task.queue_name == queue_name:
                            to_remove.append(tid)
                for tid in to_remove:
                    await dlq.remove(tid)
                purged = len(to_remove)
            else:
                purged = await self.queue_manager.dead_letter_queue.purge()

        logger.info(f"DLQ purge: {purged} tasks removed")
        return {
            "status": "ok",
            "action": "purged",
            "count": purged,
        }

    async def _handle_stats(self, message: dict, writer) -> dict:
        """
        Handle STATS command — return broker-wide statistics.

        Aggregates stats from the queue manager, worker registry,
        load shedder, and WAL store (if enabled).

        Expected message format:
            {"command": "STATS"}
        """
        stats = {
            "status": "ok",
            "queue_manager": self.queue_manager.get_stats(),
            "worker_registry": self.worker_registry.get_stats(),
            "load_shedder": self.load_shedder.get_stats(),
            "metrics": self.metrics.get_stats(),
            "active_connections": self._active_connections,
        }
        if self.wal_store:
            stats["wal_store"] = self.wal_store.get_stats()
        return stats

    async def _handle_metrics(self, message: dict, writer) -> dict:
        """
        Handle METRICS command -- return a full metrics snapshot.

        This is the same data served by the HTTP /metrics endpoint,
        but available over the TCP protocol for programmatic access.

        Expected message format:
            {"command": "METRICS"}
        """
        return await self._get_metrics_snapshot()

    async def _get_metrics_snapshot(self) -> dict:
        """
        Build a complete metrics snapshot for the dashboard.

        Combines data from the MetricsCollector (throughput, latency)
        with live data from the QueueManager (queue depths, in-flight)
        and WorkerRegistry (active workers, avg latency).
        """
        # Get queue names from both the queue manager and metrics collector
        qm_names = list(self.queue_manager._queues.keys())
        snapshot = self.metrics.snapshot(queue_names=qm_names)

        # Fill in live queue depths from the queue manager
        for name in qm_names:
            if name in snapshot["queues"]:
                snapshot["queues"][name]["depth"] = (
                    self.queue_manager.queue_depth(name)
                )

        # Add worker stats
        worker_stats = self.worker_registry.get_stats()
        
        active_workers_list = []
        for w in self.worker_registry.get_active_workers():
            active_workers_list.append({
                "id": w.worker_id,
                "latency_ms": 0,
                "status": w.status
            })

        snapshot["status"] = "ok"
        snapshot["workers"] = {
            "active": active_workers_list,
            "evicted_count": worker_stats.get("total_evictions", 0),
        }

        # Add DLQ count
        if self.wal_store:
            dlq_tasks = self.wal_store.get_dlq_tasks()
            snapshot["dlq"] = {"total_tasks": len(dlq_tasks)}
        else:
            dlq_tasks = await self.queue_manager.dead_letter_queue.peek(limit=1000)
            snapshot["dlq"] = {"total_tasks": len(dlq_tasks)}

        # Add in-flight count
        snapshot["broker"]["in_flight_tasks"] = (
            self.queue_manager.in_flight_count()
        )
        snapshot["broker"]["active_connections"] = self._active_connections
        snapshot["load_shedder"] = self.load_shedder.get_stats()
        snapshot["status"] = "ok"

        return snapshot

    async def _get_dlq_listing(self) -> dict:
        """Get DLQ listing for the HTTP API."""
        return await self._handle_dlq_list({"command": "DLQ_LIST"}, None)

    async def _get_stats_snapshot(self) -> dict:
        """Get stats snapshot for the HTTP API."""
        return await self._handle_stats({"command": "STATS"}, None)

    async def _crash_worker_handler(self) -> dict:
        import random
        import time
        workers = list(self.worker_registry._workers.values())
        active = [w for w in workers if w.status == "active"]
        if not active:
            return {"status": "error", "message": "No active workers to crash"}
        w = random.choice(active)
        
        # Actually sever the TCP connection to force a guaranteed, clean disconnect
        if w.writer and not w.writer.is_closing():
            try:
                w.writer.close()
            except Exception:
                pass
                
        w.last_heartbeat = time.time() - 10.0
        return {"status": "ok", "message": f"Worker {w.worker_id} crashed"}

    async def _replay_dlq_handler(self) -> dict:
        dlq_tasks = await self.queue_manager.dead_letter_queue.peek(limit=1000)
        replayed = 0
        for task in dlq_tasks:
            success = await self.queue_manager.dead_letter_queue.retry(task["task_id"], self.queue_manager)
            if success:
                replayed += 1
        return {"status": "ok", "message": f"Replayed {replayed} tasks"}

    async def _reset_handler(self) -> dict:
        """Reset broker state from scratch via dashboard."""
        async with self.queue_manager._lock:
            self.queue_manager.reset()
            if self.wal_store:
                self.wal_store.clear_all_data()
        self.metrics.reset()
        return {"status": "ok", "message": "System fully reset to scratch"}

    # ── Server Lifecycle ───────────────────────────────────────────────────────

    async def start(self):
        """
        Start the TCP server and serve forever.

        Startup sequence:
            1. Recover unfinished tasks from the WAL store (if persistence enabled)
            2. Start the TCP server
            3. Start the worker eviction loop
            4. Register signal handlers for graceful shutdown

        Crash recovery:
            Before accepting any connections, we reload all tasks that were
            pending or in-flight at the time of the last shutdown/crash.
            In-flight tasks are treated as pending because the worker that
            was processing them is no longer alive.
        """
        # ── Phase 1: Crash recovery from WAL store ────────────────
        if self.wal_store:
            pending_tasks = self.wal_store.load_pending_tasks()
            recovered = 0
            for row in pending_tasks:
                payload = json.loads(row["payload"])
                await self.queue_manager.enqueue_recovered(
                    queue_name=row["queue_name"],
                    task_id=row["task_id"],
                    payload=payload,
                    attempts=row["attempts"],
                )
                # Reset status to pending in the WAL (was in_flight before crash)
                self.wal_store.update_task_status(
                    row["task_id"], "pending",
                    attempts=row["attempts"],
                )
                recovered += 1
            if recovered:
                logger.info(
                    f"Crash recovery: {recovered} tasks restored from WAL store"
                )

        # ── Phase 2: Start the TCP server ─────────────────────────
        self._server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        # ── Phase 3: Start the HTTP dashboard/metrics API ─────────
        self.http_api = HTTPAPIServer(
            host=self.host,
            port=self.http_port,
            metrics_handler=self._get_metrics_snapshot,
            dlq_handler=self._get_dlq_listing,
            stats_handler=self._get_stats_snapshot,
            crash_worker_handler=self._crash_worker_handler,
            replay_dlq_handler=self._replay_dlq_handler,
            reset_handler=self._reset_handler,
        )
        await self.http_api.start()

        # Start the worker eviction loop as a background task.
        # This checks every 2s for workers that missed their heartbeat
        # deadline and reassigns their in-flight tasks.
        self.worker_registry._eviction_task = asyncio.create_task(
            self.worker_registry.start_eviction_loop()
        )

        # Register signal handlers for graceful shutdown (Unix only)
        # On Windows, signal handling is more limited but Ctrl+C still works
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        addrs = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"DistroSync Broker listening on {addrs}")
        logger.info(f"Dashboard available at http://{self.host}:{self.http_port}")
        logger.info("Press Ctrl+C to stop")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        """
        Gracefully shut down the server.

        Closes the listening socket so no new connections are accepted,
        then allows existing connections to finish. If persistence is
        enabled, the WAL store connection is closed cleanly.
        """
        if self._server:
            logger.info("Shutting down broker server...")
            # Stop the eviction loop before closing the server
            await self.worker_registry.stop_eviction_loop()

            # Stop the HTTP API server
            if self.http_api:
                await self.http_api.stop()

            self._server.close()
            await self._server.wait_closed()

            # Close the WAL store connection
            if self.wal_store:
                self.wal_store.close()
                logger.info("WAL store closed")

            logger.info("Broker server stopped")


def main():
    """Entry point to run the broker as a standalone process."""
    import argparse

    parser = argparse.ArgumentParser(description="DistroSync Broker Server")
    parser.add_argument(
        "--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=5555,
        help="TCP port to listen on (default: 5555)",
    )
    parser.add_argument(
        "--http-port", type=int, default=8000,
        help="HTTP port for dashboard/metrics (default: 8000)",
    )
    parser.add_argument(
        "--db-path", default=None,
        help="Path to SQLite database for persistence (default: in-memory)",
    )
    args = parser.parse_args()

    broker = BrokerServer(
        host=args.host,
        port=args.port,
        http_port=args.http_port,
        db_path=args.db_path,
    )

    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        logger.info("Broker interrupted by user")


if __name__ == "__main__":
    main()
