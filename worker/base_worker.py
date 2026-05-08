"""
Base Worker — Connects to the broker, pulls tasks, and executes them.

This is the foundation class for all DistroSync workers. It handles:
    - TCP connection to the broker (same length-prefixed JSON protocol)
    - Worker registration (REGISTER command on connect)
    - Heartbeat loop (sends HEARTBEAT every 2 seconds to prove liveness)
    - Task consumption loop (polls for tasks, executes, sends ACK/NACK)

To create a custom worker, subclass BaseWorker and override execute():

    class EmailWorker(BaseWorker):
        async def execute(self, payload: dict) -> dict:
            send_email(payload["to"], payload["subject"], payload["body"])
            return {"sent": True}

    worker = EmailWorker(queue_name="email_queue")
    asyncio.run(worker.run())

Critical concurrency note — why we need a connection lock:
    The heartbeat loop and consume loop run as separate coroutines on
    the same event loop, and they BOTH use the same TCP connection
    (reader/writer pair). Without synchronization, this can happen:

        1. Consume loop sends CONSUME command, starts reading response
        2. Event loop yields to heartbeat coroutine (at an await point)
        3. Heartbeat sends HEARTBEAT command
        4. Broker responds to HEARTBEAT first
        5. Consume loop reads the HEARTBEAT response instead of its
           CONSUME response — data corruption!

    We solve this with an asyncio.Lock on all send+receive operations.
    The lock ensures that each send-then-receive pair is atomic — no
    other coroutine can interleave between the send and the read.
    This is the same problem that database connection pools solve.
"""

import asyncio
import json
import uuid
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BaseWorker:
    """
    Base class for DistroSync task workers.

    Lifecycle:
        1. connect()    — establishes TCP connection and registers with broker
        2. run()        — starts heartbeat + consume loop (runs forever)
        3. execute()    — override this to define your task processing logic
        4. shutdown()   — graceful cleanup (called automatically on interrupt)

    The worker runs two concurrent coroutines:
        - _heartbeat_loop: sends HEARTBEAT every 2s so broker knows we're alive
        - main consume loop in run(): polls for tasks, executes them, sends ACK/NACK
    """

    def __init__(
        self,
        queue_name: str,
        host: str = "localhost",
        port: int = 5555,
        poll_interval: float = 0.1,
        heartbeat_interval: float = 2.0,
        worker_id: Optional[str] = None,
    ):
        """
        Initialize the worker.

        Args:
            queue_name:         Name of the queue to consume tasks from.
            host:               Broker hostname.
            port:               Broker TCP port.
            poll_interval:      Seconds to wait between polls when queue is empty.
                                Lower = more responsive but more CPU/network usage.
                                0.1s (100ms) is a good default.
            heartbeat_interval: Seconds between heartbeat pings (default: 2.0).
                                The broker expects heartbeats every 2s and evicts
                                after 3 missed beats (6s timeout).
            worker_id:          Optional custom worker ID. If None, a UUID4 is
                                generated automatically.
        """
        self.queue_name = queue_name
        self.host = host
        self.port = port
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.worker_id = worker_id or str(uuid.uuid4())

        # TCP connection state
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected: bool = False

        # Connection lock — prevents heartbeat and consume from
        # interleaving their send/receive operations on the same socket
        self._conn_lock = asyncio.Lock()

        # Stats tracking
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._running: bool = False

    async def connect(self):
        """
        Connect to the broker and register this worker.

        Registration tells the broker:
            - Our worker_id (so it can track us)
            - Which queues we consume from (for future routing)

        After registration, the broker starts expecting heartbeats.
        If we miss 3 consecutive heartbeats (6 seconds), the broker
        will evict us and reassign our in-flight tasks.
        """
        logger.info(
            f"Worker {self.worker_id[:8]}... connecting to "
            f"{self.host}:{self.port}..."
        )
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        self._connected = True
        logger.info(f"Worker {self.worker_id[:8]}... TCP connection established")

        # Register with the broker
        response = await self._send({
            "command": "REGISTER",
            "worker_id": self.worker_id,
            "queues": [self.queue_name],
        })

        if response.get("status") != "ok":
            raise RuntimeError(
                f"Worker registration failed: {response.get('reason', 'unknown')}"
            )

        logger.info(
            f"Worker {self.worker_id[:8]}... registered for queue "
            f"'{self.queue_name}'"
        )

    async def _send(self, message: dict) -> dict:
        """
        Send a length-prefixed JSON message and read the response.

        IMPORTANT: This method acquires the connection lock to prevent
        the heartbeat loop and consume loop from interleaving their
        reads and writes on the same TCP socket.

        Without the lock, this race condition can occur:
            1. Consume sends CONSUME, starts waiting for response
            2. Heartbeat sends HEARTBEAT on the same socket
            3. Broker sends HEARTBEAT response
            4. Consume reads the HEARTBEAT response — wrong data!

        The lock makes each send+receive atomic.

        Args:
            message: Command dict to send to the broker.

        Returns:
            The broker's response as a dictionary.

        Raises:
            ConnectionError: If the connection has been lost.
        """
        if not self._connected or self._writer is None:
            raise ConnectionError("Not connected to broker")

        async with self._conn_lock:
            # Encode and send
            encoded = json.dumps(message).encode("utf-8")
            length_prefix = len(encoded).to_bytes(4, byteorder="big")
            self._writer.write(length_prefix + encoded)
            await self._writer.drain()

            # Read response
            raw_len = await self._reader.readexactly(4)
            msg_len = int.from_bytes(raw_len, byteorder="big")
            raw_resp = await self._reader.readexactly(msg_len)

            return json.loads(raw_resp.decode("utf-8"))

    async def _heartbeat_loop(self):
        """
        Send periodic heartbeat pings to the broker.

        This runs as a background coroutine alongside the consume loop.
        The broker uses heartbeats to detect dead workers:
            - Worker sends HEARTBEAT every 2 seconds
            - If broker misses 3 consecutive heartbeats, it marks the
              worker as dead and reassigns its in-flight tasks

        Why heartbeats instead of relying on TCP keepalive?
            TCP keepalive is OS-level and typically has very long timeouts
            (2 hours by default on Linux). Application-level heartbeats
            give us much finer control — we can detect failures in 6
            seconds instead of 2 hours. This is how Redis Sentinel,
            ZooKeeper, and Kafka all detect node failures.
        """
        logger.info(
            f"Worker {self.worker_id[:8]}... heartbeat loop started "
            f"(interval: {self.heartbeat_interval}s)"
        )
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            if not self._running:
                break
            try:
                response = await self._send({
                    "command": "HEARTBEAT",
                    "worker_id": self.worker_id,
                })
                if response.get("status") != "ok":
                    logger.warning(
                        f"Heartbeat response unexpected: {response}"
                    )
            except asyncio.CancelledError:
                raise  # Let cancellation propagate
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")

    async def execute(self, payload: dict) -> dict:
        """
        Execute a task. OVERRIDE THIS IN YOUR SUBCLASS.

        This is where your actual business logic goes. The payload
        is whatever the producer sent when it created the task.

        Args:
            payload: The task data dictionary from the producer.

        Returns:
            A result dictionary (logged for debugging, not sent to broker).

        Raises:
            Any exception — the worker will catch it and send a NACK.
        """
        raise NotImplementedError(
            "Subclasses must implement execute(). "
            "This method receives the task payload and should return a result dict."
        )

    async def run(self):
        """
        Main worker loop — connect, then poll/execute/ACK forever.

        Flow for each iteration:
            1. Send CONSUME to broker to request the next task
            2. If queue is empty: sleep for poll_interval, then retry
            3. If task received: call execute(payload)
            4. If execute succeeds: send ACK (task completed)
            5. If execute raises: send NACK (task failed, will be retried)
            6. Go to step 1

        The heartbeat loop runs concurrently as a separate coroutine.
        Both use the _conn_lock to safely share the TCP connection.
        """
        await self.connect()
        self._running = True

        # Start heartbeat as a background task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        logger.info(
            f"Worker {self.worker_id[:8]}... consuming from "
            f"'{self.queue_name}' (poll interval: {self.poll_interval}s)"
        )

        try:
            while self._running:
                # Step 1: Request the next task from the broker
                response = await self._send({
                    "command": "CONSUME",
                    "queue": self.queue_name,
                    "worker_id": self.worker_id,
                })

                # Step 2: If queue is empty, wait and retry
                if response["status"] == "empty":
                    await asyncio.sleep(self.poll_interval)
                    continue

                if response["status"] != "ok":
                    logger.error(f"Unexpected consume response: {response}")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # Step 3: We got a task — extract it
                task = response["task"]
                task_id = task["task_id"]
                attempt = task.get("attempts", 1)

                logger.info(
                    f"Worker {self.worker_id[:8]}... received task "
                    f"{task_id[:8]}... (attempt #{attempt})"
                )

                # Step 4-5: Execute and send ACK or NACK
                try:
                    result = await self.execute(task["payload"])
                    await self._send({
                        "command": "ACK",
                        "task_id": task_id,
                    })
                    self._tasks_completed += 1
                    logger.info(
                        f"Task {task_id[:8]}... completed successfully "
                        f"(result: {result})"
                    )
                except Exception as e:
                    logger.error(
                        f"Task {task_id[:8]}... execution failed: {e}",
                        exc_info=True,
                    )
                    await self._send({
                        "command": "NACK",
                        "task_id": task_id,
                        "error": str(e),
                    })
                    self._tasks_failed += 1

        except asyncio.CancelledError:
            logger.info(f"Worker {self.worker_id[:8]}... cancelled")
        except ConnectionError as e:
            logger.error(f"Worker {self.worker_id[:8]}... lost connection: {e}")
        except Exception as e:
            logger.error(
                f"Worker {self.worker_id[:8]}... unexpected error: {e}",
                exc_info=True,
            )
        finally:
            self._running = False
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            await self._close()

    async def shutdown(self):
        """
        Signal the worker to stop gracefully.

        Sets _running to False, which causes the consume loop to exit
        on its next iteration. The heartbeat task is then cancelled
        and the connection is closed.
        """
        logger.info(f"Worker {self.worker_id[:8]}... shutting down...")
        self._running = False

    async def _close(self):
        """Close the TCP connection to the broker."""
        if self._writer and self._connected:
            self._writer.close()
            await self._writer.wait_closed()
            self._connected = False
            logger.info(f"Worker {self.worker_id[:8]}... disconnected from broker")

    def get_stats(self) -> dict:
        """Return worker statistics for monitoring."""
        return {
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "connected": self._connected,
            "running": self._running,
            "tasks_completed": self._tasks_completed,
            "tasks_failed": self._tasks_failed,
        }

# added heartbeat loop
