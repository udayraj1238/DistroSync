"""
Producer Client — Submits tasks to the DistroSync broker over TCP.

This is the client library that producers use to connect to the broker
and submit tasks into named queues. It handles:

    - TCP connection management (connect, reconnect, close)
    - Length-prefixed JSON framing (matching the broker's protocol)
    - Automatic retry with exponential backoff when the broker is
      shedding load (responds with "rate_limited" status)
    - Batch submission for high-throughput scenarios

The producer is designed to be resilient. When the broker is overloaded
and starts rejecting tasks (Week 3's adaptive load shedding), the
producer doesn't just crash — it backs off and retries, giving the
broker time to drain its queues.

Usage:
    async with ProducerClient("localhost", 5555) as producer:
        task_id = await producer.produce("email_queue", {
            "to": "user@example.com",
            "subject": "Hello",
        })
        print(f"Task submitted: {task_id}")

    # Or without context manager:
    producer = ProducerClient()
    await producer.connect()
    task_id = await producer.produce("email_queue", {"to": "user@example.com"})
    await producer.close()
"""

import asyncio
import json
import logging
from typing import Optional

from producer.backoff import ExponentialBackoff

logger = logging.getLogger(__name__)


class ProducerClient:
    """
    Async TCP client for submitting tasks to the DistroSync broker.

    The client maintains a persistent TCP connection to the broker.
    This is more efficient than opening a new connection for each task,
    because TCP connection setup involves a 3-way handshake (SYN,
    SYN-ACK, ACK) which adds latency.

    The client uses the same length-prefixed JSON protocol as the broker:
        [4 bytes: message length][JSON payload]
    """

    def __init__(self, host: str = "localhost", port: int = 5555):
        """
        Initialize the producer client.

        Does NOT connect immediately — call connect() or use as an
        async context manager.

        Args:
            host: Broker hostname or IP address.
            port: Broker TCP port.
        """
        self.host = host
        self.port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected: bool = False

    async def connect(self):
        """
        Establish a TCP connection to the broker.

        This opens an asyncio stream connection. Under the hood,
        asyncio.open_connection() creates a socket, performs the TCP
        handshake, and wraps the socket in StreamReader/StreamWriter
        objects for convenient async I/O.

        Raises:
            ConnectionRefusedError: If the broker isn't running.
            OSError: For other network errors.
        """
        logger.info(f"Connecting to broker at {self.host}:{self.port}...")
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        self._connected = True
        logger.info(f"Connected to broker at {self.host}:{self.port}")

    async def _send(self, message: dict) -> dict:
        """
        Send a length-prefixed JSON message and wait for the response.

        This is the core wire protocol method. It:
            1. JSON-encodes the message
            2. Prepends the 4-byte length prefix
            3. Writes both to the TCP stream
            4. Flushes the write buffer (drain)
            5. Reads the 4-byte response length
            6. Reads exactly that many bytes of response JSON
            7. Decodes and returns the response dict

        Args:
            message: The command dict to send to the broker.

        Returns:
            The broker's response as a dictionary.

        Raises:
            ConnectionError: If the connection has been lost.
        """
        if not self._connected or self._writer is None:
            raise ConnectionError(
                "Not connected to broker. Call connect() first."
            )

        # Encode and send with length prefix
        encoded = json.dumps(message).encode("utf-8")
        length_prefix = len(encoded).to_bytes(4, byteorder="big")
        self._writer.write(length_prefix + encoded)
        await self._writer.drain()

        # Read the response (also length-prefixed)
        raw_len = await self._reader.readexactly(4)
        msg_len = int.from_bytes(raw_len, byteorder="big")
        raw_resp = await self._reader.readexactly(msg_len)

        return json.loads(raw_resp.decode("utf-8"))

    async def produce(
        self,
        queue_name: str,
        payload: dict,
        max_retries: int = 10,
    ) -> str:
        """
        Submit a task to the broker, with automatic retry on load shedding.

        The produce flow:
            1. Send PRODUCE command to the broker
            2. If status="ok": task accepted, return the task_id
            3. If status="rate_limited": broker is overloaded.
               Use exponential backoff to wait, then retry.
            4. If status="error": something unexpected, raise an exception
            5. If retries exhausted: raise RuntimeError

        The exponential backoff with jitter prevents the "thundering herd"
        problem: if all producers retry at the exact same time, they'd
        create an even worse load spike. Jitter spreads retries out randomly.

        Args:
            queue_name:  Name of the target queue.
            payload:     Task data as a dictionary.
            max_retries: Maximum number of retry attempts on rate limiting.

        Returns:
            The task_id assigned by the broker (UUID4 string).

        Raises:
            RuntimeError: If the broker returns an error or retries exhausted.
            ConnectionError: If the connection is lost.
        """
        backoff = ExponentialBackoff(max_attempts=max_retries)

        while True:
            response = await self._send({
                "command": "PRODUCE",
                "queue": queue_name,
                "task": payload,
            })

            if response["status"] == "ok":
                task_id = response["task_id"]
                # Reset backoff on success (for the next produce call)
                backoff.reset()
                logger.info(
                    f"Task {task_id[:8]}... submitted to queue '{queue_name}'"
                )
                return task_id

            elif response["status"] == "rate_limited":
                # The broker is shedding load (Week 3 feature).
                # Respect the retry_after hint if provided.
                retry_after = response.get("retry_after_seconds", 1.0)
                wait_time = backoff.next_wait(retry_after=retry_after)
                logger.warning(
                    f"Rate limited by broker. Backing off for {wait_time:.2f}s "
                    f"(attempt #{backoff.attempt}/{backoff.max_attempts})"
                )
                await asyncio.sleep(wait_time)

            else:
                # Unexpected error from broker
                reason = response.get("reason", "Unknown error")
                raise RuntimeError(
                    f"Broker rejected task: {reason} (response: {response})"
                )

    async def produce_batch(
        self,
        queue_name: str,
        payloads: list[dict],
    ) -> list[str]:
        """
        Submit multiple tasks to the same queue.

        This is a convenience method that submits tasks one at a time
        over the persistent connection. Since TCP is a stream, pipelining
        these requests over a single connection is much faster than
        opening a new connection for each task.

        Args:
            queue_name: Name of the target queue.
            payloads:   List of task data dictionaries.

        Returns:
            List of task_id strings, in the same order as the payloads.
        """
        task_ids = []
        for i, payload in enumerate(payloads):
            task_id = await self.produce(queue_name, payload)
            task_ids.append(task_id)

        logger.info(
            f"Batch submitted {len(task_ids)} tasks to queue '{queue_name}'"
        )
        return task_ids

    async def close(self):
        """
        Close the TCP connection to the broker.

        Always call this when done (or use the async context manager).
        Not closing the connection leaves a dangling socket that the
        OS has to clean up via TCP timeout, wasting resources on both
        the client and broker side.
        """
        if self._writer and self._connected:
            self._writer.close()
            await self._writer.wait_closed()
            self._connected = False
            logger.info("Disconnected from broker")

    # ── Async Context Manager ──────────────────────────────────────

    async def __aenter__(self):
        """
        Support for `async with ProducerClient() as producer:` syntax.

        Automatically connects when entering the context.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Automatically closes the connection when exiting the context.

        This ensures cleanup even if an exception occurs inside the
        `async with` block.
        """
        await self.close()

    @property
    def is_connected(self) -> bool:
        """Check if the client is currently connected to the broker."""
        return self._connected
