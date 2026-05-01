"""
Broker Server — The core TCP server for DistroSync.

This is the central hub of the entire system. It listens on a TCP port,
accepts connections from producers and workers, and routes commands
to the appropriate handlers.

How TCP communication works here:
    TCP is a *stream* protocol, not a *message* protocol. That means if
    you send "hello" and then "world" in two separate send() calls, the
    receiver might read "helloworld" as one chunk, or "hel" and "loworld"
    as two chunks. There are no message boundaries in TCP.

    To fix this, we use LENGTH-PREFIXED FRAMING:
        [4 bytes: message length as big-endian integer][JSON payload bytes]

    The sender first writes the length of the JSON message (as a 4-byte
    big-endian integer), then writes the JSON itself. The receiver reads
    exactly 4 bytes to learn the message length, then reads exactly that
    many bytes to get the complete message. This is the same approach
    used by Redis (RESP), HTTP/2 frames, gRPC, and Kafka's binary protocol.

Why asyncio?
    The broker needs to handle many simultaneous connections (producers
    and workers). Using one thread per connection would waste memory and
    hit OS thread limits. asyncio uses a single-threaded event loop that
    multiplexes I/O across all connections using non-blocking sockets.
    When one connection is waiting for data, the event loop serves others.
    This is the same model that Node.js uses, and it's how nginx handles
    10,000+ concurrent connections on a single core.

Command protocol:
    All messages are JSON objects with a "command" field:
        {"command": "PRODUCE", "queue": "emails", "task": {"to": "x@y.com"}}
        {"command": "CONSUME", "queue": "emails", "worker_id": "w1"}
        {"command": "HEARTBEAT", "worker_id": "w1"}
        {"command": "ACK", "task_id": "uuid-here"}
        {"command": "NACK", "task_id": "uuid-here"}
        {"command": "REGISTER", "worker_id": "w1", "queues": ["emails"]}
"""

import asyncio
import json
import signal
import logging
import sys

from broker.queue_manager import QueueManager
from broker.worker_registry import WorkerRegistry

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

    def __init__(self, host: str = "0.0.0.0", port: int = 5555):
        """
        Initialize the broker server.

        Args:
            host: The IP address to bind to. "0.0.0.0" means all interfaces.
            port: The TCP port to listen on. 5555 is our default.
        """
        self.host = host
        self.port = port
        self.queue_manager = QueueManager()
        self.worker_registry = WorkerRegistry()
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
        }

        handler = handlers.get(command)
        if handler is None:
            return {"status": "error", "reason": f"Unknown command: {command}"}

        return await handler(message, writer)

    # ── Command Handlers ───────────────────────────────────────────────

    async def _handle_produce(self, message: dict, writer) -> dict:
        """
        Handle PRODUCE command — add a task to a queue.

        Expected message format:
            {"command": "PRODUCE", "queue": "queue_name", "task": {...}}

        The "task" field is the actual payload the worker will receive.
        """
        queue_name = message.get("queue")
        task_payload = message.get("task")

        if not queue_name:
            return {"status": "error", "reason": "Missing 'queue' field"}
        if task_payload is None:
            return {"status": "error", "reason": "Missing 'task' field"}

        task_id = await self.queue_manager.enqueue(queue_name, task_payload)
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
        is_new = self.worker_registry.register(worker_id, address=addr, queues=queues)
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

        found = self.worker_registry.record_heartbeat(worker_id)
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
            return {"status": "ok", "message": f"Task {task_id} acknowledged"}
        return {"status": "error", "reason": f"Task {task_id} not found in-flight"}

    async def _handle_nack(self, message: dict, writer) -> dict:
        """
        Handle NACK command — mark a task as failed and re-queue it.

        Expected message format:
            {"command": "NACK", "task_id": "uuid-here"}
        """
        task_id = message.get("task_id")

        if not task_id:
            return {"status": "error", "reason": "Missing 'task_id' field"}

        success = await self.queue_manager.negative_acknowledge(task_id)
        if success:
            return {"status": "ok", "message": f"Task {task_id} returned to queue"}
        return {"status": "error", "reason": f"Task {task_id} not found in-flight"}

    # ── Server Lifecycle ───────────────────────────────────────────────

    async def start(self):
        """
        Start the TCP server and serve forever.

        asyncio.start_server() creates a TCP server that calls
        handle_connection() for every new incoming connection.
        Each connection runs as its own coroutine on the event loop.

        The server runs until it receives a shutdown signal (SIGINT/SIGTERM).
        """
        self._server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        # Register signal handlers for graceful shutdown (Unix only)
        # On Windows, signal handling is more limited but Ctrl+C still works
        loop = asyncio.get_running_loop()
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        addrs = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"DistroSync Broker listening on {addrs}")
        logger.info("Press Ctrl+C to stop")

        async with self._server:
            await self._server.serve_forever()

    async def stop(self):
        """
        Gracefully shut down the server.

        Closes the listening socket so no new connections are accepted,
        then allows existing connections to finish.
        """
        if self._server:
            logger.info("Shutting down broker server...")
            self._server.close()
            await self._server.wait_closed()
            logger.info("Broker server stopped")


def main():
    """Entry point to run the broker as a standalone process."""
    import argparse

    parser = argparse.ArgumentParser(description="DistroSync Broker Server")
    parser.add_argument(
        "--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=5555, help="Port to listen on (default: 5555)"
    )
    args = parser.parse_args()

    broker = BrokerServer(host=args.host, port=args.port)

    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        logger.info("Broker interrupted by user")


if __name__ == "__main__":
    main()
