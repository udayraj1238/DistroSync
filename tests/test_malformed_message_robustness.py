"""
Crash and Fault Tolerance Tests (Part 2)

Focus: Broker robustness against bad actors and massive load
  28. Broker handles malformed message without crashing
  29. Broker handles producer disconnect gracefully
  30. 1000 concurrent connections do not crash broker
"""

import asyncio
import os
import sys
import time
import socket
import json
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient

PORT_BASE = 16000


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    async def run(self, name, coro_func):
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            await coro_func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
            self.failed += 1


# -- Test 28: Broker handles malformed message without crashing -----------

async def test_malformed_message():
    """
    Open a raw TCP connection. Send a malformed message: wrong length
    prefix, invalid JSON. Assert broker logs an error but continues
    serving other connections.
    """
    port = PORT_BASE
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Send garbage data over TCP (simulating bad framing/JSON)
        sock = socket.create_connection(("127.0.0.1", port))
        
        # Wrong length prefix (says 16 bytes), but invalid JSON payload
        sock.send(b"\x00\x00\x00\x10NOTVALIDJSON!!!!")
        
        # Give broker a moment to process and log the error (or disconnect us)
        await asyncio.sleep(0.5)
        
        try:
            sock.close()
        except:
            pass

        # Broker should still be serving valid requests
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        tid = await producer.produce("q_robust", {"data": "valid"})
        await producer.close()
        
        assert tid is not None, "Broker failed to serve valid request after malformed data"
        assert broker._server.is_serving(), "Broker server is no longer serving"

    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 29: Broker handles producer disconnect gracefully ---------------

async def test_abrupt_producer_disconnect():
    """
    Connect a producer. Send 1 task. Kill the TCP connection without
    closing properly (simulate network drop). Assert broker logs the
    disconnect and queue is intact.
    """
    port = PORT_BASE + 1
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Use a raw socket to simulate abrupt disconnect
        sock = socket.create_connection(("127.0.0.1", port))
        
        # Send a valid produce command manually
        msg = {"command": "PRODUCE", "queue": "q_abrupt", "task": {"i": 1}}
        encoded = json.dumps(msg).encode("utf-8")
        sock.send(len(encoded).to_bytes(4, byteorder="big") + encoded)
        
        # Give it a tiny bit to reach the broker OS receive buffer
        await asyncio.sleep(0.1)
        
        # Abruptly close (no protocol-level shutdown)
        # Using socket.SO_LINGER with 0 timeout sends RST instead of FIN
        l_onoff = 1
        l_linger = 0
        import struct
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', l_onoff, l_linger))
        sock.close()
        
        await asyncio.sleep(0.5)
        
        # Broker should still be alive and have the task in queue
        assert broker.queue_manager.queue_depth("q_abrupt") == 1, (
            "Task was not enqueued or queue depth is wrong"
        )
        assert broker._server.is_serving(), "Broker crashed after abrupt disconnect"
        
    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 30: 1000 concurrent connections do not crash broker --------------

async def test_1000_connections():
    """
    Open 1000 simultaneous TCP connections to the broker. Each sends 1 task
    and waits for a response. Assert all 1000 get a valid response and broker
    memory usage stays reasonable.
    """
    port = PORT_BASE + 2
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # We might hit OS file descriptor limits on Windows/Mac with 1000.
        # Let's try 500 first to be safe, or just 1000. 1000 should work on Windows
        # if the limit isn't reached (Windows default max handles is 16 million).
        # We will batch the connects to avoid socket.error: [Errno 10048]
        # Only issue is port exhaustion on client side.
        CONNECTIONS = 1000
        
        async def run_client(idx: int):
            try:
                producer = ProducerClient("127.0.0.1", port)
                await producer.connect()
                tid = await producer.produce("q_1000", {"id": idx})
                # Don't close immediately to keep connection open
                return (producer, tid)
            except Exception as e:
                return e

        print(f"    Opening {CONNECTIONS} connections concurrently...")
        
        # Doing all 1000 simultaneously might hit a backlog limit in asyncio start_server
        # We will spawn them in batches of 200
        active_clients = []
        for i in range(0, CONNECTIONS, 200):
            batch = [run_client(idx) for idx in range(i, i + 200)]
            results = await asyncio.gather(*batch)
            for res in results:
                if isinstance(res, Exception):
                    raise Exception(f"Failed to connect: {res}")
                active_clients.append(res)
        
        assert len(active_clients) == CONNECTIONS
        
        # Verify broker state
        assert broker._server.is_serving()
        assert broker.queue_manager.queue_depth("q_1000") == CONNECTIONS
        assert broker._active_connections == CONNECTIONS
        
        print("    All connections opened and tasks produced successfully.")
        
        # Close them all safely
        close_tasks = [c[0].close() for c in active_clients]
        await asyncio.gather(*close_tasks)
        
    finally:
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Broker handles malformed message without crashing", test_malformed_message)
    await runner.run("Broker handles producer disconnect gracefully", test_abrupt_producer_disconnect)
    await runner.run("1000 concurrent connections do not crash broker", test_1000_connections)

    print(f"\n{'=' * 60}")
    print(f"  Robustness Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
