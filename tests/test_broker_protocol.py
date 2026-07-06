"""
Integration tests for the TCP broker wire protocol —
produce/consume flow, framing, and command dispatch.

This script starts the broker, connects as a producer to submit tasks,
then connects as a consumer to pull them back out. It verifies the
length-prefixed framing, JSON command protocol, and queue mechanics
all work correctly.
"""

import asyncio
import json
import sys
import os
import traceback

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer

TEST_PORT = 15555


async def send_message(writer: asyncio.StreamWriter, data: dict):
    """Send a length-prefixed JSON message to the broker."""
    encoded = json.dumps(data).encode("utf-8")
    length_prefix = len(encoded).to_bytes(4, byteorder="big")
    writer.write(length_prefix + encoded)
    await writer.drain()


async def receive_response(reader: asyncio.StreamReader) -> dict:
    """Read a length-prefixed JSON response from the broker."""
    raw_len = await reader.readexactly(4)
    msg_len = int.from_bytes(raw_len, byteorder="big")
    raw_msg = await reader.readexactly(msg_len)
    return json.loads(raw_msg.decode("utf-8"))


class TestRunner:
    """Runs all broker tests using a single persistent connection per test."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.first_task_id = None

    async def connect(self):
        """Open a new connection to the broker."""
        return await asyncio.open_connection("127.0.0.1", TEST_PORT)

    async def test_produce_single(self):
        """Test 1: PRODUCE a single task."""
        print("\n--- Test 1: PRODUCE a single task ---")
        reader, writer = await self.connect()
        try:
            await send_message(writer, {
                "command": "PRODUCE",
                "queue": "test_queue",
                "task": {"action": "send_email", "to": "user@example.com"},
            })
            response = await receive_response(reader)

            assert response["status"] == "ok", f"Expected 'ok', got {response['status']}"
            assert "task_id" in response, "Response missing 'task_id'"
            self.first_task_id = response["task_id"]
            print(f"  PASS: Task produced with ID: {self.first_task_id[:8]}...")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_produce_multiple(self):
        """Test 2: PRODUCE multiple tasks to verify batch enqueue works."""
        print("\n--- Test 2: PRODUCE multiple tasks ---")
        reader, writer = await self.connect()
        try:
            for i in range(5):
                await send_message(writer, {
                    "command": "PRODUCE",
                    "queue": "test_queue",
                    "task": {"action": "process", "item": i},
                })
                resp = await receive_response(reader)
                assert resp["status"] == "ok"
            print(f"  PASS: Produced 5 tasks successfully")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_consume_fifo(self):
        """Test 3: CONSUME returns tasks in FIFO order."""
        print("\n--- Test 3: CONSUME tasks in FIFO order ---")
        reader, writer = await self.connect()
        try:
            await send_message(writer, {
                "command": "CONSUME",
                "queue": "test_queue",
                "worker_id": "test_worker_1",
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok", f"Expected 'ok', got {resp['status']}"
            assert resp["task"]["payload"]["action"] == "send_email"
            self.first_task_id = resp["task"]["task_id"]
            print(f"  PASS: First dequeue returned the email task (FIFO confirmed)")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_consume_empty(self):
        """Test 4: CONSUME from empty/nonexistent queue returns 'empty'."""
        print("\n--- Test 4: CONSUME from empty queue ---")
        reader, writer = await self.connect()
        try:
            await send_message(writer, {
                "command": "CONSUME",
                "queue": "nonexistent_queue",
                "worker_id": "test_worker_1",
            })
            resp = await receive_response(reader)
            assert resp["status"] == "empty", f"Expected 'empty', got {resp['status']}"
            print(f"  PASS: Empty queue correctly returned 'empty' status")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_ack(self):
        """Test 5: ACK marks a task as completed."""
        print("\n--- Test 5: ACK a task ---")
        reader, writer = await self.connect()
        try:
            await send_message(writer, {
                "command": "ACK",
                "task_id": self.first_task_id,
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok", f"Expected 'ok', got {resp['status']}"
            print(f"  PASS: Task {self.first_task_id[:8]}... acknowledged successfully")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_nack_and_requeue(self):
        """Test 6: NACK returns a task to the queue and increments attempts."""
        print("\n--- Test 6: NACK and re-queue ---")
        reader, writer = await self.connect()
        try:
            # Consume one task
            await send_message(writer, {
                "command": "CONSUME",
                "queue": "test_queue",
                "worker_id": "test_worker_2",
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok"
            nack_task_id = resp["task"]["task_id"]

            # NACK it (simulate failure)
            await send_message(writer, {
                "command": "NACK",
                "task_id": nack_task_id,
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok"

            # Consume again — should get the same task back with attempt=2
            await send_message(writer, {
                "command": "CONSUME",
                "queue": "test_queue",
                "worker_id": "test_worker_2",
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok"
            assert resp["task"]["task_id"] == nack_task_id
            assert resp["task"]["attempts"] == 2
            print(f"  PASS: NACKed task re-queued with attempt count = 2")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_register_worker(self):
        """Test 7: REGISTER adds a worker to the registry."""
        print("\n--- Test 7: REGISTER a worker ---")
        reader, writer = await self.connect()
        try:
            await send_message(writer, {
                "command": "REGISTER",
                "worker_id": "worker_alpha",
                "queues": ["test_queue", "high_priority"],
            })
            resp = await receive_response(reader)
            assert resp["status"] == "ok"
            assert resp["registered"] is True
            print(f"  PASS: Worker registered successfully")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_invalid_commands(self):
        """Test 8: Invalid commands are rejected gracefully."""
        print("\n--- Test 8: Invalid/unknown commands ---")
        reader, writer = await self.connect()
        try:
            # Unknown command
            await send_message(writer, {"command": "INVALID_CMD"})
            resp = await receive_response(reader)
            assert resp["status"] == "error"
            assert "Unknown command" in resp["reason"]
            print(f"  PASS: Unknown command rejected")

            # Missing command field
            await send_message(writer, {"foo": "bar"})
            resp = await receive_response(reader)
            assert resp["status"] == "error"
            assert "Missing" in resp["reason"]
            print(f"  PASS: Missing command field rejected")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_missing_fields(self):
        """Test 9: Commands with missing required fields are rejected."""
        print("\n--- Test 9: Missing required fields ---")
        reader, writer = await self.connect()
        try:
            # PRODUCE without queue
            await send_message(writer, {"command": "PRODUCE", "task": {}})
            resp = await receive_response(reader)
            assert resp["status"] == "error"
            print(f"  PASS: PRODUCE without queue rejected")

            # CONSUME without worker_id
            await send_message(writer, {"command": "CONSUME", "queue": "test_queue"})
            resp = await receive_response(reader)
            assert resp["status"] == "error"
            print(f"  PASS: CONSUME without worker_id rejected")
            self.passed += 1
        finally:
            writer.close()
            await writer.wait_closed()


async def run_tests():
    """Start the broker and run all tests."""
    broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
    runner = TestRunner()

    # Start the broker in the background
    server_task = asyncio.create_task(broker.start())

    # Give the server time to start listening
    await asyncio.sleep(0.5)

    # Run each test individually so one failure doesn't block others
    tests = [
        runner.test_produce_single,
        runner.test_produce_multiple,
        runner.test_consume_fifo,
        runner.test_consume_empty,
        runner.test_ack,
        runner.test_nack_and_requeue,
        runner.test_register_worker,
        runner.test_invalid_commands,
        runner.test_missing_fields,
    ]

    for test_func in tests:
        try:
            await test_func()
        except AssertionError as e:
            print(f"  FAIL: ASSERTION FAILED: {e}")
            runner.failed += 1
        except Exception as e:
            print(f"  FAIL: ERROR: {e}")
            traceback.print_exc()
            runner.failed += 1

    # Shut down the broker
    await broker.stop()
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # Summary
    print(f"\n{'='*50}")
    print(f"  Results: {runner.passed} passed, {runner.failed} failed")
    print(f"{'='*50}")
    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
