"""
Integration Tests for Crash Recovery — End-to-End Live Process Validation

These tests run the broker as a separate background process, communicate
with it over TCP using the real ProducerClient, kill the broker process
unceremoniously, and verify that no data is lost upon restart.

Tests cover:
    1. A pending task survives a broker restart
    2. An in-flight task is reassigned after broker restart
    3. The WAL store accurately reloads state across process boundaries
"""

import asyncio
import json
import logging
import os
import signal
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient
from broker.server import BrokerServer
from worker.base_worker import BaseWorker

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IntegrationWorker(BaseWorker):
    """A minimal worker for testing."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_tasks = []

    async def execute(self, payload):
        self.processed_tasks.append(payload)
        return {"status": "success"}


class BrokerProcessManager:
    """Helper to manage the broker as a subprocess during testing."""

    def __init__(self, db_path: str, port: int):
        self.db_path = db_path
        self.port = port
        self.process = None

    async def start(self):
        """Start the broker as a subprocess."""
        logger.info(f"Starting broker process on port {self.port} with DB {self.db_path}")
        cmd = [
            sys.executable,
            "-m", "broker.server",
            "--host", "127.0.0.1",
            "--port", str(self.port),
            "--http-port", str(self.port + 1000), # Ensure no conflicts
            "--db-path", self.db_path,
        ]
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        # Give it a moment to bind the port
        await asyncio.sleep(2.0)
        
        # Drain some output so buffer doesn't fill
        asyncio.create_task(self._drain_output(self.process.stdout, "BROKER [OUT]"))
        asyncio.create_task(self._drain_output(self.process.stderr, "BROKER [ERR]"))

    async def _drain_output(self, stream, prefix):
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                # Optional: print broker output for debugging
                # print(f"{prefix}: {line.decode().strip()}")
        except Exception:
            pass

    async def kill(self):
        """Forcefully kill the broker process."""
        if self.process:
            logger.info(f"Killing broker process {self.process.pid}")
            try:
                # Use SIGKILL on Unix, TerminateProcess on Windows
                self.process.kill()
                await self.process.wait()
            except ProcessLookupError:
                pass
            self.process = None
            await asyncio.sleep(1.0) # Wait for OS to release ports

    async def stop(self):
        """Gracefully stop the broker process (if possible)."""
        if self.process:
             logger.info(f"Gracefully stopping broker process {self.process.pid}")
             try:
                 if sys.platform != "win32":
                     self.process.send_signal(signal.SIGTERM)
                 else:
                     # On Windows we can't easily send SIGTERM to a subprocess
                     # We'll just kill it, which is fine for these tests
                     self.process.kill()
                 await asyncio.wait_for(self.process.wait(), timeout=5.0)
             except (ProcessLookupError, asyncio.TimeoutError):
                 await self.kill()
             self.process = None


def run_test(name, fn):
    """Run a single async test and report pass/fail."""
    try:
        asyncio.run(fn())
        print(f"  PASS: {name}")
        return True
    except Exception as e:
        print(f"  FAIL: {name} -- {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_task_survives_broker_restart():
    """Test 1: A pending task survives a broker process kill and restart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "distrosync_test.db")
        port = 5566
        queue_name = "crash_test_q"

        manager = BrokerProcessManager(db_path, port)
        await manager.start()

        try:
            # Connect producer and enqueue a task
            producer = ProducerClient(host="127.0.0.1", port=port)
            await producer.connect()
            task_id = await producer.produce(queue_name, {"message": "survive_this"})
            assert isinstance(task_id, str)

            # Wait a moment to ensure it's written to WAL
            await asyncio.sleep(0.5)
            
            # Kill the broker process unceremoniously
            await manager.kill()

            # Restart the broker
            await manager.start()
            
            # Start a worker to consume the task
            worker = IntegrationWorker(
                queue_name=queue_name,
                host="127.0.0.1",
                port=port,
                worker_id="test-worker-1"
            )
            
            worker_task = asyncio.create_task(worker.run())
            
            # Wait for worker to connect and process
            await asyncio.sleep(2.0)
            
            # Stop the worker
            await worker.shutdown()
            await asyncio.sleep(0.5)

            # Verify the task was processed
            assert len(worker.processed_tasks) == 1
            assert worker.processed_tasks[0]["message"] == "survive_this"

        finally:
            await manager.stop()


async def test_in_flight_task_reassigned_after_restart():
    """Test 2: An in-flight task is reverted to pending on broker restart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "distrosync_test2.db")
        port = 5567
        queue_name = "inflight_test_q"

        manager = BrokerProcessManager(db_path, port)
        await manager.start()

        try:
            # Connect producer and enqueue
            producer = ProducerClient(host="127.0.0.1", port=port)
            await producer.connect()
            task_id = await producer.produce(queue_name, {"message": "inflight_test"})
            
            # Start a worker that will hang while processing
            class HangingWorker(BaseWorker):
                async def execute(self, payload):
                    # Hang forever to keep task in-flight
                    await asyncio.sleep(3600)
                    return {"status": "success"}
                    
            hanging_worker = HangingWorker(
                queue_name=queue_name,
                host="127.0.0.1",
                port=port,
                worker_id="hanging-worker"
            )
            
            worker_task = asyncio.create_task(hanging_worker.run())
            
            # Wait for the worker to dequeue the task
            await asyncio.sleep(2.0)
            
            # At this point, the task is in-flight.
            # Kill the broker (and the hanging worker, just to be clean)
            await manager.kill()
            await hanging_worker.shutdown()
            
            # Restart the broker
            await manager.start()
            
            # The task should have been reverted to 'pending'.
            # Start a normal worker to process it.
            good_worker = IntegrationWorker(
                queue_name=queue_name,
                host="127.0.0.1",
                port=port,
                worker_id="good-worker"
            )
            
            good_task = asyncio.create_task(good_worker.run())
            await asyncio.sleep(2.0)
            await good_worker.shutdown()
            
            # Verify the good worker got the task
            assert len(good_worker.processed_tasks) == 1
            assert good_worker.processed_tasks[0]["message"] == "inflight_test"
            
        finally:
            await manager.stop()


if __name__ == "__main__":
    tests = [
        ("Pending task survives broker restart", test_task_survives_broker_restart),
        ("In-flight task reassigned after restart", test_in_flight_task_reassigned_after_restart),
    ]

    passed = 0
    failed = 0

    print(f"\n--- Integration Crash Recovery Tests ({len(tests)} tests) ---\n")
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}\n")

    sys.exit(0 if failed == 0 else 1)
