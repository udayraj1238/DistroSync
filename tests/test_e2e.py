"""
End-to-End Tests for DistroSync

Spins up a live broker, multiple live workers, and produces a batch
of tasks. Verifies that all tasks are processed successfully, the queues
empty out, and the system shuts down cleanly.
"""

import asyncio
import os
import sys
import tempfile
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from worker.task_executor import ProcessPoolWorker
from producer.client import ProducerClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BrokerProcess:
    """Runs the broker in a background subprocess."""
    def __init__(self, db_path, port):
        self.db_path = db_path
        self.port = port
        self.process = None

    async def start(self):
        cmd = [
            sys.executable, "-m", "broker.server",
            "--host", "127.0.0.1",
            "--port", str(self.port),
            "--http-port", str(self.port + 1000),
            "--db-path", self.db_path,
        ]
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        await asyncio.sleep(2.0)
        asyncio.create_task(self._drain(self.process.stdout))
        asyncio.create_task(self._drain(self.process.stderr))

    async def _drain(self, stream):
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
        except Exception:
            pass

    async def stop(self):
        if self.process:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5.0)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None


async def test_e2e_batch_processing():
    """Produce 500 tasks, have 2 multiprocessing workers consume them."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "e2e_test.db")
        port = 5568
        queue_name = "e2e_tasks"

        # 1. Start Broker
        broker = BrokerProcess(db_path, port)
        await broker.start()

        try:
            # 2. Start Workers
            worker1 = ProcessPoolWorker(
                queue_name=queue_name,
                host="127.0.0.1",
                port=port,
                worker_id="e2e-worker-1",
                max_workers=2
            )
            worker2 = ProcessPoolWorker(
                queue_name=queue_name,
                host="127.0.0.1",
                port=port,
                worker_id="e2e-worker-2",
                max_workers=2
            )
            
            w1_task = asyncio.create_task(worker1.run())
            w2_task = asyncio.create_task(worker2.run())
            await asyncio.sleep(2.0) # Wait for registration

            # 3. Produce Tasks
            producer = ProducerClient(host="127.0.0.1", port=port)
            await producer.connect()

            total_tasks = 500
            print(f"Producing {total_tasks} tasks...")
            
            task_ids = set()
            for i in range(total_tasks):
                task_id = await producer.produce(
                    queue_name, 
                    {"job": "sleep", "duration": 0.01, "index": i}
                )
                task_ids.add(task_id)

            print("All tasks produced. Waiting for workers to finish...")

            # 4. Wait for processing
            # We can poll the broker metrics or just wait based on known processing rate.
            # 500 tasks / 4 processes * 0.01s = 1.25s theoretical minimum
            
            # Let's poll the API via producer (since producer just does raw TCP, we can send METRICS)
            # Actually, ProducerClient doesn't expose METRICS easily. 
            # We will just wait and check the workers' local metrics if possible, or wait a generous amount of time.
            
            timeout = 30.0
            start_time = time.time()
            done = False
            
            while time.time() - start_time < timeout:
                # We can check if workers are idle, but that's tricky.
                # Just sleep and check.
                await asyncio.sleep(2.0)
                
                # Check DB directly to see if all tasks are done
                import sqlite3
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT count(*) FROM tasks WHERE status != 'done'")
                pending_count = cursor.fetchone()[0]
                conn.close()
                
                print(f"Pending/In-flight tasks in DB: {pending_count}")
                if pending_count == 0:
                    done = True
                    break
                    
            assert done, "Workers did not finish processing tasks in time!"
            
            # 5. Stop Workers
            print("Stopping workers...")
            await worker1.shutdown()
            await worker2.shutdown()
            
            print("E2E Test Passed successfully.")

        finally:
            await broker.stop()

if __name__ == "__main__":
    asyncio.run(test_e2e_batch_processing())
