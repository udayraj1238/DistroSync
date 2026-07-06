"""
Resilience Tests

Focus: System resilience and recovery
  38. System recovers from 5x overload within 8 seconds
  39. Zero tasks lost during rolling worker restart
"""

import asyncio
import time
import os
import sys
import traceback
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.base_worker import BaseWorker

PORT_BASE = 16300


class FastWorker(BaseWorker):
    async def execute(self, payload: dict) -> dict:
        return {"ok": True}


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


async def flood_system(host: str, port: int, q_name: str, duration: int):
    """Flood the system by spawning many producers to spam requests."""
    end_time = time.monotonic() + duration
    
    async def spam():
        client = ProducerClient(host, port)
        await client.connect()
        while time.monotonic() < end_time:
            try:
                # Disable retries, we just want to spam
                await client.produce(q_name, {"flood": True}, max_retries=0)
            except Exception:
                pass
        await client.close()
        
    tasks = [asyncio.create_task(spam()) for _ in range(20)]
    await asyncio.gather(*tasks)


# -- Test 38: System recovers from 5x overload within 8 seconds -----------

async def test_overload_recovery():
    """
    Flood the system at high throughput for 10 seconds. Then stop flooding.
    Assert that within 8 seconds of flood stopping, queue depth drops below 20
    and new tasks are accepted at normal rate.
    """
    port = PORT_BASE
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        workers = []
        worker_tasks = []
        for _ in range(2):  # Only 2 workers so they get overwhelmed
            w = FastWorker("resilience_q", host="127.0.0.1", port=port, poll_interval=0.01)
            workers.append(w)
            worker_tasks.append(asyncio.create_task(w.run()))
            
        await asyncio.sleep(0.5)
        
        # We start with load shedder tuned normally
        # By flooding it, we should see queue depth increase and workers get slow,
        # which triggers the adaptive rate limiter.
        
        print("    Flooding system for 10 seconds...")
        await flood_system("127.0.0.1", port, "resilience_q", 10)
        
        flood_stop_time = time.monotonic()
        
        print("    Flood stopped. Waiting up to 8 seconds for recovery...")
        recovered = False
        while time.monotonic() - flood_stop_time < 8:
            depth = broker.queue_manager.queue_depth("resilience_q")
            if depth < 20:
                recovered = True
                break
            await asyncio.sleep(0.5)
            
        depth = broker.queue_manager.queue_depth("resilience_q")
        print(f"    Queue depth after recovery period: {depth}")
        assert recovered, f"Queue depth did not drop below 20 in 8s (current: {depth})"
        
        # Normal produces should now be accepted
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        try:
            r = await producer.produce("resilience_q", {"normal": True})
            assert r is not None, "Normal produce failed (was None)"
        finally:
            await producer.close()
            
    finally:
        for w in workers:
            await w.shutdown()
        for wt in worker_tasks:
            wt.cancel()
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Test 39: Zero tasks lost during rolling worker restart ---------------

async def test_rolling_restart():
    """
    Produce 200 tasks. Start 4 workers. While they process, kill and restart
    each worker one at a time with 2s between each kill. Assert all 200
    tasks eventually complete.
    """
    port = PORT_BASE + 1
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Produce 200 tasks
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        for i in range(200):
            await producer.produce("rolling_q", {"i": i})
        await producer.close()
        
        depth = broker.queue_manager.queue_depth("rolling_q")
        print(f"    Produced {depth} tasks.")
        assert depth == 200
        
        # Start 4 workers using actual child processes so we can kill them hard
        worker_script = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "worker", "task_executor.py"
        )
        
        # We need a small launcher for a FastWorker if we want to run as subprocess.
        # It's easier to use the standard Echo worker that we have, or simply create tasks
        # here and cancel() them abruptly without proper shutdown.
        
        # Let's use asyncio tasks and cancel them abruptly
        # To simulate a hard crash (without ACK), we will just destroy the socket and task
        workers = []
        worker_tasks = []
        
        def start_worker():
            # Use base worker but monkey-patch execute to be a bit slower so we catch them in flight
            class SlowWorker(BaseWorker):
                async def execute(self, payload: dict) -> dict:
                    await asyncio.sleep(0.1)
                    return {"ok": True}
                    
            w = SlowWorker("rolling_q", host="127.0.0.1", port=port, poll_interval=0.01)
            t = asyncio.create_task(w.run())
            return w, t
            
        print("    Starting 4 workers...")
        for _ in range(4):
            w, t = start_worker()
            workers.append(w)
            worker_tasks.append(t)
            
        # Rolling restart logic
        for i in range(4):
            print(f"    Killing worker {i+1}...")
            # Hard kill: cancel task and close socket abruptly
            w, t = workers[i], worker_tasks[i]
            # Don't use w.shutdown()! We want a dirty crash.
            if w._writer:
                w._writer.close()
            t.cancel()
            
            await asyncio.sleep(2)
            
            print(f"    Starting replacement for worker {i+1}...")
            new_w, new_t = start_worker()
            workers.append(new_w)
            worker_tasks.append(new_t)
            
        # Wait for all tasks to be processed
        print("    Waiting for tasks to complete (up to 30s)...")
        wait_start = time.monotonic()
        while broker.queue_manager.queue_depth("rolling_q") > 0 or broker.queue_manager.in_flight_count() > 0:
            if time.monotonic() - wait_start > 30:
                print("    TIMEOUT waiting for tasks to complete!")
                break
            await asyncio.sleep(1.0)
            
        depth = broker.queue_manager.queue_depth("rolling_q")
        in_flight = broker.queue_manager.in_flight_count()
        dlq_count = broker.queue_manager.dead_letter_queue.count()
        
        processed = 200 - depth - in_flight - dlq_count
        
        print(f"    Remaining depth: {depth}, In-flight: {in_flight}, DLQ: {dlq_count}")
        assert depth == 0, f"Expected depth 0, got {depth}"
        assert in_flight == 0, f"Expected in-flight 0, got {in_flight}"
        assert dlq_count == 0, f"Expected DLQ 0, got {dlq_count} (meaning some were dropped)"
        assert broker.queue_manager.get_stats()["total_acked"] >= 200, "Should have acked 200 tasks"
        
    finally:
        for w in workers:
            try:
                await w.shutdown()
            except:
                pass
        for wt in worker_tasks:
            wt.cancel()
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("System recovers from 5x overload within 8 seconds", test_overload_recovery)
    await runner.run("Zero tasks lost during rolling worker restart", test_rolling_restart)

    print(f"\n{'=' * 60}")
    print(f"  Resilience Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
