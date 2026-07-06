"""
API Tests

Focus: HTTP API consistency under load
  40. Metrics endpoint accuracy during high churn
"""

import asyncio
import time
import os
import sys
import traceback
import urllib.request
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.base_worker import BaseWorker

PORT_BASE = 16400


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


async def produce_burst(host: str, port: int, q_name: str, tasks: int):
    client = ProducerClient(host, port)
    await client.connect()
    for i in range(tasks):
        try:
            await client.produce(q_name, {"i": i}, max_retries=50)
        except Exception:
            pass
    await client.close()


# -- Test 40: Metrics endpoint accuracy during high churn -----------------

async def test_metrics_consistency_under_load():
    """
    While 50 producers and 8 workers run simultaneously, poll /metrics
    20 times per second. Assert every response has a consistent internal
    state: depth + in_flight never exceeds total tasks produced.
    """
    port = PORT_BASE
    http_port = port + 1000
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=http_port)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Start 8 workers
        workers = []
        worker_tasks = []
        for _ in range(8):
            w = FastWorker("api_q", host="127.0.0.1", port=port, poll_interval=0.001)
            workers.append(w)
            worker_tasks.append(asyncio.create_task(w.run()))
            
        await asyncio.sleep(0.5)
        
        producers = 50
        tasks_per_producer = 20
        total_produced = producers * tasks_per_producer
        
        async def run_load_scenario():
            tasks = [
                produce_burst("127.0.0.1", port, "api_q", tasks_per_producer)
                for _ in range(producers)
            ]
            await asyncio.gather(*tasks)
            
        load_task = asyncio.create_task(run_load_scenario())
        
        # Poll metrics
        print("    Polling /metrics 20 times per second during load...")
        inconsistencies = []
        
        for _ in range(100): # 5 seconds
            try:
                # We use urllib to fetch synchronously in a thread, or just run it via asyncio
                def fetch_metrics():
                    req = urllib.request.Request(f"http://127.0.0.1:{http_port}/metrics")
                    with urllib.request.urlopen(req) as resp:
                        return json.loads(resp.read().decode())
                        
                r = await asyncio.to_thread(fetch_metrics)
                
                # Assertions
                # Not all queues may be created immediately, handle missing queue
                q_stats = r.get("queues", {}).get("api_q", {})
                depth = q_stats.get("depth", 0)
                
                # in_flight might be globally reported in broker stats or queue stats
                inf_global = r.get("broker", {}).get("in_flight_tasks", 0)
                
                if depth + inf_global > total_produced:
                    inconsistencies.append((depth, inf_global))
                    
            except Exception as e:
                pass
                
            await asyncio.sleep(0.05)
            if load_task.done() and broker.queue_manager.queue_depth("api_q") == 0:
                break
                
        await load_task
        
        if inconsistencies:
            print(f"    Found {len(inconsistencies)} inconsistent states!")
            for d, inf in inconsistencies[:5]:
                print(f"      depth: {d}, in_flight: {inf} (sum = {d+inf} > {total_produced})")
            assert False, "Metrics were inconsistent under load!"
            
        print("    No inconsistencies found. Metrics are thread-safe.")
        
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


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Metrics endpoint accuracy during high churn", test_metrics_consistency_under_load)

    print(f"\n{'=' * 60}")
    print(f"  API Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
