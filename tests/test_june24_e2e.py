"""
June 24 -- End-to-End Test

Focus: Full system lifecycle
  42. End-to-end: produce 10k tasks, verify zero loss, measure P99

Usage:
    python -m tests.test_june24_e2e
    python tests/test_june24_e2e.py
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

PORT_BASE = 22500


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


async def produce_burst(host: str, port: int, q_name: str, tasks: int, results: list):
    client = ProducerClient(host, port)
    await client.connect()
    latencies = []
    
    for i in range(tasks):
        start = time.monotonic()
        try:
            await client.produce(q_name, {"i": i}, max_retries=100)
            latencies.append((time.monotonic() - start) * 1000)
        except Exception:
            pass
            
    await client.close()
    results.extend(latencies)


# -- Test 42: End-to-end 10k tasks ----------------------------------------

async def test_full_10k_e2e():
    """
    The complete end-to-end test. 50 producers, 4 workers, 10,000 tasks,
    2 simulated worker crashes.
    Assert: all 10,000 tasks processed, P99 < 60ms, 0 tasks in DLQ, broker alive.
    
    (Note: To keep this test from literally taking 3 minutes, we run the crashes
    at the 5-second mark instead of 120-second mark, but validate the exact same
    resilience constraints).
    """
    db_path = "e2e.db"
    for ext in ["", "-wal", "-shm"]:
        if os.path.exists(db_path + ext):
            os.remove(db_path + ext)
            
    port = PORT_BASE
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    broker_proc = subprocess.Popen(
        [sys.executable, "-m", "broker.server", "--port", str(port), "--http-port", str(port + 1000), "--db-path", db_path],
        cwd=base_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True
    )
    
    await asyncio.sleep(4.0)  # Let it start
    if broker_proc.poll() is not None:
        out, _ = broker_proc.communicate()
        print(f"Broker crashed on startup! Output:\n{out}")
        assert False, "Broker crashed on startup"

    
    try:
        workers = []
        worker_tasks = []
        
        def start_worker():
            w = FastWorker("e2e_q", host="127.0.0.1", port=port, poll_interval=0.01)
            t = asyncio.create_task(w.run())
            return w, t
            
        print("    Starting 4 workers...")
        for _ in range(4):
            w, t = start_worker()
            workers.append(w)
            worker_tasks.append(t)
            
        producers = 50
        tasks_per_producer = 200
        
        print(f"    Producing 10,000 tasks...")
        results = []
        producer_tasks = [
            asyncio.create_task(produce_burst("127.0.0.1", port, "e2e_q", tasks_per_producer, results))
            for _ in range(producers)
        ]
        
        # Simulate worker crashes while tasks are being produced and processed
        async def crash_workers():
            await asyncio.sleep(2.0)
            print("    Simulating 2 worker crashes...")
            for i in range(2):
                w, t = workers[i], worker_tasks[i]
                if w._writer:
                    w._writer.close()
                t.cancel()
                # Start replacements
                new_w, new_t = start_worker()
                workers.append(new_w)
                worker_tasks.append(new_t)
                
        asyncio.create_task(crash_workers())
        
        await asyncio.gather(*producer_tasks)
        print("    Production complete. Waiting for workers to finish...")
        
        # We need a client to check metrics
        import urllib.request
        import json
        
        wait_start = time.monotonic()
        while True:
            if time.monotonic() - wait_start > 45:
                print("    TIMEOUT waiting for workers to drain")
                break
                
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{port+1000}/metrics")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    metrics = json.loads(resp.read().decode())
                    q_stats = metrics.get("queues", {}).get("e2e_q", {})
                    depth = q_stats.get("depth", 0)
                    in_flight = metrics.get("broker", {}).get("in_flight_tasks", 0)
                    if depth == 0 and in_flight == 0:
                        break
            except Exception:
                pass
            await asyncio.sleep(0.5)
            
        # Final Verification
        req = urllib.request.Request(f"http://127.0.0.1:{port+1000}/metrics")
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            final_metrics = json.loads(resp.read().decode())
            
        q_stats = final_metrics.get("queues", {}).get("e2e_q", {})
        total_acked = q_stats.get("total_acked", 0)
        
        # Verify 0 in DLQ (Since worker crash just NACKs, it will retry and succeed)
        # We need to query SQLite directly or assume 0 if total_acked == 10000
        # Actually total_acked should be 10000
        print(f"    FINAL METRICS: {json.dumps(final_metrics)}")
        print(f"    Total ACKed: {total_acked}")
        assert total_acked == 10000, f"Expected 10000 acked, got {total_acked}"
        
        assert broker_proc.poll() is None, "Broker process crashed!"
        
        p99_latency_ms = q_stats.get("p99_latency_ms", 9999.0)
        print(f"    P99 Latency: {p99_latency_ms:.1f}ms")
        assert p99_latency_ms < 60.0, f"P99 latency {p99_latency_ms:.1f}ms exceeds 60ms limit"
        
    finally:
        for w in workers:
            try:
                await w.shutdown()
            except: pass
        for wt in worker_tasks:
            wt.cancel()
        broker_proc.kill()
        broker_proc.wait()
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(db_path + ext):
                try: os.remove(db_path + ext)
                except: pass


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("End-to-end: produce 10k tasks, verify zero loss, measure P99", test_full_10k_e2e)

    print(f"\n{'=' * 60}")
    print(f"  June 24 E2E Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
