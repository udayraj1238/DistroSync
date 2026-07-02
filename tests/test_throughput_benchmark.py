"""
June 23 -- Performance and Concurrency Tests (Part 1)

Focus: Throughput, Latency, and Thread Safety
  31. Throughput benchmark: 8000+ tasks/min
  32. P99 latency stays under 50ms at nominal load
  33. Token bucket thread safety under concurrent access

Usage:
    python -m tests.test_june23_performance
    python tests/test_june23_performance.py
"""

import asyncio
import time
import os
import sys
import traceback
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.base_worker import BaseWorker
from broker.load_shedder import AdaptiveLoadShedder
from broker.queue_manager import QueueManager
from broker.worker_registry import WorkerRegistry

PORT_BASE = 16100


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


async def produce_burst(host: str, port: int, tasks: int, results: list):
    client = ProducerClient(host, port)
    await client.connect()
    latencies = []
    
    for i in range(tasks):
        start = time.monotonic()
        try:
            # We don't care about rate limiting here, just push
            await client.produce("perf_q", {"i": i}, max_retries=50)
            latencies.append(time.monotonic() - start)
        except Exception:
            pass
            
    await client.close()
    results.extend(latencies)


# -- Test 31: Throughput benchmark: 8000+ tasks/min ------------------------

async def test_throughput_benchmark():
    """
    The formal benchmark. 50 concurrent producers x 200 tasks each = 10,000 tasks.
    4 workers. Measure wall-clock time. Assert throughput >= 8000 tasks/min.
    """
    port = PORT_BASE
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        # Start 4 workers
        workers = []
        worker_tasks = []
        for _ in range(4):
            w = FastWorker("perf_q", host="127.0.0.1", port=port, poll_interval=0.001)
            workers.append(w)
            worker_tasks.append(asyncio.create_task(w.run()))
            
        await asyncio.sleep(0.5)
        
        producers = 50
        tasks_per_producer = 200
        total_tasks = producers * tasks_per_producer
        
        start = time.monotonic()
        
        # Run producers
        results = []
        producer_tasks = [
            produce_burst("127.0.0.1", port, tasks_per_producer, results)
            for _ in range(producers)
        ]
        await asyncio.gather(*producer_tasks)
        
        # Wait for workers to drain the queue
        while broker.queue_manager.queue_depth("perf_q") > 0 or broker.queue_manager.in_flight_count() > 0:
            await asyncio.sleep(0.1)
            
        elapsed = time.monotonic() - start
        
        throughput_sec = total_tasks / elapsed
        throughput_min = throughput_sec * 60
        
        print(f"    Elapsed: {elapsed:.2f}s, Throughput: {throughput_min:.0f} tasks/min")
        assert throughput_min >= 8000, f"Got {throughput_min:.0f} tasks/min (expected >= 8000)"
        
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


# -- Test 32: P99 latency stays under 50ms at nominal load ----------------

async def test_p99_latency():
    """
    Send 1000 tasks with 4 workers. Record latency for each. Assert P99
    (the 990th slowest of 1000) is under 50ms.
    """
    port = PORT_BASE + 1
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    
    # Increase load shedder capacity so we don't get rate limited
    broker.load_shedder.BUCKET_CAPACITY = 2000.0
    broker.load_shedder.BASE_FILL_RATE = 2000.0
    
    server_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)

    try:
        workers = []
        worker_tasks = []
        for _ in range(4):
            w = FastWorker("perf_q", host="127.0.0.1", port=port, poll_interval=0.001)
            workers.append(w)
            worker_tasks.append(asyncio.create_task(w.run()))
            
        await asyncio.sleep(0.5)
        
        results = []
        # Run 10 producers with 100 tasks each = 1000 tasks
        producer_tasks = [
            produce_burst("127.0.0.1", port, 100, results)
            for _ in range(10)
        ]
        await asyncio.gather(*producer_tasks)
        
        assert len(results) == 1000, f"Expected 1000 latencies, got {len(results)}"
        
        sorted_latencies = sorted(results)
        p99_latency_ms = sorted_latencies[990] * 1000  # convert to ms
        
        print(f"    P99 latency: {p99_latency_ms:.1f}ms")
        assert p99_latency_ms < 50.0, f"P99 was {p99_latency_ms:.1f}ms (expected < 50ms)"
        
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


# -- Test 33: Token bucket thread safety under concurrent access -----------

async def test_bucket_thread_safety():
    """
    Run 100 coroutines simultaneously, all calling check_and_consume()
    on the same queue. Assert that the total number of accepted tasks
    exactly equals the bucket capacity.
    """
    qm = QueueManager()
    wr = WorkerRegistry(qm)
    shedder = AdaptiveLoadShedder(qm, wr)
    
    # Setup bucket with exact capacity
    shedder.BUCKET_CAPACITY = 50.0
    # Use a low fill rate so it doesn't refill during our micro-burst
    shedder.BASE_FILL_RATE = 0.001 
    
    # Initialize the bucket for "q"
    await shedder.check_and_consume("q")
    
    # Reset tokens to exactly 50
    shedder._buckets["q"].tokens = 50.0
    shedder._buckets["q"].last_refill = time.monotonic()
    
    # Run 100 concurrent consume attempts
    results = await asyncio.gather(*[
        shedder.check_and_consume("q") for _ in range(100)
    ])
    
    accepted = sum(1 for allowed, _ in results if allowed)
    
    # We did 1 initial + 100 concurrent. Wait, the initial was to initialize the bucket.
    # We reset tokens to exactly 50. So the next 100 attempts should yield exactly 50 acceptances.
    print(f"    Accepted: {accepted}, Capacity: 50")
    assert accepted == 50, f"Expected exactly 50 accepted, got {accepted}"


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Throughput benchmark: 8000+ tasks/min", test_throughput_benchmark)
    await runner.run("P99 latency stays under 50ms at nominal load", test_p99_latency)
    await runner.run("Token bucket thread safety under concurrent access", test_bucket_thread_safety)

    print(f"\n{'=' * 60}")
    print(f"  June 23 Performance Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
