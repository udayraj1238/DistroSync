"""
June 24 -- Worker Registry and Timing Boundaries

Focus: Heartbeat eviction timing
  37. Heartbeat eviction fires exactly at 6-second boundary

Usage:
    python -m tests.test_june24_worker_registry
    python tests/test_june24_worker_registry.py
"""

import asyncio
import time
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.worker_registry import WorkerRegistry
from broker.queue_manager import QueueManager

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


# -- Test 37: Heartbeat eviction fires exactly at 6-second boundary -------

async def test_eviction_timing():
    """
    Register a worker. Wait 5.5 seconds. Assert worker is still in registry.
    Wait 1 more second. Assert worker is evicted.
    """
    qm = QueueManager()
    wr = WorkerRegistry(qm)
    
    # Register the worker
    await wr.register_heartbeat("w1", "test_q", ("127.0.0.1", 12345))
    
    assert "w1" in wr._workers
    
    # Simulate time passing by modifying last_heartbeat
    wr._workers["w1"].last_heartbeat = time.time() - 5.5
    
    # Evict dead workers
    await wr._evict_dead_workers()
    
    print("    At 5.5s, worker should NOT be evicted")
    assert "w1" in wr._workers, "Worker was evicted too early"
    
    # Simulate more time passing
    wr._workers["w1"].last_heartbeat = time.time() - 6.5
    
    await wr._evict_dead_workers()
    
    print("    At 6.5s, worker SHOULD be evicted")
    assert "w1" not in wr._workers, "Worker was not evicted after 6.0s threshold"


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Heartbeat eviction fires exactly at 6-second boundary", test_eviction_timing)

    print(f"\n{'=' * 60}")
    print(f"  June 24 Worker Registry Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
