"""
June 23 -- Persistence and Scale Tests (Part 2)

Focus: SQLite WAL Mode Concurrency and Scalability
  34. WAL store concurrent writes do not corrupt data
  35. Dead letter queue handles 10,000 entries

Usage:
    python -m tests.test_june23_persistence
    python tests/test_june23_persistence.py
"""

import sys
import os
import time
import uuid
import traceback
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from persistence.wal_store import WALStore
from broker.queue_manager import Task


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    def run(self, name, func):
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
            self.failed += 1


def make_task(idx: int) -> Task:
    task = Task("concurrent_q", {"idx": idx})
    # Pre-set some task_ids to ensure no collisions
    task.task_id = str(uuid.uuid4())
    return task


def make_failed_task(idx: int) -> Task:
    task = Task("dlq_scale_q", {"idx": idx})
    task.task_id = str(uuid.uuid4())
    task.attempts = 3
    task.status = "dead_lettered"
    task.dead_lettered_at = time.time()
    return task


# -- Test 34: WAL store concurrent writes do not corrupt data -------------

def test_wal_concurrent_writes():
    """
    Write 500 tasks to WALStore from 10 concurrent threads. Read them
    all back. Assert all 500 are present with correct data.
    """
    db_path = "test_concurrent_writes.db"
    for ext in ["", "-wal", "-shm"]:
        if os.path.exists(db_path + ext):
            os.remove(db_path + ext)
            
    try:
        wal = WALStore(db_path)
        tasks = [make_task(i) for i in range(500)]
        
        with ThreadPoolExecutor(max_workers=10) as pool:
            # We map save_task over the tasks
            list(pool.map(wal.save_task, tasks))
            
        stored = wal.load_pending_tasks()
        assert len(stored) == 500, f"Expected 500 tasks, got {len(stored)}"
        
        ids = [t["task_id"] for t in stored]
        assert len(set(ids)) == 500, "Duplicate task IDs found, data corruption!"
        
        print("    All 500 tasks written and read successfully.")
        
    finally:
        if 'wal' in locals():
            # Clean up connections
            if hasattr(wal._local, "conn") and wal._local.conn:
                wal._local.conn.close()
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(db_path + ext):
                try:
                    os.remove(db_path + ext)
                except:
                    pass


# -- Test 35: Dead letter queue handles 10,000 entries --------------------

def test_dlq_scale():
    """
    Add 10,000 tasks to the DLQ. Call get_dlq_tasks(). Assert all 10,000
    are returned. Measure query time -- assert under 500ms.
    """
    db_path = "test_dlq_scale.db"
    for ext in ["", "-wal", "-shm"]:
        if os.path.exists(db_path + ext):
            os.remove(db_path + ext)
            
    try:
        wal = WALStore(db_path)
        
        # Add 10,000 tasks
        for i in range(10000):
            wal.add_to_dlq(make_failed_task(i), f"error_{i}")
            
        start = time.monotonic()
        tasks = wal.get_dlq_tasks()
        elapsed = (time.monotonic() - start) * 1000  # ms
        
        print(f"    10,000 DLQ tasks read in {elapsed:.1f}ms")
        assert len(tasks) == 10000, f"Expected 10,000 tasks, got {len(tasks)}"
        assert elapsed < 500, f"Read took {elapsed:.1f}ms (expected < 500ms)"
        
    finally:
        if 'wal' in locals():
            if hasattr(wal._local, "conn") and wal._local.conn:
                wal._local.conn.close()
        for ext in ["", "-wal", "-shm"]:
            if os.path.exists(db_path + ext):
                try:
                    os.remove(db_path + ext)
                except:
                    pass


# -- Main ------------------------------------------------------------------

def run_tests():
    runner = TestRunner()

    runner.run("WAL store concurrent writes do not corrupt data", test_wal_concurrent_writes)
    runner.run("Dead letter queue handles 10,000 entries", test_dlq_scale)

    print(f"\n{'=' * 60}")
    print(f"  June 23 Persistence Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")
    
    return runner.failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
