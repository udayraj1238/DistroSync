"""
June 22 -- Crash and Fault Tolerance Tests (Part 1)

Focus: Broker and Worker crash recovery
  25. Tasks survive broker restart
  26. Partial-write task is not double-counted after restart
  27. Worker crash mid-task: task is reassigned, not lost

Usage:
    python -m tests.test_june22_crash_recovery
    python tests/test_june22_crash_recovery.py
"""

import asyncio
import json
import os
import sys
import time
import socket
import sqlite3
import subprocess
import traceback
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient

PORT_BASE = 15900
DB_PATH = "test_recovery.db"


class RawClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )

    async def send(self, message: dict) -> dict:
        encoded = json.dumps(message).encode("utf-8")
        self._writer.write(len(encoded).to_bytes(4, byteorder="big") + encoded)
        await self._writer.drain()
        raw_len = await self._reader.readexactly(4)
        msg_len = int.from_bytes(raw_len, byteorder="big")
        raw_resp = await self._reader.readexactly(msg_len)
        return json.loads(raw_resp.decode("utf-8"))

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()


def start_broker(port: int, db_path: str) -> subprocess.Popen:
    """Start the broker as a separate process with WAL persistence."""
    broker_script = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "broker", "server.py"
    )
    # Using python -m broker.server if possible, but sys.executable script is safer
    cmd = [
        sys.executable, broker_script,
        "--port", str(port),
        "--http-port", str(port + 1000),
        "--db-path", db_path
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    time.sleep(1.5)  # give it time to start
    return proc


def cleanup_db(db_path: str):
    for ext in ["", "-wal", "-shm"]:
        p = db_path + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


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


# -- Test 25: Tasks survive broker restart ---------------------------------

async def test_crash_recovery():
    """
    Produce 10 tasks. Kill the broker process. Restart it. Start a worker.
    Assert all 10 tasks are processed -- none were lost.
    """
    db_path = DB_PATH + "_25.db"
    cleanup_db(db_path)
    port = PORT_BASE
    
    broker_proc = start_broker(port, db_path)
    
    try:
        # 1. Produce 10 tasks
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        for i in range(10):
            await producer.produce("recovery_q", {"i": i})
        await producer.close()
        
        # 2. Hard kill the broker
        broker_proc.kill()
        broker_proc.wait()
        
        # 3. Restart the broker
        broker_proc = start_broker(port, db_path)
        
        # 4. Connect worker and consume all tasks
        worker = RawClient("127.0.0.1", port)
        await worker.connect()
        await worker.send({
            "command": "REGISTER",
            "worker_id": "recovery-worker",
            "queues": ["recovery_q"],
        })
        
        processed = 0
        for _ in range(15):
            resp = await worker.send({
                "command": "CONSUME",
                "queue": "recovery_q",
                "worker_id": "recovery-worker",
            })
            if resp["status"] == "empty":
                break
            if resp["status"] == "ok":
                await worker.send({
                    "command": "ACK",
                    "task_id": resp["task"]["task_id"],
                    "worker_id": "recovery-worker",
                })
                processed += 1
                
        await worker.close()
        
        assert processed == 10, f"Expected 10 tasks recovered, got {processed}"
        
    finally:
        broker_proc.kill()
        broker_proc.wait()
        cleanup_db(db_path)


# -- Test 26: Partial-write task is not double-counted --------------------

async def test_no_double_count():
    """
    Produce 10 tasks. Simulate broker crash mid-write by truncating
    the WAL file/DB after task 7. Restart. Assert exactly 7 tasks are
    recovered, not 10 or 8.
    """
    db_path = DB_PATH + "_26.db"
    cleanup_db(db_path)
    port = PORT_BASE + 1
    
    broker_proc = start_broker(port, db_path)
    
    try:
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        for i in range(10):
            await producer.produce("truncate_q", {"i": i})
        await producer.close()
        
        broker_proc.kill()
        broker_proc.wait()
        
        # Simulate partial WAL write / crash mid-write by deleting last 3 tasks
        conn = sqlite3.connect(db_path)
        # Delete the 3 most recently created tasks
        conn.execute(
            "DELETE FROM tasks WHERE task_id IN "
            "(SELECT task_id FROM tasks ORDER BY created_at DESC LIMIT 3)"
        )
        conn.commit()
        conn.close()
        
        # Restart
        broker_proc = start_broker(port, db_path)
        
        # Check how many tasks are recovered
        worker = RawClient("127.0.0.1", port)
        await worker.connect()
        await worker.send({
            "command": "REGISTER",
            "worker_id": "truncate-worker",
            "queues": ["truncate_q"],
        })
        
        processed = 0
        for _ in range(15):
            resp = await worker.send({
                "command": "CONSUME",
                "queue": "truncate_q",
                "worker_id": "truncate-worker",
            })
            if resp["status"] == "empty":
                break
            if resp["status"] == "ok":
                await worker.send({
                    "command": "ACK",
                    "task_id": resp["task"]["task_id"],
                    "worker_id": "truncate-worker",
                })
                processed += 1
                
        await worker.close()
        
        assert processed == 7, f"Expected exactly 7 tasks recovered, got {processed}"
        
    finally:
        broker_proc.kill()
        broker_proc.wait()
        cleanup_db(db_path)


# -- Test 27: Worker crash mid-task ----------------------------------------

async def test_mid_task_worker_crash():
    """
    Enqueue 1 task. Worker picks it up. Kill the worker mid-execution
    (before ACK). Wait for eviction (8s). Start new worker. Assert task
    is processed exactly once.
    """
    db_path = DB_PATH + "_27.db"
    cleanup_db(db_path)
    port = PORT_BASE + 2
    
    broker_proc = start_broker(port, db_path)
    
    try:
        # Enqueue 1 task
        producer = ProducerClient("127.0.0.1", port)
        await producer.connect()
        tid = await producer.produce("reassign_q", {"data": "important"})
        await producer.close()
        
        # Start W1, pick up task, then hard disconnect
        w1 = RawClient("127.0.0.1", port)
        await w1.connect()
        await w1.send({
            "command": "REGISTER",
            "worker_id": "dying-worker",
            "queues": ["reassign_q"],
        })
        resp = await w1.send({
            "command": "CONSUME",
            "queue": "reassign_q",
            "worker_id": "dying-worker",
        })
        assert resp["status"] == "ok"
        
        # Kill W1 (close socket abruptly without ACK)
        await w1.close()
        
        # Wait for eviction (6s timeout + 2s check interval)
        print("    Waiting 8.5 seconds for dead worker eviction...")
        await asyncio.sleep(8.5)
        
        # Start W2
        w2 = RawClient("127.0.0.1", port)
        await w2.connect()
        await w2.send({
            "command": "REGISTER",
            "worker_id": "healthy-worker",
            "queues": ["reassign_q"],
        })
        
        resp2 = await w2.send({
            "command": "CONSUME",
            "queue": "reassign_q",
            "worker_id": "healthy-worker",
        })
        
        assert resp2["status"] == "ok", "Task was not reassigned"
        assert resp2["task"]["task_id"] == tid, "Reassigned wrong task"
        assert resp2["task"]["attempts"] == 2, (
            f"Expected attempt 2 after reassignment, got {resp2['task']['attempts']}"
        )
        
        await w2.send({
            "command": "ACK",
            "task_id": tid,
            "worker_id": "healthy-worker",
        })
        
        await w2.close()
        
    finally:
        broker_proc.kill()
        broker_proc.wait()
        cleanup_db(db_path)


# -- Main ------------------------------------------------------------------

async def run_tests():
    runner = TestRunner()

    await runner.run("Tasks survive broker restart", test_crash_recovery)
    await runner.run("Partial-write task is not double-counted after restart", test_no_double_count)
    await runner.run("Worker crash mid-task: task is reassigned, not lost", test_mid_task_worker_crash)

    print(f"\n{'=' * 60}")
    print(f"  June 22 Crash Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
