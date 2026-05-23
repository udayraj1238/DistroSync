"""
Tests for crash recovery — verifying that the broker can survive
restarts and recover tasks from the WAL store.

Tests cover:
    1. enqueue_recovered preserves task_id and attempt count
    2. Full crash recovery: enqueue → simulate crash → recover → verify
    3. In-flight tasks are recovered as pending after crash
    4. Done tasks are NOT recovered (already completed)
    5. DLQ tasks are NOT recovered into the main queue
    6. Broker server with persistence: produce → ACK → verify WAL
    7. Broker server crash recovery: tasks survive restart
    8. Multiple queues recover independently
"""

import asyncio
import os
import sys
import json
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.queue_manager import QueueManager, Task
from broker.server import BrokerServer
from persistence.wal_store import WALStore


def run_test(name, fn):
    """Run a single async or sync test and report pass/fail."""
    try:
        if asyncio.iscoroutinefunction(fn):
            asyncio.run(fn())
        else:
            fn()
        print(f"  PASS: {name}")
        return True
    except Exception as e:
        print(f"  FAIL: {name} — {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_enqueue_recovered_preserves_id():
    """Test 1: enqueue_recovered keeps the original task_id and attempts."""
    qm = QueueManager()
    original_id = "preserved-uuid-001"

    result_id = await qm.enqueue_recovered(
        queue_name="emails",
        task_id=original_id,
        payload={"to": "alice@example.com"},
        attempts=2,
    )

    assert result_id == original_id, f"Expected {original_id}, got {result_id}"
    assert qm.queue_depth("emails") == 1, "Queue should have 1 task"

    # Dequeue and verify the task preserved its identity
    task = await qm.dequeue("emails", "w1")
    assert task["task_id"] == original_id, "task_id must be preserved"
    assert task["attempts"] == 3, "Dequeue increments attempts (2 → 3)"
    assert task["payload"]["to"] == "alice@example.com"


async def test_full_crash_recovery_cycle():
    """Test 2: Complete crash → recovery cycle using WAL store."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        # --- Phase 1: Create tasks and persist them ---
        store = WALStore(db_path)
        qm = QueueManager()

        task_id = await qm.enqueue("emails", {"subject": "hello"})
        # Find the task and save it
        for t in qm._queues["emails"]:
            if t.task_id == task_id:
                store.save_task(t)
                break

        # Simulate: worker dequeues the task
        task_data = await qm.dequeue("emails", "w1")
        store.update_task_status(task_id, "in_flight", attempts=1)

        store.close()

        # --- Phase 2: Simulate crash (new QueueManager, new WALStore) ---
        store2 = WALStore(db_path)
        qm2 = QueueManager()

        # Recovery: reload pending + in_flight tasks
        pending = store2.load_pending_tasks()
        assert len(pending) == 1, f"Expected 1 recoverable task, got {len(pending)}"
        assert pending[0]["status"] == "in_flight", "Should recover in_flight task"

        # Re-enqueue with preserved identity
        row = pending[0]
        payload = json.loads(row["payload"])
        await qm2.enqueue_recovered(
            queue_name=row["queue_name"],
            task_id=row["task_id"],
            payload=payload,
            attempts=row["attempts"],
        )

        # Verify recovered task is available
        assert qm2.queue_depth("emails") == 1
        recovered_task = await qm2.dequeue("emails", "w2")
        assert recovered_task["task_id"] == task_id, "Same task_id after crash"
        assert recovered_task["payload"]["subject"] == "hello"
        # Attempts: was 1 in WAL, dequeue increments to 2
        assert recovered_task["attempts"] == 2

        store2.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_done_tasks_not_recovered():
    """Test 3: Completed tasks should NOT be recovered after crash."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        qm = QueueManager()

        # Enqueue and immediately complete
        task_id = await qm.enqueue("emails", {"done": True})
        for t in qm._queues["emails"]:
            if t.task_id == task_id:
                store.save_task(t)
                break
        await qm.dequeue("emails", "w1")
        await qm.acknowledge(task_id)
        store.update_task_status(task_id, "done")

        # Recovery should find nothing
        pending = store.load_pending_tasks()
        assert len(pending) == 0, f"Done tasks should not be recovered, got {len(pending)}"

        store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_dlq_tasks_not_in_recovery():
    """Test 4: DLQ tasks should NOT appear in pending recovery."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        # Create a task and move it to DLQ
        task = Task(
            task_id="dlq-test-001",
            queue_name="emails",
            payload={"failed": True},
        )
        task.attempts = 3
        store.save_task(task)
        store.add_to_dlq(task, "permanent failure")

        # Recovery should find nothing (task is in DLQ, not tasks table)
        pending = store.load_pending_tasks()
        assert len(pending) == 0, "DLQ tasks must not appear in recovery"

        # But the DLQ should have it
        dlq = store.get_dlq_tasks()
        assert len(dlq) == 1, "DLQ should have the task"
        assert dlq[0]["task_id"] == "dlq-test-001"

        store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_multi_queue_recovery():
    """Test 5: Tasks from different queues recover independently."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        qm = QueueManager()

        # Enqueue tasks into different queues
        ids = {}
        for q in ["emails", "images", "notifications"]:
            tid = await qm.enqueue(q, {"queue": q})
            for t in qm._queues[q]:
                if t.task_id == tid:
                    store.save_task(t)
                    ids[q] = tid
                    break

        # Complete the images task
        await qm.dequeue("images", "w1")
        await qm.acknowledge(ids["images"])
        store.update_task_status(ids["images"], "done")

        # Recovery should find emails + notifications, not images
        pending = store.load_pending_tasks()
        recovered_ids = {row["task_id"] for row in pending}
        assert ids["emails"] in recovered_ids, "Email task should recover"
        assert ids["notifications"] in recovered_ids, "Notification should recover"
        assert ids["images"] not in recovered_ids, "Completed image should NOT recover"
        assert len(pending) == 2

        store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_broker_persistence_produce_ack():
    """Test 6: Broker with persistence stores and updates task status."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        import logging
        logging.getLogger("broker").setLevel(logging.WARNING)
        logging.getLogger("persistence").setLevel(logging.WARNING)

        broker = BrokerServer(host="127.0.0.1", port=0, db_path=db_path)

        # Manually exercise the handlers (no TCP needed)
        # Produce a task
        produce_resp = await broker._handle_produce(
            {"command": "PRODUCE", "queue": "test_q", "task": {"data": 42}},
            None
        )
        assert produce_resp["status"] == "ok", f"Produce failed: {produce_resp}"
        task_id = produce_resp["task_id"]

        # Check WAL store has the task
        tasks = broker.wal_store.load_all_tasks()
        assert len(tasks) == 1, f"WAL should have 1 task, got {len(tasks)}"
        assert tasks[0]["status"] == "pending"

        # Register a worker and consume
        await broker._handle_register(
            {"command": "REGISTER", "worker_id": "w1", "queues": ["test_q"]},
            type("FakeWriter", (), {"get_extra_info": lambda self, k: ("127.0.0.1", 9999)})()
        )
        consume_resp = await broker._handle_consume(
            {"command": "CONSUME", "queue": "test_q", "worker_id": "w1"},
            None
        )
        assert consume_resp["status"] == "ok"

        # Check WAL: status should be in_flight
        tasks = broker.wal_store.load_all_tasks()
        assert tasks[0]["status"] == "in_flight"

        # ACK the task
        ack_resp = await broker._handle_ack(
            {"command": "ACK", "task_id": task_id, "worker_id": "w1"},
            None
        )
        assert ack_resp["status"] == "ok"

        # Check WAL: status should be done
        tasks = broker.wal_store.load_all_tasks()
        assert tasks[0]["status"] == "done"

        broker.wal_store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_broker_crash_recovery_on_start():
    """Test 7: Broker recovers tasks from WAL store on startup."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        import logging
        logging.getLogger("broker").setLevel(logging.WARNING)
        logging.getLogger("persistence").setLevel(logging.WARNING)

        # Phase 1: Produce tasks with first broker instance
        broker1 = BrokerServer(host="127.0.0.1", port=0, db_path=db_path)
        for i in range(3):
            await broker1._handle_produce(
                {"command": "PRODUCE", "queue": "crash_q", "task": {"job": i}},
                None
            )

        # Consume one task (makes it in_flight in WAL)
        await broker1._handle_register(
            {"command": "REGISTER", "worker_id": "w1", "queues": ["crash_q"]},
            type("FW", (), {"get_extra_info": lambda s, k: ("127.0.0.1", 1)})()
        )
        await broker1._handle_consume(
            {"command": "CONSUME", "queue": "crash_q", "worker_id": "w1"},
            None
        )
        # Don't ACK — simulate crash. Close the WAL store.
        broker1.wal_store.close()

        # Phase 2: New broker instance — should recover all 3 tasks
        broker2 = BrokerServer(host="127.0.0.1", port=0, db_path=db_path)

        # Run the recovery portion of start() manually
        pending_tasks = broker2.wal_store.load_pending_tasks()
        recovered = 0
        for row in pending_tasks:
            payload = json.loads(row["payload"])
            await broker2.queue_manager.enqueue_recovered(
                queue_name=row["queue_name"],
                task_id=row["task_id"],
                payload=payload,
                attempts=row["attempts"],
            )
            recovered += 1

        # All 3 tasks should be recovered (2 pending + 1 in_flight)
        assert recovered == 3, f"Expected 3 recovered, got {recovered}"
        assert broker2.queue_manager.queue_depth("crash_q") == 3

        broker2.wal_store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_attempt_count_preserved_through_crash():
    """Test 8: Attempt counts survive crashes for correct DLQ routing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        import logging
        logging.getLogger("broker").setLevel(logging.WARNING)
        logging.getLogger("persistence").setLevel(logging.WARNING)

        # Phase 1: Produce and NACK twice (attempts = 2)
        broker1 = BrokerServer(host="127.0.0.1", port=0, db_path=db_path)
        resp = await broker1._handle_produce(
            {"command": "PRODUCE", "queue": "retry_q", "task": {"fragile": True}},
            None
        )
        task_id = resp["task_id"]

        # Register worker
        await broker1._handle_register(
            {"command": "REGISTER", "worker_id": "w1", "queues": ["retry_q"]},
            type("FW", (), {"get_extra_info": lambda s, k: ("127.0.0.1", 1)})()
        )

        # NACK twice (attempt 1, then attempt 2)
        for _ in range(2):
            await broker1._handle_consume(
                {"command": "CONSUME", "queue": "retry_q", "worker_id": "w1"},
                None
            )
            await broker1._handle_nack(
                {"command": "NACK", "task_id": task_id, "worker_id": "w1"},
                None
            )

        broker1.wal_store.close()

        # Phase 2: Recover and verify attempts are preserved
        store2 = WALStore(db_path)
        pending = store2.load_pending_tasks()
        assert len(pending) == 1
        assert pending[0]["attempts"] == 2, (
            f"Attempts should be 2 after 2 NACKs, got {pending[0]['attempts']}"
        )

        # One more NACK should send it to DLQ (attempt 3 >= max_retries 3)
        store2.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


if __name__ == "__main__":
    tests = [
        ("enqueue_recovered preserves id and attempts", test_enqueue_recovered_preserves_id),
        ("Full crash recovery cycle", test_full_crash_recovery_cycle),
        ("Done tasks not recovered", test_done_tasks_not_recovered),
        ("DLQ tasks not in recovery", test_dlq_tasks_not_in_recovery),
        ("Multi-queue recovery", test_multi_queue_recovery),
        ("Broker persistence: produce, ACK, WAL", test_broker_persistence_produce_ack),
        ("Broker crash recovery on start", test_broker_crash_recovery_on_start),
        ("Attempt count preserved through crash", test_attempt_count_preserved_through_crash),
    ]

    passed = 0
    failed = 0

    print(f"\n--- Crash Recovery Tests ({len(tests)} tests) ---\n")
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}\n")

    sys.exit(0 if failed == 0 else 1)
