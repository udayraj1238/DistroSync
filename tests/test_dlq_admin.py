"""
Tests for DLQ Admin API — the operator-facing commands for inspecting,
replaying, and purging dead-lettered tasks.

Tests cover:
    1. DLQ_LIST returns all DLQ tasks
    2. DLQ_LIST with queue filter returns only matching tasks
    3. DLQ_LIST with limit caps the results
    4. DLQ_LIST returns empty when no tasks exist
    5. DLQ_REPLAY moves a task from DLQ back to its queue
    6. DLQ_REPLAY with persistence (WAL store)
    7. DLQ_REPLAY returns error for nonexistent task
    8. DLQ_PURGE removes all DLQ tasks
    9. DLQ_PURGE with queue filter removes only matching tasks
    10. STATS returns aggregated broker statistics
"""

import asyncio
import os
import sys
import json
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from broker.queue_manager import Task


class FakeWriter:
    """Minimal mock for asyncio.StreamWriter to satisfy handler signatures."""
    def get_extra_info(self, key):
        return ("127.0.0.1", 9999)


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
        print(f"  FAIL: {name} -- {e}")
        import traceback
        traceback.print_exc()
        return False


async def _push_task_to_dlq(broker, queue_name, payload, worker_id="w1"):
    """
    Helper: produce a task and NACK it 3 times to move it to DLQ.
    Returns the task_id of the dead-lettered task.
    """
    resp = await broker._handle_produce(
        {"command": "PRODUCE", "queue": queue_name, "task": payload},
        None
    )
    task_id = resp["task_id"]

    for _ in range(3):
        await broker._handle_consume(
            {"command": "CONSUME", "queue": queue_name, "worker_id": worker_id},
            None
        )
        await broker._handle_nack(
            {"command": "NACK", "task_id": task_id, "worker_id": worker_id},
            None
        )

    return task_id


async def test_dlq_list_all():
    """Test 1: DLQ_LIST returns all dead-lettered tasks."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    task1_id = await _push_task_to_dlq(broker, "emails", {"to": "a@b.com"})
    task2_id = await _push_task_to_dlq(broker, "images", {"file": "cat.jpg"})

    resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert resp["status"] == "ok"
    assert resp["count"] == 2, f"Expected 2 DLQ tasks, got {resp['count']}"
    task_ids = {t["task_id"] for t in resp["tasks"]}
    assert task1_id in task_ids
    assert task2_id in task_ids


async def test_dlq_list_queue_filter():
    """Test 2: DLQ_LIST with queue filter returns only matching tasks."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    await _push_task_to_dlq(broker, "emails", {"to": "x@y.com"})
    await _push_task_to_dlq(broker, "images", {"file": "dog.jpg"})

    resp = await broker._handle_dlq_list(
        {"command": "DLQ_LIST", "queue": "emails"}, None
    )
    assert resp["count"] == 1, f"Expected 1 email task, got {resp['count']}"
    assert resp["tasks"][0]["queue_name"] == "emails"


async def test_dlq_list_with_limit():
    """Test 3: DLQ_LIST respects the limit parameter."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    for i in range(5):
        await _push_task_to_dlq(broker, "batch", {"job": i})

    resp = await broker._handle_dlq_list(
        {"command": "DLQ_LIST", "limit": 2}, None
    )
    assert resp["count"] == 2, f"Expected 2 (limited), got {resp['count']}"


async def test_dlq_list_empty():
    """Test 4: DLQ_LIST returns empty list when no DLQ tasks exist."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert resp["status"] == "ok"
    assert resp["count"] == 0
    assert resp["tasks"] == []


async def test_dlq_replay_in_memory():
    """Test 5: DLQ_REPLAY moves a task from DLQ back to its original queue."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    task_id = await _push_task_to_dlq(broker, "emails", {"to": "replay@test.com"})

    # Verify it's in the DLQ
    dlq_resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert dlq_resp["count"] == 1

    # Replay it
    replay_resp = await broker._handle_dlq_replay(
        {"command": "DLQ_REPLAY", "task_id": task_id}, None
    )
    assert replay_resp["status"] == "ok"
    assert replay_resp["action"] == "replayed"
    assert replay_resp["original_task_id"] == task_id

    # DLQ should be empty
    dlq_resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert dlq_resp["count"] == 0, "DLQ should be empty after replay"

    # Task should be back in the queue
    assert broker.queue_manager.queue_depth("emails") == 1


async def test_dlq_replay_with_persistence():
    """Test 6: DLQ_REPLAY works correctly with WAL store persistence."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        import logging
        logging.getLogger("broker").setLevel(logging.WARNING)
        logging.getLogger("persistence").setLevel(logging.WARNING)

        broker = BrokerServer(host="127.0.0.1", port=0, db_path=db_path)
        await broker._handle_register(
            {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
            FakeWriter()
        )

        task_id = await _push_task_to_dlq(broker, "emails", {"to": "wal@test.com"})

        # Verify DLQ in WAL store
        dlq_tasks = broker.wal_store.get_dlq_tasks()
        assert len(dlq_tasks) == 1, f"WAL DLQ should have 1 task, got {len(dlq_tasks)}"

        # Replay
        replay_resp = await broker._handle_dlq_replay(
            {"command": "DLQ_REPLAY", "task_id": task_id}, None
        )
        assert replay_resp["status"] == "ok"
        assert replay_resp["action"] == "replayed"
        new_task_id = replay_resp["new_task_id"]
        assert new_task_id != task_id, "Replayed task should get a new UUID"

        # WAL DLQ should be empty, new task should be in tasks table
        dlq_tasks = broker.wal_store.get_dlq_tasks()
        assert len(dlq_tasks) == 0, "WAL DLQ should be empty after replay"

        all_tasks = broker.wal_store.load_all_tasks()
        pending = [t for t in all_tasks if t["status"] == "pending"]
        assert len(pending) >= 1, "New task should be persisted as pending"

        broker.wal_store.close()
    finally:
        for ext in ["", "-wal", "-shm"]:
            path = db_path + ext
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except PermissionError:
                    pass


async def test_dlq_replay_nonexistent():
    """Test 7: DLQ_REPLAY returns error for a task not in the DLQ."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    resp = await broker._handle_dlq_replay(
        {"command": "DLQ_REPLAY", "task_id": "nonexistent-uuid"}, None
    )
    assert resp["status"] == "error"
    assert "not found" in resp["reason"]


async def test_dlq_purge_all():
    """Test 8: DLQ_PURGE removes all DLQ tasks."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    for i in range(3):
        await _push_task_to_dlq(broker, f"q{i}", {"job": i})

    # Verify DLQ has 3 tasks
    dlq_resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert dlq_resp["count"] == 3

    # Purge all
    purge_resp = await broker._handle_dlq_purge(
        {"command": "DLQ_PURGE"}, None
    )
    assert purge_resp["status"] == "ok"
    assert purge_resp["action"] == "purged"
    assert purge_resp["count"] == 3

    # DLQ should be empty
    dlq_resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert dlq_resp["count"] == 0


async def test_dlq_purge_by_queue():
    """Test 9: DLQ_PURGE with queue filter only removes matching tasks."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    await _push_task_to_dlq(broker, "emails", {"to": "a@b.com"})
    await _push_task_to_dlq(broker, "emails", {"to": "c@d.com"})
    await _push_task_to_dlq(broker, "images", {"file": "pic.jpg"})

    # Purge only emails
    purge_resp = await broker._handle_dlq_purge(
        {"command": "DLQ_PURGE", "queue": "emails"}, None
    )
    assert purge_resp["count"] == 2, f"Expected 2 purged, got {purge_resp['count']}"

    # images should still be in DLQ
    dlq_resp = await broker._handle_dlq_list({"command": "DLQ_LIST"}, None)
    assert dlq_resp["count"] == 1
    assert dlq_resp["tasks"][0]["queue_name"] == "images"


async def test_stats():
    """Test 10: STATS returns aggregated broker statistics."""
    broker = BrokerServer(host="127.0.0.1", port=0)
    await broker._handle_register(
        {"command": "REGISTER", "worker_id": "w1", "queues": ["q1"]},
        FakeWriter()
    )

    # Produce some tasks
    await broker._handle_produce(
        {"command": "PRODUCE", "queue": "test_q", "task": {"data": 1}}, None
    )
    await broker._handle_produce(
        {"command": "PRODUCE", "queue": "test_q", "task": {"data": 2}}, None
    )

    resp = await broker._handle_stats({"command": "STATS"}, None)
    assert resp["status"] == "ok"
    assert "queue_manager" in resp
    assert "worker_registry" in resp
    assert "load_shedder" in resp
    assert resp["queue_manager"]["total_enqueued"] == 2
    assert resp["active_connections"] == 0


if __name__ == "__main__":
    import logging
    logging.getLogger("broker").setLevel(logging.WARNING)
    logging.getLogger("persistence").setLevel(logging.WARNING)

    tests = [
        ("DLQ_LIST returns all tasks", test_dlq_list_all),
        ("DLQ_LIST with queue filter", test_dlq_list_queue_filter),
        ("DLQ_LIST with limit", test_dlq_list_with_limit),
        ("DLQ_LIST empty", test_dlq_list_empty),
        ("DLQ_REPLAY in-memory", test_dlq_replay_in_memory),
        ("DLQ_REPLAY with persistence", test_dlq_replay_with_persistence),
        ("DLQ_REPLAY nonexistent task", test_dlq_replay_nonexistent),
        ("DLQ_PURGE all", test_dlq_purge_all),
        ("DLQ_PURGE by queue", test_dlq_purge_by_queue),
        ("STATS aggregated", test_stats),
    ]

    passed = 0
    failed = 0

    print(f"\n--- DLQ Admin API Tests ({len(tests)} tests) ---\n")
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}\n")

    sys.exit(0 if failed == 0 else 1)
