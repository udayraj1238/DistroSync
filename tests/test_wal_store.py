"""
Tests for the WAL-mode SQLite persistence layer.

Tests cover:
    1. Schema creation (tables and indexes exist)
    2. Task CRUD (save, load, update status, delete)
    3. Crash recovery (pending + in_flight tasks are recovered)
    4. DLQ operations (add, query, replay, purge, filtering)
    5. Atomic DLQ transition (task moves from tasks → DLQ in one tx)
    6. Broker metadata key-value store
    7. WAL mode verification
    8. Thread safety (thread-local connections)
    9. Concurrent read/write under WAL mode
    10. Stats reporting
"""

import asyncio
import os
import sys
import json
import time
import threading
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from persistence.wal_store import WALStore
from broker.queue_manager import Task


def make_task(task_id="test-001", queue_name="emails",
              payload=None, status="pending", attempts=0):
    """Helper to create a Task object for testing."""
    t = Task(
        task_id=task_id,
        queue_name=queue_name,
        payload=payload or {"to": "test@example.com"},
        status=status,
    )
    t.attempts = attempts
    t.created_at = time.time()
    return t


def run_test(name, fn):
    """Run a single test and report pass/fail."""
    try:
        fn()
        print(f"  PASS: {name}")
        return True
    except Exception as e:
        print(f"  FAIL: {name} — {e}")
        import traceback
        traceback.print_exc()
        return False


def test_schema_creation():
    """Test 1: Database schema is created with correct tables and indexes."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        conn = store._get_conn()

        # Check tables exist
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {row["name"] for row in tables}
        assert "tasks" in table_names, f"Missing 'tasks' table: {table_names}"
        assert "dead_letter_queue" in table_names, f"Missing 'dead_letter_queue' table"
        assert "broker_metadata" in table_names, f"Missing 'broker_metadata' table"

        # Check indexes exist
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        idx_names = {row["name"] for row in indexes}
        assert "idx_tasks_queue_status" in idx_names, "Missing queue_status index"
        assert "idx_dlq_queue" in idx_names, "Missing DLQ queue index"

        store.close()
    finally:
        os.unlink(db_path)


def test_wal_mode():
    """Test 2: WAL journal mode is active."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        conn = store._get_conn()
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal", f"Expected WAL mode, got: {mode}"
        store.close()
    finally:
        os.unlink(db_path)


def test_task_save_and_load():
    """Test 3: Tasks can be saved and loaded back."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        task = make_task("save-001", "emails", {"subject": "hello"})
        store.save_task(task)

        # Load pending tasks should return our task
        tasks = store.load_pending_tasks()
        assert len(tasks) == 1, f"Expected 1 task, got {len(tasks)}"
        assert tasks[0]["task_id"] == "save-001"
        assert tasks[0]["queue_name"] == "emails"
        assert json.loads(tasks[0]["payload"]) == {"subject": "hello"}
        assert tasks[0]["status"] == "pending"
        store.close()
    finally:
        os.unlink(db_path)


def test_task_status_update():
    """Test 4: Task status can be updated."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        task = make_task("update-001")
        store.save_task(task)

        # Update to in_flight
        result = store.update_task_status(
            "update-001", "in_flight", attempts=1, assigned_worker="w1"
        )
        assert result is True, "Update should return True"

        # Verify the update
        tasks = store.load_all_tasks()
        assert len(tasks) == 1
        assert tasks[0]["status"] == "in_flight"
        assert tasks[0]["attempts"] == 1
        assert tasks[0]["assigned_worker"] == "w1"

        # Update nonexistent task
        result = store.update_task_status("nonexistent", "done")
        assert result is False, "Nonexistent update should return False"

        store.close()
    finally:
        os.unlink(db_path)


def test_task_delete():
    """Test 5: Tasks can be deleted."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        task = make_task("delete-001")
        store.save_task(task)

        result = store.delete_task("delete-001")
        assert result is True, "Delete should return True"
        assert len(store.load_all_tasks()) == 0, "Tasks table should be empty"

        result = store.delete_task("nonexistent")
        assert result is False, "Delete nonexistent should return False"
        store.close()
    finally:
        os.unlink(db_path)


def test_crash_recovery():
    """Test 6: Crash recovery loads both pending and in_flight tasks."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        # Simulate pre-crash state: mix of statuses
        pending = make_task("crash-001", status="pending")
        in_flight = make_task("crash-002", status="in_flight")
        done = make_task("crash-003", status="done")

        store.save_task(pending)
        store.save_task(in_flight)
        store.save_task(done)

        # Recovery should get pending + in_flight, not done
        recovered = store.load_pending_tasks()
        recovered_ids = {t["task_id"] for t in recovered}
        assert "crash-001" in recovered_ids, "Pending task should be recovered"
        assert "crash-002" in recovered_ids, "In-flight task should be recovered"
        assert "crash-003" not in recovered_ids, "Done task should NOT be recovered"
        assert len(recovered) == 2, f"Expected 2 recovered, got {len(recovered)}"

        store.close()
    finally:
        os.unlink(db_path)


def test_dlq_add_and_query():
    """Test 7: DLQ add, query by queue, and query all."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        task1 = make_task("dlq-001", "emails", attempts=3)
        task2 = make_task("dlq-002", "images", attempts=3)

        # Also save to tasks table first (realistic scenario)
        store.save_task(task1)
        store.save_task(task2)

        # Move to DLQ (should also delete from tasks table)
        store.add_to_dlq(task1, "handler timeout")
        store.add_to_dlq(task2, "out of memory")

        # Query all DLQ
        dlq_all = store.get_dlq_tasks()
        assert len(dlq_all) == 2, f"Expected 2 DLQ tasks, got {len(dlq_all)}"

        # Query by queue
        dlq_emails = store.get_dlq_tasks(queue_name="emails")
        assert len(dlq_emails) == 1
        assert dlq_emails[0]["task_id"] == "dlq-001"
        assert dlq_emails[0]["final_error"] == "handler timeout"

        # Verify tasks were removed from main tasks table
        remaining = store.load_all_tasks()
        assert len(remaining) == 0, "Tasks should be deleted after DLQ move"

        store.close()
    finally:
        os.unlink(db_path)


def test_dlq_replay():
    """Test 8: DLQ tasks can be replayed (moved back to pending)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        task = make_task("replay-001", "emails", attempts=3)
        store.add_to_dlq(task, "network error")

        # Replay the task
        replayed = store.replay_dlq_task("replay-001")
        assert replayed is not None, "Replay should return task data"
        assert replayed["task_id"] == "replay-001"
        assert replayed["queue_name"] == "emails"

        # DLQ should be empty now
        assert store.get_dlq_count() == 0, "DLQ should be empty after replay"

        # Replay nonexistent should return None
        result = store.replay_dlq_task("nonexistent")
        assert result is None, "Replay nonexistent should return None"

        store.close()
    finally:
        os.unlink(db_path)


def test_dlq_purge():
    """Test 9: DLQ purge removes all or queue-specific tasks."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        # Add 3 DLQ tasks: 2 in 'emails', 1 in 'images'
        for i, q in enumerate(["emails", "emails", "images"]):
            task = make_task(f"purge-{i:03d}", q, attempts=3)
            store.add_to_dlq(task, "test error")

        assert store.get_dlq_count() == 3, "Should have 3 DLQ tasks"

        # Purge only 'emails'
        purged = store.purge_dlq("emails")
        assert purged == 2, f"Expected 2 purged, got {purged}"
        assert store.get_dlq_count() == 1, "Should have 1 remaining"

        # Purge all remaining
        purged = store.purge_dlq()
        assert purged == 1, f"Expected 1 purged, got {purged}"
        assert store.get_dlq_count() == 0, "DLQ should be empty"

        store.close()
    finally:
        os.unlink(db_path)


def test_broker_metadata():
    """Test 10: Broker metadata key-value store works."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        # Set and get
        store.set_metadata("broker_epoch", "42")
        assert store.get_metadata("broker_epoch") == "42"

        # Overwrite
        store.set_metadata("broker_epoch", "43")
        assert store.get_metadata("broker_epoch") == "43"

        # Get nonexistent
        assert store.get_metadata("nonexistent") is None

        store.close()
    finally:
        os.unlink(db_path)


def test_thread_safety():
    """Test 11: Thread-local connections work correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)
        results = []

        def write_from_thread(thread_id):
            """Each thread writes a task using its own connection."""
            try:
                task = make_task(f"thread-{thread_id}", "concurrent")
                store.save_task(task)
                results.append(("ok", thread_id))
            except Exception as e:
                results.append(("error", thread_id, str(e)))
            finally:
                # Close the thread-local connection so Windows can delete the file
                store.close()

        threads = [
            threading.Thread(target=write_from_thread, args=(i,))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should succeed
        errors = [r for r in results if r[0] == "error"]
        assert len(errors) == 0, f"Thread errors: {errors}"

        # All 5 tasks should be persisted
        all_tasks = store.load_all_tasks()
        assert len(all_tasks) == 5, f"Expected 5 tasks, got {len(all_tasks)}"

        store.close()
    finally:
        os.unlink(db_path)


def test_stats():
    """Test 12: Stats reporting works."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        store = WALStore(db_path)

        task1 = make_task("stats-001", status="pending")
        task2 = make_task("stats-002", status="in_flight")
        store.save_task(task1)
        store.save_task(task2)

        dlq_task = make_task("stats-003", attempts=3)
        store.add_to_dlq(dlq_task, "error")

        stats = store.get_stats()
        assert stats["tasks_by_status"]["pending"] == 1
        assert stats["tasks_by_status"]["in_flight"] == 1
        assert stats["dlq_count"] == 1
        assert stats["db_path"] == db_path

        store.close()
    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    tests = [
        ("Schema creation", test_schema_creation),
        ("WAL mode active", test_wal_mode),
        ("Task save and load", test_task_save_and_load),
        ("Task status update", test_task_status_update),
        ("Task delete", test_task_delete),
        ("Crash recovery", test_crash_recovery),
        ("DLQ add and query", test_dlq_add_and_query),
        ("DLQ replay", test_dlq_replay),
        ("DLQ purge", test_dlq_purge),
        ("Broker metadata", test_broker_metadata),
        ("Thread safety", test_thread_safety),
        ("Stats reporting", test_stats),
    ]

    passed = 0
    failed = 0

    print(f"\n--- WAL Store Tests ({len(tests)} tests) ---\n")
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    print(f"{'=' * 50}\n")

    sys.exit(0 if failed == 0 else 1)
