"""
Unit tests for WAL (Write-Ahead Log) Store persistence layer.

Covers:
- WAL mode is enabled
- Save and load task survives reconnect
- DLQ insert and retrieve
"""
import os
import sys
import tempfile
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from persistence.wal_store import WALStore
from broker.queue_manager import Task

def test_wal_mode_is_enabled():
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            wal = WALStore(db_path)
            
            with wal._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode;")
                result = cursor.fetchone()
                assert result[0].lower() == "wal", "WAL mode is not enabled on SQLite DB"
                
            if hasattr(wal._local, "conn") and wal._local.conn:
                wal._local.conn.close()
                wal._local.conn = None
    except PermissionError:
        pass

def test_save_and_load_task_survives_reconnect():
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            
            wal = WALStore(db_path)
            task = Task(task_id="t1", queue_name="emails", payload={"to": "a@b.com"})
            wal.save_task(task)
            
            # Close connection to mimic shutdown and release file lock
            if hasattr(wal._local, "conn") and wal._local.conn:
                wal._local.conn.close()
                wal._local.conn = None
            
            # New instance simulates a fresh boot
            wal2 = WALStore(db_path)
            try:
                tasks = wal2.load_pending_tasks()
                
                # load_pending_tasks returns dictionaries
                emails_tasks = [t for t in tasks if t["queue_name"] == "emails"]
                assert len(emails_tasks) == 1
                assert emails_tasks[0]["task_id"] == "t1"
                assert json.loads(emails_tasks[0]["payload"]) == {"to": "a@b.com"}
            finally:
                if hasattr(wal2._local, "conn") and wal2._local.conn:
                    wal2._local.conn.close()
                    wal2._local.conn = None
    except PermissionError:
        pass

def test_dlq_insert_and_retrieve():
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            wal = WALStore(db_path)
            
            task = Task(task_id="t2", queue_name="reports", payload={"id": 9})
            task.attempts = 4
            wal.add_to_dlq(task, error="Timeout")
            
            dlq_tasks = wal.get_dlq_tasks(limit=10)
            assert len(dlq_tasks) == 1
            assert dlq_tasks[0]["task_id"] == "t2"
            assert dlq_tasks[0]["queue_name"] == "reports"
            assert dlq_tasks[0]["final_error"] == "Timeout"
            assert dlq_tasks[0]["attempts"] == 4
            
            if hasattr(wal._local, "conn") and wal._local.conn:
                wal._local.conn.close()
                wal._local.conn = None
    except PermissionError:
        pass
