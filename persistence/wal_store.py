"""
WAL-Mode SQLite Store — Crash-safe persistence for DistroSync.

This module provides durable, crash-safe storage for tasks and the Dead
Letter Queue using SQLite in Write-Ahead Logging (WAL) mode.

Why WAL mode?
    Normal SQLite uses a rollback journal: before writing to the database
    file, it copies the original pages to a journal, then modifies the
    database file directly. If a crash happens mid-write, the journal is
    used to restore the database to its pre-write state.

    WAL mode flips this: writes go to a separate .wal file first, and
    the main database file is only updated during periodic checkpoints.
    This has two key advantages:
        1. Readers and writers don't block each other (readers read from
           the main DB + WAL, writers append to the WAL)
        2. A crash mid-write only loses the uncommitted WAL entry —
           the main database is always in a consistent state

    This is the same approach used by:
        - Firefox (stores history, bookmarks, cookies in WAL-mode SQLite)
        - Android (all app databases default to WAL mode since API 16)
        - Apple's Core Data (WAL mode for concurrent access)

Threading model:
    SQLite connections are not safely shareable across threads. This
    module uses threading.local() to give each thread its own connection.
    Within each connection, we enable WAL mode, NORMAL synchronous mode
    (safe for WAL — only FULL is needed for non-WAL), and foreign keys.

Schema:
    tasks:              Active tasks (pending, in_flight, done)
    dead_letter_queue:  Permanently failed tasks for inspection/replay
    broker_metadata:    Key-value store for broker state (epoch, etc.)

Performance notes:
    - PRAGMA synchronous=NORMAL: WAL-safe. Writes are ~2x faster than
      FULL because the OS isn't forced to flush after every commit.
    - PRAGMA journal_mode=WAL: Set once, persists across connections.
    - Index on (queue_name, status) covers the two most common queries:
      "give me pending tasks in queue X" and "recovery: reload all pending".
"""

import sqlite3
import json
import time
import threading
import logging
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

DB_PATH = "distrosync.db"


class WALStore:
    """
    SQLite WAL-mode persistence backend for the DistroSync broker.

    This class handles all database interactions. It's designed to be
    used by the QueueManager and DeadLetterQueue as an optional
    persistence layer — the broker can run without it (in-memory only)
    or with it (crash-safe).

    Each thread gets its own SQLite connection via threading.local(),
    preventing the "ProgrammingError: SQLite objects created in a thread
    can only be used in that same thread" issue.

    Usage:
        store = WALStore("distrosync.db")
        store.save_task(task)                    # Persist a task
        tasks = store.load_pending_tasks()       # Recover after crash
        store.update_task_status(task_id, "done") # Mark complete
        store.add_to_dlq(task, "handler crashed") # Move to DLQ
        dlq = store.get_dlq_tasks("emails")      # Query the DLQ
        task = store.replay_dlq_task(task_id)     # Replay a DLQ task
    """

    def __init__(self, db_path: str = DB_PATH):
        """
        Initialize the WAL store.

        Args:
            db_path: Path to the SQLite database file. The file and its
                     WAL/SHM companion files will be created automatically.
                     Use ":memory:" for testing (no disk I/O).
        """
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
        logger.info(
            f"WALStore initialized (db={db_path}, "
            f"journal_mode=WAL, synchronous=NORMAL)"
        )

    def _get_conn(self) -> sqlite3.Connection:
        """
        Get or create a thread-local SQLite connection.

        Each thread gets its own connection to avoid SQLite's thread-safety
        constraints. The connection is configured with:
            - WAL journal mode (concurrent reads + writes)
            - NORMAL synchronous mode (safe for WAL, faster than FULL)
            - Foreign key enforcement (data integrity)
            - Row factory (access columns by name, not index)
        """
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        """
        Create the database schema if it doesn't exist.

        This is idempotent — safe to call on every startup. The schema
        uses IF NOT EXISTS so it won't clobber existing data.

        Tables:
            tasks:              Active tasks in the system
            dead_letter_queue:  Tasks that have permanently failed
            broker_metadata:    Key-value pairs for broker state

        Indexes:
            idx_tasks_queue_status: Covers the most common queries
                (pending tasks in a specific queue, recovery queries)
        """
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id         TEXT PRIMARY KEY,
                queue_name      TEXT NOT NULL,
                payload         TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                attempts        INTEGER NOT NULL DEFAULT 0,
                assigned_worker TEXT,
                created_at      REAL NOT NULL,
                updated_at      REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                task_id             TEXT PRIMARY KEY,
                queue_name          TEXT NOT NULL,
                payload             TEXT NOT NULL,
                final_error         TEXT,
                attempts            INTEGER NOT NULL,
                dead_lettered_at    REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS broker_metadata (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_queue_status
                ON tasks(queue_name, status);

            CREATE INDEX IF NOT EXISTS idx_dlq_queue
                ON dead_letter_queue(queue_name);
        """)
        conn.commit()

    # ── Task Operations ───────────────────────────────────────────

    def save_task(self, task) -> None:
        """
        Persist a task to the database (insert or update).

        Uses INSERT OR REPLACE so this works for both new tasks and
        status updates. The updated_at timestamp is always refreshed.

        Args:
            task: A Task object (from queue_manager.Task dataclass).
        """
        now = time.time()
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO tasks
            (task_id, queue_name, payload, status, attempts,
             assigned_worker, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task.task_id,
            task.queue_name,
            json.dumps(task.payload),
            task.status,
            task.attempts,
            getattr(task, "assigned_worker", None),
            getattr(task, "created_at", now),
            now,
        ))
        conn.commit()

    def update_task_status(self, task_id: str, status: str,
                           attempts: Optional[int] = None,
                           assigned_worker: Optional[str] = None) -> bool:
        """
        Update only the status (and optionally attempts/worker) of a task.

        More efficient than save_task() when you only need to change the
        status field (e.g., pending -> in_flight -> done).

        Args:
            task_id:         The UUID of the task.
            status:          New status value.
            attempts:        New attempt count (if changed).
            assigned_worker: New assigned worker (if changed).

        Returns:
            True if the task was found and updated, False otherwise.
        """
        conn = self._get_conn()
        now = time.time()

        if attempts is not None and assigned_worker is not None:
            cursor = conn.execute("""
                UPDATE tasks
                SET status = ?, attempts = ?, assigned_worker = ?, updated_at = ?
                WHERE task_id = ?
            """, (status, attempts, assigned_worker, now, task_id))
        elif attempts is not None:
            cursor = conn.execute("""
                UPDATE tasks SET status = ?, attempts = ?, updated_at = ?
                WHERE task_id = ?
            """, (status, attempts, now, task_id))
        elif assigned_worker is not None:
            cursor = conn.execute("""
                UPDATE tasks SET status = ?, assigned_worker = ?, updated_at = ?
                WHERE task_id = ?
            """, (status, assigned_worker, now, task_id))
        else:
            cursor = conn.execute("""
                UPDATE tasks SET status = ?, updated_at = ?
                WHERE task_id = ?
            """, (status, now, task_id))

        conn.commit()
        return cursor.rowcount > 0

    def delete_task(self, task_id: str) -> bool:
        """
        Remove a task from the tasks table entirely.

        Called when a task is moved to the DLQ (it gets a new row
        in dead_letter_queue and is removed from tasks).

        Args:
            task_id: The UUID of the task to delete.

        Returns:
            True if the task was found and deleted.
        """
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM tasks WHERE task_id = ?", (task_id,)
        )
        conn.commit()
        return cursor.rowcount > 0

    def load_pending_tasks(self) -> List[Dict]:
        """
        Load all recoverable tasks from the database.

        Called on broker startup to recover tasks from before a crash.
        Both 'pending' and 'in_flight' tasks are recovered — in_flight
        tasks are treated as pending because the worker that was
        processing them may have crashed along with the broker.

        Returns:
            List of task dicts ordered by creation time (oldest first).
        """
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT * FROM tasks
            WHERE status IN ('pending', 'in_flight')
            ORDER BY created_at ASC
        """).fetchall()

        tasks = [dict(row) for row in rows]
        if tasks:
            logger.info(
                f"WALStore recovered {len(tasks)} tasks "
                f"(pending + in_flight) from disk"
            )
        return tasks

    def load_all_tasks(self) -> List[Dict]:
        """
        Load all tasks regardless of status.

        Useful for debugging and admin inspection.

        Returns:
            List of all task dicts.
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY created_at ASC"
        ).fetchall()
        return [dict(row) for row in rows]

    # ── Dead Letter Queue Operations ──────────────────────────────

    def add_to_dlq(self, task, error: str = "") -> None:
        """
        Move a permanently failed task to the Dead Letter Queue.

        This is an atomic operation: the task is inserted into the DLQ
        table and deleted from the tasks table in a single transaction.

        Args:
            task:  The Task object that has exhausted its retries.
            error: A description of the final error.
        """
        now = time.time()
        conn = self._get_conn()

        # Atomic: insert into DLQ + delete from tasks in one transaction
        conn.execute("BEGIN")
        try:
            conn.execute("""
                INSERT OR REPLACE INTO dead_letter_queue
                (task_id, queue_name, payload, final_error, attempts,
                 dead_lettered_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                task.task_id,
                task.queue_name,
                json.dumps(task.payload),
                error,
                task.attempts,
                now,
            ))
            conn.execute(
                "DELETE FROM tasks WHERE task_id = ?",
                (task.task_id,)
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        logger.info(
            f"WALStore: task {task.task_id[:8]}... moved to DLQ "
            f"(error: {error[:50] if error else 'none'})"
        )

    def get_dlq_tasks(self, queue_name: Optional[str] = None,
                      limit: int = 100) -> List[Dict]:
        """
        Query the Dead Letter Queue.

        Supports filtering by queue name for targeted inspection.

        Args:
            queue_name: Optional queue filter. None returns all DLQ tasks.
            limit:      Maximum results to return.

        Returns:
            List of DLQ task dicts, newest first.
        """
        conn = self._get_conn()
        if queue_name:
            rows = conn.execute("""
                SELECT * FROM dead_letter_queue
                WHERE queue_name = ?
                ORDER BY dead_lettered_at DESC
                LIMIT ?
            """, (queue_name, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM dead_letter_queue
                ORDER BY dead_lettered_at DESC
                LIMIT ?
            """, (limit,)).fetchall()

        return [dict(row) for row in rows]

    def get_dlq_count(self, queue_name: Optional[str] = None) -> int:
        """
        Count tasks in the Dead Letter Queue.

        Args:
            queue_name: Optional queue filter.

        Returns:
            Number of DLQ tasks.
        """
        conn = self._get_conn()
        if queue_name:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM dead_letter_queue "
                "WHERE queue_name = ?",
                (queue_name,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM dead_letter_queue"
            ).fetchone()
        return row["cnt"]

    def replay_dlq_task(self, task_id: str) -> Optional[Dict]:
        """
        Move a task from the DLQ back to the pending queue.

        The task is removed from the DLQ table and its data is returned
        so the caller (QueueManager) can re-enqueue it with a fresh
        attempt counter.

        Args:
            task_id: The UUID of the DLQ task to replay.

        Returns:
            A dict with the task data, or None if not found.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM dead_letter_queue WHERE task_id = ?",
            (task_id,)
        ).fetchone()

        if not row:
            logger.warning(
                f"WALStore: DLQ replay failed — task {task_id[:8]}... "
                f"not found"
            )
            return None

        task_data = dict(row)
        conn.execute(
            "DELETE FROM dead_letter_queue WHERE task_id = ?",
            (task_id,)
        )
        conn.commit()

        logger.info(
            f"WALStore: task {task_id[:8]}... replayed from DLQ "
            f"(queue: {task_data['queue_name']})"
        )
        return task_data

    def purge_dlq(self, queue_name: Optional[str] = None) -> int:
        """
        Remove tasks from the DLQ.

        Args:
            queue_name: If specified, only purge tasks from this queue.
                        If None, purge everything.

        Returns:
            Number of tasks purged.
        """
        conn = self._get_conn()
        if queue_name:
            cursor = conn.execute(
                "DELETE FROM dead_letter_queue WHERE queue_name = ?",
                (queue_name,)
            )
        else:
            cursor = conn.execute("DELETE FROM dead_letter_queue")
        conn.commit()
        count = cursor.rowcount
        if count > 0:
            logger.warning(f"WALStore: purged {count} DLQ tasks")
        return count

    # ── Broker Metadata Operations ────────────────────────────────

    def set_metadata(self, key: str, value: str) -> None:
        """Store a key-value pair in the broker metadata table."""
        conn = self._get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO broker_metadata (key, value) "
            "VALUES (?, ?)",
            (key, value)
        )
        conn.commit()

    def get_metadata(self, key: str) -> Optional[str]:
        """Retrieve a metadata value by key."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT value FROM broker_metadata WHERE key = ?",
            (key,)
        ).fetchone()
        return row["value"] if row else None

    # ── Utility ───────────────────────────────────────────────────

    def clear_all_data(self) -> None:
        """
        Completely wipes all persistent tasks and metrics.
        Used for resetting the system state from the dashboard.
        """
        with self._get_conn() as conn:
            conn.execute("DELETE FROM tasks")
            conn.execute("DELETE FROM dead_letter_queue")
            conn.execute("DELETE FROM broker_metadata")
            conn.commit()

    def get_stats(self) -> Dict:
        """
        Return a snapshot of database statistics.

        Useful for the admin/observability layer.
        """
        conn = self._get_conn()

        task_counts = conn.execute("""
            SELECT status, COUNT(*) as cnt
            FROM tasks GROUP BY status
        """).fetchall()

        dlq_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM dead_letter_queue"
        ).fetchone()

        return {
            "tasks_by_status": {
                row["status"]: row["cnt"] for row in task_counts
            },
            "dlq_count": dlq_count["cnt"],
            "db_path": self.db_path,
        }

    def close(self) -> None:
        """Close the thread-local connection if open."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
            logger.info("WALStore connection closed")
