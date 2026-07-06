"""
Integration test for Retry Counting and Dead Letter Queue.

Tests the retry/DLQ flow:
  1. Task retried up to max_retries then moved to DLQ
  2. DLQ stores the permanently failed task with correct metadata
  3. DLQ peek shows tasks without removing them
  4. DLQ retry moves task back to original queue
  5. DLQ purge clears all dead-lettered tasks
  6. Full pipeline: produce -> fail 3 times -> DLQ
  7. Tasks under retry limit are re-queued (not DLQ'd)
  8. Custom max_retries setting works
"""

import asyncio
import json
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker.server import BrokerServer
from broker.queue_manager import QueueManager
from broker.dead_letter import DeadLetterQueue
from producer.client import ProducerClient
from worker.base_worker import BaseWorker

TEST_PORT = 15560


class AlwaysFailWorker(BaseWorker):
    """Worker that always fails — used to test retry exhaustion."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nack_count = 0

    async def execute(self, payload: dict) -> dict:
        self.nack_count += 1
        raise ValueError(f"Intentional failure #{self.nack_count}")


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    async def test_retry_counting_unit(self):
        """Test 1: Task retried max_retries times, then dead-lettered."""
        print("\n--- Test 1: Retry counting -> DLQ after max_retries ---")

        qm = QueueManager(max_retries=3)
        task_id = await qm.enqueue("retry_q", {"data": "test"})

        # Simulate 3 dequeue+NACK cycles
        for i in range(3):
            task_data = await qm.dequeue("retry_q", f"worker_{i}")
            assert task_data is not None, f"Dequeue #{i+1} returned None"
            result = await qm.negative_acknowledge(task_id)
            assert result is not None, f"NACK #{i+1} returned None"

            if i < 2:
                # First 2 NACKs: should be requeued
                assert result["action"] == "requeued", \
                    f"NACK #{i+1}: expected 'requeued', got '{result['action']}'"
                assert qm.queue_depth("retry_q") == 1, \
                    f"Task should be back in queue after NACK #{i+1}"
            else:
                # 3rd NACK: should be dead-lettered
                assert result["action"] == "dead_lettered", \
                    f"NACK #{i+1}: expected 'dead_lettered', got '{result['action']}'"
                assert qm.queue_depth("retry_q") == 0, \
                    "Queue should be empty after DLQ routing"
                assert qm.dead_letter_queue.count() == 1, \
                    "DLQ should have 1 task"

        print(f"  PASS: Task retried 3 times then moved to DLQ")
        self.passed += 1

    async def test_dlq_peek(self):
        """Test 2: DLQ peek returns task metadata without removing."""
        print("\n--- Test 2: DLQ peek ---")

        qm = QueueManager(max_retries=1)
        task_id = await qm.enqueue("peek_q", {"item": "peek_test"})

        # Dequeue + NACK once -> DLQ (max_retries=1)
        await qm.dequeue("peek_q", "worker_1")
        result = await qm.negative_acknowledge(task_id)
        assert result["action"] == "dead_lettered"

        # Peek should show the task
        peeked = await qm.dead_letter_queue.peek(limit=10)
        assert len(peeked) == 1
        assert peeked[0]["task_id"] == task_id
        assert peeked[0]["payload"] == {"item": "peek_test"}
        assert peeked[0]["queue_name"] == "peek_q"
        assert peeked[0]["status"] == "dead_lettered"

        # Task should still be in DLQ after peek
        assert qm.dead_letter_queue.count() == 1

        print(f"  PASS: Peek shows task metadata without removing it")
        self.passed += 1

    async def test_dlq_retry(self):
        """Test 3: DLQ retry moves task back to its original queue."""
        print("\n--- Test 3: DLQ retry -> back to queue ---")

        qm = QueueManager(max_retries=1)
        task_id = await qm.enqueue("retry_q", {"data": "retry_me"})

        # Send to DLQ
        await qm.dequeue("retry_q", "worker_1")
        await qm.negative_acknowledge(task_id)
        assert qm.dead_letter_queue.count() == 1
        assert qm.queue_depth("retry_q") == 0

        # Retry from DLQ
        success = await qm.dead_letter_queue.retry(task_id, qm)
        assert success, "DLQ retry should return True"
        assert qm.dead_letter_queue.count() == 0, "Task should be out of DLQ"
        assert qm.queue_depth("retry_q") == 1, "Task should be back in queue"

        # Dequeue the retried task — attempts should be reset to 0
        task_data = await qm.dequeue("retry_q", "worker_2")
        assert task_data is not None
        assert task_data["attempts"] == 1  # Fresh attempt #1

        print(f"  PASS: DLQ retry moved task back with reset attempt counter")
        self.passed += 1

    async def test_dlq_purge(self):
        """Test 4: DLQ purge clears all tasks."""
        print("\n--- Test 4: DLQ purge ---")

        qm = QueueManager(max_retries=1)

        # Send 3 tasks to DLQ
        for i in range(3):
            task_id = await qm.enqueue("purge_q", {"item": i})
            await qm.dequeue("purge_q", f"worker_{i}")
            await qm.negative_acknowledge(task_id)

        assert qm.dead_letter_queue.count() == 3

        # Purge
        purged = await qm.dead_letter_queue.purge()
        assert purged == 3
        assert qm.dead_letter_queue.count() == 0

        print(f"  PASS: Purged 3 tasks from DLQ")
        self.passed += 1

    async def test_dlq_remove(self):
        """Test 5: DLQ remove deletes a specific task."""
        print("\n--- Test 5: DLQ remove specific task ---")

        qm = QueueManager(max_retries=1)
        task_ids = []
        for i in range(3):
            tid = await qm.enqueue("remove_q", {"item": i})
            await qm.dequeue("remove_q", f"worker_{i}")
            await qm.negative_acknowledge(tid)
            task_ids.append(tid)

        assert qm.dead_letter_queue.count() == 3

        # Remove the middle task
        success = await qm.dead_letter_queue.remove(task_ids[1])
        assert success
        assert qm.dead_letter_queue.count() == 2

        # The other two should still be there
        peeked = await qm.dead_letter_queue.peek()
        remaining_ids = {t["task_id"] for t in peeked}
        assert task_ids[0] in remaining_ids
        assert task_ids[2] in remaining_ids
        assert task_ids[1] not in remaining_ids

        print(f"  PASS: Removed specific task, others remain")
        self.passed += 1

    async def test_full_pipeline_retry_then_dlq(self):
        """Test 6: Full pipeline — produce, fail 3 times, land in DLQ."""
        print("\n--- Test 6: Full pipeline retry -> DLQ ---")

        broker = BrokerServer(host="127.0.0.1", port=TEST_PORT)
        server_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.5)

        try:
            # Produce a task
            async with ProducerClient("127.0.0.1", TEST_PORT) as producer:
                task_id = await producer.produce("fail_pipeline", {"data": "doomed"})

            # Run a worker that always fails
            worker = AlwaysFailWorker(
                queue_name="fail_pipeline",
                host="127.0.0.1",
                port=TEST_PORT,
                poll_interval=0.05,
            )

            worker_task = asyncio.create_task(worker.run())
            # Wait enough for 3 dequeue+NACK cycles + DLQ routing
            await asyncio.sleep(2.0)

            # The task should now be in the DLQ
            dlq_count = broker.queue_manager.dead_letter_queue.count()
            assert dlq_count == 1, \
                f"Expected 1 task in DLQ, got {dlq_count}"

            # Queue should be empty (task is in DLQ, not being retried)
            queue_depth = broker.queue_manager.queue_depth("fail_pipeline")
            assert queue_depth == 0, \
                f"Expected empty queue, got depth {queue_depth}"

            # Worker should have stopped failing (no more tasks)
            assert worker._tasks_failed == 3, \
                f"Expected exactly 3 failures, got {worker._tasks_failed}"

            print(f"  PASS: Task failed 3 times then moved to DLQ via full pipeline")
            self.passed += 1

            await worker.shutdown()
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        finally:
            await broker.stop()
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

    async def test_under_max_retries_stays_in_queue(self):
        """Test 7: Task with attempts under max_retries stays in queue."""
        print("\n--- Test 7: Under max_retries -> stays in queue ---")

        qm = QueueManager(max_retries=5)  # High retry limit
        task_id = await qm.enqueue("survive_q", {"data": "survivor"})

        # 2 dequeue+NACK cycles (well under max_retries=5)
        for i in range(2):
            await qm.dequeue("survive_q", f"worker_{i}")
            result = await qm.negative_acknowledge(task_id)
            assert result["action"] == "requeued"

        # Task should still be in queue, NOT in DLQ
        assert qm.queue_depth("survive_q") == 1
        assert qm.dead_letter_queue.count() == 0

        print(f"  PASS: Task with 2/5 attempts stays in queue (not DLQ'd)")
        self.passed += 1

    async def test_custom_max_retries(self):
        """Test 8: Custom max_retries value is respected."""
        print("\n--- Test 8: Custom max_retries=5 ---")

        qm = QueueManager(max_retries=5)
        task_id = await qm.enqueue("custom_q", {"data": "custom"})

        # 5 dequeue+NACK cycles
        for i in range(5):
            task_data = await qm.dequeue("custom_q", f"worker_{i}")
            result = await qm.negative_acknowledge(task_id)

            if i < 4:
                assert result["action"] == "requeued"
            else:
                assert result["action"] == "dead_lettered"

        assert qm.dead_letter_queue.count() == 1
        assert qm.queue_depth("custom_q") == 0

        print(f"  PASS: Custom max_retries=5 respected correctly")
        self.passed += 1

    async def test_dlq_stats(self):
        """Test 9: DLQ and QueueManager stats track correctly."""
        print("\n--- Test 9: DLQ and QueueManager stats ---")

        qm = QueueManager(max_retries=1)

        # Send 2 tasks to DLQ
        for i in range(2):
            tid = await qm.enqueue("stats_q", {"item": i})
            await qm.dequeue("stats_q", f"w_{i}")
            await qm.negative_acknowledge(tid)

        qm_stats = qm.get_stats()
        dlq_stats = qm.dead_letter_queue.get_stats()

        assert qm_stats["total_nacked"] == 2
        assert dlq_stats["current_size"] == 2
        assert dlq_stats["total_received"] == 2

        print(f"  PASS: Stats correctly track 2 NACKs and 2 DLQ entries")
        self.passed += 1


async def run_tests():
    runner = TestRunner()

    tests = [
        runner.test_retry_counting_unit,
        runner.test_dlq_peek,
        runner.test_dlq_retry,
        runner.test_dlq_purge,
        runner.test_dlq_remove,
        runner.test_full_pipeline_retry_then_dlq,
        runner.test_under_max_retries_stays_in_queue,
        runner.test_custom_max_retries,
        runner.test_dlq_stats,
    ]

    for test_func in tests:
        try:
            await test_func()
        except AssertionError as e:
            print(f"  FAIL: ASSERTION: {e}")
            runner.failed += 1
        except Exception as e:
            print(f"  FAIL: ERROR: {e}")
            traceback.print_exc()
            runner.failed += 1

    print(f"\n{'='*50}")
    print(f"  Results: {runner.passed} passed, {runner.failed} failed")
    print(f"{'='*50}")
    return runner.failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
