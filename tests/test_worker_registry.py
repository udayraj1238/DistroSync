import asyncio
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient
from worker.base_worker import BaseWorker

logging.basicConfig(level=logging.INFO)

async def main():
    import tempfile
    from broker.server import BrokerServer
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        server = BrokerServer(host="127.0.0.1", port=5553, http_port=8003, db_path=db_path)
        broker_task = asyncio.create_task(server.start())
        await asyncio.sleep(1) # wait for bind
        
        # 1. Worker that NACKs 2 times and ACKs on 3rd
        class FlakyWorker(BaseWorker):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.attempts = {}
                
            async def execute(self, payload):
                task_id = payload.get("id")
                self.attempts[task_id] = self.attempts.get(task_id, 0) + 1
                
                if self.attempts[task_id] <= 2:
                    print(f"FlakyWorker Failing task {task_id} (attempt {self.attempts[task_id]})")
                    raise ValueError("Simulated failure")
                
                print(f"FlakyWorker Succeeding task {task_id} on attempt {self.attempts[task_id]}!")
                return {"status": "success"}

        # 2. Worker that always NACKs
        class DoomedWorker(BaseWorker):
            async def execute(self, payload):
                print(f"DoomedWorker failing task intentionally...")
                raise ValueError("Fatal task")

        flaky_worker = FlakyWorker(queue_name="flaky_q", host="127.0.0.1", port=5553, worker_id="test-flaky")
        doomed_worker = DoomedWorker(queue_name="doomed_q", host="127.0.0.1", port=5553, worker_id="test-doomed")
        
        flaky_task = asyncio.create_task(flaky_worker.run())
        doomed_task = asyncio.create_task(doomed_worker.run())
        await asyncio.sleep(0.5)
        
        producer = ProducerClient("127.0.0.1", 5553)
        await producer.connect()
        
        print("\n--- Producing task to Flaky Worker ---")
        await producer.produce("flaky_q", {"id": "task_recoverable"})
        
        print("\n--- Producing task to Doomed Worker ---")
        await producer.produce("doomed_q", {"id": "task_poison_pill"})
        
        # Wait enough time for retries (workers retry immediately but have a small backoff loop potentially)
        await asyncio.sleep(2.0)
        
        # Verify queues
        flaky_depth = server.queue_manager.queue_depth("flaky_q")
        doomed_depth = server.queue_manager.queue_depth("doomed_q")
        dlq_count = len(server.queue_manager.dead_letter_queue)
        
        print(f"\nFlaky Queue Depth: {flaky_depth} (should be 0)")
        print(f"Doomed Queue Depth: {doomed_depth} (should be 0)")
        print(f"DLQ Count: {dlq_count} (should be 1)")
        
        assert flaky_depth == 0
        assert doomed_depth == 0
        assert dlq_count == 1
        
        # Shutdown
        await flaky_worker.shutdown()
        await doomed_worker.shutdown()
        await server.stop()
        broker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
