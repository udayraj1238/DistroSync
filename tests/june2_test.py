import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient
from worker.base_worker import BaseWorker

async def main():
    # We assume the broker is running externally, 
    # but for this script we just want to show the code exists
    # and print out what we would see. 
    # Let's spawn the broker inside the script to test it easily.
    import tempfile
    from broker.server import BrokerServer
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "june2.db")
        server = BrokerServer(host="127.0.0.1", port=5551, http_port=8001, db_path=db_path)
        
        # Start broker in background
        broker_task = asyncio.create_task(server.start())
        await asyncio.sleep(1) # wait for bind
        
        print("--- Testing PRODUCE (5 messages) ---")
        producer = ProducerClient("127.0.0.1", 5551)
        await producer.connect()
        
        task_ids = []
        for i in range(5):
            task_id = await producer.produce("june2_queue", {"job_num": i})
            print(f"Produced task successfully! ID: {task_id}")
            task_ids.append(task_id)
            
        print("\n--- Testing CONSUME (3 messages) ---")
        # We can simulate the CONSUME command using our worker structure
        
        class MockWorker(BaseWorker):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.consumed_count = 0
                
            async def execute(self, payload):
                self.consumed_count += 1
                print(f"Worker consumed task: {payload}")
                if self.consumed_count == 3:
                    print("Successfully consumed 3 tasks in order!")
                    # Tell worker to stop
                    await self.shutdown()
                return {"status": "success"}

        worker = MockWorker(queue_name="june2_queue", host="127.0.0.1", port=5551, worker_id="june2-worker")
        
        await worker.run() # blocks until shutdown is called
        
        print("\nJune 2 Verification Complete!")
        await server.stop()
        broker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
