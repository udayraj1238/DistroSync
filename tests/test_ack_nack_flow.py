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
        server = BrokerServer(host="127.0.0.1", port=5552, http_port=8002, db_path=db_path)
        broker_task = asyncio.create_task(server.start())
        await asyncio.sleep(1) # wait for bind
        
        # 1. Start a worker on an empty queue (should poll every 100ms)
        class AckNackWorker(BaseWorker):
            async def execute(self, payload):
                if payload.get("fail"):
                    print(f"Worker forcefully failing task: {payload}")
                    raise ValueError("Intentional Failure for NACK test")
                print(f"Worker executing task successfully: {payload}")
                return {"status": "success"}

        worker = AckNackWorker(
            queue_name="test_queue", 
            host="127.0.0.1", 
            port=5552, 
            poll_interval=0.1,  # polls every 100ms when empty
            worker_id="test-worker"
        )
        worker_task = asyncio.create_task(worker.run())
        
        # Wait a bit to let it poll the empty queue
        await asyncio.sleep(0.5) 
        
        # 2. Produce one task that succeeds (ACK)
        producer = ProducerClient("127.0.0.1", 5552)
        await producer.connect()
        print("\n--- Producing success task ---")
        await producer.produce("test_queue", {"job": "good", "fail": False})
        
        # Wait for worker to process
        await asyncio.sleep(0.5)
        
        # 3. Produce one task that fails (NACK)
        print("\n--- Producing failing task ---")
        await producer.produce("test_queue", {"job": "bad", "fail": True})
        
        # Wait for worker to process and NACK
        await asyncio.sleep(0.5)
        
        print("\nWorker stats:", worker.get_stats())
        
        # Shutdown
        await worker.shutdown()
        await server.stop()
        broker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
