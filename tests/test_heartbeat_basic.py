import asyncio
import sys
import os
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient
from worker.base_worker import BaseWorker

logging.basicConfig(level=logging.INFO)

async def main():
    import tempfile
    from broker.server import BrokerServer
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        server = BrokerServer(host="127.0.0.1", port=5554, http_port=8004, db_path=db_path)
        broker_task = asyncio.create_task(server.start())
        await asyncio.sleep(1) # wait for bind
        
        producer = ProducerClient("127.0.0.1", 5554)
        await producer.connect()
        await producer.produce("evict_q", {"task": "test_eviction"})
        
        # We need a worker that starts, consumes, but its heartbeat dies.
        # We can just manually connect, REGISTER, CONSUME, and then close the connection without ACKing.
        
        print("\n--- Connecting raw worker to simulate abrupt death ---")
        reader, writer = await asyncio.open_connection("127.0.0.1", 5554)
        
        async def send_msg(msg):
            import json
            encoded = json.dumps(msg).encode("utf-8")
            writer.write(len(encoded).to_bytes(4, "big") + encoded)
            await writer.drain()
            
        async def read_msg():
            import json
            raw_len = await reader.readexactly(4)
            msg_len = int.from_bytes(raw_len, "big")
            raw = await reader.readexactly(msg_len)
            return json.loads(raw.decode("utf-8"))
            
        # Register
        await send_msg({"command": "REGISTER", "worker_id": "dead-worker", "queues": ["evict_q"]})
        resp = await read_msg()
        assert resp["status"] == "ok"
        
        # Consume
        await send_msg({"command": "CONSUME", "worker_id": "dead-worker", "queue_name": "evict_q"})
        task_msg = await read_msg()
        print(f"Raw worker consumed task: {task_msg['task_id']}")
        
        # Verify in-flight
        in_flight_count = len(server.queue_manager._in_flight)
        print(f"In-flight tasks: {in_flight_count} (should be 1)")
        assert in_flight_count == 1
        
        # Now kill the worker completely (simulating a crash)
        print("\n--- Killing raw worker abruptly ---")
        writer.close()
        await writer.wait_closed()
        
        # Wait for eviction (threshold is 6 seconds, check loop runs every 2s, so wait 8s)
        print("Waiting 8 seconds for broker to detect missed heartbeats and evict...")
        await asyncio.sleep(8.0)
        
        # Verify eviction and requeue
        active_workers = server.worker_registry.active_worker_count()
        in_flight_count = len(server.queue_manager._in_flight)
        pending_depth = server.queue_manager.queue_depth("evict_q")
        
        print(f"\nActive workers: {active_workers} (should be 0)")
        print(f"In-flight tasks: {in_flight_count} (should be 0)")
        print(f"Pending tasks (requeued): {pending_depth} (should be 1)")
        
        assert active_workers == 0
        assert in_flight_count == 0
        assert pending_depth == 1
        
        print("\nEviction and Requeue verified successfully!")
        
        # Cleanup
        await server.stop()
        broker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
