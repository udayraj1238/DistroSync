import os, asyncio, sys
from worker.base_worker import BaseWorker

import random

class DockerWorker(BaseWorker):
    async def execute(self, payload: dict) -> dict:
        await asyncio.sleep(0.01)
        if random.random() < 0.20:  # 20% chance of random processing failure
            raise RuntimeError("Random simulation failure")
        return {'status': 'completed', 'task_id': payload.get('task_id', 'unknown')}

async def main():
    host = os.environ.get('BROKER_HOST', '127.0.0.1')
    port = int(os.environ.get('BROKER_PORT', '5555'))
    queue = os.environ.get('QUEUE_NAME', 'tasks')
    worker_id = os.environ.get('WORKER_ID', f'worker-{os.getpid()}')
    w = DockerWorker(queue_name=queue, host=host, port=port, worker_id=worker_id)
    while True:
        try:
            await w.run()
            break
        except ConnectionRefusedError:
            print(f"[{worker_id}] Connection refused. Broker not ready. Retrying in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[{worker_id}] Disconnected: {e}. Simulating dead worker for 15s before auto-recovering...")
            await asyncio.sleep(15)

if __name__ == '__main__':
    asyncio.run(main())
