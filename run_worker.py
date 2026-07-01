import os, asyncio, sys
from worker.base_worker import BaseWorker

class DockerWorker(BaseWorker):
    async def process_task(self, task):
        import time
        time.sleep(0.01)
        return {'status': 'completed', 'task_id': task.get('task_id', 'unknown')}

async def main():
    host = os.environ.get('BROKER_HOST', '127.0.0.1')
    port = int(os.environ.get('BROKER_PORT', '5555'))
    queue = os.environ.get('QUEUE_NAME', 'tasks')
    worker_id = os.environ.get('WORKER_ID', f'worker-{os.getpid()}')
    w = DockerWorker(queue_name=queue, host=host, port=port, worker_id=worker_id)
    await w.start()

if __name__ == '__main__':
    asyncio.run(main())
