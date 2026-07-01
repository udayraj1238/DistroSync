#!/bin/bash
# A unified startup script to run the broker and workers in the same container.
# This is perfect for PaaS deployments (like Render) that only allow one exposed port.

# Use Render's PORT or fallback to 10000 (which we mapped in Dockerfile)
HTTP_PORT=${PORT:-10000}
TCP_PORT=5555

echo "Starting DistroSync Broker on HTTP port $HTTP_PORT and TCP port $TCP_PORT..."
python -m broker.server --host 0.0.0.0 --port $TCP_PORT --http-port $HTTP_PORT &
BROKER_PID=$!

# Give the broker a moment to initialize the SQLite WAL and bind to ports
sleep 2

cat << 'EOF' > run_worker.py
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
    w = DockerWorker(broker_host=host, broker_port=port, worker_id=worker_id, queues=[queue])
    await w.start()

asyncio.run(main())
EOF

export BROKER_HOST="127.0.0.1"
export BROKER_PORT=$TCP_PORT

QUEUE_NAME=tasks WORKER_ID=wkr-render-1 python run_worker.py &
QUEUE_NAME=emails WORKER_ID=wkr-render-2 python run_worker.py &
QUEUE_NAME=reports WORKER_ID=wkr-render-3 python run_worker.py &

# Wait for all background processes
wait $BROKER_PID
