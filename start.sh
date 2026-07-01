#!/bin/bash
# A unified startup script to run the broker and workers in the same container.
# This is perfect for PaaS deployments (like Render) that only allow one exposed port.

# Use Render's PORT or fallback to 10000 (which we mapped in Dockerfile)
HTTP_PORT=${PORT:-10000}
TCP_PORT=5555

echo "Starting DistroSync Broker on HTTP port $HTTP_PORT and TCP port $TCP_PORT..."
python -m broker.server --host 0.0.0.0 --port $TCP_PORT --http-port $HTTP_PORT &
BROKER_PID=$!

export BROKER_HOST="127.0.0.1"
export BROKER_PORT=$TCP_PORT

# Give the broker a moment to initialize before workers connect
sleep 2

QUEUE_NAME=tasks WORKER_ID=wkr-render-1 python run_worker.py &
QUEUE_NAME=emails WORKER_ID=wkr-render-2 python run_worker.py &
QUEUE_NAME=reports WORKER_ID=wkr-render-3 python run_worker.py &

# Wait for all background processes
wait $BROKER_PID
