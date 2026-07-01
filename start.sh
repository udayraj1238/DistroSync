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

echo "Starting 3 DistroSync Workers..."
# We point the workers to localhost because they are in the same container
export BROKER_HOST="127.0.0.1"
export BROKER_PORT=$TCP_PORT

python -m worker.client --id wkr-render-1 --queue tasks --max-processes 2 &
python -m worker.client --id wkr-render-2 --queue emails --max-processes 1 &
python -m worker.client --id wkr-render-3 --queue reports --max-processes 1 &

# Wait for all background processes
wait $BROKER_PID
