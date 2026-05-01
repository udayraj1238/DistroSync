# DistroSync

A distributed task queue with adaptive load shedding — built from scratch in Python.

## What is this?

DistroSync is a ground-up implementation of a message broker and task queue system, similar to what Kafka, RabbitMQ, and Celery do under the hood. No libraries for the core — just raw Python sockets, asyncio, and multiprocessing.

## Architecture

- **Broker** — Central TCP server that accepts tasks from producers and dispatches them to workers
- **Producers** — Client programs that submit tasks to the broker
- **Workers** — Processes that pull tasks and execute them

## Tech Stack

- Python 3.10+
- asyncio (non-blocking I/O)
- Raw TCP sockets (length-prefixed JSON protocol)
- SQLite / WAL mode (crash-safe persistence)
- multiprocessing (CPU-bound task execution)
- FastAPI (observability dashboard)
- Docker (containerized deployment)

## Status

🚧 Under active development
