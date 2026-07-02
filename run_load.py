import asyncio
import argparse
import time
from producer.client import ProducerClient

async def run_producer(prod_id: int, num_tasks: int, host: str, port: int, queue_prefix: str):
    client = ProducerClient(host, port)
    await client.connect()
    
    for i in range(num_tasks):
        queue = f"{queue_prefix}tasks" if i % 2 == 0 else f"{queue_prefix}emails"
        await client.produce(
            queue,
            {"test": True, "prod_id": prod_id, "seq": i}
        )
    await client.close()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--producers", type=int, default=50)
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--queue", type=str, default="")
    args = parser.parse_args()

    q_prefix = args.queue + "_" if args.queue else ""
    cors = [run_producer(i, args.tasks, "127.0.0.1", args.port, q_prefix) for i in range(args.producers)]
    await asyncio.gather(*cors)

if __name__ == "__main__":
    asyncio.run(main())
