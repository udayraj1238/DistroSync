import asyncio
import argparse
import time
from producer.client import ProducerClient

async def run_producer(prod_id: int, num_tasks: int, host: str, port: int, queue_prefix: str, delay: float = 0.0):
    client = ProducerClient(host, port)
    await client.connect()
    
    for i in range(num_tasks):
        queue = f"{queue_prefix}tasks" if i % 2 == 0 else f"{queue_prefix}emails"
        try:
            await client.produce(
                queue,
                {"test": True, "prod_id": prod_id, "seq": i},
                max_retries=0
            )
        except RuntimeError:
            pass # Task dropped due to rate limiting, exactly what we want in a flood test
        if delay > 0:
            await asyncio.sleep(delay)
    await client.close()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--producers", type=int, default=50)
    parser.add_argument("--tasks", type=int, default=200)
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--queue", type=str, default="")
    parser.add_argument("--delay", type=float, default=0.0)
    args = parser.parse_args()

    q_prefix = args.queue + "_" if args.queue else ""
    cors = [run_producer(i, args.tasks, "127.0.0.1", args.port, q_prefix, args.delay) for i in range(args.producers)]
    await asyncio.gather(*cors)

if __name__ == "__main__":
    asyncio.run(main())
