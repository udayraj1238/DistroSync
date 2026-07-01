import asyncio
import argparse
import time
from producer.client import ProducerClient

async def run_producer(prod_id: int, num_tasks: int, host: str, port: int):
    client = ProducerClient(host, port)
    await client.connect()
    
    for i in range(num_tasks):
        queue = "tasks" if i % 2 == 0 else "emails"
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
    args = parser.parse_args()

    cors = [run_producer(i, args.tasks, "127.0.0.1", args.port) for i in range(args.producers)]
    await asyncio.gather(*cors)

if __name__ == "__main__":
    asyncio.run(main())
