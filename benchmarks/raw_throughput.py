"""
Raw broker throughput benchmark (load shedding lifted).

50 concurrent producers x 200 tasks = 10,000 tasks, 4 workers, localhost.
Reports accepted/failed tasks, wall time (submit + drain), throughput,
and per-PRODUCE latency percentiles.

    python -m benchmarks.raw_throughput            # shedding lifted
    python -m benchmarks.raw_throughput --shedding # default 100 tasks/sec budget
"""
import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.disable(logging.WARNING)

from broker.server import BrokerServer
from producer.client import ProducerClient
from worker.base_worker import BaseWorker


class EchoWorker(BaseWorker):
    async def execute(self, payload: dict) -> dict:
        return {"ok": True}


async def producer(port, n, latencies, failures):
    client = ProducerClient("127.0.0.1", port)
    await client.connect()
    for i in range(n):
        start = time.monotonic()
        try:
            await client.produce("bench", {"i": i}, max_retries=5)
            latencies.append((time.monotonic() - start) * 1000)
        except Exception:
            failures.append(i)
    await client.close()


async def main(producers, tasks, workers, shedding, port):
    broker = BrokerServer(host="127.0.0.1", port=port, http_port=port + 1000)
    if not shedding:
        broker.load_shedder.BUCKET_CAPACITY = 1e9
        broker.load_shedder.BASE_FILL_RATE = 1e9
    server = asyncio.create_task(broker.start())
    await asyncio.sleep(0.5)
    ws = [EchoWorker("bench", host="127.0.0.1", port=port, poll_interval=0.001) for _ in range(workers)]
    wts = [asyncio.create_task(w.run()) for w in ws]
    await asyncio.sleep(0.5)

    latencies, failures = [], []
    t0 = time.monotonic()
    await asyncio.gather(*[producer(port, tasks, latencies, failures) for _ in range(producers)])
    while broker.queue_manager.queue_depth("bench") > 0 or broker.queue_manager.in_flight_count() > 0:
        await asyncio.sleep(0.01)
    elapsed = time.monotonic() - t0

    lat = sorted(latencies)
    pct = lambda p: lat[int(p * (len(lat) - 1))] if lat else float("nan")
    print(f"shedding={'on' if shedding else 'off'}  tasks={producers * tasks}  accepted={len(lat)}  "
          f"failed={len(failures)}  elapsed={elapsed:.2f}s  throughput={len(lat) / elapsed:,.0f}/s  "
          f"P50={pct(0.50):.1f}ms  P90={pct(0.90):.1f}ms  P99={pct(0.99):.1f}ms")

    for w in ws:
        await w.shutdown()
    for t in wts:
        t.cancel()
    await broker.stop()
    server.cancel()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--producers", type=int, default=50)
    ap.add_argument("--tasks", type=int, default=200)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--shedding", action="store_true")
    ap.add_argument("--port", type=int, default=17100)
    a = ap.parse_args()
    asyncio.run(main(a.producers, a.tasks, a.workers, a.shedding, a.port))
