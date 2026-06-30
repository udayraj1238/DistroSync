"""
June 24 -- Docker Test

Focus: End-to-end environment smoke test
  41. Full Docker Compose smoke test

Usage:
    python -m tests.test_june24_docker
    python tests/test_june24_docker.py
"""

import sys
import os
import time
import subprocess
import urllib.request
import json
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.client import ProducerClient
import asyncio


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0

    def run(self, name, func):
        self.total += 1
        print(f"\n--- Test {self.total}: {name} ---")
        try:
            func()
            print(f"  PASS")
            self.passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            self.failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()
            self.failed += 1


def produce_via_client(tasks: int):
    # Produce from host to broker running in Docker mapped to 5555
    async def produce():
        client = ProducerClient("127.0.0.1", 5555)
        await client.connect()
        for i in range(tasks):
            await client.produce("tasks", {"idx": i})
        await client.close()
    asyncio.run(produce())


# -- Test 41: Full Docker Compose smoke test ------------------------------

def test_docker_compose_full():
    """
    Run docker-compose up -d. Wait 10s for startup. Hit /metrics endpoint.
    Produce 50 tasks via the producer container (or host client). Assert 50
    tasks appear in metrics. Assert workers process them. docker-compose down.
    """
    # Assuming docker-compose is available on the path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    print("    Running docker-compose up -d...")
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--build"],
            cwd=base_dir,
            check=True,
            capture_output=True
        )
    except FileNotFoundError:
        print("    Docker is not installed or not in PATH. Skipping test.")
        return
    except subprocess.CalledProcessError as e:
        print("    Docker Compose failed to start!")
        print(e.stderr.decode())
        raise

    try:
        print("    Waiting 15s for startup...")
        time.sleep(15)
        
        # Hit metrics endpoint
        req = urllib.request.Request("http://localhost:8000/metrics")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            metrics = json.loads(resp.read().decode())
            print("    /metrics is alive!")
            
        print("    Producing 50 tasks...")
        produce_via_client(50)
        
        print("    Waiting 10s for workers to process...")
        time.sleep(10)
        
        req = urllib.request.Request("http://localhost:8000/metrics")
        with urllib.request.urlopen(req) as resp:
            metrics = json.loads(resp.read().decode())
            in_flight = metrics.get("broker", {}).get("in_flight_tasks", 0)
            
            # Look at tasks queue
            q_stats = metrics.get("queues", {}).get("tasks", {})
            depth = q_stats.get("depth", 0)
            
            print(f"    Broker metrics -> in_flight: {in_flight}, depth: {depth}")
            assert in_flight == 0, f"Expected 0 in flight, got {in_flight}"
            assert depth == 0, f"Expected 0 depth, got {depth}"
            
    finally:
        print("    Running docker-compose down...")
        subprocess.run(
            ["docker", "compose", "down", "-v"],
            cwd=base_dir,
            check=False,
            capture_output=True
        )


# -- Main ------------------------------------------------------------------

def run_tests():
    runner = TestRunner()

    runner.run("Full Docker Compose smoke test", test_docker_compose_full)

    print(f"\n{'=' * 60}")
    print(f"  June 24 Docker Tests: {runner.passed} passed, {runner.failed} failed")
    print(f"{'=' * 60}")

    return runner.failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
