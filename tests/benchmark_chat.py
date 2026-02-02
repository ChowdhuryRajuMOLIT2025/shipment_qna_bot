# scripts/benchmark_chat.py

import statistics
import time

import requests

BASE_URL = "http://127.0.0.1:8000"  # adjust if different


def run_single_request(session, payload):
    t0 = time.perf_counter()
    resp = session.post(f"{BASE_URL}/api/chat", json=payload, timeout=30)
    dt = (time.perf_counter() - t0) * 1000.0  # ms

    return resp.status_code, dt


def main():
    payload = {
        "question": "Show me the status for container TCLU2937251",
        "consignee_codes": ["0000866"],
        # No conversation_id to also measure server-side UUID generation path
    }

    n = 30  # number of sequential requests for a rough sanity check

    timings = []
    with requests.Session() as session:
        for i in range(n):
            code, dt = run_single_request(session, payload)
            print(f"[{i+1:02d}] status={code}, latency={dt:.1f} ms")
            timings.append(dt)

    print("\n--- Summary ---")
    print(f"Count: {len(timings)}")
    print(f"Min:   {min(timings):.1f} ms")
    print(f"Mean:  {statistics.mean(timings):.1f} ms")
    print(f"Median:{statistics.median(timings):.1f} ms")
    print(
        f"P95:   {statistics.quantiles(timings, n=20)[18]:.1f} ms"
    )  # ~95th percentile


if __name__ == "__main__":
    main()
