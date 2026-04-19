"""FastAPI response-time benchmark: measures latency with and without Redis cache."""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("measure_api")

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

REQUESTS_PER_ENDPOINT = 1000
HEALTH_POLL_INTERVAL_S = 5.0
HEALTH_TIMEOUT_S = 120.0


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_api_url() -> str:
    """Return the base URL for the FastAPI service."""
    return os.environ.get("FASTAPI_URL", "http://localhost:8000").rstrip("/")


def wait_for_api(base_url: str) -> None:
    """Block until the FastAPI /health endpoint returns 200."""
    deadline = time.perf_counter() + HEALTH_TIMEOUT_S
    while time.perf_counter() < deadline:
        try:
            resp = requests.get(f"{base_url}/health", timeout=5)
            if resp.status_code == 200:
                log.info("FastAPI is healthy.")
                return
        except requests.RequestException:
            pass
        log.info("Waiting for FastAPI …")
        time.sleep(HEALTH_POLL_INTERVAL_S)
    raise TimeoutError(f"FastAPI did not become healthy within {HEALTH_TIMEOUT_S} s")


def flush_redis_cache() -> None:
    """Flush all Redis keys via the API's cache-flush endpoint, or directly via redis-cli."""
    import subprocess

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    host = redis_url.replace("redis://", "").split(":")[0]
    port = int(redis_url.replace("redis://", "").split(":")[-1])
    try:
        subprocess.run(
            ["redis-cli", "-h", host, "-p", str(port), "FLUSHALL"],
            check=True,
            capture_output=True,
        )
        log.info("Redis cache flushed.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.warning("Could not flush Redis via redis-cli; cold-cache results may be inaccurate.")


def get_valid_player_ids(n: int = 200) -> list[int]:
    """Fetch a sample of valid player IDs from the target database."""
    target_host = os.environ.get("TARGET_DB_HOST", "localhost")
    target_port = int(os.environ.get("TARGET_DB_PORT", 5432))
    target_db = os.environ.get("TARGET_DB_NAME", "target_db")
    target_user = os.environ.get("TARGET_DB_USER", "postgres")
    target_pw = os.environ.get("TARGET_DB_PASSWORD", "")

    try:
        conn = psycopg2.connect(
            host=target_host, port=target_port, dbname=target_db,
            user=target_user, password=target_pw,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM players ORDER BY random() LIMIT %s", (n,))
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        return ids
    except psycopg2.Error as exc:
        log.warning("Could not fetch player IDs: %s — using range 1–%d", exc, n)
        return list(range(1, n + 1))


# ─── Request timing ───────────────────────────────────────────────────────────

def measure_endpoint(
    base_url: str,
    path: str,
    n_requests: int,
    player_ids: list[int],
) -> list[float]:
    """
    Fire *n_requests* GET requests to *path* and return per-request latencies in ms.

    For paths containing `{player_id}`, a random ID is substituted per request.
    """
    latencies: list[float] = []
    session = requests.Session()

    for _ in range(n_requests):
        url = f"{base_url}{path}"
        if "{player_id}" in url and player_ids:
            url = url.replace("{player_id}", str(random.choice(player_ids)))

        t0 = time.perf_counter()
        try:
            resp = session.get(url, timeout=10)
            _ = resp.content  # ensure body is fully read
        except requests.RequestException as exc:
            log.debug("Request to %s failed: %s", url, exc)
        finally:
            latencies.append((time.perf_counter() - t0) * 1000.0)

    session.close()
    return latencies


def compute_stats(latencies: list[float]) -> dict[str, float]:
    """Return descriptive statistics for a latency sample."""
    arr = np.array(latencies)
    return {
        "mean_ms": round(float(np.mean(arr)), 3),
        "std_ms": round(float(np.std(arr)), 3),
        "p50_ms": round(float(np.percentile(arr, 50)), 3),
        "p95_ms": round(float(np.percentile(arr, 95)), 3),
        "p99_ms": round(float(np.percentile(arr, 99)), 3),
        "n": len(latencies),
    }


# ─── Output ───────────────────────────────────────────────────────────────────

ENDPOINTS = [
    "/api/v1/rfm-scores",
    "/api/v1/customers/{player_id}",
    "/api/v1/metrics/summary",
    "/api/v1/segments",
]


def print_comparison_table(results: dict[str, dict[str, dict[str, float]]]) -> None:
    """Print a formatted cold-vs-warm cache comparison table."""
    header = (
        f"{'Endpoint':<35} | {'Cold Mean':>10} | {'Cold p95':>10} | "
        f"{'Warm Mean':>10} | {'Warm p95':>10} | {'Speedup':>8}"
    )
    sep = "-" * len(header)
    print("\n" + sep)
    print("  FastAPI Response-Time Benchmark  (cold vs. warm cache)")
    print(sep)
    print(header)
    print(sep)
    for ep, modes in results.items():
        cold = modes.get("cold", {})
        warm = modes.get("warm", {})
        cold_mean = cold.get("mean_ms", 0.0)
        warm_mean = warm.get("mean_ms", 0.0)
        speedup = cold_mean / warm_mean if warm_mean > 0 else float("nan")
        print(
            f"{ep:<35} | {cold_mean:>8.2f}ms | {cold.get('p95_ms', 0):>8.2f}ms | "
            f"{warm_mean:>8.2f}ms | {warm.get('p95_ms', 0):>8.2f}ms | {speedup:>6.1f}×"
        )
    print(sep + "\n")


def save_results(results: dict[str, dict[str, dict[str, float]]]) -> None:
    """Persist results to JSON and CSV."""
    (RESULTS_DIR / "api_results.json").write_text(json.dumps(results, indent=2))

    flat: list[dict[str, Any]] = []
    for ep, modes in results.items():
        for mode, stats in modes.items():
            flat.append({"endpoint": ep, "mode": mode, **stats})

    if flat:
        keys = list(flat[0].keys())
        with (RESULTS_DIR / "api_results.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            writer.writerows(flat)
    log.info("Results saved to %s", RESULTS_DIR)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    """Run the API benchmark in cold and warm cache modes."""
    base_url = get_api_url()
    wait_for_api(base_url)

    player_ids = get_valid_player_ids()
    log.info("Using %d player IDs for /customers/{id} endpoint.", len(player_ids))

    results: dict[str, dict[str, dict[str, float]]] = {}

    for endpoint in ENDPOINTS:
        results[endpoint] = {}

        # ── Cold cache (flush before each endpoint) ──────────────────────────
        log.info("Cold cache — %s", endpoint)
        flush_redis_cache()
        time.sleep(1.0)
        cold_lats = measure_endpoint(base_url, endpoint, REQUESTS_PER_ENDPOINT, player_ids)
        results[endpoint]["cold"] = compute_stats(cold_lats)

        # ── Warm cache (re-use existing cache entries) ────────────────────────
        log.info("Warm cache — %s", endpoint)
        warm_lats = measure_endpoint(base_url, endpoint, REQUESTS_PER_ENDPOINT, player_ids)
        results[endpoint]["warm"] = compute_stats(warm_lats)

        log.info(
            "%s  cold_mean=%.2f ms  warm_mean=%.2f ms",
            endpoint,
            results[endpoint]["cold"]["mean_ms"],
            results[endpoint]["warm"]["mean_ms"],
        )

    save_results(results)
    print_comparison_table(results)


if __name__ == "__main__":
    main()
