"""CDC pipeline latency, throughput and consistency benchmark."""

from __future__ import annotations

import csv
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("benchmark_cdc")

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

CONNECTOR_CONFIG_PATH = Path(__file__).parent.parent / "debezium" / "postgres-connector.json"
POLL_INTERVAL_S = 0.1
MAX_WAIT_S = 120.0


# ─── Database helpers ─────────────────────────────────────────────────────────

def _conn_params(db: str) -> dict[str, Any]:
    """Return psycopg2 connect kwargs for the given db ('source' or 'target')."""
    prefix = db.upper()
    default_port = 5433 if db == "source" else 5432
    return {
        "host": os.environ.get(f"{prefix}_DB_HOST", "localhost"),
        "port": int(os.environ.get(f"{prefix}_DB_PORT", default_port)),
        "dbname": os.environ.get(f"{prefix}_DB_NAME", f"{db}_db"),
        "user": os.environ.get(f"{prefix}_DB_USER", "postgres"),
        "password": os.environ.get(f"{prefix}_DB_PASSWORD", ""),
    }


def get_connection(db: str) -> psycopg2.extensions.connection:
    """Open and return a new psycopg2 connection."""
    return psycopg2.connect(**_conn_params(db))


def clear_tables(conn: psycopg2.extensions.connection) -> None:
    """Truncate all CDC-tracked tables, resetting sequences."""
    tables = ["events", "transactions", "game_sessions", "rfm_scores", "players"]
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    conn.commit()


def count_table(conn: psycopg2.extensions.connection, table: str) -> int:
    """Return the current row count for *table*."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]  # type: ignore[index]


# ─── Connector management ─────────────────────────────────────────────────────

def ensure_connector_registered() -> None:
    """POST the Debezium connector config if it is not already registered."""
    connect_url = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
    connector_name = "postgres-source-connector"

    resp = requests.get(f"{connect_url}/connectors/{connector_name}", timeout=10)
    if resp.status_code == 200:
        log.info("Connector already registered.")
        return

    # Patch env vars into config before posting
    raw = json.loads(CONNECTOR_CONFIG_PATH.read_text())
    cfg = raw["config"]
    cfg["database.hostname"] = os.environ.get("SOURCE_DB_HOST", "postgres-source")
    cfg["database.user"] = os.environ.get("SOURCE_DB_USER", "postgres")
    cfg["database.password"] = os.environ.get("SOURCE_DB_PASSWORD", "sourcepass")

    post_resp = requests.post(
        f"{connect_url}/connectors",
        json=raw,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    post_resp.raise_for_status()
    log.info("Connector registered: %s", connector_name)


# ─── Data insertion ───────────────────────────────────────────────────────────

def insert_players_timed(
    conn: psycopg2.extensions.connection,
    n: int,
) -> dict[str, int]:
    """Insert *n* players and return {external_id: insert_time_ns} mapping."""
    timestamps: dict[str, int] = {}
    sql = (
        "INSERT INTO players (external_id, username, email, tier, registered_at, platform) "
        "VALUES (%s, %s, %s, %s, NOW(), %s) RETURNING external_id"
    )
    tiers = ["bronze", "silver", "gold", "platinum"]
    platforms = ["PC", "Mobile", "Console"]
    rng = np.random.default_rng()

    with conn.cursor() as cur:
        for _ in range(n):
            ext_id = str(uuid.uuid4())
            t_ns = time.perf_counter_ns()
            cur.execute(
                sql,
                (
                    ext_id,
                    f"player_{ext_id[:8]}",
                    f"{ext_id[:8]}@example.com",
                    rng.choice(tiers),
                    rng.choice(platforms),
                ),
            )
            timestamps[ext_id] = t_ns
        conn.commit()

    return timestamps


def poll_until_replicated(
    target_conn: psycopg2.extensions.connection,
    expected_count: int,
    insert_timestamps: dict[str, int],
) -> tuple[float, float, float]:
    """
    Poll target DB until *expected_count* rows appear or timeout.

    Returns (mean_latency_ms, throughput_recs_per_s, consistency_pct).
    """
    deadline = time.perf_counter() + MAX_WAIT_S
    last_count = 0

    while time.perf_counter() < deadline:
        current = count_table(target_conn, "players")
        if current >= expected_count:
            break
        if current > last_count:
            log.debug("Replicated %d / %d", current, expected_count)
            last_count = current
        time.sleep(POLL_INTERVAL_S)

    end_ns = time.perf_counter_ns()
    final_count = count_table(target_conn, "players")

    # Latencies: end_ns − insert_ns for each row (approximation using wall clock)
    latencies_ms = [
        (end_ns - ts_ns) / 1e6 for ts_ns in insert_timestamps.values()
    ]

    mean_lat = float(np.mean(latencies_ms))
    total_s = (end_ns - min(insert_timestamps.values())) / 1e9
    throughput = expected_count / total_s if total_s > 0 else 0.0
    consistency = (final_count / expected_count * 100.0) if expected_count > 0 else 0.0

    return mean_lat, throughput, consistency


# ─── Core benchmark ───────────────────────────────────────────────────────────

def run_single(
    volume: int,
    source_conn: psycopg2.extensions.connection,
    target_conn: psycopg2.extensions.connection,
) -> dict[str, Any]:
    """Run one CDC benchmark iteration for *volume* records."""
    clear_tables(source_conn)
    clear_tables(target_conn)
    time.sleep(1.0)  # let Debezium settle after truncate

    insert_ts = insert_players_timed(source_conn, volume)
    mean_lat, throughput, consistency = poll_until_replicated(target_conn, volume, insert_ts)

    # Re-fetch individual latencies for percentile calculations
    end_ns = time.perf_counter_ns()
    latencies_ms = [(end_ns - ts) / 1e6 for ts in insert_ts.values()]
    arr = np.array(latencies_ms)

    return {
        "volume": volume,
        "mean_latency_ms": round(float(np.mean(arr)), 2),
        "std_latency_ms": round(float(np.std(arr)), 2),
        "p50_latency_ms": round(float(np.percentile(arr, 50)), 2),
        "p95_latency_ms": round(float(np.percentile(arr, 95)), 2),
        "p99_latency_ms": round(float(np.percentile(arr, 99)), 2),
        "throughput_recs_per_s": round(throughput, 2),
        "consistency_pct": round(consistency, 4),
    }


def aggregate_repetitions(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate metrics across repetitions."""
    volume = results[0]["volume"]
    fields = [
        "mean_latency_ms", "std_latency_ms", "p50_latency_ms",
        "p95_latency_ms", "p99_latency_ms", "throughput_recs_per_s", "consistency_pct",
    ]
    agg: dict[str, Any] = {"volume": volume}
    for f in fields:
        vals = [r[f] for r in results]
        agg[f"mean_{f}"] = round(float(np.mean(vals)), 4)
        agg[f"std_{f}"] = round(float(np.std(vals)), 4)
    return agg


# ─── Output helpers ───────────────────────────────────────────────────────────

def print_table(rows: list[dict[str, Any]]) -> None:
    """Print aggregated results as a formatted ASCII table."""
    header = (
        f"{'Volume':>10} | {'Mean Lat':>10} | {'p50':>8} | {'p95':>8} | "
        f"{'p99':>8} | {'Throughput':>12} | {'Consistency':>12}"
    )
    sep = "-" * len(header)
    print("\n" + sep)
    print("  CDC Pipeline Benchmark Results")
    print(sep)
    print(header)
    print(sep)
    for row in rows:
        print(
            f"{row['volume']:>10,} | "
            f"{row['mean_mean_latency_ms']:>9.1f}ms | "
            f"{row['mean_p50_latency_ms']:>7.1f}ms | "
            f"{row['mean_p95_latency_ms']:>7.1f}ms | "
            f"{row['mean_p99_latency_ms']:>7.1f}ms | "
            f"{row['mean_throughput_recs_per_s']:>10.1f}/s | "
            f"{row['mean_consistency_pct']:>10.2f}%"
        )
    print(sep + "\n")


def save_results(all_raw: list[dict[str, Any]], aggregated: list[dict[str, Any]]) -> None:
    """Persist results to JSON and CSV."""
    (RESULTS_DIR / "cdc_results.json").write_text(
        json.dumps({"raw": all_raw, "aggregated": aggregated}, indent=2)
    )
    if aggregated:
        keys = list(aggregated[0].keys())
        with (RESULTS_DIR / "cdc_results.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            writer.writerows(aggregated)
    log.info("Results saved to %s", RESULTS_DIR)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    """Run the full CDC benchmark suite."""
    volumes = [
        int(v)
        for v in os.environ.get("BENCHMARK_DATA_VOLUMES", "1000,10000,100000").split(",")
        if v.strip()
    ]
    repetitions = int(os.environ.get("BENCHMARK_REPETITIONS", "10"))

    log.info("Volumes: %s | Repetitions: %d", volumes, repetitions)
    ensure_connector_registered()

    source_conn = get_connection("source")
    target_conn = get_connection("target")

    all_raw: list[dict[str, Any]] = []
    aggregated: list[dict[str, Any]] = []

    try:
        for vol in volumes:
            rep_results: list[dict[str, Any]] = []
            for rep in range(1, repetitions + 1):
                log.info("Volume=%d  Rep=%d/%d", vol, rep, repetitions)
                result = run_single(vol, source_conn, target_conn)
                rep_results.append(result)
                all_raw.append({**result, "repetition": rep})
            agg = aggregate_repetitions(rep_results)
            aggregated.append(agg)
            log.info(
                "Volume=%d  mean_lat=%.1f ms  throughput=%.1f/s  consistency=%.4f%%",
                vol,
                agg["mean_mean_latency_ms"],
                agg["mean_throughput_recs_per_s"],
                agg["mean_consistency_pct"],
            )
    finally:
        source_conn.close()
        target_conn.close()

    save_results(all_raw, aggregated)
    print_table(aggregated)


if __name__ == "__main__":
    main()
