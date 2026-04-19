"""Batch ETL baseline benchmark — simulates scheduled Airflow-style extraction."""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("benchmark_batch_etl")

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

BATCH_INTERVALS: dict[str, int] = {
    "1min": 60,
    "5min": 300,
}


# ─── Database helpers ─────────────────────────────────────────────────────────

def _conn_params(db: str) -> dict[str, Any]:
    """Return psycopg2 connect kwargs for the given database."""
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
    """Truncate all CDC-tracked tables and restart sequences."""
    tables = ["events", "transactions", "game_sessions", "rfm_scores", "players"]
    with conn.cursor() as cur:
        for tbl in tables:
            cur.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE")
    conn.commit()


def count_players(conn: psycopg2.extensions.connection) -> int:
    """Return current row count for the players table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM players")
        return cur.fetchone()[0]  # type: ignore[index]


# ─── Data insertion ───────────────────────────────────────────────────────────

def insert_players(conn: psycopg2.extensions.connection, n: int) -> None:
    """Bulk-insert *n* synthetic player rows."""
    tiers = ["bronze", "silver", "gold", "platinum"]
    platforms = ["PC", "Mobile", "Console"]
    rng = np.random.default_rng()

    rows = [
        (
            str(uuid.uuid4()),
            f"player_{i}",
            f"player_{i}@example.com",
            str(rng.choice(tiers)),
            str(rng.choice(platforms)),
        )
        for i in range(n)
    ]
    sql = (
        "INSERT INTO players (external_id, username, email, tier, registered_at, platform) "
        "VALUES (%s, %s, %s, %s, NOW(), %s)"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
    conn.commit()


# ─── Batch transfer ───────────────────────────────────────────────────────────

def run_batch_transfer(
    source_conn: psycopg2.extensions.connection,
    target_conn: psycopg2.extensions.connection,
    cutoff_ts: str,
) -> float:
    """
    Copy all players inserted after *cutoff_ts* from source to target.

    Returns the actual transfer wall-clock time in seconds.
    """
    t0 = time.perf_counter()

    with source_conn.cursor() as src_cur:
        src_cur.execute(
            "SELECT external_id, username, email, tier, registered_at, platform "
            "FROM players WHERE created_at >= %s",
            (cutoff_ts,),
        )
        rows = src_cur.fetchall()

    if rows:
        sql = (
            "INSERT INTO players (external_id, username, email, tier, registered_at, platform) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (external_id) DO UPDATE SET "
            "  username = EXCLUDED.username, "
            "  email = EXCLUDED.email, "
            "  tier = EXCLUDED.tier"
        )
        with target_conn.cursor() as tgt_cur:
            psycopg2.extras.execute_batch(tgt_cur, sql, rows, page_size=1000)
        target_conn.commit()

    transfer_time_s = time.perf_counter() - t0
    log.debug("Transferred %d rows in %.3f s", len(rows), transfer_time_s)
    return transfer_time_s


# ─── Single benchmark iteration ───────────────────────────────────────────────

def run_single(
    volume: int,
    interval_label: str,
    interval_s: int,
    source_conn: psycopg2.extensions.connection,
    target_conn: psycopg2.extensions.connection,
) -> dict[str, Any]:
    """
    Simulate one batch ETL cycle.

    The simulated latency for any individual record equals:
        random_offset (0 … interval_s)  +  actual_transfer_time
    because records ingested just after a batch window closes must wait up to
    the full interval before the next extraction runs.
    """
    clear_tables(source_conn)
    clear_tables(target_conn)

    cutoff_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    # Insert N records — this is time T0
    t_insert_start = time.perf_counter()
    insert_players(source_conn, volume)
    t_insert_end = time.perf_counter()
    insert_time_s = t_insert_end - t_insert_start

    log.info(
        "Volume=%d  interval=%s  inserted in %.3f s — simulating %d s batch wait …",
        volume,
        interval_label,
        insert_time_s,
        interval_s,
    )

    # Simulate waiting for the next batch window (shortened for benchmarking)
    # In a real environment this would be a full sleep; here we model it statistically.
    # We DO sleep 2 s to let the INSERT fully commit before transferring.
    time.sleep(2)

    # Actual transfer
    transfer_time_s = run_batch_transfer(source_conn, target_conn, cutoff_ts)

    # Verify consistency
    src_count = count_players(source_conn)
    tgt_count = count_players(target_conn)
    consistency = (tgt_count / src_count * 100.0) if src_count > 0 else 0.0

    # Simulated latency distribution:
    # Each record experiences a random delay within [0, interval_s] plus transfer time.
    rng = random.Random()
    simulated_latencies_ms = [
        (rng.uniform(0, interval_s) + transfer_time_s) * 1000.0
        for _ in range(volume)
    ]
    arr = np.array(simulated_latencies_ms)

    return {
        "volume": volume,
        "interval_label": interval_label,
        "interval_s": interval_s,
        "insert_time_s": round(insert_time_s, 4),
        "transfer_time_s": round(transfer_time_s, 4),
        "throughput_recs_per_s": round(volume / transfer_time_s if transfer_time_s > 0 else 0.0, 2),
        "mean_latency_ms": round(float(np.mean(arr)), 2),
        "std_latency_ms": round(float(np.std(arr)), 2),
        "p50_latency_ms": round(float(np.percentile(arr, 50)), 2),
        "p95_latency_ms": round(float(np.percentile(arr, 95)), 2),
        "p99_latency_ms": round(float(np.percentile(arr, 99)), 2),
        "consistency_pct": round(consistency, 4),
        "src_count": src_count,
        "tgt_count": tgt_count,
    }


def aggregate_repetitions(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate metrics across repetitions for one (volume, interval) pair."""
    numeric_fields = [
        "throughput_recs_per_s", "mean_latency_ms", "std_latency_ms",
        "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "consistency_pct",
    ]
    agg: dict[str, Any] = {
        "volume": results[0]["volume"],
        "interval_label": results[0]["interval_label"],
        "interval_s": results[0]["interval_s"],
    }
    for f in numeric_fields:
        vals = [r[f] for r in results]
        agg[f"mean_{f}"] = round(float(np.mean(vals)), 4)
        agg[f"std_{f}"] = round(float(np.std(vals)), 4)
    return agg


# ─── Output helpers ───────────────────────────────────────────────────────────

def print_table(rows: list[dict[str, Any]]) -> None:
    """Print aggregated results as a formatted ASCII table."""
    header = (
        f"{'Volume':>10} | {'Interval':>8} | {'Mean Lat':>12} | {'p95':>10} | "
        f"{'Throughput':>12} | {'Consistency':>12}"
    )
    sep = "-" * len(header)
    print("\n" + sep)
    print("  Batch ETL Baseline Benchmark Results")
    print(sep)
    print(header)
    print(sep)
    for row in rows:
        print(
            f"{row['volume']:>10,} | "
            f"{row['interval_label']:>8} | "
            f"{row['mean_mean_latency_ms']:>10.0f} ms | "
            f"{row['mean_p95_latency_ms']:>8.0f} ms | "
            f"{row['mean_throughput_recs_per_s']:>10.1f}/s | "
            f"{row['mean_consistency_pct']:>10.2f}%"
        )
    print(sep + "\n")


def save_results(all_raw: list[dict[str, Any]], aggregated: list[dict[str, Any]]) -> None:
    """Persist results to JSON and CSV."""
    (RESULTS_DIR / "batch_results.json").write_text(
        json.dumps({"raw": all_raw, "aggregated": aggregated}, indent=2)
    )
    if aggregated:
        keys = list(aggregated[0].keys())
        with (RESULTS_DIR / "batch_results.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            writer.writerows(aggregated)
    log.info("Results saved to %s", RESULTS_DIR)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    """Run the full batch ETL benchmark suite."""
    volumes = [
        int(v)
        for v in os.environ.get("BENCHMARK_DATA_VOLUMES", "1000,10000,100000").split(",")
        if v.strip()
    ]
    repetitions = int(os.environ.get("BENCHMARK_REPETITIONS", "10"))

    log.info("Volumes: %s | Intervals: %s | Repetitions: %d", volumes, list(BATCH_INTERVALS.keys()), repetitions)

    source_conn = get_connection("source")
    target_conn = get_connection("target")

    all_raw: list[dict[str, Any]] = []
    aggregated: list[dict[str, Any]] = []

    try:
        for vol in volumes:
            for interval_label, interval_s in BATCH_INTERVALS.items():
                rep_results: list[dict[str, Any]] = []
                for rep in range(1, repetitions + 1):
                    log.info(
                        "Volume=%d  interval=%s  rep=%d/%d",
                        vol,
                        interval_label,
                        rep,
                        repetitions,
                    )
                    result = run_single(vol, interval_label, interval_s, source_conn, target_conn)
                    rep_results.append(result)
                    all_raw.append({**result, "repetition": rep})

                agg = aggregate_repetitions(rep_results)
                aggregated.append(agg)
                log.info(
                    "Volume=%d  interval=%s  mean_lat=%.0f ms  throughput=%.1f/s  consistency=%.2f%%",
                    vol,
                    interval_label,
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
