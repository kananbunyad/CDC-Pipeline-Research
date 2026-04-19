"""Synthetic e-gaming CRM data generator for CDC pipeline benchmarks."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("generate_data")

SCHEMA_PATH = Path(__file__).parent.parent / "src" / "db" / "schema.sql"

TIER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]
TIERS = ["bronze", "silver", "gold", "platinum"]

PLATFORM_WEIGHTS = [0.40, 0.35, 0.25]
PLATFORMS = ["PC", "Mobile", "Console"]

GAME_TYPE_WEIGHTS = [0.35, 0.25, 0.25, 0.15]
GAME_TYPES = ["slots", "poker", "sports_betting", "casino"]

PAYMENT_WEIGHTS = [0.60, 0.25, 0.15]
PAYMENT_METHODS = ["card", "crypto", "wallet"]

EVENT_TYPE_WEIGHTS = [0.45, 0.25, 0.20, 0.10]
EVENT_TYPES = ["login", "achievement", "level_up", "referral"]


def get_connection(database: str = "source") -> psycopg2.extensions.connection:
    """Return a psycopg2 connection to the specified database."""
    prefix = database.upper()
    return psycopg2.connect(
        host=os.environ.get(f"{prefix}_DB_HOST", "localhost"),
        port=int(os.environ.get(f"{prefix}_DB_PORT", 5433 if database == "source" else 5432)),
        dbname=os.environ.get(f"{prefix}_DB_NAME", f"{database}_db"),
        user=os.environ.get(f"{prefix}_DB_USER", "postgres"),
        password=os.environ.get(f"{prefix}_DB_PASSWORD", ""),
    )


def apply_schema(conn: psycopg2.extensions.connection) -> None:
    """Run schema.sql against the given connection, idempotently."""
    sql = SCHEMA_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("Schema applied successfully.")


def weighted_choice(rng: np.random.Generator, choices: list[str], weights: list[float]) -> str:
    """Return a single weighted random choice."""
    return rng.choice(choices, p=weights)  # type: ignore[return-value]


def generate_players(
    n: int,
    fake: Faker,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Generate *n* synthetic player records."""
    now = fake.date_time_this_year()
    records = []
    for _ in range(n):
        records.append(
            {
                "external_id": str(uuid.uuid4()),
                "username": fake.user_name(),
                "email": fake.email(),
                "tier": weighted_choice(rng, TIERS, TIER_WEIGHTS),
                "registered_at": fake.date_time_between(start_date="-3y", end_date=now),
                "platform": weighted_choice(rng, PLATFORMS, PLATFORM_WEIGHTS),
            }
        )
    return records


def generate_sessions(
    player_ids: list[int],
    n: int,
    fake: Faker,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Generate *n* synthetic game-session records linked to *player_ids*."""
    durations = rng.gamma(shape=2.5, scale=15.0, size=n)
    records = []
    for i in range(n):
        records.append(
            {
                "player_id": int(rng.choice(player_ids)),
                "start_time": fake.date_time_between(start_date="-1y"),
                "duration_minutes": float(round(durations[i], 2)),
                "game_type": weighted_choice(rng, GAME_TYPES, GAME_TYPE_WEIGHTS),
                "platform": weighted_choice(rng, PLATFORMS, PLATFORM_WEIGHTS),
            }
        )
    return records


def generate_transactions(
    player_ids: list[int],
    n: int,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Generate *n* synthetic transaction records."""
    amounts = np.exp(rng.normal(loc=12.5, scale=0.8, size=n))
    amounts = np.clip(amounts, 1.0, 50_000.0)
    records = []
    for i in range(n):
        records.append(
            {
                "player_id": int(rng.choice(player_ids)),
                "amount": float(round(amounts[i], 2)),
                "currency": "USD",
                "payment_method": weighted_choice(rng, PAYMENT_METHODS, PAYMENT_WEIGHTS),
            }
        )
    return records


def generate_events(
    player_ids: list[int],
    n: int,
    fake: Faker,
    rng: np.random.Generator,
) -> list[dict[str, Any]]:
    """Generate *n* synthetic behavioural event records."""
    import json

    records = []
    for _ in range(n):
        etype = weighted_choice(rng, EVENT_TYPES, EVENT_TYPE_WEIGHTS)
        metadata: dict[str, Any] = {"source": fake.bothify("srv-##??"), "ip": fake.ipv4()}
        if etype == "achievement":
            metadata["achievement_id"] = fake.numerify("ACH-####")
        elif etype == "level_up":
            metadata["new_level"] = int(rng.integers(2, 101))
        elif etype == "referral":
            metadata["referral_code"] = fake.lexify("?????").upper()
        records.append(
            {
                "player_id": int(rng.choice(player_ids)),
                "event_type": etype,
                "metadata": json.dumps(metadata),
            }
        )
    return records


def insert_batch(
    cur: psycopg2.extensions.cursor,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> None:
    """Bulk-insert rows into *table* using execute_values."""
    template = "(" + ",".join(["%s"] * len(columns)) + ")"
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_list}) VALUES %s"
    data = [tuple(row[c] for c in columns) for row in rows]
    psycopg2.extras.execute_values(cur, sql, data, template=template, page_size=1000)


def run(volume: int) -> None:
    """Generate and insert *volume* records (distributed across all tables)."""
    fake = Faker()
    rng = np.random.default_rng()

    # Proportional split: players ~ 20%, sessions ~ 35%, txns ~ 30%, events ~ 15%
    n_players = max(100, volume // 5)
    n_sessions = max(100, int(volume * 0.35))
    n_transactions = max(100, int(volume * 0.30))
    n_events = max(50, volume - n_players - n_sessions - n_transactions)

    log.info(
        "Generating %d total records: %d players, %d sessions, %d transactions, %d events",
        volume,
        n_players,
        n_sessions,
        n_transactions,
        n_events,
    )

    conn = get_connection("source")
    try:
        apply_schema(conn)

        t0_ns = time.perf_counter_ns()

        with conn.cursor() as cur:
            # Players
            players = generate_players(n_players, fake, rng)
            insert_batch(cur, "players", ["external_id", "username", "email", "tier", "registered_at", "platform"], players)
            conn.commit()
            cur.execute("SELECT id FROM players ORDER BY created_at DESC LIMIT %s", (n_players,))
            player_ids = [row[0] for row in cur.fetchall()]
            log.info("Inserted %d players (sample ids: %s...)", len(player_ids), player_ids[:3])

            # Game sessions
            sessions = generate_sessions(player_ids, n_sessions, fake, rng)
            insert_batch(cur, "game_sessions", ["player_id", "start_time", "duration_minutes", "game_type", "platform"], sessions)
            conn.commit()
            log.info("Inserted %d game sessions.", n_sessions)

            # Transactions
            txns = generate_transactions(player_ids, n_transactions, rng)
            insert_batch(cur, "transactions", ["player_id", "amount", "currency", "payment_method"], txns)
            conn.commit()
            log.info("Inserted %d transactions.", n_transactions)

            # Events
            evts = generate_events(player_ids, n_events, fake, rng)
            insert_batch(cur, "events", ["player_id", "event_type", "metadata"], evts)
            conn.commit()
            log.info("Inserted %d events.", n_events)

        elapsed_ns = time.perf_counter_ns() - t0_ns
        elapsed_s = elapsed_ns / 1e9
        total_records = n_players + n_sessions + n_transactions + n_events
        rate = total_records / elapsed_s

        # Summary printed to stdout for easy parsing
        print("\n" + "=" * 60)
        print(f"  Data Generation Summary")
        print("=" * 60)
        print(f"  Total records inserted : {total_records:,}")
        print(f"  Players                : {n_players:,}")
        print(f"  Game sessions          : {n_sessions:,}")
        print(f"  Transactions           : {n_transactions:,}")
        print(f"  Events                 : {n_events:,}")
        print(f"  Elapsed time           : {elapsed_s:.3f} s")
        print(f"  Insert rate            : {rate:,.1f} rec/s")
        print("=" * 60 + "\n")

    finally:
        conn.close()


def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(description="Generate synthetic e-gaming CRM data.")
    parser.add_argument(
        "--volume",
        type=int,
        default=10_000,
        help="Total number of records to generate (default: 10 000)",
    )
    args = parser.parse_args()
    run(args.volume)


if __name__ == "__main__":
    main()
