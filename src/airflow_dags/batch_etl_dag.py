"""Airflow DAG: 1-minute batch ETL pipeline (source → target PostgreSQL)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger("batch_etl_dag")

DEFAULT_ARGS = {
    "owner": "cdc-research",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}

TABLES = ["players", "game_sessions", "transactions", "events"]
VARIABLE_KEY = "batch_etl_last_run"
SOURCE_CONN_ID = "source_postgres"
TARGET_CONN_ID = "target_postgres"


# ─── Task functions ───────────────────────────────────────────────────────────

def check_source_db(**kwargs: Any) -> None:
    """Verify that the source database is reachable and has data."""
    hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        log.info("Source database is reachable.")
    finally:
        conn.close()


def extract_and_load(**kwargs: Any) -> None:
    """
    Copy new rows from source to target for all tracked tables.

    Uses the last_run_timestamp Airflow Variable as the incremental watermark.
    Only rows with created_at > watermark are transferred.
    """
    try:
        last_run_str = Variable.get(VARIABLE_KEY)
        last_run = datetime.fromisoformat(last_run_str)
    except KeyError:
        last_run = datetime(2000, 1, 1)
        log.info("No previous run timestamp — performing full load.")

    run_start = datetime.utcnow()
    log.info("Extracting rows created after %s", last_run.isoformat())

    source_hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    src_conn = source_hook.get_conn()
    tgt_conn = target_hook.get_conn()

    try:
        for table in TABLES:
            _transfer_table(src_conn, tgt_conn, table, last_run)
        tgt_conn.commit()
        log.info("All tables transferred successfully.")
    except Exception:
        tgt_conn.rollback()
        raise
    finally:
        src_conn.close()
        tgt_conn.close()

    Variable.set(VARIABLE_KEY, run_start.isoformat())
    log.info("Updated last_run to %s", run_start.isoformat())


def _transfer_table(
    src_conn: Any,
    tgt_conn: Any,
    table: str,
    since: datetime,
) -> None:
    """Copy rows from *table* on source to target where created_at > *since*."""
    with src_conn.cursor() as src_cur:
        src_cur.execute(
            f"SELECT * FROM {table} WHERE created_at > %s ORDER BY created_at ASC",
            (since,),
        )
        cols = [desc[0] for desc in src_cur.description]
        rows = src_cur.fetchall()

    if not rows:
        log.info("No new rows for table %s", table)
        return

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)

    # Build ON CONFLICT clause per table
    if table == "players":
        conflict = "ON CONFLICT (external_id) DO UPDATE SET " + ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c not in ("id", "external_id")
        )
    else:
        conflict = "ON CONFLICT (id) DO UPDATE SET " + ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c != "id"
        )

    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) {conflict}"

    import psycopg2.extras

    with tgt_conn.cursor() as tgt_cur:
        psycopg2.extras.execute_batch(tgt_cur, sql, rows, page_size=500)

    log.info("Transferred %d rows to %s", len(rows), table)


def update_rfm_scores(**kwargs: Any) -> None:
    """
    Trigger RFM score recomputation via the FastAPI service.

    The API's background task owns the actual SQL; this task just signals it.
    """
    fastapi_url = os.environ.get("FASTAPI_URL", "http://fastapi:8000")
    try:
        resp = requests.get(f"{fastapi_url}/api/v1/rfm-scores", timeout=30)
        resp.raise_for_status()
        log.info("RFM recompute triggered successfully.")
    except requests.RequestException as exc:
        log.warning("RFM recompute request failed (non-critical): %s", exc)


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="batch_etl_pipeline",
    description="1-minute batch ETL: source PostgreSQL → target PostgreSQL",
    default_args=DEFAULT_ARGS,
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cdc-research", "etl", "baseline"],
) as dag:

    t_check_source = PythonOperator(
        task_id="check_source_db",
        python_callable=check_source_db,
        doc="Verify source database is reachable before attempting extraction.",
    )

    t_extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        doc="Incremental extract from source; bulk-upsert into target.",
    )

    t_update_rfm = PythonOperator(
        task_id="update_rfm_scores",
        python_callable=update_rfm_scores,
        doc="Signal FastAPI to recompute RFM scores after new data lands.",
    )

    t_check_source >> t_extract_load >> t_update_rfm
