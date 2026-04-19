"""FastAPI analytics service for the CDC pipeline research project."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import asyncpg
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("fastapi_app")

# ─── Configuration ────────────────────────────────────────────────────────────

DB_DSN = (
    f"postgresql://{os.environ.get('TARGET_DB_USER', 'postgres')}:"
    f"{os.environ.get('TARGET_DB_PASSWORD', '')}@"
    f"{os.environ.get('TARGET_DB_HOST', 'localhost')}:"
    f"{os.environ.get('TARGET_DB_PORT', 5432)}/"
    f"{os.environ.get('TARGET_DB_NAME', 'target_db')}"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

CACHE_TTL_LONG = 60    # seconds — RFM, customers, segments
CACHE_TTL_SHORT = 30   # seconds — metrics summary
RFM_RECOMPUTE_INTERVAL = 300  # 5 minutes


# ─── App state ────────────────────────────────────────────────────────────────

_db_pool: asyncpg.Pool | None = None
_redis: aioredis.Redis | None = None


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialise DB pool and Redis on startup; close on shutdown."""
    global _db_pool, _redis

    log.info("Connecting to database …")
    _db_pool = await asyncpg.create_pool(
        dsn=DB_DSN,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    log.info("Database pool ready.")

    log.info("Connecting to Redis …")
    _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    await _redis.ping()
    log.info("Redis ready.")

    recompute_task = asyncio.create_task(_rfm_recompute_loop())

    try:
        yield
    finally:
        recompute_task.cancel()
        try:
            await recompute_task
        except asyncio.CancelledError:
            pass
        if _db_pool:
            await _db_pool.close()
        if _redis:
            await _redis.aclose()
        log.info("Shutdown complete.")


app = FastAPI(
    title="CDC Pipeline Analytics API",
    version="1.0.0",
    description="Real-time CRM analytics backed by the CDC pipeline.",
    lifespan=lifespan,
)


# ─── Dependency helpers ───────────────────────────────────────────────────────

def db() -> asyncpg.Pool:
    """Return the active connection pool (raises if not initialised)."""
    if _db_pool is None:
        raise RuntimeError("DB pool not initialised")
    return _db_pool


def cache() -> aioredis.Redis:
    """Return the active Redis client (raises if not initialised)."""
    if _redis is None:
        raise RuntimeError("Redis not initialised")
    return _redis


async def cached_query(
    key: str,
    ttl: int,
    query: str,
    *args: Any,
) -> Any:
    """Return cached JSON if available; otherwise execute *query* and cache the result."""
    cached = await cache().get(key)
    if cached:
        return json.loads(cached)

    async with db().acquire() as conn:
        rows = await conn.fetch(query, *args)
    data = [dict(r) for r in rows]
    await cache().setex(key, ttl, json.dumps(data, default=str))
    return data


# ─── RFM background recompute ─────────────────────────────────────────────────

async def _rfm_recompute_loop() -> None:
    """Periodically recompute RFM scores for all players."""
    while True:
        await asyncio.sleep(RFM_RECOMPUTE_INTERVAL)
        try:
            await _compute_rfm_scores()
            log.info("RFM scores recomputed.")
        except Exception as exc:
            log.error("RFM recompute failed: %s", exc, exc_info=True)


async def _compute_rfm_scores() -> None:
    """
    Compute quintile-based RFM scores (1–5 each) for all players with activity,
    then label segments based on the combined score.
    """
    sql = """
    WITH recency AS (
        SELECT player_id, MAX(created_at) AS last_txn
        FROM transactions
        GROUP BY player_id
    ),
    frequency AS (
        SELECT player_id, COUNT(*) AS txn_count
        FROM transactions
        GROUP BY player_id
    ),
    monetary AS (
        SELECT player_id, SUM(amount) AS total_spent
        FROM transactions
        GROUP BY player_id
    ),
    rfm_raw AS (
        SELECT
            p.id AS player_id,
            EXTRACT(EPOCH FROM (NOW() - COALESCE(r.last_txn, p.registered_at))) / 86400 AS days_since,
            COALESCE(f.txn_count, 0)   AS freq,
            COALESCE(m.total_spent, 0) AS monetary
        FROM players p
        LEFT JOIN recency   r ON r.player_id = p.id
        LEFT JOIN frequency f ON f.player_id = p.id
        LEFT JOIN monetary  m ON m.player_id = p.id
    ),
    scored AS (
        SELECT
            player_id,
            NTILE(5) OVER (ORDER BY days_since DESC)  AS r_score,
            NTILE(5) OVER (ORDER BY freq ASC)          AS f_score,
            NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
        FROM rfm_raw
    )
    INSERT INTO rfm_scores (player_id, recency_score, frequency_score, monetary_score, rfm_segment, last_computed)
    SELECT
        s.player_id,
        s.r_score,
        s.f_score,
        s.m_score,
        CASE
            WHEN s.r_score >= 4 AND s.f_score >= 4 AND s.m_score >= 4 THEN 'Champions'
            WHEN s.r_score >= 3 AND s.f_score >= 3                     THEN 'Loyal'
            WHEN s.r_score >= 3 AND s.f_score >= 1 AND s.f_score <= 2  THEN 'Potential Loyalist'
            WHEN s.r_score >= 4 AND s.f_score <= 2                     THEN 'New Customer'
            WHEN s.r_score <= 2 AND s.f_score >= 3 AND s.m_score >= 3  THEN 'At Risk'
            WHEN s.r_score <= 2 AND s.f_score >= 4 AND s.m_score >= 4  THEN 'Cannot Lose Them'
            WHEN s.r_score <= 2 AND s.f_score <= 2                     THEN 'Lost'
            ELSE 'Hibernating'
        END,
        NOW()
    FROM scored s
    ON CONFLICT (player_id) DO UPDATE SET
        recency_score   = EXCLUDED.recency_score,
        frequency_score = EXCLUDED.frequency_score,
        monetary_score  = EXCLUDED.monetary_score,
        rfm_segment     = EXCLUDED.rfm_segment,
        last_computed   = EXCLUDED.last_computed
    """
    async with db().acquire() as conn:
        await conn.execute(sql)


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
async def health() -> JSONResponse:
    """Liveness probe: verifies DB and Redis connectivity."""
    db_status = "connected"
    redis_status = "connected"

    try:
        async with db().acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception as exc:
        log.warning("DB health check failed: %s", exc)
        db_status = "error"

    try:
        await cache().ping()
    except Exception as exc:
        log.warning("Redis health check failed: %s", exc)
        redis_status = "error"

    overall = "ok" if db_status == "connected" and redis_status == "connected" else "degraded"
    return JSONResponse({"status": overall, "db": db_status, "redis": redis_status})


@app.get("/api/v1/rfm-scores", tags=["analytics"])
async def get_rfm_scores() -> list[dict[str, Any]]:
    """Return the top 100 players ranked by combined RFM score, cached 60 s."""
    return await cached_query(
        "rfm:top100",
        CACHE_TTL_LONG,
        """
        SELECT
            p.id, p.external_id, p.username, p.tier,
            r.recency_score, r.frequency_score, r.monetary_score,
            (r.recency_score + r.frequency_score + r.monetary_score) AS total_score,
            r.rfm_segment, r.last_computed
        FROM rfm_scores r
        JOIN players p ON p.id = r.player_id
        ORDER BY total_score DESC
        LIMIT 100
        """,
    )


@app.get("/api/v1/customers/{player_id}", tags=["analytics"])
async def get_customer(player_id: int) -> dict[str, Any]:
    """Return a player profile with their latest RFM score, cached 60 s per player."""
    cache_key = f"customer:{player_id}"
    cached = await cache().get(cache_key)
    if cached:
        return json.loads(cached)

    async with db().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                p.id, p.external_id, p.username, p.email, p.tier,
                p.registered_at, p.platform, p.created_at,
                r.recency_score, r.frequency_score, r.monetary_score, r.rfm_segment
            FROM players p
            LEFT JOIN rfm_scores r ON r.player_id = p.id
            WHERE p.id = $1
            """,
            player_id,
        )

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Player not found")

    data = dict(row)
    await cache().setex(cache_key, CACHE_TTL_LONG, json.dumps(data, default=str))
    return data


@app.get("/api/v1/metrics/summary", tags=["analytics"])
async def get_metrics_summary() -> dict[str, Any]:
    """Return aggregate CRM statistics, cached 30 s."""
    cache_key = "metrics:summary"
    cached = await cache().get(cache_key)
    if cached:
        return json.loads(cached)

    async with db().acquire() as conn:
        summary = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*)           FROM players)                                      AS total_players,
                (SELECT COUNT(*)           FROM transactions)                                  AS total_transactions,
                (SELECT AVG(amount)        FROM transactions)                                  AS avg_transaction_value,
                (SELECT COUNT(DISTINCT player_id) FROM events
                 WHERE created_at >= NOW() - INTERVAL '7 days')                               AS active_last_7_days
            """
        )

    data = {
        "total_players": summary["total_players"],
        "total_transactions": summary["total_transactions"],
        "avg_transaction_value": round(float(summary["avg_transaction_value"] or 0), 2),
        "active_last_7_days": summary["active_last_7_days"],
    }
    await cache().setex(cache_key, CACHE_TTL_SHORT, json.dumps(data))
    return data


@app.get("/api/v1/segments", tags=["analytics"])
async def get_segments() -> list[dict[str, Any]]:
    """Return player count per RFM segment, cached 60 s."""
    return await cached_query(
        "rfm:segments",
        CACHE_TTL_LONG,
        """
        SELECT rfm_segment, COUNT(*) AS player_count
        FROM rfm_scores
        GROUP BY rfm_segment
        ORDER BY player_count DESC
        """,
    )


@app.get("/api/v1/events/recent", tags=["analytics"])
async def get_recent_events() -> list[dict[str, Any]]:
    """Return the 50 most recent events across all players (no cache — always fresh)."""
    async with db().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id, e.player_id, p.username, e.event_type, e.metadata, e.created_at
            FROM events e
            JOIN players p ON p.id = e.player_id
            ORDER BY e.created_at DESC
            LIMIT 50
            """
        )
    return [dict(r) for r in rows]
