"""Micro-batch Kafka consumer that upserts CDC events into the target PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections import defaultdict
from typing import Any

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("kafka_consumer")

MICRO_BATCH_MAX_MS = 100.0
MICRO_BATCH_MAX_MESSAGES = 500

UPSERT_SQL: dict[str, str] = {
    "players": """
        INSERT INTO players
            (external_id, username, email, tier, registered_at, platform, created_at)
        VALUES
            (%(external_id)s, %(username)s, %(email)s, %(tier)s,
             %(registered_at)s, %(platform)s, %(created_at)s)
        ON CONFLICT (external_id) DO UPDATE SET
            username      = EXCLUDED.username,
            email         = EXCLUDED.email,
            tier          = EXCLUDED.tier,
            registered_at = EXCLUDED.registered_at,
            platform      = EXCLUDED.platform
    """,
    "game_sessions": """
        INSERT INTO game_sessions
            (id, player_id, start_time, duration_minutes, game_type, platform, created_at)
        VALUES
            (%(id)s, %(player_id)s, %(start_time)s, %(duration_minutes)s,
             %(game_type)s, %(platform)s, %(created_at)s)
        ON CONFLICT (id) DO UPDATE SET
            player_id        = EXCLUDED.player_id,
            start_time       = EXCLUDED.start_time,
            duration_minutes = EXCLUDED.duration_minutes,
            game_type        = EXCLUDED.game_type,
            platform         = EXCLUDED.platform
    """,
    "transactions": """
        INSERT INTO transactions
            (id, player_id, amount, currency, payment_method, created_at)
        VALUES
            (%(id)s, %(player_id)s, %(amount)s, %(currency)s, %(payment_method)s, %(created_at)s)
        ON CONFLICT (id) DO UPDATE SET
            player_id      = EXCLUDED.player_id,
            amount         = EXCLUDED.amount,
            currency       = EXCLUDED.currency,
            payment_method = EXCLUDED.payment_method
    """,
    "events": """
        INSERT INTO events
            (id, player_id, event_type, metadata, created_at)
        VALUES
            (%(id)s, %(player_id)s, %(event_type)s, %(metadata)s, %(created_at)s)
        ON CONFLICT (id) DO UPDATE SET
            player_id  = EXCLUDED.player_id,
            event_type = EXCLUDED.event_type,
            metadata   = EXCLUDED.metadata
    """,
}

TABLE_FROM_TOPIC: dict[str, str] = {
    "cdc.public.players": "players",
    "cdc.public.game_sessions": "game_sessions",
    "cdc.public.transactions": "transactions",
    "cdc.public.events": "events",
}


class CDCConsumer:
    """Consumes Debezium CDC events and upserts them into the target PostgreSQL."""

    def __init__(self) -> None:
        self._running = True
        self._messages_consumed: int = 0
        self._batches_processed: int = 0
        self._errors: int = 0

        self._consumer = self._build_consumer()
        self._db_conn = self._build_db_connection()

    def _build_consumer(self) -> Consumer:
        """Construct and subscribe a Confluent Kafka consumer."""
        config = {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": "cdc-target-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 50,
        }
        consumer = Consumer(config)
        consumer.subscribe(
            list(TABLE_FROM_TOPIC.keys()),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        return consumer

    def _build_db_connection(self) -> psycopg2.extensions.connection:
        """Open a psycopg2 connection to the target database."""
        return psycopg2.connect(
            host=os.environ.get("TARGET_DB_HOST", "localhost"),
            port=int(os.environ.get("TARGET_DB_PORT", 5432)),
            dbname=os.environ.get("TARGET_DB_NAME", "target_db"),
            user=os.environ.get("TARGET_DB_USER", "postgres"),
            password=os.environ.get("TARGET_DB_PASSWORD", ""),
        )

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("Partitions assigned: %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("Partitions revoked: %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    def _collect_batch(self) -> tuple[list[Any], list[Any]]:
        """
        Poll messages until MICRO_BATCH_MAX_MS elapses or MICRO_BATCH_MAX_MESSAGES
        are accumulated.

        Returns (messages, raw_kafka_messages) where raw_kafka_messages is kept
        for offset commits.
        """
        deadline = time.monotonic() + MICRO_BATCH_MAX_MS / 1000.0
        messages: list[Any] = []
        raw: list[Any] = []

        while time.monotonic() < deadline and len(messages) < MICRO_BATCH_MAX_MESSAGES:
            remaining = max(0.0, deadline - time.monotonic())
            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                self._errors += 1
                continue
            messages.append(msg)
            raw.append(msg)

        return messages, raw

    def _parse_message(self, msg: Any) -> tuple[str | None, dict[str, Any] | None]:
        """
        Decode a Debezium CDC message.

        Returns (table_name, row_dict) or (None, None) for tombstones / deletes.
        """
        if msg.value() is None:
            return None, None

        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Failed to decode message: %s", exc)
            return None, None

        # After ExtractNewRecordState transform, value IS the row (or null for delete)
        if payload is None or payload.get("__deleted") == "true":
            return None, None

        table = TABLE_FROM_TOPIC.get(msg.topic())
        if table is None:
            log.debug("Unknown topic %s — skipping", msg.topic())
            return None, None

        return table, payload

    def _flush_batch(self, batch: dict[str, list[dict[str, Any]]]) -> None:
        """Bulk-upsert all rows in *batch* into the target database."""
        with self._db_conn.cursor() as cur:
            for table, rows in batch.items():
                if not rows or table not in UPSERT_SQL:
                    continue
                psycopg2.extras.execute_batch(
                    cur,
                    UPSERT_SQL[table],
                    rows,
                    page_size=500,
                )
        self._db_conn.commit()

    def _commit_offsets(self, messages: list[Any]) -> None:
        """Commit Kafka offsets for the processed message batch."""
        if messages:
            self._consumer.commit(message=messages[-1], asynchronous=False)

    def _log_stats(self) -> None:
        log.info(
            "Stats — consumed=%d  batches=%d  errors=%d",
            self._messages_consumed,
            self._batches_processed,
            self._errors,
        )

    def run(self) -> None:
        """Main event loop — runs until SIGTERM or SIGINT."""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        log.info("CDC consumer started. Subscribed to: %s", list(TABLE_FROM_TOPIC.keys()))
        last_stats = time.monotonic()

        try:
            while self._running:
                raw_messages, _ = self._collect_batch()
                if not raw_messages:
                    continue

                batch: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for msg in raw_messages:
                    table, row = self._parse_message(msg)
                    if table and row:
                        batch[table].append(row)

                try:
                    self._flush_batch(batch)
                    self._commit_offsets(raw_messages)
                    n_rows = sum(len(v) for v in batch.values())
                    self._messages_consumed += n_rows
                    self._batches_processed += 1
                    log.debug("Flushed batch: %d rows across %d tables", n_rows, len(batch))
                except psycopg2.Error as exc:
                    log.error("DB upsert failed: %s", exc, exc_info=True)
                    self._errors += 1
                    try:
                        self._db_conn.rollback()
                    except psycopg2.Error:
                        pass

                if time.monotonic() - last_stats > 60:
                    self._log_stats()
                    last_stats = time.monotonic()

        except KafkaException as exc:
            log.critical("Unrecoverable Kafka error: %s", exc)
        finally:
            self._shutdown()

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        log.info("Shutdown signal received (%d) — draining …", signum)
        self._running = False

    def _shutdown(self) -> None:
        log.info("Closing consumer …")
        self._consumer.close()
        self._db_conn.close()
        self._log_stats()


def main() -> None:
    """Entry point."""
    CDCConsumer().run()


if __name__ == "__main__":
    main()
