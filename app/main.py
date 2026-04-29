from __future__ import annotations

import logging
import time

import psycopg

from app.collectors import collect_backfill_documents, collect_documents
from app.config import (
    DATABASE_DSN,
    DB_RECONNECT_SLEEP_SECONDS,
    INCREMENTAL_FETCH_LIMIT,
    INITIAL_BACKFILL_LIMIT,
    JOB_PUBLISH_CLAIM_TIMEOUT_SECONDS,
    LOOP_SLEEP_SECONDS,
    MAX_RETRY_COUNT,
    QUEUE_BATCH_SIZE,
    SOURCE_BATCH_SIZE,
)
from app.db import (
    build_document_from_job_row,
    build_job_from_row,
    build_source_from_job_row,
    claim_due_sources,
    claim_jobs_ready_to_publish,
    connect,
    mark_job_publish_failed,
    mark_job_published,
    mark_jobs_dead,
    mark_source_failure,
    mark_source_success,
    save_new_documents,
)
from app.models import Source
from app.publisher import enqueue_document_with_retry, get_redis_client

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def process_source(conn, source: Source) -> None:
    is_backfill = not source.initial_backfill_done
    fetch_limit = INITIAL_BACKFILL_LIMIT if is_backfill else INCREMENTAL_FETCH_LIMIT

    if is_backfill:
        docs = collect_backfill_documents(source, limit=fetch_limit)
    else:
        docs = collect_documents(source, limit=fetch_limit)

    inserted_count, queued_job_count = save_new_documents(conn, docs)
    mark_source_success(conn, source.id, backfill_done=is_backfill)

    logger.info(
        "source processed source_id=%s name=%s backfill=%s fetched=%s inserted=%s queued_jobs=%s next_interval_minutes=%s",
        source.id,
        source.name,
        is_backfill,
        len(docs),
        inserted_count,
        queued_job_count,
        source.poll_interval_minutes,
    )


def safe_mark_source_failure(conn, source: Source, error: str) -> None:
    mark_source_failure(conn, source.id, error)
    logger.exception(
        "source processing failed source_id=%s name=%s",
        source.id,
        source.name,
    )


def publish_ingestion_jobs(conn, redis_client):
    rows = claim_jobs_ready_to_publish(
        conn,
        max_retry_count=MAX_RETRY_COUNT,
        claim_timeout_seconds=JOB_PUBLISH_CLAIM_TIMEOUT_SECONDS,
        limit=QUEUE_BATCH_SIZE,
    )

    if not rows:
        logger.debug("no ingestion jobs ready for publishing")
        return redis_client

    for row in rows:
        source = build_source_from_job_row(row)
        document = build_document_from_job_row(row)
        job = build_job_from_row(row)

        try:
            redis_client = enqueue_document_with_retry(
                redis_client,
                source,
                document,
                job,
            )
        except Exception as exc:
            mark_job_publish_failed(conn, job.id, job.source_document_id, str(exc))
            logger.exception(
                "job publish failed job_id=%s source_document_id=%s",
                job.id,
                job.source_document_id,
            )
            continue

        mark_job_published(conn, job.id, job.source_document_id)
        logger.info(
            "job published job_id=%s source_document_id=%s canonical_url=%s",
            job.id,
            job.source_document_id,
            document.canonical_url,
        )

    return redis_client


def open_db_connection() -> psycopg.Connection:
    while True:
        try:
            conn = connect(DATABASE_DSN)
            logger.info(
                "database connection established host_dsn=%s",
                DATABASE_DSN.rsplit("@", maxsplit=1)[-1],
            )
            return conn
        except psycopg.Error:
            logger.exception(
                "database connection failed; retrying in %s seconds",
                DB_RECONNECT_SLEEP_SECONDS,
            )
            time.sleep(DB_RECONNECT_SLEEP_SECONDS)


def reconnect_db(conn: psycopg.Connection | None) -> psycopg.Connection:
    if conn is not None and not conn.closed:
        try:
            conn.close()
        except psycopg.Error:
            logger.exception("failed to close broken database connection cleanly")

    return open_db_connection()


def close_dead_jobs(conn) -> None:
    dead_job_ids = mark_jobs_dead(
        conn, max_retry_count=MAX_RETRY_COUNT, limit=QUEUE_BATCH_SIZE
    )
    for job_id in dead_job_ids:
        logger.warning("job moved to DEAD job_id=%s", job_id)


def run() -> None:
    configure_logging()
    conn = open_db_connection()
    redis_client = get_redis_client()
    logger.info("collector started")

    while True:
        try:
            due_sources = claim_due_sources(conn, limit=SOURCE_BATCH_SIZE)

            if due_sources:
                logger.info("processing due sources count=%s", len(due_sources))

            for source in due_sources:
                try:
                    process_source(conn, source)
                except psycopg.Error:
                    raise
                except Exception as exc:
                    safe_mark_source_failure(conn, source, str(exc))

            redis_client = publish_ingestion_jobs(conn, redis_client)
            close_dead_jobs(conn)
        except psycopg.Error:
            logger.exception("database operation failed; reconnecting")
            conn = reconnect_db(conn)
        except Exception:
            logger.exception("collector loop failed unexpectedly")

        time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    run()
