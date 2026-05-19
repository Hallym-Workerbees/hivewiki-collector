from __future__ import annotations

import logging
import time

import psycopg

from app.collectors import collect_backfill_documents, collect_documents
from app.config import (
    DATABASE_DSN,
    DB_RECONNECT_SLEEP_SECONDS,
    INCREMENTAL_MAX_SCAN_LIMIT,
    INCREMENTAL_SCAN_LIMIT,
    INITIAL_BACKFILL_LIMIT,
    JOB_PUBLISH_CLAIM_TIMEOUT_SECONDS,
    LOOP_SLEEP_SECONDS,
    MAX_RETRY_COUNT,
    PROBE_HOST,
    PROBE_PORT,
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
from app.probe import start_probe_server
from app.publisher import enqueue_document_with_retry, get_redis_client

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def process_source(conn, source: Source) -> None:
    is_backfill = not source.initial_backfill_done
    fetch_limit = INITIAL_BACKFILL_LIMIT if is_backfill else INCREMENTAL_SCAN_LIMIT

    logger.info(
        "source processing started source_id=%s name=%s backfill=%s target_url=%s fetch_limit=%s poll_interval_minutes=%s",
        source.id,
        source.name,
        is_backfill,
        source.target_url,
        fetch_limit,
        source.poll_interval_minutes,
    )

    if is_backfill:
        docs = collect_backfill_documents(source, limit=fetch_limit)
        logger.info(
            "source backfill fetched source_id=%s name=%s fetched=%s fetch_limit=%s",
            source.id,
            source.name,
            len(docs),
            fetch_limit,
        )
        inserted_count, queued_job_count = save_new_documents(conn, docs)
    else:
        docs = []
        inserted_count = 0
        queued_job_count = 0
        seen_urls = set()
        scan_limit = INCREMENTAL_SCAN_LIMIT
        keep_scanning = True

        while keep_scanning:
            logger.info(
                "source incremental scan started source_id=%s name=%s scan_limit=%s seen_urls=%s",
                source.id,
                source.name,
                scan_limit,
                len(seen_urls),
            )
            available_docs = [
                doc
                for doc in collect_documents(source, limit=scan_limit)
                if doc.canonical_url not in seen_urls
            ]
            logger.info(
                "source incremental scan fetched source_id=%s name=%s scan_limit=%s available=%s seen_urls=%s",
                source.id,
                source.name,
                scan_limit,
                len(available_docs),
                len(seen_urls),
            )

            for doc in available_docs:
                doc_inserted_count, doc_queued_job_count = save_new_documents(
                    conn,
                    [doc],
                )
                docs.append(doc)
                seen_urls.add(doc.canonical_url)
                inserted_count += doc_inserted_count
                queued_job_count += doc_queued_job_count

                if doc_inserted_count == 0:
                    logger.info(
                        "source incremental scan stopping at existing document source_id=%s name=%s canonical_url=%s scanned=%s inserted=%s queued_jobs=%s",
                        source.id,
                        source.name,
                        doc.canonical_url,
                        len(docs),
                        inserted_count,
                        queued_job_count,
                    )
                    keep_scanning = False
                    break

            if len(available_docs) < scan_limit or not keep_scanning:
                if len(available_docs) < scan_limit:
                    logger.info(
                        "source incremental scan stopping because feed returned fewer entries source_id=%s name=%s scan_limit=%s available=%s",
                        source.id,
                        source.name,
                        scan_limit,
                        len(available_docs),
                    )
                break

            if scan_limit >= INCREMENTAL_MAX_SCAN_LIMIT:
                logger.info(
                    "source incremental scan reached max limit source_id=%s name=%s scan_limit=%s max_scan_limit=%s",
                    source.id,
                    source.name,
                    scan_limit,
                    INCREMENTAL_MAX_SCAN_LIMIT,
                )
                break

            if keep_scanning and len(seen_urls) >= scan_limit:
                scan_limit = min(scan_limit * 2, INCREMENTAL_MAX_SCAN_LIMIT)
                logger.info(
                    "source incremental scan limit increased source_id=%s name=%s next_scan_limit=%s seen_urls=%s",
                    source.id,
                    source.name,
                    scan_limit,
                    len(seen_urls),
                )
            else:
                break

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
        "source processing failed source_id=%s name=%s target_url=%s error=%s",
        source.id,
        source.name,
        source.target_url,
        error,
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

    logger.info("publishing ingestion jobs claimed count=%s", len(rows))
    published_count = 0
    failed_count = 0

    for row in rows:
        source = build_source_from_job_row(row)
        document = build_document_from_job_row(row)
        job = build_job_from_row(row)

        try:
            logger.info(
                "job publish started job_id=%s source_document_id=%s source_id=%s source_name=%s canonical_url=%s retry_count=%s",
                job.id,
                job.source_document_id,
                source.id,
                source.name,
                document.canonical_url,
                job.retry_count,
            )
            redis_client = enqueue_document_with_retry(
                redis_client,
                source,
                document,
                job,
            )
        except Exception as exc:
            mark_job_publish_failed(conn, job.id, job.source_document_id, str(exc))
            logger.exception(
                "job publish failed job_id=%s source_document_id=%s source_id=%s canonical_url=%s error=%s",
                job.id,
                job.source_document_id,
                source.id,
                document.canonical_url,
                exc,
            )
            failed_count += 1
            continue

        mark_job_published(conn, job.id, job.source_document_id)
        published_count += 1
        logger.info(
            "job published job_id=%s source_document_id=%s source_id=%s source_name=%s canonical_url=%s",
            job.id,
            job.source_document_id,
            source.id,
            source.name,
            document.canonical_url,
        )

    logger.info(
        "publishing ingestion jobs completed claimed=%s published=%s failed=%s",
        len(rows),
        published_count,
        failed_count,
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
    if dead_job_ids:
        logger.warning(
            "jobs moved to DEAD count=%s job_ids=%s",
            len(dead_job_ids),
            dead_job_ids,
        )
    for job_id in dead_job_ids:
        logger.warning("job moved to DEAD job_id=%s", job_id)


def run() -> None:
    configure_logging()
    start_probe_server(PROBE_HOST, PROBE_PORT)
    conn = open_db_connection()
    redis_client = get_redis_client()
    logger.info("collector started")

    while True:
        try:
            due_sources = claim_due_sources(conn, limit=SOURCE_BATCH_SIZE)

            if due_sources:
                logger.info(
                    "processing due sources count=%s source_ids=%s",
                    len(due_sources),
                    [source.id for source in due_sources],
                )

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
