from __future__ import annotations

import time

from app.collectors import collect_backfill_documents, collect_documents
from app.config import (
    DATABASE_DSN,
    LOOP_SLEEP_SECONDS,
    MAX_RETRY_COUNT,
    POLL_INTERVAL_MINUTES,
    SOURCE_BATCH_SIZE,
)
from app.db import (
    connect,
    get_backfill_sources,
    get_dead_candidate_documents,
    get_enabled_sources,
    get_failed_documents_for_retry,
    get_pending_documents,
    mark_backfill_done,
    mark_document_dead,
    mark_document_enqueue_failed,
    mark_document_enqueued,
    mark_source_failure,
    mark_source_polled,
    mark_source_success,
    upsert_new_rss_document,
)
from app.models import CollectedDocument, Source
from app.publisher import enqueue_document, get_redis_client


def process_source(conn, source: Source, backfill: bool = False) -> None:
    mark_source_polled(conn, source.id)

    if backfill:
        docs = collect_backfill_documents(source, max_items=200)
    else:
        docs = collect_documents(source)

    inserted_count = 0
    for doc in docs:
        inserted = upsert_new_rss_document(conn, doc)
        if inserted:
            inserted_count += 1

    mark_source_success(conn, source.id)

    if backfill:
        mark_backfill_done(conn, source.id)

    print(
        f"[INFO] source={source.id} backfill={backfill} docs={len(docs)} inserted={inserted_count}"
    )


def _row_to_doc_and_source(row: dict) -> tuple[Source, CollectedDocument]:
    source = Source(
        id=row["source_id"],
        name=row["source_name"],
        target_url="",
        enabled=True,
        initial_backfill_done=True,
    )

    doc = CollectedDocument(
        source_id=row["source_id"],
        external_id=row["external_id"],
        canonical_url=row["canonical_url"],
        title=row["title"],
        body_text=row["body_text"] or "",
        published_at=row["published_at"],
    )
    return source, doc


def enqueue_pending_documents(conn) -> None:
    redis_client = get_redis_client()
    rows = get_pending_documents(conn, limit=100)

    for row in rows:
        source, doc = _row_to_doc_and_source(row)

        try:
            enqueue_document(redis_client, source, doc)
            mark_document_enqueued(conn, row["id"])
            print(
                f"[INFO] enqueued document_id={row['id']} external_id={row['external_id']}"
            )
        except Exception as e:
            mark_document_enqueue_failed(conn, row["id"], str(e))
            print(f"[ERROR] enqueue failed document_id={row['id']} error={e}")


def retry_failed_documents(conn) -> None:
    redis_client = get_redis_client()
    rows = get_failed_documents_for_retry(
        conn, max_retry_count=MAX_RETRY_COUNT, limit=100
    )

    for row in rows:
        source, doc = _row_to_doc_and_source(row)

        try:
            enqueue_document(redis_client, source, doc)
            mark_document_enqueued(conn, row["id"])
            print(
                f"[INFO] re-enqueued document_id={row['id']} retry_count={row['retry_count']}"
            )
        except Exception as e:
            mark_document_enqueue_failed(conn, row["id"], str(e))
            print(f"[ERROR] re-enqueue failed document_id={row['id']} error={e}")


def mark_dead_documents(conn) -> None:
    rows = get_dead_candidate_documents(
        conn, max_retry_count=MAX_RETRY_COUNT, limit=100
    )

    for row in rows:
        mark_document_dead(conn, row["id"])
        print(f"[WARN] document moved to DEAD document_id={row['id']}")


def run() -> None:
    conn = connect(DATABASE_DSN)

    while True:
        backfill_sources = get_backfill_sources(conn, limit=SOURCE_BATCH_SIZE)

        if backfill_sources:
            for source in backfill_sources:
                try:
                    process_source(conn, source, backfill=True)
                except Exception as e:
                    mark_source_failure(conn, source.id, str(e))
                    print(f"[ERROR] backfill failed source={source.id} error={e}")

            enqueue_pending_documents(conn)
            retry_failed_documents(conn)
            mark_dead_documents(conn)

            time.sleep(LOOP_SLEEP_SECONDS)
            continue

        sources = get_enabled_sources(conn, limit=SOURCE_BATCH_SIZE)

        for source in sources:
            try:
                process_source(conn, source, backfill=False)
            except Exception as e:
                mark_source_failure(conn, source.id, str(e))
                print(f"[ERROR] polling failed source={source.id} error={e}")

        enqueue_pending_documents(conn)
        retry_failed_documents(conn)
        mark_dead_documents(conn)

        time.sleep(POLL_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    run()
