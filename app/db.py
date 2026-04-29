from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.models import CollectedDocument, IngestionJob, Source


def connect(dsn: str):
    return psycopg.connect(dsn, row_factory=dict_row)


def _row_to_source(row: dict[str, Any]) -> Source:
    return Source(
        id=row["id"],
        name=row["name"],
        target_url=row["target_url"],
        enabled=row["enabled"],
        poll_interval_minutes=row["poll_interval_minutes"],
        next_poll_at=row["next_poll_at"],
        initial_backfill_done=row["initial_backfill_done"],
    )


def _row_to_job(row: dict[str, Any]) -> IngestionJob:
    return IngestionJob(
        id=row["job_id"],
        source_document_id=row["source_document_id"],
        status=row["job_status"],
        retry_count=row["retry_count"],
        error_message=row["error_message"],
        queued_at=row["queued_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def get_due_sources(conn, limit: int = 50) -> list[Source]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM sources
            WHERE enabled = TRUE
              AND next_poll_at <= NOW()
            ORDER BY initial_backfill_done ASC, next_poll_at ASC, id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [_row_to_source(row) for row in rows]


def mark_source_polled(conn, source_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_polled_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (source_id,),
        )
    conn.commit()


def mark_source_success(conn, source_id: int, *, backfill_done: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET initial_backfill_done = CASE
                    WHEN %s THEN TRUE
                    ELSE initial_backfill_done
                END,
                backfill_completed_at = CASE
                    WHEN %s THEN COALESCE(backfill_completed_at, NOW())
                    ELSE backfill_completed_at
                END,
                last_success_at = NOW(),
                last_error_at = NULL,
                last_error_message = NULL,
                consecutive_failures = 0,
                next_poll_at = NOW() + (poll_interval_minutes * INTERVAL '1 minute'),
                updated_at = NOW()
            WHERE id = %s
            """,
            (backfill_done, backfill_done, source_id),
        )
    conn.commit()


def mark_source_failure(conn, source_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_error_at = NOW(),
                last_error_message = %s,
                consecutive_failures = consecutive_failures + 1,
                next_poll_at = NOW() + (poll_interval_minutes * INTERVAL '1 minute'),
                updated_at = NOW()
            WHERE id = %s
            """,
            (error[:1000], source_id),
        )
    conn.commit()


def insert_source_document(conn, doc: CollectedDocument) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_documents (
                source_id,
                canonical_url,
                title,
                published_at,
                body_text,
                fetch_status,
                wiki_status,
                collected_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                'NOT_REQUESTED',
                NOW()
            )
            ON CONFLICT (source_id, canonical_url) DO NOTHING
            RETURNING id
            """,
            (
                doc.source_id,
                doc.canonical_url,
                doc.title,
                doc.published_at,
                doc.body_text or None,
                "COMPLETED" if doc.body_text else "PENDING",
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return row["id"] if row else None


def create_ingestion_job(conn, source_document_id: int) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_jobs (
                source_document_id,
                status,
                retry_count,
                queued_at
            )
            SELECT %s, 'QUEUED', 0, NOW()
            WHERE NOT EXISTS (
                SELECT 1
                FROM ingestion_jobs
                WHERE source_document_id = %s
            )
            RETURNING id
            """,
            (source_document_id, source_document_id),
        )
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE source_documents
                SET wiki_status = 'QUEUED'
                WHERE id = %s
                """,
                (source_document_id,),
            )
    conn.commit()
    return row["id"] if row else None


def save_new_documents(conn, docs: list[CollectedDocument]) -> tuple[int, int]:
    inserted_documents = 0
    queued_jobs = 0

    for doc in docs:
        source_document_id = insert_source_document(conn, doc)
        if source_document_id is None:
            continue

        inserted_documents += 1
        job_id = create_ingestion_job(conn, source_document_id)
        if job_id is not None:
            queued_jobs += 1

    return inserted_documents, queued_jobs


def get_jobs_ready_to_publish(
    conn, max_retry_count: int, limit: int = 100
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                j.id AS job_id,
                j.source_document_id,
                j.status AS job_status,
                j.retry_count,
                j.error_message,
                j.queued_at,
                j.started_at,
                j.completed_at,
                s.id AS source_id,
                s.name AS source_name,
                s.target_url,
                d.canonical_url,
                d.title,
                d.body_text,
                d.published_at
            FROM ingestion_jobs j
            JOIN source_documents d ON d.id = j.source_document_id
            JOIN sources s ON s.id = d.source_id
            WHERE j.status IN ('QUEUED', 'FAILED')
              AND j.retry_count <= %s
            ORDER BY j.queued_at ASC, j.id ASC
            LIMIT %s
            """,
            (max_retry_count, limit),
        )
        return cur.fetchall()


def mark_job_published(conn, job_id: int, source_document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_jobs
            SET status = 'ENQUEUED',
                error_message = NULL
            WHERE id = %s
            """,
            (job_id,),
        )
        cur.execute(
            """
            UPDATE source_documents
            SET wiki_status = 'QUEUED'
            WHERE id = %s
            """,
            (source_document_id,),
        )
    conn.commit()


def mark_job_publish_failed(
    conn,
    job_id: int,
    source_document_id: int,
    error: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_jobs
            SET status = 'FAILED',
                retry_count = retry_count + 1,
                error_message = %s
            WHERE id = %s
            """,
            (error[:1000], job_id),
        )
        cur.execute(
            """
            UPDATE source_documents
            SET wiki_status = 'FAILED'
            WHERE id = %s
            """,
            (source_document_id,),
        )
    conn.commit()


def mark_jobs_dead(conn, max_retry_count: int, limit: int = 100) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_document_id
            FROM ingestion_jobs
            WHERE status = 'FAILED'
              AND retry_count > %s
            ORDER BY queued_at ASC, id ASC
            LIMIT %s
            """,
            (max_retry_count, limit),
        )
        rows = cur.fetchall()

        dead_job_ids: list[int] = []
        for row in rows:
            cur.execute(
                """
                UPDATE ingestion_jobs
                SET status = 'DEAD'
                WHERE id = %s
                """,
                (row["id"],),
            )
            cur.execute(
                """
                UPDATE source_documents
                SET wiki_status = 'DEAD'
                WHERE id = %s
                """,
                (row["source_document_id"],),
            )
            dead_job_ids.append(row["id"])

    conn.commit()
    return dead_job_ids


def build_source_from_job_row(row: dict[str, Any]) -> Source:
    return Source(
        id=row["source_id"],
        name=row["source_name"],
        target_url=row["target_url"],
        enabled=True,
        poll_interval_minutes=0,
        next_poll_at=None,
        initial_backfill_done=True,
    )


def build_document_from_job_row(row: dict[str, Any]) -> CollectedDocument:
    return CollectedDocument(
        source_id=row["source_id"],
        canonical_url=row["canonical_url"],
        title=row["title"],
        body_text=row["body_text"] or "",
        published_at=row["published_at"],
    )


def build_job_from_row(row: dict[str, Any]) -> IngestionJob:
    return _row_to_job(row)
