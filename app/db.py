from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.models import CollectedDocument, Source


def connect(dsn: str):
    return psycopg.connect(dsn, row_factory=dict_row)


def _row_to_source(row: dict[str, Any]) -> Source:
    return Source(
        id=row["id"],
        name=row["name"],
        target_url=row["target_url"],
        enabled=row["enabled"],
        initial_backfill_done=row["initial_backfill_done"],
    )


def get_enabled_sources(conn, limit: int = 50) -> list[Source]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM sources
            WHERE enabled = TRUE
            ORDER BY id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [_row_to_source(row) for row in rows]


def get_backfill_sources(conn, limit: int = 50) -> list[Source]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM sources
            WHERE enabled = TRUE
              AND initial_backfill_done = FALSE
            ORDER BY id ASC
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


def mark_source_success(conn, source_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_success_at = NOW(),
                last_error_at = NULL,
                last_error_message = NULL,
                consecutive_failures = 0,
                updated_at = NOW()
            WHERE id = %s
            """,
            (source_id,),
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
                updated_at = NOW()
            WHERE id = %s
            """,
            (error[:1000], source_id),
        )
    conn.commit()


def mark_backfill_done(conn, source_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET initial_backfill_done = TRUE,
                backfill_completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (source_id,),
        )
    conn.commit()


def get_document(conn, source_id: int, external_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM documents
            WHERE source_id = %s
              AND external_id = %s
            """,
            (source_id, external_id),
        )
        return cur.fetchone()


def upsert_new_rss_document(conn, doc: CollectedDocument) -> bool:
    existing = get_document(conn, doc.source_id, doc.external_id)

    if existing is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (
                    source_id,
                    external_id,
                    canonical_url,
                    title,
                    published_at,
                    body_text,
                    queue_status,
                    first_seen_at,
                    last_seen_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', NOW(), NOW(), NOW(), NOW())
                """,
                (
                    doc.source_id,
                    doc.external_id,
                    doc.canonical_url,
                    doc.title,
                    doc.published_at,
                    doc.body_text,
                ),
            )
        conn.commit()
        return True

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET last_seen_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (existing["id"],),
        )
    conn.commit()
    return False


def get_pending_documents(conn, limit: int = 100) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.*, s.name AS source_name
            FROM documents d
            JOIN sources s ON s.id = d.source_id
            WHERE d.queue_status = 'PENDING'
            ORDER BY d.id ASC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def get_failed_documents_for_retry(
    conn, max_retry_count: int, limit: int = 100
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.*, s.name AS source_name
            FROM documents d
            JOIN sources s ON s.id = d.source_id
            WHERE d.queue_status = 'FAILED'
              AND d.retry_count < %s
            ORDER BY d.id ASC
            LIMIT %s
            """,
            (max_retry_count, limit),
        )
        return cur.fetchall()


def get_dead_candidate_documents(
    conn, max_retry_count: int, limit: int = 100
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM documents
            WHERE queue_status = 'FAILED'
              AND retry_count >= %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (max_retry_count, limit),
        )
        return cur.fetchall()


def mark_document_enqueued(conn, document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'ENQUEUED',
                last_enqueued_at = NOW(),
                last_error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (document_id,),
        )
    conn.commit()


def mark_document_enqueue_failed(conn, document_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'FAILED',
                retry_count = retry_count + 1,
                last_error_message = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (error[:1000], document_id),
        )
    conn.commit()


def mark_document_processing(conn, document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'ENQUEUED',
                updated_at = NOW()
            WHERE id = %s
            """,
            (document_id,),
        )
    conn.commit()


def mark_document_done_and_clear_body(conn, document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'DONE',
                processed_at = NOW(),
                body_text = NULL,
                last_error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (document_id,),
        )
    conn.commit()


def mark_document_failed_by_consumer(conn, document_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'FAILED',
                retry_count = retry_count + 1,
                last_error_message = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (error[:1000], document_id),
        )
    conn.commit()


def mark_document_dead(conn, document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET queue_status = 'DEAD',
                updated_at = NOW()
            WHERE id = %s
            """,
            (document_id,),
        )
    conn.commit()


def get_document_by_source_and_external_id(
    conn,
    source_id: int,
    external_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM documents
            WHERE source_id = %s
              AND external_id = %s
            """,
            (source_id, external_id),
        )
        return cur.fetchone()
