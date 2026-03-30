from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.rows import dict_row

from app.models import CollectedDocument, Source


def now_utc() -> datetime:
    return datetime.now(UTC)


def connect(dsn: str):
    return psycopg.connect(dsn, row_factory=dict_row)


def init_schema(conn, schema_path: str = "schema.sql") -> None:
    with open(schema_path, encoding="utf-8") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _row_to_source(row: dict) -> Source:
    return Source(
        id=row["id"],
        name=row["name"],
        source_type=row["source_type"],
        parser_type=row["parser_type"],
        target_url=row["target_url"],
        category_path=row["category_path"],
        enabled=row["enabled"],
        poll_interval_minutes=row["poll_interval_minutes"],
        latest_fetch_limit=row["latest_fetch_limit"],
        rate_limit_seconds=row["rate_limit_seconds"],
        update_policy=row["update_policy"],
        ocr_enabled=row["ocr_enabled"],
        next_run_at=row["next_run_at"],
    )


def get_due_sources(conn, limit: int = 10) -> list[Source]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM sources
            WHERE enabled = TRUE
              AND (next_run_at IS NULL OR next_run_at <= NOW())
            ORDER BY next_run_at NULLS FIRST, id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [_row_to_source(row) for row in rows]


def mark_source_running(conn, source_id: int) -> None:
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


def mark_source_success(conn, source: Source) -> None:
    next_run_at = now_utc() + timedelta(minutes=source.poll_interval_minutes)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_success_at = NOW(),
                last_error_at = NULL,
                last_error_message = NULL,
                consecutive_failures = 0,
                next_run_at = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (next_run_at, source.id),
        )
    conn.commit()


def mark_source_failure(conn, source: Source, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT consecutive_failures
            FROM sources
            WHERE id = %s
            """,
            (source.id,),
        )
        row = cur.fetchone()

    failures = (row["consecutive_failures"] if row else 0) + 1
    backoff_minutes = min(source.poll_interval_minutes * failures, 180)
    next_run_at = now_utc() + timedelta(minutes=backoff_minutes)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_error_at = NOW(),
                last_error_message = %s,
                consecutive_failures = %s,
                next_run_at = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (error[:1000], failures, next_run_at, source.id),
        )
    conn.commit()


def get_document(conn, source_id: int, external_id: str) -> dict | None:
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


def should_dispatch(conn, source: Source, doc: CollectedDocument) -> bool:
    existing = get_document(conn, source.id, doc.external_id)

    # 처음 보는 문서
    if existing is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents (
                    source_id, external_id, canonical_url, title,
                    published_at, content_hash,
                    first_seen_at, last_seen_at, process_status,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), 'PENDING', NOW(), NOW())
                """,
                (
                    source.id,
                    doc.external_id,
                    doc.canonical_url,
                    doc.title,
                    doc.published_at,
                    doc.content_hash,
                ),
            )
        conn.commit()
        return True

    # 이미 있는 문서는 최소 last_seen, title 등 갱신
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET canonical_url = %s,
                title = %s,
                published_at = %s,
                last_seen_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                doc.canonical_url,
                doc.title,
                doc.published_at,
                existing["id"],
            ),
        )
    conn.commit()

    # RSS는 새 글만
    if source.update_policy == "new_only":
        return False

    # 크롤링/정적 페이지는 수정 감지
    if source.update_policy == "detect_changes":
        old_hash = existing["content_hash"]
        if old_hash != doc.content_hash:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE documents
                    SET content_hash = %s,
                        process_status = 'PENDING',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (doc.content_hash, existing["id"]),
                )
            conn.commit()
            return True

    return False


def mark_document_dispatched(conn, source_id: int, external_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET last_dispatched_at = NOW(),
                process_status = 'DONE',
                updated_at = NOW()
            WHERE source_id = %s
              AND external_id = %s
            """,
            (source_id, external_id),
        )
    conn.commit()


def mark_document_failed(conn, source_id: int, external_id: str, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
            SET process_status = 'FAILED',
                retry_count = retry_count + 1,
                last_error_message = %s,
                updated_at = NOW()
            WHERE source_id = %s
              AND external_id = %s
            """,
            (error[:1000], source_id, external_id),
        )
    conn.commit()
