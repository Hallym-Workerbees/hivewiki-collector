from __future__ import annotations

import time

from app.collectors import collect_documents
from app.config import DATABASE_DSN, DUE_SOURCE_BATCH_SIZE, LOOP_SLEEP_SECONDS
from app.db import (
    connect,
    get_due_sources,
    init_schema,
    mark_document_dispatched,
    mark_document_failed,
    mark_source_failure,
    mark_source_running,
    mark_source_success,
    should_dispatch,
)
from app.publisher import publish


def run() -> None:
    conn = connect(DATABASE_DSN)
    init_schema(conn)

    while True:
        due_sources = get_due_sources(conn, limit=DUE_SOURCE_BATCH_SIZE)

        if not due_sources:
            time.sleep(LOOP_SLEEP_SECONDS)
            continue

        for source in due_sources:
            try:
                print(
                    f"[INFO] collecting source={source.id} name={source.name} type={source.source_type}"
                )
                mark_source_running(conn, source.id)

                docs = collect_documents(source)

                for doc in docs:
                    if not should_dispatch(conn, source, doc):
                        continue

                    try:
                        publish(source, doc)
                        mark_document_dispatched(conn, source.id, doc.external_id)
                        print(
                            f"[INFO] dispatched source={source.id} external_id={doc.external_id}"
                        )
                    except Exception as e:
                        mark_document_failed(conn, source.id, doc.external_id, str(e))
                        print(
                            f"[ERROR] document dispatch failed source={source.id} external_id={doc.external_id} error={e}"
                        )

                    # source별 요청 간격
                    time.sleep(source.rate_limit_seconds)

                mark_source_success(conn, source)

            except Exception as e:
                mark_source_failure(conn, source, str(e))
                print(
                    f"[ERROR] source failed id={source.id} name={source.name} error={e}"
                )

        time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    run()
