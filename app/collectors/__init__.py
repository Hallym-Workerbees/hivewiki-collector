from __future__ import annotations

from app.collectors import rss
from app.models import CollectedDocument, Source


def collect_documents(source: Source) -> list[CollectedDocument]:
    return rss.collect_latest(source)


def collect_backfill_documents(
    source: Source, max_items: int = 200
) -> list[CollectedDocument]:
    return rss.collect_backfill(source, max_items=max_items)
