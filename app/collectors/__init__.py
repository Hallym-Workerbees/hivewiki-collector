from __future__ import annotations

from app.collectors import rss
from app.models import CollectedDocument, Source


def collect_documents(source: Source, limit: int) -> list[CollectedDocument]:
    return rss.collect_latest(source, limit=limit)


def collect_backfill_documents(source: Source, limit: int) -> list[CollectedDocument]:
    return rss.collect_backfill(source, limit=limit)
