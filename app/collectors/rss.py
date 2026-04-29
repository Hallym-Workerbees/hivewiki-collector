from __future__ import annotations

import feedparser

from app.models import CollectedDocument, Source
from app.utils import html_to_text, parse_datetime


def _entry_to_document(source: Source, entry) -> CollectedDocument | None:
    html = ""
    if "content" in entry and entry.content:
        html = entry.content[0].value
    elif "summary" in entry:
        html = entry.summary

    body_text = html_to_text(html) if html else ""

    canonical_url = (entry.get("link") or "").strip()
    if not canonical_url:
        return None

    return CollectedDocument(
        source_id=source.id,
        canonical_url=canonical_url,
        title=entry.get("title", "").strip(),
        body_text=body_text,
        published_at=parse_datetime(entry.get("published")),
    )


def collect_latest(source: Source, limit: int) -> list[CollectedDocument]:
    feed = feedparser.parse(source.target_url)
    docs: list[CollectedDocument] = []

    for entry in list(feed.entries)[:limit]:
        doc = _entry_to_document(source, entry)
        if doc is not None:
            docs.append(doc)

    return docs


def collect_backfill(source: Source, limit: int) -> list[CollectedDocument]:
    return collect_latest(source, limit=limit)
