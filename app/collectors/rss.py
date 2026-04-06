from __future__ import annotations

import feedparser

from app.config import RSS_FETCH_LIMIT
from app.models import CollectedDocument, Source
from app.utils import html_to_text, parse_datetime


def _entry_to_document(source: Source, entry) -> CollectedDocument:
    html = ""
    if "content" in entry and entry.content:
        html = entry.content[0].value
    elif "summary" in entry:
        html = entry.summary

    body_text = html_to_text(html) if html else ""

    external_id = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or f"{entry.get('title', '')}|{entry.get('published', '')}"
    )

    return CollectedDocument(
        source_id=source.id,
        external_id=str(external_id),
        canonical_url=entry.get("link", "").strip(),
        title=entry.get("title", "").strip(),
        body_text=body_text,
        published_at=parse_datetime(entry.get("published")),
    )


def collect_latest(source: Source) -> list[CollectedDocument]:
    feed = feedparser.parse(source.target_url)
    entries = list(feed.entries)[:RSS_FETCH_LIMIT]
    return [_entry_to_document(source, entry) for entry in entries]


def collect_backfill(source: Source, max_items: int = 200) -> list[CollectedDocument]:
    feed = feedparser.parse(source.target_url)
    entries = list(feed.entries)[:max_items]
    return [_entry_to_document(source, entry) for entry in entries]
