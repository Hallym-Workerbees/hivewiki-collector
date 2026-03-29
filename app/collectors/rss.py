from __future__ import annotations

import feedparser

from app.models import CollectedDocument, Source
from app.utils import extract_image_urls, html_to_text, parse_datetime, sha256_text


def collect(source: Source) -> list[CollectedDocument]:
    feed = feedparser.parse(source.target_url)
    docs: list[CollectedDocument] = []

    entries = list(feed.entries)[: source.latest_fetch_limit]

    for entry in entries:
        html = ""
        if "content" in entry and entry.content:
            html = entry.content[0].value
        elif "summary" in entry:
            html = entry.summary

        body_text = html_to_text(html) if html else ""
        image_urls = (
            extract_image_urls(html, base_url=entry.get("link")) if html else []
        )

        external_id = (
            entry.get("id")
            or entry.get("guid")
            or entry.get("link")
            or f"{entry.get('title', '')}|{entry.get('published', '')}"
        )

        docs.append(
            CollectedDocument(
                source_id=source.id,
                external_id=str(external_id),
                canonical_url=entry.get("link", "").strip(),
                title=entry.get("title", "").strip(),
                body_text=body_text,
                published_at=parse_datetime(entry.get("published")),
                content_hash=sha256_text(body_text) if body_text else None,
                image_urls=image_urls,
            )
        )

    return docs
