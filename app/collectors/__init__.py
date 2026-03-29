from __future__ import annotations

from app.collectors import html_board, html_page, rss
from app.models import CollectedDocument, Source


def collect_documents(source: Source) -> list[CollectedDocument]:
    if source.source_type == "rss":
        return rss.collect(source)

    if source.source_type == "html_page":
        return html_page.collect(source)

    if source.source_type == "html_board":
        return html_board.collect(source)

    raise ValueError(f"unsupported source_type: {source.source_type}")
