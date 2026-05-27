from __future__ import annotations

import logging
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import requests
from requests import HTTPError, Timeout

from app import metrics
from app.config import HTTP_TIMEOUT_SECONDS, RSS_LIMIT_QUERY_PARAM, USER_AGENT
from app.models import CollectedDocument, Source
from app.utils import html_to_text, parse_datetime

logger = logging.getLogger(__name__)


def _entry_to_document(
    source: Source,
    entry,
    base_url: str,
) -> CollectedDocument | None:
    html = ""
    if "content" in entry and entry.content:
        html = entry.content[0].value
    elif "summary" in entry:
        html = entry.summary

    body_text = html_to_text(html) if html else ""

    canonical_url = urljoin(base_url, (entry.get("link") or "").strip())
    if not canonical_url:
        return None

    return CollectedDocument(
        source_id=source.id,
        canonical_url=canonical_url,
        title=_clean_title(entry.get("title", "")),
        body_text=body_text,
        published_at=parse_datetime(entry.get("published")),
    )


def _clean_title(value: str | None) -> str:
    title = (value or "").strip()
    if title.endswith("}"):
        title = title[:-1].rstrip()
    return title


def _with_limit_query_param(url: str, limit: int) -> str:
    split_url = urlsplit(url)
    query_params = [
        (key, value)
        for key, value in parse_qsl(split_url.query, keep_blank_values=True)
        if key != RSS_LIMIT_QUERY_PARAM
    ]
    query_params.append((RSS_LIMIT_QUERY_PARAM, str(limit)))
    return urlunsplit(
        split_url._replace(query=urlencode(query_params)),
    )


def collect_available(source: Source, limit: int) -> list[CollectedDocument]:
    request_url = _with_limit_query_param(source.target_url, limit)
    started_at = time.monotonic()
    logger.info(
        "rss fetch started source_id=%s name=%s limit=%s request_url=%s",
        source.id,
        source.name,
        limit,
        request_url,
    )
    try:
        response = requests.get(
            request_url,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        logger.info(
            "rss fetch response received source_id=%s status_code=%s final_url=%s content_bytes=%s elapsed_ms=%s",
            source.id,
            response.status_code,
            response.url,
            len(response.content),
            int(response.elapsed.total_seconds() * 1000),
        )
        response.raise_for_status()
    except Timeout:
        metrics.record_fetch(
            source,
            "failure",
            time.monotonic() - started_at,
        )
        raise
    except HTTPError:
        metrics.record_fetch(
            source,
            "failure",
            time.monotonic() - started_at,
        )
        raise
    except Exception:
        metrics.record_fetch(
            source,
            "failure",
            time.monotonic() - started_at,
        )
        raise

    try:
        feed = feedparser.parse(response.content)
    except Exception:
        metrics.record_fetch(source, "failure", time.monotonic() - started_at)
        raise

    metrics.record_fetch(source, "success", time.monotonic() - started_at)
    metrics.record_items_seen(source, len(feed.entries))
    docs: list[CollectedDocument] = []
    skipped_entries = 0

    for entry in feed.entries:
        try:
            doc = _entry_to_document(source, entry, response.url)
        except Exception:
            skipped_entries += 1
            logger.exception(
                "rss entry processing failed source_id=%s",
                source.id,
            )
            continue
        if doc is not None:
            docs.append(doc)
        else:
            skipped_entries += 1

    if feed.bozo:
        logger.warning(
            "rss feed parsed with warning source_id=%s entries=%s warning=%s",
            source.id,
            len(feed.entries),
            feed.bozo_exception,
        )

    metrics.record_items_failed(source, skipped_entries)

    logger.info(
        "rss feed parsed source_id=%s entries=%s documents=%s skipped_entries=%s",
        source.id,
        len(feed.entries),
        len(docs),
        skipped_entries,
    )

    return docs


def collect_latest(source: Source, limit: int) -> list[CollectedDocument]:
    return collect_available(source, limit=limit)[:limit]


def collect_backfill(source: Source, limit: int) -> list[CollectedDocument]:
    return collect_latest(source, limit=limit)
