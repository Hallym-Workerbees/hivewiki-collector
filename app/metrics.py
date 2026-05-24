from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from app.models import Source

DISPATCH_TARGET_WIKIFIER = "wikifier"

REASON_HTTP_ERROR = "http_error"
REASON_TIMEOUT = "timeout"
REASON_PARSE_ERROR = "parse_error"
REASON_DB_ERROR = "db_error"
REASON_DISPATCH_ERROR = "dispatch_error"
REASON_UNKNOWN = "unknown"

VALID_REASONS = {
    REASON_HTTP_ERROR,
    REASON_TIMEOUT,
    REASON_PARSE_ERROR,
    REASON_DB_ERROR,
    REASON_DISPATCH_ERROR,
    REASON_UNKNOWN,
}


rss_fetch_total = Counter(
    "collector_rss_fetch_total",
    "Total RSS fetch attempts by source and status.",
    ("source", "status"),
)
rss_items_seen_total = Counter(
    "collector_rss_items_seen_total",
    "Total RSS feed items seen by source.",
    ("source",),
)
rss_items_new_total = Counter(
    "collector_rss_items_new_total",
    "Total new RSS documents by source.",
    ("source",),
)
rss_items_unchanged_total = Counter(
    "collector_rss_items_unchanged_total",
    "Total unchanged RSS documents by source.",
    ("source",),
)
rss_items_failed_total = Counter(
    "collector_rss_items_failed_total",
    "Total RSS items that failed item-level processing by source.",
    ("source",),
)
document_hash_changed_total = Counter(
    "collector_document_hash_changed_total",
    "Total documents treated as changed by source.",
    ("source",),
)
document_hash_unchanged_total = Counter(
    "collector_document_hash_unchanged_total",
    "Total documents treated as unchanged by source.",
    ("source",),
)
dispatch_total = Counter(
    "collector_dispatch_total",
    "Total dispatch attempts by source, target, and status.",
    ("source", "target", "status"),
)
dispatch_failed_total = Counter(
    "collector_dispatch_failed_total",
    "Total failed dispatch attempts by source, target, and bounded reason.",
    ("source", "target", "reason"),
)
loop_errors_total = Counter(
    "collector_loop_errors_total",
    "Total unexpected collector loop errors.",
)

rss_fetch_duration_seconds = Histogram(
    "collector_rss_fetch_duration_seconds",
    "RSS fetch duration in seconds by source.",
    ("source",),
)
dispatch_duration_seconds = Histogram(
    "collector_dispatch_duration_seconds",
    "Dispatch duration in seconds by source and target.",
    ("source", "target"),
)
loop_duration_seconds = Histogram(
    "collector_loop_duration_seconds",
    "Collector polling loop duration in seconds.",
)

last_success_timestamp = Gauge(
    "collector_last_success_timestamp",
    "Last successful source processing Unix timestamp.",
    ("source",),
)
last_failure_timestamp = Gauge(
    "collector_last_failure_timestamp",
    "Last failed source processing Unix timestamp.",
    ("source",),
)
consecutive_failures = Gauge(
    "collector_consecutive_failures",
    "Current consecutive source processing failures.",
    ("source",),
)
source_next_run_timestamp = Gauge(
    "collector_source_next_run_timestamp",
    "Next scheduled source run Unix timestamp.",
    ("source",),
)
source_enabled = Gauge(
    "collector_source_enabled",
    "Whether a source is enabled, as 1 or 0.",
    ("source",),
)
pending_documents = Gauge(
    "collector_pending_documents",
    "Current number of pending documents.",
)
failed_documents = Gauge(
    "collector_failed_documents",
    "Current number of failed documents.",
)
dead_documents = Gauge(
    "collector_dead_documents",
    "Current number of dead documents.",
)
enabled_sources = Gauge(
    "collector_enabled_sources",
    "Current number of enabled sources.",
)
disabled_sources = Gauge(
    "collector_disabled_sources",
    "Current number of disabled sources.",
)
active_sources = Gauge(
    "collector_active_sources",
    "Current number of enabled sources due to run.",
)


def source_label(source_or_id: Source | int | str) -> str:
    if isinstance(source_or_id, Source):
        return str(source_or_id.id)
    return str(source_or_id)


def bounded_reason(reason: str) -> str:
    return reason if reason in VALID_REASONS else REASON_UNKNOWN


def unix_timestamp(value: datetime | None) -> float | None:
    if value is None:
        return None
    return value.timestamp()


def now() -> float:
    return time.time()


def record_fetch(source: Source | int | str, status: str, duration_seconds: float) -> None:
    label = source_label(source)
    rss_fetch_total.labels(source=label, status=status).inc()
    rss_fetch_duration_seconds.labels(source=label).observe(duration_seconds)


def record_items_seen(source: Source | int | str, count: int) -> None:
    if count > 0:
        rss_items_seen_total.labels(source=source_label(source)).inc(count)


def record_items_failed(source: Source | int | str, count: int) -> None:
    if count > 0:
        rss_items_failed_total.labels(source=source_label(source)).inc(count)


def record_document_results(
    source: Source | int | str,
    *,
    new_count: int,
    unchanged_count: int,
) -> None:
    label = source_label(source)
    if new_count > 0:
        rss_items_new_total.labels(source=label).inc(new_count)
        document_hash_changed_total.labels(source=label).inc(new_count)
    if unchanged_count > 0:
        rss_items_unchanged_total.labels(source=label).inc(unchanged_count)
        document_hash_unchanged_total.labels(source=label).inc(unchanged_count)


def record_dispatch(
    source: Source | int | str,
    target: str,
    status: str,
    duration_seconds: float,
) -> None:
    label = source_label(source)
    dispatch_total.labels(source=label, target=target, status=status).inc()
    dispatch_duration_seconds.labels(source=label, target=target).observe(duration_seconds)


def record_dispatch_failure(
    source: Source | int | str,
    target: str,
    reason: str,
) -> None:
    dispatch_failed_total.labels(
        source=source_label(source),
        target=target,
        reason=bounded_reason(reason),
    ).inc()


def record_source_success(source: Source) -> None:
    label = source_label(source)
    last_success_timestamp.labels(source=label).set(now())
    consecutive_failures.labels(source=label).set(0)
    update_source_schedule(source)


def record_source_failure(source: Source) -> None:
    label = source_label(source)
    last_failure_timestamp.labels(source=label).set(now())
    consecutive_failures.labels(source=label).set(source.consecutive_failures + 1)
    update_source_schedule(source)


def update_source_schedule(source: Source) -> None:
    label = source_label(source)
    source_enabled.labels(source=label).set(1 if source.enabled else 0)
    next_run_timestamp = unix_timestamp(source.next_poll_at)
    if next_run_timestamp is not None:
        source_next_run_timestamp.labels(source=label).set(next_run_timestamp)


def update_source_gauges(rows: list[dict[str, Any]]) -> None:
    enabled_count = 0
    disabled_count = 0
    active_count = 0

    for row in rows:
        label = source_label(row["id"])
        is_enabled = bool(row["enabled"])
        if is_enabled:
            enabled_count += 1
        else:
            disabled_count += 1
        if bool(row["active"]):
            active_count += 1

        source_enabled.labels(source=label).set(1 if is_enabled else 0)
        consecutive_failures.labels(source=label).set(row["consecutive_failures"])

        next_run_timestamp = unix_timestamp(row["next_poll_at"])
        if next_run_timestamp is not None:
            source_next_run_timestamp.labels(source=label).set(next_run_timestamp)

        success_timestamp = unix_timestamp(row["last_success_at"])
        if success_timestamp is not None:
            last_success_timestamp.labels(source=label).set(success_timestamp)

        failure_timestamp = unix_timestamp(row["last_error_at"])
        if failure_timestamp is not None:
            last_failure_timestamp.labels(source=label).set(failure_timestamp)

    enabled_sources.set(enabled_count)
    disabled_sources.set(disabled_count)
    active_sources.set(active_count)


def update_document_gauges(counts: dict[str, int]) -> None:
    pending_documents.set(counts.get("pending", 0))
    failed_documents.set(counts.get("failed", 0))
    dead_documents.set(counts.get("dead", 0))
