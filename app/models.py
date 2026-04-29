from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Source:
    id: int
    name: str
    target_url: str
    enabled: bool
    poll_interval_minutes: int
    next_poll_at: datetime | None
    initial_backfill_done: bool


@dataclass
class CollectedDocument:
    source_id: int
    canonical_url: str
    title: str
    body_text: str
    published_at: datetime | None


@dataclass
class IngestionJob:
    id: int
    source_document_id: int
    status: str
    retry_count: int
    error_message: str | None
    queued_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
