from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Source:
    id: int
    name: str
    target_url: str
    enabled: bool
    initial_backfill_done: bool


@dataclass
class CollectedDocument:
    source_id: int
    external_id: str
    canonical_url: str
    title: str
    body_text: str
    published_at: datetime | None
