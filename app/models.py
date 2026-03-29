from dataclasses import dataclass
from datetime import datetime


@dataclass
class Source:
    id: int
    name: str
    source_type: str
    parser_type: str | None
    target_url: str
    category_path: str | None
    enabled: bool
    poll_interval_minutes: int
    latest_fetch_limit: int
    rate_limit_seconds: int
    update_policy: str
    ocr_enabled: bool
    next_run_at: datetime | None


@dataclass
class CollectedDocument:
    source_id: int
    external_id: str
    canonical_url: str
    title: str
    body_text: str
    published_at: datetime | None
    content_hash: str | None
    image_urls: list[str]
