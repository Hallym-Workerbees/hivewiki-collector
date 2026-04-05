from __future__ import annotations

import json

from redis import Redis

from app.config import REDIS_DB, REDIS_HOST, REDIS_PORT, REDIS_QUEUE_NAME
from app.models import CollectedDocument, Source


def get_redis_client() -> Redis:
    return Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def enqueue_document(
    redis_client: Redis, source: Source, doc: CollectedDocument
) -> None:
    payload = {
        "source": {
            "id": source.id,
            "name": source.name,
        },
        "document": {
            "source_id": doc.source_id,
            "external_id": doc.external_id,
            "canonical_url": doc.canonical_url,
            "title": doc.title,
            "body_text": doc.body_text,
            "published_at": doc.published_at.isoformat() if doc.published_at else None,
        },
    }

    redis_client.rpush(REDIS_QUEUE_NAME, json.dumps(payload, ensure_ascii=False))
