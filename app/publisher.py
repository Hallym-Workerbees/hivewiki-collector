from __future__ import annotations

import json

from redis import Redis
from redis.exceptions import RedisError

from app.config import (
    REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    REDIS_QUEUE_NAME,
    REDIS_SOCKET_TIMEOUT_SECONDS,
    REDIS_URL,
)
from app.models import CollectedDocument, IngestionJob, Source


def get_redis_client() -> Redis:
    common_kwargs = {
        "decode_responses": True,
        "socket_connect_timeout": REDIS_SOCKET_TIMEOUT_SECONDS,
        "socket_timeout": REDIS_SOCKET_TIMEOUT_SECONDS,
        "health_check_interval": REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    }

    return Redis.from_url(REDIS_URL, **common_kwargs)


def enqueue_document(
    redis_client: Redis,
    source: Source,
    doc: CollectedDocument,
    job: IngestionJob,
) -> None:
    payload = {
        "job": {
            "id": job.id,
            "source_document_id": job.source_document_id,
            "status": job.status,
            "retry_count": job.retry_count,
            "queued_at": job.queued_at.isoformat(),
        },
        "source": {
            "id": source.id,
            "name": source.name,
        },
        "document": {
            "source_id": doc.source_id,
            "canonical_url": doc.canonical_url,
            "title": doc.title,
            "body_text": doc.body_text,
            "published_at": doc.published_at.isoformat() if doc.published_at else None,
        },
    }

    redis_client.rpush(REDIS_QUEUE_NAME, json.dumps(payload, ensure_ascii=False))


def enqueue_document_with_retry(
    redis_client: Redis,
    source: Source,
    doc: CollectedDocument,
    job: IngestionJob,
) -> Redis:
    try:
        enqueue_document(redis_client, source, doc, job)
        return redis_client
    except RedisError:
        refreshed_client = get_redis_client()
        enqueue_document(refreshed_client, source, doc, job)
        return refreshed_client
