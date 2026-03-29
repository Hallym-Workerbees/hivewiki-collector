from __future__ import annotations

import requests

from app.config import HTTP_TIMEOUT_SECONDS, USER_AGENT, WIKIFIER_URL
from app.models import CollectedDocument, Source


def publish(source: Source, doc: CollectedDocument) -> None:
    payload = {
        "source": {
            "id": source.id,
            "name": source.name,
            "type": source.source_type,
            "category_path": source.category_path,
            "ocr_enabled": source.ocr_enabled,
        },
        "document": {
            "external_id": doc.external_id,
            "canonical_url": doc.canonical_url,
            "title": doc.title,
            "body_text": doc.body_text,
            "published_at": doc.published_at.isoformat() if doc.published_at else None,
            "content_hash": doc.content_hash,
            "image_urls": doc.image_urls,
        },
    }

    resp = requests.post(
        WIKIFIER_URL,
        json=payload,
        timeout=HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()
