from __future__ import annotations

import requests
from bs4 import BeautifulSoup

from app.config import HTTP_TIMEOUT_SECONDS, USER_AGENT
from app.models import CollectedDocument, Source
from app.utils import extract_image_urls, html_to_text, sha256_text


def collect(source: Source) -> list[CollectedDocument]:
    resp = requests.get(
        source.target_url,
        timeout=HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else source.name

    body_text = html_to_text(resp.text)
    image_urls = extract_image_urls(resp.text, base_url=source.target_url)

    return [
        CollectedDocument(
            source_id=source.id,
            external_id=source.target_url,
            canonical_url=source.target_url,
            title=title,
            body_text=body_text,
            published_at=None,
            content_hash=sha256_text(body_text) if body_text else None,
            image_urls=image_urls,
        )
    ]
