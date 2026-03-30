from __future__ import annotations

import hashlib
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def extract_image_urls(html: str, base_url: str | None = None) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[str] = []

    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue

        src = src.strip()
        if base_url:
            src = urljoin(base_url, src)

        results.append(src)

    # 중복 제거
    return list(dict.fromkeys(results))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        # RSS published 같은 RFC822 형태 처리
        return parsedate_to_datetime(value)
    except Exception:
        pass

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
