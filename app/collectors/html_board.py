from __future__ import annotations

from app.models import CollectedDocument, Source


def collect(source: Source) -> list[CollectedDocument]:
    raise NotImplementedError("html_board collector is not implemented yet")
