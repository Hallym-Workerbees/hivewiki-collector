from __future__ import annotations

import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "collector")
DB_USER = os.getenv("DB_USER", "collector")
DB_PASSWORD = os.getenv("DB_PASSWORD", "collector")

DATABASE_DSN = os.getenv(
    "DATABASE_DSN",
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

WIKIFIER_URL = os.getenv("WIKIFIER_URL", "http://wikifier:8000/ingest")
LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "5"))
DUE_SOURCE_BATCH_SIZE = int(os.getenv("DUE_SOURCE_BATCH_SIZE", "10"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
USER_AGENT = os.getenv("USER_AGENT", "HiveWiki-Collector/0.1")
