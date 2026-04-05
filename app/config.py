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

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "wikify_queue")

LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "5"))
POLL_INTERVAL_MINUTES = int(os.getenv("POLL_INTERVAL_MINUTES", "10"))
SOURCE_BATCH_SIZE = int(os.getenv("SOURCE_BATCH_SIZE", "50"))
RSS_FETCH_LIMIT = int(os.getenv("RSS_FETCH_LIMIT", "10"))
MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
USER_AGENT = os.getenv("USER_AGENT", "HiveWiki-Collector/0.1")
