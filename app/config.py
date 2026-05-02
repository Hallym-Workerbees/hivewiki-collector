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
DB_RECONNECT_SLEEP_SECONDS = int(os.getenv("DB_RECONNECT_SLEEP_SECONDS", "5"))

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "wikify_queue")
REDIS_SOCKET_TIMEOUT_SECONDS = int(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "5"))
REDIS_HEALTH_CHECK_INTERVAL_SECONDS = int(
    os.getenv("REDIS_HEALTH_CHECK_INTERVAL_SECONDS", "30")
)

LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "5"))
SOURCE_BATCH_SIZE = int(os.getenv("SOURCE_BATCH_SIZE", "50"))
INITIAL_BACKFILL_LIMIT = int(os.getenv("INITIAL_BACKFILL_LIMIT", "100"))
INCREMENTAL_FETCH_LIMIT = int(os.getenv("INCREMENTAL_FETCH_LIMIT", "10"))
QUEUE_BATCH_SIZE = int(os.getenv("QUEUE_BATCH_SIZE", "100"))
JOB_PUBLISH_CLAIM_TIMEOUT_SECONDS = int(
    os.getenv("JOB_PUBLISH_CLAIM_TIMEOUT_SECONDS", "300")
)
MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
USER_AGENT = os.getenv("USER_AGENT", "HiveWiki-Collector/0.1")
