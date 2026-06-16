import os
import multiprocessing


WEB_PORT = int(os.environ.get("WEB_PORT", 5000))
WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", max(1, multiprocessing.cpu_count())))
STORAGE_PATH = os.environ.get("STORAGE_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
RESULT_BACKEND = os.environ.get("RESULT_BACKEND", "fs")

AUTH_USERNAME = os.environ.get("AUTH_USERNAME", "admin")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "admin123")
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "sk-task-scheduler-2024")

CONCURRENCY_MODEL = os.environ.get("CONCURRENCY_MODEL", "threading")

QUEUE_HIGH_CONCURRENCY = int(os.environ.get("QUEUE_HIGH_CONCURRENCY", WORKER_CONCURRENCY))
QUEUE_MEDIUM_CONCURRENCY = int(os.environ.get("QUEUE_MEDIUM_CONCURRENCY", max(1, WORKER_CONCURRENCY - 1)))
QUEUE_LOW_CONCURRENCY = int(os.environ.get("QUEUE_LOW_CONCURRENCY", max(1, WORKER_CONCURRENCY // 2)))
QUEUE_BATCH_CONCURRENCY = int(os.environ.get("QUEUE_BATCH_CONCURRENCY", max(1, WORKER_CONCURRENCY // 4)))

DEFAULT_TASK_TIMEOUT = int(os.environ.get("DEFAULT_TASK_TIMEOUT", 300))
DEFAULT_MAX_RETRIES = int(os.environ.get("DEFAULT_MAX_RETRIES", 3))
RETRY_BACKOFF_BASE = int(os.environ.get("RETRY_BACKOFF_BASE", 2))
RETRY_BACKOFF_MAX = int(os.environ.get("RETRY_BACKOFF_MAX", 60))

SSE_INTERVAL = int(os.environ.get("SSE_INTERVAL", 3))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 10))

QUEUE_NAMES = ["high", "medium", "low", "batch"]

QUEUE_CONCURRENCY_MAP = {
    "high": QUEUE_HIGH_CONCURRENCY,
    "medium": QUEUE_MEDIUM_CONCURRENCY,
    "low": QUEUE_LOW_CONCURRENCY,
    "batch": QUEUE_BATCH_CONCURRENCY,
}
