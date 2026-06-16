import config


def should_retry(task_payload, exc):
    retry_count = task_payload.get("retry_count", 0)
    max_retries = task_payload.get("max_retries", config.DEFAULT_MAX_RETRIES)
    return retry_count < max_retries


def calculate_backoff(retry_count, base=None, max_backoff=None):
    if base is None:
        base = config.RETRY_BACKOFF_BASE
    if max_backoff is None:
        max_backoff = config.RETRY_BACKOFF_MAX
    delay = min(base ** retry_count, max_backoff)
    return delay
