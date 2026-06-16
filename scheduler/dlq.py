import threading
from datetime import datetime, timezone


class DLQ:
    def __init__(self, result_backend):
        self.result_backend = result_backend
        self._lock = threading.Lock()

    def push(self, task_payload):
        task_payload["status"] = "dead"
        task_payload["dlq_entered_at"] = datetime.now(timezone.utc).isoformat()
        self.result_backend.push_dlq(task_payload)

    def get_all(self):
        return self.result_backend.get_dlq()

    def size(self):
        return len(self.result_backend.get_dlq())

    def requeue(self, task_id, queue_manager):
        dlq_items = self.result_backend.get_dlq()
        for item in dlq_items:
            if item.get("id") == task_id:
                item["retry_count"] = 0
                item["status"] = "pending"
                item.pop("dlq_entered_at", None)
                queue_manager.enqueue(item)
                return True
        return False
