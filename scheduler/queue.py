import heapq
import threading


class QueueManager:
    QUEUE_NAMES = ["high", "medium", "low", "batch"]

    def __init__(self):
        self._queues = {name: [] for name in self.QUEUE_NAMES}
        self._lock = threading.Lock()
        self._counter = 0

    def enqueue(self, task_payload):
        queue_name = task_payload.get("queue", "medium")
        if queue_name not in self._queues:
            queue_name = "medium"
        priority = task_payload.get("priority", 5)
        with self._lock:
            self._counter += 1
            entry = (priority, self._counter, task_payload)
            heapq.heappush(self._queues[queue_name], entry)

    def dequeue(self, queue_name):
        with self._lock:
            if queue_name not in self._queues or not self._queues[queue_name]:
                return None
            _, _, task_payload = heapq.heappop(self._queues[queue_name])
            return task_payload

    def queue_depth(self, queue_name):
        with self._lock:
            return len(self._queues.get(queue_name, []))

    def all_depths(self):
        with self._lock:
            return {name: len(q) for name, q in self._queues.items()}

    def peek(self, queue_name, limit=20):
        with self._lock:
            queue = self._queues.get(queue_name, [])
            return [entry[2] for entry in queue[:limit]]

    def remove(self, task_id):
        with self._lock:
            for name in self.QUEUE_NAMES:
                for i, (pri, cnt, payload) in enumerate(self._queues[name]):
                    if payload.get("id") == task_id:
                        self._queues[name].pop(i)
                        heapq.heapify(self._queues[name])
                        return True
            return False

    def get_task(self, task_id):
        with self._lock:
            for name in self.QUEUE_NAMES:
                for _, _, payload in self._queues[name]:
                    if payload.get("id") == task_id:
                        return payload
            return None
