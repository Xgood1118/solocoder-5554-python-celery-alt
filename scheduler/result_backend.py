import json
import os
import threading
from datetime import datetime, timezone


class ResultBackend:
    def get(self, task_id):
        raise NotImplementedError

    def set(self, task_id, data):
        raise NotImplementedError

    def update_status(self, task_id, status):
        raise NotImplementedError

    def store_result(self, task_id, result, meta=None):
        raise NotImplementedError

    def delete(self, task_id):
        raise NotImplementedError

    def query(self, status=None, queue=None, limit=100, offset=0, time_from=None, time_to=None):
        raise NotImplementedError

    def get_dlq(self):
        raise NotImplementedError


class FileSystemBackend(ResultBackend):
    def __init__(self, storage_path):
        self._base = os.path.join(storage_path, "results")
        self._dlq_path = os.path.join(storage_path, "dlq")
        self._lock = threading.Lock()
        os.makedirs(self._base, exist_ok=True)
        os.makedirs(self._dlq_path, exist_ok=True)

    def _task_path(self, task_id):
        return os.path.join(self._base, f"{task_id}.json")

    def _dlq_task_path(self, task_id):
        return os.path.join(self._dlq_path, f"{task_id}.json")

    def get(self, task_id):
        path = self._task_path(task_id)
        if not os.path.exists(path):
            return None
        with self._lock:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    def set(self, task_id, data):
        path = self._task_path(task_id)
        with self._lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, default=str)

    def update_status(self, task_id, status):
        data = self.get(task_id)
        if data is None:
            data = {"id": task_id, "status": status}
        data["status"] = status
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.set(task_id, data)

    def store_result(self, task_id, result, meta=None):
        data = self.get(task_id) or {"id": task_id}
        data["result"] = result
        data["result_meta"] = meta or {}
        data["result_stored_at"] = datetime.now(timezone.utc).isoformat()
        self.set(task_id, data)

    def delete(self, task_id):
        path = self._task_path(task_id)
        with self._lock:
            if os.path.exists(path):
                os.remove(path)

    def query(self, status=None, queue=None, limit=100, offset=0, time_from=None, time_to=None):
        results = []
        if not os.path.exists(self._base):
            return results
        with self._lock:
            for fname in os.listdir(self._base):
                if not fname.endswith(".json"):
                    continue
                fpath = os.path.join(self._base, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                if status and data.get("status") != status:
                    continue
                if queue and data.get("queue") != queue:
                    continue
                created = data.get("created_at")
                if time_from and created and created < time_from:
                    continue
                if time_to and created and created > time_to:
                    continue
                results.append(data)
        results.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return results[offset:offset + limit]

    def get_dlq(self):
        results = []
        if not os.path.exists(self._dlq_path):
            return results
        with self._lock:
            for fname in os.listdir(self._dlq_path):
                if not fname.endswith(".json"):
                    continue
                fpath = os.path.join(self._dlq_path, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                results.append(data)
        return results

    def push_dlq(self, task_data):
        task_id = task_data.get("id", "unknown")
        task_data["dlq_entered_at"] = datetime.now(timezone.utc).isoformat()
        path = self._dlq_task_path(task_id)
        with self._lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(task_data, f, ensure_ascii=False, default=str)


class MemoryBackend(ResultBackend):
    def __init__(self):
        self._store = {}
        self._dlq = []
        self._lock = threading.Lock()

    def get(self, task_id):
        with self._lock:
            return self._store.get(task_id)

    def set(self, task_id, data):
        with self._lock:
            self._store[task_id] = data

    def update_status(self, task_id, status):
        with self._lock:
            if task_id not in self._store:
                self._store[task_id] = {"id": task_id, "status": status}
            self._store[task_id]["status"] = status
            self._store[task_id]["updated_at"] = datetime.now(timezone.utc).isoformat()

    def store_result(self, task_id, result, meta=None):
        with self._lock:
            if task_id not in self._store:
                self._store[task_id] = {"id": task_id}
            self._store[task_id]["result"] = result
            self._store[task_id]["result_meta"] = meta or {}
            self._store[task_id]["result_stored_at"] = datetime.now(timezone.utc).isoformat()

    def delete(self, task_id):
        with self._lock:
            self._store.pop(task_id, None)

    def query(self, status=None, queue=None, limit=100, offset=0, time_from=None, time_to=None):
        with self._lock:
            results = []
            for data in self._store.values():
                if status and data.get("status") != status:
                    continue
                if queue and data.get("queue") != queue:
                    continue
                created = data.get("created_at")
                if time_from and created and created < time_from:
                    continue
                if time_to and created and created > time_to:
                    continue
                results.append(data)
        results.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return results[offset:offset + limit]

    def get_dlq(self):
        with self._lock:
            return list(self._dlq)

    def push_dlq(self, task_data):
        task_data["dlq_entered_at"] = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._dlq.append(task_data)


def create_backend(backend_type="fs", storage_path="./data"):
    if backend_type == "mem":
        return MemoryBackend()
    return FileSystemBackend(storage_path)
