import threading


class TaskRegistry:
    def __init__(self):
        self._tasks = {}
        self._lock = threading.Lock()

    def register(self, name, func, queue="medium", default_args=None,
                 retry_config=None, timeout=300, depends_on=None, cleanup=None):
        if default_args is None:
            default_args = {}
        if retry_config is None:
            retry_config = {"max_retries": 3, "backoff_base": 2, "backoff_max": 60}
        task_info = {
            "name": name,
            "func": func,
            "queue": queue,
            "default_args": default_args,
            "retry_config": retry_config,
            "timeout": timeout,
            "depends_on": depends_on,
            "cleanup": cleanup,
        }
        with self._lock:
            self._tasks[name] = task_info

    def get(self, name):
        with self._lock:
            if name not in self._tasks:
                raise KeyError(f"Task '{name}' not found")
            return self._tasks[name]

    def all(self):
        with self._lock:
            return dict(self._tasks)

    def __contains__(self, name):
        with self._lock:
            return name in self._tasks
