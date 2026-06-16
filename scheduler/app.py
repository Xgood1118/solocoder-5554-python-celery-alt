import uuid
from datetime import datetime, timezone, timedelta

from scheduler.registry import TaskRegistry
from scheduler.queue import QueueManager
from scheduler.result_backend import create_backend
import config


class TaskProxy:
    def __init__(self, app, task_name, func, task_info):
        self._app = app
        self.name = task_name
        self._func = func
        self._task_info = task_info
        self.queue = task_info.get("queue", "medium")
        self.depends_on = task_info.get("depends_on")
        self.cleanup = task_info.get("cleanup")

    def delay(self, *args, **kwargs):
        return self.apply_async(args=args, kwargs=kwargs)

    def apply_async(self, args=None, kwargs=None, queue=None, countdown=None,
                    eta=None, priority=5):
        effective_queue = queue or self.queue
        effective_args = args or ()
        effective_kwargs = kwargs or {}
        merged_kwargs = {**self._task_info.get("default_args", {}), **effective_kwargs}
        retry_config = self._task_info.get("retry_config", {})
        max_retries = retry_config.get("max_retries", config.DEFAULT_MAX_RETRIES)
        timeout = self._task_info.get("timeout", config.DEFAULT_TASK_TIMEOUT)

        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        if countdown is not None:
            eta = (now + timedelta(seconds=countdown)).isoformat()

        payload = {
            "id": task_id,
            "task_name": self.name,
            "args": list(effective_args),
            "kwargs": merged_kwargs,
            "queue": effective_queue,
            "priority": priority,
            "countdown": countdown,
            "eta": eta,
            "retry_count": 0,
            "max_retries": max_retries,
            "timeout": timeout,
            "created_at": now.isoformat(),
            "depends_on": self.depends_on,
            "cleanup": self.cleanup,
            "parent_task_id": None,
            "status": "pending",
        }

        self._app.result_backend.set(task_id, {
            "id": task_id,
            "task_name": self.name,
            "queue": effective_queue,
            "status": "pending",
            "args": list(effective_args),
            "kwargs": merged_kwargs,
            "priority": priority,
            "retry_count": 0,
            "max_retries": max_retries,
            "timeout": timeout,
            "created_at": now.isoformat(),
            "eta": eta,
            "depends_on": self.depends_on,
        })

        self._app.queue_manager.enqueue(payload)
        return task_id

    def apply(self, args=None, kwargs=None):
        effective_args = args or ()
        effective_kwargs = kwargs or {}
        merged_kwargs = {**self._task_info.get("default_args", {}), **effective_kwargs}
        return self._func(*effective_args, **merged_kwargs)


class TaskApp:
    def __init__(self):
        self.registry = TaskRegistry()
        self.queue_manager = QueueManager()
        self.result_backend = create_backend(config.RESULT_BACKEND, config.STORAGE_PATH)
        self.tasks = {}
        self._scheduler = None

    def task(self, name=None, queue="medium", default_args=None,
             retry_config=None, timeout=300, depends_on=None, cleanup=None):
        def decorator(func):
            task_name = name or func.__name__
            effective_retry = retry_config
            if effective_retry is None:
                effective_retry = {"max_retries": config.DEFAULT_MAX_RETRIES,
                                   "backoff_base": config.RETRY_BACKOFF_BASE,
                                   "backoff_max": config.RETRY_BACKOFF_MAX}
            self.registry.register(
                task_name, func, queue=queue, default_args=default_args,
                retry_config=effective_retry, timeout=timeout,
                depends_on=depends_on, cleanup=cleanup,
            )
            task_info = self.registry.get(task_name)
            proxy = TaskProxy(self, task_name, func, task_info)
            self.tasks[task_name] = proxy
            return proxy
        return decorator

    def get_scheduler(self):
        if self._scheduler is None:
            from scheduler.scheduler import TaskScheduler
            self._scheduler = TaskScheduler(self)
        return self._scheduler
