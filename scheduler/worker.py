import time
import asyncio
import threading
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from scheduler.retry import should_retry, calculate_backoff
from scheduler.dlq import DLQ
from scheduler.dependency import handle_dependency, handle_chain, handle_task_failure
import config


class WorkerPool:
    def __init__(self, app, concurrency_model="threading"):
        self.app = app
        self.concurrency_model = concurrency_model
        self.result_backend = app.result_backend
        self.queue_manager = app.queue_manager
        self._running = False
        self._workers = {}
        self._executors = {}
        self._processes = {}
        self._heartbeat_mgr = None
        self._lock = threading.Lock()
        self._stats = {
            q: {"running": 0, "completed": 0, "failed": 0, "last_heartbeat": None}
            for q in config.QUEUE_NAMES
        }

        if concurrency_model == "gevent":
            try:
                import gevent
                self._gevent = gevent
            except ImportError:
                self.concurrency_model = "threading"

    def start(self):
        self._running = True
        from scheduler.heartbeat import HeartbeatManager
        self._heartbeat_mgr = HeartbeatManager(interval=config.HEARTBEAT_INTERVAL)

        for queue_name, concurrency in config.QUEUE_CONCURRENCY_MAP.items():
            for i in range(concurrency):
                worker_id = f"{queue_name}-worker-{i}"
                self._heartbeat_mgr.register(worker_id, queue_name)
                self._stats[queue_name]["last_heartbeat"] = datetime.now(timezone.utc).isoformat()

                if self.concurrency_model == "threading":
                    if queue_name not in self._executors:
                        self._executors[queue_name] = ThreadPoolExecutor(max_workers=concurrency)
                    t = threading.Thread(target=self._worker_thread, args=(queue_name, worker_id), daemon=True)
                    t.start()
                    self._workers[worker_id] = t
                elif self.concurrency_model == "gevent":
                    self._start_gevent_worker(queue_name, worker_id)
                elif self.concurrency_model == "prefork":
                    p = multiprocessing.Process(target=self._prefork_worker, args=(queue_name, worker_id), daemon=True)
                    p.start()
                    self._processes[worker_id] = p
                    self._workers[worker_id] = p
                elif self.concurrency_model == "async":
                    if queue_name not in self._executors:
                        self._executors[queue_name] = asyncio.new_event_loop()
                    t = threading.Thread(target=self._async_worker_loop, args=(queue_name, worker_id), daemon=True)
                    t.start()
                    self._workers[worker_id] = t

    def stop(self):
        self._running = False
        for executor in self._executors.values():
            if isinstance(executor, ThreadPoolExecutor):
                executor.shutdown(wait=False)
            elif isinstance(executor, asyncio.AbstractEventLoop):
                executor.call_soon_threadsafe(executor.stop)
        for p in self._processes.values():
            p.terminate()
            p.join(timeout=5)
        for w in self._workers.values():
            if isinstance(w, threading.Thread):
                w.join(timeout=5)

    def get_worker_status(self):
        status = {}
        for queue_name in config.QUEUE_NAMES:
            status[queue_name] = {
                "running": self._stats[queue_name]["running"],
                "completed": self._stats[queue_name]["completed"],
                "failed": self._stats[queue_name]["failed"],
                "last_heartbeat": self._stats[queue_name]["last_heartbeat"],
            }
        return status

    def get_heartbeat_data(self):
        if self._heartbeat_mgr:
            return self._heartbeat_mgr.get_all()
        return {}

    def _worker_thread(self, queue_name, worker_id):
        while self._running:
            task = self.queue_manager.dequeue(queue_name)
            if task is None:
                time.sleep(0.1)
                continue
            self._heartbeat_mgr.beat(worker_id)
            with self._lock:
                self._stats[queue_name]["running"] += 1
            self._execute_task(task, queue_name, worker_id)
            with self._lock:
                self._stats[queue_name]["running"] = max(0, self._stats[queue_name]["running"] - 1)
            self._stats[queue_name]["last_heartbeat"] = datetime.now(timezone.utc).isoformat()

    def _start_gevent_worker(self, queue_name, worker_id):
        try:
            gevent = self._gevent

            def _gevent_loop(qn, wid):
                while self._running:
                    task = self.queue_manager.dequeue(qn)
                    if task is None:
                        gevent.sleep(0.1)
                        continue
                    self._heartbeat_mgr.beat(wid)
                    with self._lock:
                        self._stats[qn]["running"] += 1
                    self._execute_task(task, qn, wid)
                    with self._lock:
                        self._stats[qn]["running"] = max(0, self._stats[qn]["running"] - 1)
                    self._stats[qn]["last_heartbeat"] = datetime.now(timezone.utc).isoformat()

            t = threading.Thread(target=_gevent_loop, args=(queue_name, worker_id), daemon=True)
            t.start()
            self._workers[worker_id] = t
        except Exception:
            t = threading.Thread(target=self._worker_thread, args=(queue_name, worker_id), daemon=True)
            t.start()
            self._workers[worker_id] = t

    def _prefork_worker(self, queue_name, worker_id):
        while self._running:
            task = self.queue_manager.dequeue(queue_name)
            if task is None:
                time.sleep(0.1)
                continue
            self._execute_task(task, queue_name, worker_id)

    def _async_worker_loop(self, queue_name, worker_id):
        loop = self._executors[queue_name]
        asyncio.set_event_loop(loop)
        while self._running:
            task = self.queue_manager.dequeue(queue_name)
            if task is None:
                time.sleep(0.1)
                continue
            self._heartbeat_mgr.beat(worker_id)
            with self._lock:
                self._stats[queue_name]["running"] += 1
            try:
                loop.run_until_complete(self._async_execute_task(task, queue_name, worker_id))
            except Exception:
                pass
            with self._lock:
                self._stats[queue_name]["running"] = max(0, self._stats[queue_name]["running"] - 1)
            self._stats[queue_name]["last_heartbeat"] = datetime.now(timezone.utc).isoformat()

    async def _async_execute_task(self, task_payload, queue_name, worker_id):
        task_id = task_payload.get("id")
        task_name = task_payload.get("task_name")
        args = task_payload.get("args", [])
        kwargs = task_payload.get("kwargs", {})
        timeout = task_payload.get("timeout", config.DEFAULT_TASK_TIMEOUT)
        retry_count = task_payload.get("retry_count", 0)

        self.result_backend.update_status(task_id, "running")
        start_time = time.time()
        try:
            task_info = self.app.registry.get(task_name)
            func = task_info["func"]
            result = await asyncio.wait_for(
                self._async_call(func, args, kwargs),
                timeout=timeout,
            )
            end_time = time.time()
            self._on_success(task_payload, queue_name, worker_id, result, start_time, end_time)
        except asyncio.TimeoutError:
            end_time = time.time()
            self.result_backend.store_result(task_id, None, {
                "start_time": start_time, "end_time": end_time,
                "duration": end_time - start_time, "error": "Task timed out",
            })
            self.result_backend.update_status(task_id, "timeout")
            self._handle_failure(task_payload, queue_name, worker_id, Exception("Task timed out"), retry_count, start_time)
        except Exception as exc:
            end_time = time.time()
            self.result_backend.store_result(task_id, None, {
                "start_time": start_time, "end_time": end_time,
                "duration": end_time - start_time, "error": str(exc),
            })
            self._handle_failure(task_payload, queue_name, worker_id, exc, retry_count, start_time)

    async def _async_call(self, func, args, kwargs):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    def _execute_task(self, task_payload, queue_name, worker_id):
        task_id = task_payload.get("id")
        task_name = task_payload.get("task_name")
        args = task_payload.get("args", [])
        kwargs = task_payload.get("kwargs", {})
        timeout = task_payload.get("timeout", config.DEFAULT_TASK_TIMEOUT)
        retry_count = task_payload.get("retry_count", 0)

        self.result_backend.update_status(task_id, "running")
        start_time = time.time()

        if self.concurrency_model == "threading" and queue_name in self._executors:
            future = self._executors[queue_name].submit(self._run_func, task_name, args, kwargs)
            try:
                result = future.result(timeout=timeout)
                end_time = time.time()
                self._on_success(task_payload, queue_name, worker_id, result, start_time, end_time)
            except (TimeoutError, concurrent.futures.TimeoutError) as exc:
                end_time = time.time()
                self._handle_timeout(task_payload, queue_name, worker_id, exc, start_time, end_time)
            except Exception as exc:
                end_time = time.time()
                self._handle_failure(task_payload, queue_name, worker_id, exc, retry_count, start_time)
        else:
            try:
                result = self._run_func_with_timeout(task_name, args, kwargs, timeout)
                end_time = time.time()
                self._on_success(task_payload, queue_name, worker_id, result, start_time, end_time)
            except TimeoutError as exc:
                end_time = time.time()
                self._handle_timeout(task_payload, queue_name, worker_id, exc, start_time, end_time)
            except Exception as exc:
                end_time = time.time()
                self._handle_failure(task_payload, queue_name, worker_id, exc, retry_count, start_time)

    def _run_func(self, task_name, args, kwargs):
        task_info = self.app.registry.get(task_name)
        func = task_info["func"]
        return func(*args, **kwargs)

    def _run_func_with_timeout(self, task_name, args, kwargs, timeout):
        task_info = self.app.registry.get(task_name)
        func = task_info["func"]

        result_container = [None]
        error_container = [None]

        def _target():
            try:
                result_container[0] = func(*args, **kwargs)
            except Exception as e:
                error_container[0] = e

        t = threading.Thread(target=_target)
        t.daemon = True
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            raise TimeoutError(f"Task {task_name} timed out after {timeout}s")
        if error_container[0] is not None:
            raise error_container[0]
        return result_container[0]

    def _on_success(self, task_payload, queue_name, worker_id, result, start_time, end_time):
        task_id = task_payload.get("id")
        self.result_backend.store_result(task_id, result, {
            "start_time": start_time, "end_time": end_time,
            "duration": end_time - start_time,
        })
        self.result_backend.update_status(task_id, "success")
        with self._lock:
            self._stats[queue_name]["completed"] += 1
        if self._heartbeat_mgr:
            self._heartbeat_mgr.update_stats(worker_id, completed=1)
        handle_dependency(task_payload, self.app)
        handle_chain(task_payload, self.app)

    def _handle_timeout(self, task_payload, queue_name, worker_id, exc, start_time, end_time):
        task_id = task_payload.get("id")
        self.result_backend.store_result(task_id, None, {
            "start_time": start_time, "end_time": end_time,
            "duration": end_time - start_time, "error": str(exc),
        })
        self.result_backend.update_status(task_id, "timeout")
        dlq = DLQ(self.result_backend)
        dlq.push(task_payload)
        with self._lock:
            self._stats[queue_name]["failed"] += 1
        if self._heartbeat_mgr:
            self._heartbeat_mgr.update_stats(worker_id, failed=1)
        handle_task_failure(task_payload, self.app, exc)

    def _handle_failure(self, task_payload, queue_name, worker_id, exc, retry_count, start_time):
        task_id = task_payload.get("id")
        task_name = task_payload.get("task_name")
        end_time = time.time()

        self.result_backend.store_result(task_id, None, {
            "start_time": start_time, "end_time": end_time,
            "duration": end_time - start_time, "error": str(exc),
        })

        if should_retry(task_payload, exc):
            backoff = calculate_backoff(retry_count)
            task_payload["retry_count"] = retry_count + 1
            self.result_backend.update_status(task_id, "retrying")
            time.sleep(backoff)
            self.queue_manager.enqueue(task_payload)
        else:
            self.result_backend.update_status(task_id, "failed")
            dlq = DLQ(self.result_backend)
            dlq.push(task_payload)
            with self._lock:
                self._stats[queue_name]["failed"] += 1
            if self._heartbeat_mgr:
                self._heartbeat_mgr.update_stats(worker_id, failed=1)
            handle_task_failure(task_payload, self.app, exc)
