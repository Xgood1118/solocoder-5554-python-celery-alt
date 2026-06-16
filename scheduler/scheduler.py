import threading
import time
from datetime import datetime, timezone

from croniter import croniter


class TaskScheduler:

    def __init__(self, app):
        self.app = app
        self._scheduled_tasks = []
        self._stop_event = threading.Event()
        self._thread = None

    def schedule(self, task_proxy, cron=None, interval=None, eta=None, args=None, kwargs=None, queue=None):
        if not cron and interval is None and not eta:
            raise ValueError("One of cron, interval, or eta must be provided")

        sched = {"cron": cron, "interval": interval, "eta": eta}
        next_run = self.calculate_next_run(sched)

        entry = {
            "task_name": task_proxy.name,
            "cron": cron,
            "interval": interval,
            "eta": eta,
            "args": args or (),
            "kwargs": kwargs or {},
            "queue": queue,
            "next_run": next_run,
        }
        self._scheduled_tasks.append(entry)
        return entry

    @staticmethod
    def calculate_next_run(sched):
        now = datetime.now(timezone.utc)

        if sched.get("cron"):
            cron = sched["cron"]
            itr = croniter(cron, now)
            next_dt = itr.get_next(datetime)
            if next_dt.tzinfo is None:
                next_dt = next_dt.replace(tzinfo=timezone.utc)
            return next_dt.isoformat()

        if sched.get("interval") is not None:
            next_dt = now.timestamp() + sched["interval"]
            return datetime.fromtimestamp(next_dt, tz=timezone.utc).isoformat()

        if sched.get("eta"):
            eta_str = sched["eta"]
            parsed = datetime.fromisoformat(eta_str)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.isoformat()

        return None

    def get_scheduled_tasks(self):
        return list(self._scheduled_tasks)

    def tick(self):
        now = datetime.now(timezone.utc)

        for entry in self._scheduled_tasks:
            next_run_str = entry["next_run"]
            if not next_run_str:
                continue

            next_run_dt = datetime.fromisoformat(next_run_str)
            if next_run_dt.tzinfo is None:
                next_run_dt = next_run_dt.replace(tzinfo=timezone.utc)

            if next_run_dt <= now:
                task_proxy = self.app.tasks.get(entry["task_name"])
                if task_proxy:
                    task_proxy.apply_async(
                        args=entry["args"],
                        kwargs=entry["kwargs"],
                        queue=entry["queue"],
                    )

                sched = {"cron": entry["cron"], "interval": entry["interval"], "eta": entry["eta"]}
                if entry["cron"] or entry.get("interval") is not None:
                    entry["next_run"] = self.calculate_next_run(sched)
                else:
                    entry["next_run"] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()

        def _run():
            while not self._stop_event.is_set():
                self.tick()
                self._stop_event.wait(1.0)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
