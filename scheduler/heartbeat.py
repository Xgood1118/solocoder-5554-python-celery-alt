import time
from datetime import datetime, timezone


class HeartbeatManager:
    def __init__(self, interval=10):
        self.interval = interval
        self._heartbeats = {}

    def register(self, worker_id, queue):
        self._heartbeats[worker_id] = {
            "queue": queue,
            "last_beat": datetime.now(timezone.utc).isoformat(),
            "status": "idle",
            "tasks_completed": 0,
            "tasks_failed": 0,
        }

    def beat(self, worker_id):
        if worker_id in self._heartbeats:
            self._heartbeats[worker_id]["last_beat"] = datetime.now(timezone.utc).isoformat()
            self._heartbeats[worker_id]["status"] = "alive"

    def update_stats(self, worker_id, completed=None, failed=None):
        if worker_id in self._heartbeats:
            if completed is not None:
                self._heartbeats[worker_id]["tasks_completed"] += completed
            if failed is not None:
                self._heartbeats[worker_id]["tasks_failed"] += failed

    def get_all(self):
        return dict(self._heartbeats)

    def get_alive(self, interval=None):
        if interval is None:
            interval = self.interval
        now = time.time()
        alive = {}
        for worker_id, data in self._heartbeats.items():
            last_beat = datetime.fromisoformat(data["last_beat"])
            elapsed = now - last_beat.timestamp()
            if elapsed <= interval:
                alive[worker_id] = data
        return alive

    def check_dead(self, interval=None):
        if interval is None:
            interval = self.interval * 3
        now = time.time()
        dead = []
        for worker_id, data in self._heartbeats.items():
            last_beat = datetime.fromisoformat(data["last_beat"])
            elapsed = now - last_beat.timestamp()
            if elapsed > interval:
                dead.append(worker_id)
        return dead

    def unregister(self, worker_id):
        self._heartbeats.pop(worker_id, None)
