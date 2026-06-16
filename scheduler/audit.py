import json
import os
import threading
from datetime import datetime, timezone


class AuditLogger:
    def __init__(self, storage_path):
        self._log_path = os.path.join(storage_path, "audit")
        self._lock = threading.Lock()
        os.makedirs(self._log_path, exist_ok=True)
        self._current_file = None
        self._current_date = None

    def _get_log_file(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._current_date != today:
            self._current_date = today
            self._current_file = os.path.join(self._log_path, f"audit-{today}.jsonl")
        return self._current_file

    def log(self, actor, action, target, ip, result="success", detail=None):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "actor": actor,
            "action": action,
            "target": target,
            "ip": ip,
            "result": result,
        }
        if detail:
            entry["detail"] = detail
        with self._lock:
            log_file = self._get_log_file()
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        return entry

    def query(self, limit=100, offset=0, actor=None, action=None, date=None):
        log_file = os.path.join(
            self._log_path,
            f"audit-{date or datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl"
        )
        if not os.path.exists(log_file):
            return []
        entries = []
        with self._lock:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if actor and entry.get("actor") != actor:
                        continue
                    if action and entry.get("action") != action:
                        continue
                    entries.append(entry)
        entries.reverse()
        return entries[offset:offset + limit]

    def get_recent(self, limit=50):
        return self.query(limit=limit)
