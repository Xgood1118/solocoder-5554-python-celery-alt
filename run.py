import os
import sys
import time
import signal
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler.app import TaskApp
from scheduler.worker import WorkerPool
from scheduler.dependency import Chain, Group
from web.app import create_web_app
import config


app = TaskApp()


@app.task(name="add", queue="high", default_args={}, timeout=60)
def add(x, y):
    return x + y


@app.task(name="multiply", queue="medium", timeout=120)
def multiply(x, y):
    return x * y


@app.task(name="slow_task", queue="low", timeout=30)
def slow_task(seconds=5):
    time.sleep(seconds)
    return f"Slept {seconds}s"


@app.task(name="failing_task", queue="medium", retry_config={"max_retries": 3})
def failing_task():
    raise ValueError("This task always fails")


@app.task(name="batch_process", queue="batch", timeout=600)
def batch_process(items=None):
    if items is None:
        items = []
    return {"processed": len(items), "items": items}


@app.task(name="downstream", queue="medium", depends_on="add")
def downstream(result=None):
    return f"Downstream received: {result}"


def run_server(host="0.0.0.0", port=None, with_worker=True):
    if port is None:
        port = config.WEB_PORT

    worker_pool = None
    if with_worker:
        worker_pool = WorkerPool(app, concurrency_model=config.CONCURRENCY_MODEL)
        worker_pool.start()
        print(f"[Worker] Started with model={config.CONCURRENCY_MODEL}")

    scheduler = app.get_scheduler()
    scheduler.schedule(
        app.tasks.get("add"),
        cron="*/5 * * * *",
        args=(1, 2),
        queue="high",
    )
    scheduler.start()
    print("[Scheduler] Started")

    flask_app = create_web_app(app, worker_pool)
    print(f"[Web] Starting on http://{host}:{port}")
    print(f"[Config] RESULT_BACKEND={config.RESULT_BACKEND}, STORAGE_PATH={config.STORAGE_PATH}")
    print(f"[Config] Auth token (masked): {config.AUTH_TOKEN[:8]}****")

    def shutdown(signum, frame):
        print("\n[Shutdown] Stopping...")
        if with_worker and worker_pool:
            worker_pool.stop()
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    flask_app.run(host=host, port=port, debug=False, threaded=True)


def run_worker_only():
    worker_pool = WorkerPool(app, concurrency_model=config.CONCURRENCY_MODEL)
    worker_pool.start()
    print(f"[Worker] Started with model={config.CONCURRENCY_MODEL}")

    scheduler = app.get_scheduler()
    scheduler.schedule(
        app.tasks.get("add"),
        cron="*/5 * * * *",
        args=(1, 2),
        queue="high",
    )
    scheduler.start()
    print("[Scheduler] Started")

    def shutdown(signum, frame):
        print("\n[Shutdown] Stopping...")
        worker_pool.stop()
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("[Worker] Running. Press Ctrl+C to stop.")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lightweight Task Scheduler")
    parser.add_argument("--mode", choices=["all", "web", "worker"], default="all",
                        help="Run mode: all (web+worker), web only, or worker only")
    parser.add_argument("--host", default="0.0.0.0", help="Web server host")
    parser.add_argument("--port", type=int, default=None, help="Web server port")
    args = parser.parse_args()

    if args.mode == "all":
        run_server(host=args.host, port=args.port, with_worker=True)
    elif args.mode == "web":
        run_server(host=args.host, port=args.port, with_worker=False)
    elif args.mode == "worker":
        run_worker_only()
