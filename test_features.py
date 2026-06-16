import sys
import os
import time
import json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler.app import TaskApp
from scheduler.dependency import Chain, Group
from scheduler.worker import WorkerPool
import config


def test_task_registration():
    print("=== Test 1: Task Registration ===")
    app = TaskApp()

    @app.task(name="test_add", queue="high", default_args={"y": 10}, timeout=60)
    def add(x, y=0):
        return x + y

    @app.task(name="test_multiply", queue="medium", retry_config={"max_retries": 2})
    def multiply(x, y):
        return x * y

    print(f"Registered tasks: {list(app.tasks.keys())}")
    assert "test_add" in app.tasks
    assert "test_multiply" in app.tasks
    print("✓ Task registration passed")
    return app


def test_task_trigger():
    print("\n=== Test 2: Task Trigger Methods ===")
    app = TaskApp()

    @app.task(name="trigger_test", queue="high")
    def test_func(x, y):
        return x + y

    result = test_func.apply(args=(2, 3))
    print(f"apply() result: {result}")
    assert result == 5

    task_id = test_func.delay(5, 5)
    print(f"delay() task_id: {task_id}")
    assert task_id is not None

    task_id2 = test_func.apply_async(args=(10, 20), queue="medium", countdown=1)
    print(f"apply_async() task_id: {task_id2}")
    assert task_id2 is not None

    print("✓ Task trigger methods passed")
    return app


def test_priority_queue():
    print("\n=== Test 3: Priority Queue ===")
    app = TaskApp()

    @app.task(name="pri_test", queue="high")
    def pri_func(x):
        return x

    task_id1 = pri_func.apply_async(args=(1,), priority=5)
    task_id2 = pri_func.apply_async(args=(2,), priority=1)
    task_id3 = pri_func.apply_async(args=(3,), priority=3)

    depths = app.queue_manager.all_depths()
    print(f"Queue depths: {depths}")
    assert depths["high"] == 3

    peek = app.queue_manager.peek("high", limit=10)
    priorities = [t["priority"] for t in peek]
    print(f"Peek priorities: {priorities}")

    task1 = app.queue_manager.dequeue("high")
    print(f"First dequeued priority: {task1['priority']}, args: {task1['args']}")
    assert task1["priority"] == 1
    assert task1["args"] == [2]

    task2 = app.queue_manager.dequeue("high")
    print(f"Second dequeued priority: {task2['priority']}, args: {task2['args']}")
    assert task2["priority"] == 3
    assert task2["args"] == [3]

    print("✓ Priority queue passed")
    return app


def test_chain():
    print("\n=== Test 4: Chain ===")
    app = TaskApp()

    @app.task(name="chain_add", queue="high")
    def chain_add(x, y):
        return x + y

    @app.task(name="chain_multiply", queue="medium")
    def chain_multiply(result):
        return result * 2

    chain = Chain(chain_add, chain_multiply)
    result = chain.apply(args=(3, 4))
    print(f"Chain apply result: {result}")
    assert result == 14

    task_id = chain.apply_async(args=(3, 4))
    print(f"Chain apply_async task_id: {task_id}")
    assert task_id is not None

    print("✓ Chain passed")
    return app


def test_group():
    print("\n=== Test 5: Group ===")
    app = TaskApp()

    @app.task(name="group_add", queue="high")
    def group_add(x, y):
        return x + y

    @app.task(name="group_multiply", queue="medium")
    def group_multiply(x, y):
        return x * y

    group = Group(group_add, group_multiply)
    results = group.apply(args=(3, 4))
    print(f"Group apply results: {results}")
    assert 7 in results
    assert 12 in results

    task_ids = group.apply_async(args=(3, 4))
    print(f"Group apply_async task_ids: {task_ids}")
    assert len(task_ids) == 2

    print("✓ Group passed")
    return app


def test_retry_backoff():
    print("\n=== Test 6: Retry Backoff ===")
    from scheduler.retry import calculate_backoff, should_retry

    task_payload = {"retry_count": 0, "max_retries": 3}
    assert should_retry(task_payload, Exception("test")) == True

    for i in range(5):
        delay = calculate_backoff(i)
        print(f"Retry {i}: backoff = {delay}s")

    assert calculate_backoff(0) == 1
    assert calculate_backoff(1) == 2
    assert calculate_backoff(2) == 4
    assert calculate_backoff(3) == 8
    assert calculate_backoff(10) == 60

    print("✓ Retry backoff passed")


def test_result_backend():
    print("\n=== Test 7: Result Backend ===")
    from scheduler.result_backend import create_backend

    fs_backend = create_backend("fs", config.STORAGE_PATH)
    task_id = "test-result-123"
    fs_backend.set(task_id, {"id": task_id, "status": "pending", "result": None})
    fs_backend.update_status(task_id, "running")
    fs_backend.store_result(task_id, "test-result", {"duration": 1.5})

    result = fs_backend.get(task_id)
    print(f"Stored result: {result}")
    assert result["status"] == "running"
    assert result["result"] == "test-result"
    assert result["result_meta"]["duration"] == 1.5

    mem_backend = create_backend("mem")
    mem_backend.set("mem-1", {"id": "mem-1", "status": "success", "result": 42})
    mem_result = mem_backend.get("mem-1")
    print(f"Memory backend result: {mem_result}")
    assert mem_result["result"] == 42

    print("✓ Result backend passed")


def test_scheduler_cron():
    print("\n=== Test 8: Scheduler Cron Calculation ===")
    from scheduler.scheduler import TaskScheduler

    app = TaskApp()

    @app.task(name="sched_test", queue="high")
    def sched_test():
        return "scheduled"

    scheduler = TaskScheduler(app)
    entry = scheduler.schedule(sched_test, cron="*/5 * * * *", args=(1, 2))
    print(f"Cron entry: next_run = {entry['next_run']}")
    assert entry["next_run"] is not None

    entry2 = scheduler.schedule(sched_test, interval=60, args=(3, 4))
    print(f"Interval entry: next_run = {entry2['next_run']}")
    assert entry2["next_run"] is not None

    print("✓ Scheduler cron calculation passed")


def test_dependency():
    print("\n=== Test 9: Task Dependency ===")
    app = TaskApp()

    @app.task(name="upstream_task", queue="high")
    def upstream(x):
        return x * 2

    @app.task(name="downstream_task", queue="medium", depends_on="upstream_task")
    def downstream(result=None):
        return f"got: {result}"

    print(f"upstream depends_on: {upstream.depends_on}")
    print(f"downstream depends_on: {downstream.depends_on}")
    assert downstream.depends_on == "upstream_task"

    print("✓ Task dependency passed")


def test_audit_logger():
    print("\n=== Test 10: Audit Logger ===")
    from scheduler.audit import AuditLogger

    logger = AuditLogger(config.STORAGE_PATH)
    entry = logger.log(
        actor="test_user",
        action="test_action",
        target="test_target",
        ip="127.0.0.1",
        result="success",
        detail={"key": "value"},
    )
    print(f"Audit entry: {entry}")
    assert entry["actor"] == "test_user"
    assert entry["action"] == "test_action"

    recent = logger.get_recent(limit=5)
    print(f"Recent logs count: {len(recent)}")
    assert len(recent) >= 1

    print("✓ Audit logger passed")


def test_auth():
    print("\n=== Test 11: Authentication ===")
    from scheduler.auth import check_auth, check_token

    assert check_auth(config.AUTH_USERNAME, config.AUTH_PASSWORD) == True
    assert check_auth("wrong", "wrong") == False
    assert check_token(config.AUTH_TOKEN) == True
    assert check_token("wrong-token") == False

    print("✓ Authentication passed")


def test_dlq():
    print("\n=== Test 12: Dead Letter Queue ===")
    from scheduler.dlq import DLQ
    from scheduler.result_backend import create_backend

    backend = create_backend("mem")
    dlq = DLQ(backend)

    task_data = {"id": "dlq-test-1", "task_name": "test", "status": "failed"}
    dlq.push(task_data)

    items = dlq.get_all()
    print(f"DLQ items: {len(items)}")
    assert len(items) == 1
    assert items[0]["status"] == "dead"
    assert "dlq_entered_at" in items[0]

    print("✓ DLQ passed")


def test_heartbeat():
    print("\n=== Test 13: Heartbeat Manager ===")
    from scheduler.heartbeat import HeartbeatManager

    hb = HeartbeatManager(interval=10)
    hb.register("worker-1", "high")
    hb.register("worker-2", "medium")

    hb.beat("worker-1")
    hb.update_stats("worker-1", completed=1, failed=0)

    all_hb = hb.get_all()
    print(f"Heartbeats: {list(all_hb.keys())}")
    assert "worker-1" in all_hb
    assert all_hb["worker-1"]["tasks_completed"] == 1

    alive = hb.get_alive()
    print(f"Alive workers: {list(alive.keys())}")
    assert "worker-1" in alive

    print("✓ Heartbeat manager passed")


if __name__ == "__main__":
    print("Starting feature tests...\n")
    all_passed = True

    tests = [
        test_task_registration,
        test_task_trigger,
        test_priority_queue,
        test_chain,
        test_group,
        test_retry_backoff,
        test_result_backend,
        test_scheduler_cron,
        test_dependency,
        test_audit_logger,
        test_auth,
        test_dlq,
        test_heartbeat,
    ]

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"✗ {test.__name__} FAILED: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False

    print("\n" + "=" * 50)
    if all_passed:
        print("✓ ALL TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED!")
        sys.exit(1)
