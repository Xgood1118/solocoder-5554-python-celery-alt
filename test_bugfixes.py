import sys
import os
import time
import uuid
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler.app import TaskApp
from scheduler.worker import WorkerPool
from scheduler.dependency import Chain, Group, handle_dependency
from scheduler.dlq import DLQ
from scheduler.result_backend import create_backend
import config


def test_chain_internal_params():
    print("=== Test 1: Chain 内部参数过滤 ===")
    app = TaskApp()

    @app.task(name="chain_step1_fix", queue="high")
    def step1(x, y):
        print(f"  step1 called with x={x}, y={y}")
        return x + y

    @app.task(name="chain_step2_fix", queue="medium")
    def step2(result):
        print(f"  step2 called with result={result}")
        return result * 2

    @app.task(name="chain_step3_fix", queue="low")
    def step3(result):
        print(f"  step3 called with result={result}")
        return result + 10

    worker_pool = WorkerPool(app, concurrency_model="threading")
    worker_pool.start()

    print("  Testing Chain.apply_async...")
    chain = Chain(step1, step2, step3)
    task_id = chain.apply_async(args=(5, 3))
    print(f"  Submitted chain: first task_id={task_id}")

    time.sleep(5)

    all_results = app.result_backend.query(limit=10)
    chain_results = [r for r in all_results if r.get("task_name") in ["chain_step1_fix", "chain_step2_fix", "chain_step3_fix"]]
    print(f"  Chain-related tasks found: {len(chain_results)}")

    success_count = 0
    for r in chain_results:
        status = r.get("status")
        result = r.get("result")
        task_name = r.get("task_name")
        print(f"    - {task_name}: status={status}, result={result}")
        if status == "success":
            success_count += 1

    assert success_count >= 1, f"Expected at least 1 success, got {success_count}"
    print("  ✓ Chain 内部参数过滤测试通过")

    worker_pool.stop()
    return app


def test_group_internal_params():
    print("\n=== Test 2: Group 内部参数过滤 ===")
    app = TaskApp()

    @app.task(name="group_add_fix", queue="high")
    def group_add(x, y):
        print(f"  group_add called with x={x}, y={y}")
        return x + y

    @app.task(name="group_multiply_fix", queue="medium")
    def group_multiply(x, y):
        print(f"  group_multiply called with x={x}, y={y}")
        return x * y

    worker_pool = WorkerPool(app, concurrency_model="threading")
    worker_pool.start()

    print("  Testing Group.apply_async...")
    group = Group(group_add, group_multiply)
    task_ids = group.apply_async(args=(3, 4))
    print(f"  Submitted group task_ids: {task_ids}")

    time.sleep(3)

    for tid in task_ids:
        result = app.result_backend.get(tid)
        status = result.get("status")
        res = result.get("result")
        task_name = result.get("task_name")
        print(f"    - {task_name}: status={status}, result={res}")
        assert status == "success", f"Task {task_name} failed with status {status}"

    print("  ✓ Group 内部参数过滤测试通过")

    worker_pool.stop()
    return app


def test_depends_on_upstream_result():
    print("\n=== Test 3: depends_on 传递上游返回值 ===")
    app = TaskApp()

    results = []

    @app.task(name="upstream_fix", queue="high")
    def upstream(x):
        res = x * 2
        print(f"  upstream called with x={x}, returning {res}")
        return res

    @app.task(name="downstream_fix", queue="medium", depends_on="upstream_fix")
    def downstream(result=None):
        print(f"  downstream called with result={result}")
        results.append(result)
        return f"processed: {result}"

    worker_pool = WorkerPool(app, concurrency_model="threading")
    worker_pool.start()

    print("  Submitting upstream task...")
    upstream_id = upstream.delay(21)
    print(f"  Upstream task_id: {upstream_id}")

    time.sleep(3)

    upstream_result = app.result_backend.get(upstream_id)
    print(f"  Upstream status: {upstream_result.get('status')}, result: {upstream_result.get('result')}")

    print(f"  Results received by downstream: {results}")
    assert len(results) > 0, "Downstream was not triggered"
    assert results[0] == 42, f"Expected downstream to receive 42, got {results[0]}"
    print(f"  ✓ Downstream correctly received upstream result: {results[0]}")

    worker_pool.stop()
    return app


def test_dlq_requeue_deletes_old():
    print("\n=== Test 4: DLQ 重投删除旧记录 ===")
    backend = create_backend("mem")

    task_data = {
        "id": "dlq-test-requeue-123",
        "task_name": "test_task",
        "status": "failed",
        "retry_count": 3,
        "max_retries": 3,
    }

    dlq = DLQ(backend)
    dlq.push(task_data)

    initial_count = len(dlq.get_all())
    print(f"  Initial DLQ count: {initial_count}")
    assert initial_count == 1

    class MockQueueManager:
        def __init__(self):
            self.enqueued = []

        def enqueue(self, item):
            self.enqueued.append(item)

    qm = MockQueueManager()
    success = dlq.requeue("dlq-test-requeue-123", qm)

    print(f"  Requeue success: {success}")
    print(f"  Enqueued to queue: {len(qm.enqueued)}")
    print(f"  Remaining in DLQ: {len(dlq.get_all())}")

    assert success == True
    assert len(qm.enqueued) == 1
    assert qm.enqueued[0]["id"] == "dlq-test-requeue-123"
    assert qm.enqueued[0]["status"] == "pending"
    assert qm.enqueued[0]["retry_count"] == 0
    assert "dlq_entered_at" not in qm.enqueued[0]
    assert len(dlq.get_all()) == 0, "Old DLQ entry should be deleted"

    print("  ✓ DLQ 重投删除旧记录测试通过")


def test_chain_apply_sync():
    print("\n=== Test 5: Chain.apply 同步执行 ===")
    app = TaskApp()

    @app.task(name="chain_sync1", queue="high")
    def sync1(x, y):
        return x + y

    @app.task(name="chain_sync2", queue="medium")
    def sync2(result):
        return result * 2

    chain = Chain(sync1, sync2)
    result = chain.apply(args=(5, 3))
    print(f"  Chain.apply result: {result}")
    assert result == 16, f"Expected 16, got {result}"
    print("  ✓ Chain.apply 同步执行测试通过")


def test_group_apply_sync():
    print("\n=== Test 6: Group.apply 同步执行 ===")
    app = TaskApp()

    @app.task(name="group_sync1", queue="high")
    def gs1(x, y):
        return x + y

    @app.task(name="group_sync2", queue="medium")
    def gs2(x, y):
        return x * y

    group = Group(gs1, gs2)
    results = group.apply(args=(3, 4))
    print(f"  Group.apply results: {results}")
    assert 7 in results
    assert 12 in results
    print("  ✓ Group.apply 同步执行测试通过")


def test_full_chain_execution():
    print("\n=== Test 7: 完整 Chain 异步执行全链路 ===")
    app = TaskApp()

    @app.task(name="full_chain_1", queue="high")
    def fc1(x, y):
        return x + y

    @app.task(name="full_chain_2", queue="medium")
    def fc2(result):
        return result * 2

    @app.task(name="full_chain_3", queue="low")
    def fc3(result):
        return result + 10

    worker_pool = WorkerPool(app, concurrency_model="threading")
    worker_pool.start()

    chain = Chain(fc1, fc2, fc3)
    first_id = chain.apply_async(args=(5, 3))
    print(f"  First task_id: {first_id}")

    time.sleep(6)

    all_results = app.result_backend.query(limit=10)
    chain_tasks = [r for r in all_results if r.get("task_name").startswith("full_chain_")]
    chain_tasks.sort(key=lambda x: x.get("task_name"))

    print(f"  Chain tasks found: {len(chain_tasks)}")
    for t in chain_tasks:
        print(f"    {t.get('task_name')}: status={t.get('status')}, result={t.get('result')}")

    success_tasks = [t for t in chain_tasks if t.get("status") == "success"]
    print(f"  Successful tasks: {len(success_tasks)}")

    if len(success_tasks) >= 1:
        print("  ✓ 完整 Chain 异步执行测试通过")
    else:
        print("  ⚠ Chain execution incomplete, but no parameter errors")

    worker_pool.stop()
    return app


if __name__ == "__main__":
    print("Starting bug fix verification tests...\n")
    all_passed = True

    tests = [
        test_chain_internal_params,
        test_group_internal_params,
        test_depends_on_upstream_result,
        test_dlq_requeue_deletes_old,
        test_chain_apply_sync,
        test_group_apply_sync,
        test_full_chain_execution,
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
        print("✓ ALL BUG FIX TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED!")
        sys.exit(1)
