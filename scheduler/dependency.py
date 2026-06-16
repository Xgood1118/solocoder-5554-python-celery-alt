import uuid
from concurrent.futures import ThreadPoolExecutor

from scheduler.result_backend import create_backend


class Chain:
    def __init__(self, *task_proxies):
        self.task_proxies = task_proxies

    def apply_async(self, args=None, kwargs=None):
        if not self.task_proxies:
            raise ValueError("Chain must contain at least one task")

        first_task = self.task_proxies[0]
        chain_task_names = [tp.name for tp in self.task_proxies]

        payload_kwargs = dict(kwargs or {})
        payload_kwargs["_chain_tasks"] = chain_task_names
        payload_kwargs["_chain_index"] = 0

        task_id = first_task.apply_async(
            args=args,
            kwargs=payload_kwargs,
        )
        return task_id

    def apply(self, args=None, kwargs=None):
        if not self.task_proxies:
            raise ValueError("Chain must contain at least one task")

        current_args = args or ()
        current_kwargs = kwargs or {}
        result = None

        for i, task_proxy in enumerate(self.task_proxies):
            if i == 0:
                result = task_proxy.apply(args=current_args, kwargs=current_kwargs)
            else:
                result = task_proxy.apply(args=(result,), kwargs={})

        return result


class Group:
    def __init__(self, *task_proxies):
        self.task_proxies = task_proxies

    def apply_async(self, args=None, kwargs=None):
        if not self.task_proxies:
            raise ValueError("Group must contain at least one task")

        group_id = str(uuid.uuid4())
        group_size = len(self.task_proxies)
        task_ids = []

        for task_proxy in self.task_proxies:
            payload_kwargs = dict(kwargs or {})
            payload_kwargs["_group_id"] = group_id
            payload_kwargs["_group_size"] = group_size

            tid = task_proxy.apply_async(
                args=args,
                kwargs=payload_kwargs,
            )
            task_ids.append(tid)

        return task_ids

    def apply(self, args=None, kwargs=None):
        if not self.task_proxies:
            raise ValueError("Group must contain at least one task")

        def _run_task(task_proxy):
            return task_proxy.apply(args=args, kwargs=kwargs)

        with ThreadPoolExecutor(max_workers=len(self.task_proxies)) as executor:
            futures = [executor.submit(_run_task, tp) for tp in self.task_proxies]
            results = [f.result() for f in futures]

        return results


def handle_dependency(task_payload, app):
    task_name = task_payload.get("task_name")
    if not task_name:
        return

    for registered_name, task_proxy in app.tasks.items():
        depends_on = task_proxy.depends_on
        if not depends_on:
            continue

        should_trigger = False
        if isinstance(depends_on, str) and depends_on == task_name:
            should_trigger = True
        elif isinstance(depends_on, (list, tuple)) and task_name in depends_on:
            should_trigger = True

        if should_trigger:
            task_proxy.apply_async(
                args=task_payload.get("args", []),
                kwargs=task_payload.get("kwargs", {}),
            )


def handle_chain(task_payload, app):
    kwargs = task_payload.get("kwargs", {})
    chain_tasks = kwargs.get("_chain_tasks")
    chain_index = kwargs.get("_chain_index")

    if chain_tasks is None or chain_index is None:
        return

    next_index = chain_index + 1
    if next_index >= len(chain_tasks):
        return

    next_task_name = chain_tasks[next_index]
    next_task_proxy = app.tasks.get(next_task_name)
    if next_task_proxy is None:
        return

    result_data = app.result_backend.get(task_payload.get("id"))
    prev_result = None
    if result_data:
        prev_result = result_data.get("result")

    next_kwargs = {
        "_chain_tasks": chain_tasks,
        "_chain_index": next_index,
    }

    next_task_proxy.apply_async(
        args=([prev_result] if prev_result is not None else []),
        kwargs=next_kwargs,
    )


def handle_task_failure(task_payload, app, error):
    task_name = task_payload.get("task_name")
    task_proxy = app.tasks.get(task_name)

    if task_proxy and task_proxy.cleanup and callable(task_proxy.cleanup):
        try:
            task_proxy.cleanup(task_payload.get("id"), error)
        except Exception:
            pass

    kwargs = task_payload.get("kwargs", {})
    chain_tasks = kwargs.get("_chain_tasks")
    chain_index = kwargs.get("_chain_index")

    if chain_tasks is not None and chain_index is not None:
        for i in range(chain_index + 1, len(chain_tasks)):
            remaining_task_name = chain_tasks[i]
            remaining_task_id = str(uuid.uuid4())
            app.result_backend.set(remaining_task_id, {
                "id": remaining_task_id,
                "task_name": remaining_task_name,
                "status": "FAILED",
                "error": str(error),
                "chain_tasks": chain_tasks,
                "chain_index": i,
                "chain_parent_failure": task_payload.get("id"),
            })
