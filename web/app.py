import json
import time
import traceback
from datetime import datetime, timezone

from flask import Flask, render_template, request, jsonify, Response, redirect, url_for
from scheduler.auth import require_auth, get_actor, get_client_ip
from scheduler.audit import AuditLogger
import config


def create_web_app(task_app, worker_pool=None):
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"),
        static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"),
    )
    app.task_app = task_app
    app.worker_pool = worker_pool
    app.audit_logger = AuditLogger(config.STORAGE_PATH)

    @app.context_processor
    def inject_globals():
        return {
            "now": datetime.now(timezone.utc).isoformat(),
            "queue_names": config.QUEUE_NAMES,
            "token_masked": config.AUTH_TOKEN[:8] + "****" if len(config.AUTH_TOKEN) > 8 else "****",
        }

    @app.route("/")
    @require_auth
    def index():
        return redirect(url_for("task_list"))

    @app.route("/tasks")
    @require_auth
    def task_list():
        status = request.args.get("status", "")
        queue = request.args.get("queue", "")
        time_from = request.args.get("time_from", "")
        time_to = request.args.get("time_to", "")
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))

        tasks = app.task_app.result_backend.query(
            status=status or None,
            queue=queue or None,
            limit=limit,
            offset=offset,
            time_from=time_from or None,
            time_to=time_to or None,
        )

        registered = app.task_app.registry.all()
        app.audit_logger.log(
            actor=get_actor(), action="list_tasks",
            target="task_list", ip=get_client_ip(), result="success",
        )

        return render_template("tasks.html",
                               tasks=tasks, registered=registered,
                               status=status, queue=queue,
                               time_from=time_from, time_to=time_to,
                               queue_names=config.QUEUE_NAMES)

    @app.route("/tasks/<task_id>")
    @require_auth
    def task_detail(task_id):
        task = app.task_app.result_backend.get(task_id)
        if not task:
            return render_template("error.html", message="Task not found"), 404

        app.audit_logger.log(
            actor=get_actor(), action="view_task",
            target=task_id, ip=get_client_ip(), result="success",
        )
        return render_template("task_detail.html", task=task, task_id=task_id)

    @app.route("/tasks/<task_id>/rerun", methods=["POST"])
    @require_auth
    def task_rerun(task_id):
        task = app.task_app.result_backend.get(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        task_name = task.get("task_name")
        task_proxy = app.task_app.tasks.get(task_name)
        if not task_proxy:
            return jsonify({"error": "Task not registered"}), 400

        new_id = task_proxy.apply_async(
            args=task.get("args", []),
            kwargs=task.get("kwargs", {}),
            queue=task.get("queue"),
            priority=task.get("priority", 5),
        )

        app.audit_logger.log(
            actor=get_actor(), action="rerun_task",
            target=task_id, ip=get_client_ip(),
            result="success", detail={"new_task_id": new_id},
        )
        return jsonify({"new_task_id": new_id}), 201

    @app.route("/queues")
    @require_auth
    def queue_panel():
        depths = app.task_app.queue_manager.all_depths()
        peek_data = {}
        for qname in config.QUEUE_NAMES:
            peek_data[qname] = app.task_app.queue_manager.peek(qname, limit=10)

        app.audit_logger.log(
            actor=get_actor(), action="view_queues",
            target="queue_panel", ip=get_client_ip(), result="success",
        )
        return render_template("queues.html", depths=depths, peek_data=peek_data)

    @app.route("/workers")
    @require_auth
    def worker_status():
        status = {}
        if app.worker_pool:
            status = app.worker_pool.get_worker_status()
            heartbeat = app.worker_pool.get_heartbeat_data()
        else:
            heartbeat = {}

        app.audit_logger.log(
            actor=get_actor(), action="view_workers",
            target="worker_status", ip=get_client_ip(), result="success",
        )
        return render_template("workers.html", status=status, heartbeat=heartbeat)

    @app.route("/scheduled")
    @require_auth
    def scheduled_tasks():
        scheduler = app.task_app.get_scheduler()
        scheduled = scheduler.get_scheduled_tasks()

        app.audit_logger.log(
            actor=get_actor(), action="view_scheduled",
            target="scheduled_tasks", ip=get_client_ip(), result="success",
        )
        return render_template("scheduled.html", scheduled=scheduled)

    @app.route("/dependencies")
    @require_auth
    def dependency_graph():
        registered = app.task_app.registry.all()
        nodes = []
        edges = []
        for name, info in registered.items():
            nodes.append({"id": name, "queue": info.get("queue", "medium")})
            dep = info.get("depends_on")
            if dep:
                if isinstance(dep, str):
                    edges.append({"from": dep, "to": name})
                elif isinstance(dep, (list, tuple)):
                    for d in dep:
                        edges.append({"from": d, "to": name})

        app.audit_logger.log(
            actor=get_actor(), action="view_dependencies",
            target="dependency_graph", ip=get_client_ip(), result="success",
        )
        return render_template("dependencies.html", nodes=nodes, edges=edges)

    @app.route("/logs")
    @require_auth
    def log_stream():
        app.audit_logger.log(
            actor=get_actor(), action="view_logs",
            target="log_stream", ip=get_client_ip(), result="success",
        )
        return render_template("logs.html")

    @app.route("/dlq")
    @require_auth
    def dead_letter_queue():
        dlq_items = app.task_app.result_backend.get_dlq()

        app.audit_logger.log(
            actor=get_actor(), action="view_dlq",
            target="dlq", ip=get_client_ip(), result="success",
        )
        return render_template("dlq.html", dlq_items=dlq_items)

    @app.route("/dlq/<task_id>/requeue", methods=["POST"])
    @require_auth
    def dlq_requeue(task_id):
        from scheduler.dlq import DLQ
        dlq = DLQ(app.task_app.result_backend)
        success = dlq.requeue(task_id, app.task_app.queue_manager)

        app.audit_logger.log(
            actor=get_actor(), action="requeue_dlq",
            target=task_id, ip=get_client_ip(),
            result="success" if success else "not_found",
        )
        return jsonify({"success": success})

    @app.route("/api/tasks/stream")
    @require_auth
    def task_stream():
        def generate():
            while True:
                depths = app.task_app.queue_manager.all_depths()
                tasks = app.task_app.result_backend.query(limit=20)
                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "queue_depths": depths,
                    "recent_tasks": tasks,
                }
                yield f"data: {json.dumps(data, default=str)}\n\n"
                time.sleep(config.SSE_INTERVAL)

        return Response(generate(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.route("/api/logs/stream")
    @require_auth
    def log_sse_stream():
        def generate():
            while True:
                logs = app.audit_logger.get_recent(limit=20)
                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "logs": logs,
                }
                yield f"data: {json.dumps(data, default=str)}\n\n"
                time.sleep(config.SSE_INTERVAL)

        return Response(generate(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.route("/api/stats")
    @require_auth
    def api_stats():
        depths = app.task_app.queue_manager.all_depths()
        worker_status = {}
        if app.worker_pool:
            worker_status = app.worker_pool.get_worker_status()
        dlq_size = len(app.task_app.result_backend.get_dlq())

        return jsonify({
            "queue_depths": depths,
            "worker_status": worker_status,
            "dlq_size": dlq_size,
            "registered_tasks": list(app.task_app.tasks.keys()),
        })

    return app


import os
