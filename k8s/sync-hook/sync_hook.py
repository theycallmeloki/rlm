"""
Metacontroller sync hook webhook for RLMJob CompositeController.

Receives the parent RLMJob spec + observed children, returns desired children
(LMHandler Pod+Service, Worker Pods, Coordinator Pod) and status updates.
"""

import json
import os

from flask import Flask, jsonify, request

app = Flask(__name__)

LM_HANDLER_PORT = 9090
BROKER_PORT = 8080
RLM_IMAGE = os.environ.get("RLM_IMAGE", "rlm:latest")


def _lm_handler_pod(name: str, spec: dict) -> dict:
    """Build the LMHandler Pod manifest."""
    backend = spec["backend"]
    backend_kwargs = spec.get("backendKwargs", {})
    backend_secret = spec["backendSecret"]
    timeout = spec.get("timeout", 600)

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-lm-handler",
            "labels": {
                "rlm-job": name,
                "rlm-role": "lm-handler",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "lm-handler",
                    "image": RLM_IMAGE,
                    "command": [
                        "python",
                        "-c",
                        (
                            "from rlm.clients import get_client; "
                            "from rlm.core.lm_handler import LMHandler; "
                            "import json, os, time; "
                            f"backend_kwargs = json.loads(os.environ['BACKEND_KWARGS']); "
                            f"client = get_client('{backend}', backend_kwargs); "
                            "handler = LMHandler(client, host='0.0.0.0', port=9090); "
                            "handler.start(); "
                            f"time.sleep({timeout})"
                        ),
                    ],
                    "ports": [{"containerPort": LM_HANDLER_PORT}],
                    "env": [
                        {
                            "name": "BACKEND_KWARGS",
                            "value": json.dumps(backend_kwargs),
                        },
                    ],
                    "envFrom": [
                        {
                            "secretRef": {"name": backend_secret},
                        },
                    ],
                },
            ],
        },
    }


def _lm_handler_service(name: str) -> dict:
    """Build the LMHandler ClusterIP Service manifest."""
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": f"{name}-lm-handler",
            "labels": {
                "rlm-job": name,
                "rlm-role": "lm-handler",
            },
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {
                "rlm-job": name,
                "rlm-role": "lm-handler",
            },
            "ports": [
                {
                    "port": LM_HANDLER_PORT,
                    "targetPort": LM_HANDLER_PORT,
                    "protocol": "TCP",
                },
            ],
        },
    }


def _worker_pod(name: str, index: int, spec: dict) -> dict:
    """Build a Worker Pod manifest."""
    image = spec.get("image", "python:3.11-slim")
    resource_requests = spec.get("resourceRequests")
    resource_limits = spec.get("resourceLimits")

    container = {
        "name": "worker",
        "image": image,
        "command": [
            "sh",
            "-c",
            (
                "pip install dill requests flask && "
                "python -c \""
                "from flask import Flask, request, jsonify; "
                "import json, threading, uuid; "
                "app = Flask(__name__); "
                "pending_requests = {}; "
                "lock = threading.Lock(); "
                "@app.route('/health'); "
                "def health(): return jsonify({'status': 'ok'}); "
                "@app.route('/enqueue', methods=['POST']); "
                "def enqueue(): "
                "    data = request.json; "
                "    request_id = str(uuid.uuid4()); "
                "    event = threading.Event(); "
                "    with lock: pending_requests[request_id] = {'request': data, 'response': None, 'event': event}; "
                "    event.wait(timeout=300); "
                "    with lock: entry = pending_requests.pop(request_id, None); "
                "    return jsonify(entry['response']) if entry and entry['response'] else (jsonify({'error': 'timeout'}), 504); "
                "@app.route('/pending'); "
                "def get_pending(): "
                "    with lock: pending = [{'id': r, 'request': e['request']} for r, e in pending_requests.items() if not e['response']]; "
                "    return jsonify({'pending': pending}); "
                "@app.route('/respond', methods=['POST']); "
                "def respond(): "
                "    data = request.json; "
                "    with lock: "
                "        if data['id'] in pending_requests: pending_requests[data['id']]['response'] = data['response']; pending_requests[data['id']]['event'].set(); return jsonify({'status': 'ok'}); "
                "    return (jsonify({'error': 'not found'}), 404); "
                "app.run(host='0.0.0.0', port=8080, threaded=True)\""
            ),
        ],
        "ports": [{"containerPort": BROKER_PORT}],
    }

    resources = {}
    if resource_requests:
        resources["requests"] = resource_requests
    if resource_limits:
        resources["limits"] = resource_limits
    if resources:
        container["resources"] = resources

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-worker-{index}",
            "labels": {
                "rlm-job": name,
                "rlm-role": "worker",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [container],
        },
    }


def _coordinator_pod(name: str, namespace: str, spec: dict) -> dict:
    """Build the Coordinator Pod manifest."""
    backend = spec["backend"]
    backend_kwargs = spec.get("backendKwargs", {})
    backend_secret = spec["backendSecret"]
    num_workers = spec.get("numWorkers", 1)
    max_iterations = spec.get("maxIterations", 30)
    max_depth = spec.get("maxDepth", 1)
    image = spec.get("image", "python:3.11-slim")
    timeout = spec.get("timeout", 600)
    prompt = spec["prompt"]

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-coordinator",
            "labels": {
                "rlm-job": name,
                "rlm-role": "coordinator",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "serviceAccountName": "rlm-coordinator",
            "containers": [
                {
                    "name": "coordinator",
                    "image": RLM_IMAGE,
                    "command": [
                        "python",
                        "/app/coordinator.py",
                    ],
                    "env": [
                        {"name": "RLM_JOB_NAME", "value": name},
                        {"name": "RLM_JOB_NAMESPACE", "value": namespace},
                        {"name": "RLM_PROMPT", "value": prompt},
                        {"name": "RLM_BACKEND", "value": backend},
                        {"name": "BACKEND_KWARGS", "value": json.dumps(backend_kwargs)},
                        {"name": "RLM_NUM_WORKERS", "value": str(num_workers)},
                        {"name": "RLM_MAX_ITERATIONS", "value": str(max_iterations)},
                        {"name": "RLM_MAX_DEPTH", "value": str(max_depth)},
                        {"name": "RLM_WORKER_IMAGE", "value": image},
                        {"name": "RLM_TIMEOUT", "value": str(timeout)},
                    ],
                    "envFrom": [
                        {
                            "secretRef": {"name": backend_secret},
                        },
                    ],
                },
            ],
        },
    }


def _compute_status(observed_children: dict) -> dict:
    """Compute RLMJob status from observed child state."""
    pods = observed_children.get("Pod.v1", {})

    coordinator_pod = None
    for pod_name, pod in pods.items():
        labels = pod.get("metadata", {}).get("labels", {})
        if labels.get("rlm-role") == "coordinator":
            coordinator_pod = pod
            break

    if coordinator_pod is None:
        return {"phase": "Pending"}

    phase = coordinator_pod.get("status", {}).get("phase", "Pending")

    if phase == "Running":
        return {"phase": "Running"}

    if phase == "Succeeded":
        container_statuses = coordinator_pod.get("status", {}).get("containerStatuses", [])
        for cs in container_statuses:
            terminated = cs.get("state", {}).get("terminated", {})
            if terminated.get("exitCode") == 0:
                return {"phase": "Completed"}
        return {"phase": "Completed"}

    if phase == "Failed":
        container_statuses = coordinator_pod.get("status", {}).get("containerStatuses", [])
        error_msg = "Coordinator pod failed"
        for cs in container_statuses:
            terminated = cs.get("state", {}).get("terminated", {})
            if terminated.get("message"):
                error_msg = terminated["message"]
                break
        return {"phase": "Failed", "error": error_msg}

    return {"phase": "Pending"}


@app.route("/sync", methods=["POST"])
def sync():
    """Metacontroller sync hook: compute desired children from parent spec."""
    body = request.json
    parent = body.get("parent", {})
    observed_children = body.get("children", {})

    name = parent["metadata"]["name"]
    namespace = parent["metadata"]["namespace"]
    spec = parent.get("spec", {})
    num_workers = spec.get("numWorkers", 1)

    # Build desired children
    desired_pods = {}
    desired_services = {}

    # LMHandler Pod + Service
    handler_pod = _lm_handler_pod(name, spec)
    desired_pods[handler_pod["metadata"]["name"]] = handler_pod

    handler_svc = _lm_handler_service(name)
    desired_services[handler_svc["metadata"]["name"]] = handler_svc

    # Worker Pods
    for i in range(num_workers):
        worker = _worker_pod(name, i, spec)
        desired_pods[worker["metadata"]["name"]] = worker

    # Coordinator Pod
    coordinator = _coordinator_pod(name, namespace, spec)
    desired_pods[coordinator["metadata"]["name"]] = coordinator

    # Compute status
    status = _compute_status(observed_children)

    return jsonify({
        "status": status,
        "children": [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "desired": list(desired_pods.values()),
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "desired": list(desired_services.values()),
            },
        ],
    })


@app.route("/finalize", methods=["POST"])
def finalize():
    """Metacontroller finalize hook: return empty children to delete all resources."""
    return jsonify({
        "status": {"phase": "Failed", "error": "Deleted"},
        "children": [
            {"apiVersion": "v1", "kind": "Pod", "desired": []},
            {"apiVersion": "v1", "kind": "Service", "desired": []},
        ],
        "finalized": True,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
