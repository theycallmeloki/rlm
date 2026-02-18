"""
Coordinator entrypoint for RLMJob Kubernetes pods.

This script runs inside the Coordinator Pod, discovers worker pods,
waits for the LMHandler service, then runs the RLM completion loop.
The final result is patched into the RLMJob status.
"""

import json
import os
import socket
import time
import traceback


def wait_for_workers(core_api, namespace: str, job_name: str, num_workers: int, timeout: int = 300):
    """Wait for all worker pods to be Running and return their IPs."""
    label_selector = f"rlm-job={job_name},rlm-role=worker"

    for _ in range(timeout):
        pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        running_pods = [
            p for p in pods.items
            if p.status.phase == "Running" and p.status.pod_ip
        ]
        if len(running_pods) >= num_workers:
            return [p.status.pod_ip for p in running_pods[:num_workers]]
        time.sleep(1)

    raise TimeoutError(f"Only {len(running_pods)}/{num_workers} workers reached Running state")


def wait_for_lm_handler(host: str, port: int, timeout: int = 120):
    """Wait for LMHandler service to be reachable via TCP."""
    for _ in range(timeout):
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
            return
        except (ConnectionRefusedError, OSError, socket.timeout):
            time.sleep(1)

    raise TimeoutError(f"LMHandler at {host}:{port} not reachable within {timeout}s")


def patch_status(custom_api, namespace: str, job_name: str, status: dict):
    """Patch the RLMJob status subresource."""
    custom_api.patch_namespaced_custom_object_status(
        group="rlm.io",
        version="v1alpha1",
        namespace=namespace,
        plural="rlmjobs",
        name=job_name,
        body={"status": status},
    )


def main():
    from kubernetes import client, config

    config.load_incluster_config()

    core_api = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    # Read configuration from environment
    job_name = os.environ["RLM_JOB_NAME"]
    namespace = os.environ["RLM_JOB_NAMESPACE"]
    prompt = os.environ["RLM_PROMPT"]
    backend = os.environ["RLM_BACKEND"]
    backend_kwargs = json.loads(os.environ["BACKEND_KWARGS"])
    num_workers = int(os.environ.get("RLM_NUM_WORKERS", "1"))
    max_iterations = int(os.environ.get("RLM_MAX_ITERATIONS", "30"))
    max_depth = int(os.environ.get("RLM_MAX_DEPTH", "1"))
    worker_image = os.environ.get("RLM_WORKER_IMAGE", "python:3.11-slim")
    timeout = int(os.environ.get("RLM_TIMEOUT", "600"))

    lm_handler_host = f"{job_name}-lm-handler"
    lm_handler_port = 9090

    try:
        # Wait for infrastructure to be ready
        worker_ips = wait_for_workers(core_api, namespace, job_name, num_workers, timeout)
        wait_for_lm_handler(lm_handler_host, lm_handler_port)

        # Patch status to Running
        patch_status(custom_api, namespace, job_name, {"phase": "Running"})

        # Create RLM instance with external handler and kubernetes environment
        from rlm.core.rlm import RLM

        rlm = RLM(
            backend=backend,
            backend_kwargs=backend_kwargs,
            environment="kubernetes",
            environment_kwargs={
                "namespace": namespace,
                "image": worker_image,
                "timeout": timeout,
                "pod_ip": worker_ips[0],
                "skip_pod_creation": True,
            },
            max_iterations=max_iterations,
            max_depth=max_depth,
            external_lm_handler_address=(lm_handler_host, lm_handler_port),
        )

        # Run completion
        result = rlm.completion(prompt)

        # Patch success status
        patch_status(custom_api, namespace, job_name, {
            "phase": "Completed",
            "result": result.response,
            "iterations": max_iterations,  # Actual count tracked internally
        })

    except Exception:
        error_msg = traceback.format_exc()
        try:
            patch_status(custom_api, namespace, job_name, {
                "phase": "Failed",
                "error": error_msg[-1000:],  # Truncate to avoid status size limits
            })
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
