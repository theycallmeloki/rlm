"""
Kubernetes REPL environment that runs Python code in a Kubernetes pod.

Follows the ModalREPL broker pattern: a Flask broker runs inside the pod,
code execution sends LLM requests to the broker, and the KubernetesREPL
polls the broker and forwards requests to the LMHandler.

Requires: pip install kubernetes dill
"""

import base64
import json
import textwrap
import threading
import time

import requests as http_requests

from rlm.core.comms_utils import LMRequest, send_lm_request, send_lm_request_batched
from rlm.core.types import REPLResult, RLMChatCompletion
from rlm.environments.base_env import IsolatedEnv

# =============================================================================
# Broker Server Script (runs inside pod, handles LLM request queue)
# Reused verbatim from modal_repl.py
# =============================================================================

_BROKER_SCRIPT = textwrap.dedent(
    '''
import json
import threading
import uuid
from flask import Flask, request, jsonify

app = Flask(__name__)

# Request queue: {request_id: {"request": {...}, "response": None, "event": Event}}
pending_requests = {}
lock = threading.Lock()

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/enqueue", methods=["POST"])
def enqueue():
    """Called by pod code to submit an LLM request and wait for response."""
    data = request.json
    request_id = str(uuid.uuid4())
    event = threading.Event()

    with lock:
        pending_requests[request_id] = {
            "request": data,
            "response": None,
            "event": event,
        }

    # Wait for response (with timeout)
    event.wait(timeout=300)

    with lock:
        entry = pending_requests.pop(request_id, None)

    if entry and entry["response"] is not None:
        return jsonify(entry["response"])
    else:
        return jsonify({"error": "Request timed out"}), 504

@app.route("/pending")
def get_pending():
    """Called by KubernetesREPL to get pending requests."""
    with lock:
        pending = [
            {"id": rid, "request": entry["request"]}
            for rid, entry in pending_requests.items()
            if entry["response"] is None
        ]
    return jsonify({"pending": pending})

@app.route("/respond", methods=["POST"])
def respond():
    """Called by KubernetesREPL to submit a response."""
    data = request.json
    request_id = data.get("id")
    response = data.get("response")

    with lock:
        if request_id in pending_requests:
            pending_requests[request_id]["response"] = response
            pending_requests[request_id]["event"].set()
            return jsonify({"status": "ok"})

    return jsonify({"error": "Request not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
'''
)


# =============================================================================
# Execution Script Builder (reused from modal_repl.py)
# =============================================================================


def _build_exec_script(code: str, broker_port: int = 8080, depth: int = 1) -> str:
    """
    Build a script that executes code with state persistence.
    LLM queries go through the local broker server.
    """
    code_b64 = base64.b64encode(code.encode()).decode()

    return textwrap.dedent(
        f'''
import sys
import io
import json
import base64
import traceback
import os
import requests

try:
    import dill
except ImportError:
    import pickle as dill

# =============================================================================
# LLM Query Functions (via local broker)
# =============================================================================

BROKER_URL = "http://127.0.0.1:{broker_port}"

def llm_query(prompt, model=None):
    """Query the LM via the broker."""
    try:
        response = requests.post(
            f"{{BROKER_URL}}/enqueue",
            json={{"type": "single", "prompt": prompt, "model": model, "depth": {depth}}},
            timeout=300,
        )
        data = response.json()
        if data.get("error"):
            return f"Error: {{data['error']}}"
        return data.get("response", "Error: No response")
    except Exception as e:
        return f"Error: LM query failed - {{e}}"


def llm_query_batched(prompts, model=None):
    """Query the LM with multiple prompts."""
    try:
        response = requests.post(
            f"{{BROKER_URL}}/enqueue",
            json={{"type": "batched", "prompts": prompts, "model": model, "depth": {depth}}},
            timeout=300,
        )
        data = response.json()
        if data.get("error"):
            return [f"Error: {{data['error']}}"] * len(prompts)
        return data.get("responses", ["Error: No response"] * len(prompts))
    except Exception as e:
        return [f"Error: LM query failed - {{e}}"] * len(prompts)


# =============================================================================
# State Management
# =============================================================================

STATE_FILE = "/tmp/rlm_state.dill"

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "rb") as f:
                return dill.load(f)
        except:
            pass
    return {{}}

def save_state(state):
    clean_state = {{}}
    for k, v in state.items():
        if k.startswith("_"):
            continue
        try:
            dill.dumps(v)
            clean_state[k] = v
        except:
            pass
    with open(STATE_FILE, "wb") as f:
        dill.dump(clean_state, f)

def serialize_locals(state):
    result = {{}}
    for k, v in state.items():
        if k.startswith("_"):
            continue
        try:
            result[k] = repr(v)
        except:
            result[k] = f"<{{type(v).__name__}}>"
    return result

# =============================================================================
# Execution
# =============================================================================

_locals = load_state()

def FINAL_VAR(variable_name):
    variable_name = variable_name.strip().strip("\\"\\'")
    if variable_name in _locals:
        return str(_locals[variable_name])
    available = [k for k in _locals.keys() if not k.startswith("_")]
    if available:
        return f"Error: Variable '{{variable_name}}' not found. Available variables: {{available}}. You must create and assign a variable BEFORE calling FINAL_VAR on it."
    return f"Error: Variable '{{variable_name}}' not found. No variables have been created yet. You must create and assign a variable in a REPL block BEFORE calling FINAL_VAR on it."

def SHOW_VARS():
    available = {{k: type(v).__name__ for k, v in _locals.items() if not k.startswith("_")}}
    if not available:
        return "No variables created yet. Use ```repl``` blocks to create variables."
    return f"Available variables: {{available}}"

_globals = {{
    "__builtins__": __builtins__,
    "__name__": "__main__",
    "llm_query": llm_query,
    "llm_query_batched": llm_query_batched,
    "FINAL_VAR": FINAL_VAR,
    "SHOW_VARS": SHOW_VARS,
}}

code = base64.b64decode("{code_b64}").decode()

stdout_buf = io.StringIO()
stderr_buf = io.StringIO()
old_stdout, old_stderr = sys.stdout, sys.stderr

try:
    sys.stdout = stdout_buf
    sys.stderr = stderr_buf
    combined = {{**_globals, **_locals}}
    exec(code, combined, combined)
    for key, value in combined.items():
        if key not in _globals and not key.startswith("_"):
            _locals[key] = value
except Exception as e:
    traceback.print_exc(file=stderr_buf)
finally:
    sys.stdout = old_stdout
    sys.stderr = old_stderr

save_state(_locals)

result = {{
    "stdout": stdout_buf.getvalue(),
    "stderr": stderr_buf.getvalue(),
    "locals": serialize_locals(_locals),
}}
print(json.dumps(result))
'''
    )


class KubernetesREPL(IsolatedEnv):
    """
    Kubernetes REPL environment that runs Python code in a Kubernetes pod.

    Uses a Flask broker inside the pod for LLM communication:
    - Pod runs a broker server on port 8080
    - KubernetesREPL polls the broker for pending LLM requests
    - KubernetesREPL forwards requests to the LM handler and posts responses back
    """

    BROKER_PORT = 8080

    def __init__(
        self,
        namespace: str = "default",
        image: str = "python:3.11-slim",
        timeout: int = 600,
        lm_handler_address: tuple[str, int] | None = None,
        context_payload: dict | list | str | None = None,
        setup_code: str | None = None,
        persistent: bool = False,
        depth: int = 1,
        resource_requests: dict[str, str] | None = None,
        resource_limits: dict[str, str] | None = None,
        service_account: str | None = None,
        pod_ip: str | None = None,
        skip_pod_creation: bool = False,
        **kwargs,
    ):
        # Initialize attributes before any exceptions so __del__ doesn't fail
        self.core_api = None
        self.pod_name: str | None = None
        self.broker_url: str | None = None
        self.poller_thread: threading.Thread | None = None
        self.poller_stop = threading.Event()
        self.pending_llm_calls: list[RLMChatCompletion] = []
        self._calls_lock = threading.Lock()
        self.skip_pod_creation = skip_pod_creation

        if persistent:
            raise NotImplementedError(
                "Persistent REPLs are currently not supported for environment: KubernetesREPL"
            )
        super().__init__(persistent=persistent, depth=depth, **kwargs)

        self.namespace = namespace
        self.image = image
        self.timeout = timeout
        self.lm_handler_address = lm_handler_address
        self.resource_requests = resource_requests
        self.resource_limits = resource_limits
        self.service_account = service_account
        self.pod_ip = pod_ip

        self.setup()

        if context_payload is not None:
            self.load_context(context_payload)

        if setup_code:
            self.execute_code(setup_code)

    def setup(self):
        """Create the Kubernetes pod, broker, and start polling."""
        from kubernetes import client, config
        from kubernetes.stream import stream

        # Load kubeconfig (in-cluster first, fallback to local)
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()

        self.core_api = client.CoreV1Api()

        if not self.skip_pod_creation:
            # Build resource requirements
            resources = {}
            if self.resource_requests:
                resources["requests"] = self.resource_requests
            if self.resource_limits:
                resources["limits"] = self.resource_limits

            # Create pod with tail -f /dev/null keepalive
            pod_manifest = client.V1Pod(
                metadata=client.V1ObjectMeta(
                    generate_name="rlm-worker-",
                    namespace=self.namespace,
                    labels={"app": "rlm-worker"},
                ),
                spec=client.V1PodSpec(
                    service_account_name=self.service_account,
                    containers=[
                        client.V1Container(
                            name="worker",
                            image=self.image,
                            command=["tail", "-f", "/dev/null"],
                            ports=[client.V1ContainerPort(container_port=self.BROKER_PORT)],
                            resources=client.V1ResourceRequirements(**resources)
                            if resources
                            else None,
                        )
                    ],
                    restart_policy="Never",
                ),
            )

            pod = self.core_api.create_namespaced_pod(
                namespace=self.namespace, body=pod_manifest
            )
            self.pod_name = pod.metadata.name

            # Poll until pod is Running
            for _ in range(self.timeout):
                pod_status = self.core_api.read_namespaced_pod(
                    name=self.pod_name, namespace=self.namespace
                )
                if pod_status.status.phase == "Running":
                    self.pod_ip = pod_status.status.pod_ip
                    break
                time.sleep(1)
            else:
                raise TimeoutError(
                    f"Pod {self.pod_name} did not reach Running state within {self.timeout}s"
                )

            # Install dependencies in the pod
            self._exec_in_pod("pip install dill requests flask 2>&1")

            # Write broker script to pod
            broker_b64 = base64.b64encode(_BROKER_SCRIPT.encode()).decode()
            self._exec_in_pod(
                f'python -c "import base64; open(\'/tmp/broker.py\',\'w\').write(base64.b64decode(\'{broker_b64}\').decode())"'
            )

            # Start broker as background process
            self._exec_in_pod("nohup python /tmp/broker.py > /tmp/broker.log 2>&1 &")

            # Wait for broker health check
            for _ in range(30):
                try:
                    resp = http_requests.get(
                        f"http://{self.pod_ip}:{self.BROKER_PORT}/health", timeout=2
                    )
                    if resp.status_code == 200:
                        break
                except http_requests.exceptions.RequestException:
                    pass
                time.sleep(1)
            else:
                raise TimeoutError("Broker did not become healthy within 30s")

        # Set broker URL from pod_ip (either created or pre-existing)
        if self.pod_ip:
            self.broker_url = f"http://{self.pod_ip}:{self.BROKER_PORT}"

        # Start polling thread if we have an LM handler and broker URL
        if self.lm_handler_address and self.broker_url:
            self.poller_stop.clear()
            self.poller_thread = threading.Thread(target=self._poll_broker, daemon=True)
            self.poller_thread.start()

    def _exec_in_pod(self, command: str) -> str:
        """Execute a command in the pod via kubernetes.stream and return output."""
        from kubernetes.stream import stream

        resp = stream(
            self.core_api.connect_get_namespaced_pod_exec,
            name=self.pod_name,
            namespace=self.namespace,
            command=["/bin/sh", "-c", command],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
        return resp

    def _poll_broker(self):
        """Poll the broker for pending LLM requests and handle them."""
        while not self.poller_stop.is_set():
            try:
                resp = http_requests.get(
                    f"{self.broker_url}/pending",
                    timeout=5,
                )
                pending = resp.json().get("pending", [])

                for item in pending:
                    request_id = item["id"]
                    req_data = item["request"]

                    response = self._handle_llm_request(req_data)

                    http_requests.post(
                        f"{self.broker_url}/respond",
                        json={"id": request_id, "response": response},
                        timeout=10,
                    )

            except http_requests.exceptions.RequestException:
                pass
            except Exception:
                pass

            time.sleep(0.1)

    def _handle_llm_request(self, req_data: dict) -> dict:
        """Handle an LLM request from the pod."""
        req_type = req_data.get("type")
        model = req_data.get("model")

        if req_type == "single":
            prompt = req_data.get("prompt")
            request = LMRequest(prompt=prompt, model=model, depth=self.depth)
            response = send_lm_request(self.lm_handler_address, request)

            if not response.success:
                return {"error": response.error}

            with self._calls_lock:
                self.pending_llm_calls.append(response.chat_completion)

            return {"response": response.chat_completion.response}

        elif req_type == "batched":
            prompts = req_data.get("prompts", [])
            responses = send_lm_request_batched(
                self.lm_handler_address, prompts, model=model, depth=self.depth
            )

            results = []
            for resp in responses:
                if not resp.success:
                    results.append(f"Error: {resp.error}")
                else:
                    with self._calls_lock:
                        self.pending_llm_calls.append(resp.chat_completion)
                    results.append(resp.chat_completion.response)

            return {"responses": results}

        return {"error": "Unknown request type"}

    def load_context(self, context_payload: dict | list | str):
        """Load context into the pod environment."""
        if isinstance(context_payload, str):
            escaped = context_payload.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
            context_code = f'context = """{escaped}"""'
        else:
            context_json = json.dumps(context_payload)
            escaped_json = context_json.replace("\\", "\\\\").replace("'", "\\'")
            context_code = f"import json; context = json.loads('{escaped_json}')"

        self.execute_code(context_code)

    def execute_code(self, code: str) -> REPLResult:
        """Execute code in the Kubernetes pod and return result."""
        start_time = time.perf_counter()

        with self._calls_lock:
            self.pending_llm_calls.clear()

        script = _build_exec_script(code, self.BROKER_PORT, self.depth)
        script_b64 = base64.b64encode(script.encode()).decode()

        # Execute via pod exec
        output = self._exec_in_pod(
            f'python -c "import base64; exec(base64.b64decode(\'{script_b64}\').decode())"'
        )

        with self._calls_lock:
            pending_calls = self.pending_llm_calls.copy()
            self.pending_llm_calls.clear()

        execution_time = time.perf_counter() - start_time

        # Parse the JSON result
        try:
            lines = output.strip().split("\n")
            result_json = lines[-1] if lines else "{}"
            result = json.loads(result_json)

            return REPLResult(
                stdout=result.get("stdout", ""),
                stderr=result.get("stderr", ""),
                locals=result.get("locals", {}),
                execution_time=execution_time,
                rlm_calls=pending_calls,
            )
        except json.JSONDecodeError:
            return REPLResult(
                stdout=output,
                stderr="Failed to parse execution result",
                locals={},
                execution_time=execution_time,
                rlm_calls=pending_calls,
            )

    def cleanup(self):
        """Terminate the pod and stop polling."""
        if self.poller_thread is not None:
            self.poller_stop.set()
            self.poller_thread.join(timeout=2)
            self.poller_thread = None

        if self.core_api and self.pod_name and not self.skip_pod_creation:
            try:
                self.core_api.delete_namespaced_pod(
                    name=self.pod_name, namespace=self.namespace
                )
            except Exception:
                pass
            self.pod_name = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False

    def __del__(self):
        self.cleanup()
