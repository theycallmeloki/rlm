"""
Example: Using RLM with the Kubernetes environment.

This example shows standalone usage of the KubernetesREPL environment
(no Metacontroller required). It creates a pod in your current kubeconfig
context and runs the RLM completion loop.

Prerequisites:
    - kubectl configured with access to a cluster
    - pip install rlm[kubernetes]

Usage:
    export OPENAI_API_KEY=sk-...
    python examples/kubernetes_repl_example.py
"""

from rlm.core.rlm import RLM

# Basic usage: RLM with Kubernetes environment
rlm = RLM(
    backend="openai",
    backend_kwargs={"model_name": "gpt-4o"},
    environment="kubernetes",
    environment_kwargs={
        "namespace": "default",
        "image": "python:3.11-slim",
        "timeout": 300,
    },
    max_iterations=30,
)

result = rlm.completion(
    "Solve: What is the integral of x^2 * sin(x) dx? "
    "Use sympy to verify your answer."
)

print(f"Result: {result.response}")
print(f"Execution time: {result.execution_time:.2f}s")
