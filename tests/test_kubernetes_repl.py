"""
Unit tests for KubernetesREPL environment.

These tests mock the kubernetes client so no real cluster is needed.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_k8s_setup():
    """Create mock kubernetes objects for a standard setup."""
    mock_pod = MagicMock()
    mock_pod.metadata.name = "rlm-worker-test"
    mock_pod.status.phase = "Running"
    mock_pod.status.pod_ip = "10.0.0.5"

    mock_core_api = MagicMock()
    mock_core_api.create_namespaced_pod.return_value = mock_pod
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    return mock_core_api, mock_pod


class TestKubernetesREPLInit:
    """Test KubernetesREPL initialization."""

    def test_init_creates_pod(self):
        """Test that init creates a pod and starts the broker."""
        mock_core_api, mock_pod = _make_mock_k8s_setup()

        with (
            patch("rlm.environments.kubernetes_repl.http_requests") as mock_requests,
        ):
            mock_health = MagicMock()
            mock_health.status_code = 200
            mock_health.json.return_value = {"status": "ok"}
            mock_requests.get.return_value = mock_health

            from kubernetes import client, config

            with (
                patch.object(config, "load_incluster_config", side_effect=Exception),
                patch.object(config, "load_kube_config"),
                patch.object(client, "CoreV1Api", return_value=mock_core_api),
                patch("kubernetes.stream.stream", return_value=""),
            ):
                from rlm.environments.kubernetes_repl import KubernetesREPL

                repl = KubernetesREPL(
                    namespace="test-ns",
                    image="python:3.11-slim",
                )

                assert repl.pod_name == "rlm-worker-test"
                assert repl.pod_ip == "10.0.0.5"
                assert repl.broker_url == "http://10.0.0.5:8080"
                mock_core_api.create_namespaced_pod.assert_called_once()

                repl.cleanup()

    def test_init_persistent_raises(self):
        """Test that persistent=True raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Persistent REPLs"):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            KubernetesREPL(persistent=True, skip_pod_creation=True)

    def test_init_skip_pod_creation(self):
        """Test skip_pod_creation mode for Metacontroller use."""
        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.99",
                skip_pod_creation=True,
            )

            assert repl.pod_ip == "10.0.0.99"
            assert repl.broker_url == "http://10.0.0.99:8080"
            assert repl.pod_name is None

            repl.cleanup()


class TestKubernetesREPLExecute:
    """Test code execution in KubernetesREPL."""

    def test_execute_code_parses_result(self):
        """Test that execute_code parses JSON output correctly."""
        result_json = json.dumps({
            "stdout": "hello world\n",
            "stderr": "",
            "locals": {"x": "42"},
        })

        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
            patch("kubernetes.stream.stream", return_value=result_json),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.5",
                skip_pod_creation=True,
            )
            repl.pod_name = "test-pod"

            result = repl.execute_code("x = 42\nprint('hello world')")

            assert result.stdout == "hello world\n"
            assert result.stderr == ""
            assert result.locals == {"x": "42"}

            repl.cleanup()

    def test_execute_code_handles_parse_error(self):
        """Test that execute_code handles non-JSON output gracefully."""
        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
            patch("kubernetes.stream.stream", return_value="not json"),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.5",
                skip_pod_creation=True,
            )
            repl.pod_name = "test-pod"

            result = repl.execute_code("print('hi')")

            assert result.stdout == "not json"
            assert "Failed to parse" in result.stderr

            repl.cleanup()


class TestKubernetesREPLCleanup:
    """Test cleanup behavior."""

    def test_cleanup_deletes_pod(self):
        """Test that cleanup deletes the pod when not skipping creation."""
        mock_core_api, mock_pod = _make_mock_k8s_setup()
        mock_pod.metadata.name = "rlm-worker-cleanup"
        mock_pod.status.pod_ip = "10.0.0.6"

        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests") as mock_requests,
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api", return_value=mock_core_api),
            patch("kubernetes.stream.stream", return_value=""),
        ):
            mock_health = MagicMock()
            mock_health.status_code = 200
            mock_health.json.return_value = {"status": "ok"}
            mock_requests.get.return_value = mock_health

            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(namespace="default")
            repl.cleanup()

            mock_core_api.delete_namespaced_pod.assert_called_once_with(
                name="rlm-worker-cleanup", namespace="default"
            )

    def test_cleanup_skip_pod_creation_no_delete(self):
        """Test that cleanup doesn't delete pod when skip_pod_creation=True."""
        from kubernetes import client, config

        mock_core_api = MagicMock()

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api", return_value=mock_core_api),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.7",
                skip_pod_creation=True,
            )
            repl.cleanup()

            mock_core_api.delete_namespaced_pod.assert_not_called()


class TestKubernetesREPLLoadContext:
    """Test context loading."""

    def test_load_context_string(self):
        """Test loading string context."""
        result_json = json.dumps({"stdout": "", "stderr": "", "locals": {"context": "'hello'"}})

        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
            patch("kubernetes.stream.stream", return_value=result_json),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.5",
                skip_pod_creation=True,
            )
            repl.pod_name = "test-pod"

            # Should not raise
            repl.load_context("hello world")

            repl.cleanup()

    def test_load_context_dict(self):
        """Test loading dict context."""
        result_json = json.dumps({"stdout": "", "stderr": "", "locals": {}})

        from kubernetes import client, config

        with (
            patch("rlm.environments.kubernetes_repl.http_requests"),
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
            patch("kubernetes.stream.stream", return_value=result_json),
        ):
            from rlm.environments.kubernetes_repl import KubernetesREPL

            repl = KubernetesREPL(
                pod_ip="10.0.0.5",
                skip_pod_creation=True,
            )
            repl.pod_name = "test-pod"

            repl.load_context({"key": "value"})

            repl.cleanup()


class TestEnvironmentRouting:
    """Test that the kubernetes environment is properly routed."""

    def test_environment_type_includes_kubernetes(self):
        """Test that EnvironmentType includes 'kubernetes'."""
        from rlm.core.types import EnvironmentType

        assert "kubernetes" in EnvironmentType.__args__

    def test_get_environment_routes_kubernetes(self):
        """Test that get_environment routes to KubernetesREPL."""
        from kubernetes import client, config

        with (
            patch.object(config, "load_incluster_config", side_effect=Exception),
            patch.object(config, "load_kube_config"),
            patch.object(client, "CoreV1Api"),
            patch("rlm.environments.kubernetes_repl.http_requests"),
        ):
            from rlm.environments import get_environment

            env = get_environment(
                "kubernetes",
                {"pod_ip": "10.0.0.1", "skip_pod_creation": True},
            )

            from rlm.environments.kubernetes_repl import KubernetesREPL

            assert isinstance(env, KubernetesREPL)
            env.cleanup()
