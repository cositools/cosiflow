"""Execution profile contracts; these do not start Airflow or a cluster."""
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "modules"))
from cosidag_execution import (  # noqa: E402
    execution_defaults, normalize_profile, pod_override_spec, resolve_execution_profile,
)


class ExecutionProfileTests(unittest.TestCase):
    def test_local_path_has_no_sdk_dependency_or_side_effect_on_defaults(self):
        original = {"retries": 3, "executor_config": {"custom": "keep"}}
        self.assertEqual(execution_defaults("science", {}, original), original)
        self.assertEqual(execution_defaults("science", {"executor": "LocalExecutor"}, original),
                         {**original, "executor": "LocalExecutor"})
        self.assertNotIn("executor", original)

    def test_explicit_empty_profile_disables_site_routing(self):
        self.assertEqual(resolve_execution_profile("science", {}, environ={"COSIFLOW_TASK_EXECUTION_FILE": "/missing"}), {})

    def test_site_routing_is_exact_and_does_not_require_a_dag_change(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "routing.json"
            path.write_text(json.dumps({"cosidag_BGO": {"executor": "KubernetesExecutor", "primary_node": "node-a"}}))
            env = {"COSIFLOW_TASK_EXECUTION_FILE": str(path)}
            self.assertEqual(resolve_execution_profile("cosidag_BGO", environ=env)["primary_node"], "node-a")
            self.assertEqual(resolve_execution_profile("init_pipelines", environ=env), {})
            self.assertEqual(resolve_execution_profile("cosidag_GeD", environ=env), {})
            self.assertEqual(resolve_execution_profile("cosidag_BGO", {"executor": "LocalExecutor"}, environ=env), {"executor": "LocalExecutor"})

    def test_missing_or_malformed_config_fails_instead_of_rerouting(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "routing.json"
            for data in (None, "[1]", '{"science": []}', "{broken"):
                if data is not None:
                    path.write_text(data)
                with self.subTest(data=data), self.assertRaises(ValueError):
                    resolve_execution_profile("science", environ={"COSIFLOW_TASK_EXECUTION_FILE": str(path)})

    def test_invalid_profiles_are_rejected_before_importing_kubernetes(self):
        profiles = [False, [], {"executor": "typo"}, {"executor": "KubernetesExecutor", "typo": 1},
                    {"executor": "LocalExecutor", "primary_node": "node-a"},
                    {"executor": "KubernetesExecutor", "fallback_nodes": ["node-a"]},
                    {"executor": "KubernetesExecutor", "primary_node": "a", "preferred_node": "b"},
                    {"executor": "KubernetesExecutor", "primary_node": "a", "fallback_nodes": "abc"},
                    {"executor": "KubernetesExecutor", "tolerations": [{"tolerationSeconds": -1}]},
                    {"executor": "KubernetesExecutor", "resources": {"requests": []}}]
        for config in profiles:
            with self.subTest(config=config), self.assertRaises((ValueError, TypeError)):
                normalize_profile(config)

    def test_affinity_preserves_authorized_pool_and_soft_preference(self):
        raw = {"executor": "KubernetesExecutor", "primary_node": "a", "fallback_nodes": ["a", "b", "b"],
               "preferred_node": "b", "node_label": "cosiflow.io/pool"}
        before = deepcopy(raw)
        profile = normalize_profile(raw)
        pod = pod_override_spec("cosidag_BGO", profile)
        node = pod["spec"]["affinity"]["nodeAffinity"]
        required = node["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"][0]
        self.assertEqual(required, {"key": "cosiflow.io/pool", "operator": "In", "values": ["a", "b"]})
        self.assertEqual(node["preferredDuringSchedulingIgnoredDuringExecution"][0]["preference"]["matchExpressions"][0]["values"], ["b"])
        self.assertEqual(raw, before)

    def test_metadata_does_not_assume_jinja_is_rendered_by_executor(self):
        pod = pod_override_spec("science." + "long" * 30, normalize_profile({"executor": "KubernetesExecutor"}))
        self.assertNotIn("{{", json.dumps(pod))
        self.assertLessEqual(len(pod["metadata"]["labels"]["cosiflow.io/pipeline"]), 63)
        fields = {e["name"]: e.get("valueFrom") for e in pod["spec"]["containers"][0]["env"]}
        self.assertEqual(fields["COSI_NODE_NAME"]["fieldRef"]["fieldPath"], "spec.nodeName")
        self.assertNotIn("affinity", pod["spec"])

    def test_site_controls_resources_tolerations_and_priority(self):
        profile = normalize_profile({"executor": "KubernetesExecutor", "resources": {"requests": {"memory": "2Gi"}},
            "priority_class_name": "science", "tolerations": [{"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 10}]})
        pod = pod_override_spec("science", profile)
        self.assertEqual(pod["spec"]["containers"][0]["resources"]["requests"]["memory"], "2Gi")
        self.assertEqual(pod["spec"]["priorityClassName"], "science")
        self.assertEqual(pod["spec"]["tolerations"][0]["tolerationSeconds"], 10)

    def test_competing_dag_level_pod_overrides_are_not_silently_lost(self):
        defaults = {"executor_config": {"pod_override": "existing"}}
        for executor in ("LocalExecutor", "KubernetesExecutor"):
            with self.subTest(executor=executor), self.assertRaisesRegex(ValueError, "conflict"):
                execution_defaults("science", {"executor": executor}, defaults)
        self.assertEqual(defaults["executor_config"]["pod_override"], "existing")

    @unittest.skipUnless(importlib.util.find_spec("kubernetes"), "Kubernetes SDK not installed")
    def test_sdk_override_preserves_other_defaults(self):
        from kubernetes.client import V1Pod
        output = execution_defaults("science", {"executor": "KubernetesExecutor"}, {"retries": 4, "executor_config": {"other": 1}})
        self.assertIsInstance(output["executor_config"]["pod_override"], V1Pod)
        self.assertEqual(output["executor_config"]["other"], 1)
        self.assertEqual(output["retries"], 4)


try:
    from datetime import datetime
    import cosidag
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    cosidag = None


@unittest.skipIf(cosidag is None, "Airflow not installed; graph acceptance is pending")
class ExecutionGraphTests(unittest.TestCase):
    def test_site_profile_routes_unmodified_cosidag_and_task_override_wins(self):
        def build_custom(dag):
            EmptyOperator(task_id="inherited", dag=dag)
            EmptyOperator(task_id="explicit", executor="LocalExecutor", dag=dag)
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "profiles.json"
            path.write_text(json.dumps({"science": {"executor": "KubernetesExecutor"}}))
            from unittest.mock import patch
            with patch.dict(os.environ, {"COSIFLOW_TASK_EXECUTION_FILE": str(path)}):
                dag = cosidag.COSIDAG(dag_id="science", monitoring_folders=[], start_date=datetime(2026, 1, 1),
                                     schedule_interval=None, auto_retrig=False, build_custom=build_custom)
            self.assertEqual(dag.get_task("inherited").executor, "KubernetesExecutor")
            self.assertEqual(dag.get_task("explicit").executor, "LocalExecutor")
