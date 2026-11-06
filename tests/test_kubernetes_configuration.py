"""Offline application bundle rendering; no kubectl/apply or live services."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deployment/kubernetes/render.py"
spec = importlib.util.spec_from_file_location("cluster_renderer", SCRIPT)
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


class KubernetesConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.site = json.loads((SCRIPT.parent / "site.example.json").read_text())

    def test_site_changes_are_propagated_to_all_artifacts(self):
        self.site.update(namespace="site-b", image="example.test/worker@sha256:abc", pvc="shared-b", secret="runtime-b")
        result = renderer.render(self.site)
        self.assertEqual(set(result), {"bootstrap.json", "init.json", "module-install.json", "runtime.json"})
        for objects in result.values():
            for item in objects:
                self.assertEqual(item["metadata"]["namespace"], "site-b")
        serialized = json.dumps(result)
        self.assertNotIn("/mnt/", serialized)
        self.assertNotIn("/Users/", serialized)
        self.assertNotIn('"hostPath"', serialized)
        self.assertNotIn('"imagePullPolicy": "Never"', serialized)
        self.assertIn('"claimName": "shared-b"', serialized)
        self.assertIn('example.test/worker@sha256:abc', serialized)

    def test_existing_fastp_dag_ids_are_routed_and_init_remains_default(self):
        result = renderer.render(self.site)
        config = next(item for item in result["bootstrap.json"] if item["metadata"]["name"] == "cosiflow-files")
        profiles = json.loads(config["data"]["task-execution.json"])
        self.assertEqual(set(profiles), {"cosidag_BGO", "cosidag_GeD"})
        pod = json.loads(config["data"]["pod_template.yaml"])
        self.assertEqual(pod["spec"]["containers"][0]["name"], "base")
        self.assertEqual(pod["spec"]["containers"][0]["command"], ["bash", "/home/gamma/kubernetes-worker.sh"])
        runtime = next(item for item in result["bootstrap.json"] if item["metadata"]["name"] == "cosiflow-runtime")
        self.assertEqual(runtime["data"]["AIRFLOW__CORE__EXECUTOR"], "LocalExecutor,KubernetesExecutor")

    def test_source_and_interpreters_have_identical_paths_for_all_tasks(self):
        bundle = renderer.render(self.site)
        config = next(item for item in bundle["bootstrap.json"] if item["metadata"]["name"] == "cosiflow-files")
        worker = json.loads(config["data"]["pod_template.yaml"])
        scheduler = bundle["runtime.json"][0]["spec"]["template"]
        self.assertEqual(worker["spec"]["containers"][0]["volumeMounts"], scheduler["spec"]["containers"][0]["volumeMounts"])
        installer = bundle["module-install.json"][0]["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(installer["args"], ["fast-transient-analysis-pipeline", "install", "-e"])
        self.assertTrue(next(m for m in installer["volumeMounts"] if m.get("subPath") == "modules-pool")["readOnly"])
        self.assertFalse(next(m for m in installer["volumeMounts"] if m.get("subPath") == "envs")["readOnly"])

    def test_invalid_site_fails_before_writing(self):
        for changes in ({"uid": 0}, {"namespace": "Bad Site"}, {"scheduler_replicas": 2}, {"postgres_port": "x"},
                        {"task_profiles": {"science": {"executor": "LocalExecutor", "primary_node": "a"}}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                renderer.render({**self.site, **changes})

    def test_render_does_not_mutate_site_or_include_secret_values(self):
        original = deepcopy(self.site)
        bundle = renderer.render(self.site)
        self.assertEqual(self.site, original)
        self.assertNotIn('"kind": "Secret"', json.dumps(bundle))
        # Only Secret key references are rendered, never their values.
        self.assertIn('"secretKeyRef"', json.dumps(bundle))
        worker_files = next(item for item in bundle["bootstrap.json"] if item["metadata"]["name"] == "cosiflow-files")
        worker = json.loads(worker_files["data"]["pod_template.yaml"])
        worker_keys = {e["name"] for e in worker["spec"]["containers"][0]["env"]}
        self.assertNotIn("AIRFLOW_ADMIN_PASSWORD", worker_keys)
        init_keys = {e["name"] for e in bundle["init.json"][0]["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertIn("AIRFLOW_ADMIN_PASSWORD", init_keys)
        self.assertNotIn("GCN_DB_PASSWORD", init_keys)

    def test_cli_writes_a_new_bundle_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "output"
            command = [sys.executable, str(SCRIPT), str(SCRIPT.parent / "site.example.json"), "--output", str(out)]
            first = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            for file in out.glob("*.json"):
                self.assertEqual(json.loads(file.read_text())["kind"], "List")
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
