#!/usr/bin/env python3
"""Render optional COSIflow application manifests. Never contacts a cluster.

JSON is accepted by kubectl and is also valid YAML for pod_template.yaml.
Existing PVC, PostgreSQL and a site-managed Secret are required. No VM names,
Mac bind mounts, cluster provisioning or credentials are embedded here.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "modules"))
from cosidag_execution import normalize_profile  # noqa: E402

MOUNTS = {
    "envs": "/home/gamma/envs", "modules-pool": "/home/gamma/airflow/modules_pool",
    "dags": "/home/gamma/airflow/dags", "pipeline": "/home/gamma/airflow/pipeline",
    "data": "/home/gamma/workspace/data", "logs": "/home/gamma/airflow/logs",
}


def validate_site(site):
    if not isinstance(site, dict):
        raise ValueError("site must be an object")
    allowed = {
        "namespace", "pvc", "secret", "image", "image_pull_policy", "image_pull_secrets", "uid", "gid",
        "scheduler_replicas", "postgres_host", "postgres_port", "postgres_user", "postgres_db", "public_base_url",
        "smtp_host", "smtp_port", "gcn_db_host", "gcn_db_name", "gcn_db_user", "component_node_selector",
        "worker_resources", "scheduler_resources", "webserver_resources", "task_profiles",
    }
    if set(site) - allowed:
        raise ValueError(f"Unknown site options: {sorted(set(site) - allowed)}")
    for name in ("namespace", "pvc", "secret"):
        if not isinstance(site.get(name), str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", site[name]):
            raise ValueError(f"Invalid Kubernetes name: {name}")
    if len(site["namespace"]) > 63 or "." in site["namespace"]:
        raise ValueError("namespace must be a DNS label of at most 63 characters")
    for name in ("image", "postgres_host", "postgres_user", "postgres_db", "public_base_url", "smtp_host", "gcn_db_host", "gcn_db_name", "gcn_db_user"):
        if not isinstance(site.get(name), str) or not site[name].strip():
            raise ValueError(f"{name} is required")
    if site.get("image_pull_policy", "IfNotPresent") not in {"Always", "IfNotPresent", "Never"}:
        raise ValueError("Invalid image_pull_policy")
    for name in ("uid", "gid"):
        if type(site.get(name)) is not int or site[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if type(site.get("scheduler_replicas", 1)) is not int or site.get("scheduler_replicas", 1) != 1:
        raise ValueError("This unvalidated application profile supports one scheduler; HA/reaper requires separate acceptance")
    for name in ("postgres_port", "smtp_port"):
        if not str(site.get(name, "")).isdigit() or not 1 <= int(site[name]) <= 65535:
            raise ValueError(f"Invalid {name}")
    if not isinstance(site.get("component_node_selector", {}), dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in site.get("component_node_selector", {}).items()
    ):
        raise ValueError("component_node_selector must map labels to string values")
    pull_secrets = site.get("image_pull_secrets", [])
    if not isinstance(pull_secrets, list) or any(not isinstance(n, str) or not n for n in pull_secrets):
        raise ValueError("image_pull_secrets must be a list of Secret names")
    for name in ("worker_resources", "scheduler_resources", "webserver_resources"):
        normalize_profile({"executor": "KubernetesExecutor", "resources": site.get(name, {})})
    profiles = site.get("task_profiles", {})
    if not isinstance(profiles, dict) or any(not isinstance(k, str) or not k for k in profiles):
        raise ValueError("task_profiles must map exact DAG IDs to profiles")
    for profile in profiles.values():
        normalize_profile(profile)


def render(site):
    validate_site(site)
    ns = site["namespace"]
    def resource(api, kind, name, **fields):
        return {"apiVersion": api, "kind": kind, "metadata": {"name": name, "namespace": ns}, **fields}
    env = {
        "AIRFLOW_HOME": "/home/gamma/airflow", "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor,KubernetesExecutor",
        "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE": ns,
        "AIRFLOW__KUBERNETES_EXECUTOR__IN_CLUSTER": "True",
        "AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE": "/opt/cosiflow/pod_template.yaml",
        "AIRFLOW__KUBERNETES_EXECUTOR__DELETE_WORKER_PODS": "True",
        "COSIFLOW_TASK_EXECUTION_FILE": "/opt/cosiflow/task-execution.json",
        "PYTHONPATH": "/home/gamma/airflow/modules:/home/gamma/airflow/callbacks",
        "POSTGRES_HOST": site["postgres_host"], "POSTGRES_PORT": str(site["postgres_port"]),
        "POSTGRES_USER": site["postgres_user"], "POSTGRES_DB": site["postgres_db"],
        "COSI_DATA_DIR": "/home/gamma/workspace/data", "AIRFLOW_WEBUI_PORT": "8080",
        "MAILHOG_WEBUI_PORT": "8025", "AIRFLOW_PUBLIC_BASE_URL": site["public_base_url"],
        "AIRFLOW__WEBSERVER__BASE_URL": site["public_base_url"],
        "COSIFLOW_HOME_URL": site["public_base_url"].rstrip("/") + "/heasarcbrowser",
        "AIRFLOW__SMTP__SMTP_HOST": site["smtp_host"], "AIRFLOW__SMTP__SMTP_PORT": str(site["smtp_port"]),
        "GCN_DB_HOST": site["gcn_db_host"], "GCN_DB_PORT": "3306",
        "GCN_DB_NAME": site["gcn_db_name"], "GCN_DB_USER": site["gcn_db_user"],
        "GCN_DRY_RUN": "true", "GCN_PRODUCER_ENABLED": "false",
    }
    def pod(name, mode, *, installer=False, worker=False):
        container = {"name": "base", "image": site["image"],
            "imagePullPolicy": site.get("image_pull_policy", "IfNotPresent"),
            "envFrom": [{"configMapRef": {"name": "cosiflow-runtime"}}],
            "volumeMounts": [{"name": "shared", "mountPath": target, "subPath": sub,
                              "readOnly": sub in {"modules-pool", "envs", "dags", "pipeline"} and not installer}
                             for sub, target in MOUNTS.items()] + [{"name": "config", "mountPath": "/opt/cosiflow", "readOnly": True}],
        }
        common_secrets = ["POSTGRES_PASSWORD", "AIRFLOW__CORE__FERNET_KEY",
            "AIRFLOW__WEBSERVER__SECRET_KEY", "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY"]
        secret_names = [] if installer else common_secrets + (
            ["AIRFLOW_ADMIN_PASSWORD", "AIRFLOW_ADMIN_USERNAME", "AIRFLOW_ADMIN_EMAIL"]
            if mode == "init" else ["GCN_DB_PASSWORD"])
        container["env"] = [{"name": name, "valueFrom": {"secretKeyRef": {"name": site["secret"], "key": name}}}
                            for name in secret_names]
        if worker:
            container["command"] = ["bash", "/home/gamma/kubernetes-worker.sh"]
            container["args"] = []  # KubernetesExecutor replaces these.
        elif installer:
            container["command"] = ["bash", "/home/gamma/hot_load_module.sh"]
            container["args"] = ["fast-transient-analysis-pipeline", "install", "-e"]
            container["env"] = [{"name": "COSIFLOW_LOADER_LOCAL", "value": "true"},
                {"name": "COSIFLOW_MODULES_HOST_DIR", "value": MOUNTS["modules-pool"]}]
            # Source is read-only even for the installation Job.
            container["volumeMounts"][1]["readOnly"] = True
        else:
            container["command"] = ["bash", "/home/gamma/entrypoint-airflow.sh"]
            container["args"] = [mode]
        resources = site.get(f"{'worker' if worker else mode}_resources")
        if resources:
            container["resources"] = deepcopy(resources)
        spec = {"serviceAccountName": "cosiflow", "restartPolicy": "Never" if worker or installer or mode == "init" else "Always",
            "securityContext": {"runAsUser": site["uid"], "runAsGroup": site["gid"], "fsGroup": site["gid"]},
            "containers": [container], "volumes": [{"name": "shared", "persistentVolumeClaim": {"claimName": site["pvc"]}},
                {"name": "config", "configMap": {"name": "cosiflow-files"}}]}
        if site.get("image_pull_secrets"):
            spec["imagePullSecrets"] = [{"name": name} for name in site["image_pull_secrets"]]
        if not worker and site.get("component_node_selector"):
            spec["nodeSelector"] = site["component_node_selector"]
        return resource("v1", "Pod", name, spec=spec)
    template = pod("cosiflow-worker", "worker", worker=True)
    files = {"pod_template.yaml": json.dumps(template), "task-execution.json": json.dumps(site.get("task_profiles", {}))}
    bootstrap = [resource("v1", "ServiceAccount", "cosiflow"), resource("rbac.authorization.k8s.io/v1", "Role", "cosiflow-worker", rules=[
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "get", "list", "watch", "delete", "patch"]},
        {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get", "list"]},
    ]), resource("rbac.authorization.k8s.io/v1", "RoleBinding", "cosiflow-worker",
        subjects=[{"kind": "ServiceAccount", "name": "cosiflow", "namespace": ns}],
        roleRef={"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "cosiflow-worker"}),
        resource("v1", "ConfigMap", "cosiflow-runtime", data=env), resource("v1", "ConfigMap", "cosiflow-files", data=files)]
    def job(name, *, installer=False):
        p = pod(name, "init", installer=installer)
        return resource("batch/v1", "Job", name, spec={"backoffLimit": 0, "template": {"spec": p["spec"]}})
    runtime = []
    for mode in ("scheduler", "webserver"):
        p = pod(f"cosiflow-{mode}", mode)
        labels = {"app.kubernetes.io/name": f"cosiflow-{mode}"}
        runtime.append(resource("apps/v1", "Deployment", f"cosiflow-{mode}", spec={"replicas": 1,
            "strategy": {"type": "Recreate"}, "selector": {"matchLabels": labels},
            "template": {"metadata": {"labels": labels}, "spec": p["spec"]}}))
    runtime.append(resource("v1", "Service", "cosiflow-webserver", spec={"selector": {"app.kubernetes.io/name": "cosiflow-webserver"},
        "ports": [{"name": "http", "port": 8080, "targetPort": 8080}], "type": "ClusterIP"}))
    return {"bootstrap.json": bootstrap, "init.json": [job("cosiflow-init")],
            "module-install.json": [job("cosiflow-install-fastp", installer=True)], "runtime.json": runtime}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifacts = render(json.loads(args.site.read_text(encoding="utf-8")))
    # Validate everything before creating any output; never overwrite a bundle.
    args.output.mkdir(parents=True, exist_ok=False)
    for name, items in artifacts.items():
        (args.output / name).write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=2) + "\n")


if __name__ == "__main__":
    main()
