"""Optional, site-owned COSIDAG execution profiles (no Airflow/SDK on import).

Ported from k3s-lab 76dba4321586792da71b45bb250c06d55d590a30 and adapted
for unchanged scientific DAGs, explicit validation and local-only installs.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re

PROFILE_ENV = "COSIFLOW_TASK_EXECUTION_FILE"
_ALLOWED = {
    "executor", "identity", "primary_node", "fallback_nodes", "preferred_node",
    "node_label", "tolerations", "priority_class_name", "resources",
}


def resolve_execution_profile(dag_id: str, explicit=None, *, environ=None):
    """Explicit profile (including {}) wins; otherwise select an exact DAG ID.

    No file configured means no change to existing DAG defaults. Malformed or
    unreadable configured files fail DAG parsing rather than silently reroute.
    The deployment file contains {"dag_id": {profile}}, without wildcard rules.
    """
    if explicit is not None:
        if not isinstance(explicit, Mapping):
            raise TypeError("task_execution must be a mapping")
        return dict(explicit)
    filename = (os.environ if environ is None else environ).get(PROFILE_ENV, "")
    if not filename:
        return {}
    try:
        profiles = json.loads(Path(filename).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"Cannot read {PROFILE_ENV} as a JSON profile mapping") from exc
    if not isinstance(profiles, dict) or any(
        not isinstance(key, str) or not key or not isinstance(value, dict)
        for key, value in profiles.items()
    ):
        raise ValueError(f"{PROFILE_ENV} must map exact DAG IDs to profile objects")
    return deepcopy(profiles.get(dag_id, {}))


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"task_execution.{name} must be a non-empty string")
    return value.strip()


def normalize_profile(config):
    if config is None:
        return {}
    if not isinstance(config, Mapping):
        raise TypeError("task_execution must be a mapping")
    if not config:
        return {}
    unknown = set(config) - _ALLOWED
    if unknown:
        raise ValueError(f"Unknown task_execution options: {sorted(unknown)}")
    result = deepcopy(dict(config))
    executor = _text(result.get("executor"), "executor")
    if executor not in {"LocalExecutor", "KubernetesExecutor"}:
        raise ValueError("task_execution.executor must be LocalExecutor or KubernetesExecutor")
    result["executor"] = executor
    if executor == "LocalExecutor":
        if set(result) - {"executor"}:
            raise ValueError("Kubernetes placement options cannot be used with LocalExecutor")
        return result
    if "identity" in result:
        result["identity"] = _text(result["identity"], "identity")
    primary = result.get("primary_node")
    fallbacks = result.get("fallback_nodes", [])
    if not isinstance(fallbacks, (list, tuple)):
        raise ValueError("task_execution.fallback_nodes must be a list of node labels")
    fallbacks = [_text(node, "fallback_nodes") for node in fallbacks]
    if not primary and (fallbacks or "preferred_node" in result or "node_label" in result):
        raise ValueError("task_execution.primary_node is required for node preferences")
    if primary is not None:
        primary = _text(primary, "primary_node")
        result["primary_node"] = primary
        result["fallback_nodes"] = list(dict.fromkeys(n for n in fallbacks if n != primary))
        result["preferred_node"] = _text(result.get("preferred_node", primary), "preferred_node")
        if result["preferred_node"] not in [primary, *result["fallback_nodes"]]:
            raise ValueError("preferred_node must belong to the authorized node pool")
        result["node_label"] = _text(result.get("node_label", "kubernetes.io/hostname"), "node_label")
    if "priority_class_name" in result:
        result["priority_class_name"] = _text(result["priority_class_name"], "priority_class_name")
    tolerations = result.get("tolerations", [])
    if not isinstance(tolerations, (list, tuple)):
        raise ValueError("task_execution.tolerations must be a list")
    result["tolerations"] = []
    for item in tolerations:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        if not isinstance(item, Mapping):
            raise ValueError("Each toleration must be an object")
        item = {k: v for k, v in item.items() if v is not None}
        if "toleration_seconds" in item:
            item["tolerationSeconds"] = item.pop("toleration_seconds")
        if set(item) - {"key", "operator", "value", "effect", "tolerationSeconds"}:
            raise ValueError("Unknown toleration field")
        if item.get("operator", "Equal") not in {"Equal", "Exists"}:
            raise ValueError("Invalid toleration operator")
        if item.get("effect") not in {None, "NoSchedule", "PreferNoSchedule", "NoExecute"}:
            raise ValueError("Invalid toleration effect")
        if "tolerationSeconds" in item and (
            type(item["tolerationSeconds"]) is not int or item["tolerationSeconds"] < 0
        ):
            raise ValueError("tolerationSeconds must be a non-negative integer")
        result["tolerations"].append(dict(item))
    resources = result.get("resources", {})
    if not isinstance(resources, Mapping) or set(resources) - {"requests", "limits"}:
        raise ValueError("resources must contain requests and/or limits")
    for values in resources.values():
        if not isinstance(values, Mapping) or any(
            not isinstance(k, str) or not isinstance(v, (str, int, float)) or isinstance(v, bool)
            for k, v in values.items()
        ):
            raise ValueError("Resource quantities must be strings or numbers")
    return result


def _label(value):
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]", "-", value).strip("-_.") or "cosidag"
    if len(cleaned) > 63:
        cleaned = cleaned[:50].rstrip("-_.") + "-" + hashlib.sha256(value.encode()).hexdigest()[:12]
    return cleaned


def pod_override_spec(dag_id, config):
    """Return a JSON-serializable pod override; config must be normalized."""
    identity = config.get("identity", dag_id)
    env = [
        {"name": "COSI_IDENTITY", "value": identity},
        {"name": "COSI_DAG_ID", "value": dag_id},
    ]
    for name, field in [("NODE_NAME", "spec.nodeName"), ("NODE_IP", "status.hostIP"),
                        ("POD_NAME", "metadata.name"), ("POD_IP", "status.podIP")]:
        env.append({"name": f"COSI_{name}", "valueFrom": {"fieldRef": {"fieldPath": field}}})
    container = {"name": "base", "env": env}
    if config.get("resources"):
        container["resources"] = config["resources"]
    spec = {"containers": [container]}
    if config.get("primary_node"):
        allowed = [config["primary_node"], *config["fallback_nodes"]]
        expression = {"key": config["node_label"], "operator": "In", "values": allowed}
        spec["affinity"] = {"nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{"matchExpressions": [expression]}]},
            "preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 100, "preference": {
                "matchExpressions": [{**expression, "values": [config["preferred_node"]]}]}}],
        }}
    # Keep the lab's short eviction policy explicit in the site profile.
    # Kubernetes defaults apply unless the administrator supplies tolerations.
    if config.get("tolerations"):
        spec["tolerations"] = config["tolerations"]
    if config.get("priority_class_name"):
        spec["priorityClassName"] = config["priority_class_name"]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {
        "labels": {"app.kubernetes.io/part-of": "cosiflow", "cosiflow.io/component": "airflow-task-runner",
                   "cosiflow.io/pipeline": _label(identity)},
        "annotations": {"cosiflow.io/dag-id": dag_id},
    }, "spec": spec}


def execution_defaults(dag_id, config, defaults=None):
    """Apply optional execution defaults without dropping existing settings.

    Per-task operator arguments still override DAG defaults through Airflow.
    Two competing DAG-level pod overrides are rejected rather than silently
    overwriting affinity/volumes. Kubernetes is imported only for a K8s profile.
    """
    config = normalize_profile(config)
    result = dict(defaults or {})
    if not config:
        return result
    result["executor"] = config["executor"]
    if config["executor"] == "LocalExecutor":
        if (result.get("executor_config") or {}).get("pod_override") is not None:
            raise ValueError("LocalExecutor profile conflicts with inherited Kubernetes pod_override")
        return result
    executor_config = dict(result.get("executor_config") or {})
    if "pod_override" in executor_config:
        raise ValueError("task_execution conflicts with default_args.executor_config.pod_override; use one DAG-level owner")
    from kubernetes.client import ApiClient

    # The SDK model is required by Airflow's KubernetesExecutor serializer.
    pod = ApiClient()._ApiClient__deserialize(pod_override_spec(dag_id, config), "V1Pod")
    executor_config["pod_override"] = pod
    result["executor_config"] = executor_config
    return result
