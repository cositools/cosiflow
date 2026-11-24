"""Drive the documented bounded GeD acceptance and save evidence.

This script is the top-level acceptance test runner.  It orchestrates the
full end-to-end test by:

1. Locating the Airflow scheduler container (Docker or Kubernetes).
2. Uploading airflow-check.py into the container via stdin and running it
   there for each action (preflight, trigger, inspect, verify).
3. Triggering the two key DAGs in order:
   - init_pipelines  — sets up the science data products directory for GeD/DC4
   - cosidag_GeD     — runs the gamma-ray burst detection pipeline
4. Polling each DAG until it succeeds or times out (2 hours).
5. For Kubernetes: verifying that all task worker pods ran on cosi-1.
6. Recording every intermediate and final result as JSON files in the
   evidence directory so the run can be audited or resumed with --resume.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("mode", choices=("compose", "kubernetes"))
# Directory where all evidence JSON files will be written.
parser.add_argument("evidence", type=Path)
parser.add_argument("--resume", action="store_true",
                    help="Monitor runs already recorded in this directory")
args = parser.parse_args()
if args.resume:
    if not args.evidence.is_dir():
        parser.error("--resume requires the existing evidence directory")
else:
    # Fresh run: fail fast if the directory already exists to avoid mixing evidence.
    args.evidence.mkdir(parents=True, exist_ok=False)

# Build the base command used to exec airflow-check.py inside the scheduler.
# For Compose: docker exec into the scheduler container by its Docker ID.
# For Kubernetes: kubectl exec into the scheduler deployment.
if args.mode == "compose":
    scheduler = subprocess.check_output([
        "docker", "ps", "-q",
        "--filter", "label=com.docker.compose.project=cosiflow-compose-retest",
        "--filter", "label=com.docker.compose.service=airflow-scheduler",
    ], text=True).strip()
    assert scheduler and "\n" not in scheduler
    command = ["docker", "exec", "-i", scheduler]
    # Compose uses LocalExecutor; Kubernetes supports both.
    executor = "LocalExecutor"
else:
    k = ["kubectl", "-n", "cosiflow-distributed-test"]
    command = k + ["exec", "-i", "deployment/cosiflow-scheduler", "--"]
    executor = "KubernetesExecutor,LocalExecutor"

# Read airflow-check.py once; it will be piped into every container exec call.
script = Path(__file__).with_name("airflow-check.py").read_text()

# Timestamp used in all run IDs created by this invocation.
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def invoke(action, dag_id="", run_id="", **payload):
    """Run one airflow-check.py action inside the scheduler container.

    Returns the parsed ACCEPTANCE_JSON dict printed by the script.
    For Kubernetes, retries up to 5 times to tolerate transient exec failures
    (e.g. pod restarts during the DAG import phase).
    """
    cmd = command + ["/home/gamma/venv/bin/python", "-", action, dag_id, run_id, json.dumps(payload)]
    attempts = 5 if args.mode == "kubernetes" else 1
    for attempt in range(1, attempts + 1):
        try:
            proc = subprocess.run(cmd, input=script, stdout=subprocess.PIPE, text=True,
                                  check=True, timeout=180)
            break
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if attempt == attempts:
                raise
            print(f"Retrying {action} for {dag_id or 'preflight'} ({attempt}/{attempts})", flush=True)
            time.sleep(8)
    # airflow-check.py prints the result as "ACCEPTANCE_JSON=<json>"; extract it.
    result = next(line.removeprefix("ACCEPTANCE_JSON=") for line in proc.stdout.splitlines()
                  if line.startswith("ACCEPTANCE_JSON="))
    return json.loads(result)


def record(name, value):
    """Atomically write a JSON evidence file (write to .tmp then rename)."""
    path = args.evidence / f"{name}.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def run(dag_id, **conf):
    """Trigger a DAG run (or resume an existing one) and poll until terminal state.

    The trigger step is idempotent: if a trigger file already exists the run_id
    is reused, so --resume picks up where the previous invocation left off.

    Polling interval: 45 s for Kubernetes (slower task scheduling), 20 s for Compose.
    Timeout: 2 hours.
    """
    trigger = args.evidence / f"{dag_id}-trigger.json"
    intent = args.evidence / f"{dag_id}-intent.json"
    if trigger.is_file():
        # Resume: the DAG was already triggered; reuse the existing run_id.
        run_id = json.loads(trigger.read_text())["run_id"]
    else:
        if intent.is_file():
            # Intent was recorded but trigger failed; verify conf matches before retrying.
            previous_intent = json.loads(intent.read_text())
            assert previous_intent["conf"] == conf, dag_id
            run_id = previous_intent["run_id"]
        else:
            # First attempt: build a deterministic run_id and save the intent.
            run_id = f"manual__retest_{args.mode}_{dag_id}_{stamp}"
            record(dag_id + "-intent", {"run_id": run_id, "conf": conf})
        record(dag_id + "-trigger", invoke("trigger", dag_id, run_id, **conf))
    limit = time.monotonic() + 7200
    previous = None
    while time.monotonic() < limit:
        state = invoke("inspect", dag_id, run_id)
        if state != previous:
            # State changed: persist the new snapshot and print progress.
            record(dag_id + "-latest", state)
            print(dag_id, state["state"], state["tasks"], flush=True)
            previous = state
        if state["state"] == "success":
            return state
        if state["state"] == "failed":
            raise SystemExit(f"{dag_id} failed ({run_id})")
        time.sleep(45 if args.mode == "kubernetes" else 20)
    raise SystemExit(f"{dag_id} did not finish within two hours ({run_id})")


# Preflight: wait up to ~3 minutes for all DAGs to be imported and no unrelated
# DAGs to be unpaused (which could interfere with the test results).
for _ in range(20):
    preflight = invoke("preflight", executor=executor)
    record("preflight", preflight)
    assert not preflight["unrelated_unpaused"], preflight["unrelated_unpaused"]
    if not preflight["missing_dags"] and not preflight["dag_import_errors"]:
        break
    time.sleep(10)
else:
    raise SystemExit(f"DAG preflight did not settle: {preflight}")

# Phase 1: initialise the science products directory for the GeD/DC4 pipeline.
# eps_time=50 sets the sliding window for trigger detection to 50 seconds.
init = run("init_pipelines", pipeline_branch="GeD", destination="tdrss",
           data_challenge="DC4", eps_time=50)

# Retrieve the products directory path from the task XCOM output.
paths = init["create_products_dir"]
products = paths.get("products_dir") or paths["products_dirs"]["GeD"]

# Phase 2: run the main GeD detection pipeline.
# level=1: standard science level.  auto_retrig=False: no automatic re-triggering.
ged = run("cosidag_GeD", monitoring_folders=[str(PurePosixPath(products).parent)],
          level=1, auto_retrig=False)
assert ged["finalize_cosidag_state"] == {"path": products, "status": "succeeded"}

# Verify: check that the expected product files exist and the GCN outbox has an entry.
record("verify", invoke("verify", "cosidag_GeD", ged["run_id"], products=products))

# Kubernetes-only: confirm that every task worker pod ran on cosi-1 (the designated
# worker node) and record both successful and failed pod attempts for auditing.
if args.mode == "kubernetes":
    pods = json.loads(subprocess.check_output(k + ["get", "pods", "-o", "json"], text=True))
    placement = {}
    failed_attempts = {}
    for run_result in (init, ged):
        run_id = run_result["run_id"]
        # Find all pods annotated with this run_id (set by the KubernetesExecutor).
        workers = [pod for pod in pods["items"]
                   if pod["metadata"].get("annotations", {}).get("run_id") == run_id]
        # Every pod must have been scheduled on cosi-1.
        assert workers and all(pod["spec"]["nodeName"] == "cosi-1" for pod in workers), run_id
        failed = [pod for pod in workers if pod["status"]["phase"] == "Failed"]
        remaining = [pod for pod in workers if pod["status"]["phase"] != "Failed"]
        tasks = {pod["metadata"]["annotations"]["task_id"] for pod in remaining}
        assert tasks and all(pod["metadata"]["annotations"]["task_id"] in tasks for pod in failed), run_id
        # Wait for all Succeeded pods to reach the terminal phase before reading their status.
        names = [pod["metadata"]["name"] for pod in remaining]
        subprocess.run(k + ["wait", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=180s"]
                       + [f"pod/{name}" for name in names], check=True)
        placement[run_id] = names
        if failed:
            # Record any pods that failed before a successful retry (retries are normal in K8s).
            failed_attempts[run_id] = [pod["metadata"]["name"] for pod in failed]
    record("worker-placement", placement)
    if failed_attempts:
        record("worker-failed-attempts", failed_attempts)

print("TEST SUCCESFUL \u2705")
