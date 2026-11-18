"""Run inside the scheduler container for the two-DAG acceptance.

This script is uploaded via stdin and executed inside the Airflow scheduler
container by science.py.  It must not import anything that is not already
available inside the container.

It accepts four positional arguments:
  action   -- one of: preflight, trigger, inspect, retry-stage, verify
  dag_id   -- DAG identifier (empty string for preflight)
  run_id   -- Airflow run identifier (empty string for preflight)
  payload  -- JSON-encoded dict of action-specific parameters

Results are printed as a single line:  ACCEPTANCE_JSON=<json>
science.py extracts and parses this line from stdout.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

# Retrieve the SQLAlchemy database connection string from the runtime secrets
# helper.  This avoids hard-coding credentials anywhere in this script.
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = subprocess.check_output(
    [sys.executable, "/home/gamma/validate_runtime_secrets.py",
     "--scope", "runtime", "--sqlalchemy-dsn"], text=True,
).strip()

import pymysql
from airflow.api.common.trigger_dag import trigger_dag
from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.errors import ParseImportError
from airflow.models.taskinstance import clear_task_instances
from airflow.models.xcom import XCom
from airflow.settings import Session

action, dag_id, run_id, payload = sys.argv[1:]
params = json.loads(payload)

with Session() as session:
    if action == "preflight":
        # Check that:
        # - No DAG files have parse errors (they would appear in the import errors table).
        # - Both expected DAGs are active (imported without errors).
        # - The executor matches what was configured in site.yaml.
        # - No unrelated DAGs are accidentally unpaused (could interfere with the test).
        errors = sorted(error.filename for error in session.query(ParseImportError))
        active_dags = session.query(DagModel).filter_by(is_active=True).all()
        expected = {"init_pipelines", "cosidag_GeD"}
        executor = conf.get("core", "executor")
        assert executor == params["executor"], executor
        result = {"executor": executor, "dag_import_errors": errors,
                  "missing_dags": sorted(expected - {dag.dag_id for dag in active_dags}),
                  "unrelated_unpaused": sorted(dag.dag_id for dag in active_dags
                                               if dag.dag_id not in expected and not dag.is_paused)}

    elif action == "trigger":
        # Unpause the DAG (Airflow imports all DAGs paused by default) then trigger a run.
        # Idempotent: if the run_id already exists, verify its conf matches and return it.
        dag = session.query(DagModel).filter_by(dag_id=dag_id).one()
        dag.is_paused = False
        session.commit()
        run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one_or_none()
        if run is None:
            run = trigger_dag(dag_id=dag_id, run_id=run_id, conf=params)
        else:
            # Resume path: confirm the previously-recorded conf matches the current request.
            assert run.conf == params, run_id
        result = {"run_id": run.run_id, "state": run.state}

    elif action == "inspect":
        # Return the current state of the DAG run and all its task instances.
        # On success: also fetch the key XCOM value and re-pause the DAG.
        run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one()
        tasks = session.query(TaskInstance).filter_by(dag_id=dag_id, run_id=run_id)
        result = {"run_id": run.run_id, "state": run.state,
                  "tasks": {task.task_id: task.state for task in tasks}}
        if run.state == "success":
            # Retrieve the products directory path from the task that created it.
            key = "create_products_dir" if dag_id == "init_pipelines" else "finalize_cosidag_state"
            result[key] = XCom.get_one(run_id=run_id, dag_id=dag_id, task_id=key,
                                       key="return_value", session=session)
            # Re-pause the DAG so it cannot be triggered again accidentally.
            session.query(DagModel).filter_by(dag_id=dag_id).one().is_paused = True
            session.commit()

    elif action == "retry-stage":
        # Recovery action for the rare case where the stage_all_files task fails
        # (e.g. a transient NFS mount issue) and all downstream tasks are upstream_failed.
        # Clears exactly those tasks so the scheduler can retry them without restarting
        # the whole DAG run from scratch.
        if dag_id != "init_pipelines" or not run_id.startswith("manual__retest_kubernetes_init_pipelines_"):
            raise ValueError("Expected the Kubernetes init_pipelines acceptance run")
        run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).with_for_update().one()
        tasks = session.query(TaskInstance).filter_by(dag_id=dag_id, run_id=run_id).all()
        stage = next(task for task in tasks if task.task_id == "stage_all_files")
        if run.state != "failed" or stage.state != "failed":
            raise ValueError(f"Expected a failed run and staging task, got {run.state}/{stage.state}")
        # Clear the failed staging task and all tasks that are upstream_failed from it.
        retry = [stage, *(task for task in tasks if task.state == "upstream_failed")]
        clear_task_instances(retry, session)
        session.commit()
        result = {"run_id": run_id, "cleared": sorted(task.task_id for task in retry)}

    elif action == "verify":
        # Post-run acceptance checks:
        # 1. All required science product files exist and are non-empty.
        # 2. The GCN outbox has at least one notice for this run.
        # 3. No delivery attempts have been made (DRY_RUN=true was set during deployment).
        products = Path(params["products"])
        outputs = sorted(str(path.relative_to(products)) for path in products.rglob("*")
                         if path.is_file() and not path.is_symlink() and path.stat().st_size)
        required = {"ged_preprocessed_data.hdf5", "duration_results.yaml",
                    "lightcurve_results.yaml", "tsmap_results.yaml",
                    "cosi_ged_alert_initial_record1.json"}
        assert required.issubset(outputs), sorted(required - set(outputs))
        # Connect to the GCN MySQL database to verify the outbox state.
        with pymysql.connect(host=os.environ["GCN_DB_HOST"],
                             port=int(os.environ["GCN_DB_PORT"]),
                             user=os.environ["GCN_DB_USER"],
                             password=os.environ["GCN_DB_PASSWORD"],
                             database=os.environ["GCN_DB_NAME"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM gcn_outbound_notices WHERE dag_run_id = %s", (run_id,))
                outbox = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM gcn_delivery_attempts")
                deliveries = cursor.fetchone()[0]
        # At least one outbox row must exist and no delivery should have been attempted.
        assert outbox >= 1 and deliveries == 0, (outbox, deliveries)
        result = {"products": str(products), "files": outputs,
                  "outbox_rows": outbox, "delivery_attempts": deliveries}

    else:
        raise ValueError(action)

# Print the result in the sentinel format that science.py scans for.
print("ACCEPTANCE_JSON=" + json.dumps(result))
