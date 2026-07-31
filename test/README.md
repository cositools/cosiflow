# COSIDAG performance test

If Python is available on the host, run from the workspace root:

```bash
python3 cosiflow/test/performance_test.py --config cosiflow/test/performance_test.yaml
```

If Python is not available on the host, run the test inside the Airflow
container:

```bash
bash cosiflow/test/run_performance_test.sh
```

To stop non-terminal runs for the configured DAGs without starting a new test:

```bash
bash cosiflow/test/run_performance_test.sh --finalize-only
```

To regenerate charts from the configured CSV without starting a new test:

```bash
bash cosiflow/test/run_performance_test.sh --charts-only
```

The wrapper executes:

```bash
python /shared_dir/test/performance_test.py --config /shared_dir/test/performance_test.yaml --inside-container
```

inside the configured Airflow service.

The YAML controls:

- which DAG/COSIDAG IDs to reset and trigger;
- optional DAG run `conf` data;
- which local data folders are cleaned before the run;
- which filenames are preserved during cleanup;
- polling interval, timeout, CSV path, resource containers, and final pause/stop behavior.

All paths in `cleanup.roots[].path` are resolved relative to the local `cosiflow`
folder, so the script does not depend on a fixed user home directory.

The CSV uses two header rows: the first row groups columns by DAG/COSIDAG or
`resources`, and the second row contains the concrete subcolumn names.

When `charts.enabled` is `true`, the test writes SVG charts under
`results/charts/`:

- `cosidag_gantt.svg`: Airflow-style task Gantt chart using task start/end metadata.
- `elapsed_time.svg`: horizontal elapsed-time bars for each COSIDAG and the full benchmark.
- `resources_memory_load_cpu.svg`: memory, 1-minute load, and estimated CPU-core usage over time.
- `resources_disk.svg`: workspace and Airflow-home disk growth during the benchmark.
- `airflow_graphs/<dag_id>_ui_graph.svg`: DAG dependency graphs rendered in an Airflow UI-style layout from `DagBag` task dependencies.

Set `charts.airflow_graphs.include_graphviz: true` to also export
`airflow_graphs/<dag_id>_graph.<format>` through `airflow dags show`. That
optional export depends on Graphviz support in the Airflow environment.
