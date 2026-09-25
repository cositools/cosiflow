# COSIDAG performance testing

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

Finalization is limited to non-terminal runs whose IDs start with the configured
`finalization.benchmark_run_id_prefix` (`perf__` by default). It does not stop
manual or operational runs of the same DAGs. The command returns non-zero when
Airflow task-runner termination cannot be verified before the configured
deadline.

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

## Cleanup safety

The committed configuration has cleanup disabled and dry-run enabled. Real
deletion requires all of the following:

1. set `cleanup.enabled: true`;
2. set `cleanup.dry_run: false`;
3. pass `--allow-destructive-cleanup` to the runner.

For example:

```bash
python3 cosiflow/test/performance_test.py \
  --config cosiflow/test/performance_test.yaml \
  --allow-destructive-cleanup
```

`cleanup.allowed_root` must be a strict descendant of the COSIflow repository,
and every `cleanup.roots[].path` must be a strict descendant of that approved
benchmark data root. The repository root, the approved root itself, external
paths, and symlink escapes are rejected. The runner validates every configured
target before deleting the first file, so an invalid later target cannot leave
a partially cleaned dataset.

Paths are resolved relative to the local `cosiflow` folder, so the script does
not depend on a fixed user home directory. Run once with dry-run enabled and
review the cleanup summary before authorizing deletion.

## Result and stop semantics

Terminal state and benchmark success are separate. The runner returns `0` only
when at least one DAG was started, every started DAG finishes in `success`, no
timeout occurred, finalization was verified, and requested diagnostic artifacts
were generated. Failed, unfinished, or timed-out runs return non-zero.

When finalization stops a run, it marks only the exact task instances owned by
that run, then waits for their Airflow jobs and `airflow tasks run` processes to
disappear. A residual job or process after `stop_timeout_seconds` is reported
and makes the benchmark fail. `stop_other_active_runs` is disabled by default;
when explicitly enabled, it still applies only to IDs with the benchmark
prefix.

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
