# Local FasTP acceptance checkpoint

This is the stopping point after the code port and optional site configuration.
**The checks below have not been executed by the porting session.** They must
be reviewed/executed on the intended local instance before cluster acceptance.
The pipeline DAG files, existing Compose configuration and scientific
requirements remain unchanged.

## Baseline and preflight

Record the COSIflow/FasTP revisions and available scientific inputs. Keep
`COSIFLOW_TASK_EXECUTION_FILE` unset for the local baseline and confirm
`airflow config get-value core executor` reports `LocalExecutor`. Confirm
import errors are empty and the real module supplies `init_pipelines`,
`cosidag_BGO` and `cosidag_GeD` (these are the exact current DAG IDs).

Check the existing module links and interpreter paths for `cosipy`, `bct`
and `nimcosipy`. Record existing run/data state: do not reset processed state,
clear tasks, remove environments or overwrite datasets just to obtain a green
run. The current default Compose files were not changed by this port.

## Run order and evidence

| Order | Action | Required evidence |
| --- | --- | --- |
| 1 | Run `init_pipelines` with `pipeline_branch=BGO` and the agreed input selections | Run succeeds; record its `create_products_dir` XCom; staged products/symlinks exist and targets are readable under the configured data root |
| 2 | Verify the BGO input set on disk, then run `cosidag_BGO` | The sensor/`resolve_inputs` select that new input; ExternalPython tasks use the expected environments; scientific outputs and final state are correct |
| 3 | Run `init_pipelines` with `pipeline_branch=GeD` and the agreed data challenge/input selections | Run succeeds; source, background, orientation and response products exist; background-window preparation succeeds; record paths from this run |
| 4 | Verify the GeD input set on disk, then run `cosidag_GeD` | Input resolver selects the staged products; ExternalPython calls and scientific outputs succeed; state finalization remains correct |

Both scientific COSIDAGs monitor the current `data/tdrss` layout. Check the
initializer's destination setting against that layout before triggering; do
not assume a different selected destination is monitored automatically. Check
branch-specific filenames and current `file_patterns` to avoid feeding the
other branch's products to a run. The scientific initializer is dev-only and
remains in place for these checks, as requested.

Preserve run IDs, parameters, interpreter paths, staged/product paths and task
states. Distinguish no new input, input-resolution failure, dependency failure
and an actual regression introduced by the port. Automatic retriggering remains
the pipeline's existing behavior; a new waiting successor is not evidence that
the completed scientific run failed.

Only after this local checkpoint is accepted should the optional profile,
Kubernetes workers, failure recovery and clean-install tests be started. The
[optional Kubernetes guide](../getting-started/kubernetes.md) describes the
prepared configuration and its current limits.
