# Creating and managing COSIflow modules

A COSIflow module adds DAG definitions, pipeline code, managed Python
environments, and optionally a Docker image to a running COSIflow instance.
`hot_load_module.sh` installs, updates, and removes modules without copying their
source trees into this repository.

## Prerequisites

Before managing modules:

1. configure and start COSIflow as described in
   [`../README.md`](../README.md);
2. keep every module directory beside `cosiflow/` in the same workspace;
3. run `hot_load_module.sh` from the host, normally from `cosiflow/env`;
4. generate the required ignored `cosiflow/env/.env` as documented in the main
   README; the stack intentionally fails when a credential is absent.

For authenticated GCN Kafka consumption, `.env` needs:

```dotenv
GCN_CLIENT_ID=<your-gcn-client-id>
GCN_CLIENT_SECRET=<your-gcn-client-secret>
```

Public-consumer credentials can receive public notices. Publishing COSI mission
notices requires separate producer authorization from the GCN team. Keep
`GCN_PRODUCER_ENABLED=false` and `GCN_DRY_RUN=true` for ordinary development.

## Module layout

The standard layout is:

```text
your-module/
├── env/
│   ├── Dockerfile
│   ├── requirements-main.txt
│   └── your-module.config.yaml
└── src/
    ├── dags/
    │   └── *.py
    └── pipeline/
        └── *.py
```

`env/Dockerfile` is needed only when `install_mode` builds a module image.
Requirement filenames are arbitrary; the YAML configuration selects them.

The current Fast Transient Analysis Pipeline is a concrete multi-environment
example:

```text
fast-transient-analysis-pipeline/
├── env/
│   ├── Dockerfile
│   ├── fta-pipe.config.yaml
│   ├── requirements_cosipy.txt
│   ├── requirements_bct.txt
│   └── requirements_nimcosipy.txt
└── src/
    ├── dags/
    │   ├── cosipipe_initpipeline.py
    │   ├── cosidag_BGO.py
    │   ├── cosidag_GeD.py
    │   └── cosidag_ARMselection.py
    └── pipeline/
        ├── stage_files.py
        ├── bkg_cut.py
        └── fast_transient_pipeline/
```

## Install a module

With the COSIflow stack running:

```bash
cd cosiflow/env
./hot_load_module.sh <module-directory> install
```

The installer:

1. creates
   `/home/gamma/airflow/dags/<module>.cfmodule`, pointing into the module's DAG
   directory under `modules_pool`;
2. creates the corresponding pipeline link under
   `/home/gamma/airflow/pipeline`;
3. recreates enabled Python environments when requested;
4. builds `<module>:latest` when requested.

Airflow can take a short time to parse newly linked DAGs.

### Command-line path overrides

Paths are relative to the module root unless absolute:

```bash
./hot_load_module.sh <module> install \
  -d src/dags \
  -p src/pipeline \
  -f env
```

| Option | Meaning |
| --- | --- |
| `-d` | DAG directory |
| `-p` | pipeline directory |
| `-f` | Docker build context containing `Dockerfile` |
| `-e` | create Python environments |
| `-r` | requirements file for legacy single-environment mode |
| `-E name1,name2` | create selected configured environments |
| `-E all` | create every configured environment |
| `-a` | create environments and build the image |
| `-c` | use an explicit YAML configuration file |

## YAML configuration

The hot-loader checks, in order:

1. `module_envs.yaml` in the module root;
2. `cosiflow.config.yaml` in the module root;
3. the first `*.config.yaml` in the module root;
4. the first `*.config.yaml` under the module's `env/` directory.

An explicit `-c` path overrides auto-detection.

Example:

```yaml
install_mode: both

paths:
  dags: src/dags
  pipeline: src/pipeline
  images: env

environments:
  analysis:
    requirements: env/requirements-analysis.txt
    requirements_no_deps: env/requirements-nodeps.txt
    venv_path: /home/gamma/envs/analysis
    enabled: true
    description: "Primary analysis environment"
    python_version: "3.12"

default_environment: analysis
```

### Installation modes

| `install_mode` | Python environments | Docker image | Links |
| --- | --- | --- | --- |
| `environment` | yes | no | yes |
| `container` | no | yes | yes |
| `both` | yes | yes | yes |
| `none` | no | no | yes |

### Environment fields

| Field | Required | Meaning |
| --- | --- | --- |
| `requirements` | yes | Main pip requirement file |
| `requirements_no_deps` | no | Additional file installed with `pip --no-deps` |
| `venv_path` | no | Target path; defaults to `/home/gamma/envs/<name>` |
| `enabled` | no | Installed by default when true |
| `description` | no | Text shown in hot-loader logs |
| `python_version` | no | Interpreter suffix, for example `3.11` or `3.12` |

`default_environment` is useful metadata for module documentation and code, but
the current hot-loader does not select environments from it. Selection is based
on `enabled` or `-E`.

## Update a module

```bash
cd cosiflow/env
./hot_load_module.sh <module-directory> update
```

`update` refreshes the links and repeats the configured installation. Managed
virtual environments are deleted and recreated to avoid stale dependencies.
The Docker image is rebuilt when the configured installation mode includes it.

Source-only DAG or pipeline edits are already visible through the symlinks, but
`update` is still appropriate after dependency or image changes.

## Remove a module

```bash
cd cosiflow/env
./hot_load_module.sh <module-directory> remove
```

With a detected configuration, removal deletes:

- the DAG and pipeline symlinks;
- every environment declared in the configuration, including its activation
  helper;
- `<module>:latest` when `install_mode` is `container` or `both`.

The module source directory and scientific data are not deleted. Without a
configuration file, legacy removal attempts to remove the default `cosipy`
environment and module image as well as the links.

## Creating a new module

Create the directories from the workspace root:

```bash
mkdir -p my-module/src/dags
mkdir -p my-module/src/pipeline
mkdir -p my-module/env
```

Add at least:

1. a DAG file under `src/dags`;
2. any task libraries under `src/pipeline`;
3. a `*.config.yaml` describing the desired links and runtimes;
4. requirement files for external-Python tasks;
5. a Dockerfile only if Docker tasks need a module image.

COSIDAG development is documented in
[`../modules/README.md`](../modules/README.md).

## Module Dockerfile

Module images are built only for externally orchestrated runners. Airflow has
no Docker daemon access. A minimal Debian/Python template is:

```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

ARG UID=501
ARG GID=20

RUN groupadd -g "${GID}" gamma || true && \
    useradd -m -u "${UID}" -g "${GID}" -s /bin/bash gamma || true

USER gamma
WORKDIR /home/gamma

RUN python -m venv /home/gamma/envs/analysis
COPY --chown=gamma:gamma requirements-analysis.txt /home/gamma/requirements.txt
RUN /home/gamma/envs/analysis/bin/python -m pip install --upgrade pip setuptools wheel && \
    /home/gamma/envs/analysis/bin/python -m pip install -r /home/gamma/requirements.txt

ENV VIRTUAL_ENV=/home/gamma/envs/analysis
ENV PATH="/home/gamma/envs/analysis/bin:${PATH}"

CMD ["/bin/bash"]
```

Choose an image and system packages compatible with the scientific stack.
Match the module image UID/GID to the Airflow `gamma` user when writing to
shared mounts. Pin released packages and VCS commits where reproducibility is
required.

## Operator selection

| Operator | Use when |
| --- | --- |
| `PythonOperator` | dependencies already exist in the Airflow environment |
| `ExternalPythonOperator` | the task needs a managed, isolated Python environment |
| Dedicated external runner | the task needs a container image; never grant the Airflow webserver Docker access |

Container images improve isolation, but reproducibility still depends on pinned
base-image digests and dependency versions.

## Plugin RBAC provisioning

During `airflow-init`, the entrypoint migrates the metastore, runs
`airflow sync-perm`, synchronizes the Admin account, and then executes
`/home/gamma/configure_rbac.py`. The configurator uses Flask-AppBuilder APIs,
never metadata-table SQL, and fails init if the verified matrix differs.

| Permission | Viewer | Scientist | Operator | Admin |
| --- | :---: | :---: | :---: | :---: |
| `can_read / COSIflow GCN Notices` | No | Yes | Yes | FAB Admin |
| `can_create / COSIflow GCN Inbox` | No | No | Yes | FAB Admin |
| `can_create / COSIflow GCN Outbox` | No | No | Yes | FAB Admin |
| `can_read / COSIflow Scientific Data` | No | Yes | Yes | FAB Admin |
| `can_read / COSIflow COSIDAG State` | No | Yes | Yes | FAB Admin |
| `can_edit / COSIflow COSIDAG State` | No | No | Yes | FAB Admin |
| `can_edit / COSIflow DAG Catalog` | No | No | Yes | FAB Admin |
| `can_read / COSIflow Mail Sandbox` | No | No | Yes | FAB Admin |

Scientist is extended from the current Viewer permission set and Operator from
the current Op set. Default Airflow roles are not modified. Reruns add missing
base or COSIflow permissions, remove only obsolete `COSIflow *` permissions and
managed menu entries from the two managed roles, and preserve unrelated custom
permissions.

Run and verify twice after a rollout:

```bash
docker compose run --rm airflow-init
docker compose run --rm airflow-init
docker compose run --rm --entrypoint python airflow-init /home/gamma/configure_rbac.py --verify-only
python3 -m unittest discover -s ../tests/security -v
```

Before assigning real users, confirm that Viewer has no `COSIflow *`
permission. Assign read-only scientific users to Scientist and operational
users to Operator. Keep Admin limited to administrators. The menu policy is
defense in depth: direct requests remain protected by route permissions.

## Troubleshooting

### DAGs do not appear

Check the links and parser from inside Airflow:

```bash
docker exec -u gamma cosi_airflow \
  ls -la /home/gamma/airflow/dags

docker exec -u gamma cosi_airflow \
  airflow dags list
```

The Airflow menu **Develop Tools → Refresh DAGs List** runs the same parser
diagnostic and synchronizes the resulting DAG bag.

### Environment creation fails

- verify that the requested interpreter exists in the Airflow image;
- verify every requirement path relative to the module root;
- recreate rather than modifying an old environment in place;
- inspect the hot-loader output for the exact pip command that failed.

### Docker build fails

- verify `paths.images` and the Dockerfile name;
- remember that the build context is the configured `images` directory;
- make sure every `COPY` source is inside that context;
- verify available disk and memory.

### Permission errors

- align the COSIflow and module-image UID/GID;
- pre-create host bind-mount directories as the host user;
- inspect ownership both on the host and inside the container.

### Module not found

- confirm that its directory is beside `cosiflow/`;
- match the command argument to the directory name exactly;
- verify the `modules_pool` mount in `docker-compose.yaml`.
