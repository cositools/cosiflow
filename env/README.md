# Creating and Managing Cosiflow Modules

This guide explains how to create, install, and manage modules for Cosiflow, using the `fastpipeline` module as a reference example.

---

## Table of Contents

1. [Module Structure](#module-structure)
2. [Creating a New Module](#creating-a-new-module)
3. [Installing a Module](#installing-a-module)
4. [Using Configuration Files](#using-configuration-files)
5. [Creating a Module from Scratch](#creating-a-module-from-scratch)
6. [Updating a Module](#updating-a-module)
7. [Removing a Module](#removing-a-module)
8. [Writing a Dockerfile for a Module](#writing-a-dockerfile-for-a-module)

---

## Module Structure

A Cosiflow module follows a standard directory structure:

```
your_module_name/
├── env/
│   ├── Dockerfile          # Container image definition
│   └── requirements.txt    # Python dependencies (optional)
├── src/
│   ├── dags/              # Airflow DAG definitions
│   │   └── *.py           # Your DAG files
│   └── pipeline/          # Pipeline scripts executed by tasks
│       └── *.py           # Your pipeline scripts
```

### Example: Fastpipeline Module Structure

```
fastpipeline/
├── env/
│   ├── Dockerfile
│   └── requirements.txt
└── src/
    ├── dags/
    │   ├── cosipipe_simdata.py
    │   ├── cosidag_lcurve.py
    │   └── cosidag_tsmap.py
    └── pipeline/
        ├── stage_files.py
        ├── bkg_cut.py
        └── ...
```

---

## Creating a New Module

To create a new module for Cosiflow:

1. **Create the module directory** in the workspace root (same level as `cosiflow/` and `fastpipeline/`):

   ```bash
   mkdir -p your_module_name/src/dags
   mkdir -p your_module_name/src/pipeline
   mkdir -p your_module_name/env
   ```

2. **Add your DAG files** in `src/dags/`:
   - Each Python file containing a DAG definition will be automatically discovered by Airflow
   - DAGs can use the `COSIDAG` framework from `cosiflow/modules/cosidag.py` for reactive, file-driven workflows

3. **Add your pipeline scripts** in `src/pipeline/`:
   - These are the Python scripts that will be executed by your DAG tasks
   - They can be called via `DockerOperator` or `PythonOperator` depending on your needs

4. **Create a Dockerfile** in `env/Dockerfile` (see [Writing a Dockerfile](#writing-a-dockerfile-for-a-module) section)

5. **Optionally create `requirements.txt`** in `env/` if your module needs Python dependencies

---

## Installing a Module

Use the `hot_load_module.sh` script to install a module into Cosiflow:

```bash
cd cosiflow/env
./hot_load_module.sh <module_name> install
```

### What the installation does

When run with `install` or `update`, `hot_load_module.sh`:

1. **Links DAGs**: creates a symbolic link from `/home/gamma/airflow/dags/<module_name>.cfmodule` to the module DAG directory (by default `src/dags/` or the one defined in the configuration file).
2. **Links Pipeline scripts**: creates a symbolic link from `/home/gamma/airflow/pipeline/<module_name>.cfmodule` to the module pipeline directory (by default `src/pipeline/` or the one defined in the configuration file).
3. **Manages Python environments and/or Docker images** according to the configuration:
   - can create one or more Python virtual environments inside the Airflow container;
   - can build a Docker image `<module_name>:latest` from the module `Dockerfile`.

### Custom paths from the command line

If your module uses a different structure, you can specify paths manually:

```bash
./hot_load_module.sh <module_name> install \
  -d <path_to_dags> \         # Default: src/dags
  -p <path_to_pipeline> \     # Default: src/pipeline
  -f <path_to_docker_context> # Default: env
```

Paths are relative to the module root, unless they are absolute.

### Quick example (without configuration file)

```bash
./hot_load_module.sh fastpipeline install
```

> **Note**: after installation, Airflow may take a few minutes to scan the DAGs and make them visible in the UI.

---

## Using Configuration Files

To simplify and standardize module installation, you can define a **YAML configuration file**
in the root of the module (same level as `src/` and `env/`).  
The `hot_load_module.sh` script automatically detects it (e.g. `*.config.yaml` or `cosiflow.config.yaml`).

A concrete example is the `fta-pipe.config.yaml` file of the *Fast Transient Analysis Pipeline* module:

```yaml
install_mode: both

paths:
  dags: src/dags
  pipeline: src/pipeline
  images: env

environments:
  cosipy:
    requirements: env/requirements.txt
    venv_path: /home/gamma/envs/cosipy
    enabled: true
    description: "Stable cosipy environment"

default_environment: cosipy
```

### Main fields of the configuration file

- **`install_mode`**: what to install when you run `install`:
  - `container`: only builds the module Docker image;
  - `environment`: only creates Python environments;
  - `both`: creates Python environments **and** builds the Docker image;
  - `none`: only creates DAG/pipeline symlinks, without building anything.

- **`paths`**:
  - `dags`: directory containing the DAGs (default `src/dags`);
  - `pipeline`: directory with pipeline scripts (default `src/pipeline`);
  - `images`: directory containing the `Dockerfile` (default `env`).

- **`environments`**:
  - map of Python environments that `hot_load_module.sh` can create inside the Airflow container;
  - for each environment:
    - `requirements`: path to the `requirements.txt` file (relative to the module root);
    - `venv_path`: path of the virtualenv in the container (e.g. `/home/gamma/envs/cosipy`);
    - `enabled`: if `true`, the environment is created automatically;
    - `description`: free-text description (for documentation/logs only).

- **`default_environment`**:
  - name of the environment considered “default” (used by scripts or documentation).

### How to use the configuration file

If the configuration file is present in the module root, you can install the module with:

```bash
cd cosiflow/env
./hot_load_module.sh <module_name> install
```

The script:
- reads the configuration file (e.g. `fta-pipe.config.yaml`);
- creates DAG/pipeline symlinks according to the configured paths;
- creates Python environments with `enabled: true`;
- builds the Docker image if requested by `install_mode`.

Optionally you can override the configuration:

```bash
# Force creation of the specified environments
./hot_load_module.sh <module_name> install -e -E env1,env2

# Install all environments defined in the YAML file
./hot_load_module.sh <module_name> install -e -E all
```

---

## Creating a Module from Scratch

This section summarizes how to create **from scratch** a new Cosiflow module,
including the folder structure and its configuration file.

### 1. Structuring the plugin folder

Assume the module is called `my_new_module` and lives at the same level as `cosiflow/`:

```bash
mkdir -p my_new_module/src/dags
mkdir -p my_new_module/src/pipeline
mkdir -p my_new_module/env
```

Struttura attesa:

```text
my_new_module/
├── env/
│   ├── Dockerfile          # Module Docker image
│   └── requirements.txt    # Python dependencies for the module environments
├── src/
│   ├── dags/               # Airflow DAG definitions
│   │   └── my_dag.py
│   └── pipeline/           # Pipeline scripts called by the DAGs
│       └── my_task.py
└── my-module.config.yaml   # (recommended) configuration file for hot_load_module.sh
```

Guidelines:
- DAGs in `src/dags/` can use the Cosiflow `COSIDAG` framework;
- scripts in `src/pipeline/` should be designed to run:
  - either inside the module Docker image;
  - or in a Python virtualenv created in the Airflow container.

### 2. Writing the Configuration File

In the module root, create a file such as `my-module.config.yaml`:

```yaml
# What 'install' should do
install_mode: both  # container | environment | both | none

# Where DAGs, pipeline and Dockerfile live
paths:
  dags: src/dags
  pipeline: src/pipeline
  images: env

# Definition of Python environments inside airflow container
environments:
  myenv:
    requirements: env/requirements.txt
    venv_path: /home/gamma/envs/myenv
    enabled: true
    description: "Environment for my_new_module"

default_environment: myenv
```

Recommendations:
- keep paths **relative** to the module root whenever possible;
- if the module needs multiple environments (e.g. stable/dev), add them under `environments`;
- use clear descriptions, they will appear in `hot_load_module.sh` logs.

### 3. Installing the new plugin

Once:
- you have written at least one DAG in `src/dags/`,
- you have added the scripts in `src/pipeline/`,
- you have prepared `env/Dockerfile` and `env/requirements.txt`,
- you have created the configuration file,

you can install the module:

```bash
cd cosiflow/env
./hot_load_module.sh my_new_module install
```

This:
- links the module DAGs and pipeline into Airflow;
- creates Python environments defined with `enabled: true`;
- builds the module Docker image (if requested by `install_mode`).

---

## Updating a Module

To update an already installed module (e.g., after modifying DAGs or pipeline scripts):

```bash
cd cosiflow/env
./hot_load_module.sh <module_name> update
```

The `update` action performs the same steps as `install`:
- Updates the DAGs and pipeline script links
- Rebuilds the Docker image if the Dockerfile has changed

**Note**: After updating, it may take a few minutes for Airflow to reload the DAGs and reflect your changes.

---

## Removing a Module

To remove a module from Cosiflow:

```bash
cd cosiflow/env
./hot_load_module.sh <module_name> remove
```

### What the removal does:

1. **Removes DAGs link**: Deletes the symbolic link in `/home/gamma/airflow/dags/<module_name>.cfmodule`
2. **Removes Pipeline scripts link**: Deletes the symbolic link in `/home/gamma/airflow/pipeline/<module_name>.cfmodule`
3. **Removes Docker image**: Removes the Docker image `<module_name>:latest`

**Note**: After removal, it may take a few minutes for Airflow to stop showing the DAGs in the UI. The DAGs will be automatically disabled and removed from the Airflow database during the next DAG refresh cycle.

---

## Writing a Dockerfile for a Module

Each module should have a `Dockerfile` in its `env/` directory. This Docker image will be used by `DockerOperator` tasks in your DAGs to execute pipeline scripts in an isolated environment.

### Basic Structure

Here's a template based on the `fastpipeline` module:

```dockerfile
# =============================================================================
#  Dockerfile — Runtime Container for Your Module
#  Base: Choose appropriate base image (see recommendations below)
# =============================================================================

FROM <base_image>

# ------------------------------ System Dependencies ------------------------------
# Install system libraries required for your scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    wget \
    curl \
    # Add other system dependencies as needed
    && rm -rf /var/lib/apt/lists/*

# ------------------------------ Non-root user ------------------------------
ARG UID=<YOUR_USER_ID>
ARG GID=<YOUR_GROUP_ID>

# Create group and user with specific UID/GID (must match Airflow container user)
RUN groupadd -g "${GID}" gamma || true && \
    useradd -m -u "${UID}" -g "${GID}" -s /bin/bash gamma || true

# Create shared directories with correct permissions
RUN mkdir -p /shared_dir /data01 /data02 && \
    chown -R "${UID}:${GID}" /shared_dir /data01 /data02

USER gamma
WORKDIR /home/gamma

# ------------------------------ Python Environment ------------------------------
# Create virtual environment
RUN python -m venv /home/gamma/envs/your_env_name

# Copy and install requirements
COPY --chown=gamma:gamma requirements.txt /home/gamma/requirements.txt
RUN . /home/gamma/envs/your_env_name/bin/activate && \
    pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /home/gamma/requirements.txt

# ------------------------------ Default Configuration ------------------------------
ENV VIRTUAL_ENV=/home/gamma/envs/your_env_name
ENV PATH="/home/gamma/envs/your_env_name/bin:$PATH"

CMD ["/bin/bash"]
```

### Example: Fastpipeline Dockerfile

The `fastpipeline` module uses `python:3.10-slim` as its base image:

```dockerfile
FROM python:3.10-slim

# System dependencies for scientific packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    wget \
    curl \
    libhdf5-dev \
    libgsl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# Non-root user setup
ARG UID=501
ARG GID=20
RUN groupadd -g "${GID}" gamma || true && \
    useradd -m -u "${UID}" -g "${GID}" -s /bin/bash gamma || true

RUN mkdir -p /shared_dir /data01 /data02 && \
    chown -R "${UID}:${GID}" /shared_dir /data01 /data02

USER gamma
WORKDIR /home/gamma

# Python environment with cosipy
RUN python -m venv /home/gamma/envs/cosipy
COPY --chown=gamma:gamma requirements.txt /home/gamma/requirements.txt
RUN . /home/gamma/envs/cosipy/bin/activate && \
    pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /home/gamma/requirements.txt

ENV VIRTUAL_ENV=/home/gamma/envs/cosipy
ENV PATH="/home/gamma/envs/cosipy/bin:$PATH"

CMD ["/bin/bash"]
```

### Choosing the Right Base Image

Selecting an appropriate base image depends on your module's requirements:

#### **For Lightweight Python Modules** (Recommended for most cases)
- **`python:3.10-slim`** or **`python:3.11-slim`**
  - Small image size (~50-100 MB)
  - Good for modules that only need Python and standard scientific libraries
  - Example: `fastpipeline` module

#### **For Modules Requiring System Libraries**
- **`python:3.10`** or **`python:3.11`** (full Debian)
  - Larger image (~200-300 MB) but includes more system tools
  - Use when you need additional system packages or compilers

#### **For Modules Requiring Specific OS Features**
- **`oraclelinux:8`** (as used in `cosiflow/env/Dockerfile.airflow`)
  - Use when you need Oracle Linux-specific packages or compatibility
  - Larger image size
  - Good for modules that need to match the Airflow container's OS

#### **For Minimal Dependencies**
- **`python:3.10-alpine`** or **`python:3.11-alpine`**
  - Smallest image size (~20-40 MB)
  - Use only if you don't need many system libraries (Alpine uses `musl` instead of `glibc`, which can cause compatibility issues)

### Best Practices

1. **User ID Matching**: Always set `UID=<YOUR_USER_ID>` and `GID=<YOUR_GROUP_ID>` to match the Airflow container user (`gamma`). This ensures proper file permissions when mounting volumes.

2. **Virtual Environments**: Use Python virtual environments to isolate dependencies and avoid conflicts.

3. **Layer Caching**: Order your Dockerfile commands from least to most frequently changing:
   - System dependencies first
   - Python environment setup
   - Requirements installation
   - Application code (if copying)

4. **Multi-stage Builds** (Optional): For complex modules, consider multi-stage builds to reduce final image size.

5. **Security**: Always use non-root users (`gamma`) and clean up package caches to reduce image size.

6. **Requirements File**: Keep a `requirements.txt` file in `env/` listing all Python dependencies with version pins for reproducibility.

7. **Choosing the Right Operator**:
   - **PythonOperator**: Prefer this for simple Python tasks that:
     - use only standard library or dependencies already available in the Airflow image;
     - are lightweight and stateless;
     - do not require a dedicated virtual environment.
     This is typically the **fastest option** because it runs in-process with the Airflow worker.
   - **BashOperator**: Use for simple shell commands and glue logic:
     - calling small CLI tools;
     - moving/renaming files;
     - orchestrating existing scripts that are already available in the Airflow container.
     Keep commands short and idempotent; avoid very complex bash logic that is hard to debug.
   - **ExternalPythonOperator** (or equivalent external-Python patterns): Use when a task:
     - needs a **specific Python environment** (virtualenv) installed inside the Airflow container;
     - depends on heavy or conflicting libraries that you do not want in the base Airflow image;
     - should be isolated but still run on the same host/container.
     Installing many different environments in the same Airflow container can make it heavier and harder to maintain, so prefer a **small number of well-defined environments**.
     Execution speed is typically **slower than `PythonOperator`** because it needs to spawn a separate process and activate a virtualenv.
   - **DockerOperator**: Use when a task:
     - requires external tools or runtimes (other languages, system tools, heavy scientific stacks);
     - must run in a **fully isolated environment** separate from the Airflow container;
     - benefits from packaging everything in a dedicated image (reproducibility, portability).
     Running many `DockerOperator` tasks in parallel can be resource-intensive (CPU, RAM, I/O), so monitor cluster capacity and concurrency. This is usually the **slowest option** (container startup + I/O), but offers the strongest isolation.

8. **Performance Hierarchy**: As a rule of thumb for execution speed (from faster to slower):
   - `PythonOperator` **>** external-Python-style operators **>** `DockerOperator`.  
   Start with the simplest/fastest operator that satisfies your isolation and dependency requirements, and move to heavier options only when necessary.

---

## Troubleshooting

### DAGs Not Appearing After Installation

- Airflow scans the DAGs directory periodically (default: every 5 minutes)
- Wait a few minutes and refresh the Airflow UI
- Check the Airflow logs for DAG parsing errors
- Verify that your DAG files don't have syntax errors
- If DAGs are not still appearing, shutdown the compose and re-up the compose

### DAGs still not visible: use `airflow dags list`

If, after installing/updating a module, DAGs still do not appear in the UI:

1. **Enter the Airflow container**:
   ```bash
   cd cosiflow/env
   docker compose exec airflow bash
   ```

2. **Run the diagnostic command**:
   ```bash
   airflow dags list
   ```

   This command:
   - forces the DAG parsing process on the filesystem;
   - shows the list of all DAGs that Airflow is actually able to load;
   - prints to the terminal any import/parsing errors from `.py` files (stack trace, missing modules, etc.).

3. **Why this can fix the problem**:
   - when you run `airflow dags list`, Airflow re-reads the files in the `dags` folder and refreshes its internal state;
   - if there are code errors or missing dependencies, you will see them explicitly in the command output:
     - you can then fix the DAG or the module Python environment;
   - once errors are fixed, running `airflow dags list` again lets you verify that the DAG is finally loaded.

### Docker Build Fails

- Ensure the Dockerfile path is correct (default: `env/Dockerfile`)
- Check that all files referenced in the Dockerfile (e.g., `requirements.txt`) exist
- Verify Docker has enough disk space and memory

### Permission Errors

- Ensure the Dockerfile creates the `gamma` user with `UID=501` and `GID=20`
- Check that mounted volumes have correct permissions

### Module Not Found

- Verify the module directory exists in the workspace root (same level as `cosiflow/`)
- Check that the module name matches the directory name exactly
- Ensure the `modules_pool` volume is correctly mounted in `docker-compose.yaml`

---

## Summary

Creating and managing Cosiflow modules is straightforward:

1. **Create** your module with the standard structure (`src/dags/`, `src/pipeline/`, `env/Dockerfile`)
2. **Install** using `./hot_load_module.sh <module_name> install`
3. **Update** using `./hot_load_module.sh <module_name> update`
4. **Remove** using `./hot_load_module.sh <module_name> remove`

Remember: Allow a few minutes for Airflow to discover and load DAGs after installation or updates.
