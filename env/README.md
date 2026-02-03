# Creating and Managing Cosiflow Modules

This guide explains how to create, install, and manage modules for Cosiflow, using the `fastpipeline` module as a reference example.

---

## Table of Contents

1. [Module Structure](#module-structure)
2. [Creating a New Module](#creating-a-new-module)
3. [Installing a Module](#installing-a-module)
4. [Updating a Module](#updating-a-module)
5. [Removing a Module](#removing-a-module)
6. [Writing a Dockerfile for a Module](#writing-a-dockerfile-for-a-module)

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

### What the installation does:

1. **Links DAGs**: Creates a symbolic link from `/home/gamma/airflow/dags/<module_name>.cfmodule` to your module's `src/dags/` directory
2. **Links Pipeline scripts**: Creates a symbolic link from `/home/gamma/airflow/pipeline/<module_name>.cfmodule` to your module's `src/pipeline/` directory
3. **Builds Docker image**: Builds a Docker image named `<module_name>:latest` from the `env/Dockerfile` in your module directory

### Custom Paths

If your module uses a different directory structure, you can specify custom paths:

```bash
./hot_load_module.sh <module_name> install \
  -d <path_to_dags> \      # Default: src/dags
  -p <path_to_pipeline> \  # Default: src/pipeline
  -f <path_to_docker_context>  # Default: env
```

Paths are relative to the module root directory unless specified as absolute paths.

### Example

```bash
./hot_load_module.sh fastpipeline install
```

**Note**: After installation, it may take a few minutes for Airflow to discover and load the new DAGs. Airflow periodically scans the DAGs directory, so be patient if your DAGs don't appear immediately in the Airflow UI.

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
ARG UID=501
ARG GID=20

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
    pip install --no-cache-dir -r /home/gamma/requirements.txt && \
    git clone https://github.com/cositools/cosipy.git /home/gamma/cosipy_stable && \
    cd /home/gamma/cosipy_stable && \
    git checkout v0.3.x && \
    pip install --no-cache-dir -e .

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

1. **User ID Matching**: Always set `UID=501` and `GID=20` to match the Airflow container user (`gamma`). This ensures proper file permissions when mounting volumes.

2. **Virtual Environments**: Use Python virtual environments to isolate dependencies and avoid conflicts.

3. **Layer Caching**: Order your Dockerfile commands from least to most frequently changing:
   - System dependencies first
   - Python environment setup
   - Requirements installation
   - Application code (if copying)

4. **Multi-stage Builds** (Optional): For complex modules, consider multi-stage builds to reduce final image size.

5. **Security**: Always use non-root users (`gamma`) and clean up package caches to reduce image size.

6. **Requirements File**: Keep a `requirements.txt` file in `env/` listing all Python dependencies with version pins for reproducibility.

---

## Troubleshooting

### DAGs Not Appearing After Installation

- Airflow scans the DAGs directory periodically (default: every 5 minutes)
- Wait a few minutes and refresh the Airflow UI
- Check the Airflow logs for DAG parsing errors
- Verify that your DAG files don't have syntax errors
- If DAGs are not still appearing, shout down the compose and re-up the compose

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
