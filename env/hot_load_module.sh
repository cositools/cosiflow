#!/bin/bash
# hot_load_module.sh
# Usage:
#   $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images]
#
# Options (paths are relative to module root unless absolute):
#   -d  path to DAGs directory (default: src/dags)
#   -p  path to pipeline directory (default: src/pipeline)
#   -f  path to Docker context directory containing Dockerfile (default: env)
#   -e  create Python virtual environment(s) in container (default: false)
#   -r  path to requirements.txt file (default: env/requirements.txt, legacy mode)
#   -E  comma-separated list of environments to create from module_envs.yaml (e.g., env1,env2 or "all")
#   -a  create Python environment AND build Docker image (equivalent to -e with Docker build)
#   -c  path to config file (default: auto-detect in module)

CONTAINER_USER="gamma"
CONTAINER_NAME="cosi_airflow"
EXTENSION_MODULE=".cfmodule"

# Resolve paths independently of the caller's current directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
MODULES_ROOT="${COSIFLOW_MODULES_HOST_DIR:-$SCRIPT_DIR/../modules-pool}"

# Default paths (relative to module root)
PATH_DAGS="src/dags"
PATH_PIPELINE="src/pipeline"
PATH_IMAGES="env"
CREATE_ENV=false
BUILD_DOCKER=false
PATH_REQUIREMENTS="env/requirements.txt"
VENV_PATH="/home/gamma/envs/cosipy"
ENV_SELECTION=""  # Empty = use YAML enabled, "all" = all, or comma-separated list
CONFIG_FILE=""    # Optional path to config file (overrides auto-detect when set)
YAML_CONFIG=""
CONTAINER_YAML_CONFIG=""
CONTAINER_MODULES_ROOT="/home/gamma/airflow/modules_pool"
CONTAINER_VENV_ROOT="/home/gamma/envs"
CONFIG_HELPER="/home/gamma/module_config.py"
CONFIG_PYTHON="/home/gamma/venv/bin/python"

# CLI overrides are applied only after YAML has been parsed and validated.
CLI_PATH_DAGS=""
CLI_PATH_PIPELINE=""
CLI_PATH_IMAGES=""
CLI_PATH_REQUIREMENTS=""
CLI_CREATE_ENV=false
CLI_BUILD_DOCKER=false
CLI_ENV_SELECTION=""
PATH_IMAGES_CONFIGURED=false

# Normalized configuration snapshot. Parallel arrays keep compatibility with
# the Bash 3.2 shipped by macOS; associative arrays and eval are deliberately
# avoided.
CONFIG_INSTALL_MODE=""
CONFIG_DEFAULT_ENVIRONMENT=""
CONFIG_ENV_NAMES=()
CONFIG_ENV_REQUIREMENTS=()
CONFIG_ENV_REQUIREMENTS_NO_DEPS=()
CONFIG_ENV_VENV_PATHS=()
CONFIG_ENV_ENABLED=()
CONFIG_ENV_DESCRIPTIONS=()
CONFIG_ENV_PYTHON_VERSIONS=()
ENVS_TO_CREATE=()

##########################################################################
# HELPER FUNCTIONS
##########################################################################

# Helper to run docker exec as airflow user
dexec() {
    docker exec -u "$CONTAINER_USER" "$CONTAINER_NAME" "$@"
}

# Run a container command as the managed user while forwarding stdin. This is
# used to create files with gamma ownership without interpolating data in a
# shell command string.
dexec_stdin() {
    docker exec -u "$CONTAINER_USER" -i "$CONTAINER_NAME" "$@"
}

# define a macro `log` for printing messages with color green
log() {
    echo -e "\033[32m$1\033[0m"
}

# define a macro `warning` for printing messages with color yellow
warning() {
    echo -e "\033[33m$1\033[0m"
}

# define a macro `error` for printing messages with color red
error() {
    echo -e "\033[31m$1\033[0m" >&2
    exit 1
}

# Function to find YAML config file in module
find_yaml_config() {
    local module_path="$1"
    
    # Try common names in module root
    local yaml_file="$module_path/module_envs.yaml"
    if [ -f "$yaml_file" ]; then
        echo "$yaml_file"
        return 0
    fi
    
    yaml_file="$module_path/cosiflow.config.yaml"
    if [ -f "$yaml_file" ]; then
        echo "$yaml_file"
        return 0
    fi
    
    # Try to find any *.config.yaml file in module root
    yaml_file=$(find "$module_path" -maxdepth 1 -name "*.config.yaml" -type f -print 2>/dev/null | LC_ALL=C sort | head -n 1)
    if [ -n "$yaml_file" ] && [ -f "$yaml_file" ]; then
        echo "$yaml_file"
        return 0
    fi
    
    # Try to find any *.config.yaml file in env/ subdirectory
    if [ -d "$module_path/env" ]; then
        yaml_file=$(find "$module_path/env" -maxdepth 1 -name "*.config.yaml" -type f -print 2>/dev/null | LC_ALL=C sort | head -n 1)
        if [ -n "$yaml_file" ] && [ -f "$yaml_file" ]; then
            echo "$yaml_file"
            return 0
        fi
    fi
    
    return 1
}

validate_identifier() {
    local value="$1"
    local label="$2"
    if [[ "$value" == "." || "$value" == ".." || ! "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
        error "$label contains unsupported characters: $value"
    fi
}

canonical_existing_path() {
    local candidate="$1"
    if [ ! -e "$candidate" ]; then
        return 1
    fi
    realpath "$candidate"
}

require_path_below() {
    local root="$1"
    local candidate="$2"
    local label="$3"
    case "$candidate" in
        "$root"/*) ;;
        *) error "$label must stay below $root: $candidate" ;;
    esac
}

resolve_module_path() {
    local configured_path="$1"
    local expected_type="$2"
    local label="$3"
    local candidate
    local resolved

    if [ -z "$configured_path" ] || [[ "$configured_path" =~ [[:cntrl:]] ]]; then
        error "$label must be a non-empty path without control characters."
    fi
    if [[ "$configured_path" = /* ]]; then
        candidate="$configured_path"
    else
        candidate="$MODULE_PATH/$configured_path"
    fi
    if ! resolved=$(canonical_existing_path "$candidate"); then
        error "$label not found: $candidate"
    fi
    require_path_below "$MODULE_PATH" "$resolved" "$label"
    if [ "$expected_type" = "file" ] && [ ! -f "$resolved" ]; then
        error "$label must be a regular file: $resolved"
    fi
    if [ "$expected_type" = "directory" ] && [ ! -d "$resolved" ]; then
        error "$label must be a directory: $resolved"
    fi
    printf '%s\n' "$resolved"
}

module_relative_path() {
    local resolved="$1"
    require_path_below "$MODULE_PATH" "$resolved" "Module path"
    printf '%s\n' "${resolved#"$MODULE_PATH"/}"
}

config_helper() {
    dexec "$CONFIG_PYTHON" "$CONFIG_HELPER" --config "$CONTAINER_YAML_CONFIG" "$@"
}

prepare_yaml_config() {
    local yaml_file="$1"
    local resolved
    local relative

    if ! resolved=$(canonical_existing_path "$yaml_file"); then
        error "Config file not found: $yaml_file"
    fi
    require_path_below "$MODULE_PATH" "$resolved" "Config file"
    if [ ! -f "$resolved" ]; then
        error "Config path is not a regular file: $resolved"
    fi

    relative="${resolved#"$MODULE_PATH"/}"
    YAML_CONFIG="$resolved"
    CONTAINER_YAML_CONFIG="$CONTAINER_MODULES_ROOT/$MODULE_NAME/$relative"
}

config_value() {
    case "$1" in
        install_mode) printf '%s\n' "$CONFIG_INSTALL_MODE" ;;
        paths.dags) printf '%s\n' "$PATH_DAGS" ;;
        paths.pipeline) printf '%s\n' "$PATH_PIPELINE" ;;
        paths.images) printf '%s\n' "$PATH_IMAGES" ;;
        *) return 1 ;;
    esac
}

list_yaml_envs() {
    printf '%s\n' "${CONFIG_ENV_NAMES[@]}"
}

config_environment_index() {
    local env_name="$1"
    local index
    for ((index = 0; index < ${#CONFIG_ENV_NAMES[@]}; index++)); do
        if [ "${CONFIG_ENV_NAMES[$index]}" = "$env_name" ]; then
            printf '%s\n' "$index"
            return 0
        fi
    done
    return 1
}

config_environment_value() {
    local env_name="$1"
    local field="$2"
    local index
    validate_identifier "$env_name" "Environment name"
    index=$(config_environment_index "$env_name") || return 1
    case "$field" in
        requirements) printf '%s\n' "${CONFIG_ENV_REQUIREMENTS[$index]}" ;;
        requirements_no_deps) printf '%s\n' "${CONFIG_ENV_REQUIREMENTS_NO_DEPS[$index]}" ;;
        venv_path) printf '%s\n' "${CONFIG_ENV_VENV_PATHS[$index]}" ;;
        enabled) printf '%s\n' "${CONFIG_ENV_ENABLED[$index]}" ;;
        description) printf '%s\n' "${CONFIG_ENV_DESCRIPTIONS[$index]}" ;;
        python_version) printf '%s\n' "${CONFIG_ENV_PYTHON_VERSIONS[$index]}" ;;
        *) return 1 ;;
    esac
}

load_yaml_config_snapshot() {
    local snapshot_file
    local snapshot_fields=()
    local field
    local index=0
    local env_count
    local env_index

    if [ -z "$YAML_CONFIG" ]; then
        return 1
    fi

    snapshot_file=$(mktemp "${TMPDIR:-/tmp}/cosiflow-config.XXXXXX") || \
        error "Cannot create configuration snapshot."
    if ! config_helper snapshot > "$snapshot_file"; then
        rm -f -- "$snapshot_file"
        error "Module configuration validation failed."
    fi
    while IFS= read -r -d '' field; do
        snapshot_fields+=("$field")
    done < "$snapshot_file"
    rm -f -- "$snapshot_file"

    if [ "${snapshot_fields[0]:-}" != "1" ]; then
        error "Unsupported module configuration snapshot."
    fi

    index=1
    CONFIG_INSTALL_MODE="${snapshot_fields[$index]}"; ((index++))
    if [ -n "${snapshot_fields[$index]}" ]; then PATH_DAGS="${snapshot_fields[$index]}"; fi; ((index++))
    if [ -n "${snapshot_fields[$index]}" ]; then PATH_PIPELINE="${snapshot_fields[$index]}"; fi; ((index++))
    if [ -n "${snapshot_fields[$index]}" ]; then
        PATH_IMAGES="${snapshot_fields[$index]}"
        PATH_IMAGES_CONFIGURED=true
    fi
    ((index++))
    CONFIG_DEFAULT_ENVIRONMENT="${snapshot_fields[$index]}"; ((index++))
    env_count="${snapshot_fields[$index]}"; ((index++))
    if [[ ! "$env_count" =~ ^[0-9]+$ ]]; then
        error "Invalid environment count in configuration snapshot."
    fi

    CONFIG_ENV_NAMES=()
    CONFIG_ENV_REQUIREMENTS=()
    CONFIG_ENV_REQUIREMENTS_NO_DEPS=()
    CONFIG_ENV_VENV_PATHS=()
    CONFIG_ENV_ENABLED=()
    CONFIG_ENV_DESCRIPTIONS=()
    CONFIG_ENV_PYTHON_VERSIONS=()
    for ((env_index = 0; env_index < env_count; env_index++)); do
        if [ $((index + 6)) -ge ${#snapshot_fields[@]} ]; then
            error "Incomplete module configuration snapshot."
        fi
        CONFIG_ENV_NAMES+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_REQUIREMENTS+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_REQUIREMENTS_NO_DEPS+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_VENV_PATHS+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_ENABLED+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_DESCRIPTIONS+=("${snapshot_fields[$index]}"); ((index++))
        CONFIG_ENV_PYTHON_VERSIONS+=("${snapshot_fields[$index]}"); ((index++))
    done
    if [ "$index" -ne "${#snapshot_fields[@]}" ]; then
        error "Unexpected data in module configuration snapshot."
    fi

    case "$CONFIG_INSTALL_MODE" in
        container) BUILD_DOCKER=true; CREATE_ENV=false ;;
        environment) BUILD_DOCKER=false; CREATE_ENV=true ;;
        both) BUILD_DOCKER=true; CREATE_ENV=true ;;
        none|"") BUILD_DOCKER=false; CREATE_ENV=false ;;
        *) error "Unsupported install mode after validation: $CONFIG_INSTALL_MODE" ;;
    esac
}

validate_container_venv_path() {
    local candidate="$1"
    local resolved
    if ! resolved=$(dexec realpath -m -- "$candidate"); then
        error "Cannot canonicalize environment path: $candidate"
    fi
    case "$resolved" in
        "$CONTAINER_VENV_ROOT"/*) ;;
        *) error "Environment path must be below $CONTAINER_VENV_ROOT: $resolved" ;;
    esac
    if [ "$resolved" = "$CONTAINER_VENV_ROOT" ]; then
        error "Refusing to operate on the environment root itself."
    fi
    printf '%s\n' "$resolved"
}

safe_remove_venv() {
    local safe_path
    if ! safe_path=$(validate_container_venv_path "$1"); then
        return 1
    fi
    dexec rm -rf -- "$safe_path"
}

preflight_environment() {
    local env_name="$1"
    local req_path
    local req_no_deps
    local venv_path

    validate_identifier "$env_name" "Environment name"
    req_path=$(config_environment_value "$env_name" requirements) || error "Cannot read requirements for '$env_name'."
    resolve_module_path "$req_path" file "Requirements file for $env_name" >/dev/null || exit 1
    req_no_deps=$(config_environment_value "$env_name" requirements_no_deps) || error "Cannot read optional requirements for '$env_name'."
    if [ -n "$req_no_deps" ]; then
        resolve_module_path "$req_no_deps" file "No-deps requirements file for $env_name" >/dev/null || exit 1
    fi
    venv_path=$(config_environment_value "$env_name" venv_path) || error "Cannot read environment path for '$env_name'."
    validate_container_venv_path "$venv_path" >/dev/null || exit 1
}

preflight_inputs() {
    local require_dockerfile="${1:-true}"
    local resolved
    local env_name
    local selected
    local configured_envs=()

    resolved=$(resolve_module_path "$PATH_DAGS" directory "DAG path") || exit 1
    PATH_DAGS=$(module_relative_path "$resolved") || exit 1
    if [ -n "$PATH_PIPELINE" ]; then
        resolved=$(resolve_module_path "$PATH_PIPELINE" directory "Pipeline path") || exit 1
        PATH_PIPELINE=$(module_relative_path "$resolved") || exit 1
    fi
    if [ "$BUILD_DOCKER" = true ] || [ "$PATH_IMAGES_CONFIGURED" = true ]; then
        resolved=$(resolve_module_path "$PATH_IMAGES" directory "Docker context path") || exit 1
        if [ "$require_dockerfile" = true ] && [ "$BUILD_DOCKER" = true ] && [ ! -f "$resolved/Dockerfile" ]; then
            error "Docker build requested but Dockerfile not found: $resolved/Dockerfile"
        fi
        PATH_IMAGES=$(module_relative_path "$resolved") || exit 1
    fi

    if [ -n "$YAML_CONFIG" ]; then
        while IFS= read -r env_name; do
            [ -n "$env_name" ] && configured_envs+=("$env_name")
        done < <(list_yaml_envs)
        for env_name in "${configured_envs[@]}"; do
            preflight_environment "$env_name"
        done

        if [ -n "$ENV_SELECTION" ] && [ "$ENV_SELECTION" != "all" ]; then
            IFS=',' read -ra selected_envs <<< "$ENV_SELECTION"
            for selected in "${selected_envs[@]}"; do
                selected=$(printf '%s' "$selected" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                validate_identifier "$selected" "Selected environment name"
                config_environment_value "$selected" requirements >/dev/null || error "Selected environment is not configured: $selected"
            done
        fi
    elif [ "$CREATE_ENV" = true ]; then
        resolve_module_path "$PATH_REQUIREMENTS" file "Legacy requirements file" >/dev/null || exit 1
        validate_container_venv_path "$VENV_PATH" >/dev/null || exit 1
    fi
}

##########################################################################
# MAIN SCRIPT
##########################################################################

# Parse positional args: module_name [action]
if [ $# -eq 0 ]; then
    MODULE_NAME=""
else
    MODULE_NAME="$1"
fi
if [ -n "$MODULE_NAME" ] && { [ "${2:-}" = "install" ] || [ "${2:-}" = "remove" ] || [ "${2:-}" = "update" ]; }; then
    ACTION="$2"
    shift 2
elif [ -n "$MODULE_NAME" ]; then
    ACTION=install
    shift 1
else
    ACTION=install
fi

# Parse options -d, -p, -f, -e, -r, -a, -E, -c
# CLI options override YAML configuration
while getopts "d:p:f:er:aE:c:" opt; do
    case $opt in
        d) CLI_PATH_DAGS="$OPTARG" ;;
        p) CLI_PATH_PIPELINE="$OPTARG" ;;
        f) CLI_PATH_IMAGES="$OPTARG" ;;
        e) CLI_CREATE_ENV=true ;;
        r) CLI_PATH_REQUIREMENTS="$OPTARG" ;;
        a) CLI_CREATE_ENV=true; CLI_BUILD_DOCKER=true ;;
        E) CLI_CREATE_ENV=true; CLI_ENV_SELECTION="$OPTARG" ;;
        c) CONFIG_FILE="$OPTARG" ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
        *) echo "Usage: $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images] -e [-r requirements.txt] -E [env1,env2|all] -a [-c config.yaml]" >&2; exit 1 ;;
    esac
done

if [ -z "$MODULE_NAME" ]; then
    echo "Usage: $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images] -e [-r requirements.txt] -E [env1,env2|all] -a [-c config.yaml]"
    echo ""
    echo "Options (paths relative to module root unless absolute):"
    echo "  -d  path to DAGs directory (default: src/dags)"
    echo "  -p  path to pipeline directory (default: src/pipeline)"
    echo "  -f  path to Docker context directory (default: env)"
    echo "  -e  create Python virtual environment(s) in container (default: false)"
    echo "  -r  path to requirements.txt file (default: env/requirements.txt, legacy single-env mode)"
    echo "  -E  comma-separated list of environments from module_envs.yaml (e.g., env1,env2) or 'all'"
    echo "      If -E is used, module_envs.yaml will be read from module root"
    echo "  -a  create Python environment AND build Docker image (equivalent to -e with Docker build)"
    echo "  -c  path to config file (default: auto-detect in module)"
    echo ""
    echo "Examples:"
    echo "  # Legacy mode: single environment"
    echo "  $0 mymodule install -e -r env/requirements.txt"
    echo ""
    echo "  # Multi-environment mode: install enabled environments from YAML"
    echo "  $0 mymodule install -e"
    echo ""
    echo "  # Multi-environment mode: install specific environments"
    echo "  $0 mymodule install -E env1,env2"
    echo ""
    echo "  # Multi-environment mode: install all environments"
    echo "  $0 mymodule install -E all"
    echo ""
    echo "  # Use a specific config file (e.g. tutorial config under docs/)"
    echo "  $0 mymodule install -c docs/tutorials/bgo-loc/cosiflow/env/bgoloc.config.yaml"
    exit 1
fi

validate_identifier "$MODULE_NAME" "Module name"
MODULES_ROOT=$(canonical_existing_path "$MODULES_ROOT") || error "Cannot resolve module pool: $MODULES_ROOT"
MODULE_PATH="$MODULES_ROOT/$MODULE_NAME"
if [ ! -d "$MODULE_PATH" ]; then
    error "Module directory not found: $MODULE_PATH"
fi
if [ -d "$MODULE_PATH" ]; then
    MODULE_PATH=$(canonical_existing_path "$MODULE_PATH") || error "Cannot resolve module directory."
    require_path_below "$MODULES_ROOT" "$MODULE_PATH" "Module directory"
fi

# Resolve and validate the complete YAML configuration before any mutation.
if [ -n "$CONFIG_FILE" ]; then
    prepare_yaml_config "$CONFIG_FILE"
elif [ -d "$MODULE_PATH" ]; then
    if found_yaml=$(find_yaml_config "$MODULE_PATH"); then
        prepare_yaml_config "$found_yaml"
    fi
fi
if [ -n "$YAML_CONFIG" ]; then
    load_yaml_config_snapshot || error "Failed to load validated module configuration."
fi

# Command-line flags override validated YAML defaults.
if [ -n "$CLI_PATH_DAGS" ]; then PATH_DAGS="$CLI_PATH_DAGS"; fi
if [ -n "$CLI_PATH_PIPELINE" ]; then PATH_PIPELINE="$CLI_PATH_PIPELINE"; fi
if [ -n "$CLI_PATH_IMAGES" ]; then
    PATH_IMAGES="$CLI_PATH_IMAGES"
    PATH_IMAGES_CONFIGURED=true
fi
if [ -n "$CLI_PATH_REQUIREMENTS" ]; then PATH_REQUIREMENTS="$CLI_PATH_REQUIREMENTS"; fi
if [ "$CLI_CREATE_ENV" = true ]; then CREATE_ENV=true; fi
if [ "$CLI_BUILD_DOCKER" = true ]; then BUILD_DOCKER=true; fi
if [ -n "$CLI_ENV_SELECTION" ]; then ENV_SELECTION="$CLI_ENV_SELECTION"; fi

resolve_environment_plan() {
    local env_name
    local enabled
    local selected_envs=()
    local existing

    ENVS_TO_CREATE=()
    if [ "$CREATE_ENV" != true ] || [ "$ACTION" = "remove" ]; then
        return 0
    fi
    if [ -z "$YAML_CONFIG" ]; then
        if [ -n "$ENV_SELECTION" ]; then
            error "-E requires a validated module YAML configuration."
        fi
        return 0
    fi

    if [ "$ENV_SELECTION" = "all" ]; then
        ENVS_TO_CREATE=("${CONFIG_ENV_NAMES[@]}")
    elif [ -n "$ENV_SELECTION" ]; then
        IFS=',' read -ra selected_envs <<< "$ENV_SELECTION"
        for env_name in "${selected_envs[@]}"; do
            env_name=$(printf '%s' "$env_name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            validate_identifier "$env_name" "Selected environment name"
            config_environment_index "$env_name" >/dev/null || \
                error "Selected environment is not configured: $env_name"
            for existing in "${ENVS_TO_CREATE[@]}"; do
                if [ "$existing" = "$env_name" ]; then
                    error "Selected environment is duplicated: $env_name"
                fi
            done
            ENVS_TO_CREATE+=("$env_name")
        done
    else
        for env_name in "${CONFIG_ENV_NAMES[@]}"; do
            enabled=$(config_environment_value "$env_name" enabled) || \
                error "Cannot read enabled flag for '$env_name'."
            if [ "$enabled" = "true" ]; then
                ENVS_TO_CREATE+=("$env_name")
            fi
        done
    fi

    if [ ${#ENVS_TO_CREATE[@]} -eq 0 ]; then
        error "Environment installation requested but no environments were selected. Use -E or enable at least one environment in YAML."
    fi
}

resolve_environment_plan

# Function to create a single Python virtual environment
# Optional 4th argument: python_version (e.g. 3.11) to use python3.11 -m venv; if empty, uses python3
# Optional 5th argument: requirements_no_deps file path; if set, installed after main requirements with pip --no-deps
create_single_env() {
    local env_name="$1"
    local requirements_file="$2"
    local venv_path="$3"
    local python_version="${4:-}"
    local requirements_no_deps_file="${5:-}"
    local safe_venv_path
    local activate_script="/home/gamma/activate_${env_name}.sh"
    local temp_activate=""

    cleanup_failed_environment() {
        if [ -n "$temp_activate" ]; then
            rm -f -- "$temp_activate"
        fi
        safe_remove_venv "$venv_path" >/dev/null 2>&1 || true
        dexec rm -f -- "$activate_script" >/dev/null 2>&1 || true
    }

    validate_identifier "$env_name" "Environment name"
    if ! safe_venv_path=$(validate_container_venv_path "$venv_path"); then
        return 1
    fi
    venv_path="$safe_venv_path"
    
    # Choose Python interpreter: python3.11, python3.10, etc., or default python3
    local python_bin="python3"
    if [ -n "$python_version" ]; then
        # Normalize: "3.11" -> python3.11; already "python3.11" -> use as-is
        if [[ "$python_version" =~ ^python ]]; then
            python_bin="$python_version"
        else
            python_bin="python${python_version}"
        fi
        log "   - Using Python: $python_bin"
    fi
    
    log "   - Creating environment '$env_name' at $venv_path..."

    # `python -m venv` succeeds even when the target already exists, leaving
    # stale editable VCS checkouts under <venv>/src. Always recreate managed
    # environments so changes of repository URL or pinned revision are applied
    # non-interactively and reproducibly.
    if ! safe_remove_venv "$venv_path"; then
        return 1
    fi
    if ! dexec "$python_bin" -m venv "$venv_path"; then
        cleanup_failed_environment
        return 1
    fi

    # Bootstrap packaging tools before installing requirements.
    # Python 3.12 removed pkgutil.ImpImporter; old setuptools/pkg_resources
    # releases still reference it and fail at import time.
    log "   - Bootstrapping pip/setuptools/wheel..."
    if ! dexec "$venv_path/bin/python" -m ensurepip --upgrade; then
        cleanup_failed_environment
        return 1
    fi

    if ! dexec "$venv_path/bin/python" -m pip install --no-cache-dir --upgrade "pip" "setuptools>=68" "wheel"; then
        cleanup_failed_environment
        return 1
    fi

    # Install packages
    log "   - Installing packages..."
    if ! dexec "$venv_path/bin/python" -m pip install --no-cache-dir -r "$requirements_file"; then
        cleanup_failed_environment
        return 1
    fi

    # Optional: install extra requirements with --no-deps (e.g. to avoid dependency conflicts)
    if [ -n "$requirements_no_deps_file" ]; then
        log "   - Installing extra packages (--no-deps)..."
        if ! dexec "$venv_path/bin/python" -m pip install --no-cache-dir --no-deps -r "$requirements_no_deps_file"; then
            cleanup_failed_environment
            return 1
        fi
    fi

    if ! dexec test -x "$venv_path/bin/python"; then
        cleanup_failed_environment
        return 1
    fi
    if ! dexec "$venv_path/bin/python" -c \
        'import os, sys; raise SystemExit(0 if os.path.realpath(sys.prefix) == os.path.realpath(sys.argv[1]) else 1)' \
        "$venv_path"; then
        cleanup_failed_environment
        return 1
    fi
    if ! dexec "$venv_path/bin/python" -m pip check; then
        cleanup_failed_environment
        return 1
    fi

    # Create activation helper script for this environment
    if ! temp_activate=$(mktemp "${TMPDIR:-/tmp}/cosiflow-activate-${env_name}.XXXXXX"); then
        cleanup_failed_environment
        return 1
    fi
    if ! {
        printf '%s\n' '#!/bin/bash'
        printf '# Helper script to activate the %s virtual environment\n' "$env_name"
        printf 'source %q\n' "$venv_path/bin/activate"
        printf '%s\n' 'echo "   - Activated Python environment: $VIRTUAL_ENV"'
        printf '%s\n' 'echo "       - Python path: $(command -v python)"'
    } > "$temp_activate"; then
        cleanup_failed_environment
        return 1
    fi
    if ! dexec_stdin tee "$activate_script" < "$temp_activate" >/dev/null; then
        rm -f -- "$temp_activate"
        cleanup_failed_environment
        return 1
    fi
    rm -f -- "$temp_activate"
    temp_activate=""
    if ! dexec chmod +x -- "$activate_script"; then
        cleanup_failed_environment
        return 1
    fi
    if ! dexec bash -n "$activate_script"; then
        cleanup_failed_environment
        return 1
    fi
    if ! dexec bash --noprofile --norc -c \
        'source "$1" >/dev/null && [ "$VIRTUAL_ENV" = "$2" ] && [ "$(command -v python)" = "$2/bin/python" ]' \
        bash "$activate_script" "$venv_path"; then
        cleanup_failed_environment
        return 1
    fi
    
    log "   - Environment '$env_name' created successfully."
    log "       - Activate with: source $activate_script"
    log "       - Or use: $venv_path/bin/python"
    
    return 0
}

create_configured_environment() {
    local env_name="$1"
    local req_path
    local req_no_deps
    local requirements_file
    local requirements_relative
    local requirements_nodeps_file=""
    local requirements_nodeps_relative
    local venv_path
    local description
    local python_version

    validate_identifier "$env_name" "Environment name"
    req_path=$(config_environment_value "$env_name" requirements) || return 1
    requirements_file=$(resolve_module_path "$req_path" file "Requirements file for $env_name") || return 1
    requirements_relative=$(module_relative_path "$requirements_file") || return 1
    requirements_file="$CONTAINER_MODULES_ROOT/$MODULE_NAME/$requirements_relative"
    req_no_deps=$(config_environment_value "$env_name" requirements_no_deps) || return 1
    if [ -n "$req_no_deps" ]; then
        requirements_nodeps_file=$(resolve_module_path "$req_no_deps" file "No-deps requirements file for $env_name") || return 1
        requirements_nodeps_relative=$(module_relative_path "$requirements_nodeps_file") || return 1
        requirements_nodeps_file="$CONTAINER_MODULES_ROOT/$MODULE_NAME/$requirements_nodeps_relative"
    fi
    venv_path=$(config_environment_value "$env_name" venv_path) || return 1
    description=$(config_environment_value "$env_name" description) || return 1
    python_version=$(config_environment_value "$env_name" python_version) || return 1

    if [ -n "$description" ]; then
        log "   - $description"
    fi
    create_single_env \
        "$env_name" \
        "$requirements_file" \
        "$venv_path" \
        "$python_version" \
        "$requirements_nodeps_file"
}

require_development_mounts() {
    dexec test -w /home/gamma/airflow/dags || \
        error "DAG directory is read-only. Restart with docker-compose.development.yaml."
    if [ -n "$PATH_PIPELINE" ]; then
        dexec test -w /home/gamma/airflow/pipeline || \
            error "Pipeline directory is read-only. Restart with docker-compose.development.yaml."
    fi
    if [ "$CREATE_ENV" = true ]; then
        dexec test -w "$CONTAINER_VENV_ROOT" || \
            error "Environment directory is read-only. Restart with docker-compose.development.yaml."
    fi
}

verify_container_link() {
    local link_path="$1"
    local expected_target="$2"
    local resolved

    dexec test -L "$link_path" || return 1
    dexec test -e "$link_path" || return 1
    resolved=$(dexec realpath -- "$link_path") || return 1
    [ "$resolved" = "$expected_target" ]
}

remove_container_path() {
    local path="$1"

    dexec rm -f -- "$path" || return 1
    dexec test ! -e "$path" || return 1
    dexec test ! -L "$path" || return 1
}

verify_container_path_absent() {
    local path="$1"

    dexec test ! -e "$path" || return 1
    dexec test ! -L "$path" || return 1
}

log "\n1.     Action: $ACTION module '$MODULE_NAME'"

# ==============================================================================
# REMOVE
# ==============================================================================
if [ "$ACTION" == "remove" ]; then
    require_development_mounts
    log "   - Removing module '$MODULE_NAME'..."

    # Removal still trusts configured module-relative paths and environment
    # targets. Validate all of them before the first unlink or deletion. A
    # Dockerfile is not required when removing an existing image.
    if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
        preflight_inputs false
    fi

    # Load config to know what to remove
    remove_envs=false
    remove_container=false

    if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
        log "   - Using configuration from: $(basename "$YAML_CONFIG")"

        install_mode=$(config_value install_mode) || error "Cannot read install mode."
        case "$install_mode" in
            container|both)
                remove_container=true
                ;;
        esac

        all_envs=()
        while IFS= read -r env_name; do
            [ -n "$env_name" ] && all_envs+=("$env_name")
        done < <(list_yaml_envs)
        if [ ${#all_envs[@]} -gt 0 ]; then
            remove_envs=true
            for env_name in "${all_envs[@]}"; do
                venv_path=$(config_environment_value "$env_name" venv_path) || error "Cannot read environment path."
                validate_container_venv_path "$venv_path" >/dev/null || exit 1
            done
        fi
    else
        # Legacy mode: remove everything
        remove_envs=true
        remove_container=true
    fi
    
    # Remove DAGs link
    log "   - Removing DAGs link..."
    remove_container_path "/home/gamma/airflow/dags/${MODULE_NAME}${EXTENSION_MODULE}" || \
        error "Failed to remove DAGs link."
    log "   - DAGs link removed."
    
    # Remove Pipeline scripts link (only if pipeline path was configured)
    if [ -n "$PATH_PIPELINE" ]; then
        log "   - Removing Pipeline scripts link..."
        remove_container_path "/home/gamma/airflow/pipeline/${MODULE_NAME}${EXTENSION_MODULE}" || \
            error "Failed to remove Pipeline scripts link."
        log "   - Pipeline scripts link removed."
    fi
    
    # Remove Python virtual environments (if configured)
    if [ "$remove_envs" = true ]; then
        if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
            log "   - Removing Python virtual environments..."
            if [ ${#all_envs[@]} -gt 0 ]; then
                for env_name in "${all_envs[@]}"; do
                    venv_path=$(config_environment_value "$env_name" "venv_path") || error "Cannot read environment path."
                    activate_script="/home/gamma/activate_${env_name}.sh"

                    safe_remove_venv "$venv_path" || error "Failed to remove environment '$env_name'."
                    verify_container_path_absent "$venv_path" || error "Environment '$env_name' still exists after removal."
                    remove_container_path "$activate_script" || error "Failed to remove activation helper for '$env_name'."
                    log "   - Removed environment '$env_name'"
                done
            else
                warning "   - No environments found in config."
            fi
        else
            # Legacy: remove default cosipy environment
            safe_remove_venv "$VENV_PATH" || error "Failed to remove legacy environment."
            verify_container_path_absent "$VENV_PATH" || error "Legacy environment still exists after removal."
            remove_container_path /home/gamma/activate_cosipy.sh || error "Failed to remove legacy activation helper."
            log "   - Removed default Python environment."
        fi
    else
        log "   - Skipping Python environments (not configured)."
    fi
    
    # Remove Docker image (if configured)
    if [ "$remove_container" = true ]; then
        log "   - Removing Docker image..."
        docker info >/dev/null 2>&1 || error "Docker daemon is unavailable; image removal was not verified."
        if docker image inspect "${MODULE_NAME}:latest" >/dev/null 2>&1; then
            docker rmi -f "${MODULE_NAME}:latest" >/dev/null || error "Failed to remove image ${MODULE_NAME}:latest."
            log "   - Removed image ${MODULE_NAME}:latest."
        else
            log "   - Image ${MODULE_NAME}:latest is already absent."
        fi
    else
        log "   - Skipping Docker image (not configured)."
    fi
    
    echo ""
    log "   - Module $MODULE_NAME removed from Airflow."
    exit 0
fi

# ==============================================================================
# INSTALL / UPDATE
# ==============================================================================
if [ "$ACTION" == "install" ] || [ "$ACTION" == "update" ]; then
    require_development_mounts

    if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
        log "   - Configuration loaded from: $(basename "$YAML_CONFIG")"
        install_mode=$(config_value install_mode) || error "Cannot read install mode."
        if [ -n "$install_mode" ]; then
            log "   - Install mode: $install_mode"
        else
            warning "   - Install mode: (empty or not found)"
        fi
        echo ""
    fi

    preflight_inputs

    log "2.  Linking module '$MODULE_NAME' into Airflow..."

    # 1. Link DAGs (Airflow needs the DAG definition)
    log "   - Linking DAGs..."
    if ! dexec ln -sfn -- \
        "$CONTAINER_MODULES_ROOT/$MODULE_NAME/$PATH_DAGS" \
        "/home/gamma/airflow/dags/${MODULE_NAME}${EXTENSION_MODULE}"; then
        error "Failed to link DAGs."
    fi
    if ! verify_container_link \
        "/home/gamma/airflow/dags/${MODULE_NAME}${EXTENSION_MODULE}" \
        "$CONTAINER_MODULES_ROOT/$MODULE_NAME/$PATH_DAGS"; then
        error "Failed to link DAGs."
    fi
    log "   - DAGs linked."

    # 2. Link Pipeline scripts (only if pipeline path is configured)
    if [ -n "$PATH_PIPELINE" ]; then
        log "   - Linking Pipeline scripts..."
        if ! dexec ln -sfn -- \
            "$CONTAINER_MODULES_ROOT/$MODULE_NAME/$PATH_PIPELINE" \
            "/home/gamma/airflow/pipeline/${MODULE_NAME}${EXTENSION_MODULE}"; then
            error "Failed to link Pipeline scripts."
        fi
        if ! verify_container_link \
            "/home/gamma/airflow/pipeline/${MODULE_NAME}${EXTENSION_MODULE}" \
            "$CONTAINER_MODULES_ROOT/$MODULE_NAME/$PATH_PIPELINE"; then
            error "Failed to link Pipeline scripts."
        fi
        log "   - Pipeline scripts linked."
    else
        log "   - Skipping Pipeline scripts (not configured)."
    fi

    # 3. Create Python Virtual Environment(s) (if requested)
    if [ "$CREATE_ENV" == true ]; then
        if [ "$ACTION" == "update" ]; then
            log "3.  Updating Python Virtual Environment(s)..."
        else
            log "3.  Creating Python Virtual Environment(s)..."
        fi
        
        if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
            log "   - Using $(basename "$YAML_CONFIG")"
            log "   - Environment plan: ${ENVS_TO_CREATE[*]}"
            success_count=0
            for env_name in "${ENVS_TO_CREATE[@]}"; do
                if ! create_configured_environment "$env_name"; then
                    error "Failed to create a usable environment for '$env_name'. Partial state was removed."
                fi
                success_count=$((success_count + 1))
                echo ""
            done
            log "   - Summary: $success_count environment(s) created and verified"
        else
            # Legacy mode: single environment with -r flag or default
            log "   - Using legacy single-environment mode"

            if ! REQUIREMENTS_FILE=$(resolve_module_path "$PATH_REQUIREMENTS" file "Legacy requirements file"); then
                exit 1
            fi
            REQUIREMENTS_RELATIVE=$(module_relative_path "$REQUIREMENTS_FILE") || exit 1
            REQUIREMENTS_FILE="$CONTAINER_MODULES_ROOT/$MODULE_NAME/$REQUIREMENTS_RELATIVE"
            if ! create_single_env "cosipy" "$REQUIREMENTS_FILE" "$VENV_PATH"; then
                error "Failed to create a usable legacy environment. Partial state was removed."
            fi
            echo ""
        fi
    fi

    # 4. Build/Prepare Docker Image (if requested)
    if [ "$BUILD_DOCKER" == true ]; then
        log "4.  Building Docker Image..."
        
        DOCKER_CONTEXT=$(resolve_module_path "$PATH_IMAGES" directory "Docker context path") || exit 1
        
        if [ -d "$MODULE_PATH" ] && [ -f "$DOCKER_CONTEXT/Dockerfile" ]; then
            log "   - Found Dockerfile in $DOCKER_CONTEXT"
            log "   - Building image '${MODULE_NAME}:latest'..."
            
            # Build the image on the HOST
            docker build -t "${MODULE_NAME}:latest" "$DOCKER_CONTEXT"
            
            if [ $? -eq 0 ]; then
                log "   - Image '${MODULE_NAME}:latest' built successfully."
            else
                error "Docker build failed."
            fi
        else
            error "Docker build requested but Dockerfile not found.\n       Checked path: $DOCKER_CONTEXT/Dockerfile"
        fi
    else
        log "4.  -  Skipping Docker image build (not requested)."
    fi

    log "Module $MODULE_NAME ready!"
    exit 0
fi

error "Unknown action: $ACTION. Use [install|remove|update]"
