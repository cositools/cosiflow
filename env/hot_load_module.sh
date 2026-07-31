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

# Resolve absolute paths to avoid confusion depending on where script is run from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")" # Go up two levels: cosiflow/env -> cosiflow -> workspace

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

##########################################################################
# HELPER FUNCTIONS
##########################################################################

# Helper to run docker exec as airflow user
dexec() {
    docker exec -u $CONTAINER_USER $CONTAINER_NAME "$@"
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
    echo -e "\033[31m$1\033[0m"
    exit 1
}

# Function to parse YAML and extract top-level configuration values
parse_yaml_config() {
    local yaml_file="$1"
    local field="$2"  # install_mode, or paths subfields: dags, pipeline, images
    
    if [ ! -f "$yaml_file" ]; then
        return 1
    fi
    
    local in_paths=false
    local paths_indent=0
    
    while IFS= read -r line; do
        # Remove comments but keep structure
        line=$(echo "$line" | sed 's/#.*$//')
        
        # Skip empty lines
        if [[ -z "${line// /}" ]]; then
            continue
        fi
        
        # Check for install_mode
        if [ "$field" = "install_mode" ]; then
            # Try with quotes first
            if [[ "$line" =~ ^install_mode:[[:space:]]*[\"'](.+)[\"'] ]]; then
                echo "${BASH_REMATCH[1]}"
                return 0
            # Try without quotes
            elif [[ "$line" =~ ^install_mode:[[:space:]]+(.+) ]]; then
                local val="${BASH_REMATCH[1]}"
                # Remove any trailing quotes or spaces
                val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//" | sed 's/[[:space:]]*$//')
                echo "$val"
                return 0
            fi
        fi
        
        # Check for paths section
        if [[ "$line" =~ ^paths: ]]; then
            in_paths=true
            paths_indent=$(echo "$line" | sed 's/[^ ].*//' | wc -c)
            ((paths_indent--))
            continue
        fi
        
        # If we hit another top-level key, stop looking in paths
        if [ "$in_paths" = true ] && [[ "$line" =~ ^[a-zA-Z_][a-zA-Z0-9_]*: ]] && [[ ! "$line" =~ ^[[:space:]]+ ]]; then
            in_paths=false
            continue
        fi
        
        # Extract path values
        if [ "$in_paths" = true ]; then
            case "$field" in
                dags)
                    if [[ "$line" =~ ^[[:space:]]+dags:[[:space:]]*[\"'](.+)[\"'] ]]; then
                        echo "${BASH_REMATCH[1]}"
                        return 0
                    elif [[ "$line" =~ ^[[:space:]]+dags:[[:space:]]+(.+) ]]; then
                        local val="${BASH_REMATCH[1]}"
                        val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                        echo "$val"
                        return 0
                    fi
                    ;;
                pipeline)
                    if [[ "$line" =~ ^[[:space:]]+pipeline:[[:space:]]*[\"'](.+)[\"'] ]]; then
                        echo "${BASH_REMATCH[1]}"
                        return 0
                    elif [[ "$line" =~ ^[[:space:]]+pipeline:[[:space:]]+(.+) ]]; then
                        local val="${BASH_REMATCH[1]}"
                        val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                        echo "$val"
                        return 0
                    fi
                    ;;
                images)
                    if [[ "$line" =~ ^[[:space:]]+images:[[:space:]]*[\"'](.+)[\"'] ]]; then
                        echo "${BASH_REMATCH[1]}"
                        return 0
                    elif [[ "$line" =~ ^[[:space:]]+images:[[:space:]]+(.+) ]]; then
                        local val="${BASH_REMATCH[1]}"
                        val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                        echo "$val"
                        return 0
                    fi
                    ;;
            esac
        fi
    done < "$yaml_file"
    
    return 1
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
    yaml_file=$(find "$module_path" -maxdepth 1 -name "*.config.yaml" -type f 2>/dev/null | head -n 1)
    if [ -n "$yaml_file" ] && [ -f "$yaml_file" ]; then
        echo "$yaml_file"
        return 0
    fi
    
    # Try to find any *.config.yaml file in env/ subdirectory
    if [ -d "$module_path/env" ]; then
        yaml_file=$(find "$module_path/env" -maxdepth 1 -name "*.config.yaml" -type f 2>/dev/null | head -n 1)
        if [ -n "$yaml_file" ] && [ -f "$yaml_file" ]; then
            echo "$yaml_file"
            return 0
        fi
    fi
    
    return 1
}

# Function to load configuration from YAML file
# Usage: load_yaml_config <module_path> [config_file_path]
# If config_file_path is given, it is used; otherwise config is auto-detected in module.
load_yaml_config() {
    local module_path="$1"
    local yaml_file
    
    if [ -n "$2" ] && [ -f "$2" ]; then
        yaml_file="$2"
    else
        # Find YAML config file
        local found_yaml=$(find_yaml_config "$module_path")
        if [ $? -ne 0 ] || [ -z "$found_yaml" ]; then
            return 1
        fi
        yaml_file="$found_yaml"
    fi
    
    # Load install_mode
    local install_mode=$(parse_yaml_config "$yaml_file" "install_mode")
    if [ -n "$install_mode" ]; then
        case "$install_mode" in
            container)
                BUILD_DOCKER=true
                CREATE_ENV=false
                ;;
            environment)
                BUILD_DOCKER=false
                CREATE_ENV=true
                ;;
            both)
                BUILD_DOCKER=true
                CREATE_ENV=true
                ;;
            none)
                BUILD_DOCKER=false
                CREATE_ENV=false
                ;;
        esac
    fi
    
    # Load paths
    local dags_path=$(parse_yaml_config "$yaml_file" "dags")
    if [ -n "$dags_path" ]; then
        PATH_DAGS="$dags_path"
    fi
    
    local pipeline_path=$(parse_yaml_config "$yaml_file" "pipeline")
    if [ -n "$pipeline_path" ]; then
        PATH_PIPELINE="$pipeline_path"
    fi
    
    local images_path=$(parse_yaml_config "$yaml_file" "images")
    if [ -n "$images_path" ]; then
        PATH_IMAGES="$images_path"
    fi
    
    return 0
}

##########################################################################
# MAIN SCRIPT
##########################################################################

# Parse positional args: module_name [action]
MODULE_NAME=$1
if [ "$2" = "install" ] || [ "$2" = "remove" ] || [ "$2" = "update" ]; then
    ACTION=$2
    shift 2
else
    ACTION=install
    shift 1
fi

# Load configuration from YAML if module exists and YAML file is present
MODULE_PATH="$WORKSPACE_ROOT/$MODULE_NAME"
if [ -d "$MODULE_PATH" ]; then
    # Load YAML config as defaults (will be overridden by CLI options)
    load_yaml_config "$MODULE_PATH"
fi

# Parse options -d, -p, -f, -e, -r, -a, -E, -c
# CLI options override YAML configuration
while getopts "d:p:f:er:aE:c:" opt; do
    case $opt in
        d) PATH_DAGS="$OPTARG" ;;
        p) PATH_PIPELINE="$OPTARG" ;;
        f) PATH_IMAGES="$OPTARG" ;;
        e) CREATE_ENV=true ;;
        r) PATH_REQUIREMENTS="$OPTARG" ;;
        a) CREATE_ENV=true; BUILD_DOCKER=true ;;
        E) CREATE_ENV=true; ENV_SELECTION="$OPTARG" ;;
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

# If -c was used: resolve to absolute path and reload config from that file
if [ -n "$CONFIG_FILE" ]; then
    if [ ! -f "$CONFIG_FILE" ]; then
        error "Config file not found: $CONFIG_FILE"
    fi
    CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"
    load_yaml_config "$MODULE_PATH" "$CONFIG_FILE"
fi

# Helper to run docker exec as airflow user
dexec() {
    docker exec -u $CONTAINER_USER $CONTAINER_NAME "$@"
}

# Function to parse YAML and extract environment configurations
# Uses simple pattern matching (works without yq dependency)
parse_yaml_envs() {
    local yaml_file="$1"
    local env_name="$2"
    local field="$3"  # requirements, venv_path, enabled, description, python_version
    
    if [ ! -f "$yaml_file" ]; then
        return 1
    fi
    
    # Simple YAML parsing - look for the environment block
    local in_environments=false
    local in_target_env=false
    local env_indent_level=0
    
    while IFS= read -r line; do
        local original_line="$line"
        # Remove comments but keep the line structure
        line=$(echo "$line" | sed 's/#.*$//')
        
        # Skip completely empty lines
        if [[ -z "${line// /}" ]]; then
            continue
        fi
        
        # Detect environments section
        if [[ "$line" =~ ^environments: ]]; then
            in_environments=true
            continue
        fi
        
        # If we hit another top-level key, stop looking
        if [ "$in_environments" = true ] && [[ "$line" =~ ^[a-zA-Z_][a-zA-Z0-9_]*: ]] && [[ ! "$line" =~ ^[[:space:]]+ ]]; then
            break
        fi
        
        # Check if we're entering the target environment block
        if [ "$in_environments" = true ] && [[ "$line" =~ ^[[:space:]]+${env_name}: ]]; then
            in_target_env=true
            # Count leading spaces to determine indent level
            env_indent_level=$(echo "$line" | sed 's/[^ ].*//' | wc -c)
            ((env_indent_level--))
            continue
        fi
        
        # Check if we're leaving the target environment block (another env at same or less indent)
        if [ "$in_target_env" = true ]; then
            local current_indent=$(echo "$line" | sed 's/[^ ].*//' | wc -c)
            ((current_indent--))
            
            # If we hit another environment at same or less indent, we've left our target
            if [[ "$line" =~ ^[[:space:]]*[a-zA-Z0-9_]+: ]] && [ $current_indent -le $env_indent_level ] && [[ ! "$line" =~ ^[[:space:]]+${env_name}: ]]; then
                in_target_env=false
                continue
            fi
            
            # Extract field value if we're in the target environment
            if [ "$in_target_env" = true ]; then
                case "$field" in
                    requirements)
                        if [[ "$line" =~ ^[[:space:]]+requirements:[[:space:]]*[\"'](.+)[\"'] ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+requirements:[[:space:]]+(.+) ]]; then
                            local val="${BASH_REMATCH[1]}"
                            val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                            echo "$val"
                            return 0
                        fi
                        ;;
                    venv_path)
                        if [[ "$line" =~ ^[[:space:]]+venv_path:[[:space:]]*[\"'](.+)[\"'] ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+venv_path:[[:space:]]+(.+) ]]; then
                            local val="${BASH_REMATCH[1]}"
                            val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                            echo "$val"
                            return 0
                        fi
                        ;;
                    enabled)
                        if [[ "$line" =~ ^[[:space:]]+enabled:[[:space:]]*(true|false) ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+enabled:[[:space:]]+[\"']?(true|false)[\"']? ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        fi
                        ;;
                    description)
                        if [[ "$line" =~ ^[[:space:]]+description:[[:space:]]*[\"'](.+)[\"'] ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+description:[[:space:]]+(.+) ]]; then
                            local val="${BASH_REMATCH[1]}"
                            val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                            echo "$val"
                            return 0
                        fi
                        ;;
                    python_version)
                        if [[ "$line" =~ ^[[:space:]]+python_version:[[:space:]]*[\"']?([0-9.]+)[\"']? ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+python_version:[[:space:]]+(.+) ]]; then
                            local val="${BASH_REMATCH[1]}"
                            val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                            echo "$val"
                            return 0
                        fi
                        ;;
                    requirements_no_deps)
                        if [[ "$line" =~ ^[[:space:]]+requirements_no_deps:[[:space:]]*[\"'](.+)[\"'] ]]; then
                            echo "${BASH_REMATCH[1]}"
                            return 0
                        elif [[ "$line" =~ ^[[:space:]]+requirements_no_deps:[[:space:]]+(.+) ]]; then
                            local val="${BASH_REMATCH[1]}"
                            val=$(echo "$val" | sed "s/^[\"']//;s/[\"']$//")
                            echo "$val"
                            return 0
                        fi
                        ;;
                esac
            fi
        fi
    done < "$yaml_file"
    
    return 1
}

# Function to list all environments from YAML
list_yaml_envs() {
    local yaml_file="$1"
    
    if [ ! -f "$yaml_file" ]; then
        return 1
    fi
    
    # Extract environment names (lines with "env_name:" that are indented under "environments:")
    # Only match keys with exactly 2 spaces of indent (environment names, not their fields)
    local in_environments=false
    while IFS= read -r line; do
        # Remove comments but preserve structure
        local original_line="$line"
        line=$(echo "$line" | sed 's/#.*$//')
        
        # Skip empty lines
        if [[ -z "${line// /}" ]]; then
            continue
        fi
        
        # Check if we're in the environments section
        if [[ "$line" =~ ^environments: ]]; then
            in_environments=true
            continue
        fi
        
        # If we hit another top-level key (no leading spaces), stop
        if [ "$in_environments" = true ] && [[ "$line" =~ ^[a-zA-Z_][a-zA-Z0-9_]*: ]] && [[ ! "$line" =~ ^[[:space:]]+ ]]; then
            break
        fi
        
        # Extract environment names (keys with exactly 2 spaces indent, not 4+ which are fields)
        if [ "$in_environments" = true ]; then
            # Count leading spaces
            local leading_spaces=$(echo "$line" | sed 's/[^ ].*//' | wc -c)
            ((leading_spaces--))
            
            # Only match lines with exactly 2 spaces (environment names)
            # Match pattern: exactly 2 spaces, then a key name ending with colon
            if [ $leading_spaces -eq 2 ] && [[ "$line" =~ ^[[:space:]][[:space:]]([a-zA-Z0-9_]+):[[:space:]]*$ ]]; then
                echo "${BASH_REMATCH[1]}"
            elif [ $leading_spaces -eq 2 ] && [[ "$line" =~ ^[[:space:]][[:space:]]([a-zA-Z0-9_]+): ]]; then
                echo "${BASH_REMATCH[1]}"
            fi
        fi
    done < "$yaml_file"
}

# Function to create a single Python virtual environment
# Optional 4th argument: python_version (e.g. 3.11) to use python3.11 -m venv; if empty, uses python3
# Optional 5th argument: requirements_no_deps file path; if set, installed after main requirements with pip --no-deps
create_single_env() {
    local env_name="$1"
    local requirements_file="$2"
    local venv_path="$3"
    local python_version="${4:-}"
    local requirements_no_deps_file="${5:-}"
    
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
    dexec rm -rf "$venv_path"
    dexec "$python_bin" -m venv "$venv_path"

    if [ $? -ne 0 ]; then
        error "    Failed to create virtual environment for '$env_name'."
    fi

    # Bootstrap packaging tools before installing requirements.
    # Python 3.12 removed pkgutil.ImpImporter; old setuptools/pkg_resources
    # releases still reference it and fail at import time.
    log "   - Bootstrapping pip/setuptools/wheel..."
    dexec "$venv_path/bin/python" -m ensurepip --upgrade
    if [ $? -ne 0 ]; then
        error "   - Failed to bootstrap pip with ensurepip for '$env_name'."
    fi

    dexec "$venv_path/bin/python" -m pip install --no-cache-dir --upgrade "pip" "setuptools>=68" "wheel"
    if [ $? -ne 0 ]; then
        error "   - Failed to upgrade pip/setuptools/wheel for '$env_name'."
    fi
    
    # Copy requirements file to container
    local temp_req="/tmp/requirements_${env_name}.txt"
    log "   - Copying requirements file to container..."
    docker cp "$requirements_file" "$CONTAINER_NAME:$temp_req"
    
    if [ $? -ne 0 ]; then
        error "   - Failed to copy requirements file."
    fi
    
    # Install packages
    log "   - Installing packages..."
    dexec "$venv_path/bin/python" -m pip install --no-cache-dir -r "$temp_req"
    
    local install_status=$?
    
    # Cleanup main requirements
    dexec rm -f "$temp_req"
    
    if [ $install_status -ne 0 ]; then
        error "   - Failed to install packages for '$env_name'."
    fi

    dexec "$venv_path/bin/python" -c "import pkg_resources" 2>/dev/null
    if [ $? -ne 0 ]; then
        error "   - pkg_resources is not importable in '$env_name' after installing setuptools."
    fi
    
    # Optional: install extra requirements with --no-deps (e.g. to avoid dependency conflicts)
    if [ -n "$requirements_no_deps_file" ] && [ -f "$requirements_no_deps_file" ]; then
        local temp_nodeps="/tmp/requirements_${env_name}_nodeps.txt"
        log "   - Installing extra packages (--no-deps)..."
        docker cp "$requirements_no_deps_file" "$CONTAINER_NAME:$temp_nodeps"
        if [ $? -ne 0 ]; then
            error "   - Failed to copy no-deps requirements file."
        fi

        dexec "$venv_path/bin/python" -m pip install --no-cache-dir --no-deps -r "$temp_nodeps"
        local nodeps_install_status=$?
        dexec rm -f "$temp_nodeps"

        if [ $nodeps_install_status -ne 0 ]; then
            error "   - Failed to install no-deps packages for '$env_name'."
        fi
    fi
    
    # Create activation helper script for this environment
    local activate_script="/home/gamma/activate_${env_name}.sh"
    dexec bash -c "cat > $activate_script << 'EOF'
#!/bin/bash
# Helper script to activate the $env_name virtual environment
source $venv_path/bin/activate
log \"   - Activated Python environment: \$VIRTUAL_ENV\"
log \"       - Python path: \$(which python)\"
EOF
        chmod +x $activate_script"
    
    log "   - Environment '$env_name' created successfully."
    log "       - Activate with: source $activate_script"
    log "       - Or use: $venv_path/bin/python"
    
    return 0
}

log "\n1.     Action: $ACTION module '$MODULE_NAME'"

# ==============================================================================
# REMOVE
# ==============================================================================
if [ "$ACTION" == "remove" ]; then
    log "   - Removing module '$MODULE_NAME'..."
    
    MODULE_PATH="$WORKSPACE_ROOT/$MODULE_NAME"
    if [ -n "$CONFIG_FILE" ]; then
        YAML_CONFIG="$CONFIG_FILE"
    else
        YAML_CONFIG=$(find_yaml_config "$MODULE_PATH")
    fi
    
    # Load config to know what to remove
    remove_envs=false
    remove_container=false
    
    if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
        log "   - Using configuration from: $(basename "$YAML_CONFIG")"
        
        # Check what was configured to install
        install_mode=$(parse_yaml_config "$YAML_CONFIG" "install_mode")
        case "$install_mode" in
            container|both)
                remove_container=true
                ;;
        esac
        
        # Check if environments were configured
        all_envs=($(list_yaml_envs "$YAML_CONFIG"))
        if [ ${#all_envs[@]} -gt 0 ]; then
            remove_envs=true
        fi
        
        # Load paths from config
        dags_path=$(parse_yaml_config "$YAML_CONFIG" "dags")
        if [ -n "$dags_path" ]; then
            PATH_DAGS="$dags_path"
        fi
        
        pipeline_path=$(parse_yaml_config "$YAML_CONFIG" "pipeline")
        if [ -n "$pipeline_path" ]; then
            PATH_PIPELINE="$pipeline_path"
        fi
    else
        # Legacy mode: remove everything
        remove_envs=true
        remove_container=true
    fi
    
    # Remove DAGs link
    log "   - Removing DAGs link..."
    dexec rm -f /home/gamma/airflow/dags/$MODULE_NAME$EXTENSION_MODULE 2>/dev/null
    log "   - DAGs link removed."
    
    # Remove Pipeline scripts link (only if pipeline path was configured)
    if [ -n "$PATH_PIPELINE" ]; then
        log "   - Removing Pipeline scripts link..."
        dexec rm -f /home/gamma/airflow/pipeline/$MODULE_NAME$EXTENSION_MODULE 2>/dev/null
        log "   - Pipeline scripts link removed."
    fi
    
    # Remove Python virtual environments (if configured)
    if [ "$remove_envs" = true ]; then
        if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
            log "   - Removing Python virtual environments..."
            all_envs=($(list_yaml_envs "$YAML_CONFIG"))
            if [ ${#all_envs[@]} -gt 0 ]; then
                for env_name in "${all_envs[@]}"; do
                    venv_path_yaml=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "venv_path")
                    venv_path="${venv_path_yaml:-/home/gamma/envs/$env_name}"
                    activate_script="/home/gamma/activate_${env_name}.sh"
                    
                    dexec rm -rf "$venv_path" 2>/dev/null
                    dexec rm -f "$activate_script" 2>/dev/null
                    log "   - Removed environment '$env_name'"
                done
            else
                warning "   - No environments found in config."
            fi
        else
            # Legacy: remove default cosipy environment
            dexec rm -rf "$VENV_PATH" 2>/dev/null
            dexec rm -f /home/gamma/activate_cosipy.sh 2>/dev/null
            log "   - Removed default Python environment."
        fi
    else
        log "   - Skipping Python environments (not configured)."
    fi
    
    # Remove Docker image (if configured)
    if [ "$remove_container" = true ]; then
        log "   - Removing Docker image..."
        docker rmi -f ${MODULE_NAME}:latest 2>/dev/null
        if [ $? -eq 0 ]; then
            log "   - Removed image ${MODULE_NAME}:latest."
        else
            warning "   - Image ${MODULE_NAME}:latest not found or already removed."
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
    
    # Find YAML config file (or use -c path)
    if [ -n "$CONFIG_FILE" ]; then
        YAML_CONFIG="$CONFIG_FILE"
    else
        YAML_CONFIG=$(find_yaml_config "$MODULE_PATH")
    fi
    
    if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ]; then
        log "   - Configuration loaded from: $(basename "$YAML_CONFIG")"
        install_mode=$(parse_yaml_config "$YAML_CONFIG" "install_mode")
        
        # Debug: check what was read
        if [ -z "$install_mode" ]; then
            warning "   - Warning: install_mode not found or empty in YAML"
            warning "   - Trying direct grep..."
            install_mode=$(grep -E "^install_mode:" "$YAML_CONFIG" | sed 's/^install_mode:[[:space:]]*//' | sed 's/[[:space:]]*$//' | head -n 1)
        fi
        
        if [ -n "$install_mode" ]; then
            log "   - Install mode: $install_mode"
        else
            warning "   - Install mode: (empty or not found)"
        fi
        echo ""
        
        # Apply install_mode settings directly (in case load_yaml_config wasn't called earlier or didn't find the file)
        if [ -n "$install_mode" ]; then
            case "$install_mode" in
                container)
                    BUILD_DOCKER=true
                    CREATE_ENV=false
                    ;;
                environment)
                    BUILD_DOCKER=false
                    CREATE_ENV=true
                    ;;
                both)
                    BUILD_DOCKER=true
                    CREATE_ENV=true
                    ;;
                none)
                    BUILD_DOCKER=false
                    CREATE_ENV=false
                    ;;
            esac
            # Debug output
            log "   - Applied settings: BUILD_DOCKER=$BUILD_DOCKER, CREATE_ENV=$CREATE_ENV"
            echo ""
        else
            warning "   - Cannot apply install_mode: value is empty"
            echo ""
        fi
        
        # Reload config to ensure we have latest paths (in case YAML was found after initial load)
        load_yaml_config "$MODULE_PATH"
        
        # Ensure install_mode is still applied after load_yaml_config (it might override)
        if [ -n "$install_mode" ]; then
            case "$install_mode" in
                container)
                    BUILD_DOCKER=true
                    CREATE_ENV=false
                    ;;
                environment)
                    BUILD_DOCKER=false
                    CREATE_ENV=true
                    ;;
                both)
                    BUILD_DOCKER=true
                    CREATE_ENV=true
                    ;;
                none)
                    BUILD_DOCKER=false
                    CREATE_ENV=false
                    ;;
            esac
        fi
    fi
    
    log "2.  Linking module '$MODULE_NAME' into Airflow..."

    # 1. Link DAGs (Airflow needs the DAG definition)
    log "   - Linking DAGs..."
    dexec ln -sfn /home/gamma/airflow/modules_pool/$MODULE_NAME/$PATH_DAGS /home/gamma/airflow/dags/$MODULE_NAME$EXTENSION_MODULE
    
    if [ $? -eq 0 ]; then
        log "   - DAGs linked."
    else
        error "Failed to link DAGs."
    fi

    # 2. Link Pipeline scripts (only if pipeline path is configured)
    if [ -n "$PATH_PIPELINE" ]; then
        log "   - Linking Pipeline scripts..."
        dexec ln -sfn /home/gamma/airflow/modules_pool/$MODULE_NAME/$PATH_PIPELINE /home/gamma/airflow/pipeline/$MODULE_NAME$EXTENSION_MODULE
        
        if [ $? -eq 0 ]; then
            log "   - Pipeline scripts linked."
        else
            error "Failed to link Pipeline scripts."
        fi
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
        
        # Check if YAML config exists and -E flag was used (multi-env mode)
        if [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ] && [ -n "$ENV_SELECTION" ]; then
            log "   - Using multi-environment mode with $(basename "$YAML_CONFIG")"
            
            # Determine which environments to create
            envs_to_create=()
            
            if [ "$ENV_SELECTION" = "all" ]; then
                # Get all environments from YAML
                envs_to_create=($(list_yaml_envs "$YAML_CONFIG"))
            else
                # Parse comma-separated list
                IFS=',' read -ra envs_to_create <<< "$ENV_SELECTION"
            fi
            
            if [ ${#envs_to_create[@]} -eq 0 ]; then
                warning "   - No environments found or specified."
                log "       Skipping environment creation."
            else
                success_count=0
                fail_count=0
                
                for env_name in "${envs_to_create[@]}"; do
                    env_name=$(echo "$env_name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                    
                    # Extract environment configuration from YAML
                    req_path=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "requirements")
                    venv_path_yaml=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "venv_path")
                    enabled=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "enabled")
                    description=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "description")
                    python_version=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "python_version")
                    req_no_deps=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "requirements_no_deps")
                    
                    if [ -z "$req_path" ]; then
                        warning "   - Environment '$env_name' not found in YAML or missing requirements."
                        ((fail_count++))
                        continue
                    fi
                    
                    # Resolve requirements file path
                    if [ "${req_path:0:1}" = "/" ]; then
                        REQUIREMENTS_FILE="$req_path"
                    else
                        REQUIREMENTS_FILE="$MODULE_PATH/$req_path"
                    fi
                    
                    # Resolve requirements_no_deps path (optional)
                    REQUIREMENTS_NODEPS_FILE=""
                    if [ -n "$req_no_deps" ]; then
                        if [ "${req_no_deps:0:1}" = "/" ]; then
                            REQUIREMENTS_NODEPS_FILE="$req_no_deps"
                        else
                            REQUIREMENTS_NODEPS_FILE="$MODULE_PATH/$req_no_deps"
                        fi
                    fi
                    
                    # Use venv_path from YAML or default
                    final_venv_path="${venv_path_yaml:-/home/gamma/envs/$env_name}"
                    
                    # Show description if available
                    if [ -n "$description" ]; then
                        log "   - $description"
                    fi
                    
                    # Check if requirements file exists
                    if [ ! -f "$REQUIREMENTS_FILE" ]; then
                        warning "   - Requirements file not found: $REQUIREMENTS_FILE"
                        log "       Skipping environment '$env_name'."
                        ((fail_count++))
                        continue
                    fi
                    
                    # Create the environment
                    if create_single_env "$env_name" "$REQUIREMENTS_FILE" "$final_venv_path" "$python_version" "$REQUIREMENTS_NODEPS_FILE"; then
                        ((success_count++))
                    else
                        ((fail_count++))
                    fi
                    echo ""
                done
                
                log "   - Summary: $success_count environment(s) created successfully"
                if [ $fail_count -gt 0 ]; then
                    warning "   - $fail_count environment(s) failed"
                fi
            fi
            
        elif [ -n "$YAML_CONFIG" ] && [ -f "$YAML_CONFIG" ] && [ -z "$ENV_SELECTION" ]; then
            # YAML exists but no -E flag: use enabled environments
            log "   - Using $(basename "$YAML_CONFIG") (installing enabled environments)"
            
            all_envs=($(list_yaml_envs "$YAML_CONFIG"))
            
            # Debug: show what was found
            if [ ${#all_envs[@]} -eq 0 ]; then
                warning "   - No environments found in YAML file: $YAML_CONFIG"
            else
                log "   - Found ${#all_envs[@]} environment(s): ${all_envs[*]}"
            fi
            
            envs_to_create=()
            
            for env_name in "${all_envs[@]}"; do
                enabled=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "enabled")
                if [ "$enabled" = "true" ]; then
                    envs_to_create+=("$env_name")
                fi
            done
            
            if [ ${#envs_to_create[@]} -eq 0 ]; then
                if [ ${#all_envs[@]} -eq 0 ]; then
                    warning "   - No environments found in YAML."
                else
                    warning "   - No enabled environments found in YAML (found ${#all_envs[@]} environment(s) but none are enabled)."
                fi
                log "       Use -E flag to specify environments or enable them in YAML."
            else
                success_count=0
                fail_count=0
                
                for env_name in "${envs_to_create[@]}"; do
                    req_path=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "requirements")
                    venv_path_yaml=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "venv_path")
                    description=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "description")
                    python_version=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "python_version")
                    req_no_deps=$(parse_yaml_envs "$YAML_CONFIG" "$env_name" "requirements_no_deps")
                    
                    if [ "${req_path:0:1}" = "/" ]; then
                        REQUIREMENTS_FILE="$req_path"
                    else
                        REQUIREMENTS_FILE="$MODULE_PATH/$req_path"
                    fi
                    
                    REQUIREMENTS_NODEPS_FILE=""
                    if [ -n "$req_no_deps" ]; then
                        if [ "${req_no_deps:0:1}" = "/" ]; then
                            REQUIREMENTS_NODEPS_FILE="$req_no_deps"
                        else
                            REQUIREMENTS_NODEPS_FILE="$MODULE_PATH/$req_no_deps"
                        fi
                    fi
                    
                    final_venv_path="${venv_path_yaml:-/home/gamma/envs/$env_name}"
                    
                    if [ -n "$description" ]; then
                        log "   - $description"
                    fi
                    
                    if [ ! -f "$REQUIREMENTS_FILE" ]; then
                        warning "   - Requirements file not found: $REQUIREMENTS_FILE"
                        ((fail_count++))
                        continue
                    fi
                    
                    if create_single_env "$env_name" "$REQUIREMENTS_FILE" "$final_venv_path" "$python_version" "$REQUIREMENTS_NODEPS_FILE"; then
                        ((success_count++))
                    else
                        ((fail_count++))
                    fi
                    echo ""
                done
                
                log "   - Summary: $success_count environment(s) created successfully"
                if [ $fail_count -gt 0 ]; then
                    warning "   - $fail_count environment(s) failed"
                fi
            fi
            
        else
            # Legacy mode: single environment with -r flag or default
            log "   - Using legacy single-environment mode"
            
            if [ "${PATH_REQUIREMENTS:0:1}" = "/" ]; then
                REQUIREMENTS_FILE="$PATH_REQUIREMENTS"
            else
                REQUIREMENTS_FILE="$MODULE_PATH/$PATH_REQUIREMENTS"
            fi
            
            if [ ! -f "$REQUIREMENTS_FILE" ]; then
                warning "   - Requirements file not found: $REQUIREMENTS_FILE"
                log "       Skipping environment creation."
            else
                if create_single_env "cosipy" "$REQUIREMENTS_FILE" "$VENV_PATH"; then
                    echo ""
                else
                    exit 1
                fi
            fi
        fi
    fi

    # 4. Build/Prepare Docker Image (if requested)
    if [ "$BUILD_DOCKER" == true ]; then
        log "4.  Building Docker Image..."
        
        # Resolve Docker context path
        if [ "${PATH_IMAGES:0:1}" = "/" ]; then
            DOCKER_CONTEXT="$PATH_IMAGES"
        else
            DOCKER_CONTEXT="$MODULE_PATH/$PATH_IMAGES"
        fi
        
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
