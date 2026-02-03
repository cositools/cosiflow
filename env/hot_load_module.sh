#!/bin/bash
# hot_load_module.sh
# Usage:
#   $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images]
#
# Options (paths are relative to module root unless absolute):
#   -d  path to DAGs directory (default: src/dags)
#   -p  path to pipeline directory (default: src/pipeline)
#   -f  path to Docker context directory containing Dockerfile (default: env)

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

# Parse positional args: module_name [action]
MODULE_NAME=$1
if [ "$2" = "install" ] || [ "$2" = "remove" ] || [ "$2" = "update" ]; then
    ACTION=$2
    shift 2
else
    ACTION=install
    shift 1
fi

# Parse options -d, -p, -f
while getopts "d:p:f:" opt; do
    case $opt in
        d) PATH_DAGS="$OPTARG" ;;
        p) PATH_PIPELINE="$OPTARG" ;;
        f) PATH_IMAGES="$OPTARG" ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
        *) echo "Usage: $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images]" >&2; exit 1 ;;
    esac
done

if [ -z "$MODULE_NAME" ]; then
    echo "Usage: $0 <module_name> [install|remove|update] -d [dags] -p [pipeline] -f [images]"
    echo ""
    echo "Options (paths relative to module root):"
    echo "  -d  path to DAGs directory (default: src/dags)"
    echo "  -p  path to pipeline directory (default: src/pipeline)"
    echo "  -f  path to Docker context directory (default: env)"
    exit 1
fi

# Helper to run docker exec as airflow user
dexec() {
    docker exec -u $CONTAINER_USER $CONTAINER_NAME "$@"
}

echo "🔹 Action: $ACTION module '$MODULE_NAME'"

# ==============================================================================
# REMOVE
# ==============================================================================
if [ "$ACTION" == "remove" ]; then
    echo "Removing module..."
    
    # Remove DAGs link
    dexec rm -f /home/gamma/airflow/dags/$MODULE_NAME$EXTENSION_MODULE
    echo "  - Removed DAGs link."

    # Remove Pipeline scripts link
    dexec rm -f /home/gamma/airflow/pipeline/$MODULE_NAME$EXTENSION_MODULE
    echo "  - Removed Pipeline scripts link."

    # Remove image
    docker rmi -f ${MODULE_NAME}:latest
    echo "  - Removed image ${MODULE_NAME}:latest."

    echo "✅ Module $MODULE_NAME removed from Airflow."
    exit 0
fi

# ==============================================================================
# INSTALL / UPDATE
# ==============================================================================
if [ "$ACTION" == "install" ] || [ "$ACTION" == "update" ]; then
    
    # 1. Link DAGs (Airflow needs the DAG definition)
    echo "1️⃣  Linking DAGs..."
    dexec ln -sfn /home/gamma/airflow/modules_pool/$MODULE_NAME/$PATH_DAGS /home/gamma/airflow/dags/$MODULE_NAME$EXTENSION_MODULE
    
    if [ $? -eq 0 ]; then
        echo "   ✅ DAGs linked."
    else
        echo "   ❌ Failed to link DAGs."
        exit 1
    fi

    # 2. Link Pipeline scripts
    echo "2️⃣  Linking Pipeline scripts..."
    dexec ln -sfn /home/gamma/airflow/modules_pool/$MODULE_NAME/$PATH_PIPELINE /home/gamma/airflow/pipeline/$MODULE_NAME$EXTENSION_MODULE
    
    if [ $? -eq 0 ]; then
        echo "   ✅ Pipeline scripts linked."
    else
        echo "   ❌ Failed to link Pipeline scripts."
        exit 1
    fi

    # 3. Build/Prepare Docker Image
    echo "3️⃣  Building Docker Image..."
    
    # Check directly in the workspace root
    MODULE_PATH="$WORKSPACE_ROOT/$MODULE_NAME"
    if [ "${PATH_IMAGES:0:1}" = "/" ]; then
        DOCKER_CONTEXT="$PATH_IMAGES"
    else
        DOCKER_CONTEXT="$MODULE_PATH/$PATH_IMAGES"
    fi
    
    if [ -d "$MODULE_PATH" ] && [ -f "$DOCKER_CONTEXT/Dockerfile" ]; then
        echo "   Found Dockerfile in $DOCKER_CONTEXT"
        echo "   Building image '${MODULE_NAME}:latest'..."
        
        # Build the image on the HOST
        docker build -t "${MODULE_NAME}:latest" "$DOCKER_CONTEXT"
        
        if [ $? -eq 0 ]; then
            echo "   ✅ Image '${MODULE_NAME}:latest' built successfully."
        else
            echo "   ❌ Docker build failed."
            exit 1
        fi
    else
        echo "   ⚠️  Could not find Dockerfile."
        echo "       Checked path: $DOCKER_CONTEXT/Dockerfile"
        echo "       Skipping build. Ensure image '${MODULE_NAME}:latest' exists manually."
    fi

    echo "🎉 Module $MODULE_NAME ready!"
    exit 0
fi

echo "❌ Unknown action: $ACTION. Use [install|remove|update]"
exit 1
