#!/bin/bash
set -euo pipefail

if [ $# -ne 0 ]; then
    printf "\n\033[31mUsage: bootstrap.sh (no arguments)\033[0m\n\n"
    exit 1
fi

MY_UID="$(id -u)"
MY_GID="$(id -g)"

echo "${MY_UID}"
echo "${MY_UID}"

echo -e "\n[INFO] Starting containers with UID=${MY_UID} and GID=${MY_GID}..."

docker compose build --build-arg MY_UID=${MY_UID} --build-arg MY_GID=${MY_GID}
# docker compose up