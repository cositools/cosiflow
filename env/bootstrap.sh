#!/bin/bash
set -euo pipefail

if [ $# -ne 0 ]; then
    printf "\n\033[31mUsage: bootstrap.sh (no arguments)\033[0m\n\n"
    exit 1
fi

MY_UID="$(id -u)"
MY_GID="$(id -g)"

echo -e "\n[INFO] Starting containers with UID=${MY_UID} and GID=${MY_GID}..."

HOST_UID="${MY_UID}" HOST_GID="${MY_GID}" docker compose build
# docker compose up