#!/bin/bash
set -euo pipefail

action="${1:-}"
case "$action" in
    start)
        docker compose up -d gcn-client
        ;;
    stop)
        docker compose stop gcn-client
        ;;
    restart)
        docker compose restart gcn-client
        ;;
    status)
        exec docker compose ps gcn-client
        ;;
    *)
        printf 'Usage: %s start|stop|restart|status\n' "$0" >&2
        exit 2
        ;;
esac

audit_dir="../data/audit"
audit_file="$audit_dir/gcn-lifecycle.jsonl"
mkdir -p "$audit_dir"
touch "$audit_file"
chmod 600 "$audit_file"
printf '{"timestamp":"%s","actor":"%s","orchestrator":"docker-compose","service":"gcn-client","action":"%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(id -un)" "$action" >>"$audit_file"
