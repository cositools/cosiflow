#!/bin/sh
set -eu

python -m mkdocs build --strict

linkcheck_state="$(mktemp -d "${TMPDIR:-/tmp}/cosiflow-linkcheck.XXXXXX")"
server_pid=""
cleanup() {
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf "$linkcheck_state"
}
trap cleanup EXIT HUP INT TERM

linkcheck_port="$(python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
python -m http.server "$linkcheck_port" \
    --bind 127.0.0.1 \
    --directory site \
    >"$linkcheck_state/http-server.log" 2>&1 &
server_pid=$!

python -c 'import sys, time, urllib.request
url = sys.argv[1]
for _ in range(50):
    try:
        urllib.request.urlopen(url, timeout=1).close()
        break
    except Exception:
        time.sleep(0.1)
else:
    raise SystemExit("documentation test server did not start")' \
    "http://127.0.0.1:$linkcheck_port/"

XDG_CONFIG_HOME="$linkcheck_state/config" \
XDG_DATA_HOME="$linkcheck_state/data" \
linkchecker \
    --no-warnings \
    --check-extern \
    "http://127.0.0.1:$linkcheck_port/" \
    >"$linkcheck_state/linkchecker.log" 2>&1 || linkcheck_status=$?

linkcheck_status="${linkcheck_status:-0}"
cat "$linkcheck_state/linkchecker.log"
if grep -q '^Result[[:space:]]*Error:' "$linkcheck_state/linkchecker.log"; then
    linkcheck_status=1
fi

cleanup
trap - EXIT HUP INT TERM
exit "$linkcheck_status"
