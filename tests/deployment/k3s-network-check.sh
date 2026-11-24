#!/usr/bin/env bash
# Mac wrapper: run the checks on cosi-1, then retrieve evidence even on failure.
set -euo pipefail
umask 077
: "${COSIFLOW_RETEST_DIR:?Set COSIFLOW_RETEST_DIR to the experiment directory}"
case "${1:-}" in
  ''|--configure-dns) ;;
  *) echo 'Usage: k3s-network-check.sh [--configure-dns]' >&2; exit 2 ;;
esac
[[ $# -le 1 ]] || exit 2
helpers=$(cd "$(dirname "$0")" && pwd)
ssh_config="$COSIFLOW_RETEST_DIR/ssh_config"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
evidence=$(mktemp -d "$COSIFLOW_RETEST_DIR/k3s-check-$stamp.XXXXXX")
status=0
if [[ ${1:-} == --configure-dns ]]; then
  remote='bash -s -- --configure-dns'
else
  remote='bash -s'
fi
ssh -F "$ssh_config" cosi-1 "$remote" < "$helpers/k3s-check.sh" \
  2>&1 | tee "$evidence/check.log" || status=$?
ssh -F "$ssh_config" cosi-1 'tar -czf - -C "$HOME" k3s-check' \
  > "$evidence/guest-evidence.tar.gz.part" && \
  mv "$evidence/guest-evidence.tar.gz.part" "$evidence/guest-evidence.tar.gz" || {
    echo 'Evidence retrieval failed; guest logs remain under ~/k3s-check.' >&2
    status=1
  }
printf 'Exit status: %s\nEvidence: %s\n' "$status" "$evidence"
exit "$status"
