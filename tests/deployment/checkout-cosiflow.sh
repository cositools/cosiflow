#!/usr/bin/env bash
# Run on cosi-1, streamed from the Mac. Never replace an existing checkout.
set -euo pipefail
umask 077
ref=${1:-128f6e87d53307c52c36f61d36211c0bab776ffb}
repository=${2:-https://github.com/cositools/cosiflow.git}
[[ $# -le 2 ]] || { echo 'Usage: checkout-cosiflow.sh [commit-or-version-tag] [repository]' >&2; exit 2; }
if [[ ! "$ref" =~ ^[0-9a-fA-F]{40}$ && ! "$ref" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.-]+)?$ ]]; then
  echo 'Use a full 40-character commit SHA or a version tag such as v1.0.0.' >&2
  exit 2
fi
target="$HOME/cosiflow"
if [[ ! -e "$target" ]]; then
  git init "$target"
  git -C "$target" remote add origin "$repository"
else
  [[ -d "$target/.git" ]] || { echo "$target already exists without Git metadata; use a fresh source directory/VM." >&2; exit 1; }
  [[ $(git -C "$target" remote get-url origin) == "$repository" ]] || { echo 'Existing checkout has a different origin.' >&2; exit 1; }
  [[ -z $(git -C "$target" status --porcelain) ]] || { echo 'Existing checkout has local changes; refusing to overwrite it.' >&2; exit 1; }
fi
git -C "$target" fetch --depth=1 origin "$ref"
sha=$(git -C "$target" rev-parse 'FETCH_HEAD^{commit}')
if [[ "$ref" =~ ^[0-9a-fA-F]{40}$ ]]; then
  [[ "$sha" == "$(printf '%s' "$ref" | tr 'A-F' 'a-f')" ]] || exit 1
fi
if current=$(git -C "$target" rev-parse --verify HEAD 2>/dev/null); then
  [[ "$current" == "$sha" ]] || { echo "Existing checkout is at $current; refusing to switch it to $sha." >&2; exit 1; }
fi
git -C "$target" checkout --detach "$sha"
mkdir -p "$HOME/.config/cosiflow-kubernetes"
printf 'repository=%s\nref=%s\ncommit=%s\n' "$repository" "$ref" "$sha" \
  > "$HOME/.config/cosiflow-kubernetes/source-ref.txt"
cat "$HOME/.config/cosiflow-kubernetes/source-ref.txt"
