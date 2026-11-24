#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_dir="$(git -C "${script_dir}/.." rev-parse --show-toplevel)"

git_sha="$(git -C "${repository_dir}" rev-parse HEAD)"
git_tag="$(git -C "${repository_dir}" describe --tags --exact-match HEAD 2>/dev/null || true)"
git_ref="$(git -C "${repository_dir}" branch --show-current)"
if [[ -z "${git_ref}" ]]; then
  git_ref="detached"
fi

origin_url="$(git -C "${repository_dir}" remote get-url origin)"
case "${origin_url}" in
  git@github.com:*) github_url="https://github.com/${origin_url#git@github.com:}" ;;
  https://github.com/*) github_url="${origin_url}" ;;
  *) github_url="https://github.com/cositools/cosiflow" ;;
esac
github_url="${github_url%.git}"

export COSIFLOW_GIT_REF="${git_ref}"
export COSIFLOW_GIT_SHA="${git_sha}"
export COSIFLOW_GIT_TAG="${git_tag}"
export COSIFLOW_GITHUB_URL="${github_url}"

cd "${script_dir}"
docker compose build "$@" airflow-webserver
