#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

# Task-only nier runner for the 2026-11 architecture remediation.
#
# Usage: run.sh LABEL BRANCH SHA COMMAND [ARG...]
#
# Fetches BRANCH from the nier transfer remote, checks out the exact SHA in a
# per-branch worktree, runs COMMAND there with the repository validation
# environment, and records ~/.local/state/yinyang-nier/runs/LABEL.{log,exit}.
set -euo pipefail
umask 022

label=${1:?label required}
branch=${2:?branch required}
sha=${3:?full commit SHA required}
shift 3
[[ "$label" =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "invalid label: $label" >&2; exit 2; }
[[ "$branch" =~ ^[a-z0-9._/-]+$ ]] || { echo "invalid branch: $branch" >&2; exit 2; }
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || { echo "a full 40-hex SHA is required" >&2; exit 2; }
test "$#" -gt 0 || { echo "command required" >&2; exit 2; }

devbox=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
base="$HOME/Code/yinyang"
repo="$base/opendal-yinyang"
state="$HOME/.local/state/yinyang-nier"
runs="$state/runs"
mkdir -p "$runs" "$base/wt"
log="$runs/$label.log"
if [[ -e "$log" || -e "$runs/$label.exit" ]]; then
    echo "label already used: $label" >&2
    exit 2
fi

exec > >(tee "$log") 2>&1
finish() {
    local rc=$?
    printf '%s\n' "$rc" > "$runs/$label.exit.tmp"
    mv "$runs/$label.exit.tmp" "$runs/$label.exit"
}
trap finish EXIT

date -Is
echo "label=$label branch=$branch sha=$sha host=$(hostname)"

wt_name=${branch//\//-}
wt="$base/wt/$wt_name"

# One run per worktree at a time; another label on the same branch waits.
exec 7> "$state/wt-$wt_name.lock"
if ! flock -n 7; then
    echo "waiting for worktree $wt_name"
    flock -w 14400 7
fi

# Serialize fetches and worktree creation in the shared clone.
exec 8> "$state/git.lock"
flock -w 600 8
git -C "$repo" fetch --quiet dev "+refs/heads/$branch:refs/remotes/dev/$branch"
git -C "$repo" cat-file -e "$sha^{commit}"
if [[ ! -d "$wt" ]]; then
    git -C "$repo" worktree add --quiet --detach "$wt" "$sha"
fi
flock -u 8

if [[ -n "$(git -C "$wt" status --porcelain --untracked-files=no)" ]]; then
    echo "worktree $wt has tracked modifications; refusing to run" >&2
    exit 2
fi
git -C "$wt" checkout --quiet --detach "$sha"
test "$(git -C "$wt" rev-parse HEAD)" = "$sha"
untracked=$(git -C "$wt" status --porcelain | wc -l)
if [[ "$untracked" -ne 0 ]]; then
    echo "warning: $untracked untracked paths in $wt"
fi

# Bound concurrent heavy runs; mbx still schedules the builds themselves.
slots=${YY_NIER_SLOTS:-3}
acquired=
for i in $(seq 1 "$slots"); do
    exec 9> "$state/slot-$i.lock"
    if flock -n 9; then
        acquired=$i
        break
    fi
done
if [[ -z "$acquired" ]]; then
    acquired=$(( (RANDOM % slots) + 1 ))
    echo "waiting for validation slot $acquired"
    exec 9> "$state/slot-$acquired.lock"
    flock -w 14400 9
fi

. "$HOME/.cargo/env"
export PATH="$HOME/.local/share/mbx/bin:$HOME/.local/lib/yinyang-devbox-tools/cargo-deny-0.14.22/bin:$HOME/.local/bin:$PATH"
unset RUSTUP_TOOLCHAIN CARGO_TARGET_DIR CARGO_BUILD_BUILD_DIR
export YINYANG_S3_BUCKET=yinyang-test
export YINYANG_S3_ENDPOINT=http://127.0.0.1:19000
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ACCESS_KEY_ID=yinyang-test
export YINYANG_S3_SECRET_ACCESS_KEY=yinyang-test-only
export YINYANG_STORAGE_PROFILE=minio
export YINYANG_DIRECT_VERSIONED_BUCKET=yinyang-direct-versioned
export YY_NIER_DEVBOX="$devbox"

date -Is
echo "slot=$acquired worktree=$wt"
git -C "$wt" log -1 --format='commit %H %s'
printf 'command:'
printf ' %q' "$@"
printf '\n'
command -v cargo
cargo --version
mbx --version

cd "$wt"
set +e
"$@"
rc=$?
set -e
date -Is
echo "result exit=$rc"
exit "$rc"
