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

# Task-only runner: invoke from a pinned checkout under tmux.
set -euo pipefail
umask 077
label=${1:?validation label required}
shift
[[ "$label" =~ ^[a-z0-9-]+$ ]]
test "$#" -gt 0
. "$HOME/.cargo/env"
export PATH="$HOME/.local/bin:$PATH"
unset RUSTUP_TOOLCHAIN CARGO_TARGET_DIR CARGO_BUILD_BUILD_DIR
state="$HOME/.local/state/yinyang-devbox"
mkdir -p "$state/runs"
log="$state/runs/$label"
test ! -e "$log.log"
exec > >(tee "$log.log") 2>&1
finish() {
    result=$?
    printf '%s\n' "$result" > "$log.exit.tmp"
    mv "$log.exit.tmp" "$log.exit"
}
trap finish EXIT
date -Is
printf 'Waiting for shared validation slot (up to 30 minutes).\n'
exec 9> "$state/validation.lock"
flock -w 1800 9
git rev-parse HEAD
git status --short
printf 'Command:'
printf ' %q' "$@"
printf '\n'
mbx --version
mise --version
mise exec -- cargo --version
export YINYANG_S3_BUCKET=yinyang-test
export YINYANG_S3_ENDPOINT=http://127.0.0.1:9000
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ACCESS_KEY_ID=yinyang-test
export YINYANG_S3_SECRET_ACCESS_KEY=yinyang-test-only
export YINYANG_STORAGE_PROFILE=minio
# Evidence remains private; native file-mode probes use the ordinary user mask.
umask 022
mise exec -- "$@"
date -Is
