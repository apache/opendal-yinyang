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

# Run in tmux from the checkout root; preserve evidence outside the repository.
set -euo pipefail
umask 077
. "$HOME/.cargo/env"
state_dir="$HOME/.local/state/yinyang-devbox"
mkdir -p "$state_dir"
exec > >(tee -a "$state_dir/verify.log") 2>&1
finish() {
    result=${1:-$?}
    printf '%s\n' "$result" > "$state_dir/verify.exit.tmp"
    mv "$state_dir/verify.exit.tmp" "$state_dir/verify.exit"
}
trap finish EXIT
date -Is
git rev-parse HEAD
uname -a
rustc --version
cargo --version
cargo x --help
for task in lint check test licenses; do
    cargo x "$task" --help
    cargo x "$task"
done
cargo +1.91.0 check --workspace --all-targets --locked
export YINYANG_S3_BUCKET=yinyang-test
export YINYANG_S3_ENDPOINT=http://127.0.0.1:9000
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ACCESS_KEY_ID=yinyang-test
export YINYANG_S3_SECRET_ACCESS_KEY=yinyang-test-only
export YINYANG_STORAGE_PROFILE=minio
cargo test --workspace --all-targets s3_ -- --ignored --nocapture

# The distro example checks host FUSE readiness, not YinYang Mount correctness.
example=$(dpkg -L libfuse3-dev | sed -n '\|/examples/hello.c$|p' | head -n 1)
test -n "$example"
cc -Wall "$example" $(pkg-config fuse3 --cflags --libs) -o "$state_dir/fuse-hello"
mount_dir=$(mktemp -d "$state_dir/fuse-smoke.XXXXXX")
"$state_dir/fuse-hello" -f "$mount_dir" > "$state_dir/fuse-hello.log" 2>&1 &
fuse_pid=$!
cleanup_fuse() {
    if mountpoint -q "$mount_dir"; then
        fusermount3 -u "$mount_dir"
    fi
    wait "$fuse_pid" || true
    rmdir "$mount_dir"
}
trap 'exit_code=$?; cleanup_fuse; finish "$exit_code"' EXIT
for attempt in {1..30}; do
    if mountpoint -q "$mount_dir"; then break; fi
    sleep 1
done
mountpoint -q "$mount_dir"
findmnt --target "$mount_dir" --output TARGET,SOURCE,FSTYPE,OPTIONS
test "$(cat "$mount_dir/hello")" = 'Hello World!'
stat "$mount_dir/hello"
fusermount3 -u "$mount_dir"
wait "$fuse_pid"
rmdir "$mount_dir"
trap finish EXIT
printf '%s\n' 'Non-root FUSE mount/read/stat/unmount passed.'
date -Is

