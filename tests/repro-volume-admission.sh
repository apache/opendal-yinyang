#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

workspace=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

mkdir -p "$scratch/cfgdir"
cat >"$scratch/cfgdir/sync.json" <<'JSON'
{"name":"nostaging","model":"managed","access":"sync","frontend":"library","publication":"object","storage_profile":"minio"}
JSON

cargo build --manifest-path "$workspace/Cargo.toml" --locked --bin yy
yy="$workspace/target/debug/yy"

run() {
    printf '\n$ yy %s\n' "$*"
    set +e
    output=$("$yy" "$@" 2>&1)
    code=$?
    set -e
    printf '%s\n' "$output"
    printf 'exit=%s\n' "$code"
    test "$code" -ne 0
}

run --volume "$scratch/cfgdir/sync.json" capabilities
run --volume "$scratch/cfgdir/sync.json" sync init --directory "$scratch/cfgdir/root"
run --volume "$scratch/cfgdir/sync.json" sync run
run --volume "$scratch/cfgdir/sync.json" doctor --json

printf '\n$ find cfgdir\n'
find "$scratch/cfgdir" -mindepth 1 -printf '%P\n' | sort
test "$(find "$scratch/cfgdir" -mindepth 1 | wc -l)" -eq 1
