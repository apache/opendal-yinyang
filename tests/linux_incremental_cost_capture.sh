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
export LC_ALL=C

usage() {
  cat <<'EOF'
Usage: tests/linux_incremental_cost_capture.sh \
  --expected-source SHA \
  [--workload incremental|length-changing] \
  --expect baseline|required \
  --label LABEL \
  --output-dir DIR

Run a writable incremental Linux cost workload from the current clean checkout and
preserve its exact release yy binary, hashes, source identity and raw collector
records under DIR. Invoke this script through the shared Linux validation runner
so the copy is made while the build lock is still held.
EOF
}

fail() {
  printf 'linux-incremental-cost-capture: %s\n' "$*" >&2
  exit 1
}

expected_source=
workload=incremental
expectation=
label=
output_dir=
while (($#)); do
  case "$1" in
    --expected-source)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      expected_source=$2
      shift 2
      ;;
    --workload)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      workload=$2
      shift 2
      ;;
    --expect)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      expectation=$2
      shift 2
      ;;
    --label)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      label=$2
      shift 2
      ;;
    --output-dir)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      output_dir=$2
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n $expected_source && -n $expectation && -n $label && -n $output_dir ]] || {
  usage >&2
  exit 2
}
[[ $expected_source =~ ^[0-9a-f]{40}$ ]] ||
  fail "expected source must be a full lowercase SHA"
[[ $label =~ ^[a-z0-9-]+$ ]] || fail "label must be lowercase letters, digits and hyphens"
[[ $workload == incremental || $workload == length-changing ]] ||
  fail "workload must be incremental or length-changing"
[[ $expectation == baseline || $expectation == required ]] ||
  fail "expectation must be baseline or required"

root=$(git rev-parse --show-toplevel)
cd "$root"
source_sha=$(git rev-parse HEAD)
[[ $source_sha == "$expected_source" ]] ||
  fail "checkout is $source_sha, expected $expected_source"
[[ -z $(git status --porcelain --untracked-files=normal) ]] ||
  fail "refusing to measure a dirty checkout"

mkdir -m 0700 -p "$output_dir"
jsonl="$output_dir/$label.jsonl"
summary="$output_dir/$label.md"
binary="$output_dir/$label.yy"
manifest="$output_dir/$label.binary.txt"
for path in "$jsonl" "$summary" "$binary" "$manifest"; do
  [[ ! -e $path ]] || fail "refusing to overwrite evidence: $path"
done

printf 'source=%s workload=%s expectation=%s label=%s\n' \
  "$source_sha" "$workload" "$expectation" "$label"
cargo x bench-linux-cost \
  --workload "$workload" \
  --incremental-expect "$expectation" \
  --publication all \
  --output "$jsonl"

readarray -t identity < <(
python3 - "$jsonl" "$source_sha" "$workload" <<'PY'
import json
import sys

path, expected_source, expected_workload = sys.argv[1:]
records = []
with open(path, encoding="utf-8") as source:
    for line in source:
        record = json.loads(line)
        if record.get("schema") != "yinyang.linux-cost.v6":
            raise SystemExit(f"unexpected schema: {record.get('schema')!r}")
        if record.get("workload") != expected_workload:
            raise SystemExit(f"unexpected workload: {record.get('workload')!r}")
        if record.get("source_sha") != expected_source:
            raise SystemExit(f"unexpected source: {record.get('source_sha')!r}")
        if record.get("source_dirty") is not False:
            raise SystemExit("benchmark record reports a dirty source")
        if record.get("publication") not in {"object", "service"}:
            raise SystemExit(f"unexpected publication: {record.get('publication')!r}")
        useful_bytes = record.get("useful_bytes")
        if not isinstance(useful_bytes, int) or useful_bytes < 0:
            raise SystemExit(f"unexpected useful_bytes: {useful_bytes!r}")
        if expected_workload == "length-changing":
            scenarios = {
                "fuse_length_changing_small_append",
                "fuse_length_changing_boundary_append",
                "fuse_length_changing_truncate",
                "fuse_length_changing_zero_extension",
                "fuse_length_changing_shrink_then_extend",
                "fuse_length_changing_unchanged_fsync",
                "fuse_length_changing_restart_tail_read",
            }
            if record.get("scenario") in scenarios and record.get("iterations", 0) < 2:
                raise SystemExit(
                    f"{record.get('scenario')} did not retain multiple samples"
                )
        records.append(record)

if not records:
    raise SystemExit("benchmark wrote no records")
if expected_workload == "length-changing":
    required = {
        "fuse_length_changing_open_shared_reader",
        "fuse_length_changing_open_writer",
        "fuse_length_changing_small_append",
        "fuse_length_changing_boundary_append",
        "fuse_length_changing_truncate",
        "fuse_length_changing_zero_extension",
        "fuse_length_changing_shrink_then_extend",
        "fuse_length_changing_unchanged_fsync",
        "fuse_length_changing_restart_tail_read",
    }
    seen = {record.get("scenario") for record in records}
    missing = sorted(required - seen)
    if missing:
        raise SystemExit(f"length-changing workload is missing records: {missing}")
binary_paths = {record.get("binary_path") for record in records}
binary_hashes = {record.get("binary_blake3") for record in records}
if len(binary_paths) != 1 or len(binary_hashes) != 1:
    raise SystemExit("records do not identify one binary")
binary_path = next(iter(binary_paths))
binary_hash = next(iter(binary_hashes))
if not isinstance(binary_path, str) or not binary_path:
    raise SystemExit("benchmark record has no binary path")
if not isinstance(binary_hash, str) or len(binary_hash) != 64:
    raise SystemExit("benchmark record has no BLAKE3 identity")
print(binary_path)
print(binary_hash)
PY
)
benchmark_binary=${identity[0]}
benchmark_blake3=${identity[1]}
[[ -f $benchmark_binary && -x $benchmark_binary ]] ||
  fail "measured binary is missing: $benchmark_binary"
install -m 0555 "$benchmark_binary" "$binary"

asserted_binary=$(sha256sum "$binary" | awk '{print $1}')
measured_binary=$(sha256sum "$benchmark_binary" | awk '{print $1}')
[[ $asserted_binary == "$measured_binary" ]] ||
  fail "preserved binary SHA-256 differs from the measured output"

{
  printf 'schema=yinyang-linux-incremental-binary.v1\n'
  printf 'label=%s\n' "$label"
  printf 'source_sha=%s\n' "$source_sha"
  printf 'workload=%s\n' "$workload"
  printf 'expectation=%s\n' "$expectation"
  printf 'measured_path=%s\n' "$benchmark_binary"
  printf 'preserved_path=%s\n' "$binary"
  printf 'binary_blake3=%s\n' "$benchmark_blake3"
  printf 'binary_sha256=%s\n' "$asserted_binary"
  printf 'jsonl=%s\n' "$jsonl"
  printf 'summary=%s\n' "$summary"
} >"$manifest"

cat "$manifest"
