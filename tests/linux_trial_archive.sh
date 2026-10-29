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
umask 077

usage() {
  cat <<'EOF'
Usage: tests/linux_trial_archive.sh --yy FILE --evidence FILE

Exercise the installed archive binary through an isolated object-backed volume:
create an authority, write and fsync retained file bytes, read them back, and
require doctor JSON to report a passing state. The isolated YINYANG_S3_*
environment must already be set.
EOF
}

fail() {
  printf 'linux-trial-archive: %s\n' "$*" >&2
  exit 1
}

yy=
evidence=
while (($#)); do
  case "$1" in
    --yy)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      yy=$2
      shift 2
      ;;
    --evidence)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      evidence=$2
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

[[ -n $yy && -n $evidence ]] || {
  usage >&2
  exit 2
}
[[ -f $yy && -x $yy ]] || fail "installed archive binary is missing: $yy"
for name in \
  YINYANG_S3_BUCKET \
  YINYANG_S3_ENDPOINT \
  YINYANG_S3_REGION \
  YINYANG_S3_ACCESS_KEY_ID \
  YINYANG_S3_SECRET_ACCESS_KEY; do
  [[ -n ${!name:-} ]] || fail "$name is unset"
done

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/state" "$evidence"
payload="$work/payload"
printf 'installed trial archive bytes\n' >"$payload"

config="$work/volume.json"
cat >"$config" <<EOF
{
  "name": "trial-archive",
  "model": "managed",
  "access": "mount",
  "frontend": "library",
  "publication": "object",
  "storage_profile": "minio",
  "staging": "$work/state",
  "read_only": false,
  "require": [],
  "service_address": null
}
EOF

"$yy" create
"$yy" --volume "$config" file create hello.txt
handle=$("$yy" --volume "$config" file open hello.txt --write)
byte_count=$(wc -c <"$payload" | awk '{print $1}')
"$yy" --volume "$config" file write "$handle" "$payload"
"$yy" --volume "$config" file truncate "$handle" "$byte_count"
commit=$("$yy" --volume "$config" file fsync "$handle")
"$yy" --volume "$config" file close "$handle"

read_handle=$("$yy" --volume "$config" file open hello.txt)
"$yy" --volume "$config" file read "$read_handle" >"$work/read-back"
"$yy" --volume "$config" file close "$read_handle"
cmp "$payload" "$work/read-back"

"$yy" --volume "$config" doctor --json >"$evidence/doctor.json"
python3 - "$evidence/doctor.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    document = json.load(source)

if document.get("schema") != "yinyang.operational-health.v1":
    raise SystemExit(f"unexpected doctor schema: {document.get('schema')!r}")
if document.get("status") != "pass":
    raise SystemExit(f"doctor did not pass: {document!r}")
PY

{
  printf 'schema=yinyang-linux-trial-archive-smoke.v1\n'
  printf 'status=pass\n'
  printf 'yy_sha256=%s\n' "$(sha256sum "$yy" | awk '{print $1}')"
  printf 'file_fsync=%s\n' "$commit"
  printf 'doctor_status=pass\n'
} | tee "$evidence/archive-smoke.txt"
