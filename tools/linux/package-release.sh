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
umask 022

usage() {
  cat <<'EOF'
Usage: tools/linux/package-release.sh --output-dir DIR

Build yy from a clean Ubuntu 24.04 x86_64 working tree. Write the directly
runnable yy, its checksum, LICENSE, NOTICE and source manifest, then package the
local qualification archive and its adjacent SHA-256 sidecar. This command does
not sign, tag, publish, or upload the artifacts.
EOF
}

output_dir=
while (($#)); do
  case "$1" in
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

[[ -n "$output_dir" ]] || {
  usage >&2
  exit 2
}

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  printf 'package-release.sh requires Ubuntu 24.04 x86_64\n' >&2
  exit 1
fi

# shellcheck disable=SC1091
. /etc/os-release
if [[ ${ID:-} != ubuntu || ${VERSION_ID:-} != 24.04 ]]; then
  printf 'package-release.sh requires Ubuntu 24.04 x86_64\n' >&2
  exit 1
fi

for command in cargo git gzip install python3 sha256sum tar; do
  command -v "$command" >/dev/null || {
    printf 'required command is unavailable: %s\n' "$command" >&2
    exit 1
  }
done

root=$(git rev-parse --show-toplevel)
cd "$root"

if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
  printf 'refusing to package a dirty worktree\n' >&2
  git status --short >&2
  exit 1
fi

source_sha=$(git rev-parse HEAD)
source_short=${source_sha:0:12}
source_date_epoch=$(git show -s --format=%ct HEAD)
version=$(
  cargo metadata --no-deps --format-version 1 |
    python3 -c 'import json, sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "yinyang"))'
)
release="${version}+${source_short}"
name="yinyang-${version}-ubuntu-24.04-x86_64-${source_short}"
archive="$output_dir/$name.tar.gz"
archive_name=$(basename "$archive")

mkdir -p "$output_dir"
if [[ -e "$archive" || -e "$archive.sha256" ]]; then
  printf 'refusing to overwrite existing artifact: %s\n' "$archive" >&2
  exit 1
fi
for artifact in yy yy.sha256 LICENSE NOTICE BUILD-MANIFEST; do
  if [[ -e "$output_dir/$artifact" ]]; then
    printf 'refusing to overwrite existing artifact: %s\n' "$output_dir/$artifact" >&2
    exit 1
  fi
done

cargo build --release --locked --bin yy

target_dir=${CARGO_TARGET_DIR:-target}
if [[ "$target_dir" != /* ]]; then
  target_dir="$root/$target_dir"
fi
yy="$target_dir/release/yy"
[[ -f "$yy" ]] || {
  printf 'release binary is missing: %s\n' "$yy" >&2
  exit 1
}

staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
tree="$staging/$name"
install -d -m 0755 \
  "$tree/bin" \
  "$tree/lib/systemd/user" \
  "$tree/share/doc/yinyang" \
  "$tree/share/yinyang"
install -m 0755 "$yy" "$tree/bin/yy"
install -m 0644 LICENSE NOTICE "$tree/"
install -m 0644 deploy/linux/environment.example "$tree/share/yinyang/"
install -m 0755 \
  tools/linux/install-release.sh \
  tools/linux/verify-release.sh \
  "$tree/share/yinyang/"
install -m 0644 deploy/linux/yinyang-serve.service \
  deploy/linux/yinyang-mount@.service \
  deploy/linux/yinyang-sync@.service \
  "$tree/lib/systemd/user/"
install -m 0644 \
  docs/linux-operations.md \
  docs/linux-qualification.md \
  docs/linux-operating-envelope.md \
  docs/linux-backup-recovery.md \
  docs/linux-trial-distribution.md \
  "$tree/share/doc/yinyang/"

binary_sha256=$(sha256sum "$tree/bin/yy" | awk '{print $1}')
cargo_version=$(cargo --version)
rustc_version=$(rustc --version)
{
  printf 'schema=yinyang-linux-release-artifact.v1\n'
  printf 'package=yinyang\n'
  printf 'version=%s\n' "$version"
  printf 'release=%s\n' "$release"
  printf 'source_commit=%s\n' "$source_sha"
  printf 'source_date_epoch=%s\n' "$source_date_epoch"
  printf 'target=ubuntu-24.04-x86_64\n'
  printf 'yy_sha256=%s\n' "$binary_sha256"
  printf 'cargo=%s\n' "$cargo_version"
  printf 'rustc=%s\n' "$rustc_version"
} >"$tree/BUILD-MANIFEST"

cat >"$tree/PREREQUISITES" <<EOF
schema=yinyang-linux-release-prerequisites.v1
release=$release
source_commit=$source_sha
target=ubuntu-24.04-x86_64
host=Ubuntu 24.04 x86_64
apt_packages=ca-certificates fuse3
optional_apt_packages=sqlite3
service_manager=systemd user manager only for the supplied unit templates
storage=an operator-supplied supported S3 endpoint and credentials
state_location=configuration, staging and metadata stay outside the release prefix
cargo_required=false
rustc_required=false
git_required=false
python_required=false
note=This local trial artifact is not a signed Apache Software Foundation release.
EOF

cat >"$tree/INSTALL" <<EOF
Apache OpenDAL YinYang local qualification artifact

Release: $release
Source:  $source_sha
Binary:  $binary_sha256

This artifact is not a signed ASF release. Verify the adjacent SHA-256 file
before use, then extract it outside the installation prefix. Read
PREREQUISITES and share/doc/yinyang/linux-trial-distribution.md first.

Verify and install into a new immutable release directory:

  set -eu
  umask 077
  archive=/absolute/path/to/$archive_name
  YY_ROOT="\$HOME/.local/lib/yinyang"
  extract=\$(mktemp -d)
  tar -xzf "\$archive" -C "\$extract"
  installer="\$extract/$name/share/yinyang/install-release.sh"
  "\$installer" install --archive "\$archive" --prefix "\$YY_ROOT"

Stop every yy service before activation. Preserve the configuration, staging,
Sync and metadata state directories. Then activate and verify:

  "\$installer" activate --prefix "\$YY_ROOT" --release "$release" --quiesced

Start the existing user units only after the new current target, binary SHA-256,
doctor output and retained volume state have been verified. The installer never
starts, stops or restarts units. Read
share/doc/yinyang/linux-operations.md and
share/doc/yinyang/linux-trial-distribution.md for the full workflow and
supported transition boundary.
EOF

(
  cd "$staging"
  tar \
    --sort=name \
    --mtime="@$source_date_epoch" \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    -cf - "$name" |
    gzip -n -9 >"$archive"
)

archive_sha256=$(sha256sum "$archive" | awk '{print $1}')
printf '%s  %s\n' "$archive_sha256" "$archive_name" >"$archive.sha256"

tools/linux/verify-release.sh \
  --archive "$archive" \
  --source-commit "$source_sha"

install -m 0755 "$tree/bin/yy" "$output_dir/yy"
printf '%s  yy\n' "$binary_sha256" >"$output_dir/yy.sha256"
install -m 0644 \
  "$tree/LICENSE" \
  "$tree/NOTICE" \
  "$tree/BUILD-MANIFEST" \
  "$output_dir/"

(
  cd "$output_dir"
  sha256sum --check yy.sha256
  ./yy --help >/dev/null
)

printf 'archive=%s\n' "$archive"
printf 'archive_sha256=%s\n' "$archive_sha256"
printf 'binary=%s\n' "$output_dir/yy"
printf 'binary_checksum=%s\n' "$output_dir/yy.sha256"
printf 'manifest=%s\n' "$output_dir/BUILD-MANIFEST"
printf 'release=%s\n' "$release"
printf 'source_commit=%s\n' "$source_sha"
printf 'yy_sha256=%s\n' "$binary_sha256"
