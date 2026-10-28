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

Build yy from a clean Ubuntu 24.04 x86_64 working tree, package the local
qualification artifact, and write SHA256SUMS. This command does not sign,
tag, publish, or upload the artifact.
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
  printf 'package-release.sh requires Linux x86_64\n' >&2
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
name="yinyang-${version}-linux-x86_64-${source_short}"
archive="$output_dir/$name.tar.gz"

mkdir -p "$output_dir"
if [[ -e "$archive" || -e "$archive.sha256" ]]; then
  printf 'refusing to overwrite existing artifact: %s\n' "$archive" >&2
  exit 1
fi

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
install -m 0644 deploy/linux/yinyang-serve.service \
  deploy/linux/yinyang-mount@.service \
  deploy/linux/yinyang-sync@.service \
  "$tree/lib/systemd/user/"
install -m 0644 \
  docs/linux-operations.md \
  docs/linux-qualification.md \
  docs/linux-operating-envelope.md \
  docs/linux-backup-recovery.md \
  docs/linux-compatibility-acceptance.md \
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
  printf 'target=linux-x86_64\n'
  printf 'yy_sha256=%s\n' "$binary_sha256"
  printf 'cargo=%s\n' "$cargo_version"
  printf 'rustc=%s\n' "$rustc_version"
} >"$tree/BUILD-MANIFEST"

cat >"$tree/INSTALL" <<EOF
Apache OpenDAL YinYang local qualification artifact

Release: $release
Source:  $source_sha
Binary:  $binary_sha256

This artifact is not a signed ASF release. Verify the adjacent SHA-256 file
before use, then extract it outside the installation prefix.

Install into a new immutable release directory:

  set -eu
  umask 077
  YY_ROOT="\$HOME/.local/lib/yinyang"
  YY_RELEASE="$release"
  install -d -m 0700 "\$YY_ROOT/\$YY_RELEASE/bin"
  install -m 0755 "$name/bin/yy" "\$YY_ROOT/\$YY_RELEASE/bin/yy"
  ln -s "\$YY_RELEASE" "\$YY_ROOT/.current.new"
  mv -Tf "\$YY_ROOT/.current.new" "\$YY_ROOT/current"

Stop every yy service before changing current. Preserve the configuration and
state directories. Start the existing user units only after the new current
target, binary SHA-256, doctor output and retained volume state have been
verified. Read share/doc/yinyang/linux-operations.md for the full workflow and
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
printf '%s  %s\n' "$archive_sha256" "$(basename "$archive")" >"$archive.sha256"

verify=$(mktemp -d)
trap 'rm -rf "$staging" "$verify"' EXIT
tar -xzf "$archive" -C "$verify"
(
  cd "$verify/$name"
  sha256sum -c <(printf '%s  bin/yy\n' "$binary_sha256")
)

printf 'archive=%s\n' "$archive"
printf 'archive_sha256=%s\n' "$archive_sha256"
printf 'release=%s\n' "$release"
printf 'source_commit=%s\n' "$source_sha"
printf 'yy_sha256=%s\n' "$binary_sha256"
