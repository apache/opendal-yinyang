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
Usage:
  install-release.sh install --archive FILE --prefix DIR [--activate --quiesced]
  install-release.sh activate --prefix DIR --release RELEASE --quiesced
  install-release.sh inspect --prefix DIR

Install creates one immutable release directory from a verified trial archive.
It never changes configuration, state or an existing release directory.

activation atomically updates the prefix's current symlink. --quiesced asserts
that every yy writer using the prefix has been stopped. The installer has no
downgrade command: after a newer binary has opened or written state, do not
relaunch an older binary against it.

inspect is read-only and reports installed release-directory sizes. It does not
inspect or authorize deletion of backend object versions, external readers,
shared physical packs, backups, staging or unresolved work.
EOF
}

die() {
  printf 'install-release: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

manifest_value() {
  local manifest=$1
  local key=$2
  local count
  count=$(awk -F= -v key="$key" '$1 == key { count++ } END { print count + 0 }' "$manifest")
  [[ $count == 1 ]] || die "$manifest must contain exactly one $key field"
  awk -F= -v key="$key" '
    $1 == key {
      sub(/^[^=]*=/, "")
      print
      exit
    }
  ' "$manifest"
}

require_host() {
  [[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] ||
    die "trial distributions require Linux x86_64"
  # shellcheck disable=SC1091
  . /etc/os-release
  [[ ${ID:-} == ubuntu && ${VERSION_ID:-} == 24.04 ]] ||
    die "trial distributions require Ubuntu 24.04 x86_64"
}

prepare_existing_prefix() {
  [[ -d $prefix ]] || die "installation prefix does not exist: $prefix"
  prefix=$(cd "$prefix" && pwd -P)
}

prepare_new_prefix() {
  install -d -m 0700 "$prefix"
  prefix=$(cd "$prefix" && pwd -P)
}

atomic_symlink() {
  local target=$1
  local name=$2
  local temporary="$prefix/.$name.$$"
  rm -f "$temporary"
  ln -s "$target" "$temporary"
  mv -Tf "$temporary" "$prefix/$name"
}

symlink_target() {
  local name=$1
  local path="$prefix/$name"
  [[ -L $path ]] || {
    [[ ! -e $path ]] || die "$path exists but is not a symlink"
    printf ''
    return
  }
  readlink "$path"
}

validate_installed_release() {
  local release=$1
  [[ $release =~ ^[0-9A-Za-z][0-9A-Za-z.+_-]*$ ]] ||
    die "invalid release directory name: $release"
  local directory="$prefix/$release"
  [[ -d $directory ]] || die "release directory does not exist: $directory"
  local binary="$directory/bin/yy"
  [[ -f $binary && -x $binary ]] || die "release is missing executable bin/yy: $directory"

  local manifest="$directory/BUILD-MANIFEST"
  if [[ -f $manifest ]]; then
    local manifest_release
    local expected_sha256
    local actual_sha256
    manifest_release=$(manifest_value "$manifest" release)
    expected_sha256=$(manifest_value "$manifest" yy_sha256)
    [[ $manifest_release == "$release" ]] ||
      die "release directory $release does not match BUILD-MANIFEST"
    actual_sha256=$(sha256sum "$binary" | awk '{print $1}')
    [[ $actual_sha256 == "$expected_sha256" ]] ||
      die "release binary SHA-256 does not match BUILD-MANIFEST: $release"
  fi
  "$binary" --help >/dev/null || die "release binary cannot execute: $release"
}

activate() {
  local release=$1
  local quiesced=$2
  [[ $quiesced == true ]] ||
    die "activation requires --quiesced after stopping every yy writer"
  prepare_existing_prefix
  validate_installed_release "$release"

  local current
  current=$(symlink_target current)
  if [[ -n $current ]]; then
    [[ $current != */* ]] || die "current must be a relative release-directory symlink"
    validate_installed_release "$current"
  fi
  atomic_symlink "$release" current
  printf 'activated_release=%s\n' "$release"
}

operation=${1:-}
if [[ -z $operation ]]; then
  usage >&2
  exit 2
fi
shift

prefix=
archive=
release=
quiesced=false
activate_after_install=false
while (($#)); do
  case "$1" in
    --prefix)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      prefix=$2
      shift 2
      ;;
    --archive)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      archive=$2
      shift 2
      ;;
    --release)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      release=$2
      shift 2
      ;;
    --quiesced)
      quiesced=true
      shift
      ;;
    --activate)
      activate_after_install=true
      shift
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

[[ -n $prefix ]] || {
  usage >&2
  exit 2
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
verifier="$script_dir/verify-release.sh"
for name in awk basename dirname du install ln mktemp mv readlink rm sha256sum sort tar uname; do
  require_command "$name"
done

case "$operation" in
  install)
    [[ -n $archive ]] || {
      usage >&2
      exit 2
    }
    [[ -z $release ]] || die "install derives the release from the archive"
    [[ $activate_after_install == false || $quiesced == true ]] ||
      die "--activate requires --quiesced after stopping every yy writer"
    require_host
    [[ -f $verifier ]] || die "verify-release.sh is missing beside install-release.sh"
    prepare_new_prefix

    verification=$("$verifier" --archive "$archive")
    printf '%s\n' "$verification"
    installed_release=$(awk -F= '$1 == "release" { print $2 }' <<<"$verification")
    artifact_root=$(awk -F= '$1 == "artifact_root" { print $2 }' <<<"$verification")
    [[ -n $installed_release && -n $artifact_root ]] ||
      die "verify-release.sh did not report the artifact identity"
    final="$prefix/$installed_release"
    [[ ! -e $final ]] ||
      die "refusing to overwrite existing release directory: $final"

    staging=$(mktemp -d "$prefix/.install.XXXXXX")
    trap 'rm -rf "$staging"' EXIT
    tar -xzf "$archive" --no-same-owner --no-same-permissions -C "$staging"
    [[ -d "$staging/$artifact_root" ]] || die "verified archive root is missing"
    mv "$staging/$artifact_root" "$final"
    rmdir "$staging"
    trap - EXIT

    validate_installed_release "$installed_release"
    installed_bytes=$(du -sb "$final" | awk '{print $1}')
    printf 'installed_release=%s\n' "$installed_release"
    printf 'installed_path=%s\n' "$final"
    printf 'installed_bytes=%s\n' "$installed_bytes"
    if [[ $activate_after_install == true ]]; then
      activate "$installed_release" "$quiesced"
    fi
    ;;
  activate)
    [[ -n $release ]] || {
      usage >&2
      exit 2
    }
    [[ -z $archive ]] || die "activate does not accept --archive"
    require_host
    activate "$release" "$quiesced"
    ;;
  inspect)
    [[ -z $archive && -z $release ]] || die "inspect accepts neither --archive nor --release"
    prepare_existing_prefix
    current=$(symlink_target current)
    printf 'schema=yinyang-linux-release-inventory.v1\n'
    printf 'prefix=%s\n' "$prefix"
    printf 'current=%s\n' "${current:-none}"
    printf 'scope=installed-release-directories-only\n'
    printf 'space_unknown=backend-versions,external-readers,shared-packs,staging,metadata,wal,backups,archives\n'
    printf 'retention=manual-no-deletion\n'
    entries=
    shopt -s nullglob
    for directory in "$prefix"/*; do
      [[ -d $directory && ! -L $directory ]] || continue
      name=$(basename "$directory")
      [[ $name != .* ]] || continue
      bytes=$(du -sb "$directory" | awk '{print $1}')
      active=false
      [[ $name != "$current" ]] || active=true
      entries+="$name $bytes $active"$'\n'
    done
    shopt -u nullglob
    sorted=$(printf '%s' "$entries" | sort)
    retained=0
    while IFS=' ' read -r name bytes active; do
      [[ -n $name ]] || continue
      retained=$((retained + 1))
      printf 'release=%s bytes=%s active=%s\n' "$name" "$bytes" "$active"
    done <<<"$sorted"
    printf 'retained_releases=%s\n' "$retained"
    ;;
  -h | --help | help)
    usage
    ;;
  *)
    printf 'unknown command: %s\n' "$operation" >&2
    usage >&2
    exit 2
    ;;
esac
